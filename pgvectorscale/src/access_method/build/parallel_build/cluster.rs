use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use pgrx::ffi::c_char;
use pgrx::pg_sys::{self, pgstat_progress_update_param};
use pgrx::*;

use crate::access_method::k_means;
use crate::access_method::meta_page::MetaPage;
use crate::access_method::pg_vector::PgVector;
use crate::access_method::stats::{WriteStats, InsertStats};
use crate::access_method::storage::StorageType;
use crate::util::ports::PROGRESS_CREATE_IDX_SUBPHASE;
use crate::access_method::graph::Graph;
use crate::access_method::graph::neighbor_store::{BuilderNeighborCache, GraphNeighborStore};
use crate::access_method::plain::storage::PlainStorage;
use crate::access_method::sbq::storage::SbqSpeedupStorage;
use crate::access_method::storage::Storage;
use crate::util::tape::Tape;
use crate::util::page::PageType;
use crate::util::ItemPointer;
use crate::access_method::distance::DistanceType;
use crate::access_method::distance;

use super::super::{
    ParallelShared, ParallelSharedParams, ParallelBuildState,
    BUILD_PHASE_COLLECTING_VECTORS, BUILD_PHASE_CLUSTERING, BUILD_PHASE_BUILDING_GRAPH,
};
use super::super::parallel::{
    self, ClusterQueues,
    ClusterStartNodes, DEFAULT_QUEUE_CAPACITY,
    SHM_TOC_CLUSTER_QUEUES_KEY, SHM_TOC_CENTROIDS_KEY, SHM_TOC_CLUSTER_START_NODES_KEY,
};

const PARALLEL_BUILD_CLUSTER_CONSUMER_MAIN: *const c_char = c"_vectorscale_build_cluster_consumer_main".as_ptr();

pub struct VectorCollector {
    pub vectors: Vec<Vec<f32>>,
    pub heap_tids: Vec<pg_sys::ItemPointerData>,
    pub max_sample_size: usize,
    pub use_sampling: bool,
    pub total_vectors_seen: usize,
    pub sample_interval: usize,
}

pub struct VectorCollectorWithMeta<'a> {
    pub collector: VectorCollector,
    pub meta_page: &'a MetaPage,
}

pub fn perform_clustering(
    vectors: Vec<Vec<f32>>,
    num_clusters: usize,
) -> (Vec<Vec<f32>>, Vec<usize>) {
    unsafe {
        pgstat_progress_update_param(PROGRESS_CREATE_IDX_SUBPHASE, BUILD_PHASE_CLUSTERING);
    }

    let actual_num_clusters = num_clusters.min(vectors.len());
    let centroids = k_means::k_means(
        actual_num_clusters,
        vectors.clone(),
        false,
        100,
        true,
    );

    notice!("K-means clustering completed with {} centroids", centroids.len());

    let mut cluster_assignments = vec![0usize; vectors.len()];
    for (i, vector) in vectors.iter().enumerate() {
        cluster_assignments[i] = k_means::k_means_lookup(vector, &centroids);
    }

    let cluster_stats: Vec<_> = (0..actual_num_clusters)
        .map(|cluster_id| {
            let count = cluster_assignments.iter().filter(|&&x| x == cluster_id).count();
            (cluster_id, count)
        })
        .collect();

    notice!("Cluster distribution:");
    for (cluster_id, count) in &cluster_stats {
        notice!("  Cluster {}: {} vectors", cluster_id, count);
    }

    (centroids, cluster_assignments)
}

pub fn collect_vectors_for_clustering(
    heap_relation: &PgRelation,
    index_relation: &PgRelation,
    index_info: *mut pg_sys::IndexInfo,
    meta_page: &MetaPage,
    max_sample_size: usize,
    _sample_threshold: usize,
) -> VectorCollector {
    unsafe {
        pgstat_progress_update_param(PROGRESS_CREATE_IDX_SUBPHASE, BUILD_PHASE_COLLECTING_VECTORS);
    }

    let collector = VectorCollector {
        vectors: Vec::new(),
        heap_tids: Vec::new(),
        max_sample_size,
        use_sampling: false,
        total_vectors_seen: 0,
        sample_interval: 1,
    };

    let mut collector_with_meta = VectorCollectorWithMeta {
        collector,
        meta_page,
    };

    unsafe {
        pg_sys::IndexBuildHeapScan(
            heap_relation.as_ptr(),
            index_relation.as_ptr(),
            index_info,
            Some(build_callback_collect_vectors),
            &mut collector_with_meta,
        );
    }

    collector_with_meta.collector
}

pub fn sample_vectors_if_needed(
    collector: &VectorCollector,
    max_sample_size: usize,
    sample_threshold: usize,
) -> (Vec<Vec<f32>>, Vec<pg_sys::ItemPointerData>) {
    let num_vectors = collector.vectors.len();
    let use_sampling = max_sample_size > 0 && (sample_threshold == 0 || num_vectors >= sample_threshold);

    if use_sampling && num_vectors > max_sample_size {
        let sample_interval = (num_vectors as f64 / max_sample_size as f64).ceil() as usize;
        let mut sampled_vectors = Vec::new();
        let mut sampled_heap_tids = Vec::new();
        
        for i in (0..num_vectors).step_by(sample_interval) {
            sampled_vectors.push(collector.vectors[i].clone());
            sampled_heap_tids.push(collector.heap_tids[i]);
        }
        
        notice!(
            "Sampled {} vectors out of {} total vectors (sampling ratio: {:.2}%)",
            sampled_vectors.len(),
            num_vectors,
            (sampled_vectors.len() as f64 / num_vectors as f64) * 100.0
        );
        
        (sampled_vectors, sampled_heap_tids)
    } else {
        notice!("Collected {} vectors for k-means clustering", num_vectors);
        (collector.vectors.clone(), collector.heap_tids.clone())
    }
}

pub fn build_index_with_clustering(
    heaprel: pg_sys::Relation,
    indexrel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    meta_page: &mut MetaPage,
    num_clusters: usize,
    heap_relation: &PgRelation,
    index_relation: &PgRelation,
) -> *mut pg_sys::IndexBuildResult {
    let max_sample_size = crate::access_method::guc::TSV_CLUSTERING_MAX_SAMPLE_SIZE.get() as usize;
    let sample_threshold = crate::access_method::guc::TSV_CLUSTERING_SAMPLE_THRESHOLD.get() as usize;

    let collector = collect_vectors_for_clustering(
        heap_relation,
        index_relation,
        index_info,
        meta_page,
        max_sample_size,
        sample_threshold,
    );

    let (vectors_for_clustering, _heap_tids_for_clustering) = sample_vectors_if_needed(&collector, max_sample_size, sample_threshold);

    if vectors_for_clustering.len() < num_clusters {
        warning!(
            "Number of vectors ({}) is less than number of clusters ({}). Using {} clusters instead.",
            vectors_for_clustering.len(), num_clusters, vectors_for_clustering.len()
        );
    }

    let (centroids, _cluster_assignments) = perform_clustering(vectors_for_clustering, num_clusters);
    let actual_num_clusters = centroids.len();

    // Save centroids to meta page
    meta_page.set_centroids(centroids.clone());

    unsafe {
        pgstat_progress_update_param(PROGRESS_CREATE_IDX_SUBPHASE, BUILD_PHASE_BUILDING_GRAPH);
    }

    let write_stats = super::super::maybe_train_quantizer(index_info, heap_relation, index_relation, meta_page);
    unsafe {
        meta_page.store(index_relation, false);
    };

    let heap_tuples = unsafe { heap_relation.rd_rel.as_ref().unwrap().reltuples as usize };
    let num_dimensions = meta_page.get_num_dimensions_to_index() as usize;

    let workers = if cfg!(feature = "build_parallel")
        && !meta_page.has_labels()
        && meta_page.get_storage_type() == StorageType::SbqCompression
    {
        let forced_workers = crate::access_method::guc::TSV_FORCE_PARALLEL_WORKERS.get();
        if forced_workers >= 0 {
            forced_workers as usize
        } else {
            if heap_tuples >= super::super::min_vectors_for_parallel_build() {
                unsafe { (*index_info).ii_ParallelWorkers as usize }
            } else {
                0
            }
        }
    } else {
        0
    };

    let is_concurrent = unsafe { (*index_info).ii_Concurrent };

    let ntuples = if workers > 0 && actual_num_clusters > 1 {
        do_parallel_cluster_build(
            heaprel,
            indexrel,
            index_info,
            heap_relation,
            index_relation,
            meta_page,
            workers,
            is_concurrent,
            &centroids,
            write_stats,
            num_dimensions,
        )
    } else {
        do_sequential_cluster_build(
            index_info,
            heap_relation,
            index_relation,
            meta_page,
            write_stats,
            &centroids,
            actual_num_clusters,
        )
    };

    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = ntuples as f64;
    result.index_tuples = ntuples as f64;

    result.into_pg()
}

fn do_sequential_cluster_build(
    index_info: *mut pg_sys::IndexInfo,
    heap_relation: &PgRelation,
    index_relation: &PgRelation,
    meta_page: &mut MetaPage,
    write_stats: WriteStats,
    centroids: &[Vec<f32>],
    num_clusters: usize,
) -> usize {
    notice!("Sequential cluster build with {} clusters", num_clusters);
    
    super::super::do_heap_scan_with_clustering(
        index_info,
        heap_relation,
        index_relation,
        meta_page,
        write_stats,
        None,
        0,
        &[],
        &[],
        centroids,
        num_clusters,
    )
}

#[allow(dead_code)]
struct ParallelClusterContext {
    pcxt: *mut pg_sys::ParallelContext,
    snapshot: *mut pg_sys::SnapshotData,
    parallel_shared: *mut ParallelShared,
    cluster_queues: *mut ClusterQueues,
    cluster_start_nodes: *mut ClusterStartNodes,
}

fn do_parallel_cluster_build(
    heaprel: pg_sys::Relation,
    indexrel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    heap_relation: &PgRelation,
    index_relation: &PgRelation,
    meta_page: &mut MetaPage,
    workers: usize,
    is_concurrent: bool,
    centroids: &[Vec<f32>],
    _write_stats: WriteStats,
    num_dimensions: usize,
) -> usize {
    let num_clusters = centroids.len();
    let heap_tuples = unsafe { heap_relation.rd_rel.as_ref().unwrap().reltuples as usize };
    
    notice!(
        "Parallel cluster build with {} workers for {} clusters",
        workers, num_clusters
    );
    notice!(
        "Indexing {} vectors with {} dimensions",
        heap_tuples, num_dimensions
    );

    // Record start time for statistics
    let start_time = std::time::Instant::now();

    unsafe {
        pg_sys::EnterParallelMode();

        let num_workers = num_clusters;
        let pcxt = pg_sys::CreateParallelContext(
            crate::EXTENSION_NAME,
            PARALLEL_BUILD_CLUSTER_CONSUMER_MAIN,
            num_workers as i32,
        );

        let snapshot = if is_concurrent {
            pg_sys::RegisterSnapshot(pg_sys::GetTransactionSnapshot())
        } else {
            &raw mut pg_sys::SnapshotAnyData
        };

        parallel::toc_estimate_single_chunk(pcxt, std::mem::size_of::<ParallelShared>());
        
        // ClusterQueues structure + queue data storage (contiguous)
        let cluster_queues_size = ClusterQueues::calculate_size(num_clusters, DEFAULT_QUEUE_CAPACITY, num_dimensions);
        parallel::toc_estimate_single_chunk(pcxt, cluster_queues_size);

        let centroids_size = std::mem::size_of::<usize>() 
            + num_clusters * (std::mem::size_of::<usize>() + num_dimensions * std::mem::size_of::<f32>());
        parallel::toc_estimate_single_chunk(pcxt, centroids_size);

        let start_nodes_size = std::mem::size_of::<ClusterStartNodes>();
        parallel::toc_estimate_single_chunk(pcxt, start_nodes_size);

        pg_sys::InitializeParallelDSM(pcxt);

        if (*pcxt).seg.is_null() {
            warning!("Failed to allocate DSM segment, falling back to sequential build");
            parallel::cleanup_parallel_context(pcxt, snapshot);
            return super::super::do_heap_scan_with_clustering(
                index_info,
                heap_relation,
                index_relation,
                meta_page,
                WriteStats::default(),
                None,
                0,
                &[],
                &[],
                centroids,
                num_clusters,
            );
        }

        let parallel_shared = pg_sys::shm_toc_allocate(
            (*pcxt).toc,
            std::mem::size_of::<ParallelShared>(),
        )
        .cast::<ParallelShared>();

        let heap_tuples = heap_relation.rd_rel.as_ref().unwrap().reltuples as usize;
        let shared_state = ParallelShared {
            params: ParallelSharedParams {
                heaprelid: heap_relation.rd_id,
                indexrelid: index_relation.rd_id,
                is_concurrent,
                num_clusters,
                total_vectors: heap_tuples,
                num_dimensions,
            },
            build_state: ParallelBuildState {
                producer_done: AtomicBool::new(false),
                producer_ntuples: AtomicUsize::new(0),
                consumers_finished: AtomicUsize::new(0),
                initialization_cv: std::mem::zeroed(),
            },
        };
        parallel_shared.write(shared_state);

        pg_sys::ConditionVariableInit(&raw mut (*parallel_shared).build_state.initialization_cv);

        // Allocate cluster queues structure + data (contiguous)
        let cluster_queues = pg_sys::shm_toc_allocate(
            (*pcxt).toc,
            cluster_queues_size,
        )
        .cast::<ClusterQueues>();
        
        // Initialize ClusterQueues in allocated memory
        let queues_template = ClusterQueues::new(num_clusters, DEFAULT_QUEUE_CAPACITY, num_dimensions);
        std::ptr::write(cluster_queues, queues_template);
        (*cluster_queues).initialize(cluster_queues as *mut u8);

        let centroids_ptr = pg_sys::shm_toc_allocate(
            (*pcxt).toc,
            centroids_size,
        ).cast::<u8>();
        write_centroids_to_shmem(centroids_ptr, centroids, num_dimensions);

        let cluster_start_nodes = pg_sys::shm_toc_allocate(
            (*pcxt).toc,
            start_nodes_size,
        )
        .cast::<ClusterStartNodes>();
        (*cluster_start_nodes) = ClusterStartNodes::new(num_clusters);

        pg_sys::shm_toc_insert(
            (*pcxt).toc,
            parallel::SHM_TOC_SHARED_KEY,
            parallel_shared.cast(),
        );
        pg_sys::shm_toc_insert(
            (*pcxt).toc,
            SHM_TOC_CLUSTER_QUEUES_KEY,
            cluster_queues.cast(),
        );
        pg_sys::shm_toc_insert(
            (*pcxt).toc,
            SHM_TOC_CENTROIDS_KEY,
            centroids_ptr.cast(),
        );
        pg_sys::shm_toc_insert(
            (*pcxt).toc,
            SHM_TOC_CLUSTER_START_NODES_KEY,
            cluster_start_nodes.cast(),
        );

        pg_sys::LaunchParallelWorkers(pcxt);

        if (*pcxt).nworkers_launched == 0 {
            warning!("No workers launched, falling back to sequential build");
            parallel::cleanup_parallel_context(pcxt, snapshot);
            return super::super::do_heap_scan_with_clustering(
                index_info,
                heap_relation,
                index_relation,
                meta_page,
                WriteStats::default(),
                None,
                0,
                &[],
                &[],
                centroids,
                num_clusters,
            );
        }

        let launched = (*pcxt).nworkers_launched as usize;
        notice!("Launched {} parallel workers", launched);

        pg_sys::WaitForParallelWorkersToAttach(pcxt);

        let mut producer_state = ProducerState {
            _parallel_shared: parallel_shared,
            cluster_queues,
            centroids: &centroids,
            ntuples: 0,
            meta_page: meta_page.clone(),
        };

        pg_sys::IndexBuildHeapScan(
            heaprel,
            indexrel,
            index_info,
            Some(producer_callback),
            &mut producer_state as *mut _ as *mut std::os::raw::c_void,
        );

        (*parallel_shared)
            .build_state
            .producer_done
            .store(true, Ordering::Release);

        let base_ptr = cluster_queues as *mut u8;
        let queues = &*cluster_queues;
        for i in 0..num_clusters {
            queues.mark_queue_finished(base_ptr, i);
        }

        (*parallel_shared)
            .build_state
            .producer_ntuples
            .store(producer_state.ntuples, Ordering::Relaxed);

        pg_sys::WaitForParallelWorkersToFinish(pcxt);

        let ntuples = (*parallel_shared)
            .build_state
            .producer_ntuples
            .load(Ordering::Relaxed);

        collect_cluster_start_nodes(cluster_start_nodes, meta_page, index_relation);

        // Record and report timing statistics
        let elapsed = start_time.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        notice!(
            "Parallel cluster build completed: {} vectors in {:.2}s ({:.0} vectors/sec)",
            ntuples,
            elapsed_secs,
            ntuples as f64 / elapsed_secs
        );
        notice!(
            "  - DSM setup: {} workers, {} clusters",
            num_clusters,
            num_clusters
        );
        notice!(
            "  - Queue capacity: {} entries per cluster",
            DEFAULT_QUEUE_CAPACITY
        );
        notice!(
            "  - Total shared memory: {} bytes",
            cluster_queues_size
        );

        parallel::cleanup_parallel_context(pcxt, snapshot);
        ntuples
    }
}

unsafe fn write_centroids_to_shmem(
    ptr: *mut u8,
    centroids: &[Vec<f32>],
    num_dimensions: usize,
) {
    let num_clusters = centroids.len();
    let num_clusters_ptr = ptr as *mut usize;
    std::ptr::write(num_clusters_ptr, num_clusters);

    let mut offset = std::mem::size_of::<usize>();
    for centroid in centroids {
        let dim_ptr = (ptr as *mut usize).add(offset / std::mem::size_of::<usize>());
        std::ptr::write(dim_ptr, num_dimensions);
        offset += std::mem::size_of::<usize>();

        let data_ptr = ptr.add(offset) as *mut f32;
        std::ptr::copy_nonoverlapping(centroid.as_ptr(), data_ptr, num_dimensions);
        offset += num_dimensions * std::mem::size_of::<f32>();
    }
}

unsafe fn read_centroids_from_shmem(
    ptr: *const u8,
) -> Vec<Vec<f32>> {
    let num_clusters_ptr = ptr as *const usize;
    let num_clusters = std::ptr::read(num_clusters_ptr);

    let mut centroids = Vec::with_capacity(num_clusters);
    let mut offset = std::mem::size_of::<usize>();

    for _ in 0..num_clusters {
        let dim_ptr = (ptr as *const usize).add(offset / std::mem::size_of::<usize>());
        let num_dimensions = std::ptr::read(dim_ptr);
        offset += std::mem::size_of::<usize>();

        let data_ptr = ptr.add(offset) as *const f32;
        let mut centroid = vec![0.0f32; num_dimensions];
        std::ptr::copy_nonoverlapping(data_ptr, centroid.as_mut_ptr(), num_dimensions);
        centroids.push(centroid);
        offset += num_dimensions * std::mem::size_of::<f32>();
    }

    centroids
}

unsafe fn collect_cluster_start_nodes(
    cluster_start_nodes: *const ClusterStartNodes,
    meta_page: &mut MetaPage,
    index_relation: &PgRelation,
) {
    let nodes = &*cluster_start_nodes;
    for i in 0..nodes.num_clusters {
        if let Some(start_node) = nodes.get_start_node(i) {
            let block_num = pgrx::itemptr::item_pointer_get_block_number_no_check(start_node);
            let offset_num = pgrx::itemptr::item_pointer_get_offset_number_no_check(start_node);
            let item_pointer = crate::util::ItemPointer::new(block_num, offset_num);
            meta_page.set_cluster_start_node(i as u32, item_pointer);
        }
    }
    meta_page.store(index_relation, false);
}

#[pg_guard]
unsafe extern "C-unwind" fn build_callback_collect_vectors(
    _index: pg_sys::Relation,
    ctid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    let collector_with_meta = (state as *mut VectorCollectorWithMeta).as_mut().unwrap();
    let vec = PgVector::from_pg_parts(values, isnull, 0, collector_with_meta.meta_page, true, false);
    if let Some(vec) = vec {
        collector_with_meta.collector.total_vectors_seen += 1;

        if collector_with_meta.collector.use_sampling {
            if collector_with_meta.collector.vectors.len() >= collector_with_meta.collector.max_sample_size {
                return;
            }

            if collector_with_meta.collector.sample_interval > 1 {
                if collector_with_meta.collector.total_vectors_seen % collector_with_meta.collector.sample_interval == 0 {
                    collector_with_meta.collector.vectors.push(vec.to_index_slice().to_vec());
                    collector_with_meta.collector.heap_tids.push(*ctid);
                }
            } else {
                collector_with_meta.collector.vectors.push(vec.to_index_slice().to_vec());
                collector_with_meta.collector.heap_tids.push(*ctid);
            }
        } else {
            collector_with_meta.collector.vectors.push(vec.to_index_slice().to_vec());
            collector_with_meta.collector.heap_tids.push(*ctid);
        }
    }
}

struct ProducerState<'a> {
    _parallel_shared: *mut ParallelShared,
    cluster_queues: *mut ClusterQueues,
    centroids: &'a [Vec<f32>],
    ntuples: usize,
    meta_page: MetaPage,
}

#[pg_guard]
unsafe extern "C-unwind" fn producer_callback(
    _index: pg_sys::Relation,
    ctid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    let producer_state = &mut *(state as *mut ProducerState);
    
    // Check if ctid is valid before dereferencing
    if ctid.is_null() {
        return;
    }
    
    let vec = PgVector::from_pg_parts(values, isnull, 0, &producer_state.meta_page, true, false);
    if let Some(vec) = vec {
        let vector_slice = vec.to_index_slice();
        let cluster_id = k_means::k_means_lookup(vector_slice, producer_state.centroids);
        
        let queues = &*producer_state.cluster_queues;
        let base_ptr = producer_state.cluster_queues as *mut u8;
        queues.push_to_queue(base_ptr, cluster_id, *ctid, vector_slice);
        
        producer_state.ntuples += 1;
    }
}

#[unsafe(no_mangle)]
#[cfg(feature = "build_parallel")]
pub extern "C" fn _vectorscale_build_cluster_consumer_main(
    _seg: *mut pg_sys::dsm_segment,
    shm_toc: *mut pg_sys::shm_toc,
) {
    // Simple null check first
    if shm_toc.is_null() {
        return;
    }
    
    let parallel_shared: *mut ParallelShared = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, parallel::SHM_TOC_SHARED_KEY, false)
            .cast::<ParallelShared>()
    };
    if parallel_shared.is_null() {
        return;
    }
    
    let cluster_queues: *mut ClusterQueues = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_CLUSTER_QUEUES_KEY, false)
            .cast::<ClusterQueues>()
    };
    if cluster_queues.is_null() {
        return;
    }
    
    let centroids_ptr: *const u8 = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_CENTROIDS_KEY, false)
            .cast::<u8>()
    };
    if centroids_ptr.is_null() {
        return;
    }
    
    let cluster_start_nodes: *mut ClusterStartNodes = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_CLUSTER_START_NODES_KEY, false)
            .cast::<ClusterStartNodes>()
    };
    if cluster_start_nodes.is_null() {
        return;
    }

    let params = unsafe { (*parallel_shared).params };
    let centroids = unsafe { read_centroids_from_shmem(centroids_ptr) };

    let worker_number = unsafe { pg_sys::ParallelWorkerNumber as usize };
    let cluster_id = worker_number;

    if cluster_id >= params.num_clusters {
        return;
    }

    notice!("Consumer worker {} starting for cluster {}", worker_number, cluster_id);
    
    // Debug: Check cluster_queues pointer and queue data
    unsafe {
        notice!("Consumer {}: cluster_queues ptr = {:?}", worker_number, cluster_queues);
        notice!("Consumer {}: num_queues = {}", worker_number, (*cluster_queues).num_queues);
        notice!("Consumer {}: entry_size = {}", worker_number, (*cluster_queues).entry_size);
        notice!("Consumer {}: queue_capacity = {}", worker_number, (*cluster_queues).queue_capacity);
        
        // Check queue header for this cluster
        let base_ptr = cluster_queues as *mut u8;
        let header = (*cluster_queues).get_header(base_ptr, cluster_id);
        notice!("Consumer {}: header head={}, tail={}, capacity={}", 
                worker_number, (*header).head.load(Ordering::Acquire), 
                (*header).tail.load(Ordering::Acquire), (*header).capacity);
    }

    let (heap_lockmode, index_lockmode) = if params.is_concurrent {
        (
            pg_sys::ShareLock as pg_sys::LOCKMODE,
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
        )
    } else {
        (
            pg_sys::ShareUpdateExclusiveLock as pg_sys::LOCKMODE,
            pg_sys::RowExclusiveLock as pg_sys::LOCKMODE,
        )
    };

    unsafe {
        let heaprel = pg_sys::table_open(params.heaprelid, heap_lockmode);
        let indexrel = pg_sys::index_open(params.indexrelid, index_lockmode);
        let heap_relation = PgRelation::from_pg(heaprel);
        let index_relation = PgRelation::from_pg(indexrel);
        let mut meta_page = MetaPage::fetch(&index_relation);

        let mut consumer_state = ConsumerState {
            cluster_id,
            cluster_queues,
            _parallel_shared: parallel_shared,
            _num_dimensions: params.num_dimensions,
            ntuples: 0,
            first_node: None,
        };

        build_cluster_subgraph(
            &mut consumer_state,
            &heap_relation,
            &index_relation,
            &mut meta_page,
            &centroids,
        );

        if let Some(first_node) = consumer_state.first_node {
            let mut item_pointer_data = pg_sys::ItemPointerData::default();
            first_node.to_item_pointer_data(&mut item_pointer_data);
            (*cluster_start_nodes).set_start_node(cluster_id, item_pointer_data);
        }

        (*parallel_shared)
            .build_state
            .consumers_finished
            .fetch_add(1, Ordering::Release);

        pg_sys::index_close(indexrel, index_lockmode);
        pg_sys::table_close(heaprel, heap_lockmode);
    }
}

struct ConsumerState {
    cluster_id: usize,
    cluster_queues: *mut ClusterQueues,
    _parallel_shared: *mut ParallelShared,
    _num_dimensions: usize,
    ntuples: usize,
    first_node: Option<crate::util::ItemPointer>,
}

unsafe fn build_cluster_subgraph(
    consumer_state: &mut ConsumerState,
    heap_relation: &PgRelation,
    index_relation: &PgRelation,
    meta_page: &mut MetaPage,
    _centroids: &[Vec<f32>],
) {
    let queues = &*consumer_state.cluster_queues;
    let base_ptr = consumer_state.cluster_queues as *mut u8;
    let cluster_id = consumer_state.cluster_id;

    let storage_type = meta_page.get_storage_type();
    const BUILDER_NEIGHBOR_CACHE_SIZE: f64 = 0.8;

    let mut graph = unsafe {
        Graph::new(
            GraphNeighborStore::Builder(BuilderNeighborCache::new(
                BUILDER_NEIGHBOR_CACHE_SIZE,
                meta_page,
                1,
            )),
            &mut *(meta_page as *mut _),
        )
    };

    let mut tape = unsafe { Tape::new(index_relation, PageType::Node) };
    let mut write_stats = WriteStats::default();
    let mut insert_stats = InsertStats::default();

    match storage_type {
        StorageType::Plain => {
            let mut plain = PlainStorage::new_for_build(
                index_relation,
                heap_relation,
                graph.get_meta_page(),
            );

            loop {
                if let Some((heap_tid, vector_data)) = queues.pop_from_queue(base_ptr, cluster_id) {
                    let heap_pointer = ItemPointer::new(
                        pgrx::itemptr::item_pointer_get_block_number_no_check(heap_tid),
                        pgrx::itemptr::item_pointer_get_offset_number_no_check(heap_tid),
                    );

                    let distance_type = meta_page.get_distance_type();
                    let vector_slice: Vec<f32> = match distance_type {
                        DistanceType::Cosine => {
                            let mut normalized = vector_data.to_vec();
                            distance::preprocess_cosine(&mut normalized);
                            normalized
                        }
                        _ => vector_data.to_vec(),
                    };

                    let index_pointer = plain.create_node(
                        &vector_slice,
                        None,
                        heap_pointer,
                        meta_page,
                        &mut tape,
                        &mut write_stats,
                    );

                    if consumer_state.first_node.is_none() {
                        consumer_state.first_node = Some(index_pointer);
                    }

                    consumer_state.ntuples += 1;
                } else if queues.is_queue_finished(base_ptr, cluster_id) {
                    break;
                } else {
                    queues.wait_on_cv(base_ptr, cluster_id);
                }
            }

            graph.maybe_flush_neighbor_cache(&mut plain, &mut insert_stats);
        }
        StorageType::SbqCompression => {
            let mut bq = unsafe {
                SbqSpeedupStorage::new_for_build(
                    index_relation,
                    heap_relation,
                    graph.get_meta_page(),
                    &mut write_stats,
                )
            };

            loop {
                if let Some((heap_tid, vector_data)) = queues.pop_from_queue(base_ptr, cluster_id) {
                    let heap_pointer = ItemPointer::new(
                        pgrx::itemptr::item_pointer_get_block_number_no_check(heap_tid),
                        pgrx::itemptr::item_pointer_get_offset_number_no_check(heap_tid),
                    );

                    let distance_type = meta_page.get_distance_type();
                    let vector_slice: Vec<f32> = match distance_type {
                        DistanceType::Cosine => {
                            let mut normalized = vector_data.to_vec();
                            distance::preprocess_cosine(&mut normalized);
                            normalized
                        }
                        _ => vector_data.to_vec(),
                    };

                    let index_pointer = bq.create_node(
                        &vector_slice,
                        None,
                        heap_pointer,
                        meta_page,
                        &mut tape,
                        &mut write_stats,
                    );

                    if consumer_state.first_node.is_none() {
                        consumer_state.first_node = Some(index_pointer);
                    }

                    consumer_state.ntuples += 1;
                } else if queues.is_queue_finished(base_ptr, cluster_id) {
                    break;
                } else {
                    queues.wait_on_cv(base_ptr, cluster_id);
                }
            }

            graph.maybe_flush_neighbor_cache(&mut bq, &mut insert_stats);
        }
    }
    
    notice!("Consumer for cluster {} processed {} vectors", cluster_id, consumer_state.ntuples);
}
