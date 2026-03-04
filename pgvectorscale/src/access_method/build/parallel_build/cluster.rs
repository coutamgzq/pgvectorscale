use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use pgrx::ffi::c_char;
use pgrx::pg_sys::{self, pgstat_progress_update_param};
use pgrx::*;
use rand::Rng;

use crate::access_method::distance;
use crate::access_method::distance::DistanceType;
use crate::access_method::graph::neighbor_store::{BuilderNeighborCache, GraphNeighborStore};
use crate::access_method::graph::Graph;
use crate::access_method::k_means;
use crate::access_method::labels::LabeledVector;
use crate::access_method::meta_page::MetaPage;
use crate::access_method::pg_vector::PgVector;
use crate::access_method::plain::storage::PlainStorage;
use crate::access_method::sbq::storage::SbqSpeedupStorage;
use crate::access_method::stats::{InsertStats, WriteStats};
use crate::access_method::storage::Storage;
use crate::access_method::storage::StorageType;
use crate::util::page::PageType;
use crate::util::ports::PROGRESS_CREATE_IDX_SUBPHASE;
use crate::util::tape::Tape;
use crate::util::ItemPointer;

use super::super::parallel::{
    self, ClusterQueues, ClusterSizes, ClusterStartNodes, WorkerAssignment, WorkerAssignments,
    DEFAULT_QUEUE_CAPACITY, MAX_WORKERS, SHM_TOC_CENTROIDS_KEY, SHM_TOC_CLUSTER_QUEUES_KEY,
    SHM_TOC_CLUSTER_SIZES_KEY, SHM_TOC_CLUSTER_START_NODES_KEY, SHM_TOC_WORKER_ASSIGNMENTS_KEY,
};
use super::super::{
    ParallelBuildState, ParallelShared, ParallelSharedParams, BUILD_PHASE_BUILDING_GRAPH,
    BUILD_PHASE_CLUSTERING, BUILD_PHASE_COLLECTING_VECTORS,
};

const PARALLEL_BUILD_CLUSTER_CONSUMER_MAIN: *const c_char =
    c"_vectorscale_build_cluster_consumer_main".as_ptr();

/// RAII guard to ensure ConditionVariableCancelSleep is called on drop.
/// This is critical to prevent crashes during PostgreSQL's process cleanup.
/// When a worker exits, shmem_exit() first releases DSM segments (including
/// ClusterQueues), then calls on_shmem_exit callbacks including CleanupProcSignalState.
/// If cv_sleep_target still points to a ConditionVariable in the released DSM,
/// ConditionVariableCancelSleep() will access invalid memory.
struct CvSleepGuard;

impl Drop for CvSleepGuard {
    fn drop(&mut self) {
        unsafe {
            pg_sys::ConditionVariableCancelSleep();
        }
    }
}

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
    let centroids = k_means::k_means(actual_num_clusters, vectors.clone(), false, 100, true);

    notice!(
        "K-means clustering completed with {} centroids",
        centroids.len()
    );

    let mut cluster_assignments = vec![0usize; vectors.len()];
    for (i, vector) in vectors.iter().enumerate() {
        cluster_assignments[i] = k_means::k_means_lookup(vector, &centroids);
    }

    let cluster_stats: Vec<_> = (0..actual_num_clusters)
        .map(|cluster_id| {
            let count = cluster_assignments
                .iter()
                .filter(|&&x| x == cluster_id)
                .count();
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
    sample_threshold: usize,
) -> VectorCollector {
    unsafe {
        pgstat_progress_update_param(PROGRESS_CREATE_IDX_SUBPHASE, BUILD_PHASE_COLLECTING_VECTORS);
    }

    // Determine sampling parameters upfront based on threshold
    // We can't know the total number of vectors in advance, so we use the threshold
    // to decide whether to use sampling. If sampling is needed, we'll adjust the
    // interval dynamically as we collect vectors.
    let use_sampling = max_sample_size > 0 && sample_threshold > 0;

    let collector = VectorCollector {
        vectors: Vec::new(),
        heap_tids: Vec::new(),
        max_sample_size,
        use_sampling,
        total_vectors_seen: 0,
        sample_interval: if use_sampling && sample_threshold > max_sample_size {
            (sample_threshold as f64 / max_sample_size as f64).ceil() as usize
        } else {
            1
        },
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
    let sample_threshold =
        crate::access_method::guc::TSV_CLUSTERING_SAMPLE_THRESHOLD.get() as usize;

    let collector = collect_vectors_for_clustering(
        heap_relation,
        index_relation,
        index_info,
        meta_page,
        max_sample_size,
        sample_threshold,
    );

    // Sampling is now applied during collection, so use the collected vectors directly
    let vectors_for_clustering = collector.vectors;
    let num_vectors = vectors_for_clustering.len();

    if num_vectors > 0 {
        notice!("Collected {} vectors for k-means clustering", num_vectors);
    }

    if vectors_for_clustering.len() < num_clusters {
        warning!(
            "Number of vectors ({}) is less than number of clusters ({}). Using {} clusters instead.",
            vectors_for_clustering.len(), num_clusters, vectors_for_clustering.len()
        );
    }

    let (centroids, _cluster_assignments) =
        perform_clustering(vectors_for_clustering, num_clusters);
    let actual_num_clusters = centroids.len();

    // Save centroids to meta page
    meta_page.set_centroids(centroids.clone());

    unsafe {
        pgstat_progress_update_param(PROGRESS_CREATE_IDX_SUBPHASE, BUILD_PHASE_BUILDING_GRAPH);
    }

    let write_stats =
        super::super::maybe_train_quantizer(index_info, heap_relation, index_relation, meta_page);
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
        workers,
        num_clusters
    );
    notice!(
        "Indexing {} vectors with {} dimensions",
        heap_tuples,
        num_dimensions
    );

    let start_time = std::time::Instant::now();

    // Phase 1: Sampling scan to determine cluster sizes
    // This allows us to properly size queues and assign workers
    let sample_rate = if heap_tuples > 100000 {
        0.1 // Sample 10% for large tables
    } else {
        1.0 // Full scan for small tables
    };

    let sampling_result = unsafe {
        sampling_scan(
            heaprel,
            indexrel,
            index_info,
            meta_page,
            centroids,
            sample_rate,
        )
    };

    // Calculate queue capacities based on cluster sizes
    let queue_capacities = calculate_queue_capacities(
        &sampling_result.cluster_stats,
        sampling_result.total_vectors,
    );

    // Calculate worker distribution
    let num_workers = workers.min(MAX_WORKERS).max(num_clusters);
    let worker_distribution =
        calculate_worker_distribution(&sampling_result.cluster_stats, num_workers);

    notice!("Queue capacities: {:?}", queue_capacities);
    notice!("Worker distribution: {:?}", worker_distribution);

    unsafe {
        pg_sys::EnterParallelMode();

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

        // Calculate total queue size with dynamic capacities
        let cluster_queues_size: usize = queue_capacities
            .iter()
            .map(|&cap| ClusterQueues::calculate_size(1, cap, num_dimensions))
            .sum();
        parallel::toc_estimate_single_chunk(pcxt, cluster_queues_size);

        let centroids_size = std::mem::size_of::<usize>()
            + num_clusters
                * (std::mem::size_of::<usize>() + num_dimensions * std::mem::size_of::<f32>());
        parallel::toc_estimate_single_chunk(pcxt, centroids_size);

        let start_nodes_size = std::mem::size_of::<ClusterStartNodes>();
        parallel::toc_estimate_single_chunk(pcxt, start_nodes_size);

        let cluster_sizes_size = std::mem::size_of::<ClusterSizes>();
        parallel::toc_estimate_single_chunk(pcxt, cluster_sizes_size);

        let worker_assignments_size = std::mem::size_of::<WorkerAssignments>();
        parallel::toc_estimate_single_chunk(pcxt, worker_assignments_size);

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

        let parallel_shared =
            pg_sys::shm_toc_allocate((*pcxt).toc, std::mem::size_of::<ParallelShared>())
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
                start_nodes_initialized: AtomicBool::new(false),
                initialization_cv: std::mem::zeroed(),
                assignments_ready: AtomicBool::new(false),
                assignments_cv: std::mem::zeroed(),
            },
            meta_page_ptr: std::ptr::null_mut(),
        };
        parallel_shared.write(shared_state);

        pg_sys::ConditionVariableInit(&raw mut (*parallel_shared).build_state.initialization_cv);
        pg_sys::ConditionVariableInit(&raw mut (*parallel_shared).build_state.assignments_cv);

        let cluster_queues =
            pg_sys::shm_toc_allocate((*pcxt).toc, cluster_queues_size).cast::<ClusterQueues>();

        // Initialize ClusterQueues in allocated memory with dynamic capacities
        let max_capacity = queue_capacities
            .iter()
            .copied()
            .max()
            .unwrap_or(DEFAULT_QUEUE_CAPACITY);
        let queues_template = ClusterQueues::new(num_clusters, max_capacity, num_dimensions);
        std::ptr::write(cluster_queues, queues_template);
        (*cluster_queues).initialize_with_capacities(cluster_queues as *mut u8, &queue_capacities);

        let centroids_ptr = pg_sys::shm_toc_allocate((*pcxt).toc, centroids_size).cast::<u8>();
        write_centroids_to_shmem(centroids_ptr, centroids, num_dimensions);

        let cluster_start_nodes =
            pg_sys::shm_toc_allocate((*pcxt).toc, start_nodes_size).cast::<ClusterStartNodes>();
        (*cluster_start_nodes) = ClusterStartNodes::new(num_clusters);

        let cluster_sizes =
            pg_sys::shm_toc_allocate((*pcxt).toc, cluster_sizes_size).cast::<ClusterSizes>();
        std::ptr::write(cluster_sizes, ClusterSizes::new(num_clusters));

        let worker_assignments = pg_sys::shm_toc_allocate((*pcxt).toc, worker_assignments_size)
            .cast::<WorkerAssignments>();
        std::ptr::write(worker_assignments, WorkerAssignments::new());

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
        pg_sys::shm_toc_insert((*pcxt).toc, SHM_TOC_CENTROIDS_KEY, centroids_ptr.cast());
        pg_sys::shm_toc_insert(
            (*pcxt).toc,
            SHM_TOC_CLUSTER_START_NODES_KEY,
            cluster_start_nodes.cast(),
        );
        pg_sys::shm_toc_insert((*pcxt).toc, SHM_TOC_CLUSTER_SIZES_KEY, cluster_sizes.cast());
        pg_sys::shm_toc_insert(
            (*pcxt).toc,
            SHM_TOC_WORKER_ASSIGNMENTS_KEY,
            worker_assignments.cast(),
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
            cluster_sizes,
            centroids: &centroids,
            ntuples: 0,
            meta_page: meta_page.clone(),
            batch_buffer: BatchBuffer::new(),
        };

        pg_sys::IndexBuildHeapScan(
            heaprel,
            indexrel,
            index_info,
            Some(producer_callback),
            &mut producer_state as *mut _ as *mut std::os::raw::c_void,
        );

        producer_state.flush_batch();

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

        let final_cluster_sizes = (*cluster_sizes).get_all();
        notice!("Cluster sizes after scan: {:?}", final_cluster_sizes);

        // Use pre-calculated worker distribution from sampling phase
        notice!("Worker assignments based on sampling:");
        for (cluster_id, workers) in worker_distribution.iter().enumerate() {
            if !workers.is_empty() {
                notice!("  Cluster {}: workers {:?}", cluster_id, workers);
            }
        }

        // Calculate ranges for each worker based on actual cluster sizes
        for (cluster_id, workers) in worker_distribution.iter().enumerate() {
            let cluster_size = final_cluster_sizes.get(cluster_id).copied().unwrap_or(0);
            if cluster_size == 0 || workers.is_empty() {
                continue;
            }

            let workers_for_cluster = workers.len();
            let chunk_size = (cluster_size + workers_for_cluster - 1) / workers_for_cluster;

            for (i, &worker_id) in workers.iter().enumerate() {
                let start_idx = i * chunk_size;
                let end_idx = ((i + 1) * chunk_size).min(cluster_size);
                let is_primary = i == 0;

                let assignment =
                    parallel::WorkerAssignment::new(cluster_id, start_idx, end_idx, is_primary);
                (*worker_assignments).set_assignment(worker_id, assignment);

                notice!(
                    "  Worker {}: cluster {} [{}..{}], primary={}",
                    worker_id,
                    cluster_id,
                    start_idx,
                    end_idx,
                    is_primary
                );
            }
        }

        (*worker_assignments).mark_ready();
        (*parallel_shared)
            .build_state
            .assignments_ready
            .store(true, Ordering::Release);
        pg_sys::ConditionVariableBroadcast(
            &raw mut (*parallel_shared).build_state.assignments_cv as *const _ as *mut _,
        );

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
        notice!("  - Total shared memory: {} bytes", cluster_queues_size);

        parallel::cleanup_parallel_context(pcxt, snapshot);
        ntuples
    }
}

unsafe fn write_centroids_to_shmem(ptr: *mut u8, centroids: &[Vec<f32>], num_dimensions: usize) {
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

unsafe fn read_centroids_from_shmem(ptr: *const u8) -> Vec<Vec<f32>> {
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
    // Check if the vector is NULL, skip if so
    if *isnull {
        return;
    }

    let collector_with_meta = (state as *mut VectorCollectorWithMeta).as_mut().unwrap();
    let vec = PgVector::from_pg_parts(
        values,
        isnull,
        0,
        collector_with_meta.meta_page,
        true,
        false,
    );
    if let Some(vec) = vec {
        collector_with_meta.collector.total_vectors_seen += 1;

        if collector_with_meta.collector.use_sampling {
            if collector_with_meta.collector.vectors.len()
                >= collector_with_meta.collector.max_sample_size
            {
                return;
            }

            if collector_with_meta.collector.sample_interval > 1 {
                if collector_with_meta.collector.total_vectors_seen
                    % collector_with_meta.collector.sample_interval
                    == 0
                {
                    collector_with_meta
                        .collector
                        .vectors
                        .push(vec.to_index_slice().to_vec());
                    collector_with_meta.collector.heap_tids.push(*ctid);
                }
            } else {
                collector_with_meta
                    .collector
                    .vectors
                    .push(vec.to_index_slice().to_vec());
                collector_with_meta.collector.heap_tids.push(*ctid);
            }
        } else {
            collector_with_meta
                .collector
                .vectors
                .push(vec.to_index_slice().to_vec());
            collector_with_meta.collector.heap_tids.push(*ctid);
        }
    }
}

const BATCH_SIZE: usize = 64;

/// Statistics for a single cluster
#[derive(Debug, Clone)]
struct ClusterStats {
    count: usize,
    sample_vectors: Vec<Vec<f32>>,
}

impl ClusterStats {
    fn new() -> Self {
        Self {
            count: 0,
            sample_vectors: Vec::new(),
        }
    }

    fn add_vector(&mut self, vector: &[f32], max_samples: usize) {
        self.count += 1;
        if self.sample_vectors.len() < max_samples {
            self.sample_vectors.push(vector.to_vec());
        }
    }
}

/// Result of the sampling scan phase
struct SamplingResult {
    cluster_stats: Vec<ClusterStats>,
    total_vectors: usize,
}

/// Quick sampling scan to determine cluster sizes
/// This runs before allocating shared memory to properly size the queues
unsafe fn sampling_scan(
    heaprel: pg_sys::Relation,
    indexrel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    meta_page: &MetaPage,
    centroids: &[Vec<f32>],
    sample_rate: f64, // 0.0 - 1.0, fraction of table to sample
) -> SamplingResult {
    notice!("Starting sampling scan with rate {:.2}", sample_rate);

    let num_clusters = centroids.len();
    let mut cluster_stats: Vec<ClusterStats> =
        (0..num_clusters).map(|_| ClusterStats::new()).collect();
    let mut total_vectors = 0usize;

    struct SamplingState<'a> {
        centroids: &'a [Vec<f32>],
        cluster_stats: &'a mut [ClusterStats],
        total_vectors: &'a mut usize,
        meta_page: MetaPage,
        sample_rate: f64,
        rng: rand::rngs::SmallRng,
    }

    #[pg_guard]
    unsafe extern "C-unwind" fn sampling_callback(
        _index: pg_sys::Relation,
        ctid: pg_sys::ItemPointer,
        values: *mut pg_sys::Datum,
        isnull: *mut bool,
        _tuple_is_alive: bool,
        state: *mut std::os::raw::c_void,
    ) {
        let state = &mut *(state as *mut SamplingState);

        if ctid.is_null() {
            return;
        }

        // Sample based on rate
        if state.rng.gen::<f64>() > state.sample_rate {
            return;
        }

        let vec = PgVector::from_pg_parts(values, isnull, 0, &state.meta_page, true, false);
        if let Some(vec) = vec {
            let vector_slice = vec.to_index_slice();
            let cluster_id = k_means::k_means_lookup(vector_slice, state.centroids);

            if cluster_id < state.cluster_stats.len() {
                state.cluster_stats[cluster_id].add_vector(vector_slice, 100);
            }
            *state.total_vectors += 1;
        }
    }

    let mut state = SamplingState {
        centroids,
        cluster_stats: &mut cluster_stats,
        total_vectors: &mut total_vectors,
        meta_page: meta_page.clone(),
        sample_rate,
        rng: rand::SeedableRng::seed_from_u64(42),
    };

    pg_sys::IndexBuildHeapScan(
        heaprel,
        indexrel,
        index_info,
        Some(sampling_callback),
        &mut state as *mut _ as *mut std::os::raw::c_void,
    );

    // Scale up counts based on sample rate
    if sample_rate < 1.0 && sample_rate > 0.0 {
        let scale_factor = 1.0 / sample_rate;
        for stats in &mut cluster_stats {
            stats.count = (stats.count as f64 * scale_factor) as usize;
        }
        total_vectors = (total_vectors as f64 * scale_factor) as usize;
    }

    notice!(
        "Sampling scan complete: {} vectors estimated",
        total_vectors
    );
    for (i, stats) in cluster_stats.iter().enumerate() {
        notice!("  Cluster {}: ~{} vectors", i, stats.count);
    }

    SamplingResult {
        cluster_stats,
        total_vectors,
    }
}

/// Calculate queue capacity for each cluster based on its size
fn calculate_queue_capacities(cluster_stats: &[ClusterStats], total_vectors: usize) -> Vec<usize> {
    let base_capacity = DEFAULT_QUEUE_CAPACITY;
    let max_capacity = DEFAULT_QUEUE_CAPACITY * 10;

    cluster_stats
        .iter()
        .map(|stats| {
            if total_vectors == 0 {
                return base_capacity;
            }
            // Proportional to cluster size, with minimum and maximum
            let ratio = stats.count as f64 / total_vectors as f64;
            let capacity = (base_capacity as f64 * (1.0 + ratio * 5.0)) as usize;
            capacity.clamp(base_capacity, max_capacity)
        })
        .collect()
}

/// Calculate worker assignments based on cluster sizes
/// Ensures each cluster gets at least 1 worker if possible
fn calculate_worker_distribution(
    cluster_stats: &[ClusterStats],
    total_workers: usize,
) -> Vec<Vec<usize>> {
    let num_clusters = cluster_stats.len();
    let mut assignments: Vec<Vec<usize>> = cluster_stats.iter().map(|_| Vec::new()).collect();

    // First pass: ensure each cluster gets at least 1 worker
    let mut worker_id = 0;
    for cluster_id in 0..num_clusters {
        if worker_id < total_workers {
            assignments[cluster_id].push(worker_id);
            worker_id += 1;
        }
    }

    // If we don't have enough workers for all clusters, return what we have
    if worker_id >= total_workers {
        return assignments;
    }

    // Second pass: assign remaining workers proportionally
    let total_vectors: usize = cluster_stats.iter().map(|s| s.count).sum();
    if total_vectors > 0 {
        // Calculate remaining workers to distribute
        let remaining_workers = total_workers - worker_id;

        // Sort clusters by size (descending) to assign more workers to larger clusters
        let mut cluster_sizes: Vec<(usize, usize)> = cluster_stats
            .iter()
            .enumerate()
            .map(|(id, stats)| (id, stats.count))
            .collect();
        cluster_sizes.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by size descending

        // Distribute remaining workers proportionally
        for (cluster_id, count) in cluster_sizes {
            if worker_id >= total_workers {
                break;
            }

            let ratio = count as f64 / total_vectors as f64;
            let extra_workers = (remaining_workers as f64 * ratio).ceil() as usize;
            let extra_workers = extra_workers.max(1);

            for _ in 0..extra_workers {
                if worker_id >= total_workers {
                    break;
                }
                assignments[cluster_id].push(worker_id);
                worker_id += 1;
            }
        }
    }

    // Assign any remaining workers to the largest cluster
    let largest_cluster = cluster_stats
        .iter()
        .enumerate()
        .max_by_key(|(_, s)| s.count)
        .map(|(i, _)| i)
        .unwrap_or(0);

    while worker_id < total_workers {
        assignments[largest_cluster].push(worker_id);
        worker_id += 1;
    }

    assignments
}

struct BatchBuffer {
    cluster_id: usize,
    entries: Vec<(pg_sys::ItemPointerData, Vec<f32>)>,
}

impl BatchBuffer {
    fn new() -> Self {
        Self {
            cluster_id: 0,
            entries: Vec::with_capacity(BATCH_SIZE),
        }
    }

    fn is_full(&self) -> bool {
        self.entries.len() >= BATCH_SIZE
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

struct ProducerState<'a> {
    _parallel_shared: *mut ParallelShared,
    cluster_queues: *mut ClusterQueues,
    cluster_sizes: *mut ClusterSizes,
    centroids: &'a [Vec<f32>],
    ntuples: usize,
    meta_page: MetaPage,
    batch_buffer: BatchBuffer,
}

impl ProducerState<'_> {
    unsafe fn flush_batch(&mut self) {
        if self.batch_buffer.entries.is_empty() {
            return;
        }

        if self.cluster_queues.is_null() {
            warning!("ProducerState::flush_batch: cluster_queues is null");
            return;
        }

        let queues = &*self.cluster_queues;
        let base_ptr = self.cluster_queues as *mut u8;
        let cluster_id = self.batch_buffer.cluster_id;

        let entries: Vec<(pg_sys::ItemPointerData, &[f32])> = self
            .batch_buffer
            .entries
            .iter()
            .map(|(tid, vec)| (*tid, vec.as_slice()))
            .collect();

        let pushed = queues.push_batch_to_queue(base_ptr, cluster_id, &entries);
        self.ntuples += pushed;
        self.batch_buffer.clear();
    }

    unsafe fn push_with_batch(
        &mut self,
        cluster_id: usize,
        heap_tid: pg_sys::ItemPointerData,
        vector: &[f32],
    ) {
        if self.batch_buffer.cluster_id != cluster_id || self.batch_buffer.is_full() {
            self.flush_batch();
            self.batch_buffer.cluster_id = cluster_id;
        }

        self.batch_buffer.entries.push((heap_tid, vector.to_vec()));

        if !self.cluster_sizes.is_null() {
            (*self.cluster_sizes).increment(cluster_id);
        }
    }
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

    if ctid.is_null() {
        return;
    }

    let vec = PgVector::from_pg_parts(values, isnull, 0, &producer_state.meta_page, true, false);
    if let Some(vec) = vec {
        let vector_slice = vec.to_index_slice();
        let cluster_id = k_means::k_means_lookup(vector_slice, producer_state.centroids);

        producer_state.push_with_batch(cluster_id, *ctid, vector_slice);
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
        pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_CLUSTER_QUEUES_KEY, false).cast::<ClusterQueues>()
    };
    if cluster_queues.is_null() {
        return;
    }

    let centroids_ptr: *const u8 =
        unsafe { pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_CENTROIDS_KEY, false).cast::<u8>() };
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

    let worker_assignments: *mut WorkerAssignments = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_WORKER_ASSIGNMENTS_KEY, false)
            .cast::<WorkerAssignments>()
    };

    let params = unsafe { (*parallel_shared).params };
    let centroids = unsafe { read_centroids_from_shmem(centroids_ptr) };

    let worker_number = unsafe { pg_sys::ParallelWorkerNumber as usize };

    // Wait for assignments to be ready
    if !worker_assignments.is_null() && !parallel_shared.is_null() {
        unsafe {
            let build_state = &(*parallel_shared).build_state;
            let assignments_cv =
                &raw const build_state.assignments_cv as *mut pg_sys::ConditionVariable;

            // Wait until assignments are ready
            while !(*worker_assignments).is_ready() {
                pg_sys::ConditionVariablePrepareToSleep(assignments_cv);
                if (*worker_assignments).is_ready() {
                    pg_sys::ConditionVariableCancelSleep();
                    break;
                }
                pg_sys::ConditionVariableSleep(assignments_cv, pg_sys::PG_WAIT_EXTENSION);
            }
        }
    }

    // Get assignment after waiting
    let (cluster_id, start_idx, end_idx, is_primary) = if worker_assignments.is_null() {
        let cluster_id = worker_number;
        if cluster_id >= params.num_clusters {
            return;
        }
        (cluster_id, 0, usize::MAX, true)
    } else {
        let assignment = unsafe { (*worker_assignments).get_assignment(worker_number) };
        match assignment {
            Some(a) => (a.cluster_id, a.start_idx, a.end_idx, a.is_primary),
            None => {
                // Worker has no assignment - exit
                notice!(
                    "Worker {} has no assignment after waiting, exiting",
                    worker_number
                );
                return;
            }
        }
    };

    notice!(
        "Consumer worker {} starting for cluster {} [{}..{}], primary={}",
        worker_number,
        cluster_id,
        start_idx,
        end_idx,
        is_primary
    );

    // Debug: Check cluster_queues pointer and queue data
    unsafe {
        notice!(
            "Consumer {}: cluster_queues ptr = {:?}",
            worker_number,
            cluster_queues
        );
        notice!(
            "Consumer {}: num_queues = {}",
            worker_number,
            (*cluster_queues).num_queues
        );
        notice!(
            "Consumer {}: entry_size = {}",
            worker_number,
            (*cluster_queues).entry_size
        );
        notice!(
            "Consumer {}: queue_capacity = {}",
            worker_number,
            (*cluster_queues).queue_capacity
        );

        // Check queue header for this cluster
        let base_ptr = cluster_queues as *mut u8;
        let header = (*cluster_queues).get_header(base_ptr, cluster_id);
        notice!(
            "Consumer {}: header head={}, tail={}, capacity={}",
            worker_number,
            (*header).head.load(Ordering::Acquire),
            (*header).tail.load(Ordering::Acquire),
            (*header).capacity
        );
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
        // Create the guard to ensure ConditionVariableCancelSleep is called on exit.
        // This must be created before any code that might call ConditionVariableSleep.
        let _cv_guard = CvSleepGuard;

        let heaprel = pg_sys::table_open(params.heaprelid, heap_lockmode);
        let indexrel = pg_sys::index_open(params.indexrelid, index_lockmode);
        let heap_relation = PgRelation::from_pg(heaprel);
        let index_relation = PgRelation::from_pg(indexrel);
        // Each worker loads its own MetaPage copy (read-only)
        // We don't share MetaPage because it contains heap-allocated fields (BTreeMap, Vec)
        let mut meta_page = MetaPage::fetch(&index_relation);

        let mut consumer_state = ConsumerState {
            cluster_id,
            start_idx,
            end_idx,
            is_primary,
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

        if is_primary {
            if let Some(first_node) = consumer_state.first_node {
                let mut item_pointer_data = pg_sys::ItemPointerData::default();
                first_node.to_item_pointer_data(&mut item_pointer_data);
                (*cluster_start_nodes).set_start_node(cluster_id, item_pointer_data);
            }
        }

        (*parallel_shared)
            .build_state
            .consumers_finished
            .fetch_add(1, Ordering::Release);

        pg_sys::index_close(indexrel, index_lockmode);
        pg_sys::table_close(heaprel, heap_lockmode);

        // CvSleepGuard will automatically call ConditionVariableCancelSleep() when it goes out of scope.
        // This ensures cv_sleep_target is cleared even if a panic occurs.
    }
}

struct ConsumerState {
    cluster_id: usize,
    start_idx: usize,
    end_idx: usize,
    is_primary: bool,
    cluster_queues: *mut ClusterQueues,
    _parallel_shared: *mut ParallelShared,
    _num_dimensions: usize,
    ntuples: usize,
    first_node: Option<crate::util::ItemPointer>,
}

/// Process vectors from queue and build graph for a single cluster.
/// This is a generic function that works with any Storage implementation.
unsafe fn process_cluster_vectors<S: Storage>(
    consumer_state: &mut ConsumerState,
    index_relation: &PgRelation,
    meta_page: &mut MetaPage,
    queues: &ClusterQueues,
    base_ptr: *mut u8,
    cluster_id: usize,
    flush_interval: usize,
    storage: &mut S,
    graph: &mut Graph,
    tape: &mut Tape,
    write_stats: &mut WriteStats,
) {
    let mut insert_stats = InsertStats::default();
    let mut queue_idx: usize = 0;
    let start_idx = consumer_state.start_idx;
    let end_idx = consumer_state.end_idx;
    let num_dimensions = meta_page.get_num_dimensions_to_index() as usize;

    let mut batch_heap_tids: Vec<pg_sys::ItemPointerData> = vec![std::mem::zeroed(); BATCH_SIZE];
    let mut batch_vectors: Vec<Vec<f32>> = (0..BATCH_SIZE)
        .map(|_| vec![0.0f32; num_dimensions])
        .collect();

    loop {
        let available = queues.queue_size(base_ptr, cluster_id);
        let batch_count = available.min(BATCH_SIZE);

        if batch_count > 0 {
            let popped = queues.pop_batch_from_queue(
                base_ptr,
                cluster_id,
                batch_count,
                &mut batch_heap_tids,
                &mut batch_vectors,
            );

            for i in 0..popped {
                if queue_idx < start_idx {
                    queue_idx += 1;
                    continue;
                }
                if queue_idx >= end_idx {
                    break;
                }
                queue_idx += 1;

                let heap_tid = batch_heap_tids[i];
                let vector_data = &batch_vectors[i];

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

                let index_pointer = storage.create_node(
                    &vector_slice,
                    None,
                    heap_pointer,
                    meta_page,
                    tape,
                    write_stats,
                );

                let labeled_vector = LabeledVector::new(PgVector::from_slice(vector_data), None);
                graph.insert(
                    index_relation,
                    index_pointer,
                    labeled_vector,
                    storage,
                    &mut insert_stats,
                );

                if consumer_state.first_node.is_none() {
                    consumer_state.first_node = Some(index_pointer);
                }

                consumer_state.ntuples += 1;

                if consumer_state.ntuples % flush_interval == 0 {
                    graph.maybe_flush_neighbor_cache(storage, &mut insert_stats);
                }
            }
        } else if queues.is_queue_finished(base_ptr, cluster_id) {
            break;
        } else {
            queues.wait_on_cv(base_ptr, cluster_id);
        }
    }

    graph.maybe_flush_neighbor_cache(storage, &mut insert_stats);
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

    // Get total vectors from parallel shared state for flush interval calculation
    let total_vectors = unsafe { (*consumer_state._parallel_shared).params.total_vectors };
    let flush_interval = parallel::flush_rate(total_vectors);

    match storage_type {
        StorageType::Plain => {
            let mut plain =
                PlainStorage::new_for_build(index_relation, heap_relation, graph.get_meta_page());

            process_cluster_vectors(
                consumer_state,
                index_relation,
                meta_page,
                queues,
                base_ptr,
                cluster_id,
                flush_interval,
                &mut plain,
                &mut graph,
                &mut tape,
                &mut write_stats,
            );
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

            process_cluster_vectors(
                consumer_state,
                index_relation,
                meta_page,
                queues,
                base_ptr,
                cluster_id,
                flush_interval,
                &mut bq,
                &mut graph,
                &mut tape,
                &mut write_stats,
            );
        }
    }

    notice!(
        "Consumer for cluster {} processed {} vectors",
        cluster_id,
        consumer_state.ntuples
    );
}
