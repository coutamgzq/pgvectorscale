use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use pgrx::ffi::c_char;
use pgrx::pg_sys::{pgstat_progress_update_param};
use pgrx::*;

use crate::access_method::k_means;
use crate::access_method::meta_page::MetaPage;
use crate::access_method::pg_vector::PgVector;
use crate::access_method::stats::WriteStats;
use crate::access_method::storage::StorageType;
use crate::util::ports::PROGRESS_CREATE_IDX_SUBPHASE;

use super::super::{
    ParallelShared, ParallelSharedParams, ParallelBuildState, ParallelBuildInfo,
    BUILD_PHASE_COLLECTING_VECTORS, BUILD_PHASE_CLUSTERING, BUILD_PHASE_BUILDING_GRAPH,
};
use super::super::parallel;

const PARALLEL_BUILD_CLUSTER_MAIN: *const c_char = c"_vectorscale_build_cluster_main".as_ptr();

/// Structure to collect vectors for k-means clustering
pub struct VectorCollector {
    pub vectors: Vec<Vec<f32>>,
    pub heap_tids: Vec<pg_sys::ItemPointerData>,
    pub max_sample_size: usize,
    pub use_sampling: bool,
    pub total_vectors_seen: usize,
    pub sample_interval: usize,
}

/// Structure to collect vectors for k-means clustering with meta_page
pub struct VectorCollectorWithMeta<'a> {
    pub collector: VectorCollector,
    pub meta_page: &'a MetaPage,
}

/// Cluster data for parallel builds
#[derive(Debug)]
#[cfg_attr(not(feature = "build_parallel"), allow(dead_code))]
pub struct ClusterParallelData {
    pub pcxt: *mut pg_sys::ParallelContext,
    pub snapshot: *mut pg_sys::SnapshotData,
    pub centroids: Vec<Vec<f32>>,
    pub cluster_assignments: Vec<usize>,
}



/// Performs k-means clustering on collected vectors and returns centroids and cluster assignments
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

/// Collects vectors from heap for clustering with optional sampling
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

/// Samples vectors if needed based on configuration
/// Returns (sampled_vectors, sampled_heap_tids)
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

/// Main entry point for cluster-based index build
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

    // Collect vectors
    let collector = collect_vectors_for_clustering(
        heap_relation,
        index_relation,
        index_info,
        meta_page,
        max_sample_size,
        sample_threshold,
    );

    // Sample vectors if needed
    let (vectors_for_clustering, heap_tids_for_clustering) = sample_vectors_if_needed(&collector, max_sample_size, sample_threshold);

    if vectors_for_clustering.len() < num_clusters {
        warning!(
            "Number of vectors ({}) is less than number of clusters ({}). Using {} clusters instead.",
            vectors_for_clustering.len(), num_clusters, vectors_for_clustering.len()
        );
    }

    // Perform clustering
    let (centroids, cluster_assignments) = perform_clustering(vectors_for_clustering, num_clusters);
    let actual_num_clusters = centroids.len();

    unsafe {
        pgstat_progress_update_param(PROGRESS_CREATE_IDX_SUBPHASE, BUILD_PHASE_BUILDING_GRAPH);
    }

    // Train quantizer if needed
    let write_stats = super::super::maybe_train_quantizer(index_info, heap_relation, index_relation, meta_page);
    unsafe {
        meta_page.store(index_relation, false);
    };

    let heap_tuples = unsafe { heap_relation.rd_rel.as_ref().unwrap().reltuples as usize };

    // Determine number of workers
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

    // Perform parallel or sequential build
    let ntuples = if workers > 0 {
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
            &cluster_assignments,
            write_stats,
        )
    } else {
        super::super::do_heap_scan_with_clustering(
            index_info,
            heap_relation,
            index_relation,
            meta_page,
            write_stats,
            None,
            workers,
            &heap_tids_for_clustering,
            &cluster_assignments,
            &centroids,
            actual_num_clusters,
        )
    };

    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = ntuples as f64;
    result.index_tuples = ntuples as f64;

    result.into_pg()
}

/// Performs parallel cluster build
fn do_parallel_cluster_build(
    heaprel: pg_sys::Relation,
    _indexrel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    heap_relation: &PgRelation,
    index_relation: &PgRelation,
    meta_page: &mut MetaPage,
    workers: usize,
    is_concurrent: bool,
    centroids: &[Vec<f32>],
    cluster_assignments: &[usize],
    write_stats: WriteStats,
) -> usize {
    notice!("Parallel build with {} workers for {} clusters", workers, centroids.len());
    
    unsafe {
        pg_sys::EnterParallelMode();

        let pcxt = pg_sys::CreateParallelContext(
            crate::EXTENSION_NAME,
            PARALLEL_BUILD_CLUSTER_MAIN,
            workers as i32,
        );
        let snapshot = if is_concurrent {
            pg_sys::RegisterSnapshot(pg_sys::GetTransactionSnapshot())
        } else {
            &raw mut pg_sys::SnapshotAnyData
        };

        parallel::toc_estimate_single_chunk(pcxt, std::mem::size_of::<ParallelShared>());
        parallel::toc_estimate_single_chunk(pcxt, std::mem::size_of::<ClusterParallelData>());
        let tablescandesc_size_estimate =
            pg_sys::table_parallelscan_estimate(heaprel, snapshot);
        parallel::toc_estimate_single_chunk(pcxt, tablescandesc_size_estimate);

        pg_sys::InitializeParallelDSM(pcxt);
        if (*pcxt).seg.is_null() {
            parallel::cleanup_parallel_context(pcxt, snapshot);
            return super::super::do_heap_scan_with_clustering(
                index_info,
                heap_relation,
                index_relation,
                meta_page,
                write_stats,
                None,
                workers,
                &[],
                cluster_assignments,
                centroids,
                centroids.len(),
            );
        }

        let parallel_shared =
            pg_sys::shm_toc_allocate((*pcxt).toc, std::mem::size_of::<ParallelShared>())
                .cast::<ParallelShared>();
        let shared_state = ParallelShared {
            params: ParallelSharedParams {
                heaprelid: heap_relation.rd_id,
                indexrelid: index_relation.rd_id,
                is_concurrent,
                worker_count: workers as usize,
                total_vectors: heap_relation.rd_rel.as_ref().unwrap().reltuples as usize,
            },
            build_state: ParallelBuildState {
                ntuples: AtomicUsize::new(0),
                start_nodes_initialized: AtomicBool::new(false),
                initializing_worker_done: AtomicBool::new(false),
                initialization_cv: std::mem::zeroed(),
            },
        };
        parallel_shared.write(shared_state);

        pg_sys::ConditionVariableInit(
            &raw mut (*parallel_shared).build_state.initialization_cv,
        );

        let cluster_data =
            pg_sys::shm_toc_allocate((*pcxt).toc, std::mem::size_of::<ClusterParallelData>())
                .cast::<ClusterParallelData>();

        let cluster_parallel_data = ClusterParallelData {
            pcxt: std::ptr::null_mut(),
            snapshot: std::ptr::null_mut(),
            centroids: centroids.to_vec(),
            cluster_assignments: cluster_assignments.to_vec(),
        };

        cluster_data.write(cluster_parallel_data);

        let tablescandesc =
            pg_sys::shm_toc_allocate((*pcxt).toc, tablescandesc_size_estimate)
                .cast::<pg_sys::ParallelTableScanDescData>();
        pg_sys::table_parallelscan_initialize(heaprel, tablescandesc, snapshot);

        pg_sys::shm_toc_insert(
            (*pcxt).toc,
            parallel::SHM_TOC_SHARED_KEY,
            parallel_shared.cast(),
        );
        pg_sys::shm_toc_insert(
            (*pcxt).toc,
            parallel::SHM_TOC_CLUSTER_DATA_KEY,
            cluster_data.cast(),
        );
        pg_sys::shm_toc_insert(
            (*pcxt).toc,
            parallel::SHM_TOC_TABLESCANDESC_KEY,
            tablescandesc.cast(),
        );

        pg_sys::LaunchParallelWorkers(pcxt);
        if (*pcxt).nworkers_launched == 0 {
            warning!("No workers launched");
            parallel::cleanup_parallel_context(pcxt, snapshot);
            return super::super::do_heap_scan_with_clustering(
                index_info,
                heap_relation,
                index_relation,
                meta_page,
                write_stats,
                None,
                workers,
                &[],
                cluster_assignments,
                centroids,
                centroids.len(),
            );
        }

        pg_sys::WaitForParallelWorkersToAttach(pcxt);
        pg_sys::WaitForParallelWorkersToFinish(pcxt);
        
        let ntuples = (*parallel_shared)
            .build_state
            .ntuples
            .load(Ordering::Relaxed);
        parallel::cleanup_parallel_context(pcxt, snapshot);
        ntuples
    }
}

/// Callback function for collecting vectors during heap scan
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

/// Parallel worker entry point for cluster build
#[pg_guard]
#[unsafe(no_mangle)]
#[cfg(feature = "build_parallel")]
pub extern "C-unwind" fn _vectorscale_build_cluster_main(
    _seg: *mut pg_sys::dsm_segment,
    shm_toc: *mut pg_sys::shm_toc,
) {
    let status_flags = unsafe { (*pg_sys::MyProc).statusFlags };
    assert!(
        status_flags == 0 || status_flags == pg_sys::PROC_IN_SAFE_IC as u8,
        "Status flags for an index build process must be unset or PROC_IN_SAFE_IC (in a safe index creation)"
    );

    let parallel_shared: *mut ParallelShared = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, parallel::SHM_TOC_SHARED_KEY, false)
            .cast::<ParallelShared>()
    };
    let cluster_data: *mut ClusterParallelData = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, parallel::SHM_TOC_CLUSTER_DATA_KEY, false)
            .cast::<ClusterParallelData>()
    };
    let tablescandesc = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, parallel::SHM_TOC_TABLESCANDESC_KEY, false)
            .cast::<pg_sys::ParallelTableScanDescData>()
    };

    let params = unsafe {
        (*parallel_shared).params
    };

    let should_initialize = unsafe {
        (*parallel_shared)
            .build_state
            .start_nodes_initialized
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    };

    if !should_initialize {
        unsafe {
            loop {
                let ntuples = (*parallel_shared)
                    .build_state
                    .ntuples
                    .load(Ordering::Relaxed);
                let init_done = (*parallel_shared)
                    .build_state
                    .initializing_worker_done
                    .load(Ordering::Relaxed);

                if ntuples >= parallel::initial_start_nodes_count() || init_done {
                    break;
                }

                pg_sys::ConditionVariableSleep(
                    &raw mut (*parallel_shared).build_state.initialization_cv,
                    pg_sys::PG_WAIT_EXTENSION,
                );
            }
        }
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

    let heaprel = unsafe { pg_sys::table_open(params.heaprelid, heap_lockmode) };
    let indexrel = unsafe { pg_sys::index_open(params.indexrelid, index_lockmode) };
    let index_info = unsafe { pg_sys::BuildIndexInfo(indexrel) };
    let heap_relation = unsafe { PgRelation::from_pg(heaprel) };
    let index_relation = unsafe { PgRelation::from_pg(indexrel) };
    let mut meta_page = MetaPage::fetch(&index_relation);

    let centroids = unsafe { (*cluster_data).centroids.clone() };
    let cluster_assignments = unsafe { (*cluster_data).cluster_assignments.clone() };
    let num_clusters = centroids.len();

    super::super::do_heap_scan_with_clustering(
        index_info,
        &heap_relation,
        &index_relation,
        &mut meta_page,
        WriteStats::default(),
        Some(ParallelBuildInfo {
            parallel_shared,
            is_initializing_worker: should_initialize,
            tablescandesc,
        }),
        params.worker_count,
        &[],
        &cluster_assignments,
        &centroids,
        num_clusters,
    );

    unsafe {
        pg_sys::index_close(indexrel, index_lockmode);
        pg_sys::table_close(heaprel, heap_lockmode);
    }
}
