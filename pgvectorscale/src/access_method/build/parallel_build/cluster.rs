use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// 聚类构建模块 - 实现基于 k-means 聚类的并行索引构建
/// 
/// 本模块实现了 pgvectorscale 扩展的聚类构建功能，主要包含:
/// 1. VectorCollector: 收集向量用于聚类分析
/// 2. ClusterParallelData: 存储聚类中心点的并行共享数据结构
/// 3. perform_clustering: 执行 k-means 聚类算法
/// 4. collect_vectors_for_clustering: 从堆表收集向量
/// 5. build_index_with_clustering: 聚类构建的主入口函数
/// 6. do_parallel_cluster_build: 并行聚类构建
/// 
/// 聚类构建的目的:
/// - 将大规模向量数据分成多个聚类，减少每个子图的规模
/// - 在顺序构建模式下，可以按聚类依次构建子图，每批只加载目标聚类的向量
/// - 减少内存占用和构建时间
/// 
/// 注意事项:
/// - 并行构建模式下，聚类信息主要用于共享中心点，每个 worker 仍然处理全部向量
/// - 顺序构建模式下，可以利用聚类信息进行过滤，只处理目标聚类的向量
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
/// Note: This structure is stored in shared memory, so we use fixed-size arrays
/// instead of Vec to ensure data is actually stored in shared memory.
/// centroids_data: flattened centroids (all centroids concatenated)
/// centroids_dimensions: dimension of each centroid vector
/// centroids_count: number of centroids
#[derive(Debug, Clone, Copy)]
#[cfg_attr(not(feature = "build_parallel"), allow(dead_code))]
#[repr(C)]
pub struct ClusterParallelData {
    pub centroids_count: usize,
    pub centroids_dimensions: usize,
    // Flexible array member - centroids follow this struct in memory
}

impl ClusterParallelData {
    /// Calculate the size needed for ClusterParallelData with given centroids
    pub fn size_needed(num_centroids: usize, dimensions: usize) -> usize {
        // Use proper alignment for f32 data
        let header_size = std::mem::size_of::<ClusterParallelData>();
        let data_size = num_centroids * dimensions * std::mem::size_of::<f32>();
        // Align to 8 bytes for safety
        let aligned_header = (header_size + 7) & !7;
        aligned_header + data_size
    }
    
    /// Get pointer to centroids data (after the header, properly aligned)
    unsafe fn centroids_data_ptr(ptr: *const Self) -> *const f32 {
        let header_size = std::mem::size_of::<ClusterParallelData>();
        let aligned_offset = (header_size + 7) & !7;
        (ptr as *const u8).add(aligned_offset) as *const f32
    }
    
    /// Write centroids data after the struct in memory
    pub unsafe fn write_centroids(&self, ptr: *mut Self, centroids: &[Vec<f32>]) {
        let data_ptr = Self::centroids_data_ptr(ptr) as *mut f32;
        for (i, centroid) in centroids.iter().enumerate() {
            let dest = data_ptr.add(i * self.centroids_dimensions);
            std::ptr::copy_nonoverlapping(centroid.as_ptr(), dest, self.centroids_dimensions);
        }
    }
    
    /// Read centroids from the memory area after this struct
    pub unsafe fn read_centroids(&self) -> Vec<Vec<f32>> {
        // Validate values to prevent overflow
        if self.centroids_count == 0 || self.centroids_dimensions == 0 {
            return Vec::new();
        }
        
        let data_ptr = Self::centroids_data_ptr(self);
        let mut centroids = Vec::with_capacity(self.centroids_count);
        for i in 0..self.centroids_count {
            let src = data_ptr.add(i * self.centroids_dimensions);
            let mut centroid = vec![0.0f32; self.centroids_dimensions];
            std::ptr::copy_nonoverlapping(src, centroid.as_mut_ptr(), self.centroids_dimensions);
            centroids.push(centroid);
        }
        centroids
    }
}



/// 执行 k-means 聚类算法
/// 
/// 该函数对输入的向量集合进行聚类，返回聚类中心点和每个向量的聚类分配
/// 
/// 算法流程:
/// 1. 更新进度状态为 BUILD_PHASE_CLUSTERING
/// 2. 确定实际聚类数量: actual_num_clusters = min(num_clusters, vectors.len())
///    - 如果向量数量少于请求的聚类数，使用向量数量作为聚类数
/// 3. 调用 k_means::k_means 执行 k-means 算法:
///    - c: 聚类数量
///    - vectors: 输入向量
///    - is_spherical: false (不使用球面 k-means)
///    - iterations: 100 (最大迭代次数)
///    - prefer_kmeanspp: true (使用 k-means++ 初始化)
/// 4. 对每个向量，调用 k_means::k_means_lookup 找到最近的中心点
/// 5. 统计每个聚类的向量数量
/// 
/// 返回值:
/// - Vec<Vec<f32>>: 聚类中心点坐标，每个 Vec 表示一个中心点
/// - Vec<usize>: 每个向量对应的聚类 ID
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

/// 聚类构建主入口函数
/// 
/// 该函数是使用聚类进行索引构建的入口点，实现了以下功能:
/// 
/// 1. 向量收集阶段 (collect_vectors_for_clustering):
///    - 扫描堆表，收集所有向量数据
///    - 可选的采样机制，根据 max_sample_size 和 sample_threshold 决定是否采样
///    - 采样可以减少 k-means 聚类的计算量
/// 
/// 2. 聚类阶段 (perform_clustering):
///    - 对收集的向量执行 k-means 聚类
///    - 返回聚类中心点 (centroids) 和每个向量的聚类分配 (cluster_assignments)
/// 
/// 3. 量化器训练 (maybe_train_quantizer):
///    - 如果使用 SBQ 压缩存储，需要训练量化器
///    - 使用全部向量进行训练
/// 
/// 4. 构建阶段:
///    - 根据 worker 数量决定并行或顺序构建
///    - 并行模式: do_parallel_cluster_build
///    - 顺序模式: do_heap_scan_with_clustering (带聚类过滤)
/// 
/// 参数说明:
/// - heaprel: 堆表 Relation
/// - indexrel: 索引 Relation
/// - index_info: 索引信息
/// - meta_page: 元页面
/// - num_clusters: 请求的聚类数量
/// - heap_relation: 堆表 PgRelation
/// - index_relation: 索引 PgRelation
/// 
/// 返回值:
/// - IndexBuildResult: 包含处理的元组数
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
            heap_tids_for_clustering.as_slice(),
            cluster_assignments.as_slice(),
            centroids.as_slice(),
            actual_num_clusters,
        )
    };

    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = ntuples as f64;
    result.index_tuples = ntuples as f64;

    result.into_pg()
}

/// Performs parallel cluster build
/// Note: cluster_assignments is not used in parallel builds because each worker
/// processes all vectors (no cluster filtering is applied in parallel mode).
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
        // Calculate size needed for ClusterParallelData with centroids
        let centroids_count = centroids.len();
        let centroids_dimensions = centroids.first().map_or(0, |c| c.len());
        // Validate dimensions
        if centroids_count > 0 && centroids_dimensions == 0 {
            warning!("Centroids have zero dimensions, falling back to sequential build");
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
        let cluster_data_size = ClusterParallelData::size_needed(centroids_count, centroids_dimensions);
        parallel::toc_estimate_single_chunk(pcxt, cluster_data_size);
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
            pg_sys::shm_toc_allocate((*pcxt).toc, cluster_data_size)
                .cast::<ClusterParallelData>();

        let cluster_parallel_data = ClusterParallelData {
            centroids_count,
            centroids_dimensions,
        };

        // Write the header
        cluster_data.write(cluster_parallel_data);
        // Write the centroids data after the header
        cluster_parallel_data.write_centroids(cluster_data, centroids);

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

/// 并行构建 Worker 入口函数 - 聚类版本
/// 
/// 这是 PostgreSQL 并行索引构建的 worker 入口函数，由主进程启动的并行 workers 执行
/// 
/// 执行流程详解:
/// 
/// 1. 初始化检查
///    - 检查进程状态标志，确保在安全的索引创建状态下
/// 
/// 2. 从共享内存获取数据
///    - ParallelShared: 包含构建参数和共享状态
///    - ClusterParallelData: 包含聚类中心点信息
///    - ParallelTableScanDescData: 并行表扫描描述符
/// 
/// 3. 初始化同步 (关键!)
///    - 使用 compare_exchange 尝试将 start_nodes_initialized 从 false 改为 true
///    - 成功的 worker 成为"初始化 worker"，负责构建起始节点
///    - 失败的 workers 进入等待循环:
///      - 等待 ntuples >= 1024 (初始节点数阈值) 或 初始化完成
///      - 使用 ConditionVariableSleep 休眠，节省 CPU
/// 
/// 4. 打开关系 (Relation)
///    - 根据 is_concurrent 选择锁模式:
///      - 并发索引: heap=ShareLock, index=AccessExclusiveLock
///      - 非并发: heap=ShareUpdateExclusiveLock, index=RowExclusiveLock
///    - 打开堆表和索引表
/// 
/// 5. 获取聚类中心点
///    - 从 ClusterParallelData 读取 centroids (共享内存)
///    - 这些中心点是在主进程聚类阶段计算好的
/// 
/// 6. 执行构建
///    - 调用 do_heap_scan_with_clustering
///    - 关键: 传入空的 heap_tids 和 cluster_assignments
///      (因为并行模式下无法高效地按聚类过滤)
///    - 所有 workers 处理全部数据，但使用相同的起始节点
/// 
/// 7. 清理资源
///    - 关闭索引和堆表
///    - 释放锁
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

    let centroids = unsafe { (*cluster_data).read_centroids() };
    let num_clusters = centroids.len();

    // In parallel build with clustering, we can't use heap_tids for filtering
    // because cluster_assignments only contains assignments for sampled vectors.
    // Instead, we pass empty arrays and disable cluster filtering in the callback.
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
        &[],
        &centroids,
        num_clusters,
    );

    unsafe {
        pg_sys::index_close(indexrel, index_lockmode);
        pg_sys::table_close(heaprel, heap_lockmode);
    }
}
