# 基于 K-means 聚类的索引构建优化

## 概述

本文档详细说明了基于 K-means 聚类的索引构建优化功能的原理和实现细节。该功能通过将向量数据聚类为多个簇，然后并行构建每个簇的索引，从而提高索引构建的性能。

## 背景

在构建向量索引时，传统的 DiskANN 算法需要遍历所有向量，并为每个向量查找最近的邻居。这个过程涉及到大量的随机 page 访问，可能导致性能瓶颈。

通过 K-means 聚类，我们可以：
1. 将向量数据划分为多个空间上相近的簇
2. 每个簇可以独立构建索引，减少跨簇的 page 访问
3. 利用多核 CPU 并行构建多个簇的索引
4. 为每个簇设置独立的入口节点，提高查询性能

## 架构设计

### 整体流程

基于 K-means 聚类的索引构建流程分为三个主要阶段：

```
┌─────────────────────────────────────────────────────────────────┐
│                      阶段 1: 收集向量                          │
│  使用 IndexBuildHeapScan 扫描 heap 表，收集所有向量            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      阶段 2: K-means 聚类                      │
│  使用 K-means 算法将向量聚类为 N 个簇                          │
│  计算每个向量所属的聚类                                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      阶段 3: 并行构建索引                        │
│  主 worker 扫描 heap 表，计算每个向量所属的聚类                  │
│  将向量分发到对应的聚类管道                                      │
│  每个 worker 从管道消费数据并构建索引                            │
└─────────────────────────────────────────────────────────────────┘
```

### 数据结构

#### 1. VectorCollector - 向量收集器

```rust
struct VectorCollector {
    vectors: Vec<Vec<f32>>,           // 收集的向量数据
    heap_tids: Vec<pg_sys::ItemPointerData>,  // 对应的 heap tid
}

struct VectorCollectorWithMeta<'a> {
    collector: VectorCollector,
    meta_page: &'a MetaPage,          // 元数据页面引用
}
```

**用途**：在第一阶段收集所有向量和对应的 heap tid。

#### 2. ClusterParallelData - 并行构建数据

```rust
struct ClusterParallelData {
    pcxt: *mut pg_sys::ParallelContext,           // 并行上下文
    snapshot: *mut pg_sys::SnapshotData,         // 快照
    centroids: Vec<Vec<f32>>,                     // 聚类中心
    cluster_assignments: Vec<usize>,              // 聚类分配结果
}
```

**用途**：存储并行构建所需的数据，包括聚类中心和聚类分配结果。

#### 3. ClusterFilterContext - 聚类过滤上下文

```rust
struct ClusterFilterContext<'a, 'b> {
    heap_tids: &'a [pg_sys::ItemPointerData],   // heap tid 列表
    cluster_assignments: &'b [usize],            // 聚类分配结果
    cluster_id: usize,                           // 当前聚类 ID
}
```

**用途**：在构建索引时，过滤出属于特定聚类的向量。

## 详细实现

### GUC 参数

在 `guc.rs` 中添加了 `diskann.num_clusters` 参数：

```rust
let num_clusters = GucRegistry::define_int_guc(
    "diskann.num_clusters",
    "Number of clusters for K-means clustering during index build",
    1,
    1,
    64,
    1,
    GucContext::Userspace,
    GucFlags::default(),
);
```

- **默认值**：1（不使用聚类）
- **最大值**：64
- **作用**：控制 K-means 聚类的中心数量

### 阶段 1: 收集向量

```rust
unsafe {
    pgstat_progress_update_param(PROGRESS_CREATE_IDX_SUBPHASE, BUILD_PHASE_COLLECTING_VECTORS);
}

let mut collector_with_meta = unsafe {
    VectorCollectorWithMeta {
        collector: VectorCollector {
            vectors: Vec::new(),
            heap_tids: Vec::new(),
        },
        meta_page: &*(&meta_page as *const _),
    }
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
```

**回调函数**：

```rust
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
        collector_with_meta.collector.vectors.push(vec.to_index_slice().to_vec());
        collector_with_meta.collector.heap_tids.push(*ctid);
    }
}
```

### 阶段 2: K-means 聚类

```rust
unsafe {
    pgstat_progress_update_param(PROGRESS_CREATE_IDX_SUBPHASE, BUILD_PHASE_CLUSTERING);
}

let actual_num_clusters = num_clusters.min(num_vectors);
let centroids = k_means::k_means(
    actual_num_clusters,
    collector_with_meta.collector.vectors.clone(),
    false,  // 不使用 k-means++ 初始化
    100,    // 最大迭代次数
    true,   // 使用并行计算
);

notice!("K-means clustering completed with {} centroids", centroids.len());

let mut cluster_assignments = vec![0usize; num_vectors];
for (i, vector) in collector_with_meta.collector.vectors.iter().enumerate() {
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
```

### 阶段 3: 并行构建索引

```rust
unsafe {
    pgstat_progress_update_param(PROGRESS_CREATE_IDX_SUBPHASE, BUILD_PHASE_BUILDING_GRAPH);
}

let write_stats = maybe_train_quantizer(index_info, &heap_relation, &index_relation, &mut meta_page);
unsafe {
    meta_page.store(&index_relation, false);
};

let heap_tuples = unsafe { heap_relation.rd_rel.as_ref().unwrap().reltuples as usize };

let workers = if cfg!(feature = "build_parallel")
    && !meta_page.has_labels()
    && meta_page.get_storage_type() == StorageType::SbqCompression
{
    let forced_workers = crate::access_method::guc::TSV_FORCE_PARALLEL_WORKERS.get();
    if forced_workers >= 0 {
        forced_workers as usize
    } else {
        if heap_tuples >= min_vectors_for_parallel_build() {
            unsafe { (*index_info).ii_ParallelWorkers as usize }
        } else {
            0
        }
    }
} else {
    0
};

let is_concurrent = unsafe { (*index_info).ii_Concurrent };

if workers > 0 {
    let cluster_parallel_data = ClusterParallelData {
        pcxt: std::ptr::null_mut(),
        snapshot: std::ptr::null_mut(),
        centroids: centroids.clone(),
        cluster_assignments: cluster_assignments.clone(),
    };

    let cluster_data = Box::new(cluster_parallel_data);
    let cluster_data_ptr = Box::into_raw(cluster_data);

    let parallel_shared = unsafe {
        parallel::setup_parallel_context(
            index_info,
            index_relation.as_ptr(),
            workers,
            Some(_vectorscale_build_cluster_main),
            cluster_data_ptr as *mut _,
        )
    };

    let ntuples = if let Some(pcxt) = parallel_shared {
        let snapshot = unsafe { pg_sys::GetTransactionSnapshot() };
        let parallel_data = ClusterParallelData {
            pcxt,
            snapshot,
            centroids: centroids.clone(),
            cluster_assignments: cluster_assignments.clone(),
        };

        unsafe {
            pg_sys::WaitForParallelWorkersToAttach(pcxt);
        }

        Some(parallel_data)
    } else {
        None
    };

    let ntuples = if let Some(ClusterParallelData { pcxt, snapshot, centroids: _, cluster_assignments: _ }) = parallel_data {
        unsafe {
            pg_sys::WaitForParallelWorkersToFinish(pcxt);
            let parallel_shared: *mut ParallelShared =
                pg_sys::shm_toc_lookup((*pcxt).toc, parallel::SHM_TOC_SHARED_KEY, false)
                    .cast::<ParallelShared>();
            let ntuples = (*parallel_shared)
                .build_state
                .ntuples
                .load(Ordering::Relaxed);
            parallel::cleanup_parallel_context(pcxt, snapshot);
            ntuples
        }
    } else {
        do_heap_scan_with_clustering(
            index_info,
            &heap_relation,
            &index_relation,
            &mut meta_page,
            write_stats,
            None,
            workers,
            &collector_with_meta.collector.heap_tids,
            &cluster_assignments,
            &centroids,
            actual_num_clusters,
        )
    };
} else {
    do_heap_scan_with_clustering(
        index_info,
        &heap_relation,
        &index_relation,
        &mut meta_page,
        write_stats,
        None,
        workers,
        &collector_with_meta.collector.heap_tids,
        &cluster_assignments,
        &centroids,
        actual_num_clusters,
    )
};
```

## 状态转换示例

下面用一个具体的例子来说明状态转换过程。

### 初始状态

假设我们有一个包含 6 个向量的数据集，每个向量是 2 维的：

```
向量 1: (1.0, 1.0)  -> heap_tid: (0, 1)
向量 2: (1.5, 1.5)  -> heap_tid: (0, 2)
向量 3: (2.0, 2.0)  -> heap_tid: (0, 3)
向量 4: (8.0, 8.0)  -> heap_tid: (0, 4)
向量 5: (8.5, 8.5)  -> heap_tid: (0, 5)
向量 6: (9.0, 9.0)  -> heap_tid: (0, 6)
```

### 阶段 1: 收集向量

执行 `IndexBuildHeapScan` 后，`VectorCollector` 的状态：

```rust
VectorCollector {
    vectors: [
        [1.0, 1.0],
        [1.5, 1.5],
        [2.0, 2.0],
        [8.0, 8.0],
        [8.5, 8.5],
        [9.0, 9.0],
    ],
    heap_tids: [
        (0, 1),
        (0, 2),
        (0, 3),
        (0, 4),
        (0, 5),
        (0, 6),
    ],
}
```

**日志输出**：
```
NOTICE:  Collected 6 vectors for k-means clustering
```

### 阶段 2: K-means 聚类

假设我们设置 `num_clusters = 2`，K-means 算法执行后的结果：

```rust
centroids: [
    [1.5, 1.5],  // 聚类 0 的中心
    [8.5, 8.5],  // 聚类 1 的中心
]

cluster_assignments: [
    0,  // 向量 1 属于聚类 0
    0,  // 向量 2 属于聚类 0
    0,  // 向量 3 属于聚类 0
    1,  // 向量 4 属于聚类 1
    1,  // 向量 5 属于聚类 1
    1,  // 向量 6 属于聚类 1
]
```

**聚类统计**：
```rust
cluster_stats: [
    (0, 3),  // 聚类 0 有 3 个向量
    (1, 3),  // 聚类 1 有 3 个向量
]
```

**日志输出**：
```
NOTICE:  K-means clustering completed with 2 centroids
NOTICE:  Cluster distribution:
NOTICE:   Cluster 0: 3 vectors
NOTICE:   Cluster 1: 3 vectors
```

### 阶段 3: 并行构建索引

#### 3.1 创建并行上下文

```rust
let cluster_parallel_data = ClusterParallelData {
    pcxt: std::ptr::null_mut(),
    snapshot: std::ptr::null_mut(),
    centroids: [
        [1.5, 1.5],
        [8.5, 8.5],
    ],
    cluster_assignments: [
        0, 0, 0, 1, 1, 1,
    ],
};
```

#### 3.2 启动并行 Workers

假设 `workers = 2`，PostgreSQL 会启动 2 个后台 worker 进程。

#### 3.3 主 Worker 执行

主 worker 执行 `do_heap_scan_with_clustering`：

```rust
fn do_heap_scan_with_clustering(
    index_info: *mut pg_sys::IndexInfo,
    heap_relation: &PgRelation,
    index_relation: &PgRelation,
    meta_page: &mut MetaPage,
    write_stats: WriteStats,
    parallel_build_info: Option<ParallelBuildInfo>,
    worker_count: usize,
    heap_tids: &[pg_sys::ItemPointerData],
    cluster_assignments: &[usize],
    centroids: &[Vec<f32>],
    num_clusters: usize,
) -> usize {
    // ...
}
```

主 worker 扫描 heap 表，对于每个向量：

1. 查找该向量属于哪个聚类
2. 如果属于当前 worker 负责的聚类，则构建索引

#### 3.4 Worker 1 处理聚类 0

Worker 1 处理聚类 0 的向量：

```rust
ClusterFilterContext {
    heap_tids: [
        (0, 1),  // 向量 1
        (0, 2),  // 向量 2
        (0, 3),  // 向量 3
    ],
    cluster_assignments: [0, 0, 0],
    cluster_id: 0,
}
```

Worker 1 为这些向量构建索引：

```
索引节点 1: vector = [1.0, 1.0], heap_tid = (0, 1)
索引节点 2: vector = [1.5, 1.5], heap_tid = (0, 2)
索引节点 3: vector = [2.0, 2.0], heap_tid = (0, 3)
```

Worker 1 设置聚类 0 的入口节点：

```rust
let entry_node = ItemPointer::new(0, 0);  // cluster_id = 0
meta_page.set_start_node(Some(StartNodes::new(entry_node)));
```

#### 3.5 Worker 2 处理聚类 1

Worker 2 处理聚类 1 的向量：

```rust
ClusterFilterContext {
    heap_tids: [
        (0, 4),  // 向量 4
        (0, 5),  // 向量 5
        (0, 6),  // 向量 6
    ],
    cluster_assignments: [1, 1, 1],
    cluster_id: 1,
}
```

Worker 2 为这些向量构建索引：

```
索引节点 4: vector = [8.0, 8.0], heap_tid = (0, 4)
索引节点 5: vector = [8.5, 8.5], heap_tid = (0, 5)
索引节点 6: vector = [9.0, 9.0], heap_tid = (0, 6)
```

Worker 2 设置聚类 1 的入口节点：

```rust
let entry_node = ItemPointer::new(1, 0);  // cluster_id = 1
meta_page.set_start_node(Some(StartNodes::new(entry_node)));
```

### 最终状态

索引构建完成后的状态：

```
聚类 0:
  中心: [1.5, 1.5]
  向量: [1.0, 1.0], [1.5, 1.5], [2.0, 2.0]
  入口节点: (0, 0)

聚类 1:
  中心: [8.5, 8.5]
  向量: [8.0, 8.0], [8.5, 8.5], [9.0, 9.0]
  入口节点: (1, 0)
```

**日志输出**：
```
NOTICE:  Indexed 6 tuples
```

## 查询过程

当执行查询时，算法会：

1. 计算查询向量与所有聚类中心的距离
2. 选择最近的聚类
3. 从该聚类的入口节点开始搜索
4. 在该聚类内查找最近的邻居

### 查询示例

假设查询向量为 `[1.2, 1.2]`：

1. 计算与聚类中心的距离：
   - 距离聚类 0 中心 [1.5, 1.5]: sqrt((1.2-1.5)^2 + (1.2-1.5)^2) = 0.42
   - 距离聚类 1 中心 [8.5, 8.5]: sqrt((1.2-8.5)^2 + (1.2-8.5)^2) = 10.32

2. 选择最近的聚类：聚类 0

3. 从聚类 0 的入口节点 (0, 0) 开始搜索

4. 在聚类 0 内查找最近的邻居：
   - [1.0, 1.0] -> 距离: 0.28
   - [1.5, 1.5] -> 距离: 0.42
   - [2.0, 2.0] -> 距离: 1.13

5. 返回最近的向量：[1.0, 1.0]

## 性能优化

### 1. 降低 Page 随机访问

通过聚类，空间上相近的向量被分配到同一个簇，索引构建时可以更好地利用局部性，减少跨簇的 page 访问。

### 2. 并行构建加速

多个聚类可以并行构建，充分利用多核 CPU。每个 worker 处理一个聚类，互不干扰。

### 3. 固定入口节点

训练完成后，每个聚类的入口节点就固定了，不需要后续更新。这减少了运行时的开销。

### 4. 更好的查询性能

查询时可以快速定位到相关聚类，减少搜索范围。对于大规模数据集，这可以显著提高查询性能。

## 使用方法

### 1. 设置聚类中心数量

```sql
-- 设置聚类中心数量为 64
SET diskann.num_clusters = 64;
```

### 2. 创建索引

```sql
-- 创建索引（会自动使用 K-means 聚类）
CREATE INDEX ON your_table USING diskann (your_vector_column);
```

### 3. 查看日志

索引构建时会输出详细的日志信息：

```
NOTICE:  Starting index build with num_neighbors=-1, search_list_size=100, max_alpha=1.2, storage_layout=SbqCompression.
NOTICE:  Using k-means clustering with 64 clusters
NOTICE:  Collected 1000000 vectors for k-means clustering
NOTICE:  K-means clustering completed with 64 centroids
NOTICE:  Cluster distribution:
NOTICE:   Cluster 0: 15625 vectors
NOTICE:   Cluster 1: 15625 vectors
...
NOTICE:   Cluster 63: 15625 vectors
NOTICE:  Indexed 1000000 tuples
```

## 注意事项

### 1. 聚类数量选择

- **小数据集**（< 10,000 向量）：聚类可能不会带来明显性能提升，建议使用默认值 1
- **中等数据集**（10,000 - 1,000,000 向量）：建议使用 8-32 个聚类
- **大数据集**（> 1,000,000 向量）：建议使用 32-64 个聚类

### 2. 聚类计算开销

聚类本身需要额外的计算时间，但可以通过并行化来抵消。对于超大规模数据集，聚类时间可能较长。

### 3. 内存使用

聚类需要存储所有向量和聚类中心，对于超大规模数据集，可能需要较大的内存。

### 4. 查询精度

使用聚类可能会略微降低查询精度，因为查询只搜索最近的聚类。如果需要更高的精度，可以增加搜索范围或减少聚类数量。

## 总结

基于 K-means 聚类的索引构建优化通过以下方式提高性能：

1. **降低 page 随机访问**：空间上相近的向量被分配到同一个簇
2. **并行构建加速**：多个聚类可以并行构建，充分利用多核 CPU
3. **固定入口节点**：训练完成后，入口节点就固定了，不需要后续更新
4. **更好的查询性能**：查询时可以快速定位到相关聚类，减少搜索范围

该功能特别适合大规模向量数据集的索引构建，可以显著提高构建速度和查询性能。
