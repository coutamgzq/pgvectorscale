# pgvectorscale 索引构建原理详解

## 1. 概述

pgvectorscale 是一个 PostgreSQL 扩展，实现了基于 DiskANN 算法的向量索引。与传统的 pgvector 索引相比，它能够支持更大规模的数据集和更快的搜索速度。

本文档详细分析 `ambuild` 函数的实现逻辑，特别是**聚类 (clustering)** 和**并发构建子图**的机制。

## 2. 核心数据结构

### 2.1 MetaPage
元页面存储索引的元数据配置，包括：
- 维度数 (dimensions)
- 距离类型 (distance_type)
- 存储类型 (storage_type)
- 邻居数量 (num_neighbors)
- 最大 alpha 值 (max_alpha)

### 2.2 Graph
DiskANN 索引的核心是一个内存中的图结构，每个节点存储：
- 向量数据
- 邻居列表 (neighbors)
- 指向堆表的指针 (heap_tid)

### 2.3 Storage
支持两种存储类型：
- **PlainStorage**: 未压缩存储
- **SbqSpeedupStorage**: SBQ (Sub-band Quantization) 压缩存储

## 3. ambuild 函数主流程

`ambuild` 是 PostgreSQL 索引访问方法的入口函数，当执行 `CREATE INDEX` 时由 PostgreSQL 调用。

```
┌─────────────────────────────────────────────────────────────┐
│                      ambuild 函数                            │
├─────────────────────────────────────────────────────────────┤
│ 1. 获取索引配置 (TSVIndexOptions)                            │
│ 2. 创建 MetaPage                                            │
│ 3. 检查是否使用聚类 (TSV_NUM_CLUSTERS > 1)                  │
│    ├─ 是: 调用 build_index_with_clustering                 │
│    │         └─ 聚类 → 并行/顺序构建                         │
│    └─ 否: 继续常规流程                                      │
│         ├─ 训练量化器 (如果是 SBQ)                          │
│         ├─ 确定 worker 数量                                 │
│         └─ 并行/顺序构建                                     │
└─────────────────────────────────────────────────────────────┘
```

### 3.1 决策流程详解

```rust
// 判断是否使用聚类
let num_clusters = TSV_NUM_CLUSTERS.get() as usize;
let use_clustering = num_clusters > 1;

if use_clustering {
    // 聚类构建流程
    return build_index_with_clustering(...)
} else {
    // 常规构建流程
    // 1. 训练量化器 (SBQ 存储需要)
    let write_stats = maybe_train_quantizer(...);
    
    // 2. 确定并行 worker 数量
    let workers = if cfg!(feature = "build_parallel") 
        && !meta_page.has_labels() 
        && meta_page.get_storage_type() == StorageType::SbqCompression {
        // 根据配置或系统决定 worker 数量
    } else { 0 };
    
    // 3. 执行构建
}
```

## 4. 聚类构建原理

### 4.1 为什么需要聚类？

对于大规模向量数据（数百万或数十亿条），直接构建一个完整的图索引面临以下挑战：

1. **内存限制**: 整个图的邻居关系需要大量内存
2. **计算复杂度**: O(n²) 或更高的复杂度
3. **构建时间**: 单线程构建耗时过长

通过聚类，可以：
- 将数据分成多个子集，每个子集构建独立的子图
- 减少内存占用
- 支持并行构建

### 4.2 聚类流程

```
┌─────────────────────────────────────────────────────────────┐
│                  聚类构建流程                                 │
├─────────────────────────────────────────────────────────────┤
│ 1. collect_vectors_for_clustering                          │
│    └─ 扫描堆表，收集所有向量 (或采样)                        │
│                                                             │
│ 2. sample_vectors_if_needed                                 │
│    └─ 如果向量数量过多，进行采样                             │
│       (减少 k-means 计算量)                                  │
│                                                             │
│ 3. perform_clustering                                       │
│    └─ 执行 k-means 聚类算法                                  │
│       ├─ 返回: centroids (聚类中心)                         │
│       └─ 返回: cluster_assignments (每向量的聚类ID)          │
│                                                             │
│ 4. maybe_train_quantizer                                    │
│    └─ 训练 SBQ 量化器 (如果使用压缩存储)                    │
│                                                             │
│ 5. 构建阶段                                                  │
│    ├─ 并行模式: do_parallel_cluster_build                  │
│    └─ 顺序模式: do_heap_scan_with_clustering               │
└─────────────────────────────────────────────────────────────┘
```

### 4.3 k-means 聚类实现

pgvectorscale 实现了标准的 k-means 算法：

```rust
pub fn k_means(
    c: usize,           // 聚类数量
    samples: Vec<Vec<f32>>, // 待聚类向量
    is_spherical: bool,    // 是否球面 k-means
    iterations: usize,     // 最大迭代次数
    prefer_kmeanspp: bool, // 是否使用 k-means++ 初始化
) -> Vec<Vec<f32>> {
    // 算法选择策略:
    // 1. 如果 is_spherical=true，先归一化向量
    // 2. 如果样本数 <= 聚类数，每个样本作为一个中心
    // 3. 如果维度=1，使用一维 k-means
    // 4. 否则使用 Lloyd 算法迭代
}
```

**关键参数说明:**

| 参数 | 说明 | 默认值 |
|------|------|--------|
| TSV_NUM_CLUSTERS | 聚类数量 | 1 (不启用) |
| TSV_CLUSTERING_MAX_SAMPLE_SIZE | 聚类采样上限 | 100000 |
| TSV_CLUSTERING_SAMPLE_THRESHOLD | 触发采样的阈值 | 100000 |

### 4.4 聚类中心点查找

聚类完成后，需要为每个向量分配聚类 ID：

```rust
pub fn k_means_lookup(vector: &[f32], centroids: &[Vec<f32>]) -> usize {
    // 遍历所有中心点，计算欧氏距离
    // 返回最近中心的索引
    let mut result = (f32::INFINITY, 0);
    for (i, centroid) in centroids.iter().enumerate() {
        let dis = squared_distance(vector, centroid);
        if dis <= result.0 {
            result = (dis, i);
        }
    }
    result.1
}
```

## 5. 并行构建原理

### 5.1 并行构建架构

```
┌─────────────────────────────────────────────────────────────┐
│                  并行构建架构                                 │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   主进程 (ambuild)                                         │
│   ┌─────────────────────────────────────┐                  │
│   │ 1. 初始化并行上下文                  │                  │
│   │ 2. 分配共享内存                      │                  │
│   │    - ParallelShared                 │                  │
│   │    - ClusterParallelData (聚类信息) │                  │
│   │    - ParallelTableScanDescData     │                  │
│   │ 3. 启动 Workers                    │                  │
│   │ 4. 等待完成                         │                  │
│   └─────────────────────────────────────┘                  │
│                    ↓                                        │
│   Workers 并行处理                                           │
│   ┌──────────┐ ┌──────────┐ ┌──────────┐                │
│   │ Worker 0 │ │ Worker 1 │ │ Worker N │                │
│   │ 扫描分区1 │ │ 扫描分区2 │ │ 扫描分区N │                │
│   │ 构建子图  │ │ 构建子图  │ │ 构建子图  │                │
│   └──────────┘ └──────────┘ └──────────┘                │
│                    ↓                                        │
│   合并结果                                                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 共享状态 (ParallelShared)

```rust
pub struct ParallelShared {
    pub params: ParallelSharedParams,    // 参数配置
    pub build_state: ParallelBuildState, // 构建状态
}

pub struct ParallelSharedParams {
    pub heaprelid: Oid,         // 堆表 OID
    pub indexrelid: Oid,        // 索引 OID
    pub is_concurrent: bool,    // 是否并发构建
    pub worker_count: usize,    // worker 数量
    pub total_vectors: usize,   // 总向量数
}

pub struct ParallelBuildState {
    pub ntuples: AtomicUsize,           // 已处理元组数 (原子)
    pub start_nodes_initialized: AtomicBool, // 起始节点是否已初始化
    pub initializing_worker_done: AtomicBool, // 初始化 worker 是否完成
    pub initialization_cv: ConditionVariable, // 条件变量 (同步)
}
```

### 5.3 初始化同步机制

并行构建的一个关键挑战是**起始节点 (start nodes)** 的初始化：

```rust
// 只有一个 worker 负责初始化起始节点
let should_initialize = shared_state.build_state
    .start_nodes_initialized
    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
    .is_ok();

if !should_initialize {
    // 非初始化 worker，等待初始化完成
    loop {
        let ntuples = shared_state.build_state.ntuples.load(Ordering::Relaxed);
        let init_done = shared_state.build_state.initializing_worker_done.load(Ordering::Relaxed);
        
        if ntuples >= 1024 || init_done {
            break;
        }
        
        // 等待条件变量
        ConditionVariableSleep(&cv, PG_WAIT_EXTENSION);
    }
}
```

**同步逻辑:**
1. 第一个 worker 获得初始化资格 (start_nodes_initialized)
2. 初始化 worker 处理前 1024 个向量作为起始节点
3. 其他 workers 在条件变量上等待
4. 达到阈值后，广播通知所有等待的 workers

### 5.4 并行回调处理

```rust
fn build_callback_parallel_internal<S: Storage>(...) {
    // 1. 增加元组计数 (原子操作)
    state.increment_ntuples();
    
    // 2. 距离预处理 (Cosine 需要归一化)
    let vector_slice = match distance_type {
        DistanceType::Cosine => preprocess_cosine(vector),
        _ => vector,
    };
    
    // 3. 创建存储节点
    let index_pointer = storage.create_node(...);
    
    // 4. 插入图 (建立邻居关系)
    state.graph.insert(...);
    
    // 5. 定期刷新缓存 (避免内存溢出)
    if local_ntuples % flush_interval == 0 {
        state.graph.maybe_flush_neighbor_cache(...);
    }
}
```

## 6. 聚类过滤原理

### 6.1 顺序模式下的聚类过滤

在顺序构建模式下，可以利用聚类信息进行**过滤**，每次只处理一个聚类的向量：

```rust
fn build_callback_cluster(...) {
    // 1. 在 heap_tids 中查找当前元组的位置
    let vector_index = filter_context.heap_tids
        .iter()
        .position(|tid| tid == heap_pointer);
    
    // 2. 如果找到，检查聚类 ID 是否匹配
    if let Some(idx) = vector_index {
        if filter_context.cluster_assignments[idx] != filter_context.cluster_id {
            return; // 跳过不属于目标聚类的向量
        }
    }
    
    // 3. 匹配的向量正常处理
    build_callback_memory_wrapper(...);
}
```

### 6.2 并行模式下的聚类

**重要**: 并行模式下，聚类信息主要用于：
1. 将聚类中心点通过共享内存传递给所有 workers
2. **不进行实际过滤** (因为并行模式下 cluster_assignments 只覆盖采样向量)

```rust
// 并行模式使用空过滤上下文
let filter_context = ClusterFilterContext {
    heap_tids: &[],           // 空数组，不进行过滤
    cluster_assignments: &[],
    cluster_id: 0,
};
```

这是因为并行模式下难以高效地按聚类过滤数据。

## 7. 执行示例

### 7.1 场景设置

假设我们有一个包含 100 万条向量的表：

```sql
CREATE TABLE items (
    id SERIAL,
    embedding vector(128)
);

-- 插入 100 万条数据
INSERT INTO items (embedding) 
SELECT random()::vector(128) FROM generate_series(1, 1000000);
```

### 7.2 配置参数

```sql
-- 设置聚类数量为 4
SET vectorscale.num_clusters = 4;

-- 设置每个聚类最大采样数
SET vectorscale.clustering_max_sample_size = 50000;

-- 设置并行 worker 数量
SET vectorscale.force_parallel_workers = 4;

-- 创建索引
CREATE INDEX idx_embedding ON items USING diskann (embedding);
```

### 7.3 执行流程详解

#### 第一阶段: 向量收集

```
[主进程]
扫描堆表收集所有向量:
  - 100万条 × 128维 × 4字节 = ~512MB
  - 存储在 VectorCollector.vectors 中
```

#### 第二阶段: 采样 (如果需要)

```
由于 100万 > 5万(阈值)，进行采样:
  - 采样间隔 = 100万 / 5万 = 20
  - 采样后: 5万条向量用于聚类
```

#### 第三阶段: K-means 聚类

```
执行 k-means (k=4):
  - 迭代 100 次
  - 输出 4 个聚类中心
  
示例输出:
  Cluster 0: 250,000 vectors
  Cluster 1: 250,000 vectors  
  Cluster 2: 250,000 vectors
  Cluster 3: 250,000 vectors
```

#### 第四阶段: 并行构建

```
启动 4 个 workers:

Worker 0:
  - 扫描第 1-250000 条
  - 构建子图 (写入本地缓存)
  - 定期刷新到共享存储

Worker 1:
  - 扫描第 250001-500000 条
  - 构建子图

Worker 2:
  - 扫描0第 500001-75000 条
  - 构建子图

Worker 3:
  - 扫描第 750001-1000000 条
  - 构建子图
```

#### 第五阶段: 合并完成

```
所有 workers 完成扫描:
  - 合并邻居缓存
  - 剪枝 (prune) 过度连接的邻居
  - 写入最终索引页
  - 保存 MetaPage
```

### 7.4 时间线示意

```
时间轴:
│──────────│──────────│──────────│──────────│──────────│
│ 收集向量  │  聚类    │ 训练量化 │ 并行构建  │  合并结果 │
│  (10s)   │  (5s)    │   (30s)  │  (60s)   │   (5s)   │
└──────────┴──────────┴──────────┴──────────┴──────────┘
总耗时: ~110s (相比单线程 200s+ 提升显著)
```

## 8. 关键配置参数

| 参数 | 说明 | 默认值 | 建议值 |
|------|------|--------|--------|
| `vectorscale.num_clusters` | 聚类数量 | 1 | 2-8 |
| `vectorscale.clustering_max_sample_size` | 聚类采样上限 | 100000 | 50000-200000 |
| `vectorscale.clustering_sample_threshold` | 采样阈值 | 100000 | 同上 |
| `vectorscale.force_parallel_workers` | 强制 worker 数 | -1 (自动) | CPU 核心数 |
| `vectorscale.min_vectors_for_parallel_build` | 并行构建最小向量数 | 100000 | 根据内存调整 |

## 9. 总结

pgvectorscale 的索引构建流程是一个复杂但高效的系统:

1. **聚类机制**: 将大规模数据分解为可管理的子集
2. **并行构建**: 利用多核 CPU 并行处理
3. **同步机制**: 通过原子操作和条件变量协调 workers
4. **内存管理**: 通过缓存刷新机制控制内存使用

这种设计使得 pgvectorscale 能够高效处理数百万甚至数十亿级的向量数据，同时保持良好的搜索性能。

## 10. _vectorscale_build_cluster_main 并行构建详细分析

### 10.1 函数概述

`_vectorscale_build_cluster_main` 是 PostgreSQL 并行索引构建的 **Worker 入口函数**，由主进程启动的并行 workers 执行。

```rust
// 函数签名
pub extern "C-unwind" fn _vectorscale_build_cluster_main(
    _seg: *mut pg_sys::dsm_segment,  // 动态共享内存段
    shm_toc: *mut pg_sys::shm_toc,   // 共享内存 TOC (Table of Contents)
)
```

### 10.2 数据读取流程

#### 10.2.1 并行表扫描机制

并行 workers 使用 PostgreSQL 的 **Parallel Table Scan** 机制读取数据：

```rust
// 1. 获取并行表扫描描述符
let tablescandesc = pg_sys::shm_toc_lookup(
    shm_toc, 
    parallel::SHM_TOC_TABLESCANDESC_KEY,  // 预定义的 key
    false
).cast::<pg_sys::ParallelTableScanDescData>();

// 2. 使用自定义的 IndexBuildHeapScanParallel 函数
IndexBuildHeapScanParallel(
    heap_relation.as_ptr(),
    index_relation.as_ptr(),
    index_info,
    Some(build_callback_parallel_cluster),  // 回调函数
    &mut filter_state,
    parallel_info.tablescandesc,
)
```

#### 10.2.2 并行扫描原理

```
┌─────────────────────────────────────────────────────────────┐
│              PostgreSQL 并行表扫描原理                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  主进程创建 ParallelTableScanDescData:                     │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ - total_blocks: 表的总块数                         │   │
│  │ - next_block: 下一个要扫描的块 (原子操作)         │   │
│  │ - phaseno: 当前阶段                                │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Workers 并行获取块:                                         │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ next_block = atomic_fetch_add(&scan.next_block, 1)│   │
│  │ 每个 worker 获取不同的块，避免重复                  │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  这就是为什么每个 worker 处理不同的数据子集!                  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 10.3 如何决定数据属于哪个 Cluster

**重要**: 并行构建模式下，**不进行实际的聚类过滤**！

#### 10.3.1 原因分析

1. **聚类信息不完整**: `cluster_assignments` 只包含采样向量的聚类分配，无法对全量数据过滤
2. **并行效率**: 在并行模式下按聚类过滤会破坏负载均衡
3. **实现简化**: 所有 workers 处理全部数据，但共享相同的起始节点

#### 10.3.2 实际行为

```rust
// 在 _vectorscale_build_cluster_main 中:
super::super::do_heap_scan_with_clustering(
    index_info,
    &heap_relation,
    &index_relation,
    &mut meta_page,
    WriteStats::default(),
    Some(ParallelBuildInfo { ... }),
    params.worker_count,
    &[],        // heap_tids: 空数组!
    &[],        // cluster_assignments: 空数组!
    &centroids, // 仍然传递 centroids
    num_clusters,
);
```

```rust
// 在回调函数 build_callback_parallel_cluster 中:
let vector_index = filter_context.heap_tids.iter().position(...);
// 由于 heap_tids 为空，vector_index 始终为 None
// 所以所有向量都会被处理，不进行过滤
```

### 10.4 图构建过程

#### 10.4.1 图插入流程

每个 worker 处理向量时，会执行以下步骤：

```rust
fn build_callback_parallel_internal<S: Storage>(...) {
    // 1. 创建存储节点
    let index_pointer = storage.create_node(
        &vector_slice,
        vector.labels().cloned(),
        heap_pointer,
        meta_page,
        &mut tape,
        &mut local_stats,
    );

    // 2. 插入到图中 (建立邻居关系)
    state.graph.insert(
        index,
        index_pointer,
        vector,
        storage,
        &mut local_stats,
    );
}
```

#### 10.4.2 图插入详细过程

```rust
pub fn insert<S: Storage>(...) {
    // 步骤1: 更新起始节点 (如果是前 N 个节点)
    self.update_start_nodes(index, index_pointer, &vec, storage, stats);

    // 步骤2: 贪婪搜索找到最近的邻居
    let search_results = self.greedy_search_for_build(...);

    // 步骤3: 添加新邻居
    let neighbor_list = self.add_neighbors(...);

    // 步骤4: 更新反向指针
    for neighbor in neighbor_list {
        self.update_back_pointer(...);
    }
}
```

### 10.5 起始节点 (Start Nodes) 存储机制

#### 10.5.1 什么是起始节点？

起始节点是图的**入口点**，搜索从这些节点开始。对于无标签向量，只有一个默认起始节点。

#### 10.5.2 起始节点更新逻辑

```rust
fn update_start_nodes<S: Storage>(...) {
    match self.meta_page.get_start_nodes() {
        Some(start_nodes) => {
            // 如果已有起始节点，检查是否需要添加标签特定的起始节点
            if start_nodes.contains_all(vec.labels()) {
                return; // 所有标签已有起始节点，跳过
            }
        }
        None => {
            // 这是第一个节点，创建初始起始节点
            let start_nodes = StartNodes::new(index_pointer);
            self.meta_page.set_start_nodes(start_nodes);
        }
    }

    // 为新标签添加起始节点
    if let Some(labels) = vec.labels() {
        for label in labels.iter() {
            if !start_nodes.contains(*label) {
                start_nodes.upsert(*label, index_pointer);
            }
        }
    }
}
```

#### 10.5.3 起始节点存储位置

```rust
// MetaPage 结构
pub struct MetaPage {
    // ... 其他字段
    start_nodes: Option<StartNodes>,  // 存储在 MetaPage 中
}

// StartNodes 结构
pub struct StartNodes {
    default_node: ItemPointer,           // 默认起始节点
    labeled_nodes: BTreeMap<Label, ItemPointer>,  // 标签->节点映射
}

// MetaPage 存储位置:
// - 在索引创建时写入索引的第一个页面 (MetaPage)
// - 搜索时从索引中读取
```

#### 10.5.4 并行构建时的起始节点处理

```
┌─────────────────────────────────────────────────────────────┐
│         并行构建时起始节点的处理流程                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. Worker 1 (初始化 Worker)                                │
│     ├─ 处理前 1024 个向量                                   │
│     ├─ 每个向量都尝试设置为起始节点                          │
│     │   └─ 只有第一个有效 (contains_all 检查)              │
│     └─ 将写入 MetaPage (在共享内存中)                       │
│                                                             │
│  2. Worker 2, 3, N (等待中的 Workers)                      │
│     ├─ 等待 ntuples >= 1024                               │
│     ├─ 唤醒后继续处理剩余向量                               │
│     └─ 可能更新标签特定的起始节点                           │
│                                                             │
│  3. 所有 Workers 完成                                       │
│     └─ 合并缓存，最终的 MetaPage 写入磁盘                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 10.6 完整的并行构建流程图

```
┌─────────────────────────────────────────────────────────────────────┐
│                    主进程 (ambuild)                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. 收集向量 (IndexBuildHeapScan)                                    │
│     ↓                                                                │
│  2. K-means 聚类 (生成 centroids)                                    │
│     ↓                                                                │
│  3. 分配共享内存                                                     │
│     ├─ ParallelShared (参数)                                        │
│     ├─ ClusterParallelData (centroids)                              │
│     └─ ParallelTableScanDescData (扫描状态)                         │
│     ↓                                                                │
│  4. 启动 Workers (LaunchParallelWorkers)                            │
│     ↓                                                                │
│  5. 等待完成 (WaitForParallelWorkersToFinish)                       │
│     ↓                                                                │
│  6. 读取结果 (ntuples)                                              │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    Worker 进程                                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  _vectorscale_build_cluster_main:                                   │
│                                                                      │
│  1. 获取共享内存指针                                                 │
│     ├─ parallel_shared                                              │
│     ├─ cluster_data (centroids)                                    │
│     └─ tablescandesc                                               │
│                                                                      │
│  2. 竞争初始化资格                                                   │
│     ├─ compare_exchange(start_nodes_initialized)                    │
│     ├─ 成功者 = 初始化 Worker                                       │
│     └─ 失败者 = 等待 Worker                                         │
│                                                                      │
│  3. 打开 Relation (堆表 + 索引)                                     │
│                                                                      │
│  4. 执行并行扫描                                                     │
│     IndexBuildHeapScanParallel(tablescandesc)                       │
│     ↓                                                                │
│     对于每个元组:                                                    │
│     ├─ build_callback_parallel_cluster()                            │
│     │   ├─ 解析向量                                                │
│     │   ├─ 创建存储节点                                            │
│     │   ├─ 插入图 (update_start_nodes + insert_internal)          │
│     │   └─ 可能刷新缓存                                            │
│     │                                                               │
│  5. 完成扫描                                                         │
│     ├─ finalize_remaining_parallel_nodes()                         │
│     └─ 写入最终 MetaPage                                           │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 10.7 关键配置参数 (并行构建)

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `parallel_initial_start_nodes_count` | 初始化Worker处理的节点数 | 1024 |
| `parallel_flush_interval` | 缓存刷新间隔 (总向量数的比例) | 0.01 (1%) |

## 11. 相关源码文件

- `pgvectorscale/src/access_method/build.rs` - 主构建逻辑
- `pgvectorscale/src/access_method/build/parallel_build/cluster.rs` - 聚类构建
- `pgvectorscale/src/access_method/k_means/mod.rs` - K-means 实现
- `pgvectorscale/src/access_method/graph/mod.rs` - 图索引实现
- `pgvectorscale/src/access_method/graph/start_nodes.rs` - 起始节点实现
- `pgvectorscale/src/access_method/meta_page.rs` - 元页面实现
- `pgvectorscale/src/util/ports.rs` - 并行扫描端口
