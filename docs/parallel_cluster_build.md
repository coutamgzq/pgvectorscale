# 并行 Cluster 构建执行逻辑

本文档详细解释 pgvectorscale 中并行带 cluster 的索引构建执行逻辑。

## 1. 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          Phase 1: 收集向量样本                                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│    ┌──────────────┐                                                            │
│    │  Heap Table  │                                                            │
│    │  (向量数据)   │                                                            │
│    └──────┬───────┘                                                            │
│           │                                                                     │
│           ▼                                                                     │
│    ┌──────────────┐      采样 (如 25%)                                          │
│    │   Producer   │ ────────────────────────────────┐                          │
│    │  (主进程)     │                                │                          │
│    └──────────────┘                                ▼                          │
│                                           ┌──────────────────┐                │
│                                           │ Sampled Vectors  │                │
│                                           │   (样本向量)      │                │
│                                           └────────┬─────────┘                │
└────────────────────────────────────────────────────┼───────────────────────────┘
                                                     │
┌────────────────────────────────────────────────────┼───────────────────────────┐
│                          Phase 2: K-means 聚类     │                           │
├────────────────────────────────────────────────────┼───────────────────────────┤
│                                                    ▼                           │
│                                           ┌──────────────────┐                │
│                                           │  K-means 算法    │                │
│                                           │  (生成 centroids)│                │
│                                           └────────┬─────────┘                │
│                                                    │                           │
│                    ┌───────────────────────────────┼───────────────────────┐   │
│                    │                               ▼                       │   │
│                    │   Centroids (聚类中心):                               │   │
│                    │   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐    │   │
│                    │   │C0 (128d)│ │C1 (128d)│ │C2 (128d)│ │C3 (128d)│    │   │
│                    │   └─────────┘ └─────────┘ └─────────┘ └─────────┘    │   │
│                    │       Cluster 0  Cluster 1  Cluster 2  Cluster 3     │   │
│                    └───────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
                                                     │
┌────────────────────────────────────────────────────┼───────────────────────────┐
│                     Phase 3: 并行构建索引          │                           │
├────────────────────────────────────────────────────┼───────────────────────────┤
│                                                    │                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                     共享内存 (DSM)                                        │   │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │   │
│  │  │ ParallelShared                                                     │  │   │
│  │  │ ├─ params: heaprelid, indexrelid, num_clusters...                │  │   │
│  │  │ └─ build_state: producer_done, consumers_finished...             │  │   │
│  │  └───────────────────────────────────────────────────────────────────┘  │   │
│  │                                                                         │   │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │   │
│  │  │ ClusterQueues (生产者-消费者队列)                                   │  │   │
│  │  │                                                                     │  │   │
│  │  │  Queue 0          Queue 1          Queue 2          Queue 3       │  │   │
│  │  │ ┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐ │  │   │
│  │  │ │ head: 0   │    │ head: 0   │    │ head: 0   │    │ head: 0   │ │  │   │
│  │  │ │ tail: 0   │    │ tail: 0   │    │ tail: 0   │    │ tail: 0   │ │  │   │
│  │  │ │ capacity  │    │ capacity  │    │ capacity  │    │ capacity  │ │  │   │
│  │  │ │  =1024    │    │  =1024    │    │  =1024    │    │  =1024    │ │  │   │
│  │  │ ├───────────┤    ├───────────┤    ├───────────┤    ├───────────┤ │  │   │
│  │  │ │ Entry 0   │    │ Entry 0   │    │ Entry 0   │    │ Entry 0   │ │  │   │
│  │  │ │ ├─heap_tid│    │ ├─heap_tid│    │ ├─heap_tid│    │ ├─heap_tid│ │  │   │
│  │  │ │ ├─vec_len │    │ ├─vec_len │    │ ├─vec_len │    │ ├─vec_len │ │  │   │
│  │  │ │ └─vector[]│    │ └─vector[]│    │ └─vector[]│    │ └─vector[]│ │  │   │
│  │  │ │ Entry 1   │    │ Entry 1   │    │ Entry 1   │    │ Entry 1   │ │  │   │
│  │  │ │ ...       │    │ ...       │    │ ...       │    │ ...       │ │  │   │
│  │  │ │ Entry 1023│    │ Entry 1023│    │ Entry 1023│    │ Entry 1023│ │  │   │
│  │  │ └───────────┘    └───────────┘    └───────────┘    └───────────┘ │  │   │
│  │  │                                                                     │  │   │
│  │  │  ConditionVariable[0-3] 用于同步等待                                 │  │   │
│  │  └───────────────────────────────────────────────────────────────────┘  │   │
│  │                                                                         │   │
│  │  ┌───────────────────┐    ┌───────────────────┐                        │   │
│  │  │ Centroids         │    │ ClusterStartNodes │                        │   │
│  │  │ (聚类中心数据)     │    │ (各 cluster 起始  │                        │   │
│  │  │                   │    │  节点指针)        │                        │   │
│  │  └───────────────────┘    └───────────────────┘                        │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                    │                           │
│                    ┌───────────────────────────────┴───────────────────────┐    │
│                    │                                                       │    │
│                    ▼                                                       ▼    │
│  ┌─────────────────────────────────────┐    ┌─────────────────────────────────┐ │
│  │         Producer (主进程)            │    │      Consumers (并行 Workers)   │ │
│  ├─────────────────────────────────────┤    ├─────────────────────────────────┤ │
│  │                                     │    │                                 │ │
│  │  1. IndexBuildHeapScan 扫描堆表     │    │  Worker 0 (Cluster 0):          │ │
│  │     ↓                               │    │  ┌─────────────────────────────┐│ │
│  │  2. 对每个向量:                      │    │  │ 1. 等待 Queue 0 有数据      ││ │
│  │     a. 计算最近 centroid            │    │  │ 2. 读取 entry (heap_tid,vec)││ │
│  │        cluster_id = lookup(vector)  │    │  │ 3. 创建索引节点             ││ │
│  │     b. 写入对应队列                  │    │  │ 4. 构建邻居连接             ││ │
│  │        push_to_queue(               │    │  │ 5. 重复直到 producer_done   ││ │
│  │          cluster_id,                │    │  │    且队列为空               ││ │
│  │          heap_tid,                  │    │  │ 6. 记录 first_node          ││ │
│  │          vector                     │    │  └─────────────────────────────┘│ │
│  │        )                            │    │                                 │ │
│  │     c. 通知等待的 consumer          │    │  Worker 1 (Cluster 1):          │ │
│  │        ConditionVariableBroadcast   │    │  ┌─────────────────────────────┐│ │
│  │                                     │    │  │        (同上)               ││ │
│  │  3. 扫描完成后:                      │    │  └─────────────────────────────┘│ │
│  │     a. 设置 finished = true         │    │                                 │ │
│  │     b. 广播通知所有 consumer        │    │  Worker 2 (Cluster 2): ...      │ │
│  │                                     │    │                                 │ │
│  └─────────────────────────────────────┘    │  Worker 3 (Cluster 3): ...      │ │
│                                             │                                 │ │
│                                             └─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
                                                     │
┌────────────────────────────────────────────────────┼───────────────────────────┐
│                     Phase 4: 合并结果              │                           │
├────────────────────────────────────────────────────┼───────────────────────────┤
│                                                    │                           │
│                    ┌───────────────────────────────┴───────────────────────┐    │
│                    ▼                                                       ▼    │
│  ┌─────────────────────────────────────┐    ┌─────────────────────────────────┐ │
│  │      Producer 等待所有 Consumer      │    │    ClusterStartNodes 更新       │ │
│  │      完成 (consumers_finished)       │    │                                 │ │
│  └──────────────────┬──────────────────┘    │  Cluster 0 → first_node_0       │ │
│                     │                        │  Cluster 1 → first_node_1       │ │
│                     ▼                        │  Cluster 2 → first_node_2       │ │
│  ┌─────────────────────────────────────┐    │  Cluster 3 → first_node_3       │ │
│  │      将 ClusterStartNodes 写入      │    │                                 │ │
│  │      MetaPage                        │    └─────────────────────────────────┘ │
│  └─────────────────────────────────────┘                                        │
│                                                                                  │
│  最终索引结构:                                                                    │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                           Index Pages                                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │   │
│  │  │ Cluster 0   │  │ Cluster 1   │  │ Cluster 2   │  │ Cluster 3   │     │   │
│  │  │ Subgraph    │  │ Subgraph    │  │ Subgraph    │  │ Subgraph    │     │   │
│  │  │             │  │             │  │             │  │             │     │   │
│  │  │ start_node─→│  │ start_node─→│  │ start_node─→│  │ start_node─→│     │   │
│  │  │   Node 1    │  │   Node 1    │  │   Node 1    │  │   Node 1    │     │   │
│  │  │   Node 2    │  │   Node 2    │  │   Node 2    │  │   Node 2    │     │   │
│  │  │   ...       │  │   ...       │  │   ...       │  │   ...       │     │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘     │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  MetaPage:                                                                       │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │ cluster_start_nodes: {0: first_node_0, 1: first_node_1,                  │   │
│  │                      2: first_node_2, 3: first_node_3}                   │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 2. 关键数据结构

### 2.1 共享内存结构

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          ParallelShared 结构                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  #[derive(Debug)]                                                               │
│  pub struct ParallelShared {                                                    │
│      pub params: ParallelSharedParams,    // 构建参数                           │
│      pub build_state: ParallelBuildState, // 构建状态                           │
│  }                                                                              │
│                                                                                 │
│  #[derive(Debug, Copy, Clone)]                                                  │
│  pub struct ParallelSharedParams {                                              │
│      pub heaprelid: Oid,           // 堆表 OID                                  │
│      pub indexrelid: Oid,          // 索引 OID                                  │
│      pub is_concurrent: bool,      // 是否并发构建                              │
│      pub num_clusters: usize,      // cluster 数量                              │
│      pub total_vectors: usize,     // 总向量数                                  │
│      pub num_dimensions: usize,    // 向量维度                                  │
│  }                                                                              │
│                                                                                 │
│  #[derive(Debug)]                                                               │
│  pub struct ParallelBuildState {                                                │
│      pub producer_done: AtomicBool,        // 生产者是否完成                    │
│      pub producer_ntuples: AtomicUsize,    // 已处理元组数                      │
│      pub consumers_finished: AtomicUsize,  // 已完成的消费者数                  │
│      pub initialization_cv: ConditionVariable, // 初始化条件变量                │
│  }                                                                              │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 队列结构

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          ClusterQueueEntry 结构                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  #[repr(C)]                                                                     │
│  #[derive(Debug, Clone, Copy)]                                                  │
│  pub struct ClusterQueueEntry {                                                 │
│      pub heap_tid: pg_sys::ItemPointerData,   // 6 bytes - 堆元组指针           │
│      pub vector_len: u32,                     // 4 bytes - 向量维度             │
│      // 后面紧跟 vector 数据: [f32; vector_len]                                  │
│  }                                                                              │
│                                                                                 │
│  内存布局:                                                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ heap_tid (6B) │ padding (2B) │ vector_len (4B) │ vector[0] │ ... │     │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                          ClusterQueues 结构                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  #[repr(C)]                                                                     │
│  pub struct ClusterQueues {                                                     │
│      pub num_queues: usize,                 // 队列数量                         │
│      pub queue_headers: [ClusterQueueHeader; 64],  // 队列头数组                │
│      pub condition_vars: [ConditionVariable; 64],  // 条件变量数组              │
│      pub entry_size: usize,                 // 每个 entry 的大小                │
│      pub queue_capacity: usize,             // 每个队列的容量                   │
│  }                                                                              │
│                                                                                 │
│  #[repr(C)]                                                                     │
│  #[derive(Debug)]                                                               │
│  pub struct ClusterQueueHeader {                                                │
│      pub head: AtomicUsize,     // 消费位置                                     │
│      pub tail: AtomicUsize,     // 生产位置                                     │
│      pub capacity: usize,       // 队列容量                                     │
│      pub element_size: usize,   // 元素大小                                     │
│      pub finished: AtomicBool,  // 是否已完成                                   │
│  }                                                                              │
│                                                                                 │
│  内存布局:                                                                       │
│  ┌───────────────────────────────────────────────────────────────────────────┐ │
│  │ ClusterQueues struct                                                      │ │
│  │ ├─ num_queues                                                             │ │
│  │ ├─ queue_headers[64]                                                      │ │
│  │ ├─ condition_vars[64]                                                     │ │
│  │ ├─ entry_size                                                             │ │
│  │ └─ queue_capacity                                                         │ │
│  ├───────────────────────────────────────────────────────────────────────────┤ │
│  │ Queue Data (紧跟在 struct 后面)                                            │ │
│  │ ├─ Queue 0: [Entry 0][Entry 1]...[Entry N]                               │ │
│  │ ├─ Queue 1: [Entry 0][Entry 1]...[Entry N]                               │ │
│  │ ├─ ...                                                                    │ │
│  │ └─ Queue K: [Entry 0][Entry 1]...[Entry N]                               │ │
│  └───────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.3 生产者-消费者同步机制

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          生产者-消费者同步机制                                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Producer:                          Consumer:                                   │
│  ┌──────────────────────┐          ┌──────────────────────┐                    │
│  │ push_to_queue()      │          │ loop {               │                    │
│  │   while queue.full() │          │   if queue.not_empty │                    │
│  │     CV.sleep()       │          │     pop entry        │                    │
│  │   write entry        │          │     process entry    │                    │
│  │   tail++             │          │     head++           │                    │
│  │   CV.broadcast() ◄───┼──────────┼─ wake up             │                    │
│  │ }                    │          │   elif finished      │                    │
│  │                      │          │     break            │                    │
│  │ set finished=true    │          │   else               │                    │
│  │ CV.broadcast() ◄─────┼──────────┼─ CV.sleep()          │                    │
│  └──────────────────────┘          │ }                    │                    │
│                                    └──────────────────────┘                    │
│                                                                                 │
│  环形队列:                                                                       │
│        ┌───┬───┬───┬───┬───┬───┬───┬───┐                                       │
│        │ 0 │ 1 │ 2 │ 3 │ 4 │...│ N │ 0 │  (capacity = N+1)                     │
│        └───┴───┴───┴───┴───┴───┴───┴───┘                                       │
│              ↑                       ↑                                         │
│            head                    tail                                         │
│          (消费位置)              (生产位置)                                      │
│                                                                                 │
│  队列状态:                                                                       │
│  - 空: head == tail                                                             │
│  - 满: (tail + 1) % capacity == head                                            │
│  - 元素数: tail >= head ? tail - head : capacity - head + tail                  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 3. 执行时序图

```
  时间轴
    │
    │  ┌─────────────────────────────────────────────────────────────────────┐
    │  │ Phase 1 & 2: 采样 + K-means (主进程串行)                             │
    │  │                                                                     │
    │  │  collect_vectors_for_clustering()                                  │
    │  │  sample_vectors_if_needed()                                        │
    │  │  perform_clustering() → centroids                                  │
    │  └─────────────────────────────────────────────────────────────────────┘
    │                                      │
    │                                      ▼
    │  ┌─────────────────────────────────────────────────────────────────────┐
    │  │ CreateParallelContext()                                             │
    │  │ InitializeParallelDSM()                                             │
    │  │ 分配共享内存: ParallelShared, ClusterQueues, Centroids...           │
    │  └─────────────────────────────────────────────────────────────────────┘
    │                                      │
    │                                      ▼
    │  ┌─────────────────────────────────────────────────────────────────────┐
    │  │ LaunchParallelWorkers()                                             │
    │  │                                                                     │
    │  │    Worker 0 ──────┐                                                │
    │  │    Worker 1 ──────┤                                                │
    │  │    Worker 2 ──────┼──► 启动并等待初始化                             │
    │  │    Worker 3 ──────┤                                                │
    │  │                   │                                                │
    │  └─────────────────────────────────────────────────────────────────────┘
    │                                      │
    │                                      ▼
    │  Producer                            │  Consumers (并行)
    │    │                                 │
    │    │  IndexBuildHeapScan             │    Worker 0-3: 等待队列数据
    │    │  ├─ 读取 tuple 1                │        │
    │    │  │  计算 cluster_id             │        │ CV.sleep()
    │    │  │  写入 Queue[cluster_id]      │        │
    │    │  │  广播 CV                     │◄───────┤
    │    │  ├─ 读取 tuple 2                │        │
    │    │  │  ...                         │        ▼
    │    │  │                              │    Worker X: 被唤醒
    │    │  │                              │        ├─ 读取 entry
    │    │  │                              │        ├─ 创建节点
    │    │  │                              │        ├─ 构建邻居
    │    │  │                              │        └─ 继续等待
    │    │  ├─ ...                         │
    │    │  │                              │
    │    │  └─ 所有 tuple 完成             │
    │    │     设置 finished = true        │        │
    │    │     广播所有 CV                  │◄───────┤
    │    │                                 │        ▼
    │    │                                 │    Worker X: 检测到 finished
    │    │                                 │        ├─ 处理剩余数据
    │    │                                 │        ├─ 记录 first_node
    │    │                                 │        └─ consumers_finished++
    │    │                                 │
    │    │  等待 consumers_finished        │
    │    │     == num_clusters             │
    │    │                                 │
    │    ▼                                 │
    │  ┌───────────────────────────────────┴─────────────────────────────────┐
    │  │ 收集 ClusterStartNodes                                              │
    │  │ 写入 MetaPage                                                        │
    │  │ DestroyParallelContext()                                            │
    │  └─────────────────────────────────────────────────────────────────────┘
    │
    ▼
```

## 4. 关键函数代码

### 4.1 入口函数: build_index_with_clustering

```rust
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

    // Phase 1: 收集向量样本
    let collector = collect_vectors_for_clustering(
        heap_relation,
        index_relation,
        index_info,
        meta_page,
        max_sample_size,
        sample_threshold,
    );

    // 采样
    let (vectors_for_clustering, _heap_tids_for_clustering) = 
        sample_vectors_if_needed(&collector, max_sample_size, sample_threshold);

    // Phase 2: K-means 聚类
    let (centroids, _cluster_assignments) = perform_clustering(vectors_for_clustering, num_clusters);
    let actual_num_clusters = centroids.len();

    // 决定是否使用并行构建
    let workers = if cfg!(feature = "build_parallel")
        && !meta_page.has_labels()
        && meta_page.get_storage_type() == StorageType::SbqCompression
    {
        // ... 计算并行 worker 数量
    } else {
        0
    };

    // Phase 3: 执行构建
    let ntuples = if workers > 0 && actual_num_clusters > 1 {
        do_parallel_cluster_build(...)
    } else {
        do_sequential_cluster_build(...)
    };

    // 返回结果
    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = ntuples as f64;
    result.index_tuples = ntuples as f64;
    result.into_pg()
}
```

### 4.2 K-means 聚类: perform_clustering

```rust
pub fn perform_clustering(
    vectors: Vec<Vec<f32>>,
    num_clusters: usize,
) -> (Vec<Vec<f32>>, Vec<usize>) {
    unsafe {
        pgstat_progress_update_param(PROGRESS_CREATE_IDX_SUBPHASE, BUILD_PHASE_CLUSTERING);
    }

    let actual_num_clusters = num_clusters.min(vectors.len());
    
    // 执行 K-means 算法
    let centroids = k_means::k_means(
        actual_num_clusters,
        vectors.clone(),
        false,
        100,
        true,
    );

    notice!("K-means clustering completed with {} centroids", centroids.len());

    // 计算每个向量的 cluster 分配
    let mut cluster_assignments = vec![0usize; vectors.len()];
    for (i, vector) in vectors.iter().enumerate() {
        cluster_assignments[i] = k_means::k_means_lookup(vector, &centroids);
    }

    // 输出 cluster 分布统计
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
```

### 4.3 并行构建: do_parallel_cluster_build

```rust
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
    notice!(
        "Parallel cluster build with {} workers for {} clusters",
        workers, num_clusters
    );

    unsafe {
        // 进入并行模式
        pg_sys::EnterParallelMode();

        // 创建并行上下文
        let num_workers = num_clusters;
        let pcxt = pg_sys::CreateParallelContext(
            crate::EXTENSION_NAME,
            PARALLEL_BUILD_CLUSTER_CONSUMER_MAIN,
            num_workers as i32,
        );

        // 获取快照
        let snapshot = if is_concurrent {
            pg_sys::RegisterSnapshot(pg_sys::GetTransactionSnapshot())
        } else {
            &raw mut pg_sys::SnapshotAnyData
        };

        // 估算共享内存大小
        parallel::toc_estimate_single_chunk(pcxt, std::mem::size_of::<ParallelShared>());
        
        let cluster_queues_size = std::mem::size_of::<ClusterQueues>() + 
            num_clusters * DEFAULT_QUEUE_CAPACITY * 
            (std::mem::size_of::<ClusterQueueEntry>() + num_dimensions * std::mem::size_of::<f32>());
        parallel::toc_estimate_single_chunk(pcxt, cluster_queues_size);

        // 初始化 DSM
        pg_sys::InitializeParallelDSM(pcxt);

        if (*pcxt).seg.is_null() {
            warning!("Failed to allocate DSM segment, falling back to sequential build");
            parallel::cleanup_parallel_context(pcxt, snapshot);
            return super::super::do_heap_scan_with_clustering(...);
        }

        // 分配并初始化共享内存结构
        let parallel_shared = pg_sys::shm_toc_allocate(
            (*pcxt).toc,
            std::mem::size_of::<ParallelShared>(),
        )
        .cast::<ParallelShared>();

        // 初始化共享状态
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

        // 初始化条件变量
        pg_sys::ConditionVariableInit(&raw mut (*parallel_shared).build_state.initialization_cv);

        // 分配并初始化 ClusterQueues
        let cluster_queues = pg_sys::shm_toc_allocate(
            (*pcxt).toc,
            cluster_queues_size,
        )
        .cast::<ClusterQueues>();
        
        (*cluster_queues) = ClusterQueues::new(num_clusters, DEFAULT_QUEUE_CAPACITY, num_dimensions);

        // 分配并写入 centroids
        let centroids_ptr = pg_sys::shm_toc_allocate(
            (*pcxt).toc,
            centroids_size,
        ).cast::<u8>();
        write_centroids_to_shmem(centroids_ptr, centroids, num_dimensions);

        // 分配 ClusterStartNodes
        let cluster_start_nodes = pg_sys::shm_toc_allocate(
            (*pcxt).toc,
            start_nodes_size,
        )
        .cast::<ClusterStartNodes>();
        (*cluster_start_nodes) = ClusterStartNodes::new(num_clusters);

        // 将所有结构插入 TOC
        pg_sys::shm_toc_insert((*pcxt).toc, parallel::SHM_TOC_SHARED_KEY, parallel_shared.cast());
        pg_sys::shm_toc_insert((*pcxt).toc, SHM_TOC_CLUSTER_QUEUES_KEY, cluster_queues.cast());
        pg_sys::shm_toc_insert((*pcxt).toc, SHM_TOC_CENTROIDS_KEY, centroids_ptr.cast());
        pg_sys::shm_toc_insert((*pcxt).toc, SHM_TOC_CLUSTER_START_NODES_KEY, cluster_start_nodes.cast());

        // 启动并行 workers
        pg_sys::LaunchParallelWorkers(pcxt);

        if (*pcxt).nworkers_launched == 0 {
            warning!("No workers launched, falling back to sequential build");
            parallel::cleanup_parallel_context(pcxt, snapshot);
            return super::super::do_heap_scan_with_clustering(...);
        }

        let launched = (*pcxt).nworkers_launched as usize;
        notice!("Launched {} parallel workers", launched);

        // 等待 workers 附加
        pg_sys::WaitForParallelWorkersToAttach(pcxt);

        // 创建生产者状态
        let mut producer_state = ProducerState {
            _parallel_shared: parallel_shared,
            cluster_queues,
            centroids: &centroids,
            num_dimensions,
            ntuples: 0,
            meta_page: meta_page.clone(),
        };

        // 执行堆扫描 (生产者)
        pg_sys::IndexBuildHeapScan(
            heaprel,
            indexrel,
            index_info,
            Some(producer_callback),
            &mut producer_state as *mut _ as *mut std::os::raw::c_void,
        );

        // 标记生产者完成
        (*parallel_shared)
            .build_state
            .producer_done
            .store(true, Ordering::Release);

        // 设置所有队列的 finished 标志并广播
        for i in 0..num_clusters {
            (*cluster_queues)
                .queue_headers[i]
                .finished
                .store(true, Ordering::Release);
            pg_sys::ConditionVariableBroadcast(
                &(*cluster_queues).condition_vars[i] as *const _ as *mut _,
            );
        }

        // 存储处理的元组数
        (*parallel_shared)
            .build_state
            .producer_ntuples
            .store(producer_state.ntuples, Ordering::Relaxed);

        // 等待所有 workers 完成
        pg_sys::WaitForParallelWorkersToFinish(pcxt);

        // 获取处理的元组数
        let ntuples = (*parallel_shared)
            .build_state
            .producer_ntuples
            .load(Ordering::Relaxed);

        // 收集 cluster 起始节点
        collect_cluster_start_nodes(cluster_start_nodes, meta_page, index_relation);

        // 清理并行上下文
        parallel::cleanup_parallel_context(pcxt, snapshot);
        ntuples
    }
}
```

### 4.4 生产者回调: producer_callback

```rust
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
    
    // 检查 ctid 是否有效
    if ctid.is_null() {
        return;
    }
    
    // 解析向量
    let vec = PgVector::from_pg_parts(values, isnull, 0, &producer_state.meta_page, true, false);
    if let Some(vec) = vec {
        let vector_slice = vec.to_index_slice();
        
        // 查找最近的 cluster
        let cluster_id = k_means::k_means_lookup(vector_slice, producer_state.centroids);
        
        // 将向量推入对应队列
        push_to_queue(
            producer_state.cluster_queues,
            cluster_id,
            *ctid,
            vector_slice,
            producer_state.num_dimensions,
        );
        
        producer_state.ntuples += 1;
    }
}
```

### 4.5 推入队列: push_to_queue

```rust
unsafe fn push_to_queue(
    cluster_queues: *mut ClusterQueues,
    cluster_id: usize,
    heap_tid: pg_sys::ItemPointerData,
    vector: &[f32],
    num_dimensions: usize,
) {
    // 验证 heap_tid
    if heap_tid.ip_posid == 0 {
        notice!("Producer: Skipping invalid heap_tid (ip_posid=0)");
        return;
    }
    
    if heap_tid.ip_posid == pg_sys::InvalidOffsetNumber {
        notice!("Producer: Skipping invalid heap_tid (ip_posid=InvalidOffsetNumber)");
        return;
    }
    
    let block_num = pgrx::itemptr::item_pointer_get_block_number_no_check(heap_tid);
    if block_num == pg_sys::InvalidBlockNumber {
        notice!("Producer: Skipping invalid heap_tid (block_num=InvalidBlockNumber)");
        return;
    }
    
    let queues = &mut *cluster_queues;
    let header = &mut queues.queue_headers[cluster_id];
    let cv = &queues.condition_vars[cluster_id];

    loop {
        let tail = header.tail.load(Ordering::Acquire);
        let head = header.head.load(Ordering::Acquire);
        let next_tail = (tail + 1) % header.capacity;

        // 检查队列是否已满
        if next_tail != head {
            // 获取 entry 指针
            let entry_ptr = (*cluster_queues).get_entry(cluster_id, tail);
            
            // 写入数据
            (*entry_ptr).heap_tid = heap_tid;
            (*entry_ptr).vector_len = num_dimensions as u32;
            
            // 写入向量数据
            let vector_ptr = (entry_ptr as *mut u8)
                .add(std::mem::size_of::<ClusterQueueEntry>()) as *mut f32;
            std::ptr::copy_nonoverlapping(vector.as_ptr(), vector_ptr, num_dimensions);

            // 更新 tail
            header.tail.store(next_tail, Ordering::Release);
            
            // 唤醒等待的消费者
            pg_sys::ConditionVariableBroadcast(cv as *const _ as *mut _);
            return;
        }

        // 队列已满，等待
        pg_sys::ConditionVariableSleep(cv as *const _ as *mut _, pg_sys::PG_WAIT_EXTENSION);
    }
}
```

### 4.6 消费者主函数: _vectorscale_build_cluster_consumer_main

```rust
#[unsafe(no_mangle)]
#[cfg(feature = "build_parallel")]
pub extern "C" fn _vectorscale_build_cluster_consumer_main(
    _seg: *mut pg_sys::dsm_segment,
    shm_toc: *mut pg_sys::shm_toc,
) {
    // 从 TOC 获取共享内存结构
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

    // 获取 worker 编号，对应 cluster_id
    let worker_number = unsafe { pg_sys::ParallelWorkerNumber as usize };
    let cluster_id = worker_number;

    if cluster_id >= params.num_clusters {
        return;
    }

    notice!("Consumer worker {} starting for cluster {}", worker_number, cluster_id);

    // 打开关系
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

        // 创建消费者状态
        let mut consumer_state = ConsumerState {
            cluster_id,
            cluster_queues,
            _parallel_shared: parallel_shared,
            _num_dimensions: params.num_dimensions,
            ntuples: 0,
            first_node: None,
        };

        // 构建 cluster 子图
        build_cluster_subgraph(
            &mut consumer_state,
            &heap_relation,
            &index_relation,
            &mut meta_page,
            &centroids,
        );

        // 记录 cluster 起始节点
        if let Some(first_node) = consumer_state.first_node {
            let mut item_pointer_data = pg_sys::ItemPointerData::default();
            first_node.to_item_pointer_data(&mut item_pointer_data);
            (*cluster_start_nodes).set_start_node(cluster_id, item_pointer_data);
        }

        // 标记消费者完成
        (*parallel_shared)
            .build_state
            .consumers_finished
            .fetch_add(1, Ordering::Release);

        // 关闭关系
        pg_sys::index_close(indexrel, index_lockmode);
        pg_sys::table_close(heaprel, heap_lockmode);
    }
}
```

### 4.7 构建 cluster 子图: build_cluster_subgraph

```rust
unsafe fn build_cluster_subgraph(
    consumer_state: &mut ConsumerState,
    heap_relation: &PgRelation,
    index_relation: &PgRelation,
    meta_page: &mut MetaPage,
    _centroids: &[Vec<f32>],
) {
    let queues = &mut *consumer_state.cluster_queues;
    let header = &mut queues.queue_headers[consumer_state.cluster_id];
    let cv = &queues.condition_vars[consumer_state.cluster_id];

    let storage_type = meta_page.get_storage_type();
    const BUILDER_NEIGHBOR_CACHE_SIZE: f64 = 0.8;

    // 创建图结构
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
                let head = header.head.load(Ordering::Acquire);
                let tail = header.tail.load(Ordering::Acquire);

                if head != tail {
                    // 读取 entry
                    let entry_ptr = (*consumer_state.cluster_queues)
                        .get_entry(consumer_state.cluster_id, head);
                    
                    let heap_tid = (*entry_ptr).heap_tid;
                    let vector_len = (*entry_ptr).vector_len as usize;
                    
                    // 验证 heap_tid
                    if heap_tid.ip_posid == 0 || 
                       heap_tid.ip_posid == pg_sys::InvalidOffsetNumber {
                        let next_head = (head + 1) % header.capacity;
                        header.head.store(next_head, Ordering::Release);
                        pg_sys::ConditionVariableBroadcast(cv as *const _ as *mut _);
                        continue;
                    }
                    
                    // 读取向量数据
                    let vector_ptr = (entry_ptr as *const u8)
                        .add(std::mem::size_of::<ClusterQueueEntry>()) as *const f32;
                    let vector_data = std::slice::from_raw_parts(vector_ptr, vector_len);

                    // 更新 head
                    let next_head = (head + 1) % header.capacity;
                    header.head.store(next_head, Ordering::Release);

                    // 广播唤醒其他等待者
                    pg_sys::ConditionVariableBroadcast(cv as *const _ as *mut _);

                    // 创建 ItemPointer
                    let heap_pointer = ItemPointer::new(
                        pgrx::itemptr::item_pointer_get_block_number_no_check(heap_tid),
                        pgrx::itemptr::item_pointer_get_offset_number_no_check(heap_tid),
                    );

                    // 处理向量 (归一化等)
                    let distance_type = meta_page.get_distance_type();
                    let vector_slice: Vec<f32> = match distance_type {
                        DistanceType::Cosine => {
                            let mut normalized = vector_data.to_vec();
                            distance::preprocess_cosine(&mut normalized);
                            normalized
                        }
                        _ => vector_data.to_vec(),
                    };

                    // 创建索引节点
                    let index_pointer = plain.create_node(
                        &vector_slice,
                        None,
                        heap_pointer,
                        meta_page,
                        &mut tape,
                        &mut write_stats,
                    );

                    // 记录第一个节点
                    if consumer_state.first_node.is_none() {
                        consumer_state.first_node = Some(index_pointer);
                    }

                    consumer_state.ntuples += 1;
                } else if header.finished.load(Ordering::Acquire) {
                    // 生产者已完成且队列为空
                    break;
                } else {
                    // 等待数据
                    pg_sys::ConditionVariableSleep(
                        cv as *const _ as *mut _, 
                        pg_sys::PG_WAIT_EXTENSION
                    );
                }
            }

            graph.maybe_flush_neighbor_cache(&mut plain, &mut insert_stats);
        }
        StorageType::SbqCompression => {
            // 类似处理...
        }
    }
}
```

## 5. 关键代码路径总结

| 阶段 | 函数 | 文件 | 说明 |
|------|------|------|------|
| 入口 | `ambuild()` | build.rs | PostgreSQL 索引构建入口 |
| 决策 | `build_index_with_clustering()` | cluster.rs | 判断是否使用 clustering |
| 采样 | `collect_vectors_for_clustering()` | cluster.rs | 收集样本向量 |
| 聚类 | `perform_clustering()` | cluster.rs | K-means 聚类生成 centroids |
| 并行构建 | `do_parallel_cluster_build()` | cluster.rs | 启动并行构建流程 |
| 生产者回调 | `producer_callback()` | cluster.rs | 扫描堆表，分发向量到队列 |
| 推入队列 | `push_to_queue()` | cluster.rs | 将向量写入共享队列 |
| 消费者主函数 | `_vectorscale_build_cluster_consumer_main()` | cluster.rs | Worker 入口函数 |
| 子图构建 | `build_cluster_subgraph()` | cluster.rs | 构建单个 cluster 的子图 |
| 结果收集 | `collect_cluster_start_nodes()` | cluster.rs | 收集各 cluster 起始节点 |

## 6. GUC 参数

| 参数名 | 默认值 | 说明 |
|--------|--------|------|
| `tsv.num_clusters` | 1 | cluster 数量，大于 1 时启用 clustering |
| `tsv.clustering_max_sample_size` | 5000 | K-means 最大样本数 |
| `tsv.clustering_sample_threshold` | 0 | 采样阈值 |
| `tsv.min_vectors_for_parallel_build` | 65536 | 启用并行构建的最小向量数 |
| `tsv.force_parallel_workers` | -1 | 强制指定并行 worker 数量 (-1 表示自动) |
