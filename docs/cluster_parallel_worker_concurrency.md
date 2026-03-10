# Cluster 并行构建：多 Worker 并发处理原理

## 1. 概述

本文档详细说明 pgvectorscale 中 **同一个 Cluster 多个 Worker 并发构建** 的原理和实现细节。这是方案 B（动态 Worker 分配）的实现，核心思想是**根据 Cluster 大小动态分配 Worker 数量**，大 Cluster 分配更多 Worker，小 Cluster 分配较少 Worker。

## 2. 架构设计

### 2.1 传统架构 vs 动态 Worker 分配

#### 传统架构（问题）
```
Worker 0 -> Cluster 0 (764 vectors)
Worker 1 -> Cluster 1 (1369 vectors)
Worker 2 -> Cluster 2 (982 vectors)
...
Worker 7 -> Cluster 7 (776 vectors)

问题：Worker 5 处理 1818 vectors，成为瓶颈
```

#### 动态 Worker 分配架构
```
Worker 0 -> Cluster 5 (part 1)  [0..500)
Worker 1 -> Cluster 5 (part 2)  [500..1000)
Worker 2 -> Cluster 5 (part 3)  [1000..1818)
Worker 3 -> Cluster 6 (part 1)  [0..400)
Worker 4 -> Cluster 6 (part 2)  [400..800)
Worker 5 -> Cluster 1 (part 1)  [0..684)
Worker 6 -> Cluster 1 (part 2)  [684..1369)
Worker 7 -> Cluster 3           [0..776)

大 cluster 被分割给多个 worker 处理
```

### 2.2 核心组件

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Leader 进程                                   │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  1. 计算 Worker 分配策略                                        │  │
│  │     - 根据 Cluster 大小比例分配 Worker                          │  │
│  │     - 生成 WorkerAssignment 表                                  │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                       │
│                              ▼                                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  2. 写入共享内存                                                │  │
│  │     - WorkerAssignments (任务分配表)                            │  │
│  │     - ClusterQueues (队列)                                     │  │
│  │     - ClusterSizes (大小统计)                                  │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                       │
│                              ▼                                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  3. 启动 Producer 扫描                                          │  │
│  │     - 全表扫描                                                  │  │
│  │     - 计算每个向量的 Cluster ID                                 │  │
│  │     - 分发到对应 Cluster 的队列                                 │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              │ 启动 Worker 进程
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Worker 0 ~ N 进程                               │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  1. 等待任务分配就绪                                            │  │
│  │     - 等待 assignments_ready 信号                               │  │
│  │     - 从 WorkerAssignments 获取自己的任务                       │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                       │
│                              ▼                                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  2. Consumer 处理                                               │  │
│  │     - 从指定 Cluster 队列消费数据                               │  │
│  │     - 只处理自己负责的索引范围 [start_idx, end_idx)             │  │
│  │     - 构建局部图索引                                            │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## 3. 核心数据结构

### 3.1 WorkerAssignment - 任务分配单元

```rust
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct WorkerAssignment {
    pub cluster_id: usize,    // 负责的 Cluster ID
    pub start_idx: usize,     // 在 Cluster 内的起始索引
    pub end_idx: usize,       // 在 Cluster 内的结束索引（不包含）
    pub is_primary: bool,     // 是否是主 Worker（负责保存 start node）
}
```

**作用**：
- 定义每个 Worker 负责的数据范围
- `is_primary` 标记主 Worker，负责保存该 Cluster 的 start node
- 多个 Worker 可以处理同一个 Cluster，但各自负责不同的索引范围

### 3.2 WorkerAssignments - 全局任务表

```rust
#[repr(C)]
pub struct WorkerAssignments {
    pub assignments: [WorkerAssignment; MAX_WORKERS],  // 任务分配数组
    pub num_assignments: AtomicUsize,                  // 实际任务数量
    pub ready: AtomicBool,                             // 任务是否就绪
}
```

**存储位置**：共享内存（DSM - Dynamic Shared Memory）

**生命周期**：
1. Leader 计算并写入
2. Workers 读取并执行
3. 所有 Worker 完成后释放

### 3.3 ClusterQueues - Cluster 数据队列

```rust
#[repr(C)]
pub struct ClusterQueues {
    pub num_queues: usize,        // Cluster 数量（每个 Cluster 一个队列）
    pub entry_size: usize,        // 每个条目大小
    pub queue_capacity: usize,    // 每个队列容量
}
```

**内存布局**：
```
[ClusterQueues header]
[queue_headers: ClusterQueueHeader; num_queues]
[condition_vars: ConditionVariable; num_queues]
[queue_data: [ClusterQueueEntry; queue_capacity]; num_queues]
```

**作用**：
- Producer 向队列写入数据（向量 + heap_tid）
- Consumers 从队列读取数据并构建索引
- 每个 Cluster 有独立的队列，避免竞争

### 3.4 ClusterSizes - Cluster 大小统计

```rust
#[repr(C)]
pub struct ClusterSizes {
    pub sizes: [AtomicUsize; 64],  // 每个 Cluster 的向量数量
    pub num_clusters: usize,
}
```

**作用**：
- 运行时统计每个 Cluster 的实际大小
- 用于动态调整 Worker 分配（预估 vs 实际）

## 4. Worker 分配算法

### 4.1 算法流程

```rust
pub fn calculate_worker_assignments(
    cluster_sizes: &[usize],    // 每个 Cluster 的大小
    num_workers: usize,          // 总 Worker 数量
) -> Vec<WorkerAssignment> {
    let total_vectors: usize = cluster_sizes.iter().sum();
    let mut assignments = Vec::new();

    for (cluster_id, &size) in cluster_sizes.iter().enumerate() {
        if size == 0 { continue; }

        // 1. 计算该 Cluster 需要的 Worker 数量（比例分配）
        let workers_for_cluster = {
            let ideal = (size as f64 * num_workers as f64 / total_vectors as f64).ceil() as usize;
            ideal.max(1).min(size).min(num_workers - assignments.len())
        };

        // 2. 计算每个 Worker 处理的向量范围
        let chunk_size = (size + workers_for_cluster - 1) / workers_for_cluster;

        // 3. 为每个 Worker 创建 Assignment
        for i in 0..workers_for_cluster {
            let start_idx = i * chunk_size;
            let end_idx = ((i + 1) * chunk_size).min(size);
            let is_primary = i == 0;  // 第一个 Worker 是主 Worker

            assignments.push(WorkerAssignment::new(
                cluster_id, start_idx, end_idx, is_primary
            ));
        }
    }

    assignments
}
```

### 4.2 分配示例

**场景**：8 Workers，4 Clusters
- Cluster 0: 1000 vectors
- Cluster 1: 3000 vectors  ← 最大
- Cluster 2: 1500 vectors
- Cluster 3: 500 vectors

**计算**：
```
总向量数 = 6000

Cluster 0: 1000/6000 * 8 = 1.33 → 2 Workers
Cluster 1: 3000/6000 * 8 = 4.0  → 4 Workers
Cluster 2: 1500/6000 * 8 = 2.0  → 2 Workers
Cluster 3: 500/6000 * 8 = 0.66  → 1 Worker

总计: 2 + 4 + 2 + 1 = 9 > 8，需要调整
实际分配:
- Cluster 0: 1 Worker (1000 vectors)
- Cluster 1: 4 Workers (750 vectors each)
- Cluster 2: 2 Workers (750 vectors each)
- Cluster 3: 1 Worker (500 vectors)
```

### 4.3 分配结果

```
Worker 0: Cluster 0 [0..1000), primary=true
Worker 1: Cluster 1 [0..750), primary=true
Worker 2: Cluster 1 [750..1500), primary=false
Worker 3: Cluster 1 [1500..2250), primary=false
Worker 4: Cluster 1 [2250..3000), primary=false
Worker 5: Cluster 2 [0..750), primary=true
Worker 6: Cluster 2 [750..1500), primary=false
Worker 7: Cluster 3 [0..500), primary=true
```

## 5. 并发处理流程

### 5.1 Leader 进程流程

```rust
pub fn build_index_with_clustering_parallel(...)
    // 1. 初始化并行上下文
    let pcxt = pg_sys::CreateParallelContext(...);
    
    // 2. 预估 Cluster 大小（基于采样）
    let estimated_cluster_sizes = estimate_cluster_sizes(...);
    
    // 3. 计算 Worker 分配
    let assignments = calculate_worker_assignments(
        &estimated_cluster_sizes, 
        num_workers
    );
    
    // 4. 分配共享内存
    let worker_assignments = shm_toc_allocate(...);
    let cluster_queues = shm_toc_allocate(...);
    let cluster_sizes = shm_toc_allocate(...);
    
    // 5. 写入任务分配
    for (worker_id, assignment) in assignments.iter().enumerate() {
        (*worker_assignments).set_assignment(worker_id, *assignment);
    }
    
    // 6. 标记任务就绪，广播给所有 Workers
    (*worker_assignments).mark_ready();
    pg_sys::ConditionVariableBroadcast(...);
    
    // 7. 启动 Producer 扫描
    pg_sys::IndexBuildHeapScan(..., producer_callback, ...);
    
    // 8. 等待所有 Workers 完成
    pg_sys::WaitForParallelWorkersToFinish(pcxt);
```

### 5.2 Producer 回调流程

```rust
unsafe extern "C-unwind" fn producer_callback(
    ...
    state: *mut std::os::raw::c_void,
) {
    let producer_state = &mut *(state as *mut ProducerState);
    
    // 1. 解析向量
    let vec = PgVector::from_pg_parts(...);
    
    // 2. 计算 Cluster ID（基于最近质心）
    let cluster_id = k_means::k_means_lookup(vector_slice, centroids);
    
    // 3. 更新 Cluster 大小统计
    (*cluster_sizes).increment(cluster_id);
    
    // 4. 将数据推入对应 Cluster 的队列
    queues.push_to_queue(base_ptr, cluster_id, *ctid, vector_slice);
    
    // 5. 增加计数
    producer_state.ntuples += 1;
}
```

### 5.3 Worker (Consumer) 流程

```rust
pub extern "C" fn _vectorscale_build_cluster_consumer_main(...) {
    // 1. 获取 worker_id
    let worker_id = (*parallel_shared).build_state.worker_id.load(...);
    
    // 2. 等待任务分配就绪
    while !(*worker_assignments).is_ready() {
        pg_sys::ConditionVariableSleep(...);
    }
    
    // 3. 获取自己的任务分配
    let assignment = (*worker_assignments).get_assignment(worker_id)
        .expect("No assignment for worker");
    
    let cluster_id = assignment.cluster_id;
    let start_idx = assignment.start_idx;
    let end_idx = assignment.end_idx;
    let is_primary = assignment.is_primary;
    
    // 4. Consumer 主循环
    loop {
        // 从队列获取数据
        let entry = queues.pop_from_queue(base_ptr, cluster_id);
        
        // 检查是否是自己负责的索引范围
        let current_idx = (*cluster_sizes).get_processed_count(cluster_id);
        
        if current_idx >= start_idx && current_idx < end_idx {
            // 处理数据：构建索引
            process_vector(entry, ...);
        }
        
        // 如果是主 Worker，保存 start node
        if is_primary && is_first_vector {
            save_start_node(...);
        }
        
        // 检查是否完成
        if producer_done && queue_empty { break; }
    }
}
```

## 6. 关键设计决策

### 6.1 为什么使用比例分配？

**优点**：
- 大 Cluster 获得更多 Worker，避免瓶颈
- 小 Cluster 获得较少 Worker，减少开销
- 自动适应不同数据分布

**对比其他策略**：

| 策略 | 优点 | 缺点 |
|------|------|------|
| **比例分配**（当前） | 负载均衡，自动适应 | 实现稍复杂 |
| 均匀分配 | 实现简单 | 大 Cluster 成为瓶颈 |
| 阈值分配 | 简单可控 | 不够灵活 |

### 6.2 为什么使用连续分割？

**连续分割** vs **交错分割**：

```rust
// 连续分割（当前实现）
Worker 0: [0..250)
Worker 1: [250..500)
Worker 2: [500..750)
Worker 3: [750..1000)

// 交错分割
Worker 0: [0, 4, 8, 12, ...)
Worker 1: [1, 5, 9, 13, ...)
Worker 2: [2, 6, 10, 14, ...)
Worker 3: [3, 7, 11, 15, ...)
```

**连续分割优点**：
- 更好的局部性（Cache Friendly）
- 实现简单
- 减少锁竞争

### 6.3 Start Node 设置机制（关键设计）

#### 6.3.1 问题背景

在同一个 Cluster 多个 Worker 并发构建的场景下，需要解决以下问题：

1. **共享图结构**：同一个 Cluster 的所有 Worker 构建的是同一个图
2. **Start Node 唯一性**：每个 Cluster 只需要一个 start node 作为搜索入口
3. **数据一致性**：多个 Worker 需要协调，避免重复设置或竞争条件

#### 6.3.2 当前实现的问题

**问题代码位置**（cluster.rs L1281-1287）：
```rust
// 在当前实现中，主 Worker 在处理完所有数据后才设置 start node
if is_primary {
    if let Some(first_node) = consumer_state.first_node {
        let mut item_pointer_data = pg_sys::ItemPointerData::default();
        first_node.to_item_pointer_data(&mut item_pointer_data);
        (*cluster_start_nodes).set_start_node(cluster_id, item_pointer_data);
    }
}
```

**问题**：
1. **时机太晚**：在处理完所有数据后才设置，其他 Worker 在构建过程中无法使用 start node
2. **位置不当**：应该在 graph insert 之前设置，确保第一个插入的节点成为 start node
3. **非主 Worker 无法获取**：其他 Worker 无法知道 start node 是否已设置

#### 6.3.3 正确的 Start Node 设置机制

**核心原则**：
1. **提前设置**：在 graph insert 之前设置 start node
2. **共享可见**：使用共享内存（ClusterStartNodes）存储，所有 Worker 可见
3. **只设置一次**：第一个插入节点的 Worker 负责设置
4. **本地缓存**：每个 Worker 在自己的 meta_page 中缓存 start node

**正确实现方案**：

```rust
// 在 graph insert 之前检查并设置 start node（cluster.rs L1391-1397）
fn process_cluster_vectors(...) {
    // ... 从队列获取数据 ...
    
    // 获取当前 Cluster 的共享 start node
    let shared_start_node = (*cluster_start_nodes).get_start_node(cluster_id);
    
    // 如果共享 start node 未设置，当前 Worker 负责设置
    let is_first_node = shared_start_node.is_none();
    
    if is_first_node {
        // 1. 设置共享内存中的 start node
        let mut item_pointer_data = pg_sys::ItemPointerData::default();
        index_pointer.to_item_pointer_data(&mut item_pointer_data);
        (*cluster_start_nodes).set_start_node(cluster_id, item_pointer_data);
        
        // 2. 设置当前 Worker 的 meta_page start node
        meta_page.set_start_node(index_pointer);
    } else {
        // 其他 Worker：从共享内存获取并设置到本地 meta_page
        if let Some(start_node) = (*cluster_start_nodes).get_start_node(cluster_id) {
            meta_page.set_start_node(ItemPointer::from_item_pointer_data(&start_node));
        }
    }
    
    // 3. 执行 graph insert
    graph.insert(
        index_relation,
        index_pointer,
        labeled_vector,
        storage,
        &mut insert_stats,
    );
    
    // ... 后续处理 ...
}
```

#### 6.3.4 数据结构

**ClusterStartNode**（parallel.rs L542-545）：
```rust
#[derive(Debug, Clone, Copy)]
pub struct ClusterStartNode {
    pub cluster_id: u32,
    pub start_node: pg_sys::ItemPointerData,
}

#[repr(C)]
pub struct ClusterStartNodes {
    pub num_clusters: usize,
    pub nodes: [ClusterStartNode; 64],
}
```

**存储位置**：共享内存（DSM）

**作用**：
- 所有 Worker 都可以读写
- 保证同一个 Cluster 只有一个 start node
- 非主 Worker 可以通过检查此结构判断 start node 是否已设置

#### 6.3.5 流程图

```
Worker 0 (Cluster 1, part 1)          Worker 1 (Cluster 1, part 2)
         │                                    │
         ▼                                    ▼
┌─────────────────────┐            ┌─────────────────────┐
│ 从队列获取向量       │            │ 从队列获取向量       │
└─────────────────────┘            └─────────────────────┘
         │                                    │
         ▼                                    ▼
┌─────────────────────┐            ┌─────────────────────┐
│ 检查共享 start node  │            │ 检查共享 start node  │
│ get_start_node(1)   │            │ get_start_node(1)   │
│ 返回: None          │            │ 返回: Some(node)    │
└─────────────────────┘            └─────────────────────┘
         │                                    │
         ▼                                    ▼
┌─────────────────────┐            ┌─────────────────────┐
│ 设置共享 start node  │            │ 从共享内存获取      │
│ set_start_node(...) │            │ 设置本地 meta_page  │
│ 设置本地 meta_page  │            └─────────────────────┘
└─────────────────────┘                         │
         │                                      │
         ▼                                      ▼
┌─────────────────────┐            ┌─────────────────────┐
│ graph.insert(...)   │            │ graph.insert(...)   │
│ (第一个节点成为起点) │            │ (使用已有的 start)  │
└─────────────────────┘            └─────────────────────┘
```

#### 6.3.6 优势

1. **正确性**：确保第一个插入的节点成为 start node
2. **一致性**：所有 Worker 使用同一个 start node
3. **无竞争**：原子操作检查并设置，避免竞争条件
4. **性能**：无需等待主 Worker 完成，非主 Worker 可以立即获取 start node

#### 6.3.7 实现注意事项

1. **原子性**：`get_start_node` 和 `set_start_node` 需要原子操作或使用锁
2. **内存顺序**：使用 `Ordering::Acquire/Release` 保证可见性
3. **错误处理**：处理 start node 设置失败的情况
4. **调试日志**：添加日志记录哪个 Worker 设置了 start node

## 7. 完整实现方案

### 7.1 修改 ClusterStartNodes 结构

**文件**: `src/access_method/build/parallel.rs`

**重要**：`ClusterStartNodes` 存储在**共享内存（DSM）**中，多个进程通过指针访问。`initialized` 必须是 `AtomicBool` 数组，支持跨进程原子操作。

```rust
use std::sync::atomic::{AtomicBool, Ordering};

#[repr(C)]
pub struct ClusterStartNodes {
    pub num_clusters: usize,
    pub nodes: [ClusterStartNode; 64],
    // 关键：必须是 AtomicBool，存储在共享内存中，支持跨进程原子操作
    pub initialized: [AtomicBool; 64],
}

impl ClusterStartNodes {
    /// 在 Leader 进程中调用，初始化共享内存中的结构
    pub fn new(num_clusters: usize) -> Self {
        let mut initialized: [AtomicBool; 64] = unsafe { std::mem::zeroed() };
        for i in 0..64 {
            initialized[i] = AtomicBool::new(false);
        }
        
        Self {
            num_clusters,
            nodes: unsafe { std::mem::zeroed() },
            initialized,
        }
    }

    /// 尝试设置 start node，如果已设置则返回 false
    /// 
    /// # 线程/进程安全
    /// 使用 CAS 操作，保证在多进程环境下只有一个调用者能成功设置
    pub fn try_set_start_node(
        &self,
        cluster_id: usize,
        start_node: pg_sys::ItemPointerData,
    ) -> bool {
        if cluster_id >= self.num_clusters {
            return false;
        }
        
        // CAS 操作：如果 initialized[cluster_id] 为 false，则设为 true
        // AcqRel 语义：Acquire 确保看到之前的写入，Release 确保后续写入可见
        if self.initialized[cluster_id]
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            // CAS 成功，当前进程是设置者
            // 安全：通过裸指针写入，因为 ClusterStartNodes 在共享内存中
            unsafe {
                let node_ptr = &self.nodes[cluster_id] as *const ClusterStartNode
                    as *mut ClusterStartNode;
                (*node_ptr) = ClusterStartNode {
                    cluster_id: cluster_id as u32,
                    start_node,
                };
            }
            true
        } else {
            // CAS 失败，其他进程已经设置
            false
        }
    }

    /// 获取 start node
    /// 
    /// # 线程/进程安全
    /// 使用 Acquire 语义，确保看到其他进程的写入
    pub fn get_start_node(&self, cluster_id: usize) -> Option<pg_sys::ItemPointerData> {
        if cluster_id >= self.num_clusters {
            return None;
        }
        
        // Acquire 语义：确保看到其他进程对 nodes[cluster_id] 的写入
        if self.initialized[cluster_id].load(Ordering::Acquire) {
            Some(self.nodes[cluster_id].start_node)
        } else {
            None
        }
    }
}
```

**关键设计点**：
1. `initialized` 是 `AtomicBool` 数组，不是普通 `bool`
2. `AtomicBool` 存储在共享内存中，多个进程可以原子访问
3. `compare_exchange` 是硬件级别的原子操作，保证多进程安全
4. `AcqRel` 内存顺序确保跨进程的内存可见性
```

### 7.2 修改 Consumer 处理逻辑

**文件**: `src/access_method/build/parallel_build/cluster.rs`

**修改位置**: `process_cluster_vectors` 函数（L1391-1397 附近）

```rust
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
    cluster_start_nodes: *mut ClusterStartNodes,  // 新增参数
) {
    let mut insert_stats = InsertStats::default();
    let mut start_node_set = false;  // 标记当前 Worker 是否设置了 start node

    loop {
        // ... 从队列获取数据的逻辑 ...

        if let Some(entry) = queues.pop_from_queue(base_ptr, cluster_id) {
            // 解析向量数据
            let vector_data = std::slice::from_raw_parts(
                entry.vector_ptr,
                consumer_state._num_dimensions,
            );
            let index_pointer = entry.heap_tid;

            // ===== 关键修改：在 graph insert 之前处理 start node =====
            if !start_node_set {
                // 1. 尝试获取或设置共享 start node
                let shared_start_node = (*cluster_start_nodes).get_start_node(cluster_id);
                
                if shared_start_node.is_none() {
                    // 2. 尝试设置 start node（CAS 操作）
                    let mut item_pointer_data = pg_sys::ItemPointerData::default();
                    index_pointer.to_item_pointer_data(&mut item_pointer_data);
                    
                    if (*cluster_start_nodes).try_set_start_node(cluster_id, item_pointer_data) {
                        // 设置成功，当前 Worker 是设置者
                        start_node_set = true;
                        meta_page.set_start_node(index_pointer);
                        
                        log!(
                            "Worker {} set start node for cluster {}: {:?}",
                            worker_id, cluster_id, index_pointer
                        );
                    } else {
                        // 其他 Worker 已经设置，获取它
                        if let Some(start_node) = (*cluster_start_nodes).get_start_node(cluster_id) {
                            meta_page.set_start_node(ItemPointer::from_item_pointer_data(&start_node));
                            
                            log!(
                                "Worker {} got existing start node for cluster {}: {:?}",
                                worker_id, cluster_id, start_node
                            );
                        }
                    }
                } else {
                    // 3. Start node 已存在，直接使用
                    meta_page.set_start_node(ItemPointer::from_item_pointer_data(
                        &shared_start_node.unwrap()
                    ));
                }
                
                // 标记已处理过 start node，后续节点不需要再检查
                start_node_set = true;
            }
            // ==========================================================

            // 4. 执行 graph insert
            let labeled_vector = LabeledVector::new(PgVector::from_slice(vector_data), None);
            graph.insert(
                index_relation,
                index_pointer,
                labeled_vector,
                storage,
                &mut insert_stats,
            );

            consumer_state.ntuples += 1;

            if consumer_state.ntuples % flush_interval == 0 {
                graph.maybe_flush_neighbor_cache(storage, &mut insert_stats);
            }
        }
        // ... 其他逻辑 ...
    }
}
```

### 7.3 修改 Consumer 主函数

**文件**: `src/access_method/build/parallel_build/cluster.rs`

**修改位置**: `_vectorscale_build_cluster_consumer_main` 函数（移除 L1281-1287 的代码）

```rust
pub extern "C" fn _vectorscale_build_cluster_consumer_main(...) {
    // ... 初始化代码 ...

    // 获取 cluster_start_nodes 指针
    let cluster_start_nodes: *mut ClusterStartNodes = unsafe {
        pg_sys::shm_toc_lookup((*pcxt).toc, SHM_TOC_CLUSTER_START_NODES_KEY, false)
            .cast::<ClusterStartNodes>()
    };

    // ... 其他初始化 ...

    // 处理向量
    process_cluster_vectors(
        &mut consumer_state,
        &index_relation,
        &mut meta_page,
        queues,
        base_ptr,
        cluster_id,
        flush_interval,
        &mut storage,
        &mut graph,
        &mut tape,
        &mut write_stats,
        cluster_start_nodes,  // 传递 cluster_start_nodes
    );

    // ===== 移除原来的 start node 设置代码 =====
    // 原来的代码（L1281-1287）：
    // if is_primary {
    //     if let Some(first_node) = consumer_state.first_node {
    //         ...
    //     }
    // }
    // ==========================================

    // ... 后续清理代码 ...
}
```

### 7.4 关键修改点总结

| 位置 | 原代码 | 修改后 |
|------|--------|--------|
| `parallel.rs` | `ClusterStartNodes` 无原子标志 | 添加 `initialized: [AtomicBool; 64]` |
| `parallel.rs` | `set_start_node` 直接设置 | `try_set_start_node` CAS 操作 |
| `cluster.rs` L1281-1287 | 主 Worker 结束后设置 | **移除** |
| `cluster.rs` L1391-1397 | 只记录 `first_node` | 检查并设置共享 start node |
| `process_cluster_vectors` | 无 `cluster_start_nodes` 参数 | 添加参数并处理 start node |

### 7.5 多进程安全保证

**重要**：Worker 是**进程级别**的（PostgreSQL Parallel Workers），不是线程。因此需要使用**进程间共享内存原子操作**。

#### 7.5.1 共享内存布局

```
┌─────────────────────────────────────────────────────────────────┐
│                    Dynamic Shared Memory (DSM)                   │
├─────────────────────────────────────────────────────────────────┤
│  ClusterStartNodes                                               │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ num_clusters: usize                                          ││
│  │ nodes: [ClusterStartNode; 64]                               ││
│  │ initialized: [AtomicBool; 64]  ← 关键：必须在共享内存中     ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
         ▲                                    ▲
         │                                    │
    Worker 0 进程                        Worker 1 进程
    (通过指针访问)                       (通过指针访问)
```

#### 7.5.2 进程安全实现

**关键**：`AtomicBool` 必须存储在**共享内存**中，这样多个进程可以通过原子操作访问。

```rust
#[repr(C)]
pub struct ClusterStartNodes {
    pub num_clusters: usize,
    pub nodes: [ClusterStartNode; 64],
    pub initialized: [AtomicBool; 64],  // 共享内存中的原子变量
}
```

**进程间 CAS 操作流程**：

```
Worker 0 进程                         Worker 1 进程
    │                                     │
    ▼                                     ▼
┌─────────────────┐               ┌─────────────────┐
│ 读取 initialized[0]              │ 读取 initialized[0]
│ (Acquire 语义)  │               │ (Acquire 语义)
│ 值 = false      │               │ 值 = false
└─────────────────┘               └─────────────────┘
    │                                     │
    ▼                                     ▼
┌─────────────────┐               ┌─────────────────┐
│ CAS 操作:       │               │ CAS 操作:       │
│ false -> true   │               │ false -> true   │
│ 成功！          │               │ 失败！          │
└─────────────────┘               │ (已被 Worker 0) │
    │                             └─────────────────┘
    ▼                                     │
┌─────────────────┐                       │
│ 写入 nodes[0]   │                       │
│ (Release 语义)  │                       │
└─────────────────┘                       │
    │                                     ▼
    │                             ┌─────────────────┐
    │                             │ 重新读取        │
    │                             │ initialized[0]  │
    │                             │ 值 = true       │
    │                             └─────────────────┘
    │                                     │
    ▼                                     ▼
┌─────────────────┐               ┌─────────────────┐
│ 返回 true       │               │ 读取 nodes[0]   │
│ (设置者)        │               │ (Acquire 语义)  │
└─────────────────┘               │ 返回 false      │
                                  │ (获取者)        │
                                  └─────────────────┘
```

#### 7.5.3 内存顺序说明

```rust
// CAS 操作使用 AcqRel 语义
self.initialized[cluster_id]
    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)

// Acquire: 确保看到之前所有对 nodes[cluster_id] 的写入
// Release: 确保后续对 nodes[cluster_id] 的写入对其他进程可见
```

#### 7.5.4 为什么能保证多进程安全？

1. **共享内存**：`ClusterStartNodes` 通过 `shm_toc_allocate` 分配在 DSM 中
2. **原子变量**：`initialized` 数组是 `AtomicBool`，支持跨进程原子操作
3. **内存屏障**：`Acquire/Release` 语义确保内存可见性
4. **CAS 操作**：硬件级别的原子比较交换，保证只有一个进程成功

#### 7.5.5 潜在问题与解决方案

| 问题 | 原因 | 解决方案 |
|------|------|----------|
| 虚假共享 | 多个 AtomicBool 在同一缓存行 | 使用 `#[repr(align(64))]` 或填充 |
| ABA 问题 | 值从 false->true->false | 不需要处理，我们只设置一次 |
| 内存顺序错误 | 使用 Relaxed 导致可见性问题 | 使用 AcqRel 保证同步 |

**优化后的结构**：

```rust
#[repr(C)]
pub struct ClusterStartNodes {
    pub num_clusters: usize,
    // 填充到缓存行边界，避免虚假共享
    _padding: [u8; 56],
    pub nodes: [ClusterStartNode; 64],
    // 每个 initialized 独立在一个缓存行
    pub initialized: [CachePadded<AtomicBool>; 64],
}

// 使用 crossbeam 或自定义的缓存行对齐包装
#[repr(align(64))]
pub struct CachePadded<T>(pub T);
```

### 7.6 优势

1. **正确性**：使用 CAS 确保只有一个 Worker 能设置 start node
2. **及时性**：在第一个节点插入前设置，确保 graph 构建正确
3. **一致性**：所有 Worker 使用同一个共享 start node
4. **无锁**：使用原子操作，无需额外锁
5. **可扩展**：支持任意数量的 Worker 并发

## 8. 性能优化

### 8.1 无锁队列

ClusterQueues 使用原子操作实现无锁队列：

```rust
pub struct ClusterQueueHeader {
    pub head: AtomicUsize,      // 生产者写入位置
    pub tail: AtomicUsize,      // 消费者读取位置
    pub capacity: usize,
    pub finished: AtomicBool,   // 生产者是否完成
}
```

**优势**：
- 无锁设计，减少竞争
- 每个 Cluster 独立队列，避免全局锁
- 使用 ConditionVariable 实现高效等待

### 8.2 批处理

Producer 使用批处理减少队列操作次数：

```rust
pub struct BatchBuffer {
    entries: Vec<(usize, ItemPointer, Vec<f32>)>,  // (cluster_id, tid, vector)
    capacity: usize,
}

// 批量刷新到队列
fn flush_batch(&mut self) {
    for (cluster_id, tid, vector) in self.entries.drain(..) {
        queues.push_to_queue(...);
    }
}
```

### 8.3 动态调整

根据实际 Cluster 大小动态调整 Worker 分配：

```rust
// 预估 vs 实际
let estimated = estimated_cluster_sizes[cluster_id];
let actual = (*cluster_sizes).get(cluster_id);

// 如果差异过大，可以在后续优化中重新分配
```

## 9. 配置参数

### 9.1 相关 GUC 参数

```sql
-- 最大并行 Worker 数量
SET diskann.force_parallel_workers = 8;

-- 队列容量（每个 Cluster）
SET diskann.queue_capacity = 10000;

-- 聚类数量
SET num_clusters = 20;
```

### 9.2 调优建议

**Worker 数量**：
- 建议设置为 CPU 核心数的 50-75%
- 过多 Worker 会增加调度开销
- 过少 Worker 无法充分利用 CPU

**队列容量**：
- 大容量减少 Producer 等待
- 但会增加内存使用
- 建议：队列容量 >= 总向量数 / Cluster 数量 * 2

## 10. 调试与监控

### 10.1 日志输出

构建时会输出详细的分配信息：

```
NOTICE:  Cluster distribution:
NOTICE:    Cluster 0: 5000 vectors
NOTICE:    Cluster 1: 15000 vectors  ← 最大
NOTICE:    Cluster 2: 8000 vectors
NOTICE:    Cluster 3: 2000 vectors

LOG:  Worker assignments:
LOG:    Worker 0: cluster 0 [0..5000), primary=true
LOG:    Worker 1: cluster 1 [0..5000), primary=true
LOG:    Worker 2: cluster 1 [5000..10000), primary=false
LOG:    Worker 3: cluster 1 [10000..15000), primary=false
LOG:    Worker 4: cluster 2 [0..8000), primary=true
LOG:    Worker 5: cluster 3 [0..2000), primary=true
```

### 9.2 性能指标

监控以下指标：
- 每个 Worker 处理的向量数量
- 队列等待时间
- 负载均衡程度（各 Worker 处理数量的标准差）

## 11. 总结

动态 Worker 分配方案通过以下方式实现高效并行构建：

1. **比例分配**：根据 Cluster 大小动态分配 Worker 数量
2. **范围分割**：每个 Worker 处理 Cluster 的连续索引范围
3. **无锁队列**：每个 Cluster 独立队列，减少竞争
4. **Start Node 共享机制**：使用 CAS 操作确保同一个 Cluster 的所有 Worker 使用同一个 start node

### 关键改进点

**Start Node 设置机制**是整个设计的核心：
- **问题**：原实现在主 Worker 结束后才设置 start node，时机太晚
- **解决方案**：在 graph insert 之前使用 CAS 操作检查和设置
- **优势**：确保第一个节点成为 start node，所有 Worker 共享同一个 start node

### 线程安全保证

- 使用 `AtomicBool` 数组记录每个 Cluster 的初始化状态
- 使用 `compare_exchange` CAS 操作确保只有一个 Worker 能设置成功
- 使用 `Acquire/Release` 内存顺序保证可见性

这种设计有效解决了大 Cluster 成为瓶颈的问题，实现了负载均衡，同时保证了同一个 Cluster 多 Worker 并发构建的正确性。
