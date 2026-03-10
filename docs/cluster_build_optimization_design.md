# Cluster 并行构建性能优化设计方案

## 一、问题分析

### 1.1 当前架构概述

当前系统采用生产者-消费者模式进行并行构建：

- **生产者（Leader进程）**：扫描 heap 表，通过 k-means 将向量分配到不同的 cluster
- **消费者（Worker进程）**：每个 worker 负责一个或多个 cluster 的图构建
- **共享队列**：每个 cluster 有一个环形队列，存储待处理的向量数据

### 1.2 核心性能问题

#### 问题1：生产者阻塞导致整体性能下降

**代码位置**：[parallel.rs:417-440](file:///home/zhangqiang/code/postgres/contrib/pgvectorscale/pgvectorscale/src/access_method/build/parallel.rs#L417-L440)

```rust
pub unsafe fn push_batch_to_queue(...) -> usize {
    for (heap_tid, vector) in entries {
        loop {
            let tail = (*header).tail.load(Ordering::Acquire);
            let head = (*header).head.load(Ordering::Acquire);
            let next_tail = (tail + 1) % (*header).capacity;

            if next_tail != head {
                // 队列未满，写入数据
                ...
                break;
            } else {
                // 队列满，阻塞等待
                pg_sys::ConditionVariableSleep(cv, PG_WAIT_EXTENSION);
            }
        }
    }
}
```

**问题描述**：
- 当某个 cluster 的队列满时，生产者会阻塞在 `ConditionVariableSleep`
- 即使其他 cluster 的队列有空闲空间，生产者也无法继续处理
- 这导致"队头阻塞"问题：一个慢消费者拖慢整个生产流程

**实际影响**（来自测试数据）：
- 2 个 cluster、8 个 worker：只处理了 1.7% 的数据
- 某些 worker（如 Worker 3, 4, 5）完全没有处理任何向量

#### 问题2：批量推送效率低下

**代码位置**：[parallel.rs:417-440](file:///home/zhangqiang/code/postgres/contrib/pgvectorscale/pgvectorscale/src/access_method/build/parallel.rs#L417-L440)

虽然 `push_batch_to_queue` 接收批量数据，但内部实现是逐条推送：

```rust
for (heap_tid, vector) in entries {
    loop {
        // 每条数据都要检查队列状态
        // 可能多次进入等待状态
    }
}
```

**问题描述**：
- 批量推送时，如果队列接近满，可能多次进入等待状态
- 没有充分利用批量操作的优势
- 循环内的原子操作和条件判断开销较大

#### 问题3：条件变量开销

**代码位置**：[parallel.rs:437-440](file:///home/zhangqiang/code/postgres/contrib/pgvectorscale/pgvectorscale/src/access_method/build/parallel.rs#L437-L440)

```rust
pg_sys::ConditionVariableSleep(cv, PG_WAIT_EXTENSION);
```

**问题描述**：
- 条件变量需要内核参与，上下文切换开销大
- 多进程环境下的同步机制复杂
- PostgreSQL 的 ConditionVariable 实现涉及信号处理，开销较高

### 1.3 问题根因

```
┌─────────────────────────────────────────────────────────────┐
│                    生产者扫描流程                              │
├─────────────────────────────────────────────────────────────┤
│  扫描 Heap 表                                                 │
│      ↓                                                       │
│  K-means 分类（cluster_id）                                    │
│      ↓                                                       │
│  本地 BatchBuffer 缓存（BATCH_SIZE=100）                       │
│      ↓                                                       │
│  flush_batch() → push_batch_to_queue()                       │
│      ↓                                                       │
│  ┌──────────────────────────────────────┐                   │
│  │  某个 cluster 队列满？                  │                   │
│  │      ↓ Yes                            │                   │
│  │  ConditionVariableSleep() ← 阻塞！     │                   │
│  │      ↓                                 │                   │
│  │  其他 cluster 队列即使空闲也无法处理     │                   │
│  └──────────────────────────────────────┘                   │
└─────────────────────────────────────────────────────────────┘
```

**关键问题**：
1. 生产者和消费者强耦合：生产者的速度受限于最慢的消费者
2. 没有背压机制：队列满时直接阻塞，而不是提供缓冲
3. 批量操作未优化：批量推送变成了多次单条推送

## 二、优化方案设计

### 2.1 设计原则

1. **解耦生产者和消费者**：生产者不应被单个消费者的速度阻塞
2. **引入内存缓冲**：当共享队列满时，使用进程私有内存暂存
3. **消除条件变量**：使用无锁设计，减少同步开销
4. **保持内存可控**：限制内存缓冲区大小，防止 OOM
5. **优化批量操作**：真正的批量推送和批量获取

### 2.2 核心设计

#### 2.2.1 三级存储架构

```
┌─────────────────────────────────────────────────────────────┐
│                      生产者进程                               │
├─────────────────────────────────────────────────────────────┤
│  Level 1: BatchBuffer（本地缓存）                             │
│  - 大小：BATCH_SIZE = 100 条                                  │
│  - 作用：聚合同一 cluster 的向量，减少推送次数                   │
│  - 内存：进程私有，无锁                                        │
└─────────────────────────────────────────────────────────────┘
                          ↓ flush_batch()
┌─────────────────────────────────────────────────────────────┐
│                Level 2: 共享队列（Shared Memory）              │
│  - 大小：基于 cluster 数据量动态计算                            │
│  - 作用：生产者-消费者通信桥梁                                  │
│  - 内存：共享内存，无锁环形队列                                 │
│  - 特点：无阻塞写入，满则转 Level 3                             │
└─────────────────────────────────────────────────────────────┘
                          ↓ 队列满时
┌─────────────────────────────────────────────────────────────┐
│              Level 3: 内存溢出缓冲区（Private Memory）          │
│  - 大小：不超过对应 cluster 共享队列容量的一半                   │
│  - 作用：暂存无法写入共享队列的数据                             │
│  - 内存：进程私有，链表结构                                     │
│  - 特点：FIFO，批量移动到共享队列                               │
└─────────────────────────────────────────────────────────────┘
```

#### 2.2.2 无锁队列设计

**核心思想**：
- 生产者只管写入，不等待
- 消费者只管读取，不等待
- 通过原子标志位协调状态

**数据结构**：

```rust
#[repr(C)]
pub struct ClusterQueueHeader {
    pub head: AtomicUsize,        // 消费者读取位置
    pub tail: AtomicUsize,        // 生产者写入位置
    pub capacity: usize,          // 队列容量
    pub element_size: usize,      // 每个元素大小
    pub finished: AtomicBool,     // 生产者是否完成
    pub overflow_count: AtomicUsize, // 溢出缓冲区中的数据量
}
```

**关键操作**：

1. **生产者写入（无阻塞）**：
```rust
unsafe fn try_push_to_queue(...) -> bool {
    let tail = tail.load(Acquire);
    let head = head.load(Acquire);
    let next_tail = (tail + 1) % capacity;
    
    if next_tail != head {
        // 有空间，写入
        write_entry(tail, data);
        tail.store(next_tail, Release);
        return true;
    } else {
        // 无空间，返回 false（不阻塞）
        return false;
    }
}
```

2. **消费者读取（无阻塞）**：
```rust
unsafe fn try_pop_from_queue(...) -> Option<Entry> {
    let head = head.load(Acquire);
    let tail = tail.load(Acquire);
    
    if head != tail {
        // 有数据，读取
        let entry = read_entry(head);
        let next_head = (head + 1) % capacity;
        head.store(next_head, Release);
        return Some(entry);
    } else {
        // 无数据，返回 None
        return None;
    }
}
```

#### 2.2.3 内存溢出缓冲区设计

**数据结构**：

```rust
pub struct OverflowBuffer {
    buffers: Vec<Vec<OverflowEntry>>,
    total_size: AtomicUsize,
    max_size: usize,  // 限制为对应共享队列容量的一半
}

struct OverflowEntry {
    heap_tid: pg_sys::ItemPointerData,
    vector: Vec<f32>,
}
```

**关键特性**：
1. **容量限制**：每个 cluster 的溢出缓冲区大小 ≤ 共享队列容量 / 2
2. **FIFO 保证**：使用 Vec 作为队列，保证顺序
3. **批量移动**：消费者可以将溢出数据批量移动到共享队列

**生产者流程**：

```rust
unsafe fn push_with_overflow(
    cluster_id: usize,
    entries: &[(ItemPointerData, &[f32])],
) -> usize {
    let mut pushed = 0;
    
    for entry in entries {
        // 尝试写入共享队列
        if try_push_to_queue(cluster_id, entry) {
            pushed += 1;
        } else {
            // 共享队列满，写入溢出缓冲区
            if overflow_buffer.can_push(cluster_id) {
                overflow_buffer.push(cluster_id, entry);
                pushed += 1;
            } else {
                // 溢出缓冲区也满了，返回已推送数量
                // 生产者可以稍后重试或等待
                break;
            }
        }
    }
    
    pushed
}
```

#### 2.2.4 消费者协作机制

**核心思想**：消费者在发现共享队列空时，检查并移动溢出数据

**消费者流程**：

```rust
unsafe fn pop_batch_smart(
    cluster_id: usize,
    max_count: usize,
) -> Vec<Entry> {
    let mut result = Vec::new();
    
    // 1. 从共享队列读取
    while result.len() < max_count {
        match try_pop_from_queue(cluster_id) {
            Some(entry) => result.push(entry),
            None => break,
        }
    }
    
    // 2. 如果共享队列空了，检查溢出缓冲区
    if result.is_empty() && !is_producer_finished(cluster_id) {
        // 尝试从溢出缓冲区移动数据到共享队列
        let moved = move_overflow_to_queue(cluster_id, max_count);
        if moved > 0 {
            // 移动成功，再次尝试读取
            while result.len() < max_count {
                match try_pop_from_queue(cluster_id) {
                    Some(entry) => result.push(entry),
                    None => break,
                }
            }
        }
    }
    
    result
}
```

### 2.3 详细设计

#### 2.3.1 生产者状态结构

```rust
struct ProducerStateV2<'a> {
    parallel_shared: *mut ParallelShared,
    cluster_queues: *mut ClusterQueues,
    cluster_sizes: *mut ClusterSizes,
    centroids: &'a [Vec<f32>],
    ntuples: usize,
    meta_page: MetaPage,
    batch_buffer: BatchBuffer,
    overflow_buffers: Vec<OverflowBuffer>,  // 每个 cluster 一个
    queue_capacities: Vec<usize>,           // 每个 cluster 的队列容量
}

struct OverflowBuffer {
    entries: Vec<OverflowEntry>,
    current_size: usize,
    max_size: usize,  // = queue_capacity / 2
}

struct OverflowEntry {
    heap_tid: pg_sys::ItemPointerData,
    vector: Vec<f32>,
}
```

#### 2.3.2 生产者推送逻辑

```rust
impl ProducerStateV2<'_> {
    unsafe fn flush_batch(&mut self) {
        if self.batch_buffer.entries.is_empty() {
            return;
        }
        
        let cluster_id = self.batch_buffer.cluster_id;
        let entries: Vec<_> = self.batch_buffer.entries
            .iter()
            .map(|(tid, vec)| (*tid, vec.as_slice()))
            .collect();
        
        let pushed = self.push_batch_smart(cluster_id, &entries);
        self.ntuples += pushed;
        self.batch_buffer.clear();
    }
    
    unsafe fn push_batch_smart(
        &mut self,
        cluster_id: usize,
        entries: &[(pg_sys::ItemPointerData, &[f32])],
    ) -> usize {
        let queues = &*self.cluster_queues;
        let base_ptr = self.cluster_queues as *mut u8;
        let mut pushed = 0;
        
        for (heap_tid, vector) in entries {
            // 跳过无效数据
            if !is_valid_entry(*heap_tid) {
                continue;
            }
            
            // 尝试写入共享队列（无阻塞）
            if queues.try_push_one(base_ptr, cluster_id, *heap_tid, vector) {
                pushed += 1;
            } else {
                // 共享队列满，写入溢出缓冲区
                let overflow = &mut self.overflow_buffers[cluster_id];
                if overflow.can_push() {
                    overflow.push(*heap_tid, vector);
                    pushed += 1;
                } else {
                    // 溢出缓冲区也满了，需要等待
                    // 这里可以选择：
                    // 1. 短暂等待后重试
                    // 2. 返回已推送数量
                    // 我们选择等待，但使用自旋而非条件变量
                    self.wait_and_retry_push(cluster_id, *heap_tid, vector);
                    pushed += 1;
                }
            }
        }
        
        pushed
    }
    
    unsafe fn wait_and_retry_push(
        &mut self,
        cluster_id: usize,
        heap_tid: pg_sys::ItemPointerData,
        vector: &[f32],
    ) {
        let queues = &*self.cluster_queues;
        let base_ptr = self.cluster_queues as *mut u8;
        
        // 自旋等待，避免条件变量开销
        loop {
            // 先尝试写入共享队列
            if queues.try_push_one(base_ptr, cluster_id, heap_tid, vector) {
                return;
            }
            
            // 检查溢出缓冲区是否有空间
            let overflow = &mut self.overflow_buffers[cluster_id];
            if overflow.can_push() {
                overflow.push(heap_tid, vector);
                return;
            }
            
            // 短暂让出 CPU
            std::hint::spin_loop();
            
            // 检查是否需要中断
            check_for_interrupts!();
        }
    }
}
```

#### 2.3.3 消费者读取逻辑

```rust
unsafe fn process_cluster_vectors_v2<S: Storage>(
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
    cluster_start_nodes: *mut ClusterStartNodes,
) {
    let mut batch_heap_tids: Vec<pg_sys::ItemPointerData> = vec![std::mem::zeroed(); MAX_BATCH_SIZE];
    let mut batch_vectors: Vec<Vec<f32>> = (0..MAX_BATCH_SIZE)
        .map(|_| vec![0.0f32; num_dimensions])
        .collect();
    
    let mut consecutive_empty = 0;
    const MAX_EMPTY_CHECKS: usize = 100;
    
    loop {
        // 智能批量读取
        let batch_count = calculate_batch_size(queues, base_ptr, cluster_id, workers_per_cluster);
        
        if batch_count > 0 {
            consecutive_empty = 0;
            
            let popped = queues.pop_batch_from_queue(
                base_ptr,
                cluster_id,
                batch_count,
                &mut batch_heap_tids,
                &mut batch_vectors,
            );
            
            // 处理数据
            for i in 0..popped {
                process_vector(
                    batch_heap_tids[i],
                    &batch_vectors[i],
                    storage,
                    graph,
                    tape,
                    write_stats,
                );
            }
        } else {
            // 队列空，检查是否完成
            if queues.is_queue_finished(base_ptr, cluster_id) {
                break;
            }
            
            consecutive_empty += 1;
            
            // 连续多次检查为空，可能需要等待
            if consecutive_empty >= MAX_EMPTY_CHECKS {
                // 使用指数退避
                let delay = std::cmp::min(1000, 10 * (1 << consecutive_empty / 10));
                std::thread::sleep(std::time::Duration::from_micros(delay));
            } else {
                // 短暂让出 CPU
                std::hint::spin_loop();
            }
        }
        
        check_for_interrupts!();
    }
}
```

#### 2.3.4 ClusterQueues 新增方法

```rust
impl ClusterQueues {
    /// 尝试推送一条数据（无阻塞）
    /// 返回 true 表示成功，false 表示队列满
    pub unsafe fn try_push_one(
        &self,
        base_ptr: *mut u8,
        cluster_id: usize,
        heap_tid: pg_sys::ItemPointerData,
        vector: &[f32],
    ) -> bool {
        let header = self.get_header(base_ptr, cluster_id);
        
        // 使用 CAS 操作尝试获取槽位
        loop {
            let tail = (*header).tail.load(Ordering::Acquire);
            let head = (*header).head.load(Ordering::Acquire);
            let next_tail = (tail + 1) % (*header).capacity;
            
            if next_tail == head {
                // 队列满
                return false;
            }
            
            // CAS 更新 tail
            match (*header).tail.compare_exchange_weak(
                tail,
                next_tail,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // 成功获取槽位，写入数据
                    let entry_ptr = self.get_entry(base_ptr, cluster_id, tail);
                    (*entry_ptr).heap_tid = heap_tid;
                    (*entry_ptr).vector_len = vector.len() as u32;
                    
                    let vector_ptr = (entry_ptr as *mut u8)
                        .add(std::mem::size_of::<ClusterQueueEntry>())
                        as *mut f32;
                    std::ptr::copy_nonoverlapping(vector.as_ptr(), vector_ptr, vector.len());
                    
                    return true;
                }
                Err(_) => {
                    // CAS 失败，重试
                    continue;
                }
            }
        }
    }
    
    /// 批量尝试推送（无阻塞）
    /// 返回成功推送的数量
    pub unsafe fn try_push_batch(
        &self,
        base_ptr: *mut u8,
        cluster_id: usize,
        entries: &[(pg_sys::ItemPointerData, &[f32])],
    ) -> usize {
        let mut pushed = 0;
        
        for (heap_tid, vector) in entries {
            if self.try_push_one(base_ptr, cluster_id, *heap_tid, vector) {
                pushed += 1;
            } else {
                // 队列满，停止推送
                break;
            }
        }
        
        pushed
    }
    
    /// 批量尝试推送（带溢出计数）
    /// 返回 (成功推送数量, 剩余数量)
    pub unsafe fn try_push_batch_with_count(
        &self,
        base_ptr: *mut u8,
        cluster_id: usize,
        entries: &[(pg_sys::ItemPointerData, &[f32])],
    ) -> (usize, usize) {
        let pushed = self.try_push_batch(base_ptr, cluster_id, entries);
        let remaining = entries.len() - pushed;
        (pushed, remaining)
    }
}
```

### 2.4 内存控制策略

#### 2.4.1 内存使用计算

```rust
fn calculate_memory_limits(
    num_clusters: usize,
    total_vectors: usize,
    num_dimensions: usize,
    vector_size: usize,
) -> Vec<MemoryLimit> {
    let entry_size = std::mem::size_of::<ClusterQueueEntry>() 
                   + num_dimensions * std::mem::size_of::<f32>();
    
    let mut limits = Vec::with_capacity(num_clusters);
    
    for cluster_id in 0..num_clusters {
        let estimated_size = estimate_cluster_size(cluster_id, total_vectors);
        let queue_capacity = calculate_queue_capacity(estimated_size);
        
        limits.push(MemoryLimit {
            cluster_id,
            queue_capacity,
            queue_memory: queue_capacity * entry_size,
            overflow_limit: (queue_capacity / 2) * entry_size,  // 共享队列容量的一半
        });
    }
    
    limits
}
```

#### 2.4.2 溢出缓冲区管理

```rust
impl OverflowBuffer {
    fn new(max_size: usize) -> Self {
        Self {
            entries: Vec::new(),
            current_size: 0,
            max_size,
        }
    }
    
    fn can_push(&self) -> bool {
        self.current_size < self.max_size
    }
    
    fn push(&mut self, heap_tid: pg_sys::ItemPointerData, vector: &[f32]) {
        let entry_size = std::mem::size_of::<OverflowEntry>() 
                       + vector.len() * std::mem::size_of::<f32>();
        
        self.entries.push(OverflowEntry {
            heap_tid,
            vector: vector.to_vec(),
        });
        self.current_size += entry_size;
    }
    
    fn pop_batch(&mut self, max_count: usize) -> Vec<OverflowEntry> {
        let count = max_count.min(self.entries.len());
        let drained: Vec<_> = self.entries.drain(..count).collect();
        
        for entry in &drained {
            let entry_size = std::mem::size_of::<OverflowEntry>() 
                           + entry.vector.len() * std::mem::size_of::<f32>();
            self.current_size -= entry_size;
        }
        
        drained
    }
    
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
    fn len(&self) -> usize {
        self.entries.len()
    }
}
```

### 2.5 性能对比分析

#### 2.5.1 理论分析

| 指标 | 当前方案 | 优化方案 | 提升 |
|------|----------|----------|------|
| 生产者阻塞 | 队列满时阻塞 | 无阻塞（使用溢出缓冲） | ✅ 消除阻塞 |
| 条件变量调用 | 每次 push/pop | 完全消除 | ✅ 减少内核开销 |
| 批量操作效率 | 逐条推送 | 真正批量推送 | ✅ 减少循环开销 |
| 内存使用 | 固定共享内存 | 共享内存 + 动态私有内存 | ⚠️ 略有增加 |
| 数据丢失风险 | 高（阻塞导致） | 低（缓冲机制） | ✅ 提高可靠性 |

#### 2.5.2 预期性能提升

**场景1：2 个 cluster、8 个 worker**
- 当前：处理 1.7% 数据
- 优化后：预期处理 >95% 数据
- 提升：约 55 倍

**场景2：6 个 cluster、8 个 worker**
- 当前：处理 80% 数据
- 优化后：预期处理 >99% 数据
- 提升：约 1.2 倍

**关键改进点**：
1. 生产者不再被单个慢消费者阻塞
2. 批量操作减少循环和原子操作开销
3. 无锁设计减少上下文切换

### 2.6 实现计划

#### Phase 1：无锁队列基础
1. 实现 `try_push_one` 和 `try_push_batch` 方法
2. 修改 `pop_batch_from_queue` 使用 CAS 操作
3. 移除条件变量调用

#### Phase 2：溢出缓冲区
1. 实现 `OverflowBuffer` 结构
2. 在 `ProducerState` 中添加溢出缓冲区
3. 实现智能推送逻辑

#### Phase 3：消费者优化
1. 实现指数退避等待策略
2. 优化批量读取逻辑
3. 添加性能监控指标

#### Phase 4：测试和调优
1. 单元测试
2. 性能基准测试
3. 内存使用监控
4. 参数调优

## 三、代码实现

### 3.1 新增数据结构

```rust
// 文件：parallel.rs

/// 溢出缓冲区条目
#[derive(Clone)]
pub struct OverflowEntry {
    pub heap_tid: pg_sys::ItemPointerData,
    pub vector: Vec<f32>,
}

/// 溢出缓冲区
pub struct OverflowBuffer {
    entries: Vec<OverflowEntry>,
    current_size: usize,
    max_size: usize,
}

impl OverflowBuffer {
    pub fn new(max_size: usize) -> Self {
        Self {
            entries: Vec::new(),
            current_size: 0,
            max_size,
        }
    }
    
    pub fn can_push(&self) -> bool {
        self.current_size < self.max_size
    }
    
    pub fn push(&mut self, heap_tid: pg_sys::ItemPointerData, vector: &[f32]) {
        let entry_size = std::mem::size_of::<OverflowEntry>() 
                       + vector.len() * std::mem::size_of::<f32>();
        
        self.entries.push(OverflowEntry {
            heap_tid,
            vector: vector.to_vec(),
        });
        self.current_size += entry_size;
    }
    
    pub fn pop_batch(&mut self, max_count: usize) -> Vec<OverflowEntry> {
        let count = max_count.min(self.entries.len());
        if count == 0 {
            return Vec::new();
        }
        
        let drained: Vec<_> = self.entries.drain(..count).collect();
        
        for entry in &drained {
            let entry_size = std::mem::size_of::<OverflowEntry>() 
                           + entry.vector.len() * std::mem::size_of::<f32>();
            self.current_size = self.current_size.saturating_sub(entry_size);
        }
        
        drained
    }
    
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    
    pub fn size(&self) -> usize {
        self.current_size
    }
}

/// 生产者溢出缓冲区管理器
pub struct ProducerOverflowManager {
    buffers: Vec<OverflowBuffer>,
    total_limit: usize,
    current_total: AtomicUsize,
}

impl ProducerOverflowManager {
    pub fn new(num_clusters: usize, queue_capacities: &[usize], entry_size: usize) -> Self {
        let buffers: Vec<OverflowBuffer> = queue_capacities
            .iter()
            .map(|&capacity| {
                let max_overflow = (capacity / 2) * entry_size;
                OverflowBuffer::new(max_overflow)
            })
            .collect();
        
        let total_limit: usize = queue_capacities
            .iter()
            .map(|&cap| (cap / 2) * entry_size)
            .sum();
        
        Self {
            buffers,
            total_limit,
            current_total: AtomicUsize::new(0),
        }
    }
    
    pub fn can_push(&self, cluster_id: usize) -> bool {
        if cluster_id >= self.buffers.len() {
            return false;
        }
        self.buffers[cluster_id].can_push()
    }
    
    pub fn push(&mut self, cluster_id: usize, heap_tid: pg_sys::ItemPointerData, vector: &[f32]) {
        if cluster_id >= self.buffers.len() {
            return;
        }
        
        let entry_size = std::mem::size_of::<OverflowEntry>() 
                       + vector.len() * std::mem::size_of::<f32>();
        
        self.buffers[cluster_id].push(heap_tid, vector);
        self.current_total.fetch_add(entry_size, Ordering::Release);
    }
    
    pub fn pop_batch(&mut self, cluster_id: usize, max_count: usize) -> Vec<OverflowEntry> {
        if cluster_id >= self.buffers.len() {
            return Vec::new();
        }
        
        let entries = self.buffers[cluster_id].pop_batch(max_count);
        
        for entry in &entries {
            let entry_size = std::mem::size_of::<OverflowEntry>() 
                           + entry.vector.len() * std::mem::size_of::<f32>();
            self.current_total.fetch_sub(entry_size, Ordering::Release);
        }
        
        entries
    }
    
    pub fn is_empty(&self, cluster_id: usize) -> bool {
        if cluster_id >= self.buffers.len() {
            return true;
        }
        self.buffers[cluster_id].is_empty()
    }
    
    pub fn len(&self, cluster_id: usize) -> usize {
        if cluster_id >= self.buffers.len() {
            return 0;
        }
        self.buffers[cluster_id].len()
    }
}
```

### 3.2 ClusterQueues 扩展

```rust
// 文件：parallel.rs

impl ClusterQueues {
    /// 尝试推送一条数据（无阻塞，使用 CAS）
    pub unsafe fn try_push_one(
        &self,
        base_ptr: *mut u8,
        cluster_id: usize,
        heap_tid: pg_sys::ItemPointerData,
        vector: &[f32],
    ) -> bool {
        if cluster_id >= self.num_queues {
            return false;
        }
        
        let header = self.get_header(base_ptr, cluster_id);
        
        loop {
            let tail = (*header).tail.load(Ordering::Acquire);
            let head = (*header).head.load(Ordering::Acquire);
            let next_tail = (tail + 1) % (*header).capacity;
            
            if next_tail == head {
                return false;
            }
            
            match (*header).tail.compare_exchange_weak(
                tail,
                next_tail,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let entry_ptr = self.get_entry(base_ptr, cluster_id, tail);
                    (*entry_ptr).heap_tid = heap_tid;
                    (*entry_ptr).vector_len = vector.len() as u32;
                    
                    let vector_ptr = (entry_ptr as *mut u8)
                        .add(std::mem::size_of::<ClusterQueueEntry>())
                        as *mut f32;
                    std::ptr::copy_nonoverlapping(vector.as_ptr(), vector_ptr, vector.len());
                    
                    return true;
                }
                Err(_) => continue,
            }
        }
    }
    
    /// 批量尝试推送（无阻塞）
    pub unsafe fn try_push_batch(
        &self,
        base_ptr: *mut u8,
        cluster_id: usize,
        entries: &[(pg_sys::ItemPointerData, &[f32])],
    ) -> usize {
        let mut pushed = 0;
        
        for (heap_tid, vector) in entries {
            if self.try_push_one(base_ptr, cluster_id, *heap_tid, vector) {
                pushed += 1;
            } else {
                break;
            }
        }
        
        pushed
    }
    
    /// 获取队列可用空间
    pub unsafe fn available_space(&self, base_ptr: *mut u8, cluster_id: usize) -> usize {
        let header = self.get_header(base_ptr, cluster_id);
        let head = (*header).head.load(Ordering::Acquire);
        let tail = (*header).tail.load(Ordering::Acquire);
        let capacity = (*header).capacity;
        
        if tail >= head {
            capacity - (tail - head) - 1
        } else {
            head - tail - 1
        }
    }
}
```

### 3.3 生产者实现

```rust
// 文件：parallel_build/cluster.rs

struct ProducerStateV2<'a> {
    parallel_shared: *mut ParallelShared,
    cluster_queues: *mut ClusterQueues,
    cluster_sizes: *mut ClusterSizes,
    centroids: &'a [Vec<f32>],
    ntuples: usize,
    meta_page: MetaPage,
    batch_buffer: BatchBuffer,
    overflow_manager: ProducerOverflowManager,
    entry_size: usize,
}

impl ProducerStateV2<'_> {
    unsafe fn flush_batch(&mut self) {
        if self.batch_buffer.entries.is_empty() {
            return;
        }
        
        if self.cluster_queues.is_null() {
            warning!("ProducerStateV2::flush_batch: cluster_queues is null");
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
        
        let pushed = self.push_batch_smart(base_ptr, queues, cluster_id, &entries);
        self.ntuples += pushed;
        self.batch_buffer.clear();
    }
    
    unsafe fn push_batch_smart(
        &mut self,
        base_ptr: *mut u8,
        queues: &ClusterQueues,
        cluster_id: usize,
        entries: &[(pg_sys::ItemPointerData, &[f32])],
    ) -> usize {
        let mut pushed = 0;
        
        for (heap_tid, vector) in entries {
            if !is_valid_entry(*heap_tid) {
                continue;
            }
            
            if queues.try_push_one(base_ptr, cluster_id, *heap_tid, vector) {
                pushed += 1;
            } else if self.overflow_manager.can_push(cluster_id) {
                self.overflow_manager.push(cluster_id, *heap_tid, vector);
                pushed += 1;
            } else {
                pushed += self.wait_and_retry_push(base_ptr, queues, cluster_id, *heap_tid, vector);
            }
        }
        
        pushed
    }
    
    unsafe fn wait_and_retry_push(
        &mut self,
        base_ptr: *mut u8,
        queues: &ClusterQueues,
        cluster_id: usize,
        heap_tid: pg_sys::ItemPointerData,
        vector: &[f32],
    ) -> usize {
        let mut attempts = 0;
        const MAX_SPIN_ATTEMPTS: usize = 1000;
        
        loop {
            if queues.try_push_one(base_ptr, cluster_id, heap_tid, vector) {
                return 1;
            }
            
            if self.overflow_manager.can_push(cluster_id) {
                self.overflow_manager.push(cluster_id, heap_tid, vector);
                return 1;
            }
            
            attempts += 1;
            
            if attempts < MAX_SPIN_ATTEMPTS {
                std::hint::spin_loop();
            } else {
                std::thread::sleep(std::time::Duration::from_micros(100));
                attempts = 0;
            }
            
            check_for_interrupts!();
        }
    }
}

unsafe fn is_valid_entry(heap_tid: pg_sys::ItemPointerData) -> bool {
    if heap_tid.ip_posid == 0 || heap_tid.ip_posid == pg_sys::InvalidOffsetNumber {
        return false;
    }
    
    let block_num = pgrx::itemptr::item_pointer_get_block_number_no_check(heap_tid);
    block_num != pg_sys::InvalidBlockNumber
}
```

### 3.4 消费者实现

```rust
// 文件：parallel_build/cluster.rs

unsafe fn process_cluster_vectors_v2<S: Storage>(
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
    cluster_start_nodes: *mut ClusterStartNodes,
) {
    let num_dimensions = meta_page.get_num_dimensions_to_index() as usize;
    let mut batch_heap_tids: Vec<pg_sys::ItemPointerData> = vec![std::mem::zeroed(); MAX_BATCH_SIZE];
    let mut batch_vectors: Vec<Vec<f32>> = (0..MAX_BATCH_SIZE)
        .map(|_| vec![0.0f32; num_dimensions])
        .collect();
    
    let mut consecutive_empty = 0;
    let mut backoff_delay_us: u64 = 10;
    const MAX_BACKOFF_US: u64 = 10000;
    
    loop {
        let available = queues.queue_size(base_ptr, cluster_id);
        let target_batch_size = calculate_batch_size(available, consumer_state.workers_per_cluster);
        
        if target_batch_size > 0 {
            consecutive_empty = 0;
            backoff_delay_us = 10;
            
            let popped = queues.pop_batch_from_queue(
                base_ptr,
                cluster_id,
                target_batch_size,
                &mut batch_heap_tids,
                &mut batch_vectors,
            );
            
            for i in 0..popped {
                process_vector(
                    batch_heap_tids[i],
                    &batch_vectors[i],
                    storage,
                    graph,
                    tape,
                    write_stats,
                    cluster_start_nodes,
                    consumer_state,
                );
            }
        } else {
            if queues.is_queue_finished(base_ptr, cluster_id) {
                break;
            }
            
            consecutive_empty += 1;
            
            if consecutive_empty > 10 {
                std::thread::sleep(std::time::Duration::from_micros(backoff_delay_us));
                backoff_delay_us = (backoff_delay_us * 2).min(MAX_BACKOFF_US);
            } else {
                std::hint::spin_loop();
            }
        }
        
        check_for_interrupts!();
    }
}

fn calculate_batch_size(available: usize, workers_per_cluster: usize) -> usize {
    if available == 0 || workers_per_cluster == 0 {
        return 0;
    }
    
    let fair_share = available / workers_per_cluster;
    fair_share.max(MIN_BATCH_SIZE).min(MAX_BATCH_SIZE)
}
```

## 四、数据丢失根本原因分析

### 4.1 数据丢失现象

根据测试数据，在 2 个 cluster、8 个 worker 的场景下：
- **预期处理**：1,000,000 个向量
- **实际处理**：17,384 个向量（仅占 1.7%）
- **数据丢失**：982,616 个向量（丢失率 98.3%）

### 4.2 数据丢失的根本原因

#### 4.2.1 生产者过早退出（主要原因）

**问题分析**：

```
当前流程：
┌─────────────────────────────────────────────────────────────┐
│  Leader 进程（生产者）                                        │
├─────────────────────────────────────────────────────────────┤
│  1. IndexBuildHeapScan() 开始扫描 heap 表                     │
│     ↓                                                        │
│  2. 对每个向量调用 producer_callback()                        │
│     ↓                                                        │
│  3. 通过 k-means 确定 cluster_id                              │
│     ↓                                                        │
│  4. push_with_batch() → flush_batch()                        │
│     ↓                                                        │
│  5. push_batch_to_queue() 写入共享队列                        │
│     ↓                                                        │
│  6. 如果队列满 → ConditionVariableSleep() 阻塞               │
│     ↓                                                        │
│  7. IndexBuildHeapScan() 返回，生产者认为"完成"               │
│     ↓                                                        │
│  8. 标记队列 finished = true                                  │
│     ↓                                                        │
│  9. 等待消费者完成...                                         │
└─────────────────────────────────────────────────────────────┘
```

**关键问题**：

1. **队列满时生产者阻塞**
   - 当某个 cluster 的队列满时，生产者调用 `ConditionVariableSleep`
   - 生产者等待消费者消费数据后唤醒
   - 但消费者可能也在等待（队列为空时）
   - **死锁风险**：生产者等消费者，消费者等生产者

2. **生产者认为"完成"时，数据并未全部入队**
   - `IndexBuildHeapScan` 返回只表示扫描完成
   - 但 `BatchBuffer` 中可能还有未 flush 的数据
   - 溢出缓冲区中可能还有数据
   - 消费者只处理了队列中的部分数据

3. **finished 标志过早设置**
   - 生产者在 `IndexBuildHeapScan` 返回后立即设置 `finished = true`
   - 消费者看到 `finished = true` 且队列为空时，认为没有更多数据
   - 但实际上生产者还在 flush 剩余数据

#### 4.2.2 消费者提前退出（次要原因）

**代码位置**：[cluster.rs:L1476-1478](file:///home/zhangqiang/code/postgres/contrib/pgvectorscale/pgvectorscale/src/access_method/build/parallel_build/cluster.rs#L1476-L1478)

```rust
} else if queues.is_queue_finished(base_ptr, cluster_id) {
    break;
} else {
    queues.wait_on_cv(base_ptr, cluster_id);
}
```

**问题**：
- 消费者检查 `is_queue_finished()` 返回 true
- 但此时队列中可能还有数据（head/tail 检查时机问题）
- 或者生产者正在写入最后一批数据

#### 4.2.3 队列容量计算不准确

**代码位置**：[cluster.rs:L900-920](file:///home/zhangqiang/code/postgres/contrib/pgvectorscale/pgvectorscale/src/access_method/build/parallel_build/cluster.rs#L900-L920)

```rust
fn calculate_queue_capacities(...) -> Vec<usize> {
    let base_capacity = DEFAULT_QUEUE_CAPACITY;  // 10240
    ...
    let capacity = (base_capacity as f64 * (1.0 + ratio * 5.0)) as usize;
    capacity.clamp(base_capacity, max_capacity)
}
```

**问题**：
- 队列容量基于采样估计，可能与实际数据量差异大
- Cluster 0 估计 614,950 个向量，队列容量只有 4,175
- 队列只能容纳不到 1% 的数据，大部分数据被阻塞或丢失

#### 4.2.4 多 Worker 竞争导致的数据丢失

**问题**：
- 多个 worker 竞争同一个 cluster 的队列
- CAS 操作失败时，数据可能被跳过
- 没有重试机制，失败的条目永久丢失

### 4.3 数据丢失的时序图

```
时间线 →

Producer:  [扫描数据]→[写入队列]→[队列满，阻塞]→[继续扫描]→[扫描完成]→[设置finished=true]
              ↓              ↓              ↓              ↓              ↓
Queue:    [空]→[有数据]→[满]→[满]→[满]→[满]→[满]→[满]→[满]→[满]
              ↓              ↓              ↓              ↓              ↓
Consumer0:        [读取数据]→[读取数据]→[读取数据]→[队列为空，等待]→[看到finished=true，退出]
Consumer1:        [读取数据]→[读取数据]→[队列为空，等待]→[看到finished=true，退出]
Consumer2:        [读取数据]→[队列为空，等待]→[看到finished=true，退出]
...

结果：
- Producer 扫描了 1,000,000 条数据
- 但队列只能容纳 ~4,000 条
- 剩余 996,000 条数据被阻塞或丢弃
- 消费者只处理了队列中的部分数据（~17,000 条）
```

## 五、确保数据不丢失的改进方案

### 5.1 核心原则

1. **生产者必须确保所有数据入队后才退出**
2. **消费者必须处理完所有数据后才退出**
3. **引入确认机制**：生产者知道消费者已处理多少数据
4. **溢出缓冲区持久化**：确保缓冲区数据不丢失

### 5.2 改进设计

#### 5.2.1 生产者端改进

```rust
struct ProducerStateV3<'a> {
    // ... 原有字段 ...
    
    // 新增：跟踪已确认处理的数据量
    confirmed_counts: Vec<AtomicUsize>,
    
    // 新增：溢出缓冲区（持久化）
    overflow_buffers: Vec<Vec<OverflowEntry>>,
    
    // 新增：未确认数据计数
    pending_count: AtomicUsize,
}

impl ProducerStateV3<'_> {
    unsafe fn run_producer_loop(&mut self) {
        // 1. 启动扫描
        pg_sys::IndexBuildHeapScan(..., Some(producer_callback_v3), self);
        
        // 2. 扫描完成后，flush 所有剩余数据
        for cluster_id in 0..self.num_clusters {
            self.flush_all_remaining(cluster_id);
        }
        
        // 3. 等待所有数据被确认处理
        self.wait_for_all_confirmed();
        
        // 4. 安全设置 finished 标志
        self.mark_all_finished();
    }
    
    unsafe fn flush_all_remaining(&mut self, cluster_id: usize) {
        // Flush BatchBuffer
        self.flush_batch();
        
        // Flush 溢出缓冲区
        let overflow = &mut self.overflow_buffers[cluster_id];
        while !overflow.is_empty() {
            let entries = overflow.pop_batch(100);
            let pushed = self.push_to_queue_with_retry(cluster_id, &entries);
            if pushed < entries.len() {
                // 重新放回溢出缓冲区
                overflow.push_back(&entries[pushed..]);
            }
        }
    }
    
    unsafe fn wait_for_all_confirmed(&self) {
        loop {
            let total_produced = self.ntuples;
            let total_confirmed: usize = self.confirmed_counts
                .iter()
                .map(|c| c.load(Ordering::Acquire))
                .sum();
            
            if total_confirmed >= total_produced {
                break;
            }
            
            // 短暂等待后重试
            std::thread::sleep(Duration::from_millis(10));
            check_for_interrupts!();
        }
    }
}
```

#### 5.2.2 消费者端改进

```rust
unsafe fn process_cluster_vectors_v3<S: Storage>(
    consumer_state: &mut ConsumerState,
    ...
) {
    let mut total_processed = 0;
    let mut last_reported = 0;
    const REPORT_INTERVAL: usize = 1000;
    
    loop {
        // 1. 尝试从队列获取数据
        let batch = pop_batch_with_backoff(queues, base_ptr, cluster_id);
        
        if !batch.is_empty() {
            // 2. 处理数据
            for entry in &batch {
                process_vector(entry);
                total_processed += 1;
            }
            
            // 3. 定期报告进度
            if total_processed - last_reported >= REPORT_INTERVAL {
                report_progress(cluster_id, total_processed);
                last_reported = total_processed;
            }
        } else {
            // 4. 队列为空，检查是否应该退出
            if should_exit(queues, base_ptr, cluster_id, total_processed) {
                break;
            }
        }
    }
    
    // 5. 最终报告
    report_progress(cluster_id, total_processed);
}

unsafe fn should_exit(
    queues: &ClusterQueues,
    base_ptr: *mut u8,
    cluster_id: usize,
    processed: usize,
) -> bool {
    // 检查 finished 标志
    if !queues.is_queue_finished(base_ptr, cluster_id) {
        return false;
    }
    
    // 检查队列是否真正为空（多次检查）
    for _ in 0..10 {
        if queues.queue_size(base_ptr, cluster_id) > 0 {
            return false;
        }
        std::thread::sleep(Duration::from_micros(100));
    }
    
    // 检查是否还有溢出数据
    if queues.has_overflow_data(base_ptr, cluster_id) {
        return false;
    }
    
    true
}
```

#### 5.2.3 确认机制

```rust
#[repr(C)]
pub struct ClusterQueueHeader {
    pub head: AtomicUsize,
    pub tail: AtomicUsize,
    pub capacity: usize,
    pub element_size: usize,
    pub finished: AtomicBool,
    pub processed_count: AtomicUsize,  // 新增：已处理计数
    pub produced_count: AtomicUsize,   // 新增：已生产计数
}
```

### 5.3 消费者 CPU 100% 运转的优化

#### 5.3.1 本地大容量缓冲区

```rust
unsafe fn process_cluster_vectors_optimized<S: Storage>(...) {
    const LOCAL_BUFFER_SIZE: usize = 10000;
    let mut local_buffer: Vec<(ItemPointerData, Vec<f32>)> = 
        Vec::with_capacity(LOCAL_BUFFER_SIZE);
    
    loop {
        // 1. 填充本地缓冲区（从共享队列批量获取）
        if local_buffer.len() < LOCAL_BUFFER_SIZE / 2 {
            let needed = LOCAL_BUFFER_SIZE - local_buffer.len();
            let batch = queues.pop_batch(base_ptr, cluster_id, needed);
            local_buffer.extend(batch);
        }
        
        // 2. 处理本地缓冲区（CPU密集型，不访问共享队列）
        if !local_buffer.is_empty() {
            // 自适应批量大小
            let batch_size = calculate_optimal_batch_size();
            let to_process = local_buffer.len().min(batch_size);
            
            for i in 0..to_process {
                process_vector(&local_buffer[i]);
            }
            
            local_buffer.drain(..to_process);
        }
        
        // 3. 检查退出条件
        if local_buffer.is_empty() && should_exit(...) {
            break;
        }
        
        // 4. 自适应等待（避免空转）
        if local_buffer.is_empty() {
            adaptive_wait(empty_count);
        }
    }
}

fn adaptive_wait(empty_count: &mut u32) {
    *empty_count += 1;
    
    if *empty_count < 100 {
        // 短暂自旋
        for _ in 0..100 { std::hint::spin_loop(); }
    } else if *empty_count < 1000 {
        // 让出 CPU
        std::thread::yield_now();
    } else {
        // 短暂 sleep
        std::thread::sleep(Duration::from_micros(10));
    }
}
```

#### 5.3.2 批量 CAS 操作

```rust
impl ClusterQueues {
    /// 批量弹出（使用单次 CAS 获取多个条目）
    pub unsafe fn pop_batch_optimized(
        &self,
        base_ptr: *mut u8,
        cluster_id: usize,
        max_count: usize,
    ) -> Vec<QueueEntry> {
        let header = self.get_header(base_ptr, cluster_id);
        
        loop {
            let head = (*header).head.load(Ordering::Acquire);
            let tail = (*header).tail.load(Ordering::Acquire);
            
            if head == tail {
                return Vec::new(); // 队列为空
            }
            
            let available = if tail > head {
                tail - head
            } else {
                (*header).capacity - head + tail
            };
            
            let to_pop = available.min(max_count);
            let new_head = (head + to_pop) % (*header).capacity;
            
            // 单次 CAS 获取多个条目
            match (*header).head.compare_exchange(
                head, new_head, Ordering::AcqRel, Ordering::Acquire
            ) {
                Ok(_) => {
                    // 成功获取 to_pop 个条目
                    let mut result = Vec::with_capacity(to_pop);
                    for i in 0..to_pop {
                        let idx = (head + i) % (*header).capacity;
                        result.push(self.read_entry(base_ptr, cluster_id, idx));
                    }
                    return result;
                }
                Err(_) => continue, // CAS 失败，重试
            }
        }
    }
}
```

## 六、设计缺陷深度分析

### 6.1 当前代码的关键问题

#### 6.1.1 生产者流程缺陷（cluster.rs:L640-680）

```rust
// 当前生产者流程
pg_sys::IndexBuildHeapScan(..., Some(producer_callback), &mut producer_state);
producer_state.flush_batch();  // 致命问题：只flush一次！

(*parallel_shared).build_state.producer_done.store(true, Ordering::Release);

// 标记所有队列完成
for i in 0..num_clusters {
    queues.mark_queue_finished(base_ptr, i);  // 致命问题：消费者可能还在处理！
}

pg_sys::WaitForParallelWorkersToFinish(pcxt);
```

**致命缺陷**：
1. `flush_batch()` 只调用一次，但 `BatchBuffer` 只能存64条数据
2. 如果队列满，数据会积压在 `BatchBuffer` 和溢出缓冲区，没有机会被flush
3. 生产者设置 `finished=true` 时，消费者可能还有大量数据未处理

#### 6.1.2 消费者流程缺陷（cluster.rs:L1536-1540）

```rust
} else if queues.is_queue_finished(base_ptr, cluster_id) {
    break;  // 致命问题：看到 finished=true 就退出，但队列中还有数据！
} else {
    queues.wait_on_cv(base_ptr, cluster_id);
}
```

**致命缺陷**：
1. 消费者检查 `is_queue_finished()` 返回 true 就立即退出
2. 但此时队列中可能还有数据（head/tail 不一致，多worker竞争）
3. 没有确认机制，不知道其他worker处理了多少数据

#### 6.1.3 最关键的问题：没有全局确认机制

```
当前状态：
- 生产者知道生产了多少数据（producer_ntuples）
- 消费者各自知道自己处理了多少数据（consumer_state.ntuples）
- 但生产者不知道消费者总共处理了多少数据
- 消费者也不知道生产者总共生产了多少数据

结果：
- 生产者认为"完成"时，消费者可能还在处理
- 消费者认为"完成"时，可能还有数据未处理
- 数据丢失不可避免
```

### 6.2 数据丢失的必然性分析

**场景模拟**：

```
Cluster 0: 614,950 个向量
队列容量：4,175 个
Worker 数量：5 个

时间线：
T1: 生产者扫描，队列快速填满（4,175个）
T2: 生产者阻塞在 ConditionVariableSleep
T3: 消费者开始处理，但速度很慢（图插入是CPU密集型）
T4: 生产者被唤醒，继续扫描，队列再次满
T5: 重复 T2-T4，但只有少量数据能入队
...
T100: IndexBuildHeapScan 返回，生产者调用 flush_batch()
T101: 生产者设置 finished=true，标记队列完成
T102: 消费者看到 finished=true，退出
T103: 生产者等待消费者完成

结果：
- 生产者扫描了 614,950 个向量
- 实际入队：~4,175 * N 次（每次队列满就阻塞）
- 消费者处理：~7,433 个向量（来自测试数据）
- 丢失：607,517 个向量（丢失率 98.8%）
```

## 七、确保 100% 数据可靠性的设计方案

### 7.1 核心原则

1. **生产者必须确保所有数据入队后才标记完成**
2. **消费者必须处理完所有数据后才退出**
3. **引入全局确认机制**：生产者知道每个消费者处理了多少数据
4. **无阻塞设计**：生产者永不阻塞，消费者永不空等

### 7.2 改进架构

```
┌─────────────────────────────────────────────────────────────────┐
│                         生产者进程                               │
├─────────────────────────────────────────────────────────────────┤
│  1. IndexBuildHeapScan 扫描数据                                   │
│     ↓                                                            │
│  2. producer_callback: 数据 → BatchBuffer                        │
│     ↓                                                            │
│  3. flush_batch: BatchBuffer → 共享队列（无阻塞）                 │
│     ↓ 如果队列满                                                   │
│  4. 写入溢出缓冲区（内存，限制容量）                                │
│     ↓                                                            │
│  5. 后台线程：溢出缓冲区 → 共享队列（循环重试）                     │
│     ↓                                                            │
│  6. 扫描完成后，等待所有数据入队                                   │
│     ↓                                                            │
│  7. 等待所有消费者确认处理完成                                     │
│     ↓                                                            │
│  8. 标记全局完成，通知消费者可以退出                                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                    共享内存队列（每个cluster一个）
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      消费者进程（多个）                           │
├─────────────────────────────────────────────────────────────────┤
│  1. 从共享队列批量获取数据（无阻塞）                               │
│     ↓                                                            │
│  2. 写入本地大容量缓冲区（10,000条）                              │
│     ↓                                                            │
│  3. 处理本地缓冲区（CPU密集型，不访问共享内存）                     │
│     ↓                                                            │
│  4. 定期报告处理进度（每1,000条）                                 │
│     ↓                                                            │
│  5. 检查全局完成标志 + 确认所有数据已处理                          │
│     ↓                                                            │
│  6. 退出                                                         │
└─────────────────────────────────────────────────────────────────┘
```

### 7.3 关键数据结构改进

#### 7.3.1 全局状态跟踪

```rust
#[repr(C)]
pub struct ParallelBuildState {
    pub producer_done: AtomicBool,
    pub producer_ntuples: AtomicUsize,
    pub consumers_finished: AtomicUsize,
    pub start_nodes_initialized: AtomicBool,
    pub initialization_cv: ConditionVariable,
    pub assignments_cv: ConditionVariable,
    pub assignments_ready: AtomicBool,
    
    // 新增：全局确认机制
    pub global_produced_count: AtomicUsize,  // 生产者总共生产了多少
    pub global_processed_count: AtomicUsize, // 消费者总共处理了多少
    pub per_cluster_produced: [AtomicUsize; 64],  // 每个cluster生产了多少
    pub per_cluster_processed: [AtomicUsize; 64], // 每个cluster处理了多少
    pub all_data_confirmed: AtomicBool,      // 所有数据已确认
}
```

#### 7.3.2 生产者状态改进

```rust
struct ProducerStateV4<'a> {
    parallel_shared: *mut ParallelShared,
    cluster_queues: *mut ClusterQueues,
    cluster_sizes: *mut ClusterSizes,
    centroids: &'a [Vec<f32>],
    ntuples: usize,
    meta_page: MetaPage,
    batch_buffer: BatchBuffer,
    
    // 新增：每个cluster的溢出缓冲区
    overflow_buffers: Vec<Vec<OverflowEntry>>,
    overflow_limits: Vec<usize>,
    
    // 新增：后台flush线程通信
    flush_needed: Arc<AtomicBool>,
    shutdown_flush: Arc<AtomicBool>,
}

impl ProducerStateV4<'_> {
    unsafe fn push_with_overflow(
        &mut self,
        cluster_id: usize,
        heap_tid: pg_sys::ItemPointerData,
        vector: &[f32],
    ) {
        if self.batch_buffer.cluster_id != cluster_id || self.batch_buffer.is_full() {
            self.flush_batch_with_overflow();
            self.batch_buffer.cluster_id = cluster_id;
        }
        
        self.batch_buffer.entries.push((heap_tid, vector.to_vec()));
        
        // 更新统计
        if !self.cluster_sizes.is_null() {
            (*self.cluster_sizes).increment(cluster_id);
        }
    }
    
    unsafe fn flush_batch_with_overflow(&mut self) {
        if self.batch_buffer.entries.is_empty() {
            return;
        }
        
        let cluster_id = self.batch_buffer.cluster_id;
        let queues = &*self.cluster_queues;
        let base_ptr = self.cluster_queues as *mut u8;
        
        let entries: Vec<_> = self.batch_buffer.entries
            .iter()
            .map(|(tid, vec)| (*tid, vec.as_slice()))
            .collect();
        
        // 尝试推送到共享队列（无阻塞）
        let pushed = queues.try_push_batch(base_ptr, cluster_id, &entries);
        self.ntuples += pushed;
        
        // 未推送的数据放入溢出缓冲区
        if pushed < entries.len() {
            let overflow = &mut self.overflow_buffers[cluster_id];
            for i in pushed..entries.len() {
                if overflow.len() < self.overflow_limits[cluster_id] {
                    overflow.push(OverflowEntry {
                        heap_tid: entries[i].0,
                        vector: entries[i].1.to_vec(),
                    });
                    self.ntuples += 1;
                } else {
                    // 溢出缓冲区满，等待并重试
                    self.wait_and_retry_push(cluster_id, entries[i].0, entries[i].1);
                }
            }
        }
        
        self.batch_buffer.clear();
    }
    
    unsafe fn run_background_flush(&mut self) {
        // 后台线程：定期将溢出缓冲区数据推送到共享队列
        loop {
            if self.shutdown_flush.load(Ordering::Acquire) {
                break;
            }
            
            let mut total_flushed = 0;
            for cluster_id in 0..self.overflow_buffers.len() {
                let overflow = &mut self.overflow_buffers[cluster_id];
                if overflow.is_empty() {
                    continue;
                }
                
                let queues = &*self.cluster_queues;
                let base_ptr = self.cluster_queues as *mut u8;
                
                // 批量推送
                let to_push: Vec<_> = overflow.iter()
                    .take(100)
                    .map(|e| (e.heap_tid, e.vector.as_slice()))
                    .collect();
                
                let pushed = queues.try_push_batch(base_ptr, cluster_id, &to_push);
                overflow.drain(..pushed);
                total_flushed += pushed;
            }
            
            if total_flushed == 0 {
                // 没有数据需要flush，短暂sleep
                std::thread::sleep(Duration::from_micros(100));
            }
        }
    }
    
    unsafe fn wait_for_all_data_processed(&self) {
        let build_state = &(*self.parallel_shared).build_state;
        
        loop {
            let produced = build_state.global_produced_count.load(Ordering::Acquire);
            let processed = build_state.global_processed_count.load(Ordering::Acquire);
            
            if processed >= produced && self.all_overflow_empty() {
                break;
            }
            
            std::thread::sleep(Duration::from_millis(10));
            check_for_interrupts!();
        }
    }
    
    fn all_overflow_empty(&self) -> bool {
        self.overflow_buffers.iter().all(|b| b.is_empty())
    }
}
```

#### 7.3.3 消费者状态改进

```rust
struct ConsumerStateV4 {
    cluster_id: usize,
    cluster_queues: *mut ClusterQueues,
    parallel_shared: *mut ParallelShared,
    num_dimensions: usize,
    ntuples: usize,
    worker_number: usize,
    workers_per_cluster: usize,
    
    // 新增：本地大容量缓冲区
    local_buffer: Vec<(pg_sys::ItemPointerData, Vec<f32>)>,
    local_buffer_size: usize,
}

impl ConsumerStateV4 {
    unsafe fn process_with_local_buffer<S: Storage>(
        &mut self,
        queues: &ClusterQueues,
        base_ptr: *mut u8,
        storage: &mut S,
        graph: &mut Graph,
        tape: &mut Tape,
        write_stats: &mut WriteStats,
    ) {
        const LOCAL_BUFFER_CAPACITY: usize = 10000;
        const REPORT_INTERVAL: usize = 1000;
        
        let mut last_reported = 0;
        let mut consecutive_empty = 0;
        
        loop {
            // 1. 填充本地缓冲区
            if self.local_buffer.len() < LOCAL_BUFFER_CAPACITY / 2 {
                let needed = LOCAL_BUFFER_CAPACITY - self.local_buffer.len();
                let batch = self.pop_batch_from_queue(queues, base_ptr, needed);
                self.local_buffer.extend(batch);
                
                if !batch.is_empty() {
                    consecutive_empty = 0;
                }
            }
            
            // 2. 处理本地缓冲区（CPU密集型）
            if !self.local_buffer.is_empty() {
                let batch_size = self.calculate_optimal_batch_size();
                let to_process = self.local_buffer.len().min(batch_size);
                
                for i in 0..to_process {
                    let (heap_tid, vector) = &self.local_buffer[i];
                    self.process_single_vector(
                        *heap_tid, vector, storage, graph, tape, write_stats
                    );
                }
                
                self.local_buffer.drain(..to_process);
                self.ntuples += to_process;
                
                // 报告进度
                if self.ntuples - last_reported >= REPORT_INTERVAL {
                    self.report_progress();
                    last_reported = self.ntuples;
                }
            }
            
            // 3. 检查是否应该退出
            if self.local_buffer.is_empty() {
                consecutive_empty += 1;
                
                if self.should_exit(queues, base_ptr, consecutive_empty) {
                    break;
                }
                
                // 自适应等待
                self.adaptive_wait(consecutive_empty);
            }
            
            check_for_interrupts!();
        }
        
        // 处理剩余数据
        for (heap_tid, vector) in &self.local_buffer {
            self.process_single_vector(
                *heap_tid, vector, storage, graph, tape, write_stats
            );
            self.ntuples += 1;
        }
        
        // 最终报告
        self.report_progress();
    }
    
    unsafe fn should_exit(
        &self,
        queues: &ClusterQueues,
        base_ptr: *mut u8,
        consecutive_empty: u32,
    ) -> bool {
        // 检查全局完成标志
        let build_state = &(*self.parallel_shared).build_state;
        if !build_state.producer_done.load(Ordering::Acquire) {
            return false;
        }
        
        // 检查是否所有数据已处理
        let produced = build_state.per_cluster_produced[self.cluster_id].load(Ordering::Acquire);
        let processed = build_state.per_cluster_processed[self.cluster_id].load(Ordering::Acquire);
        
        if processed < produced {
            return false;
        }
        
        // 多次检查队列是否为空
        if consecutive_empty < 100 {
            return false;
        }
        
        // 最终确认：检查队列是否真正为空
        for _ in 0..10 {
            if queues.queue_size(base_ptr, self.cluster_id) > 0 {
                return false;
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        
        true
    }
    
    unsafe fn report_progress(&self) {
        let build_state = &(*self.parallel_shared).build_state;
        build_state.per_cluster_processed[self.cluster_id]
            .fetch_add(self.ntuples, Ordering::Release);
    }
    
    fn adaptive_wait(&self, consecutive_empty: u32) {
        if consecutive_empty < 100 {
            for _ in 0..100 { std::hint::spin_loop(); }
        } else if consecutive_empty < 1000 {
            std::thread::yield_now();
        } else {
            std::thread::sleep(Duration::from_micros(10));
        }
    }
}
```

### 7.4 生产者主流程改进

```rust
unsafe fn do_heap_scan_parallel_clustered_v4(...) -> usize {
    // ... 初始化代码 ...
    
    let mut producer_state = ProducerStateV4 {
        // ... 初始化 ...
    };
    
    // 启动后台flush线程
    let flush_handle = std::thread::spawn(move || {
        producer_state.run_background_flush();
    });
    
    // 主扫描循环
    pg_sys::IndexBuildHeapScan(
        heaprel,
        indexrel,
        index_info,
        Some(producer_callback_v4),
        &mut producer_state as *mut _ as *mut std::os::raw::c_void,
    );
    
    // 扫描完成后，flush所有剩余数据
    producer_state.flush_all_remaining();
    
    // 停止后台flush线程
    producer_state.shutdown_flush.store(true, Ordering::Release);
    flush_handle.join().unwrap();
    
    // 更新全局生产计数
    let build_state = &(*parallel_shared).build_state;
    build_state.global_produced_count.store(producer_state.ntuples, Ordering::Release);
    build_state.producer_done.store(true, Ordering::Release);
    
    // 等待所有数据被处理
    producer_state.wait_for_all_data_processed();
    
    // 通知消费者可以退出
    build_state.all_data_confirmed.store(true, Ordering::Release);
    
    // 等待消费者退出
    pg_sys::WaitForParallelWorkersToFinish(pcxt);
    
    // ... 清理代码 ...
    producer_state.ntuples
}
```

## 八、总结

### 8.1 当前设计的根本缺陷

1. **生产者过早标记完成**：`IndexBuildHeapScan` 返回不等于所有数据入队
2. **消费者过早退出**：看到 `finished=true` 就退出，但数据还在队列中
3. **缺乏全局确认机制**：生产者和消费者之间没有数据同步
4. **队列容量严重不足**：只能容纳不到 1% 的数据

### 8.2 改进方案核心要点

1. **100% 数据可靠性**：
   - 生产者等待所有数据被确认处理后才退出
   - 消费者处理完所有数据后才退出
   - 全局计数器跟踪生产和处理进度

2. **消费者 CPU 100% 运转**：
   - 大容量本地缓冲区（10,000条）
   - 批量从队列获取数据
   - 处理本地缓冲区时不访问共享内存
   - 自适应等待策略

3. **无阻塞设计**：
   - 生产者永不阻塞（使用溢出缓冲区）
   - 后台线程持续flush溢出数据
   - 消费者批量获取数据，减少CAS竞争

### 8.3 预期效果

| 指标 | 当前 | 优化后 |
|------|------|--------|
| 数据可靠性 | 1.7% | 100% |
| 数据丢失率 | 98.3% | 0% |
| 消费者 CPU 利用率 | ~30% | ~95% |
| 整体吞吐量 | 低 | 提升 5-10 倍 |
