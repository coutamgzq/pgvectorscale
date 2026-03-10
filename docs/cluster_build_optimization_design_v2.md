# Cluster 并行构建优化设计方案 V2（精简版）

## 一、设计目标

1. **消费者 CPU 100% 运转**：消费者始终有数据可处理，不空闲等待
2. **生产者不阻塞**：队列满时定期检查，不阻塞扫描流程
3. **100% 数据可靠性**：所有生产的数据都被消费者处理
4. **简化实现**：无后台线程，无持久化，单线程生产者

## 二、核心设计

### 2.1 三级存储架构（内存 only）

```
┌─────────────────────────────────────────────────────────────┐
│                      生产者进程（单线程）                       │
├─────────────────────────────────────────────────────────────┤
│  Level 1: LocalBuffer（本地缓存）                             │
│  - 大小：BATCH_SIZE = 100 条                                  │
│  - 作用：聚合同一 cluster 的向量                               │
│  - 策略：满则 flush 到共享队列                                 │
└─────────────────────────────────────────────────────────────┘
                          ↓ try_push_batch (无阻塞)
┌─────────────────────────────────────────────────────────────┐
│                Level 2: 共享队列（Shared Memory）              │
│  - 大小：DEFAULT_QUEUE_CAPACITY = 10240 条                   │
│  - 作用：生产者-消费者通信桥梁                                  │
│  - 策略：无阻塞写入，满则转到 Level 3                          │
└─────────────────────────────────────────────────────────────┘
                          ↓ 队列满时
┌─────────────────────────────────────────────────────────────┐
│              Level 3: 内存溢出缓冲区（Private Memory）          │
│  - 大小：max_overflow = queue_capacity / 2 = 5120 条         │
│  - 作用：暂存无法写入共享队列的数据                             │
│  - 策略：定期尝试 flush 到共享队列，扫描结束后全部 flush        │
│  - 保护：溢出缓冲区满时必须暂停扫描，防止数据丢失                 │
│  - 清理：进程结束自动释放，不持久化                            │
└─────────────────────────────────────────────────────────────┘```

### 2.2 生产者流程

```rust
struct ProducerStateV2<'a> {
    parallel_shared: *mut ParallelShared,
    cluster_queues: *mut ClusterQueues,
    centroids: &'a [Vec<f32>],
    ntuples: usize,
    meta_page: MetaPage,
    
    // Level 1: 本地缓存
    local_buffer: LocalBuffer,
    
    // Level 3: 溢出缓冲区 (cluster_id -> Vec<entries>)
    overflow_buffers: Vec<Vec<(ItemPointerData, Vec<f32>)>>,
    overflow_counts: Vec<usize>,  // 每个 cluster 的溢出数量
}

impl ProducerStateV2<'_> {
    // 主入口：处理一条数据
    unsafe fn push(&mut self, cluster_id: usize, heap_tid: ItemPointerData, vector: &[f32]) {
        // 【关键保护】检查该 cluster 的溢出缓冲区是否已满
        // 如果溢出缓冲区已满，必须先腾出空间，否则继续扫描会导致数据丢失
        let max_overflow = self.get_max_overflow(cluster_id);
        if self.overflow_counts[cluster_id] >= max_overflow {
            self.wait_for_overflow_space(cluster_id);
        }
        
        // 1. 尝试写入本地缓存
        if self.local_buffer.cluster_id == cluster_id && !self.local_buffer.is_full() {
            self.local_buffer.push(heap_tid, vector);
            return;
        }
        
        // 2. 本地缓存满或 cluster 变化，先 flush
        self.flush_local_buffer();
        self.local_buffer.cluster_id = cluster_id;
        self.local_buffer.push(heap_tid, vector);
    }
    
    // 获取指定 cluster 的最大溢出缓冲区大小
    fn get_max_overflow(&self, cluster_id: usize) -> usize {
        // 假设所有 cluster 队列容量相同
        // 实际实现中可以从 queues 获取
        5120  // queue_capacity / 2
    }
    
    // 【关键方法】等待溢出缓冲区腾出空间
    // 当某个 cluster 的共享队列满且溢出缓冲区也满时，必须暂停扫描
    // 否则继续扫描 heap 会导致属于该 cluster 的数据丢失
    unsafe fn wait_for_overflow_space(&mut self, cluster_id: usize) {
        let max_overflow = self.get_max_overflow(cluster_id);
        
        // 循环尝试 flush 溢出缓冲区，直到有空间可用
        while self.overflow_counts[cluster_id] >= max_overflow {
            // 1. 尝试将溢出缓冲区的数据 flush 到共享队列
            self.retry_flush_overflow(cluster_id);
            
            // 2. 检查是否成功腾出空间
            if self.overflow_counts[cluster_id] >= max_overflow {
                // 仍然没有空间，说明消费者处理速度跟不上
                // 必须暂停扫描，等待消费者消费数据
                // 使用较短的 sleep 时间以快速响应
                std::thread::sleep(Duration::from_millis(5));
                
                // 可选：添加超时或日志警告，防止无限等待
                // 在实际生产环境中，这种情况不应该频繁发生
                // 如果频繁发生，说明队列容量设置过小或消费者数量不足
            }
        }
    }
    
    // Flush 本地缓存到共享队列
    unsafe fn flush_local_buffer(&mut self) {
        if self.local_buffer.is_empty() {
            return;
        }
        
        let cluster_id = self.local_buffer.cluster_id;
        let entries = self.local_buffer.drain();
        
        // 尝试批量推送到共享队列
        let pushed = queues.try_push_batch(base_ptr, cluster_id, &entries);
        self.ntuples += pushed;
        
        // 未推送的数据放入溢出缓冲区
        if pushed < entries.len() {
            let remaining = &entries[pushed..];
            self.add_to_overflow(cluster_id, remaining);
        }
    }
    
    // 添加到溢出缓冲区
    // 【关键保护】在添加前必须确保有空间，否则调用 wait_for_overflow_space
    unsafe fn add_to_overflow(&mut self, cluster_id: usize, entries: &[(ItemPointerData, &[f32])]) {
        let max_overflow = self.get_max_overflow(cluster_id);
        
        for (tid, vec) in entries {
            // 检查溢出缓冲区是否已满
            if self.overflow_counts[cluster_id] >= max_overflow {
                // 溢出缓冲区已满，必须暂停并等待腾出空间
                // 不能继续扫描，否则数据会丢失
                self.wait_for_overflow_space(cluster_id);
            }
            
            // 现在确定有空间，安全添加
            self.overflow_buffers[cluster_id].push((*tid, vec.to_vec()));
            self.overflow_counts[cluster_id] += 1;
        }
    }
    
    // 定期尝试 flush 溢出缓冲区
    unsafe fn retry_flush_overflow(&mut self, cluster_id: usize) {
        let buffer = &mut self.overflow_buffers[cluster_id];
        if buffer.is_empty() {
            return;
        }
        
        // 尝试推送尽可能多的数据
        let entries: Vec<_> = buffer.iter()
            .map(|(tid, vec)| (*tid, vec.as_slice()))
            .collect();
        
        let pushed = queues.try_push_batch(base_ptr, cluster_id, &entries);
        
        // 移除已推送的数据
        if pushed > 0 {
            buffer.drain(0..pushed);
            self.overflow_counts[cluster_id] -= pushed;
            self.ntuples += pushed;
        }
    }
    
    // 扫描结束后，flush 所有剩余数据
    unsafe fn flush_all_remaining(&mut self) {
        // 1. flush 本地缓存
        self.flush_local_buffer();
        
        // 2. 持续 flush 溢出缓冲区直到为空
        loop {
            let mut total_remaining = 0;
            
            for cluster_id in 0..self.overflow_buffers.len() {
                self.retry_flush_overflow(cluster_id);
                total_remaining += self.overflow_counts[cluster_id];
            }
            
            if total_remaining == 0 {
                break;
            }
            
            // 还有数据，等待一下再试
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

// 生产者主流程
unsafe fn do_heap_scan_parallel_clustered_v2(...) -> usize {
    let mut producer_state = ProducerStateV2::new(...);
    
    // 1. 扫描 heap 表
    pg_sys::IndexBuildHeapScan(..., Some(producer_callback_v2), &mut producer_state);
    
    // 2. Flush 所有剩余数据（包括溢出缓冲区）
    producer_state.flush_all_remaining();
    
    // 3. 更新全局生产计数
    (*parallel_shared).build_state.global_produced_count.store(producer_state.ntuples, Release);
    (*parallel_shared).build_state.producer_done.store(true, Release);
    
    // 4. 等待所有消费者完成（关键：确保 100% 数据被处理）
    pg_sys::WaitForParallelWorkersToFinish(pcxt);
    
    producer_state.ntuples
}
```

### 2.3 消费者流程

```rust
struct ConsumerStateV2 {
    cluster_id: usize,
    cluster_queues: *mut ClusterQueues,
    parallel_shared: *mut ParallelShared,
    ntuples: usize,
    worker_number: usize,
    
    // 大容量本地缓冲区（关键：让消费者 CPU 100% 运转）
    local_buffer: Vec<(ItemPointerData, Vec<f32>)>,
    buffer_capacity: usize,  // 10,000 条
}

impl ConsumerStateV2 {
    // 主处理循环
    unsafe fn process_loop(&mut self) {
        loop {
            // 1. 尝试批量获取数据到本地缓冲区
            let fetched = self.fetch_to_local_buffer();
            
            if fetched > 0 {
                // 2. 处理本地缓冲区（此时不访问共享内存，CPU 100% 运转）
                self.process_local_buffer();
                continue;
            }
            
            // 3. 本地缓冲区为空，检查是否可以退出
            if self.should_exit() {
                break;
            }
            
            // 4. 短暂等待后重试（自适应等待）
            self.adaptive_wait();
        }
    }
    
    // 批量获取数据到本地缓冲区
    unsafe fn fetch_to_local_buffer(&mut self) -> usize {
        let available = self.local_buffer.capacity() - self.local_buffer.len();
        if available == 0 {
            return 0;
        }
        
        let queues = &*self.cluster_queues;
        let base_ptr = self.cluster_queues as *mut u8;
        
        // 使用 pop_batch_fast 批量获取
        let mut heap_tids = vec![std::mem::zeroed(); available];
        let mut vectors = vec![Vec::new(); available];
        
        let popped = queues.pop_batch_fast(
            base_ptr, 
            self.cluster_id, 
            available,
            &mut heap_tids, 
            &mut vectors
        );
        
        // 更新全局处理计数
        if popped > 0 {
            (*self.parallel_shared)
                .build_state
                .global_processed_count
                .fetch_add(popped, Ordering::Release);
            
            // 添加到本地缓冲区
            for i in 0..popped {
                self.local_buffer.push((heap_tids[i], vectors[i].clone()));
            }
        }
        
        popped
    }
    
    // 处理本地缓冲区（CPU 密集型，无共享内存访问）
    fn process_local_buffer(&mut self) {
        for (heap_tid, vector) in &self.local_buffer {
            // 构建图索引（CPU 密集型操作）
            self.build_graph_node(*heap_tid, vector);
            self.ntuples += 1;
        }
        
        self.local_buffer.clear();
    }
    
    // 检查是否可以退出
    unsafe fn should_exit(&self) -> bool {
        let build_state = &(*self.parallel_shared).build_state;
        
        // 生产者未完成，不能退出
        if !build_state.producer_done.load(Ordering::Acquire) {
            return false;
        }
        
        // 检查该 cluster 是否还有数据
        let queues = &*self.cluster_queues;
        let base_ptr = self.cluster_queues as *mut u8;
        
        let produced = queues.get_produced_count(base_ptr, self.cluster_id);
        let processed = queues.get_processed_count(base_ptr, self.cluster_id);
        
        // 还有数据未处理，不能退出
        if processed < produced {
            return false;
        }
        
        // 队列中还有数据，不能退出
        if queues.queue_size(base_ptr, self.cluster_id) > 0 {
            return false;
        }
        
        true
    }
    
    // 自适应等待（减少 CPU 空转）
    fn adaptive_wait(&self) {
        // 简单实现：短暂 yield
        std::thread::yield_now();
    }
}
```

## 三、关键改进点

### 3.1 生产者流量控制（防数据丢失）

```
传统方式（有风险）：
  队列满 -> 写入溢出缓冲区 -> 继续扫描
  问题：如果溢出缓冲区也满，继续扫描会导致数据丢失

新方式（安全）：
  队列满 -> 写入溢出缓冲区 -> 继续扫描
  溢出缓冲区满 -> 暂停扫描，等待消费者 -> 有空间后继续
  
关键保护机制：
  1. push() 入口检查：如果溢出缓冲区已满，先调用 wait_for_overflow_space()
  2. add_to_overflow() 检查：添加每条数据前确保有空间
  3. wait_for_overflow_space()：循环尝试 flush，直到腾出空间
```

### 3.2 消费者 CPU 100% 运转

```
传统方式：
  从队列取1条 -> 处理 -> 从队列取1条 -> 处理（频繁 CAS 竞争）

新方式：
  批量取 10,000 条到本地 -> 处理 10,000 条（无共享内存访问）-> 批量取
  
优势：
  - 批量获取减少 CAS 竞争
  - 处理本地数据时 CPU 100% 运转，不访问共享内存
  - 消费者之间无竞争
```

### 3.3 100% 数据可靠性

```
生产者：
  1. push() 入口保护：溢出缓冲区满时暂停扫描，防止数据丢失
  2. add_to_overflow() 保护：添加数据前确保有空间
  3. 扫描完成后 flush_all_remaining()（确保所有数据入队）
  4. 设置 producer_done = true
  5. WaitForParallelWorkersToFinish()（等待所有消费者完成）

消费者：
  1. 持续处理直到 should_exit() 返回 true
  2. should_exit() 条件：
     - producer_done == true
     - produced_count == processed_count
     - 队列为空
```

## 四、关键保护机制详解

### 4.1 数据丢失风险场景

**问题场景**：
```
1. 生产者扫描 heap 表，按顺序读取元组
2. 某个 cluster 的共享队列已满（10,240 条）
3. 该 cluster 的溢出缓冲区也已满（5,120 条）
4. 如果继续扫描，下一个元组恰好属于该 cluster
5. 此时无处可写，数据就会丢失！
```

**解决方案**（方案2：全局暂停机制）：
```
在 push() 入口和 add_to_overflow() 中添加保护：
- 检查 overflow_counts[cluster_id] >= max_overflow
- 如果已满，调用 wait_for_overflow_space()
- wait_for_overflow_space() 循环尝试 flush，直到有空间
- 期间暂停 heap 扫描，确保数据不会丢失
```

### 4.2 保护机制流程图

```
生产者处理一条数据：
┌─────────────────┐
│ 1. push() 入口   │
│ 检查溢出缓冲区    │
│ 是否已满？       │
└────────┬────────┘
         │
    是 ──┴──► ┌─────────────────────┐
              │ wait_for_overflow() │
              │ 循环尝试 flush      │
              │ 直到腾出空间        │
              └──────────┬──────────┘
                         │
    ◄────────────────────┘
    │
    ▼
┌─────────────────┐
│ 2. 正常处理流程  │
│ - 写入本地缓存   │
│ - 或 flush 到队列│
└─────────────────┘
```

### 4.3 性能考虑

- **正常情况**：溢出缓冲区不会满，无额外开销
- **压力情况**：消费者处理速度跟不上时，生产者会短暂暂停
- **最坏情况**：如果频繁触发等待，说明：
  - 队列容量设置过小，需要调大 `DEFAULT_QUEUE_CAPACITY`
  - 消费者数量不足，需要增加并行工作进程数

## 五、内存控制

| 层级 | 大小 | 总量（4 clusters） |
|------|------|-------------------|
| LocalBuffer（生产者） | 100 条 | 100 条 |
| 共享队列 | 10,240 条/cluster | 40,960 条 |
| 溢出缓冲区 | 5,120 条/cluster | 20,480 条 |
| 消费者本地缓冲 | 10,000 条/worker | 80,000 条 (8 workers) |

**总内存**：约 141,540 条 × (向量大小 + 开销) ≈ 100-200 MB（假设 768 维向量）

## 六、代码实现要点

### 6.1 需要修改的文件

1. **parallel.rs**：
   - 添加 `pop_batch_fast()` 方法（已实现）
   - 添加 `get_produced_count()` / `get_processed_count()`（已实现）

2. **cluster.rs**：
   - 修改 `ProducerState` 添加溢出缓冲区
   - **添加 `wait_for_overflow_space()` 方法（关键：防数据丢失）**
   - 修改 `push()` 添加入口检查
   - 修改 `add_to_overflow()` 添加溢出缓冲区满检查
   - 修改 `ConsumerState` 添加大容量本地缓冲区
   - 修改 `process_cluster_vectors()` 使用批量获取
   - 修改生产者主流程添加 `flush_all_remaining()`

### 6.2 关键代码片段

```rust
// 生产者回调
unsafe extern "C-unwind" fn producer_callback_v2(...) {
    let state = &mut *(state as *mut ProducerStateV2);
    
    let vec = PgVector::from_pg_parts(...);
    if let Some(vec) = vec {
        let cluster_id = k_means::k_means_lookup(vec, state.centroids);
        state.push(cluster_id, *ctid, vec);
    }
}

// 消费者处理
unsafe fn process_cluster_vectors_v2(...) {
    let mut consumer_state = ConsumerStateV2::new(...);
    consumer_state.process_loop();
}
```

## 七、预期效果

| 指标 | 当前 | 优化后 |
|------|------|--------|
| 数据可靠性 | 1.7% | **100%** |
| 消费者 CPU 利用率 | ~30% | **~95%** |
| 生产者阻塞 | 有 | **无（正常情况下）** |
| 整体吞吐量 | 低 | **提升 3-5 倍** |

**注意**：生产者仅在溢出缓冲区满时会短暂暂停，这是为了防止数据丢失的必要保护机制。正常情况下，由于有充足的队列容量和溢出缓冲区，不会触发暂停。
