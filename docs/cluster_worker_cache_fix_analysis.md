# Cluster Worker Cache 修复分析

## 问题

在 `build_cluster_subgraph` 中，`BuilderNeighborCache::new` 的 `worker_count` 参数最初被 hardcode 为 1：

```rust
let mut graph = unsafe {
    Graph::new(
        GraphNeighborStore::Builder(BuilderNeighborCache::new(
            BUILDER_NEIGHBOR_CACHE_SIZE,
            meta_page,
            1,  // <-- 这里应该是同一个 cluster 下的 worker 数量
        )),
        &mut *(meta_page as *mut _),
    )
};
```

**已修复：** 现在使用 `workers_per_cluster` 参数：

```rust
let workers_per_cluster = consumer_state.workers_per_cluster;
let mut graph = unsafe {
    Graph::new(
        GraphNeighborStore::Builder(BuilderNeighborCache::new(
            BUILDER_NEIGHBOR_CACHE_SIZE,
            meta_page,
            workers_per_cluster,  // 正确的 worker 数量
        )),
        &mut *(meta_page as *mut _),
    )
};
```

## 分析：非 Cluster 和 Cluster 模式的对比

### 两种模式的核心机制完全相同

| 特性 | 非 Cluster 模式 | Cluster 模式 |
|------|----------------|--------------|
| Graph 实例 | 每个 worker 独立创建 | 每个 worker 独立创建 |
| BuilderNeighborCache | 每个 worker 独立创建 | 每个 worker 独立创建 |
| 插入节点 | `graph.insert()` | `graph.insert()` |
| 缓存 flush | `maybe_flush_neighbor_cache()` | `maybe_flush_neighbor_cache()` |
| Reconcile 机制 | 驱逐时合并磁盘和缓存 | 驱逐时合并磁盘和缓存 |
| PostgreSQL 页面锁 | 使用 | 使用 |

**关键结论：** 两种模式使用完全相同的并发控制机制！

### 非 Cluster 模式下多个 Worker 并发构建的正确性原理

#### 1. 整体流程

```
主进程 (Leader)
    │
    ├── 启动多个 Worker 进程
    │
    ├── 每个 Worker 执行 _vectorscale_build_main()
    │       │
    │       ├── 创建独立的 Graph 实例
    │       │       Graph::new(GraphNeighborStore::Builder(BuilderNeighborCache::new(...)))
    │       │
    │       ├── 并行扫描表数据 (IndexBuildHeapScanParallel)
    │       │       PostgreSQL 自动将数据分区，每个 Worker 处理不同分区
    │       │
    │       └── 对每个向量调用 build_callback_parallel()
    │               │
    │               ├── 创建节点: storage.create_node()
    │               │
    │               ├── 插入图: graph.insert()
    │               │       │
    │               │       ├── 搜索邻居 (greedy search)
    │               │       │       读取节点邻居时:
    │               │       │       - 先查本地缓存
    │               │       │       - 缓存未命中则读磁盘 (带 PostgreSQL 共享锁)
    │               │       │
    │               │       ├── 更新邻居: set_neighbors()
    │               │       │       - 写入本地缓存
    │               │       │       - 缓存满时驱逐旧条目
    │               │       │       - 驱逐时 reconcile_with_disk_neighbors()
    │               │       │         (合并磁盘数据 + 本地缓存数据)
    │               │       │       - prune_neighbors() 剪枝
    │               │       │       - 写回磁盘 (带 PostgreSQL 排他锁)
    │               │       │
    │               │       └── 返回
    │               │
    │               └── 定期 flush: maybe_flush_neighbor_cache()
    │
    └── 等待所有 Worker 完成
```

#### 2. 关键函数

**入口函数：**
- `_vectorscale_build_main()` - Worker 进程入口
- `do_heap_scan()` → `do_heap_scan_with_clustering()` - 主扫描函数

**回调函数：**
- `build_callback_parallel()` - 并行构建回调
- `build_callback_parallel_internal()` - 内部实现

**核心操作：**
- `Graph::insert()` - 插入节点到图
- `BuilderNeighborCache::set_neighbors()` - 设置邻居
- `BuilderNeighborCache::reconcile_with_disk_neighbors()` - 合并磁盘和缓存数据
- `Graph::prune_neighbors()` - 剪枝邻居列表
- `PlainStorage::set_neighbors_on_disk()` - 写入磁盘

**缓存管理：**
- `maybe_flush_neighbor_cache()` - 定期 flush 缓存
- `Drop for BuilderNeighborCache` - 析构时 flush 所有缓存

#### 3. 正确性保证机制

**机制 1：PostgreSQL 页面级锁**

```rust
// 读取 - 共享锁
pub unsafe fn read<'a, S: StatsNodeRead>(...) -> ReadablePlainNode<'a> {
    let buffer = ReadableBuffer::read_bytes(index_pointer, index);
    // PostgreSQL 自动获取共享锁
    ...
}

// 写入 - 排他锁
pub unsafe fn modify<'a, S: StatsNodeModify>(...) -> WritablePlainNode<'a> {
    let buffer = WritableBuffer::read_bytes(index_pointer, index);
    // PostgreSQL 自动获取排他锁
    ...
}
```

**机制 2：Reconcile 合并**

```rust
fn reconcile_with_disk_neighbors<S: Storage>(
    &self,
    neighbors_of: ItemPointer,
    cached_neighbors: Vec<NeighborWithDistance>,
    storage: &S,
    stats: &mut PruneNeighborStats,
) -> Vec<NeighborWithDistance> {
    // 1. 从磁盘读取（可能包含其他 Worker 写入的数据）
    let disk_neighbors = storage.get_neighbors_with_distances_from_disk(neighbors_of, stats);
    
    // 2. 使用 HashSet 去重
    let cached_pointers: HashSet<_> = cached_neighbors
        .iter()
        .map(|n| n.get_index_pointer_to_neighbor())
        .collect();
    
    // 3. 先添加缓存中的邻居
    let mut all_neighbors = Vec::with_capacity(...);
    all_neighbors.extend(cached_neighbors);
    
    // 4. 再添加磁盘中独有的邻居（不丢失任何邻居）
    all_neighbors.extend(disk_neighbors.into_iter().filter(|disk_neighbor| {
        !cached_pointers.contains(&disk_neighbor.get_index_pointer_to_neighbor())
    }));

    all_neighbors
}
```

**机制 3：最终一致性**

```rust
impl Drop for BuilderNeighborCache {
    fn drop(&mut self) {
        let (neighbor_map, stats) = self.neighbor_map.into_inner().into_parts();
        for (neighbors_of, entry) in neighbor_map.into_iter() {
            // Worker 结束时，所有缓存数据 reconcile 后写入磁盘
            let all_neighbors = self.reconcile_with_disk_neighbors(...);
            storage.set_neighbors_on_disk(neighbors_of, all_neighbors.as_slice(), stats);
        }
    }
}
```

**机制 4：HNSW 容错性**

- 即使某些邻居选择不是最优的，图仍然可以工作
- 通过 prune 操作逐步优化邻居列表
- 最终图质量主要取决于数据分布

### 为什么 Cluster 模式也能工作？

Cluster 模式与非 Cluster 模式使用**完全相同的机制**：

1. **相同的 Graph 创建方式**
   ```rust
   // Cluster 模式
   let mut graph = Graph::new(
       GraphNeighborStore::Builder(BuilderNeighborCache::new(
           BUILDER_NEIGHBOR_CACHE_SIZE,
           meta_page,
           workers_per_cluster,
       )),
       &mut *(meta_page as *mut _),
   );
   ```

2. **相同的插入流程**
   ```rust
   graph.insert(index_relation, index_pointer, labeled_vector, storage, &mut insert_stats);
   ```

3. **相同的 flush 机制**
   ```rust
   if consumer_state.ntuples % flush_interval == 0 {
       graph.maybe_flush_neighbor_cache(storage, &mut insert_stats);
   }
   ```

4. **相同的 reconcile 机制**
   - 驱逐时自动合并磁盘和缓存数据
   - Worker 结束时 flush 所有缓存

### Cluster 模式的额外考虑

#### 1. Start Node 同步

Cluster 模式需要确保同一个 cluster 的所有 worker 使用相同的 start node：

```rust
// 使用 CAS 操作确保只有一个 worker 设置 start node
if (*cluster_start_nodes).try_set_start_node(cluster_id, item_pointer_data, Some(consumer_state.worker_number)) {
    // 设置成功
    meta_page.set_start_nodes(StartNodes::new(index_pointer));
} else {
    // 其他 worker 已设置，获取它
    if let Some(start_node) = (*cluster_start_nodes).get_start_node(cluster_id) {
        meta_page.set_start_nodes(StartNodes::new(item_ptr));
    }
}
```

#### 2. 更频繁的 Flush

Cluster 模式下，同一个 cluster 的多个 worker 会并发修改同一个子图，因此需要更频繁的 flush：

```rust
let base_flush_interval = parallel::flush_rate(cluster_vectors.max(1));
let flush_interval = if workers_per_cluster > 1 {
    // 多 worker 时更频繁 flush
    (base_flush_interval / workers_per_cluster).max(100)
} else {
    base_flush_interval
};
```

## 核心修改点总结

### 已完成的修改

1. **worker_count 参数修复**
   ```rust
   // 修复前
   BuilderNeighborCache::new(BUILDER_NEIGHBOR_CACHE_SIZE, meta_page, 1)
   
   // 修复后
   BuilderNeighborCache::new(BUILDER_NEIGHBOR_CACHE_SIZE, meta_page, workers_per_cluster)
   ```

2. **Start Node 共享机制**
   - 使用 `ClusterStartNodes` 共享内存结构
   - CAS 操作确保原子性设置
   - 所有 worker 获取相同的 start node

3. **自适应 Flush 间隔**
   - 根据 `workers_per_cluster` 动态调整 flush 频率
   - 多 worker 时更频繁同步

### 无需修改的部分

以下机制在 Cluster 模式和非 Cluster 模式下**完全相同**，无需额外修改：

- ✅ Graph 创建和销毁
- ✅ BuilderNeighborCache 的 reconcile 机制
- ✅ PostgreSQL 页面锁
- ✅ 定期 flush 机制
- ✅ Worker 结束时的最终 flush

## 结论

1. **非 Cluster 和 Cluster 模式使用完全相同的并发控制机制**
   - 都依赖 PostgreSQL 页面锁
   - 都使用 BuilderNeighborCache 的 reconcile 机制
   - 都通过最终一致性保证正确性

2. **Cluster 模式只需要额外的 start node 同步**
   - 使用共享内存 + CAS 操作
   - 确保同一个 cluster 的所有 worker 从相同的 start node 开始

3. **worker_count 参数的正确设置很重要**
   - 影响每个 worker 的缓存容量分配
   - 影响 flush 频率的计算

4. **两种模式都能正确工作**
   - 不需要共享 Graph 实例
   - 不需要额外的缓存同步机制
   - reconcile + PostgreSQL 页面锁已足够

---

## 附录：worker_count 参数的详细说明

### 作用原理

`worker_count` 参数用于**控制每个 worker 的缓存容量**，防止多个 worker 同时运行时内存溢出。

### 代码实现

在 `BuilderNeighborCache::new()` 中：

```rust
pub fn new(memory_budget: f64, meta_page: &MetaPage, worker_count: usize) -> Self {
    // 1. 获取系统总内存 (maintenance_work_mem)
    let total_memory = maintenance_work_mem_bytes() as f64;
    
    // 2. 计算缓存预算 (默认使用 80% 的 maintenance_work_mem)
    let memory_budget = (total_memory * memory_budget).ceil() as usize;
    
    // 3. 计算单个条目的大小
    let capacity = memory_budget
        / NeighborCacheEntry::size(meta_page.get_num_neighbors() as _, meta_page.has_labels());
    
    // 4. 关键：将容量平均分配给所有 worker
    let capacity = if worker_count > 0 {
        capacity / worker_count  // <-- 这里体现作用！
    } else {
        capacity
    };

    Self {
        neighbor_map: RefCell::new(LruCacheWithStats::new(
            NonZero::new(capacity).unwrap(),
            "Builder neighbor",
        )),
        ...
    }
}
```

### 为什么需要这个参数？

**场景示例：**

假设系统配置：
- `maintenance_work_mem = 1GB`
- `BUILDER_NEIGHBOR_CACHE_SIZE = 0.8` (使用 80%)
- 每个邻居条目约 100 字节
- 总可用缓存 = 1GB × 0.8 = 800MB

| 场景 | worker_count | 每个 worker 容量 | 总内存使用 |
|------|-------------|-----------------|-----------|
| 单 Worker | 1 | 8,000,000 条目 | 800MB ✓ |
| 4 Workers (正确配置) | 4 | 2,000,000 条目 | 800MB ✓ |
| 4 Workers (错误配置) | 1 | 8,000,000 条目 | 3.2GB ✗ (OOM!) |

### 使用位置

**非 Cluster 模式：**
```rust
// build.rs
let graph = Graph::new(
    GraphNeighborStore::Builder(BuilderNeighborCache::new(
        BUILDER_NEIGHBOR_CACHE_SIZE,
        meta_page,
        worker_count,  // 传入总 worker 数量
    )),
    ...
);
```

**Cluster 模式：**
```rust
// cluster.rs
let workers_per_cluster = consumer_state.workers_per_cluster;
let mut graph = Graph::new(
    GraphNeighborStore::Builder(BuilderNeighborCache::new(
        BUILDER_NEIGHBOR_CACHE_SIZE,
        meta_page,
        workers_per_cluster,  // 传入该 cluster 的 worker 数量
    )),
    ...
);
```

### 关键区别

| 模式 | worker_count 含义 | 传入值 |
|------|------------------|--------|
| 非 Cluster | 总 worker 数量 | `worker_count` |
| Cluster | 每个 cluster 的 worker 数量 | `workers_per_cluster` |

**注意：** 在 Cluster 模式下，每个 cluster 的 workers 只构建该 cluster 的子图，因此只需要按 `workers_per_cluster` 分配缓存，而不是总 worker 数量。
