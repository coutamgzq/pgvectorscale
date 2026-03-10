# Cluster 并行构建问题分析文档

## 问题背景

在测试中发现性能差异：
- **8 cluster + 8 worker** (1 worker per cluster): 372秒
- **4 cluster + 8 worker** (2 workers per cluster): 120秒

同时怀疑 4 cluster + 8 worker 模式下构建的图可能不正确。

## 原始并行构建机制（非 cluster 模式）

### 关键设计

1. **所有 Worker 构建同一个图**
   - 所有 worker 调用 `do_heap_scan`，传入相同的 `worker_count`
   - `BuilderNeighborCache::new(BUILDER_NEIGHBOR_CACHE_SIZE, meta_page, worker_count)` 正确传递 worker 数量
   - 每个 worker 分配 `capacity / worker_count` 的缓存

2. **数据分区**
   - PostgreSQL 的 `ParallelTableScanDesc` 自动将表数据分区给不同 worker
   - 每个 worker 处理不同的数据范围

3. **并发安全机制**
   - `storage.create_node()` 使用 PostgreSQL 页面锁保证并发安全
   - `graph.insert()` 中的 `update_back_pointer` 会修改其他节点的邻居列表

### 为什么原始实现能工作？

**关键：`reconcile_with_disk_neighbors` 的设计保证了最终一致性**

```rust
fn reconcile_with_disk_neighbors<S: Storage>(
    &self,
    neighbors_of: ItemPointer,
    cached_neighbors: Vec<NeighborWithDistance>,
    storage: &S,
    stats: &mut PruneNeighborStats,
) -> Vec<NeighborWithDistance> {
    let disk_neighbors = storage.get_neighbors_with_distances_from_disk(neighbors_of, stats);
    // 合并缓存和磁盘的邻居
    all_neighbors.extend(cached_neighbors);
    all_neighbors.extend(disk_neighbors.into_iter().filter(...));
}
```

**工作流程：**
1. Worker A 更新了节点 N 的邻居并写入磁盘
2. Worker B 的缓存中有节点 N 的旧版本
3. 当 worker B 需要更新节点 N 时，调用 `reconcile_with_disk_neighbors`
4. 读取磁盘上的最新版本，与缓存合并，**不会丢失 worker A 的更新**

## Cluster 模式并行构建的问题

### 问题 1: BuilderNeighborCache 的 worker_count 参数设置错误

在 `build_cluster_subgraph` 中：

```rust
let mut graph = unsafe {
    Graph::new(
        GraphNeighborStore::Builder(BuilderNeighborCache::new(
            BUILDER_NEIGHBOR_CACHE_SIZE,
            meta_page,
            1,  // <-- 这里 hardcode 为 1，应该是实际的 worker 数量！
        )),
        &mut *(meta_page as *mut _),
    )
};
```

这导致每个 worker 分配了完整的缓存容量，而不是与其他 worker 共享。

### 问题 2: 更严重的问题 - 缓存独立性

**核心问题：每个 worker 有自己的独立 Graph 和 BuilderNeighborCache 实例**

在原始非 cluster 实现中：
- 所有 worker 共享同一个 `BuildStateParallel`，其中包含同一个 `Graph` 实例
- `Graph` 实例包含同一个 `BuilderNeighborCache`
- 缓存是共享的，一个 worker 的更新对其他 worker 可见

在 cluster 实现中：
- 每个 worker 有自己的 `Graph` 实例
- 每个 `Graph` 有自己的 `BuilderNeighborCache`
- **缓存不共享！** Worker A 的缓存更新对 Worker B 不可见

### 问题 3: 邻居更新冲突

当两个 worker 同时更新同一个节点的邻居时：

1. Worker A 读取磁盘邻居，添加新邻居 X，写回磁盘
2. Worker B 读取磁盘邻居（可能没有 X），添加新邻居 Y，写回磁盘
3. **结果：X 或 Y 可能丢失！**

虽然 `reconcile_with_disk_neighbors` 设计用于解决这个问题，但由于缓存独立，worker B 可能在调用 reconcile 之前就基于过时的缓存数据做出了决策。

## 性能差异分析

### 为什么 8 cluster + 8 worker 更慢但正确？

| 配置 | Worker 分配 | 构建模式 | 问题 |
|------|------------|---------|------|
| 8 cluster + 8 worker | [0], [1], [2], [3], [4], [5], [6], [7] | 每个 worker 独立构建一个图 | 没有并行协作，8 倍 flush 开销 |
| 4 cluster + 8 worker | [0,1], [2,3], [4,5], [6,7] | 每 2 个 worker 协作构建一个图 | 有并行协作，但存在缓存不一致问题 |

**8 cluster + 8 worker 特点：**
- 每个 cluster 只有一个 worker，没有并发冲突
- 每个 worker 独立构建自己的图，没有竞争，没有更新丢失
- 虽然总时间更长（因为 8 个图 vs 4 个图），但每个图都是正确的

**4 cluster + 8 worker 特点：**
- 同一个 cluster 的两个 worker 会并发更新同一个图
- 存在缓存不一致问题
- 虽然可能更快（因为 4 个图 vs 8 个图），但图可能不正确

## 为什么不是 CAS 的问题？

原始的 `reconcile_with_disk_neighbors` 设计已经考虑了并发更新，不需要额外的 CAS 机制：

```rust
fn reconcile_with_disk_neighbors<S: Storage>(
    &self,
    neighbors_of: ItemPointer,
    cached_neighbors: Vec<NeighborWithDistance>,
    storage: &S,
    stats: &mut PruneNeighborStats,
) -> Vec<NeighborWithDistance> {
    let disk_neighbors = storage.get_neighbors_with_distances_from_disk(neighbors_of, stats);
    let mut all_neighbors = Vec::with_capacity(cached_neighbors.len() + disk_neighbors.len());

    let cached_pointers: HashSet<_> = cached_neighbors
        .iter()
        .map(|n| n.get_index_pointer_to_neighbor())
        .collect();
    all_neighbors.extend(cached_neighbors);
    all_neighbors.extend(disk_neighbors.into_iter().filter(|disk_neighbor| {
        !cached_pointers.contains(&disk_neighbor.get_index_pointer_to_neighbor())
    }));

    all_neighbors
}
```

**这个设计的核心思想：**
- 每次更新前，先从磁盘读取最新状态
- 合并缓存中的修改和磁盘上的最新状态
- 确保不会丢失其他 worker 的更新

**问题在于：** cluster 模式的实现破坏了这一设计，因为每个 worker 有自己的独立缓存。

## 解决方案

### 方案 1: 限制每个 cluster 只能有一个 worker

最简单的解决方案：确保 worker 数 <= cluster 数

```rust
let workers_per_cluster = (total_workers + num_clusters - 1) / num_clusters;
assert!(workers_per_cluster <= 1, "Each cluster can have at most 1 worker");
```

**优点：**
- 实现简单
- 保证图的正确性

**缺点：**
- 无法利用多 worker 并行构建同一个 cluster 的优势

### 方案 2: 让同一个 cluster 的多个 worker 共享 Graph 实例

通过共享内存让同一个 cluster 的多个 worker 共享同一个 `Graph` 和 `BuilderNeighborCache` 实例。

**需要修改：**
1. 在共享内存中创建 `Graph` 和 `BuilderNeighborCache`
2. 同一个 cluster 的所有 worker 使用共享的实例
3. 添加必要的同步机制（锁或原子操作）

**优点：**
- 可以充分利用多 worker 并行构建
- 保持原始设计的正确性

**缺点：**
- 实现复杂
- 需要仔细设计同步机制

### 方案 3: 使用全局锁同步邻居更新

在更新节点邻居时使用全局锁。

**优点：**
- 实现相对简单
- 保证正确性

**缺点：**
- 性能较差，锁竞争严重
- 失去了并行构建的优势

### 方案 4: 版本控制 + CAS

为每个节点的邻居列表添加版本号，使用 CAS 操作更新。

**优点：**
- 无锁设计，性能较好
- 保证正确性

**缺点：**
- 实现复杂
- 需要修改存储格式

## 建议

**短期方案：** 采用方案 1，限制每个 cluster 只能有一个 worker，确保图的正确性。

**长期方案：** 采用方案 2，让同一个 cluster 的多个 worker 共享 Graph 实例，既保证正确性又提高性能。

## 相关代码位置

1. **问题代码位置：**
   - `/home/zhangqiang/code/postgres/contrib/pgvectorscale/pgvectorscale/src/access_method/build/parallel_build/cluster.rs`
   - 函数：`build_cluster_subgraph` (约第 1480 行)

2. **原始正确实现：**
   - `/home/zhangqiang/code/postgres/contrib/pgvectorscale/pgvectorscale/src/access_method/build.rs`
   - 函数：`do_heap_scan_with_clustering` (约第 770 行)

3. **关键数据结构：**
   - `/home/zhangqiang/code/postgres/contrib/pgvectorscale/pgvectorscale/src/access_method/graph/neighbor_store.rs`
   - 结构：`BuilderNeighborCache`
   - 方法：`reconcile_with_disk_neighbors`

## 日志验证

从日志中可以验证同一个 cluster 的多个 worker 确实在构建同一个图：

```
[Worker 0] Successfully set start node for cluster 0: ItemPointerData { ip_blkid: ..., ip_lo: 2 }, ip_posid: 1 }
[Worker 3] Using existing start node for cluster 0: ItemPointerData { ip_blkid: ..., ip_lo: 2 }, ip_posid: 1 }

[Worker 0] Graph::insert - Cluster: 0, Index OID: 22048061, Start Node: Some([ItemPointer { block_number: 2, offset: 1 }]), Inserting: ...
[Worker 3] Graph::insert - Cluster: 0, Index OID: 22048061, Start Node: Some([ItemPointer { block_number: 2, offset: 1 }]), Inserting: ...
```

- **Index OID 相同**：所有 worker 都是 `22048061`，说明写入同一个索引文件
- **Start Node 相同**：Cluster 0 的所有 worker 使用 start node `(2, 1)`
- **CAS 机制工作正常**：Worker 0 成功设置，Worker 3 使用现有的

但即使 Start Node 相同，由于缓存独立，邻居更新仍可能冲突。
