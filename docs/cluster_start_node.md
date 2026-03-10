# Cluster 构建中 Start Node 问题的分析与修复

## 问题描述

在 cluster 并行构建中，调用 `update_start_nodes` 函数时出现 assertion 失败错误：

```
assertion `left == right` failed
  left: ItemPointer { block_number: 0, offset: 3 }
  right: ItemPointer { block_number: 0, offset: 2 }
```

错误发生在 `meta_page.rs` 第 396 和 401 行的断言：
- `assert_eq!(off, ItemPointer::new(META_BLOCK_NUMBER, META_HEADER_OFFSET));`
- `assert_eq!(off, ItemPointer::new(META_BLOCK_NUMBER, META_OFFSET));`

## 问题分析

### 非 Cluster 并行构建 vs Cluster 并行构建

**非 Cluster 并行构建**（正常工作）：
- 所有工作线程共享同一个 `MetaPage` 实例（通过 `&mut *(meta_page as *mut _)` 转换）
- 当调用 `update_start_nodes` 时，虽然多个工作线程会同时调用，但它们都操作同一个 `MetaPage` 实例
- 只有第一个工作线程会设置 `start_nodes`，其他工作线程会看到 `start_nodes` 已经被设置，直接返回
- 不会导致并发写入冲突

**Cluster 并行构建**（出现问题）：
- 每个消费者工作线程都调用 `MetaPage::fetch(&index_relation)` 加载自己的 `MetaPage` 副本
- 每个工作线程有自己的 `MetaPage` 实例，互不影响
- 每个工作线程的 `self.meta_page.get_start_nodes()` 都返回 `None`，都会尝试设置 `start_nodes` 并调用 `meta_page.store`
- 多个工作线程同时写入磁盘上的同一个 meta page，导致并发写入冲突

### 根本原因

在 `_vectorscale_build_cluster_consumer_main` 函数中（`cluster.rs:683`），每个消费者工作线程都独立加载 `MetaPage`：

```rust
let mut meta_page = MetaPage::fetch(&index_relation);
```

这意味着每个工作线程都有自己的 `MetaPage` 副本。当调用 `graph.insert` 时，会触发 `update_start_nodes`，该函数会：
1. 检查 `start_nodes` 是否为 `None`
2. 如果是，创建新的 `StartNodes` 并设置
3. 调用 `meta_page.store(index, false)` 写入磁盘

由于每个工作线程都有自己的 `MetaPage` 副本，它们都会认为 `start_nodes` 为 `None`，都会尝试写入，导致并发冲突。

## 修复尝试与问题

### 尝试方案 1：共享 MetaPage（失败）

**方案**：在 `ParallelShared` 中添加 `meta_page_ptr` 字段，让所有 worker 共享同一个 `MetaPage` 实例。

**实现**：
1. 在 `ParallelShared` 结构体中添加 `meta_page_ptr` 字段
2. 在 leader 进程中分配共享内存并复制 `MetaPage`
3. 在 worker 进程中通过指针访问共享的 `MetaPage`

**问题**：Core dump 发生在 `_vectorscale_build_cluster_consumer_main` 函数中。

**失败原因**：
当我们尝试将 `MetaPage` 复制到共享内存时：

```rust
std::ptr::write(shared_meta_page, std::ptr::read(meta_page));
```

`MetaPage` 结构体包含堆分配的字段：
- `cluster_start_nodes: BTreeMap<u32, ItemPointer>` - 使用堆内存
- `centroids: Vec<Vec<f32>>` - 使用堆内存

`std::ptr::read` 会执行浅拷贝，只复制结构体的字段值，而不会深拷贝 `BTreeMap` 和 `Vec` 指向的堆内存。当 worker 进程尝试访问这些字段时，会访问无效的内存地址，导致 core dump。

### 正确方案：在 `update_start_nodes` 中跳过 Cluster 构建

**方案**：在 `update_start_nodes` 函数中，检查 `MetaPage` 是否有 centroids。如果有 centroids（表示是 cluster 构建），则跳过 `update_start_nodes`：

```rust
fn update_start_nodes<S: Storage>(
    &mut self,
    index: &PgRelation,
    index_pointer: IndexPointer,
    vec: &LabeledVector,
    storage: &S,
    stats: &mut PruneNeighborStats,
) {
    // For cluster builds, skip updating start_nodes.
    // Cluster start nodes are managed separately via set_cluster_start_node.
    // This avoids conflicts between cluster_start_nodes and start_nodes.
    if !self.meta_page.get_centroids().is_empty() {
        return;
    }

    // ... rest of the function for non-cluster builds
}
```

**修复说明**：

1. **非 Cluster 并行构建**：`update_start_nodes` 逻辑保持不变，继续正常工作
2. **Cluster 构建**：跳过 `update_start_nodes`，避免并发写入冲突
3. **Cluster Start Nodes**：在构建完成后通过 `set_cluster_start_node` 单独设置（已在 `cluster.rs` 中实现）

**为什么这个方案有效**：
- 对于 cluster 构建，start node 应该保存到 `cluster_start_nodes`（`BTreeMap<u32, ItemPointer>`），而不是 `start_nodes`（`Option<StartNodes>`）
- `update_start_nodes` 是为 `start_nodes` 设计的，不适用于 cluster 构建
- 跳过 `update_start_nodes` 避免了并发写入冲突，同时 cluster 的 start nodes 通过独立的机制管理

## 修改的文件

- `src/access_method/graph/mod.rs`：在 `update_start_nodes` 函数中添加 cluster 构建检查

## 验证

修复后，cluster 并行构建不再出现 assertion 失败错误，且：
- 非 cluster 并行构建继续正常工作
- cluster 并行构建的 start nodes 在构建完成后正确设置
