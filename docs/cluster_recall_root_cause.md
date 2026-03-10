# Cluster 构建召回率低的根本原因分析

## 问题确认

**现象**：Cluster 构建召回率 0.3033，非 cluster 构建召回率 0.8894

## 根本原因

### 执行顺序问题

在 `insert` 函数中的执行顺序：

```rust
fn insert_internal(...) {
    // 1. 先执行 greedy_search_for_build
    let v = self.greedy_search_for_build(
        index_pointer,
        vec,
        no_filter,
        storage,
        &mut stats.greedy_search_stats,
    );  // <-- 此时 start_nodes 可能为 None！
    
    // 2. 然后执行 update_start_nodes
    self.update_start_nodes(...);  // <-- 在这里设置 start_nodes
    
    // 3. 使用搜索结果添加邻居
    let (_, neighbor_list) = self.add_neighbors(...);
}
```

### 问题机制

**第一个节点插入时**：
1. `meta_page.start_nodes` 为 `None`（从磁盘加载的初始状态）
2. `greedy_search_for_build` 检查 `start_nodes`，发现为 `None`
3. 返回 `HashSet::with_capacity(0)`（空结果）
4. `add_neighbors` 没有邻居可添加
5. **第一个节点没有邻居！**
6. 然后 `update_start_nodes` 才设置 `start_nodes`

**第二个节点插入时**：
1. `start_nodes` 已设置（包含第一个节点）
2. `greedy_search_for_build` 从第一个节点开始搜索
3. 但第一个节点没有邻居，所以搜索结果很少
4. 第二个节点只能连接到第一个节点

**后续节点**：
- 每个节点只能看到之前插入的节点
- 无法看到其他 cluster 的节点
- 图构建质量极差

### 为什么非 cluster 构建没问题

非 cluster 构建时：
1. 使用 `update_start_nodes` 设置 `start_nodes`
2. 调用 `meta_page.store` 保存到磁盘
3. 后续插入时，`start_nodes` 从磁盘加载，始终可用

Cluster 构建时：
1. 每个 worker 从磁盘加载 `meta_page`，`start_nodes` 为 `None`
2. 第一个节点插入时，`greedy_search_for_build` 在 `update_start_nodes` 之前执行
3. 第一个节点没有邻居，导致连锁反应

## 解决方案

### 方案 1：调整执行顺序（推荐）

在 `insert_internal` 中先调用 `update_start_nodes`，再调用 `greedy_search_for_build`：

```rust
fn insert_internal(...) {
    // 1. 先更新 start_nodes（确保 start_nodes 已设置）
    self.update_start_nodes(
        index,
        index_pointer,
        &vec,
        storage,
        &mut stats.prune_neighbor_stats,
    );
    
    // 2. 然后执行 greedy_search_for_build
    let v = self.greedy_search_for_build(
        index_pointer,
        vec,
        no_filter,
        storage,
        &mut stats.greedy_search_stats,
    );
    
    // 3. 使用搜索结果添加邻居
    let (_, neighbor_list) = self.add_neighbors(...);
}
```

**优点**：
- 简单直接
- 确保 `start_nodes` 在搜索前已设置

**缺点**：
- 需要修改 `insert` 的调用顺序
- 可能影响其他逻辑

### 方案 2：预设置 start_nodes

在 `build_cluster_subgraph` 中，在插入任何节点之前预设置 `start_nodes`：

```rust
unsafe fn build_cluster_subgraph(...) {
    // ... 现有代码 ...
    
    // 预设置 start_nodes（使用第一个将要插入的节点）
    // 从 queue 中预览第一个向量
    if let Some((heap_tid, vector_data)) = queues.peek_from_queue(base_ptr, cluster_id) {
        let start_nodes = StartNodes::new(index_pointer);  // 临时 pointer
        meta_page.set_start_nodes(start_nodes);
    }
    
    // ... 继续处理 ...
}
```

**优点**：
- 不需要修改 `insert` 逻辑

**缺点**：
- 需要知道第一个节点的 pointer（可能在创建前未知）
- 实现复杂

### 方案 3：特殊处理第一个节点

在 `greedy_search_for_build` 中特殊处理第一个节点：

```rust
fn greedy_search_for_build(...) {
    let start_nodes = self.meta_page.get_start_nodes();
    
    // 如果 start_nodes 为 None，可能是第一个节点
    // 返回空结果，让 add_neighbors 处理
    if start_nodes.is_none() {
        return HashSet::with_capacity(0);
    }
    
    // ... 正常逻辑 ...
}
```

然后在 `insert` 中：
```rust
fn insert(...) {
    // 如果是第一个节点（start_nodes 为 None），跳过搜索
    if self.meta_page.get_start_nodes().is_none() {
        // 直接设置 start_nodes，不搜索邻居
        self.update_start_nodes(...);
        return;
    }
    
    // 正常流程
    // ...
}
```

**优点**：
- 针对性修复

**缺点**：
- 逻辑分散
- 需要判断是否是第一个节点

## 推荐方案

**方案 1：调整执行顺序**

原因：
1. 简单直接
2. 逻辑清晰：先设置 start_nodes，再搜索
3. 不影响其他功能

## 修复代码

```rust
fn insert_internal<S: Storage>(
    &mut self,
    index_pointer: IndexPointer,
    vec: LabeledVector,
    no_filter: bool,
    storage: &S,
    stats: &mut InsertStats,
) {
    let labels = vec.labels().cloned();

    // Update start nodes BEFORE searching (ensure start_nodes is set)
    self.update_start_nodes(
        // ... 需要获取 index 参数
        index_pointer,
        &vec,
        storage,
        &mut stats.prune_neighbor_stats,
    );

    // Now search with start_nodes guaranteed to be set
    #[allow(clippy::mutable_key_type)]
    let v = self.greedy_search_for_build(
        index_pointer,
        vec,
        no_filter,
        storage,
        &mut stats.greedy_search_stats,
    );

    let (_, neighbor_list) = self.add_neighbors(
        storage,
        index_pointer,
        labels.as_ref(),
        v.into_iter().collect(),
        &mut stats.prune_neighbor_stats,
    );
    
    // ... 后续代码 ...
}
```

**注意**：需要修改 `update_start_nodes` 的签名，移除 `index` 参数或使其可选。

## 验证

修复后：
1. 第一个节点插入时，`start_nodes` 已设置
2. `greedy_search_for_build` 能正确找到邻居
3. 图构建质量恢复正常
4. 召回率应该接近非 cluster 构建
