# Cluster 构建召回率低问题分析

## 问题现象

- **非 cluster 构建召回率**: 0.8894
- **Cluster 构建召回率**: 0.3033
- **差异**: 召回率下降约 66%

## 根本原因分析

### 1. 问题定位

在 `greedy_search_for_build` 函数中：

```rust
fn greedy_search_for_build<S: Storage>(
    &mut self,
    index_pointer: IndexPointer,
    query: LabeledVector,
    no_filter: bool,
    storage: &S,
    stats: &mut GreedySearchStats,
) -> HashSet<NeighborWithDistance> {
    let start_nodes = self.meta_page.get_start_nodes();
    if start_nodes.is_none() {
        //no nodes in the graph
        return HashSet::with_capacity(0);  // <-- 问题在这里！
    }
    // ...
}
```

### 2. 问题机制

**Cluster 构建时的状态**:
1. `start_nodes` 为 `None`（因为我们跳过了 `meta_page.store`）
2. `cluster_start_nodes` 包含了所有 cluster 的 start nodes
3. 但 `greedy_search_for_build` 只检查 `start_nodes`

**后果**:
- 当 `start_nodes` 为 `None` 时，`greedy_search_for_build` 返回空结果
- `insert_internal` 无法找到任何邻居
- 每个节点插入时都没有正确的邻居连接
- 图构建质量极差，导致召回率大幅下降

### 3. 代码流程分析

```
process_cluster_vectors
    └── graph.insert(...)
        └── insert_internal(...)
            └── greedy_search_for_build(...)  <-- 返回空结果！
                └── add_neighbors(...)  <-- 没有邻居可添加
```

### 4. 为什么之前没发现

之前的修复只修改了 `greedy_search_streaming_init`（用于扫描），但没有修改 `greedy_search_for_build`（用于构建）。

- `greedy_search_streaming_init`: 用于查询时的图遍历 ✅ 已修复
- `greedy_search_for_build`: 用于构建时的邻居搜索 ❌ 未修复

## 解决方案

### 方案：统一修改 `greedy_search_for_build`

与 `greedy_search_streaming_init` 类似，修改 `greedy_search_for_build` 使其支持 `cluster_start_nodes`：

```rust
fn greedy_search_for_build<S: Storage>(
    &mut self,
    index_pointer: IndexPointer,
    query: LabeledVector,
    no_filter: bool,
    storage: &S,
    stats: &mut GreedySearchStats,
) -> HashSet<NeighborWithDistance> {
    let start_nodes = self.meta_page.get_start_nodes();
    
    // Get start nodes: use start_nodes for non-cluster builds,
    // or cluster_start_nodes for cluster builds
    let start_nodes_vec: Vec<ItemPointer> = if let Some(start_nodes) = start_nodes {
        // Non-cluster build: use start_nodes
        if no_filter {
            start_nodes.get_for_node(None)
        } else {
            start_nodes.get_for_node(query.labels())
        }
    } else {
        // Cluster build: use cluster_start_nodes
        let cluster_start_nodes = self.meta_page.get_all_cluster_start_nodes();
        if cluster_start_nodes.is_empty() {
            return HashSet::with_capacity(0);
        }
        // Collect all cluster start nodes
        cluster_start_nodes.values().copied().collect()
    };
    
    if start_nodes_vec.is_empty() {
        return HashSet::with_capacity(0);
    }
    
    let dm = storage.get_query_distance_measure(query);
    let search_list_size = self.meta_page.get_search_list_size_for_build() as usize;
    let num_neighbors = self.meta_page.get_num_neighbors();
    let mut l = ListSearchResult::new(
        start_nodes_vec,
        dm,
        Some(index_pointer),
        search_list_size,
        num_neighbors,
        self.get_neighbor_store(),
        storage,
    );
    let mut visited_nodes = HashSet::with_capacity(search_list_size);
    self.greedy_search_iterate(
        &mut l,
        search_list_size,
        no_filter,
        Some(&mut visited_nodes),
        storage,
    );
    stats.combine(&l.stats);
    visited_nodes
}
```

### 修复要点

1. **检查 `start_nodes` 是否为 `None`**
2. **如果为 `None`，使用 `cluster_start_nodes`**
3. **收集所有 cluster 的 start nodes**
4. **使用这些 start nodes 进行搜索**

## 验证计划

修复后需要验证：

1. **构建阶段**: `greedy_search_for_build` 能够正确找到邻居
2. **图质量**: 每个节点都有正确的邻居连接
3. **召回率**: cluster 构建的召回率应该接近非 cluster 构建
4. **扫描阶段**: 之前修复的 `greedy_search_streaming_init` 仍然正常工作

## 相关文件

- `src/access_method/graph/mod.rs`
  - `greedy_search_for_build` 函数（需要修改）
  - `greedy_search_streaming_init` 函数（已修复）

## 结论

召回率低的根本原因是 `greedy_search_for_build` 在 cluster 构建时无法获取 start nodes，导致无法找到邻居。修复方案是统一修改该函数，使其支持从 `cluster_start_nodes` 获取 start nodes。
