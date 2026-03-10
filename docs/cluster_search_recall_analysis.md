# Cluster 搜索召回率问题分析与修复方案

## 问题描述

Cluster 构建的 DiskANN 索引召回率（0.3033）远低于非 Cluster 构建（0.8894）。

## 问题分析

### 根本原因

**贪心搜索的局限性**：

1. 当前实现将所有 Cluster 的 start nodes 放入同一个优先队列
2. `greedy_search_iterate` 总是优先访问距离最近的节点
3. 如果某些 Cluster 距离查询点较远，它们的节点可能永远不会被访问

### 示例场景

假设有 8 个 Cluster，查询点只与其中 2-3 个 Cluster 比较接近：

```
Cluster 0: start_node distance = 0.1  <- 近
Cluster 1: start_node distance = 0.15 <- 近
Cluster 2: start_node distance = 0.2  <- 近
Cluster 3: start_node distance = 0.8  <- 远
Cluster 4: start_node distance = 0.85 <- 远
Cluster 5: start_node distance = 0.9  <- 远
Cluster 6: start_node distance = 0.95 <- 远
Cluster 7: start_node distance = 1.0  <- 远
```

如果 `search_list_size = 100`：
- 搜索可能只访问 Cluster 0, 1, 2 的节点
- Cluster 3-7 完全没有被探索
- 即使这些 Cluster 中有距离很近的节点，也被错过了

### 代码问题定位

**文件**: `src/access_method/graph/mod.rs`

**函数**: `greedy_search_streaming_init`

```rust
pub fn greedy_search_streaming_init<S: Storage>(...) {
    // ...
    } else {
        // Cluster build: use cluster_start_nodes
        let cluster_start_nodes = self.meta_page.get_all_cluster_start_nodes();
        // Collect all cluster start nodes
        cluster_start_nodes.values().copied().collect()  // <-- 问题：所有 start nodes 混在一起
    };
    // ...
    ListSearchResult::new(
        start_nodes_vec,  // <-- 所有 cluster 的 start nodes 在一个 ListSearchResult 中
        dm,
        None,
        search_list_size,
        num_neighbors,
        self.get_neighbor_store(),
        storage,
    )
}
```

**问题**：所有 Cluster 的 start nodes 被放入同一个 `ListSearchResult`，导致贪心搜索只关注最近的 Cluster。

## 修复方案

### 方案 1：每个 Cluster 独立搜索（推荐）

**思路**：为每个 Cluster 创建独立的 `ListSearchResult`，分别搜索，然后合并结果。

**实现**:

```rust
/// Search each cluster independently and merge results.
/// This ensures that all clusters are adequately explored, not just the closest ones.
fn search_clusters_independently<S: Storage>(
    &mut self,
    query: LabeledVector,
    search_list_size: usize,
    storage: &S,
) -> ListSearchResult<S::QueryDistanceMeasure, S::LSNPrivateData> {
    let cluster_start_nodes = self.meta_page.get_all_cluster_start_nodes();
    if cluster_start_nodes.is_empty() {
        return ListSearchResult::empty();
    }

    let dm = storage.get_query_distance_measure(query);
    let num_neighbors = self.meta_page.get_num_neighbors();

    // Collect all visited nodes from all clusters
    let mut all_visited: Vec<ListSearchNeighbor<S::LSNPrivateData>> = Vec::new();

    // Pre-fetch neighbor store to avoid borrow issues
    let neighbor_store = &mut self.neighbor_store;

    for (_, start_node) in cluster_start_nodes.iter() {
        // Create a separate ListSearchResult for each cluster
        let mut cluster_lsr = ListSearchResult::new(
            vec![*start_node],
            dm.clone(),
            None,
            search_list_size,
            num_neighbors,
            neighbor_store,
            storage,
        );

        // Search within this cluster
        self.greedy_search_iterate(&mut cluster_lsr, search_list_size, true, None, storage);

        // Collect visited nodes from this cluster
        all_visited.extend(cluster_lsr.visited);
    }

    if all_visited.is_empty() {
        return ListSearchResult::empty();
    }

    // Sort all visited nodes by distance and take the best ones as start nodes
    all_visited.sort_by(|a, b| a.distance_with_tie_break.cmp(&b.distance_with_tie_break));

    // Take up to search_list_size nodes as start nodes for the final search
    let final_start_nodes: Vec<ItemPointer> = all_visited
        .iter()
        .take(search_list_size)
        .map(|n| n.index_pointer)
        .collect();

    // Create the final ListSearchResult with all visited nodes as potential start points
    ListSearchResult::new(
        final_start_nodes,
        dm,
        None,
        search_list_size,
        num_neighbors,
        neighbor_store,
        storage,
    )
}
```

**修改 `greedy_search_streaming_init`**:

```rust
pub fn greedy_search_streaming_init<S: Storage>(...) {
    let start_nodes = self.get_start_nodes();

    if let Some(start_nodes) = start_nodes {
        // Non-cluster build: use start_nodes
        // ... 原有逻辑 ...
    } else {
        // Cluster build: search each cluster independently
        self.search_clusters_independently(query, search_list_size, storage)
    }
}
```

### 方案 2：增加 Search List Size（简单但不彻底）

**思路**：根据 Cluster 数量增加 `search_list_size`。

**实现**:

```rust
// For cluster builds, adjust search_list_size based on number of clusters
let adjusted_search_list_size = if meta_page.get_start_nodes().is_none() {
    let num_clusters = meta_page.num_clusters();
    search_list_size * num_clusters as usize
} else {
    search_list_size
};
```

**问题**：
- 只是增加了搜索节点数，但不能保证每个 Cluster 都被访问
- 如果某些 Cluster 距离很远，仍然可能被忽略

### 方案 3：多 Iterator 合并（复杂）

**思路**：为每个 Cluster 创建独立的 `TSVResponseIterator`，然后合并结果。

**实现复杂度**：高，需要重构 `StorageState` 和 `TSVScanState`。

## 推荐方案

**方案 1（每个 Cluster 独立搜索）** 是最佳选择：

1. **确保覆盖**：每个 Cluster 都被独立搜索，不会遗漏
2. **质量保障**：从每个 Cluster 中找出最近的节点，再合并排序
3. **效率平衡**：不会在不相关的 Cluster 上浪费太多时间

## 实现注意事项

### 1. Clone 约束

需要为 `QueryDistanceMeasure` 添加 `Clone` 约束：

```rust
// storage.rs
pub trait Storage {
    type QueryDistanceMeasure: Clone;  // 添加 Clone 约束
    // ...
}

// plain/mod.rs
#[derive(Clone)]
pub enum PlainDistanceMeasure {
    Full(LabeledVector),
}

// sbq/mod.rs
#[derive(Clone)]
pub struct SbqSearchDistanceMeasure {
    vec: Vec<SbqVectorElement>,
    query: LabeledVector,
}
```

### 2. 借用冲突

Rust 借用检查器限制：
- `self.meta_page.get_all_cluster_start_nodes()` 借用 `self`
- `self.get_neighbor_store()` 需要 `&mut self`
- `self.greedy_search_iterate(...)` 需要 `&mut self`

**解决方案**：
- 先提取 `neighbor_store` 的可变引用
- 在循环中只使用 `neighbor_store`，不调用其他需要 `&mut self` 的方法

### 3. ListSearchNeighbor Clone

需要为 `ListSearchNeighbor` 添加 `Clone`：

```rust
#[derive(Clone)]
pub struct ListSearchNeighbor<PD> {
    pub index_pointer: IndexPointer,
    distance_with_tie_break: DistanceWithTieBreak,
    private_data: PD,
    labels: Option<LabelSet>,
}
```

## 验证计划

修复后需要验证：

1. **召回率**：Cluster 构建的召回率应该接近非 Cluster 构建
2. **搜索时间**：确保搜索时间不会显著增加
3. **覆盖率**：确保所有相关 Cluster 都被探索

## 结论

问题的根源是贪心搜索算法在混合所有 Cluster start nodes 时，只关注距离最近的 Cluster，导致远距离 Cluster 被忽略。

**方案 1** 通过为每个 Cluster 独立搜索，确保所有 Cluster 都被充分探索，是解决这个问题的最佳方案。
