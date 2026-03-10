# Cluster 扫描搜索问题分析

## 问题描述

Cluster 构建的召回率只有 0.3033，而非 cluster 构建有 0.8894。

用户提出：扫描时是否因为带了 limit，导致只搜索了其中一个 cluster 就结束？

## 问题分析

### 当前扫描逻辑

```rust
// 1. 初始化时，将所有 cluster 的 start nodes 加入候选队列
let lsr = graph.greedy_search_streaming_init(query, search_list_size, storage);

// 2. 迭代搜索，使用 search_list_size 限制访问节点数
graph.greedy_search_iterate(
    &mut self.lsr,
    self.search_list_size,  // 限制访问的节点数
    !self.has_label_filter,
    None,
    storage,
);
```

### 潜在问题

假设：
- `search_list_size = 100`
- 8 个 cluster，每个 cluster 有 1000 个节点
- 查询向量只与其中 2-3 个 cluster 比较接近

**问题场景**：
1. 所有 8 个 cluster 的 start nodes 被加入候选队列
2. 距离查询向量最近的 start nodes 会被优先访问
3. 如果 `search_list_size = 100`，可能只访问了 2-3 个 cluster 的节点
4. 其他 5-6 个 cluster 根本没有被探索
5. 导致召回率下降

### 验证问题

假设每个 cluster 的 start node 到查询向量的距离：
- Cluster 0: 0.1
- Cluster 1: 0.15
- Cluster 2: 0.2
- Cluster 3: 0.8
- Cluster 4: 0.85
- Cluster 5: 0.9
- Cluster 6: 0.95
- Cluster 7: 1.0

如果 `search_list_size = 30`：
- 可能只访问了 Cluster 0, 1, 2 的节点
- Cluster 3-7 完全没有被探索
- 即使这些 cluster 中有距离很近的节点，也被错过了

## 解决方案

### 方案 1：增加搜索列表大小（简单）

根据 cluster 数量动态调整 `search_list_size`：

```rust
let num_clusters = meta_page.num_clusters();
let adjusted_search_list_size = search_list_size * num_clusters;

graph.greedy_search_iterate(
    &mut self.lsr,
    adjusted_search_list_size,
    !self.has_label_filter,
    None,
    storage,
);
```

**优点**：
- 实现简单
- 确保所有 cluster 都有机会被探索

**缺点**：
- 搜索时间增加
- 可能访问很多不相关的节点

### 方案 2：两阶段搜索（推荐）

用户建议的方案：

**第一阶段**：从所有 cluster 的 start nodes 开始，找出每个 cluster 中最近的节点

```rust
// 对每个 cluster，找到最近的节点
let mut cluster_best: Vec<Option<NeighborWithDistance>> = vec![None; num_clusters];

for cluster_id in 0..num_clusters {
    let start_node = meta_page.get_cluster_start_node(cluster_id);
    // 从 start_node 开始局部搜索，找到该 cluster 中最近的节点
    let best_in_cluster = local_search(start_node, query, small_search_list_size);
    cluster_best[cluster_id] = best_in_cluster;
}
```

**第二阶段**：从所有 cluster 的最佳节点中，选择全局最近的继续搜索

```rust
// 将所有 cluster 的最佳节点加入候选队列
for best in cluster_best.iter().flatten() {
    lsr.insert_neighbor(best.clone());
}

// 继续全局搜索
graph.greedy_search_iterate(&mut lsr, search_list_size, ...);
```

**优点**：
- 确保每个 cluster 都被探索
- 不会浪费时间在明显不相关的 cluster 上
- 搜索效率更高

**缺点**：
- 实现复杂
- 需要额外的局部搜索开销

### 方案 3：Best-First 跨 Cluster 搜索

修改 `greedy_search_streaming_init`，使用更智能的初始化：

```rust
pub fn greedy_search_streaming_init<S: Storage>(...) {
    let cluster_start_nodes = self.meta_page.get_all_cluster_start_nodes();
    
    // 计算查询向量到所有 cluster start nodes 的距离
    let mut cluster_distances: Vec<(f32, usize, ItemPointer)> = cluster_start_nodes
        .iter()
        .map(|(cluster_id, start_node)| {
            let dist = compute_distance(query, start_node);
            (dist, *cluster_id, *start_node)
        })
        .collect();
    
    // 按距离排序
    cluster_distances.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    
    // 只选择最近的 k 个 cluster 进行深入搜索
    let k = (num_clusters as f32 * 0.5).ceil() as usize;  // 例如，选择 50% 的 cluster
    let selected_clusters: Vec<ItemPointer> = cluster_distances
        .iter()
        .take(k)
        .map(|(_, _, start_node)| *start_node)
        .collect();
    
    // 使用选中的 cluster start nodes 初始化搜索
    ListSearchResult::new(selected_clusters, ...)
}
```

**优点**：
- 智能选择最相关的 cluster
- 减少不相关 cluster 的搜索开销

**缺点**：
- 可能错过距离远但相关的节点
- 需要调参（选择多少 cluster）

## 推荐方案

**方案 2（两阶段搜索）** 是最佳选择，因为：

1. **确保覆盖**：每个 cluster 都被探索，不会遗漏
2. **效率平衡**：不会在不相关的 cluster 上浪费太多时间
3. **质量保证**：从每个 cluster 的最佳节点开始全局搜索

## 实现思路

```rust
pub fn greedy_search_streaming_init<S: Storage>(
    &mut self,
    query: LabeledVector,
    search_list_size: usize,
    storage: &S,
) -> ListSearchResult<S::QueryDistanceMeasure, S::LSNPrivateData> {
    let start_nodes = self.meta_page.get_start_nodes();
    
    let start_nodes_vec: Vec<ItemPointer> = if let Some(start_nodes) = start_nodes {
        // Non-cluster build: use start_nodes
        start_nodes.get_for_node(query.labels())
    } else {
        // Cluster build: two-phase search
        let cluster_start_nodes = self.meta_page.get_all_cluster_start_nodes();
        if cluster_start_nodes.is_empty() {
            return ListSearchResult::empty();
        }
        
        // Phase 1: Find best node in each cluster
        let dm = storage.get_query_distance_measure(query.clone());
        let mut cluster_best_nodes: Vec<(f32, ItemPointer)> = Vec::new();
        
        for (_, start_node) in cluster_start_nodes.iter() {
            // Quick local search from each cluster start node
            // Find the closest node in this cluster to the query
            let local_search_size = 10;  // Small search within cluster
            let mut local_lsr = ListSearchResult::new(
                vec![*start_node],
                dm.clone(),
                None,
                local_search_size,
                self.meta_page.get_num_neighbors(),
                self.get_neighbor_store(),
                storage,
            );
            
            // Quick local iteration
            self.greedy_search_iterate(&mut local_lsr, local_search_size, true, None, storage);
            
            // Get the closest node found in this cluster
            if let Some(closest) = local_lsr.get_closest_visited() {
                cluster_best_nodes.push((closest.distance, closest.index_pointer));
            }
        }
        
        // Phase 2: Select top candidates from all clusters
        cluster_best_nodes.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let num_clusters_to_search = (cluster_best_nodes.len() as f32 * 0.7).ceil() as usize;
        
        cluster_best_nodes
            .into_iter()
            .take(num_clusters_to_search)
            .map(|(_, ptr)| ptr)
            .collect()
    };
    
    // ... rest of the function
}
```

## 验证计划

修复后需要验证：

1. **召回率**：cluster 构建的召回率应该接近非 cluster 构建
2. **搜索时间**：确保搜索时间不会显著增加
3. **覆盖率**：确保所有相关 cluster 都被探索

## 结论

用户的分析是正确的：当前的扫描逻辑可能因为 `search_list_size` 限制，导致只探索了部分 cluster，从而降低了召回率。

推荐采用**两阶段搜索**方案，确保每个 cluster 都被适当探索，同时保持搜索效率。
