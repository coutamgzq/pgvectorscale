# Cluster Centroid 搜索设计方案

## 背景

当前 Cluster 构建的 DiskANN 存在召回率问题（0.3033 vs 非 Cluster 构建的 0.8894）。

根本原因：所有 Cluster 的 start nodes 被放入同一个优先队列，贪心搜索只关注最近的 Cluster，导致远距离 Cluster 被忽略。

## 设计目标

1. **提高召回率**：确保相关 Cluster 都被搜索
2. **保持性能**：避免搜索所有 Cluster，减少不必要的计算
3. **可配置性**：允许用户根据场景调整搜索策略

## 核心思路

利用 K-Means 聚类的 Centroids，在搜索时先找到查询向量最近的 1-2 个 Cluster，然后只搜索这些 Cluster。

## 详细设计

### 1. 算法流程

```
搜索流程：
1. 获取查询向量 Q
2. 计算 Q 到所有 Centroids 的距离
3. 选择距离最近的 K 个 Centroids（对应 K 个 Clusters）
4. 只在这 K 个 Clusters 中执行贪心搜索
5. 返回结果
```

### 2. 关键组件

#### 2.1 MetaPage 扩展

需要添加获取 Centroids 的方法：

```rust
impl MetaPage {
    /// 获取所有 cluster 的 centroids
    pub fn get_centroids(&self) -> &Vec<Vec<f32>> {
        &self.centroids
    }
    
    /// 获取指定 cluster 的 centroid
    pub fn get_centroid(&self, cluster_id: u32) -> Option<&Vec<f32>> {
        self.centroids.get(cluster_id as usize)
    }
}
```

#### 2.2 距离计算

需要计算查询向量到 centroid 的距离：

```rust
fn compute_distance_to_centroid(query: &[f32], centroid: &[f32], distance_fn: DistanceFn) -> f32 {
    distance_fn(query, centroid)
}
```

#### 2.3 搜索初始化修改

修改 `greedy_search_streaming_init`：

```rust
pub fn greedy_search_streaming_init<S: Storage>(
    &mut self,
    query: LabeledVector,
    search_list_size: usize,
    storage: &S,
) -> ListSearchResult<S::QueryDistanceMeasure, S::LSNPrivateData> {
    let start_nodes = self.get_start_nodes();

    if let Some(start_nodes) = start_nodes {
        // 非 Cluster 构建：原有逻辑
        // ...
    } else {
        // Cluster 构建：使用 Centroid 选择最近的 K 个 Clusters
        self.search_nearest_clusters(query, search_list_size, storage)
    }
}
```

#### 2.4 最近 Cluster 搜索

```rust
/// 搜索距离查询向量最近的 K 个 Clusters
fn search_nearest_clusters<S: Storage>(
    &mut self,
    query: LabeledVector,
    search_list_size: usize,
    storage: &S,
) -> ListSearchResult<S::QueryDistanceMeasure, S::LSNPrivateData> {
    let cluster_start_nodes = self.meta_page.get_all_cluster_start_nodes();
    let centroids = self.meta_page.get_centroids();
    
    if cluster_start_nodes.is_empty() || centroids.is_empty() {
        return ListSearchResult::empty();
    }

    let dm = storage.get_query_distance_measure(query.clone());
    let num_neighbors = self.meta_page.get_num_neighbors();
    let distance_fn = self.meta_page.get_distance_function();
    
    // 计算查询向量到所有 centroids 的距离
    let mut cluster_distances: Vec<(f32, u32, ItemPointer)> = Vec::new();
    
    for (cluster_id, start_node) in cluster_start_nodes.iter() {
        if let Some(centroid) = centroids.get(*cluster_id as usize) {
            let query_vec = query.vec().to_index_slice();
            let dist = compute_distance(query_vec, centroid, distance_fn);
            cluster_distances.push((dist, *cluster_id, *start_node));
        }
    }
    
    if cluster_distances.is_empty() {
        return ListSearchResult::empty();
    }
    
    // 按距离排序
    cluster_distances.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    
    // 获取配置的 K 值（搜索最近的 K 个 clusters）
    let k = get_cluster_search_top_k(); // GUC 参数，默认 2
    
    // 选择最近的 K 个 clusters
    let selected_start_nodes: Vec<ItemPointer> = cluster_distances
        .into_iter()
        .take(k)
        .map(|(_, _, start_node)| start_node)
        .collect();
    
    if selected_start_nodes.is_empty() {
        return ListSearchResult::empty();
    }
    
    // 创建 ListSearchResult，只搜索选中的 clusters
    ListSearchResult::new(
        selected_start_nodes,
        dm,
        None,
        search_list_size,
        num_neighbors,
        self.get_neighbor_store(),
        storage,
    )
}
```

### 3. GUC 参数配置

添加可配置的 GUC 参数：

```rust
// guc.rs

/// 搜索时选择的最近 cluster 数量
pub static CLUSTER_SEARCH_TOP_K: GucSetting<i32> = GucSetting::new(2);

pub fn init_gucs() {
    Guc::new(
        "diskann.cluster_search_top_k",
        "Number of nearest clusters to search",
        "Select the top K nearest clusters based on centroid distance for search",
        &CLUSTER_SEARCH_TOP_K,
        1,
        100, // 最大搜索所有 clusters
        GucContext::User,
        GucFlags::default(),
    );
}

pub fn get_cluster_search_top_k() -> usize {
    CLUSTER_SEARCH_TOP_K.get() as usize
}
```

### 4. 配置建议

| 场景 | 推荐 K 值 | 说明 |
|------|----------|------|
| 高性能要求 | 1 | 只搜索最近的 cluster，速度最快 |
| 平衡模式 | 2 | 搜索最近的 2 个 clusters，召回率和性能平衡 |
| 高召回率要求 | 3-5 | 搜索更多 clusters，提高召回率 |
| 精确搜索 | 所有 | 搜索所有 clusters，召回率最高 |

### 5. 边界情况处理

#### 5.1 查询向量位于 Cluster 边界

当查询向量位于两个 cluster 的边界时：
- 方案：选择 K=2，搜索最近的 2 个 clusters
- 这样即使最近邻在次近的 cluster 中，也能被找到

#### 5.2 Cluster 数量较少

如果 cluster 数量 <= K：
- 直接搜索所有 clusters

#### 5.3 Centroid 缺失

如果某些 cluster 没有 centroid（异常情况）：
- 跳过这些 cluster
- 如果所有 centroids 都缺失，回退到搜索所有 clusters

### 6. 性能优化

#### 6.1 距离计算优化

- 使用 SIMD 加速距离计算
- 预计算 centroid 的归一化向量（如果使用 cosine 距离）

#### 6.2 缓存优化

- centroids 在 MetaPage 中已经加载，无需额外 I/O

### 7. 召回率分析

#### 7.1 理论分析

假设：
- 数据均匀分布在 N 个 clusters 中
- 查询向量到其最近邻的距离为 d
- Cluster 半径为 R

如果 K=1（只搜索最近的 cluster）：
- 召回率 ≈ 1 - (概率：最近邻在其他 cluster)
- 对于边界附近的查询，召回率可能降低

如果 K=2（搜索最近的 2 个 clusters）：
- 召回率显著提高，因为覆盖了边界区域
- 额外开销：约 2x 的搜索时间（相比 K=1）

#### 7.2 实验验证

需要测试不同 K 值下的召回率：
- K=1: 召回率？
- K=2: 召回率？
- K=3: 召回率？
- K=所有: 召回率？

### 8. 实现步骤

1. **添加 GUC 参数** (`guc.rs`)
   - 添加 `diskann.cluster_search_top_k` 参数

2. **扩展 MetaPage** (`meta_page.rs`)
   - 添加 `get_centroids()` 方法

3. **修改 Graph** (`graph/mod.rs`)
   - 添加 `search_nearest_clusters()` 方法
   - 修改 `greedy_search_streaming_init()` 使用新方法

4. **添加距离计算** (`distance.rs` 或 `graph/mod.rs`)
   - 添加 `compute_distance()` 辅助函数

5. **测试**
   - 测试不同 K 值下的召回率
   - 测试搜索性能
   - 验证边界情况

### 9. 代码示例

完整的修改示例：

```rust
// graph/mod.rs

impl<'a> Graph<'a> {
    pub fn greedy_search_streaming_init<S: Storage>(
        &mut self,
        query: LabeledVector,
        search_list_size: usize,
        storage: &S,
    ) -> ListSearchResult<S::QueryDistanceMeasure, S::LSNPrivateData> {
        let start_nodes = self.get_start_nodes();

        if let Some(start_nodes) = start_nodes {
            // 非 Cluster 构建：原有逻辑
            let start_nodes_vec = start_nodes.get_for_node(query.labels());
            if start_nodes_vec.is_empty() {
                return ListSearchResult::empty();
            }

            let dm = storage.get_query_distance_measure(query);
            let num_neighbors = self.meta_page.get_num_neighbors();
            ListSearchResult::new(
                start_nodes_vec,
                dm,
                None,
                search_list_size,
                num_neighbors,
                self.get_neighbor_store(),
                storage,
            )
        } else {
            // Cluster 构建：使用 Centroid 选择最近的 Clusters
            self.search_nearest_clusters(query, search_list_size, storage)
        }
    }
    
    fn search_nearest_clusters<S: Storage>(
        &mut self,
        query: LabeledVector,
        search_list_size: usize,
        storage: &S,
    ) -> ListSearchResult<S::QueryDistanceMeasure, S::LSNPrivateData> {
        // ... 实现见上文 ...
    }
}
```

### 10. 风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| K 值设置过小 | 召回率下降 | 默认 K=2，提供调优指南 |
| Centroid 计算错误 | 选择错误的 Cluster | 验证 K-Means 聚类质量 |
| 边界情况处理不当 | 遗漏最近邻 | 充分测试边界情况 |
| 性能下降 | 搜索变慢 | 优化距离计算，提供 K=1 选项 |

## 结论

使用 Centroid 选择最近 Clusters 的方案是可行的，能够：

1. **提高召回率**：通过选择多个最近 Clusters（K=2），避免遗漏边界情况的最近邻
2. **保持性能**：只搜索 1-2 个 Clusters，避免搜索所有 Clusters 的开销
3. **灵活配置**：通过 GUC 参数，用户可以根据场景调整 K 值

推荐默认 K=2，在召回率和性能之间取得平衡。
