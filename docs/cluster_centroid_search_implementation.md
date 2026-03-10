# Cluster Centroid 搜索实现文档

## 实现概述

基于 Centroid 的 Cluster 搜索优化已实施，用于提高 Cluster 构建的 DiskANN 索引召回率。

## 核心改动

### 1. GUC 参数 (guc.rs)

添加新参数 `diskann.cluster_search_top_k`：

```rust
pub static TSV_CLUSTER_SEARCH_TOP_K: GucSetting<i32> = GucSetting::<i32>::new(2);
```

- **默认值**: 2
- **范围**: 1-1024
- **说明**: 基于 Centroid 距离选择最近的 K 个 Cluster 进行搜索
- **上下文**: UserSet (用户可动态调整)

### 2. MetaPage 扩展 (meta_page.rs)

添加方法获取指定 Cluster 的 Centroid：

```rust
/// Get centroid for a specific cluster
pub fn get_centroid(&self, cluster_id: u32) -> Option<&Vec<f32>> {
    self.centroids.get(cluster_id as usize)
}
```

注意：`get_centroids()` 方法已存在，无需重复添加。

### 3. Graph 搜索优化 (graph/mod.rs)

#### 修改 `greedy_search_streaming_init`

```rust
pub fn greedy_search_streaming_init<S: Storage>(...) {
    let start_nodes = self.get_start_nodes();

    if let Some(start_nodes) = start_nodes {
        // 非 Cluster 构建：原有逻辑
        // ...
    } else {
        // Cluster 构建：基于 Centroid 选择最近的 Clusters
        self.search_nearest_clusters(query, search_list_size, storage)
    }
}
```

#### 新增 `search_nearest_clusters` 方法

```rust
/// Search nearest clusters based on centroid distance.
fn search_nearest_clusters<S: Storage>(
    &mut self,
    query: LabeledVector,
    search_list_size: usize,
    storage: &S,
) -> ListSearchResult<S::QueryDistanceMeasure, S::LSNPrivateData> {
    let cluster_start_nodes = self.meta_page.get_all_cluster_start_nodes();
    let centroids = self.meta_page.get_centroids();

    // 计算查询向量到所有 Centroids 的距离
    let mut cluster_distances: Vec<(f32, u32, ItemPointer)> = Vec::new();
    let query_vec = query.vec().to_index_slice();

    for (cluster_id, start_node) in cluster_start_nodes.iter() {
        if let Some(centroid) = centroids.get(*cluster_id as usize) {
            let dist = distance_fn(query_vec, centroid);
            cluster_distances.push((dist, *cluster_id, *start_node));
        }
    }

    // 按距离排序，选择最近的 K 个
    cluster_distances.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Equal));
    
    let k = guc::TSV_CLUSTER_SEARCH_TOP_K.get() as usize;
    let selected_start_nodes: Vec<ItemPointer> = cluster_distances
        .into_iter()
        .take(k)
        .map(|(_, _, start_node)| start_node)
        .collect();

    // 使用选中的 Clusters 创建 ListSearchResult
    ListSearchResult::new(selected_start_nodes, ...)
}
```

## 算法流程

```
搜索流程：
1. 获取查询向量 Q
2. 计算 Q 到所有 Centroids 的距离
3. 按距离排序，选择最近的 K 个 Centroids
4. 只在这 K 个 Clusters 中执行贪心搜索
5. 返回结果
```

## 配置建议

| 场景 | K 值 | 说明 |
|------|------|------|
| 高性能 | 1 | 只搜索最近 Cluster，速度最快 |
| 平衡模式 | 2 (默认) | 搜索最近 2 Clusters，召回率和性能平衡 |
| 高召回率 | 3-5 | 搜索更多 Clusters |
| 精确搜索 | 所有 | 搜索所有 Clusters |

## 使用方式

```sql
-- 设置搜索的 Cluster 数量
SET diskann.cluster_search_top_k = 2;

-- 查询向量
SELECT * FROM items 
ORDER BY embedding <=> '[1,2,3]' 
LIMIT 10;
```

## 性能与召回率权衡

### K=1 (最近 Cluster)
- **优点**: 搜索速度最快
- **缺点**: 可能遗漏边界情况的最近邻
- **适用**: 高性能要求场景

### K=2 (默认)
- **优点**: 平衡召回率和性能
- **缺点**: 搜索时间约 2x
- **适用**: 一般场景

### K=3-5
- **优点**: 召回率更高
- **缺点**: 搜索时间增加
- **适用**: 高召回率要求场景

## 边界情况处理

1. **Cluster 数量 < K**: 自动调整为可用 Cluster 数量
2. **Centroid 缺失**: 跳过该 Cluster
3. **所有 Centroids 缺失**: 返回空结果

## 测试建议

1. **召回率测试**: 对比不同 K 值下的召回率
2. **性能测试**: 测量不同 K 值下的搜索时间
3. **边界测试**: 测试查询向量位于 Cluster 边界的情况

## 未来优化

1. **自适应 K**: 根据查询向量到最近 Centroid 的距离动态调整 K
2. **并行搜索**: 并行搜索多个 Clusters
3. **缓存优化**: 缓存 Centroid 距离计算结果
