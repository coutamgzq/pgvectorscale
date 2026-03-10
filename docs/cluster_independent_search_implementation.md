# Cluster 独立搜索实现文档

## 实现概述

实现了方案二：每个 cluster 独立搜索，然后合并结果。这种方案确保每个 cluster 都被充分搜索，从而提高召回率。

## 核心改动

### 1. ClusterSearchResult 结构 (scan.rs)

```rust
/// Result from searching a single cluster
struct ClusterSearchResult {
    heap_pointer: HeapPointer,
    index_pointer: IndexPointer,
    distance: f32,
}

impl Ord for ClusterSearchResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min heap: smaller distance has higher priority
        other.distance.total_cmp(&self.distance)
    }
}
```

### 2. ListSearchResult 扩展 (graph/mod.rs)

添加了 `consume_with_distance` 方法返回距离：

```rust
/// Consumes and returns the first element with its distance.
pub fn consume_with_distance<S: Storage>(
    &mut self,
    storage: &S,
) -> Option<(HeapPointer, IndexPointer, f32)> {
    if self.visited.is_empty() {
        return None;
    }
    let lsn = self.visited.remove(0);
    let distance = lsn.distance_with_tie_break.get_distance();
    let heap_pointer = storage.return_lsn(&lsn, &mut self.stats);
    Some((heap_pointer, lsn.index_pointer, distance))
}
```

同时将 `empty()`, `new()`, `is_empty()` 方法改为 public。

### 3. TSVResponseIterator 修改 (scan.rs)

#### 新增字段

```rust
struct TSVResponseIterator<QDM, PD> {
    // ... 现有字段
    cluster_results: BinaryHeap<ClusterSearchResult>,
    is_cluster_mode: bool,
}
```

#### 初始化逻辑

```rust
fn new<S: Storage>(...) -> Self {
    let is_cluster_mode = meta_page.get_start_nodes().is_none();

    if is_cluster_mode {
        // Cluster 模式：预先搜索每个 cluster
        let cluster_results = Self::search_all_clusters(
            storage, &mut meta_page, query, search_list_size, !has_label_filter
        );

        Self {
            lsr: ListSearchResult::empty(),
            cluster_results,
            is_cluster_mode: true,
            // ...
        }
    } else {
        // 非 Cluster 模式：原有逻辑
        // ...
    }
}
```

#### search_all_clusters 方法

```rust
fn search_all_clusters<S: Storage>(
    storage: &S,
    meta_page: &mut MetaPage,
    query: LabeledVector,
    search_list_size: usize,
    no_filter: bool,
) -> BinaryHeap<ClusterSearchResult> {
    let cluster_start_nodes = meta_page.get_all_cluster_start_nodes();
    // 预先收集 start nodes 避免借用冲突
    let start_nodes_vec: Vec<(u32, IndexPointer)> = cluster_start_nodes
        .iter()
        .map(|(&id, &node)| (id, node))
        .collect();

    let mut all_results = BinaryHeap::new();

    for (_cluster_id, start_node) in start_nodes_vec {
        // 为每个 cluster 创建独立的搜索
        let mut lsr = ListSearchResult::new(
            vec![start_node],
            dm,
            None,
            search_list_size,
            num_neighbors,
            &mut GraphNeighborStore::Disk,
            storage,
        );

        let mut graph = Graph::new(GraphNeighborStore::Disk, meta_page);

        loop {
            graph.greedy_search_iterate(&mut lsr, ...);

            // 收集结果（包含距离）
            while let Some((heap_pointer, index_pointer, distance)) =
                lsr.consume_with_distance(storage)
            {
                all_results.push(ClusterSearchResult { ... });
            }

            if lsr.is_empty() {
                break;
            }
        }
    }

    all_results
}
```

#### next 方法修改

```rust
fn next<S: Storage>(&mut self, storage: &S) -> Option<(HeapPointer, IndexPointer)> {
    if self.is_cluster_mode {
        // Cluster 模式：从缓冲区返回（已按距离排序）
        return self.cluster_results.pop().map(|r| (r.heap_pointer, r.index_pointer));
    }

    // 非 Cluster 模式：原有逻辑
    // ...
}
```

## 算法流程

```
Cluster 搜索流程：
1. 检测是否是 Cluster 构建（meta_page.get_start_nodes().is_none()）
2. 如果是 Cluster 模式：
   a. 获取所有 cluster 的 start nodes
   b. 对每个 cluster：
      - 创建独立的 ListSearchResult
      - 执行贪心搜索直到搜索完成
      - 收集所有结果（包含距离）
   c. 将所有结果按距离排序放入 BinaryHeap
   d. 后续调用 next() 时从堆中返回结果
3. 如果不是 Cluster 模式：
   - 使用原有逻辑
```

## 关键设计决策

### 1. 预先搜索 vs 延迟搜索

选择**预先搜索**所有 cluster，原因：
- 简化实现，避免复杂的状态管理
- 确保结果按距离正确排序
- 对大多数查询场景（LIMIT 较小）性能可接受

### 2. 借用冲突处理

通过预先收集 start nodes 解决借用冲突：

```rust
// 避免在循环中持有 cluster_start_nodes 的不可变借用
let start_nodes_vec: Vec<(u32, IndexPointer)> = cluster_start_nodes
    .iter()
    .map(|(&id, &node)| (id, node))
    .collect();

// 然后可以安全地可变借用 meta_page
for (_cluster_id, start_node) in start_nodes_vec {
    let mut graph = Graph::new(GraphNeighborStore::Disk, meta_page);
    // ...
}
```

### 3. 内存考虑

- 所有 cluster 的搜索结果存储在 BinaryHeap 中
- 对于大数据集，可能需要考虑限制每个 cluster 的结果数量

## 性能特点

### 优点
1. **高召回率**：每个 cluster 都被充分搜索
2. **结果正确排序**：按距离返回结果
3. **代码简洁**：最小化对现有代码的改动

### 缺点
1. **初始化时间增加**：需要预先搜索所有 cluster
2. **内存使用增加**：存储所有 cluster 的搜索结果

## 未来优化

1. **延迟搜索**：按需搜索每个 cluster，而不是预先搜索
2. **结果数量限制**：限制每个 cluster 返回的结果数量
3. **并行搜索**：并行搜索多个 cluster
4. **早停优化**：当找到足够好的结果时提前停止

## 测试建议

1. **召回率测试**：对比 cluster 模式和非 cluster 模式的召回率
2. **性能测试**：测量初始化时间和搜索时间
3. **内存测试**：监控内存使用情况
4. **边界测试**：测试空 cluster、单个 cluster 等边界情况
