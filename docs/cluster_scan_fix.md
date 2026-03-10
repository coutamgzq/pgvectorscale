# Cluster 构建的扫描问题分析与修复

## 问题描述

对于带 cluster 构建的图，start nodes 保存在 `MetaPage.cluster_start_nodes: BTreeMap<u32, ItemPointer>` 中，而不是 `MetaPage.start_nodes: Option<StartNodes>` 中。

但在扫描时，`greedy_search_streaming_init` 函数调用 `self.get_start_nodes()`，它只返回 `start_nodes` 字段，忽略了 `cluster_start_nodes`。

## 问题分析

### 当前代码流程

1. **TSVResponseIterator::new** (scan.rs:177)
   - 创建 `Graph` 实例
   - 调用 `graph.greedy_search_streaming_init(query, search_list_size, storage)`

2. **greedy_search_streaming_init** (graph/mod.rs:331)
   - 调用 `self.get_start_nodes()`
   - 如果 `start_nodes` 为 `None`，返回空结果
   - 否则，从 `start_nodes` 开始搜索

### 问题

对于 cluster 构建：
- `start_nodes` 为 `None`（因为我们跳过了 `update_start_nodes` 的存储）
- `cluster_start_nodes` 包含了所有 cluster 的 start nodes
- 但 `greedy_search_streaming_init` 只检查 `start_nodes`，忽略了 `cluster_start_nodes`

这导致：
1. 扫描时无法找到任何节点（因为 `start_nodes` 为 `None`）
2. 或者只能找到单个 start node（如果设置了 `start_nodes`），无法遍历所有 cluster

## 解决方案

### 重要发现：`ListSearchResult::new` 已支持多个 start nodes

查看 `ListSearchResult::new` 函数签名：
```rust
fn new<S: Storage<QueryDistanceMeasure = QDM, LSNPrivateData = PD>>(
    start_nodes: Vec<ItemPointer>,  // 接受 Vec<ItemPointer>，支持多个 start nodes
    sdm: S::QueryDistanceMeasure,
    tie_break_item_pointer: Option<ItemPointer>,
    search_list_size: usize,
    num_neighbors: u32,
    gns: &mut GraphNeighborStore,
    storage: &S,
) -> Self
```

`ListSearchResult::new` 已经接受 `Vec<ItemPointer>`，这意味着它本身就支持多个 start nodes！

### 推荐方案：修改 `greedy_search_streaming_init`

修改 `greedy_search_streaming_init` 函数，使其：
1. 首先检查 `start_nodes`
2. 如果 `start_nodes` 为 `None`，检查 `cluster_start_nodes`
3. 如果 `cluster_start_nodes` 不为空，收集所有 cluster 的 start nodes
4. 使用所有 start nodes 创建 `ListSearchResult`

```rust
pub fn greedy_search_streaming_init<S: Storage>(
    &mut self,
    query: LabeledVector,
    search_list_size: usize,
    storage: &S,
) -> ListSearchResult<S::QueryDistanceMeasure, S::LSNPrivateData> {
    // Try to get start nodes from start_nodes field first
    let start_nodes = self.get_start_nodes();
    
    let start_nodes_vec: Vec<ItemPointer> = if let Some(start_nodes) = start_nodes {
        // Non-cluster build: use start_nodes
        start_nodes.get_for_node(query.labels())
    } else {
        // Cluster build: use cluster_start_nodes
        let cluster_start_nodes = self.meta_page.get_all_cluster_start_nodes();
        if cluster_start_nodes.is_empty() {
            return ListSearchResult::empty();
        }
        // Collect all cluster start nodes
        cluster_start_nodes.values().copied().collect()
    };
    
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
}
```

### 方案优势

1. **简单**：只需要修改一个函数
2. **有效**：`ListSearchResult` 已经支持多个 start nodes
3. **兼容**：非 cluster 构建继续正常工作
4. **完整**：扫描时会遍历所有 cluster 的图

## 需要修改的文件

1. **src/access_method/graph/mod.rs**
   - 修改 `greedy_search_streaming_init` 函数
   - 添加对 `cluster_start_nodes` 的支持

## ListSearchResult 功能与实现原理分析

### 结构定义

```rust
pub struct ListSearchResult<QDM, PD> {
    candidates: BinaryHeap<Reverse<ListSearchNeighbor<PD>>>,  // 候选节点（最小堆）
    visited: Vec<ListSearchNeighbor<PD>>,                     // 已访问节点（按距离排序）
    inserted: HashSet<ItemPointer>,                           // 已插入节点（去重）
    pub sdm: Option<QDM>,                                     // 距离度量
    tie_break_item_pointer: Option<ItemPointer>,              // 用于距离相同时的排序
    pub stats: GreedySearchStats,                             // 搜索统计
    pub prune_stats: PruneNeighborStats,                      // 剪枝统计
}
```

### 核心功能

#### 1. 初始化（new 方法）

```rust
fn new<S: Storage<QueryDistanceMeasure = QDM, LSNPrivateData = PD>>(
    start_nodes: Vec<ItemPointer>,  // 多个起始节点
    sdm: S::QueryDistanceMeasure,
    tie_break_item_pointer: Option<ItemPointer>,
    search_list_size: usize,
    num_neighbors: u32,
    gns: &mut GraphNeighborStore,
    storage: &S,
) -> Self
```

**初始化流程**：
1. 为每个 `start_node` 调用 `storage.create_lsn_for_start_node()` 创建 `ListSearchNeighbor`
2. 使用 `lsr.prepare_insert(index_pointer)` 检查节点是否已处理（去重）
3. 将有效的 start node 通过 `insert_neighbor(lsn)` 加入候选队列

**关键代码**：
```rust
for index_pointer in start_nodes {
    let lsn = storage.create_lsn_for_start_node(&mut res, index_pointer, gns);
    if let Some(lsn) = lsn {
        res.insert_neighbor(lsn);
    }
}
```

#### 2. 候选节点管理

**insert_neighbor**：将节点加入候选队列（最小堆）
```rust
pub fn insert_neighbor(&mut self, n: ListSearchNeighbor<PD>) {
    self.stats.record_candidate();
    self.candidates.push(Reverse(n));  // Reverse 使最小堆按距离升序排列
}
```

**prepare_insert**：检查节点是否已处理（去重）
```rust
pub fn prepare_insert(&mut self, ip: ItemPointer) -> bool {
    self.inserted.insert(ip)  // HashSet::insert 返回 true 表示之前不存在
}
```

#### 3. 贪心搜索迭代（visit_closest）

```rust
fn visit_closest(&mut self, pos_limit: usize) -> Option<usize> {
    if self.candidates.is_empty() {
        return None;
    }

    // 如果已访问节点数超过限制，检查是否需要继续
    if self.visited.len() > pos_limit {
        let node_at_pos = &self.visited[pos_limit - 1];
        let head = self.candidates.peek().unwrap();
        if head.0 >= *node_at_pos {
            return None;  // 候选队列中的最近节点比已访问的第 pos_limit 个节点还远
        }
    }

    // 弹出最近的候选节点
    let head = self.candidates.pop().unwrap();
    // 在 visited 数组中找到插入位置（保持有序）
    let idx = self.visited.partition_point(|x| *x < head.0);
    self.visited.insert(idx, head.0);
    Some(idx)
}
```

#### 4. 结果消费（consume）

```rust
pub fn consume<S: Storage<QueryDistanceMeasure = QDM, LSNPrivateData = PD>>(
    &mut self,
    storage: &S,
) -> Option<(HeapPointer, IndexPointer)> {
    if self.visited.is_empty() {
        return None;
    }
    // 移除并返回最近的已访问节点
    let lsn = self.visited.remove(0);
    let heap_pointer = storage.return_lsn(&lsn, &mut self.stats);
    Some((heap_pointer, lsn.index_pointer))
}
```

### 贪心搜索算法流程

1. **初始化阶段**（`greedy_search_streaming_init`）：
   - 从所有 start nodes 创建初始候选队列
   - 计算每个 start node 到查询向量的距离

2. **迭代阶段**（`greedy_search_iterate`）：
   ```rust
   while let Some(list_search_entry_idx) = lsr.visit_closest(visit_n_closest) {
       lsr.stats.record_visit();
       storage.visit_lsn(lsr, list_search_entry_idx, &mut self.neighbor_store, no_filter);
   }
   ```
   - 每次从候选队列中取出最近的节点
   - 访问该节点的所有邻居
   - 将邻居加入候选队列（如果未访问过）
   - 重复直到访问了足够多的节点

3. **结果返回阶段**（`consume`）：
   - 按距离顺序返回已访问的节点

### 多 Start Node 支持原理

`ListSearchResult` 天然支持多个 start nodes：

1. **初始化时**：遍历所有 start nodes，为每个创建 `ListSearchNeighbor`
2. **去重机制**：`inserted` HashSet 确保同一节点不会被重复处理
3. **距离排序**：`candidates` 使用 `BinaryHeap<Reverse<...>>` 实现最小堆，始终优先处理距离最近的节点
4. **统一搜索**：无论有多少个 start node，它们都被平等地加入候选队列，搜索过程统一处理

**示例**：
- Cluster 0 start node: distance = 0.5
- Cluster 1 start node: distance = 0.3
- Cluster 2 start node: distance = 0.7

候选队列初始状态（按距离排序）：
1. Cluster 1 start node (0.3)
2. Cluster 0 start node (0.5)
3. Cluster 2 start node (0.7)

搜索会优先从 Cluster 1 开始扩展，然后自然地扩展到其他 cluster 的节点。

### 为什么支持 Cluster 扫描

当传入多个 cluster 的 start nodes 时：
1. 所有 start nodes 被同时加入候选队列
2. 距离查询向量最近的 start node 会被优先处理
3. 搜索会自然地从最近的 cluster 向外扩展
4. 最终遍历所有 cluster 中距离查询向量最近的节点

这种设计使得 `ListSearchResult` 天然支持从多个起点同时搜索，非常适合 cluster 构建的图扫描。

## 验证

修复后：
- 非 cluster 构建的扫描继续正常工作
- Cluster 构建的扫描能够遍历所有 cluster 的图
- 搜索结果包含所有 cluster 的相关节点
