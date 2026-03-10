# Cluster 独立搜索实现方案

## 目标

实现方案二：每个 cluster 独立搜索，然后合并结果。

## 问题分析

### 当前架构限制

1. **类型系统限制**：`TSVResponseIterator<QDM, PD>` 是泛型结构，创建多个实例并合并结果需要处理不同类型
2. **生命周期限制**：`Graph` 和 `MetaPage` 的借用关系复杂
3. **状态管理**：`StorageState` 枚举需要支持新的 cluster 搜索模式

### 核心挑战

- 如何为每个 cluster 创建独立的搜索上下文
- 如何合并多个 cluster 的搜索结果
- 如何最小化对现有代码的改动

## 设计方案

### 方案：Cluster 搜索结果缓冲

**核心思路**：在 `TSVResponseIterator` 初始化时，预先搜索每个 cluster，收集结果到缓冲区，然后按距离排序返回。

#### 1. 新增结构

```rust
/// Cluster 搜索结果
struct ClusterSearchResult {
    heap_pointer: HeapPointer,
    index_pointer: IndexPointer,
    distance: f32,
}

impl Ord for ClusterSearchResult {
    fn cmp(&self, other: &Self) -> Ordering {
        // 最小堆：距离小的优先
        other.distance.total_cmp(&self.distance)
    }
}
```

#### 2. 修改 TSVResponseIterator

```rust
struct TSVResponseIterator<QDM, PD> {
    // 现有字段...
    lsr: ListSearchResult<QDM, PD>,
    search_list_size: usize,
    
    // 新增：cluster 搜索结果缓冲区
    cluster_results: BinaryHeap<ClusterSearchResult>,
    is_cluster_mode: bool,
}
```

#### 3. 初始化逻辑

```rust
impl<QDM, PD> TSVResponseIterator<QDM, PD> {
    fn new<S: Storage>(...) -> Self {
        let mut meta_page = MetaPage::fetch(index);
        
        // 检查是否是 cluster 构建
        let is_cluster_mode = meta_page.get_start_nodes().is_none();
        
        if is_cluster_mode {
            // Cluster 模式：预先搜索每个 cluster
            let cluster_results = Self::search_all_clusters(
                storage, 
                &mut meta_page, 
                query, 
                search_list_size
            );
            
            Self {
                lsr: ListSearchResult::empty(),
                cluster_results,
                is_cluster_mode: true,
                // ...其他字段
            }
        } else {
            // 非 Cluster 模式：原有逻辑
            let mut graph = Graph::new(GraphNeighborStore::Disk, &mut meta_page);
            let lsr = graph.greedy_search_streaming_init(query, search_list_size, storage);
            
            Self {
                lsr,
                cluster_results: BinaryHeap::new(),
                is_cluster_mode: false,
                // ...其他字段
            }
        }
    }
    
    fn search_all_clusters<S: Storage>(
        storage: &S,
        meta_page: &mut MetaPage,
        query: LabeledVector,
        search_list_size: usize,
    ) -> BinaryHeap<ClusterSearchResult> {
        let cluster_start_nodes = meta_page.get_all_cluster_start_nodes();
        let mut all_results = BinaryHeap::new();
        
        for (_, start_node) in cluster_start_nodes.iter() {
            // 为每个 cluster 创建独立的搜索
            let mut graph = Graph::new(GraphNeighborStore::Disk, meta_page);
            let mut lsr = ListSearchResult::new(
                vec![*start_node],
                // ...
            );
            
            // 搜索这个 cluster
            graph.greedy_search_iterate(&mut lsr, search_list_size, true, None, storage);
            
            // 收集结果
            while let Some((heap_pointer, index_pointer)) = lsr.consume(storage) {
                // 计算距离并添加到结果
                all_results.push(ClusterSearchResult {
                    heap_pointer,
                    index_pointer,
                    distance: /* 从 lsr 获取 */,
                });
            }
        }
        
        all_results
    }
}
```

#### 4. next 方法修改

```rust
fn next<S: Storage>(&mut self, storage: &S) -> Option<(HeapPointer, IndexPointer)> {
    if self.is_cluster_mode {
        // Cluster 模式：从缓冲区返回
        self.cluster_results.pop().map(|r| (r.heap_pointer, r.index_pointer))
    } else {
        // 非 Cluster 模式：原有逻辑
        // ...
    }
}
```

### 问题：距离信息丢失

当前 `ListSearchResult::consume` 返回 `(HeapPointer, IndexPointer)`，不包含距离信息。

**解决方案**：修改 `consume` 方法或添加新方法返回距离。

## 实现步骤

### 步骤 1：添加 ClusterSearchResult 结构

在 `scan.rs` 中添加：

```rust
struct ClusterSearchResult {
    heap_pointer: HeapPointer,
    index_pointer: IndexPointer,
    distance: f32,
}

impl PartialEq for ClusterSearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.heap_pointer == other.heap_pointer
    }
}

impl Eq for ClusterSearchResult {}

impl PartialOrd for ClusterSearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ClusterSearchResult {
    fn cmp(&self, other: &Self) -> Ordering {
        // 最小堆：距离小的优先
        other.distance.total_cmp(&self.distance)
    }
}
```

### 步骤 2：修改 ListSearchResult 添加获取距离的方法

在 `graph/mod.rs` 中添加：

```rust
impl<QDM, PD> ListSearchResult<QDM, PD> {
    /// 获取最近访问节点的距离
    pub fn get_last_visited_distance(&self) -> Option<f32> {
        self.visited.last().map(|n| n.distance_with_tie_break.distance())
    }
}
```

### 步骤 3：修改 TSVResponseIterator

添加 cluster 模式支持。

### 步骤 4：实现 search_all_clusters

为每个 cluster 独立搜索。

### 步骤 5：修改 next 方法

支持 cluster 模式。

## 风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 内存使用增加 | 预先搜索所有 cluster 需要存储更多结果 | 限制每个 cluster 的搜索结果数量 |
| 搜索时间增加 | 需要搜索所有 cluster | 使用 GUC 参数控制搜索深度 |
| 类型系统复杂性 | 泛型处理复杂 | 使用 trait object 或 enum 简化 |

## 替代方案

如果改动太大，可以考虑：

1. **创建新的 ClusterTSVResponseIterator**：专门处理 cluster 搜索
2. **使用 trait object**：动态分发不同类型的 iterator
3. **延迟搜索**：按需搜索每个 cluster，而不是预先搜索所有

## 结论

推荐采用"Cluster 搜索结果缓冲"方案，在 `TSVResponseIterator` 初始化时预先搜索每个 cluster，收集结果到缓冲区，然后按距离排序返回。

这样可以：
1. 最小化对现有代码的改动
2. 确保每个 cluster 都被充分搜索
3. 保持现有的 API 接口不变
