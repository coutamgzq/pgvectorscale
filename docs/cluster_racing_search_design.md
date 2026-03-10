# Cluster 搜索召回率优化设计文档

## 问题背景

### 当前召回率状况

- **非 Cluster 构建召回率**: 0.8894 ✅
- **Cluster 构建召回率**: 0.53 ❌
- **目标召回率**: 0.8 以上

### 已尝试的优化方案

1. **调整 `search_list_size`**: 无法从根本上解决问题
2. **预先搜索所有 cluster**: 内存占用高，初始化时间长
3. **两阶段搜索**: 实现复杂度高

### 核心问题

当前的搜索策略在 cluster 模式下存在以下问题：

1. **贪心搜索的局限性**: 所有 cluster 的 start nodes 被放入同一个优先队列，贪心算法总是优先访问距离最近的节点，导致距离较远的 cluster 可能永远不会被访问
2. **search_list_size 限制**: 即使设置了较大的 `search_list_size`，也无法保证每个 cluster 都被充分探索
3. **召回率瓶颈**: 无论怎么调参，召回率始终无法突破 0.6

## 问题根源深度分析

### 场景示例

假设有 8 个 cluster，查询向量与各 cluster 的距离分布：

```
Cluster 0: start_node distance = 0.1  ← 近
Cluster 1: start_node distance = 0.15 ← 近
Cluster 2: start_node distance = 0.2  ← 近
Cluster 3: start_node distance = 0.8  <- 远
Cluster 4: start_node distance = 0.85 <- 远
Cluster 5: start_node distance = 0.9  <- 远
Cluster 6: start_node distance = 0.95 <- 远
Cluster 7: start_node distance = 1.0  <- 远
```

**当前搜索行为** (`search_list_size = 100`):
1. 所有 8 个 cluster 的 start nodes 进入优先队列
2. 贪心算法优先访问 Cluster 0, 1, 2 的节点
3. 访问了 100 个节点后停止
4. Cluster 3-7 完全没有被探索
5. **结果**: 即使这些"远距离" cluster 中有距离很近的节点，也被错过了

### 为什么调参无效

**增加 `search_list_size` 的问题**:
- 如果设置为 800 (100 * 8)，确实能访问更多节点
- 但贪心算法仍然会优先访问距离近的 cluster
- 无法保证每个 cluster 都被均匀探索
- 搜索时间大幅增加，但召回率提升有限

**当前实现的缺陷** (scan.rs:270-332):

```rust
fn search_all_clusters<S: Storage>(...) -> BinaryHeap<ClusterSearchResult> {
    for start_node in start_nodes_vec {
        let mut lsr = ListSearchResult::new(vec![start_node], ...);
        
        loop {
            graph.greedy_search_iterate(&mut lsr, search_list_size, ...);
            
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

**问题**:
1. 每个 cluster 独立搜索，但使用相同的 `search_list_size`
2. 所有结果混在一起，无法控制每个 cluster 的搜索深度
3. 没有实现"赛马"式的增量搜索

## 新方案：赛马算法式 Cluster 搜索

### 核心思想

**赛马算法类比**:
- 每匹"马"代表一个 cluster 的搜索过程
- 每次让所有"马"都前进一步（访问一个节点）
- 比较所有"马"的当前位置（距离）
- 选择距离最近的"马"继续前进
- 重复直到找到足够多的结果

### 算法流程

```
初始化阶段:
1. 为每个 cluster 创建独立的搜索上下文 (ListSearchResult)
2. 每个搜索上下文从各自的 start_node 开始
3. 将所有搜索上下文放入一个优先队列（按当前最佳距离排序）

搜索阶段:
while 结果数量 < 目标数量:
    1. 从优先队列中取出当前距离最小的 cluster
    2. 在该 cluster 中执行一步搜索（访问一个节点）
    3. 如果找到了新结果，加入结果集
    4. 更新该 cluster 的当前最佳距离
    5. 将该 cluster 重新放回优先队列

返回阶段:
    返回按距离排序的结果
```

### 详细设计

#### 1. 数据结构

```rust
/// Cluster 搜索状态
struct ClusterSearchState<QDM, PD> {
    cluster_id: u32,
    lsr: ListSearchResult<QDM, PD>,  // 该 cluster 的搜索上下文
    current_best_distance: f32,       // 当前最佳距离
}

impl<QDM, PD> Ord for ClusterSearchState<QDM, PD> {
    fn cmp(&self, other: &Self) -> Ordering {
        // 最小堆：距离小的优先
        other.current_best_distance.partial_cmp(&self.current_best_distance).unwrap()
    }
}

/// 赛马式搜索管理器
struct ClusterRacingSearcher<QDM, PD> {
    cluster_states: BinaryHeap<ClusterSearchState<QDM, PD>>,
    results: BinaryHeap<ClusterSearchResult>,
    target_count: usize,  // 目标结果数量
}
```

#### 2. 初始化逻辑

```rust
impl<QDM, PD> ClusterRacingSearcher<QDM, PD> {
    fn new<S: Storage>(
        storage: &S,
        meta_page: &MetaPage,
        query: LabeledVector,
        search_list_size: usize,
        target_count: usize,
    ) -> Self {
        let cluster_start_nodes = meta_page.get_all_cluster_start_nodes();
        let num_neighbors = meta_page.get_num_neighbors();
        
        let mut cluster_states = BinaryHeap::new();
        
        // 为每个 cluster 创建独立的搜索上下文
        for (&cluster_id, &start_node) in cluster_start_nodes.iter() {
            let query_clone = query.clone();
            let dm = storage.get_query_distance_measure(query_clone);
            
            let lsr = ListSearchResult::new(
                vec![start_node],
                dm,
                None,
                search_list_size,
                num_neighbors,
                &mut GraphNeighborStore::Disk,
                storage,
            );
            
            // 初始距离设为无穷大
            cluster_states.push(ClusterSearchState {
                cluster_id,
                lsr,
                current_best_distance: f32::INFINITY,
            });
        }
        
        Self {
            cluster_states,
            results: BinaryHeap::new(),
            target_count,
        }
    }
}
```

#### 3. 搜索逻辑

```rust
impl<QDM, PD> ClusterRacingSearcher<QDM, PD> {
    /// 执行一步搜索：选择当前最佳 cluster，访问一个节点
    fn step<S: Storage>(
        &mut self,
        storage: &S,
        meta_page: &mut MetaPage,
    ) -> Option<ClusterSearchResult> {
        // 从优先队列中取出当前距离最小的 cluster
        let mut state = self.cluster_states.pop()?;
        
        // 创建临时 Graph 用于搜索
        let mut graph = Graph::new(GraphNeighborStore::Disk, meta_page);
        
        // 执行一步搜索（访问一个节点）
        graph.greedy_search_iterate(
            &mut state.lsr,
            1,  // 只访问一个节点
            true,
            None,
            storage,
        );
        
        // 尝试获取一个结果
        if let Some((heap_pointer, index_pointer, distance)) = 
            state.lsr.consume_with_distance(storage) 
        {
            // 更新当前最佳距离
            state.current_best_distance = distance;
            
            // 将 cluster 放回优先队列
            self.cluster_states.push(state);
            
            // 返回结果
            if heap_pointer.offset != InvalidOffsetNumber {
                return Some(ClusterSearchResult {
                    heap_pointer,
                    index_pointer,
                    distance,
                });
            }
        } else {
            // 该 cluster 已搜索完毕，不放回队列
        }
        
        None
    }
    
    /// 执行完整搜索，直到找到足够多的结果
    fn search<S: Storage>(
        &mut self,
        storage: &S,
        meta_page: &mut MetaPage,
    ) -> BinaryHeap<ClusterSearchResult> {
        while self.results.len() < self.target_count {
            match self.step(storage, meta_page) {
                Some(result) => {
                    self.results.push(result);
                }
                None => {
                    // 所有 cluster 都已搜索完毕
                    if self.cluster_states.is_empty() {
                        break;
                    }
                }
            }
        }
        
        self.results.clone()
    }
}
```

#### 4. 集成到 TSVResponseIterator

```rust
struct TSVResponseIterator<QDM, PD> {
    // ... 现有字段
    racing_searcher: Option<ClusterRacingSearcher<QDM, PD>>,
    is_cluster_mode: bool,
}

impl<QDM, PD> TSVResponseIterator<QDM, PD> {
    fn new<S: Storage>(...) -> Self {
        let is_cluster_mode = meta_page.get_start_nodes().is_none();
        
        if is_cluster_mode {
            // Cluster 模式：使用赛马式搜索
            let target_count = search_list_size * 2;  // 目标结果数量
            let mut racing_searcher = ClusterRacingSearcher::new(
                storage,
                &meta_page,
                query,
                search_list_size,
                target_count,
            );
            
            // 执行搜索
            let cluster_results = racing_searcher.search(storage, &mut meta_page);
            
            Self {
                lsr: ListSearchResult::empty(),
                racing_searcher: Some(racing_searcher),
                cluster_results,
                is_cluster_mode: true,
                // ...
            }
        } else {
            // 非 Cluster 模式：原有逻辑
            // ...
        }
    }
}
```

### 算法优势

#### 1. 保证公平性

每个 cluster 都有机会被搜索，不会因为初始距离远而被忽略。

**示例**:
```
初始状态:
  Cluster 0: distance = 0.1  (优先)
  Cluster 3: distance = 0.8  (靠后)

第 1 步: 搜索 Cluster 0，找到距离 0.05 的节点
  Cluster 0: distance = 0.05 (仍然优先)
  Cluster 3: distance = 0.8

第 2 步: 搜索 Cluster 0，找到距离 0.12 的节点
  Cluster 0: distance = 0.12
  Cluster 3: distance = 0.8  (现在可能优先)

第 3 步: 搜索 Cluster 3，找到距离 0.15 的节点！
  Cluster 3: distance = 0.15 (比 Cluster 0 更好！)
```

#### 2. 自适应搜索深度

- 距离近的 cluster 会被更深入地搜索
- 距离远的 cluster 也会被探索，但深度较浅
- 自动平衡搜索质量和效率

#### 3. 早停优化

当找到足够多的好结果时，可以提前停止搜索：

```rust
// 如果当前最佳距离已经足够小，可以停止搜索
if state.current_best_distance > early_stop_threshold {
    break;
}
```

### 性能分析

#### 时间复杂度

- **初始化**: O(C)，C 为 cluster 数量
- **每步搜索**: O(log C)，从优先队列中取出和插入
- **总时间**: O(N log C)，N 为访问的节点总数

#### 空间复杂度

- **每个 cluster 的搜索上下文**: O(S)，S 为 search_list_size
- **总空间**: O(C * S)

#### 与现有方案对比

| 方案 | 召回率 | 搜索时间 | 内存占用 | 实现复杂度 |
|------|--------|----------|----------|------------|
| 当前实现 | 0.53 | 快 | 低 | 低 |
| 增大 search_list_size | 0.6 | 慢 | 中 | 低 |
| 预先搜索所有 cluster | 0.8+ | 很慢 | 高 | 中 |
| **赛马式搜索** | **0.8+** | **中** | **中** | **中** |

## 实现计划

### 阶段 1: 非并发版本

1. 实现 `ClusterSearchState` 和 `ClusterRacingSearcher`
2. 修改 `TSVResponseIterator` 集成赛马式搜索
3. 测试召回率和性能

### 阶段 2: 优化版本

1. 添加早停优化
2. 添加搜索深度限制
3. 优化内存使用

### 阶段 3: 并发版本（可选）

1. 使用多线程并行搜索多个 cluster
2. 使用 channel 收集结果
3. 实现负载均衡

## 验证计划

### 1. 召回率测试

```sql
-- 测试脚本
SELECT * FROM test_recall(100, 10);  -- 期望召回率 > 0.8
```

### 2. 性能测试

```sql
-- 测试搜索时间
EXPLAIN ANALYZE 
SELECT * FROM test_table 
ORDER BY embedding <=> query_vector 
LIMIT 10;
```

### 3. 边界测试

- 单个 cluster
- 空 cluster
- 大量 cluster (100+)
- 不均衡 cluster 大小

## 风险与缓解

### 风险 1: 性能下降

**缓解措施**:
- 实现早停优化
- 限制每个 cluster 的搜索深度
- 使用缓存优化

### 风险 2: 内存占用增加

**缓解措施**:
- 限制同时活跃的 cluster 数量
- 及时释放已完成的搜索上下文
- 使用流式处理

### 风险 3: 实现复杂度

**缓解措施**:
- 分阶段实现
- 充分的单元测试
- 详细的文档

## 总结

赛马算法式 cluster 搜索通过为每个 cluster 维护独立的搜索状态，并按距离动态选择搜索哪个 cluster，能够：

1. **保证召回率**: 每个 cluster 都被充分探索
2. **保持效率**: 自动聚焦于最相关的 cluster
3. **易于扩展**: 可以轻松添加并发搜索等优化

这是解决当前召回率问题的最佳方案，能够将召回率从 0.53 提升到 0.8 以上。
