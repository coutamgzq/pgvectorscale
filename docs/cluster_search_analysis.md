# Cluster 独立搜索实现分析

## 问题背景

实现了每个 cluster 独立搜索的方案，但发现召回率从 0.4832 降低到了 0.1292，远低于预期的 0.8+。

## 根本原因分析

### 原来的搜索策略（非 cluster 模式）

在原来的 `next()` 方法中：
```rust
loop {
    graph.greedy_search_iterate(&mut self.lsr, self.search_list_size, ...);
    let item = self.lsr.consume(_storage);
    // 返回一个结果
}
```

**关键点**：
1. **流式搜索**：每次调用 `next()` 只返回一个结果
2. **渐进式扩展**：`greedy_search_iterate` 会扩展 `search_list_size` 个节点，但 `visited` 队列保持这些节点
3. **动态深度**：第 N 次调用 `next()` 时，已经扩展了 `N * search_list_size` 个节点
4. **持续扩展**：`greedy_search_iterate` 从 `visited` 中的节点继续扩展邻居

### 错误的实现（第一次）

```rust
loop {
    graph.greedy_search_iterate(&mut lsr, search_list_size, ...);
    
    // 错误：一次性消费所有结果
    while let Some((...)) = lsr.consume_with_distance(storage) {
        // 收集所有结果
    }
    
    if lsr.is_empty() { break; }
}
```

**问题**：
1. **一次性消费**：`while` 循环消费了 `visited` 中的所有节点
2. **无法继续扩展**：`greedy_search_iterate` 只能从 `visited` 中的节点扩展，但 `visited` 已被清空
3. **搜索深度不足**：每个 cluster 实际上只搜索了 `search_list_size` 个节点

### 正确的实现（修复后）

```rust
loop {
    // 扩展更多节点
    graph.greedy_search_iterate(&mut lsr, search_list_size, no_filter, None, storage);
    
    // 只消费一个结果（模仿原来的 next() 方法）
    match lsr.consume_with_distance(storage) {
        Some((...)) => { /* 处理结果 */ }
        None => {
            if lsr.is_empty() { break; }
            continue; // 继续扩展
        }
    }
}
```

**改进**：
1. **一次消费一个结果**：模仿原来的 `next()` 方法
2. **持续扩展**：当 `visited` 为空时，再次调用 `greedy_search_iterate` 继续扩展
3. **保持搜索深度**：确保每个 cluster 能够搜索足够的节点

## 关于是否访问所有 Cluster 的问题

### 当前实现的行为

**是的，当前实现会访问所有 cluster**，即使查询带了 `LIMIT`。

代码逻辑：
```rust
for (_cluster_id, start_node) in start_nodes_vec {
    // 为每个 cluster 创建独立的搜索
    let mut lsr = ListSearchResult::new(...);
    
    loop {
        graph.greedy_search_iterate(...);
        // 收集结果...
        
        if cluster_result_count >= results_per_cluster { break; }
    }
    
    if all_results.len() >= queue_size { break; }
}
```

### 问题

1. **预计算所有结果**：在 `TSVResponseIterator::new()` 中，我们就已经搜索了所有 cluster
2. **忽略 LIMIT**：无论 `LIMIT` 是多少（如 `LIMIT 10`），我们都会搜索所有 cluster 并收集 `queue_size`（默认 1000）个结果
3. **性能开销**：对于小的 `LIMIT`，这种策略会造成不必要的计算

### 优化方向

#### 方案 1：延迟搜索（Lazy Search）

不预先搜索所有 cluster，而是在 `next()` 被调用时按需搜索：

```rust
struct TSVResponseIterator<QDM, PD> {
    // ... 现有字段
    cluster_search_states: Vec<ClusterSearchState>,  // 每个 cluster 的搜索状态
    current_cluster_idx: usize,
}

fn next(&mut self, ...) -> Option<...> {
    // 按需从当前 cluster 获取结果
    // 当当前 cluster 耗尽时，移动到下一个 cluster
}
```

**优点**：
- 只搜索需要的 cluster
- 对于小的 `LIMIT`，性能更好

**缺点**：
- 实现复杂
- 需要维护多个搜索状态

#### 方案 2：Early Termination

在预计算阶段，当收集到足够的结果时提前停止：

```rust
fn search_all_clusters(...) -> BinaryHeap<ClusterSearchResult> {
    let limit = get_limit_from_query();  // 获取 LIMIT 值
    let target_results = limit * 2;  // 收集 2x LIMIT 个结果作为缓冲
    
    for (cluster_id, start_node) in start_nodes_vec {
        // 搜索 cluster...
        
        // 如果已经收集到足够的结果，提前停止
        if all_results.len() >= target_results {
            break;
        }
    }
}
```

**优点**：
- 实现简单
- 对于小的 `LIMIT`，性能更好

**缺点**：
- 需要知道 `LIMIT` 值
- 可能错过其他 cluster 的更好结果

#### 方案 3：优先级搜索（推荐）

根据 query 与 cluster centroid 的距离，优先搜索最近的 cluster：

```rust
fn search_all_clusters(...) -> BinaryHeap<ClusterSearchResult> {
    // 计算 query 与每个 cluster centroid 的距离
    let mut cluster_distances: Vec<(f32, u32)> = ...;
    cluster_distances.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    
    for (dist, cluster_id) in cluster_distances {
        // 搜索 cluster...
        
        // 如果已经收集到足够的结果，且当前 cluster 距离较远，停止
        if all_results.len() >= target_results && dist > threshold {
            break;
        }
    }
}
```

**优点**：
- 优先搜索最可能包含好结果的 cluster
- 可以结合 early termination

**缺点**：
- 需要计算 centroid 距离
- 可能错过远距离 cluster 的好结果

## 建议

对于当前的实现，建议：

1. **保持当前的全搜索策略**：确保高召回率
2. **调整 `diskann.cluster_search_queue_size`**：根据 `LIMIT` 大小动态调整
3. **未来优化**：实现优先级搜索或延迟搜索，提高小 `LIMIT` 查询的性能

## 相关 GUC 参数

- `diskann.cluster_search_queue_size`：cluster 搜索结果队列大小（默认 1000）
- `diskann.query_search_list_size`：查询搜索列表大小（默认 100）
- `diskann.cluster_search_top_k`：搜索最近的 K 个 cluster（在 centroid 搜索模式下使用）
