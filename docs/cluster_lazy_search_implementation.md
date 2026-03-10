# Cluster 延迟搜索实现文档

## 实现概述

实现了延迟搜索方案：在 `next` 方法中按需搜索每个 cluster，而不是预先搜索所有 cluster。这种方案解决了大数据量、小内存场景下的问题。

## 核心设计

### 问题

预先搜索所有 cluster 的问题：
1. **内存不足**：大数据量时，所有 cluster 的搜索结果可能超出内存
2. **初始化时间长**：需要等待所有 cluster 搜索完成才能返回第一个结果

### 解决方案

延迟搜索：在 `next` 方法中按需搜索每个 cluster

## 核心改动

### 1. TSVResponseIterator 结构体修改

```rust
struct TSVResponseIterator<QDM, PD> {
    // ... 现有字段
    cluster_results: BinaryHeap<ClusterSearchResult>,
    is_cluster_mode: bool,
    cluster_start_nodes: Vec<IndexPointer>,  // 所有 cluster 的起始节点
    current_cluster_idx: usize,               // 当前搜索的 cluster 索引
    query: Option<LabeledVector>,             // 保存查询向量用于后续搜索
}
```

### 2. 初始化逻辑 (new 方法)

```rust
fn new<S: Storage>(...) -> Self {
    if is_cluster_mode {
        // 获取所有 cluster 的起始节点
        let cluster_start_nodes = meta_page.get_all_cluster_start_nodes();
        let start_nodes_vec: Vec<IndexPointer> = cluster_start_nodes.values().copied().collect();

        // 只初始化第一个 cluster 的搜索
        let dm = storage.get_query_distance_measure(query.clone());
        let first_lsr = if let Some(&first_start) = start_nodes_vec.first() {
            ListSearchResult::new(vec![first_start], dm, ...)
        } else {
            ListSearchResult::empty()
        };

        Self {
            lsr: first_lsr,
            cluster_start_nodes: start_nodes_vec,
            current_cluster_idx: 0,
            query: Some(query),  // 保存查询向量
            // ...
        }
    } else {
        // 非 cluster 模式：原有逻辑
    }
}
```

### 3. next 方法实现

```rust
fn next<S: Storage>(&mut self, storage: &S) -> Option<(HeapPointer, IndexPointer)> {
    if self.is_cluster_mode {
        loop {
            // 1. 尝试从当前结果缓冲区返回
            if let Some(result) = self.cluster_results.pop() {
                return Some((result.heap_pointer, result.index_pointer));
            }

            // 2. 缓冲区空了，搜索下一个 cluster
            if !self.search_next_cluster(storage) {
                return None;  // 所有 cluster 都搜索完了
            }
        }
    }
    // 非 cluster 模式：原有逻辑
}
```

### 4. search_next_cluster 方法

```rust
fn search_next_cluster<S: Storage>(&mut self, storage: &S) -> bool {
    // 移动到下一个 cluster
    self.current_cluster_idx += 1;

    // 检查是否还有 cluster 需要搜索
    if self.current_cluster_idx >= self.cluster_start_nodes.len() {
        return false;
    }

    // 获取下一个 cluster 的起始节点
    let start_node = self.cluster_start_nodes[self.current_cluster_idx];

    // 创建新的搜索上下文
    let query = self.query.as_ref()?.clone();
    let dm = storage.get_query_distance_measure(query);
    let mut lsr = ListSearchResult::new(vec![start_node], dm, ...);

    // 执行完整的贪心搜索
    let mut graph = Graph::new(GraphNeighborStore::Disk, &mut self.meta_page);
    loop {
        graph.greedy_search_iterate(&mut lsr, ...);

        // 收集结果到缓冲区
        while let Some((heap_pointer, index_pointer, distance)) =
            lsr.consume_with_distance(storage)
        {
            self.cluster_results.push(ClusterSearchResult { ... });
        }

        if lsr.is_empty() {
            break;
        }
    }

    true
}
```

## 算法流程

```
延迟搜索流程：
1. 初始化：
   - 获取所有 cluster 的起始节点
   - 只初始化第一个 cluster 的搜索
   - 保存查询向量

2. next() 调用：
   a. 尝试从 cluster_results 缓冲区返回结果
   b. 如果缓冲区空了：
      - 调用 search_next_cluster() 搜索下一个 cluster
      - 如果所有 cluster 都搜索完了，返回 None
   c. 返回结果

3. search_next_cluster()：
   - 移动到下一个 cluster
   - 创建新的搜索上下文
   - 执行完整的贪心搜索
   - 将结果存入 cluster_results 缓冲区
```

## 内存管理

### 内存使用特点

1. **按需加载**：每次只搜索一个 cluster
2. **缓冲区大小可控**：每个 cluster 的结果数量有限
3. **避免内存溢出**：不会一次性加载所有 cluster 的结果

### 内存优化

如果单个 cluster 的结果太多，可以进一步优化：
- 限制每个 cluster 返回的最大结果数
- 使用更小的缓冲区

## 性能特点

### 优点

1. **低内存占用**：每次只搜索一个 cluster
2. **快速响应**：初始化时只搜索第一个 cluster
3. **按需搜索**：不需要的结果不会被搜索

### 缺点

1. **结果顺序问题**：当前实现中，每个 cluster 的结果按距离排序，但不同 cluster 之间的结果可能不是全局最优的
2. **重复搜索**：如果用户需要所有结果，仍然需要搜索所有 cluster

## 结果顺序说明

当前实现的结果顺序：
1. 第一个 cluster 的结果（按距离排序）
2. 第二个 cluster 的结果（按距离排序）
3. ...

**注意**：这不是全局最优顺序。如果需要全局最优顺序，可以考虑：
1. 使用更复杂的合并策略
2. 预先搜索所有 cluster（但会增加内存使用）

## 未来优化

1. **全局排序**：实现全局最优排序
2. **并行搜索**：并行搜索多个 cluster
3. **早停优化**：当找到足够好的结果时提前停止
4. **结果数量限制**：限制每个 cluster 返回的结果数量

## 测试建议

1. **功能测试**：验证延迟搜索是否正确工作
2. **内存测试**：监控内存使用情况
3. **性能测试**：测量搜索时间
4. **边界测试**：测试空 cluster、单个 cluster 等边界情况
