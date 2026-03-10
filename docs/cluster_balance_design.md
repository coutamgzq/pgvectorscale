# Cluster 负载均衡设计方案

## 1. 问题背景

当前 K-Means 聚类导致 cluster 大小差异显著（如 764 vs 1818，差异达 2.38 倍），造成并行构建时负载不均衡，严重影响索引构建性能。

## 2. 设计目标

1. **减少 cluster 大小差异**：通过改进 K-Means 算法，使各 cluster 大小更均衡
2. **优化并行构建**：在 cluster 大小仍有差异时，通过动态 worker 分配最大化并行效率
3. **保持聚类质量**：在均衡性和聚类质量之间取得平衡
4. **向后兼容**：不影响非 cluster 构建的现有功能

## 3. 方案概述

采用**双管齐下的策略**：

### 方案 A：约束 K-Means（源头控制）
在 K-Means 算法中加入大小约束，减少 cluster 大小差异。

### 方案 B：动态 Worker 分配（运行时优化）
根据 cluster 实际大小动态分配 worker 数量，最大化并行效率。

两个方案**独立实现，互补工作**。

---

## 4. 方案 A：约束 K-Means 详细设计

### 4.1 核心思想

在标准 K-Means 的分配阶段（Assignment Step）加入大小约束，当某个 cluster 已经达到最大容量时，将其样本重新分配到次优的 cluster。

### 4.2 算法流程

```
标准 K-Means 迭代：
1. 分配阶段：每个样本分配到最近的 centroid
2. 更新阶段：重新计算 centroids

约束 K-Means 迭代：
1. 分配阶段：
   a. 按距离排序所有 (样本, centroid) 对
   b. 依次分配，但检查目标 cluster 是否已满
   c. 如果已满，分配到次优的可用 cluster
2. 更新阶段：重新计算 centroids（与标准 K-Means 相同）
```

### 4.3 关键参数

```rust
pub struct ConstrainedKMeansConfig {
    /// 最大允许的 cluster 大小（相对于平均大小的倍数）
    /// 默认 1.5，表示最大 cluster 不超过平均的 1.5 倍
    pub max_size_factor: f32,
    
    /// 次优分配的距离容忍度
    /// 如果次优 centroid 的距离 > 最优距离 * (1 + tolerance)，则强制分配
    /// 默认 0.2，表示允许 20% 的距离增加
    pub tolerance: f32,
    
    /// 是否启用约束
    pub enabled: bool,
}
```

### 4.4 实现细节

#### 4.4.1 修改文件

**文件**: `src/access_method/k_means/lloyd.rs`

#### 4.4.2 数据结构修改

```rust
pub struct LloydKMeans {
    // ... 现有字段 ...
    
    /// 约束配置
    config: ConstrainedKMeansConfig,
    
    /// 目标 cluster 大小（n / c）
    target_size: usize,
}
```

#### 4.4.3 分配阶段实现

```rust
/// 带约束的分配阶段
fn constrained_assignment(&mut self) -> Vec<usize> {
    let n = self.samples.len();
    let c = self.c;
    let target_size = self.target_size;
    let max_size = (target_size as f32 * self.config.max_size_factor) as usize;
    
    // 计算所有样本到所有 centroids 的距离
    let distances: Vec<Vec<(f32, usize)>> = (0..n)
        .into_par_iter()
        .map(|i| {
            let mut dists: Vec<(f32, usize)> = (0..c)
                .map(|j| (squared_distance(&self.samples[i], &self.centroids[j]), j))
                .collect();
            dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            dists
        })
        .collect();
    
    // 分配结果
    let mut assignment = vec![0usize; n];
    let mut cluster_sizes = vec![0usize; c];
    
    // 按距离排序所有分配候选
    let mut all_candidates: Vec<(f32, usize, usize)> = Vec::with_capacity(n * c);
    for i in 0..n {
        for (rank, (dist, cluster)) in distances[i].iter().enumerate() {
            // 优先级 = 距离 + 排名惩罚
            let priority = *dist + rank as f32 * 0.001;
            all_candidates.push((priority, i, *cluster));
        }
    }
    all_candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    
    // 依次分配
    let mut assigned = vec![false; n];
    for (_, sample_idx, cluster_idx) in all_candidates {
        if assigned[sample_idx] {
            continue;
        }
        
        // 检查 cluster 是否已满
        if cluster_sizes[cluster_idx] >= max_size {
            // 尝试次优 cluster
            let mut assigned_to_alternative = false;
            for (alt_dist, alt_cluster) in &distances[sample_idx][1..] {
                if cluster_sizes[*alt_cluster] < max_size {
                    // 检查距离容忍度
                    let best_dist = distances[sample_idx][0].0;
                    if *alt_dist <= best_dist * (1.0 + self.config.tolerance) {
                        assignment[sample_idx] = *alt_cluster;
                        cluster_sizes[*alt_cluster] += 1;
                        assigned[sample_idx] = true;
                        assigned_to_alternative = true;
                        break;
                    }
                }
            }
            
            // 如果所有可用 cluster 都超出容忍度，强制分配到最近的未满 cluster
            if !assigned_to_alternative {
                for (_, alt_cluster) in &distances[sample_idx] {
                    if cluster_sizes[*alt_cluster] < max_size {
                        assignment[sample_idx] = *alt_cluster;
                        cluster_sizes[*alt_cluster] += 1;
                        assigned[sample_idx] = true;
                        break;
                    }
                }
            }
        } else {
            assignment[sample_idx] = cluster_idx;
            cluster_sizes[cluster_idx] += 1;
            assigned[sample_idx] = true;
        }
    }
    
    assignment
}
```

#### 4.4.4 修改 iterate 方法

```rust
pub fn iterate(&mut self) -> bool {
    // ... 现有的 centroid 更新代码 ...
    
    // 使用约束分配替代标准分配
    let assign = if self.config.enabled {
        self.constrained_assignment()
    } else {
        // 标准分配
        (0..n)
            .into_par_iter()
            .map(|i| {
                let mut result = (f32::INFINITY, 0);
                for j in 0..c {
                    let dis_2 = squared_distance(&self.samples[i], &self.centroids[j]);
                    if dis_2 <= result.0 {
                        result = (dis_2, result.1);
                    }
                }
                result.1
            })
            .collect::<Vec<_>>()
    };
    
    // ... 后续代码不变 ...
}
```

### 4.5 性能考虑

- **时间复杂度**：从 O(n*c) 增加到 O(n*c*log(n*c))，主要由于排序
- **优化**：可以使用部分排序或近似算法减少开销
- **收敛性**：约束可能导致收敛变慢，需要调整迭代次数

### 4.6 配置接口

```rust
// 在 mod.rs 中
pub fn k_means(
    c: usize,
    mut samples: Vec<Vec<f32>>,
    is_spherical: bool,
    iterations: usize,
    prefer_kmeanspp: bool,
    config: Option<ConstrainedKMeansConfig>,  // 新增参数
) -> Vec<Vec<f32>> {
    // ...
}
```

---

## 5. 方案 B：动态 Worker 分配详细设计

### 5.1 核心思想

不改 K-Means 算法，而是在并行构建阶段根据 cluster 大小动态分配 worker 数量。大 cluster 分配更多 worker，小 cluster 分配较少 worker。

### 5.2 架构设计

#### 5.2.1 当前架构（问题）

```
Worker 0 -> Cluster 0 (764 vectors)
Worker 1 -> Cluster 1 (1369 vectors)
Worker 2 -> Cluster 2 (982 vectors)
...
Worker 7 -> Cluster 7 (776 vectors)

问题：Worker 5 处理 1818 vectors，成为瓶颈
```

#### 5.2.2 目标架构

```
Worker 0 -> Cluster 5 (part 1)  
Worker 1 -> Cluster 5 (part 2)  
Worker 2 -> Cluster 5 (part 3)  
Worker 3 -> Cluster 6 (part 1)  
Worker 4 -> Cluster 6 (part 2)  
Worker 5 -> Cluster 1 (part 1)  
Worker 6 -> Cluster 1 (part 2)  
Worker 7 -> Cluster 3

大 cluster 被分割给多个 worker 处理
```

### 5.3 关键设计决策

#### 5.3.1 Worker 分配策略

**策略 1：比例分配**
```rust
workers_for_cluster = max(1, cluster_size * total_workers / total_vectors)
```

**策略 2：阈值分配**
```rust
if cluster_size > threshold {
    workers_for_cluster = 2  // 或更多
} else {
    workers_for_cluster = 1
}
```

**推荐：策略 1（比例分配）**，更灵活。

#### 5.3.2 Cluster 分割方式

**方式 1：连续分割**
将 cluster 的向量范围连续分割给不同 worker。

**方式 2：交错分割**
按索引交错分配（如 worker 0 处理索引 0,3,6...）。

**推荐：方式 1（连续分割）**，实现简单，局部性好。

### 5.4 实现细节

#### 5.4.1 修改文件

**文件**: `src/access_method/build/parallel_build/cluster.rs`

#### 5.4.2 新增数据结构

```rust
/// Worker 任务分配信息
#[derive(Debug, Clone)]
pub struct WorkerAssignment {
    /// Worker ID
    pub worker_id: usize,
    /// 负责的 cluster ID
    pub cluster_id: usize,
    /// 在 cluster 内的起始索引
    pub start_idx: usize,
    /// 在 cluster 内的结束索引（不包含）
    pub end_idx: usize,
    /// 是否是该 cluster 的主 worker（负责保存 start node）
    pub is_primary: bool,
}

/// 全局任务调度器（存储在共享内存）
#[repr(C)]
pub struct TaskScheduler {
    /// 总任务数
    pub total_tasks: usize,
    /// 已完成任务数
    pub completed_tasks: AtomicUsize,
    /// 任务分配表
    pub assignments: [WorkerAssignment; MAX_PARALLEL_WORKERS],
    /// 每个 cluster 的 worker 数量
    pub cluster_worker_count: [usize; MAX_CLUSTERS],
}
```

#### 5.4.3 任务分配算法

```rust
/// 在 leader 进程中计算任务分配
fn calculate_worker_assignments(
    cluster_sizes: &[usize],
    num_workers: usize,
    num_clusters: usize,
) -> Vec<WorkerAssignment> {
    let total_vectors: usize = cluster_sizes.iter().sum();
    let mut assignments = Vec::with_capacity(num_workers);
    let mut worker_id = 0;
    
    for cluster_id in 0..num_clusters {
        let cluster_size = cluster_sizes[cluster_id];
        
        // 计算该 cluster 需要的 worker 数量
        let workers_for_cluster = if cluster_size == 0 {
            0
        } else {
            let ideal_workers = cluster_size * num_workers / total_vectors;
            ideal_workers.max(1)  // 至少一个 worker
        };
        
        // 分割 cluster
        let vectors_per_worker = cluster_size / workers_for_cluster;
        let remainder = cluster_size % workers_for_cluster;
        
        let mut start_idx = 0;
        for i in 0..workers_for_cluster {
            let extra = if i < remainder { 1 } else { 0 };
            let count = vectors_per_worker + extra;
            let end_idx = start_idx + count;
            
            assignments.push(WorkerAssignment {
                worker_id,
                cluster_id,
                start_idx,
                end_idx,
                is_primary: i == 0,  // 第一个 worker 是主 worker
            });
            
            start_idx = end_idx;
            worker_id += 1;
        }
    }
    
    assignments
}
```

#### 5.4.4 修改 ClusterQueues 支持范围查询

```rust
impl ClusterQueues {
    /// 从指定范围弹出向量
    pub fn pop_from_queue_range(
        &self,
        base_ptr: *mut u8,
        cluster_id: usize,
        start_idx: usize,
        end_idx: usize,
    ) -> Option<(pg_sys::ItemPointerData, &[f32])> {
        // 只处理 [start_idx, end_idx) 范围内的向量
        // ...
    }
}
```

#### 5.4.5 修改 Consumer 逻辑

```rust
pub unsafe extern "C" fn _vectorscale_build_cluster_consumer_main(
    arg: *mut c_void,
) {
    // ... 现有初始化代码 ...
    
    // 获取任务分配
    let scheduler: *mut TaskScheduler = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_TASK_SCHEDULER_KEY, false)
            .cast::<TaskScheduler>()
    };
    
    let worker_number = unsafe { pg_sys::ParallelWorkerNumber as usize };
    let assignment = unsafe { (*scheduler).assignments[worker_number] };
    
    // 检查该 worker 是否有任务
    if assignment.worker_id != worker_number {
        notice!("Worker {} has no assignment, exiting", worker_number);
        return;
    }
    
    let cluster_id = assignment.cluster_id;
    let start_idx = assignment.start_idx;
    let end_idx = assignment.end_idx;
    let is_primary = assignment.is_primary;
    
    notice!(
        "Worker {} assigned to cluster {} [{}..{}], primary={}",
        worker_number, cluster_id, start_idx, end_idx, is_primary
    );
    
    // 修改 consumer_state 包含范围信息
    let mut consumer_state = ConsumerState {
        cluster_id,
        cluster_queues,
        _parallel_shared: parallel_shared,
        _num_dimensions: params.num_dimensions,
        ntuples: 0,
        first_node: None,
        start_idx,  // 新增
        end_idx,    // 新增
    };
    
    // 构建子图（只处理指定范围）
    build_cluster_subgraph_range(
        &mut consumer_state,
        &heap_relation,
        &index_relation,
        &mut meta_page,
        &centroids,
    );
    
    // 只有主 worker 保存 start node
    if is_primary {
        if let Some(first_node) = consumer_state.first_node {
            let mut item_pointer_data = pg_sys::ItemPointerData::default();
            first_node.to_item_pointer_data(&mut item_pointer_data);
            (*cluster_start_nodes).set_start_node(cluster_id, item_pointer_data);
        }
    }
    
    // ...
}
```

#### 5.4.6 修改 build_cluster_subgraph

```rust
unsafe fn build_cluster_subgraph_range(
    consumer_state: &mut ConsumerState,
    heap_relation: &PgRelation,
    index_relation: &PgRelation,
    meta_page: &mut MetaPage,
    _centroids: &[Vec<f32>],
) {
    // ... 现有初始化代码 ...
    
    match storage_type {
        StorageType::Plain => {
            let mut plain = PlainStorage::new_for_build(
                index_relation,
                heap_relation,
                graph.get_meta_page(),
            );

            process_cluster_vectors_range(
                consumer_state,
                index_relation,
                meta_page,
                queues,
                base_ptr,
                cluster_id,
                flush_interval,
                &mut plain,
                &mut graph,
                &mut tape,
                &mut write_stats,
                consumer_state.start_idx,  // 新增
                consumer_state.end_idx,    // 新增
            );
        }
        // ... SbqCompression 类似 ...
    }
}
```

#### 5.4.7 修改 process_cluster_vectors

```rust
fn process_cluster_vectors_range<S: Storage>(
    consumer_state: &mut ConsumerState,
    index_relation: &PgRelation,
    meta_page: &mut MetaPage,
    queues: &ClusterQueues,
    base_ptr: *mut u8,
    cluster_id: usize,
    flush_interval: usize,
    storage: &mut S,
    graph: &mut Graph,
    tape: &mut Tape,
    write_stats: &mut WriteStats,
    start_idx: usize,  // 新增
    end_idx: usize,    // 新增
) {
    let mut insert_stats = InsertStats::default();
    let mut processed_count = 0;
    let mut queue_idx = 0;

    loop {
        if let Some((heap_tid, vector_data)) = queues.pop_from_queue(base_ptr, cluster_id) {
            // 只处理指定范围内的向量
            if queue_idx < start_idx {
                queue_idx += 1;
                continue;
            }
            if queue_idx >= end_idx {
                break;
            }
            queue_idx += 1;
            
            // ... 现有处理逻辑 ...
        } else if queues.is_queue_finished(base_ptr, cluster_id) {
            break;
        } else {
            queues.wait_on_cv(base_ptr, cluster_id);
        }
    }
}
```

### 5.5 同步问题

#### 5.5.1 多个 Worker 访问同一 Cluster Queue

多个 worker 可能同时从同一个 cluster queue 读取，需要确保：
1. 每个向量只被一个 worker 处理
2. 使用索引范围来避免冲突

**解决方案**：
- 每个 worker 知道自己的索引范围 [start_idx, end_idx)
- 跳过范围外的向量
- 不需要额外的锁，因为只是读取

#### 5.5.2 Start Node 保存

只有主 worker（is_primary）保存 start node。

#### 5.5.3 完成检测

```rust
// 使用原子计数器跟踪完成的任务数
let completed = (*scheduler).completed_tasks.fetch_add(1, Ordering::SeqCst);
if completed + 1 == (*scheduler).total_tasks {
    // 所有任务完成，通知 leader
}
```

### 5.6 性能考虑

- **负载均衡**：大 cluster 被多个 worker 分担，减少瓶颈
- **通信开销**：任务分配在启动时完成，运行时无额外通信
- **局部性**：连续分割保持数据局部性，有利于缓存

---

## 6. 两个方案的协同工作

### 6.1 独立工作

- **方案 A** 在 K-Means 阶段减少 cluster 大小差异
- **方案 B** 在并行构建阶段优化 worker 分配

### 6.2 组合效果

| 场景 | 方案 A 单独 | 方案 B 单独 | 组合使用 |
|------|------------|------------|----------|
| Cluster 大小差异小 | 良好 | 良好 | 最佳 |
| Cluster 大小差异大 | 中等 | 良好 | 最佳 |
| 数据分布极不均匀 | 中等 | 良好 | 最佳 |

### 6.3 配置建议

```rust
// 推荐配置
let kmeans_config = ConstrainedKMeansConfig {
    max_size_factor: 1.5,  // 最大 cluster 不超过平均的 1.5 倍
    tolerance: 0.2,        // 允许 20% 的距离增加
    enabled: true,
};

// 动态 worker 分配始终启用
let enable_dynamic_worker = true;
```

---

## 7. 实现计划

### 阶段 1：方案 A（约束 K-Means）

1. 添加 `ConstrainedKMeansConfig` 结构体
2. 修改 `LloydKMeans` 添加约束分配方法
3. 修改 `k_means` 函数支持配置参数
4. 测试验证 cluster 大小分布

### 阶段 2：方案 B（动态 Worker 分配）

1. 添加 `WorkerAssignment` 和 `TaskScheduler` 结构体
2. 实现 `calculate_worker_assignments` 算法
3. 修改 `_vectorscale_build_cluster_consumer_main` 支持范围处理
4. 修改 `process_cluster_vectors` 支持范围查询
5. 测试验证并行效率提升

### 阶段 3：集成测试

1. 组合两个方案进行测试
2. 对比不同配置的构建时间和召回率
3. 确定最佳默认参数

---

## 8. 风险评估

### 8.1 方案 A 风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 约束导致聚类质量下降 | 召回率降低 | 调整 tolerance 参数，在质量和均衡间取得平衡 |
| 收敛速度变慢 | 构建时间增加 | 增加最大迭代次数，或使用早期停止 |
| 参数调优复杂 | 使用困难 | 提供合理的默认参数 |

### 8.2 方案 B 风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 分割导致图质量下降 | 召回率降低 | 确保同一 cluster 的多个 worker 协作构建完整的图 |
| 同步开销增加 | 性能下降 | 使用无锁设计，减少同步点 |
| 实现复杂度高 | 维护困难 | 详细文档和测试覆盖 |

---

## 9. 验证计划

### 9.1 功能验证

1. **Cluster 大小分布**：
   - 测试不同数据集，验证 cluster 大小差异是否在预期范围内
   - 对比启用/禁用约束 K-Means 的效果

2. **并行效率**：
   - 使用不同数量的 worker，测量构建时间
   - 对比启用/禁用动态 worker 分配的效果

### 9.2 性能验证

1. **构建时间**：
   - 对比优化前后的构建时间
   - 测试不同数据规模和维度

2. **召回率**：
   - 使用 VectorBench 测试召回率
   - 确保优化不降低搜索质量

### 9.3 稳定性验证

1. **并发测试**：
   - 多次运行，验证结果一致性
   - 测试边界条件（如空 cluster、单 cluster）

---

## 10. 结论

本设计方案通过**约束 K-Means**和**动态 Worker 分配**两个互补的方案，解决 cluster 负载不均衡问题：

1. **约束 K-Means** 从源头减少 cluster 大小差异
2. **动态 Worker 分配** 在运行时优化并行效率

两个方案可以独立实现和启用，组合使用可达到最佳效果。

建议先实现**方案 A**（约束 K-Means），因为它：
- 实现相对简单
- 直接解决 cluster 大小差异问题
- 不需要修改并行构建的复杂逻辑

然后再实现**方案 B**（动态 Worker 分配），作为进一步的优化。
