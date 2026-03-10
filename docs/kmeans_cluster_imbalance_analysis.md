# K-Means Cluster 分配不均匀问题分析

## 问题描述

在创建索引时，cluster 分配如下：
```
NOTICE:    Cluster 0: 764 vectors
NOTICE:    Cluster 1: 1369 vectors
NOTICE:    Cluster 2: 982 vectors
NOTICE:    Cluster 3: 1326 vectors
NOTICE:    Cluster 4: 1288 vectors
NOTICE:    Cluster 5: 1818 vectors
NOTICE:    Cluster 6: 1677 vectors
NOTICE:    Cluster 7: 776 vectors
```

Cluster 5 有 1818 个向量，而 Cluster 0 只有 764 个向量，**差异达到 2.38 倍**。

这会导致：
- 数据量大的 cluster 构建图时耗时更长
- 并行构建时负载不均衡
- 某些 worker 空闲，而其他 worker 过载

## K-Means 算法实现分析

### 1. 算法选择逻辑 (mod.rs)

```rust
pub fn k_means(
    c: usize,
    mut samples: Vec<Vec<f32>>,
    is_spherical: bool,
    iterations: usize,
    prefer_kmeanspp: bool,
) -> Vec<Vec<f32>> {
    // ...
    if n <= c {
        return quick_centers::quick_centers(c, samples);
    }

    if dims == 1 {
        // 使用 kmeans1d
        let flat_samples: Vec<f32> = samples.iter().flat_map(|v| v.iter().copied()).collect();
        let centroids = kmeans1d(c, &flat_samples);
        return centroids.into_iter().map(|c| vec![c]).collect();
    }

    // 使用 Lloyd K-Means
    let mut lloyd_k_means = LloydKMeans::new(c, samples, is_spherical, prefer_kmeanspp);
    for _ in 0..iterations {
        if lloyd_k_means.iterate() {
            break;
        }
    }
    lloyd_k_means.finish()
}
```

### 2. Lloyd K-Means 实现 (lloyd.rs)

#### 初始化阶段

```rust
pub fn new(c: usize, samples: Vec<Vec<f32>>, is_spherical: bool, prefer_kmeanspp: bool) -> Self {
    // ...
    if prefer_kmeanspp {
        // K-Means++ 初始化
        centroids.push(samples[rng.gen_range(0..n)].clone());
        let mut weight = vec![f32::INFINITY; n];
        for i in 1..c {
            // 根据距离加权采样
            let dis_2 = (0..n)
                .into_par_iter()
                .map(|j| squared_distance(&samples[j], &centroids[i - 1]))
                .collect::<Vec<_>>();
            for j in 0..n {
                if dis_2[j] < weight[j] {
                    weight[j] = dis_2[j];
                }
            }
            let sum: f32 = weight.iter().sum();
            let index = 'a: {
                let mut choice = sum * rng.gen_range(0.0..1.0);
                for j in 0..(n - 1) {
                    choice -= weight[j];
                    if choice < 0.0f32 {
                        break 'a j;
                    }
                }
                n - 1
            };
            centroids.push(samples[index].clone());
        }
    } else {
        // 随机采样
        let indices: Vec<usize> = rand::seq::index::sample(&mut rng, n, c).into_iter().collect();
        for index in indices {
            centroids.push(samples[index].clone());
        }
    }
    // ...
}
```

#### 迭代阶段

```rust
pub fn iterate(&mut self) -> bool {
    // 1. 重新计算 centroids（基于当前分配）
    let (sum, mut count) = (0..n)
        .into_par_iter()
        .fold(
            || (vec![vec![0.0f32; dims]; c], vec![0.0f32; c]),
            |(mut sum, mut count), i| {
                vector_add_inplace(&mut sum[self.assign[i]], &samples[i]);
                count[self.assign[i]] += 1.0;
                (sum, count)
            },
        )
        .reduce(/* ... */);

    // 2. 处理空 cluster
    for i in 0..c {
        if count[i] != 0.0f32 {
            continue;
        }
        // 从其他 cluster 分裂出一个 centroid
        let mut o = 0;
        loop {
            let alpha = rng.gen_range(0.0..1.0f32);
            let beta = (count[o] - 1.0) / (n - c) as f32;
            if alpha < beta {
                break;
            }
            o = (o + 1) % c;
        }
        centroids[i] = centroids[o].clone();
        // 添加随机扰动
        for val in centroids[i].iter_mut() {
            let perturbation = rng.gen_range(-DELTA..DELTA);
            *val = *val + perturbation;
        }
        count[i] = count[o] / 2.0;
        count[o] -= count[i];
    }

    // 3. 重新分配样本到最近的 centroid
    let assign = (0..n)
        .into_par_iter()
        .map(|i| {
            let mut result = (f32::INFINITY, 0);
            for j in 0..c {
                let dis_2 = squared_distance(&samples[i], &centroids[j]);
                if dis_2 <= result.0 {
                    result = (dis_2, result.1);
                }
            }
            result.1
        })
        .collect::<Vec<_>>();

    // 4. 检查是否收敛
    let result = (0..n).all(|i| assign[i] == self.assign[i]);
    self.assign = assign;
    result
}
```

### 3. K-Means 1D 实现 (kmeans1d.rs)

```rust
pub fn kmeans1d(c: usize, a: &[f32]) -> Vec<f32> {
    // ...
    let chunk_size = n / c;
    for i in 1..c {
        boundaries[i] = i * chunk_size;
    }
    // ...
}
```

**注意**：1D 情况下使用均匀分块，但在高维情况下不适用。

## 分配不均匀的原因

### 1. 数据分布不均匀

K-Means 算法本身**不保证** cluster 大小相等。它只最小化每个样本到其 centroid 的距离平方和。

如果数据在某些区域更密集，那些区域的 cluster 会包含更多样本。

### 2. 初始化敏感性

- **K-Means++** 虽然比随机初始化更好，但仍然可能选中数据密集区域的点作为初始 centroids
- 一旦初始 centroids 偏向某些区域，迭代过程可能无法完全纠正

### 3. 空 Cluster 处理机制

```rust
// 处理空 cluster
if count[i] != 0.0f32 {
    continue;
}
// 从最大的 cluster 分裂
let beta = (count[o] - 1.0) / (n - c) as f32;
```

这个机制：
- 只处理**完全为空**的 cluster
- 不会处理**过小**的 cluster
- 分裂逻辑偏向从大的 cluster 分裂，但分裂后大小差异仍然存在

### 4. 算法目标函数

K-Means 的目标是最小化：
```
J = Σᵢ Σₓ∈Cᵢ ||x - μᵢ||²
```

这个目标函数**不考虑 cluster 大小平衡**，只考虑距离最小化。

## 为什么这会导致构建性能问题

### 并行构建的负载不均衡

假设使用 8 个 worker 并行构建：
- Worker 5 处理 Cluster 5 (1818 个向量)
- Worker 0 处理 Cluster 0 (764 个向量)

构建时间比例约为 1818:764 ≈ 2.38:1

这意味着：
- Worker 0 完成后需要等待 Worker 5
- 整体构建时间由最慢的 worker 决定
- 并行效率降低

### 图构建复杂度

DiskANN 图构建的时间复杂度约为 O(n log n) 到 O(n²)，取决于参数设置。

如果 cluster 大小差异大：
- 大 cluster 的构建时间呈非线性增长
- 小 cluster 快速完成，但大 cluster 成为瓶颈

## 可能的解决方案

### 方案 1：Balanced K-Means

修改 K-Means 算法，在目标函数中加入 cluster 大小平衡项：

```rust
J = Σᵢ Σₓ∈Cᵢ ||x - μᵢ||² + λ * Σᵢ (|Cᵢ| - n/c)²
```

**优点**：
- 直接解决大小不平衡问题

**缺点**：
- 需要调整超参数 λ
- 可能降低聚类质量

### 方案 2：约束 K-Means

在分配阶段加入大小约束：

```rust
// 优先分配到较小的 cluster
if cluster_size[best_cluster] > max_size && 
   distance_to_second_best < threshold {
    assign_to_second_best();
}
```

**优点**：
- 保持算法简单
- 可控的大小差异

**缺点**：
- 需要确定 max_size 和 threshold
- 可能影响聚类质量

### 方案 3：后处理重分配

在 K-Means 完成后，进行后处理：

```rust
// 将大 cluster 的边界样本移动到小 cluster
while max_size > target_size * 1.2 {
    // 找到大 cluster 中距离 centroid 最远的边界样本
    // 移动到最近的小 cluster
}
```

**优点**：
- 不影响 K-Means 迭代过程
- 灵活的调整策略

**缺点**：
- 增加额外的计算开销
- 可能影响图构建质量

### 方案 4：动态 Worker 分配

不改 K-Means，而是改进并行构建策略：

```rust
// 根据 cluster 大小动态分配 worker
let total_vectors: usize = clusters.iter().map(|c| c.size).sum();
let num_workers = 8;

for cluster in clusters {
    let workers_for_cluster = 
        (cluster.size * num_workers / total_vectors).max(1);
    // 多个 worker 协作构建一个大 cluster
}
```

**优点**：
- 不改 K-Means 算法
- 充分利用并行资源

**缺点**：
- 需要修改并行构建逻辑
- 大 cluster 的并发访问需要同步

### 方案 5：使用 K-Means 的变体

考虑使用其他聚类算法：

1. **Balanced Iterative Reducing and Clustering using Hierarchies (BIRCH)**
2. **Constrained K-Means**
3. **Fair K-Means**

**优点**：
- 专门设计用于平衡聚类

**缺点**：
- 需要引入新的依赖
- 可能需要调整其他部分的代码

## 推荐方案

### 短期：方案 4（动态 Worker 分配）

不改 K-Means 算法，通过改进并行构建策略来解决负载不均衡问题：

1. 根据 cluster 大小计算每个 cluster 需要的 worker 数量
2. 大 cluster 使用多个 worker 协作构建
3. 使用 work-stealing 机制平衡负载

### 长期：方案 2（约束 K-Means）

在 K-Means 分配阶段加入大小约束，确保 cluster 大小差异在可接受范围内（如最大不超过平均值的 1.5 倍）。

## 结论

Cluster 分配不均匀是 K-Means 算法的固有特性，不是实现 bug。解决这个问题的最佳方法取决于：

1. **是否接受略微降低的聚类质量**（选择平衡约束）
2. **是否愿意修改并行构建逻辑**（选择动态 worker 分配）
3. **对构建时间的敏感度**（如果构建时间不是瓶颈，可以暂时不处理）

建议先评估当前构建时间的实际影响，再决定是否需要优化。
