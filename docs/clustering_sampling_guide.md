# K-means 聚类采样参数配置指南

## 概述

pgvectorscale 在构建带聚类的索引时，使用 K-means 算法对向量进行聚类。为了控制内存使用和构建速度，提供了两个 GUC 参数来控制采样行为。

## 参数说明

### 1. `diskann.clustering_max_sample_size`

**作用**: 控制最终用于 K-means 训练的最大向量数量（采样数量 x）

**默认值**: 100000

**取值范围**: 0 ~ INT_MAX

**说明**:
- 这是最终参与 K-means 训练的向量数量上限
- 设置为 0 表示禁用采样，使用所有向量
- 值越大，K-means 训练越准确，但内存和 CPU 消耗越高
- 建议值: 50000 ~ 200000

### 2. `diskann.clustering_sample_threshold`

**作用**: 控制触发采样的数据量阈值，同时影响采样间隔（扫描数据量 y）

**默认值**: 1000000

**取值范围**: 0 ~ INT_MAX

**说明**:
- 当表数据量超过此阈值时，启用采样
- 同时决定了采样间隔: `sample_interval = sample_threshold / max_sample_size`
- 值越大，扫描的数据越多，采样越均匀
- 设置为 0 时总是启用采样

## 采样原理

### 采样流程

```
┌─────────────────────────────────────────────────────────────┐
│                    数据扫描阶段                              │
├─────────────────────────────────────────────────────────────┤
│  全表扫描 (1kw 行)                                          │
│      │                                                       │
│      ▼                                                       │
│  判断是否启用采样:                                           │
│  use_sampling = (max_sample_size > 0 && sample_threshold > 0)│
│      │                                                       │
│      ▼                                                       │
│  计算采样间隔:                                                │
│  sample_interval = sample_threshold / max_sample_size        │
│  例如: 1000000 / 100000 = 10                                │
│      │                                                       │
│      ▼                                                       │
│  逐行扫描:                                                   │
│  - 每 sample_interval 行采样一个向量                          │
│  - 直到收集满 max_sample_size 个向量                          │
│      │                                                       │
│      ▼                                                       │
│  输出: max_sample_size 个向量用于 K-means 训练               │
└─────────────────────────────────────────────────────────────┘
```

### 代码实现

```rust
// 采样间隔计算
let sample_interval = if use_sampling && sample_threshold > max_sample_size {
    (sample_threshold as f64 / max_sample_size as f64).ceil() as usize
} else {
    1
};

// 扫描回调中的采样逻辑
if collector_with_meta.collector.use_sampling {
    // 已收集足够数量，停止采样
    if collector_with_meta.collector.vectors.len() >= max_sample_size {
        return;
    }

    // 按间隔采样
    if sample_interval > 1 {
        if total_vectors_seen % sample_interval == 0 {
            // 采样此向量
            vectors.push(vector);
        }
    } else {
        // 无间隔，采样所有向量
        vectors.push(vector);
    }
}
```

## 配置建议

### 场景 1: 数据量适中 (100w ~ 500w)，追求高质量聚类

```sql
-- 使用所有数据进行 K-means 训练
SET diskann.clustering_max_sample_size = 0;
SET diskann.clustering_sample_threshold = 0;
```

**效果**: 扫描全部数据，使用全部数据训练，聚类质量最高，但内存消耗大。

### 场景 2: 数据量大 (500w ~ 2000w)，平衡性能和质量

```sql
-- 采样 10w 向量，扫描 200w 行
SET diskann.clustering_max_sample_size = 100000;
SET diskann.clustering_sample_threshold = 2000000;
```

**效果**: 
- 扫描约 200w 行数据（每 20 行采样 1 个）
- 最终使用 10w 向量训练 K-means
- 采样均匀，聚类质量较好

### 场景 3: 数据量很大 (2000w+)，优先性能

```sql
-- 采样 5w 向量，扫描 100w 行
SET diskann.clustering_max_sample_size = 50000;
SET diskann.clustering_sample_threshold = 1000000;
```

**效果**:
- 扫描约 100w 行数据（每 20 行采样 1 个）
- 最终使用 5w 向量训练
- 内存消耗小，速度快

### 场景 4: 用户需求 - 1kw 数据，多扫描少采样

**需求分析**:
- 总数据量: 1000w (10,000,000)
- 采样数量 x: 用于 K-means 训练
- 扫描数量 y: 影响采样均匀性

**推荐配置**:

```sql
-- 方案 A: 采样 10w，扫描 500w（每 50 行采样 1 个）
SET diskann.clustering_max_sample_size = 100000;
SET diskann.clustering_sample_threshold = 5000000;

-- 方案 B: 采样 5w，扫描 250w（每 50 行采样 1 个）
SET diskann.clustering_max_sample_size = 50000;
SET diskann.clustering_sample_threshold = 2500000;

-- 方案 C: 采样 10w，扫描 1000w（每 100 行采样 1 个，最均匀）
SET diskann.clustering_max_sample_size = 100000;
SET diskann.clustering_sample_threshold = 10000000;
```

### 场景 4.1: 1kw 数据 + 20 个 Cluster（推荐配置）

**关键原则**: 每个 cluster 至少需要 1000~5000 个采样向量才能获得良好的聚类中心

```sql
-- 推荐方案：采样 10w，扫描 500w
SET diskann.clustering_max_sample_size = 100000;
SET diskann.clustering_sample_threshold = 5000000;

-- 创建索引时指定 20 个 cluster
CREATE INDEX idx ON table USING diskann(column) 
WITH (num_clusters = 20);
```

**效果分析**:
```
总数据量:     10,000,000 行
扫描数据量:   5,000,000 行 (50%)
采样数量:     100,000 个向量
每个 cluster: 100,000 / 20 = 5,000 个向量（用于训练）

采样间隔:     5,000,000 / 100,000 = 50
             每 50 行采样 1 个向量
```

**内存估算**:
```
采样向量内存: 100,000 × 768 × 4 bytes ≈ 300 MB
聚类中心内存: 20 × 768 × 4 bytes ≈ 60 KB
总内存:       ~300 MB（可接受）
```

**其他可选方案**:

```sql
-- 方案 B: 更高采样质量（内存充足时）
SET diskann.clustering_max_sample_size = 200000;  -- 每个 cluster 1w 个向量
SET diskann.clustering_sample_threshold = 5000000;

-- 方案 C: 更低内存消耗（内存紧张时）
SET diskann.clustering_max_sample_size = 40000;   -- 每个 cluster 2000 个向量
SET diskann.clustering_sample_threshold = 2000000;
```

**不同 cluster 数量的采样建议**:

| Cluster 数量 | 推荐 max_sample_size | 每个 cluster 向量数 | 内存消耗 |
|-------------|---------------------|-------------------|---------|
| 4           | 40,000              | 10,000            | ~120 MB |
| 8           | 80,000              | 10,000            | ~240 MB |
| 16          | 80,000              | 5,000             | ~240 MB |
| **20**      | **100,000**         | **5,000**         | **~300 MB** |
| 32          | 160,000             | 5,000             | ~480 MB |
| 64          | 320,000             | 5,000             | ~960 MB |

**计算公式**:
```
采样间隔 = sample_threshold / max_sample_size
实际扫描行数 ≈ sample_threshold（如果数据量足够）
采样数量 = max_sample_size
```

## 参数关系图

```
                sample_threshold (扫描阈值)
                        │
                        ▼
    ┌───────────────────────────────────────┐
    │         实际扫描的数据量               │
    │   min(sample_threshold, 总数据量)      │
    └───────────────────────────────────────┘
                        │
                        │ 采样间隔 = sample_threshold / max_sample_size
                        ▼
    ┌───────────────────────────────────────┐
    │         采样间隔                       │
    │   每 N 行采样 1 个向量                 │
    └───────────────────────────────────────┘
                        │
                        ▼
    ┌───────────────────────────────────────┐
    │      max_sample_size (采样数量)       │
    │      最终用于 K-means 的向量数         │
    └───────────────────────────────────────┘
```

## 性能与质量权衡

| 配置 | 内存消耗 | K-means 时间 | 聚类质量 | 推荐场景 |
|------|----------|--------------|----------|----------|
| 不采样 (max=0) | 最高 | 最长 | 最好 | 数据量 < 100w |
| 高采样 (max=20w) | 高 | 长 | 好 | 数据量 100w~500w |
| 中采样 (max=10w) | 中 | 中 | 较好 | 数据量 500w~2000w |
| 低采样 (max=5w) | 低 | 短 | 一般 | 数据量 > 2000w |

## 常见问题

### Q1: 为什么设置了 max_sample_size，但实际采样数量少于预期？

**A**: 可能原因：
1. 数据总量少于 `sample_threshold`，采样间隔为 1，但数据量不足
2. 数据总量少于 `max_sample_size`

### Q2: 如何确保聚类均衡？

**A**: 
1. 增大 `sample_threshold`，使采样间隔更均匀
2. 确保 `max_sample_size` 足够大（建议 >= num_clusters * 1000）
3. 使用 K-means++ 初始化（代码中已默认启用）

### Q3: 采样对索引质量的影响？

**A**: 
- 采样只影响 K-means 聚类中心的质量
- 不影响最终索引的构建（所有向量都会被索引）
- 聚类中心质量影响搜索时选择正确的聚类
- 建议在可接受范围内使用较大的采样数量

## 监控与调试

构建索引时会输出日志信息：

```
NOTICE:  Collected 100000 vectors for k-means clustering
NOTICE:  K-means clustering completed with 4 centroids
NOTICE:  Cluster distribution:
NOTICE:    Cluster 0: 25000 vectors
NOTICE:    Cluster 1: 25000 vectors
NOTICE:    Cluster 2: 25000 vectors
NOTICE:    Cluster 3: 25000 vectors
```

通过观察聚类分布，可以判断采样是否均衡：
- 各聚类数量相近: 采样均匀，配置合理
- 各聚类数量差异大: 考虑增大 `sample_threshold` 或 `max_sample_size`

## 最佳实践总结

1. **数据量 < 100w**: 不采样，使用全部数据
2. **数据量 100w ~ 500w**: 采样 10w~20w，扫描 2x~5x
3. **数据量 500w ~ 2000w**: 采样 5w~10w，扫描 2x~5x
4. **数据量 > 2000w**: 采样 5w，扫描 1x~2x

**核心原则**: 
- `sample_threshold` 控制扫描量，影响采样均匀性
- `max_sample_size` 控制训练量，影响内存和时间
- 两者比值决定采样间隔，建议在 10~100 之间
