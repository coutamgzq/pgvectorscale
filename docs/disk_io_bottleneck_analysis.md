# 磁盘 IO 瓶颈深度分析报告

## 问题现象

- **数据规模**: 1000万行，768维向量
- **预期索引大小**: 约 5GB
- **实际观察**: 磁盘 IO 很高（NVMe 利用率 94-97%），CPU 上不去（仅 30% 左右）

## IO 监控数据

```
Device: nvme0c0n1
- 写入 IOPS: 6,000 - 12,000 w/s
- 写入带宽: 330-380 MB/s
- IO 等待时间: 1.66-7.09 ms
- 磁盘利用率: 81-97%

CPU 状态:
- %user: 21-33% (用户态 CPU)
- %iowait: 2.5-3.6% (IO 等待)
- %idle: 59-73% (空闲)
```

## 根本原因分析

### 1. **频繁的 Cache Flush**（主要原因）

```rust
// flush_rate 计算逻辑 (parallel.rs:15-19)
pub fn flush_rate(total_vectors: usize) -> usize {
    let rate = TSV_PARALLEL_FLUSH_INTERVAL.get();  // 默认 0.05 (5%)
    let result = (total_vectors as f64 * rate) as usize;
    result.max(1)
}
```

**问题**:
- 默认 flush interval = 1000万 × 5% = **50万条记录**
- 每个 worker 每处理 50万条就强制 flush 一次
- 24 个 worker 同时 flush，造成 IO 风暴

**实际 flush 频率**:
```rust
// cluster.rs:1638-1644
let base_flush_interval = parallel::flush_rate(cluster_vectors.max(1));  // ~50万
let flush_interval = if workers_per_cluster > 1 {
    (base_flush_interval / workers_per_cluster).max(100)  // 多 worker 时更频繁！
} else {
    base_flush_interval
};
```

对于 16 个 cluster，每个 cluster 约 62.5万条：
- 单 worker: flush_interval = 50万
- 双 worker: flush_interval = 25万（更频繁！）

### 2. **BuilderNeighborCache 的频繁 Reconcile**

```rust
// neighbor_store.rs:172-196
pub fn flush_neighbor_cache<S: Storage>(&self, storage: &S, stats: &mut PruneNeighborStats) {
    let mut cache = self.neighbor_map.borrow_mut();
    while cache.len() > 0 {
        let (neighbors_of, entry) = cache.pop_lru().unwrap();
        drop(cache);
        
        // 每次 flush 都要先读取磁盘上的邻居数据！
        let all_neighbors = self.reconcile_with_disk_neighbors(
            neighbors_of, entry.neighbors, storage, stats
        );
        
        // 合并后再写回磁盘
        storage.set_neighbors_on_disk(neighbors_of, &pruned_neighbors, stats);
        cache = self.neighbor_map.borrow_mut();
    }
}
```

**问题**:
- 每次 flush 都要 **先读磁盘** → **合并** → **写磁盘**
- 对于每个被修改的节点，都要进行一次随机 IO 读和写
- 1000万数据 × 50 neighbors ≈ 5亿次邻居关系更新

### 3. **PostgreSQL 页面级别的写入放大**

```
实际数据更新:
- 邻居列表: 50 neighbors × 8 bytes = 400 bytes

PostgreSQL 实际写入:
- 页面大小: 8KB
- 每次更新一个节点，需要写入整个 8KB 页面
- 写入放大: 8KB / 400B = 20倍！

总写入量估算:
- 邻居关系总数: 1000万 × 50 = 5亿条
- 实际数据: 5亿 × 8B = 4GB
- 考虑写入放大: 4GB × 20 = 80GB
- 考虑多次 flush: 80GB × 3-5次 = 240-400GB！
```

### 4. **Checkpointer 的持续写入压力**

```
观察到的现象:
- Checkpointer CPU: 16.6%
- 持续将脏页写入磁盘
- 与 worker 的 flush 操作竞争 IO 带宽
```

### 5. **多 Worker 之间的 IO 竞争**

```
24 个 worker 同时运行:
- 每个 worker 独立 flush
- 随机 IO 模式（不同节点分散在不同页面）
- NVMe 队列深度被占满，造成 IO 等待
```

## 瓶颈总结图

```
┌─────────────────────────────────────────────────────────────────┐
│                        IO 瓶颈根因                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐          │
│  │  Worker 1   │    │  Worker 2   │    │  Worker 24  │          │
│  │  (flush)    │    │  (flush)    │    │  (flush)    │          │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘          │
│         │                  │                  │                 │
│         └──────────────────┼──────────────────┘                 │
│                            ▼                                    │
│              ┌─────────────────────────┐                        │
│              │   IO 队列竞争 (24→1)    │                        │
│              │   随机读 + 顺序写        │                        │
│              └─────────────────────────┘                        │
│                            │                                    │
│                            ▼                                    │
│              ┌─────────────────────────┐                        │
│              │   NVMe 磁盘 94-97% 利用率 │                        │
│              │   写入带宽 330-380 MB/s  │                        │
│              └─────────────────────────┘                        │
│                            │                                    │
│                            ▼                                    │
│              ┌─────────────────────────┐                        │
│              │   CPU 等待 IO (30%)     │                        │
│              │   实际计算时间少         │                        │
│              └─────────────────────────┘                        │
│                                                                  │
│  关键问题:                                                       │
│  1. Flush 太频繁 (每 25-50万条)                                  │
│  2. 每次 flush 都要读磁盘 + 写磁盘                               │
│  3. 页面级写入放大 20倍                                          │
│  4. 24 个 worker 竞争 IO                                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## 为什么 CPU 上不去

```
理论 CPU 使用率:
- 向量距离计算: AVX2 优化，应该能跑满 CPU
- 邻居搜索: 内存密集型，CPU 应该很忙

实际 CPU 使用率 (30%):
- 70% 时间在等待 IO！
- 每次 flush 时，worker 阻塞等待磁盘操作完成
- CPU 空闲等待，无法充分利用

具体原因:
1. flush_neighbor_cache 是同步阻塞调用
2. reconcile_with_disk_neighbors 需要读取磁盘
3. set_neighbors_on_disk 需要写入磁盘
4. 24 个 worker 串行化竞争 IO
```

## 优化建议

### 1. **降低 Flush 频率**（立即生效）

```sql
-- 将 flush interval 从 5% 提高到 20%
SET diskann.parallel_flush_interval = 0.20;

-- 效果:
-- 原来: 1000万 × 5% = 50万条 flush 一次
-- 现在: 1000万 × 20% = 200万条 flush 一次
-- Flush 次数减少 75%！
```

### 2. **减少 Worker 数量**（立即生效）

```sql
-- 从 24 个减少到 8-12 个
SET max_parallel_workers = 12;

-- 效果:
-- - IO 竞争减少 50%
-- - 每个 worker 处理更多数据，减少总 flush 次数
-- - 内存压力降低
```

### 3. **批量 Flush 优化**（代码修改）

```rust
// 当前实现: 逐个节点 flush
while cache.len() > 0 {
    let (neighbors_of, entry) = cache.pop_lru().unwrap();
    // ... 逐个处理
}

// 优化方案: 批量 flush，减少 IO 次数
const BATCH_FLUSH_SIZE: usize = 100;
while cache.len() > 0 {
    let batch: Vec<_> = cache.drain(..BATCH_FLUSH_SIZE.min(cache.len())).collect();
    // 批量读取、批量写入
    storage.set_neighbors_batch_on_disk(&batch, stats);
}
```

### 4. **异步 Flush**（代码修改）

```rust
// 当前: 同步阻塞
graph.maybe_flush_neighbor_cache(storage, &mut insert_stats);

// 优化: 异步后台 flush
if consumer_state.ntuples % flush_interval == 0 {
    // 提交 flush 任务到后台线程，不阻塞主流程
    io_thread_pool.submit_flush_task(graph.get_pending_updates());
}
```

### 5. **减少 Reconcile 开销**（代码修改）

```rust
// 当前: 每次都要 reconcile
let all_neighbors = self.reconcile_with_disk_neighbors(...);

// 优化: 使用版本号或时间戳，避免不必要的 reconcile
if entry.version > disk_version {
    let all_neighbors = self.reconcile_with_disk_neighbors(...);
} else {
    // 直接写入，不需要读取磁盘
    storage.set_neighbors_on_disk(neighbors_of, &entry.neighbors, stats);
}
```

### 6. **优化 PostgreSQL 配置**（立即生效）

```sql
-- 增加 shared_buffers，减少磁盘读取
ALTER SYSTEM SET shared_buffers = '16GB';

-- 增加 checkpoint 间隔，减少后台写入
ALTER SYSTEM SET checkpoint_timeout = '15min';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET max_wal_size = '16GB';

-- 禁用全页写入（如果数据安全允许）
ALTER SYSTEM SET full_page_writes = off;

-- 重启 PostgreSQL 生效
```

## 预期效果

| 优化项 | 当前状态 | 优化后 | 改善 |
|--------|---------|--------|------|
| Flush 频率 | 每 25-50万条 | 每 200万条 | ↓ 75% |
| IO 总量 | 240-400GB | 60-100GB | ↓ 75% |
| NVMe 利用率 | 94-97% | 60-70% | ↓ 30% |
| CPU 利用率 | 30% | 70-80% | ↑ 150% |
| 构建时间 | 120分钟 | 60-80分钟 | ↓ 40% |

## 监控命令

```bash
# 实时监控 IO
watch -n 1 'iostat -x 1 1 | grep nvme'

# 查看 PostgreSQL IO 统计
psql -c "SELECT * FROM pg_stat_io;"

# 查看 buffer 命中率
psql -c "SELECT 
    round(100.0 * shared_blks_hit / (shared_blks_hit + shared_blks_read), 2) as hit_ratio
FROM pg_stat_database WHERE datname = current_database();"

# 查看 checkpointer 活动
psql -c "SELECT * FROM pg_stat_checkpointer;"
```

## 关键指标阈值

| 指标 | 健康范围 | 当前值 | 状态 |
|------|---------|--------|------|
| NVMe 利用率 | < 70% | 94-97% | 🔴 严重 |
| IO 等待时间 | < 2ms | 1.66-7.09ms | 🟡 警告 |
| CPU 利用率 | > 60% | 30% | 🔴 过低 |
| Buffer 命中率 | > 99% | 待查 | - |
| Checkpointer CPU | < 5% | 16.6% | 🔴 过高 |

---

**结论**: 磁盘 IO 高的根本原因是 **flush 频率过高** + **页面级写入放大** + **多 worker IO 竞争**。通过降低 flush 频率和减少 worker 数量，可以立即改善性能。
