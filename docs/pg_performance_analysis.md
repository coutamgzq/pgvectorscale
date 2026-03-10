# PostgreSQL 性能监控与瓶颈分析报告

## 监控时间
2026-03-09 20:19 - 21:00 CST

## 实时监控状态
✅ **监控脚本已启动**: `/home/zhangqiang/code/postgres/contrib/pgvectorscale/scripts/pg_monitor.sh`
- 采样间隔: 5秒
- 监控时长: 60秒
- 日志文件: `/home/zhangqiang/code/postgres/contrib/pgvectorscale/logs/pg_monitor_20260309_205911.log`

## 一、进程概览

### 1.1 主要 PostgreSQL 进程

| PID | 用户 | CPU% | 内存% | RSS | 进程类型 | 状态 |
|-----|------|------|-------|-----|----------|------|
| 5714 | zhangqi+ | 0.0 | 0.3 | 452MB | postgres main | Ss |
| 5715 | zhangqi+ | 16.6 | 10.8 | 14.2GB | checkpointer | Rs |
| 5716 | zhangqi+ | 0.0 | 0.1 | 174MB | background writer | Ss |
| 5848 | zhangqi+ | 1.0 | 0.0 | 21MB | walwriter | Ss |
| 7562 | zhangqi+ | 7.7 | 21.3 | 28.1GB | CREATE INDEX (主进程) | Ssl |

### 1.2 并行工作进程 (Parallel Workers)

当前有 24 个并行工作进程正在运行，用于构建向量索引：

| PID | CPU% | 内存% | RSS | Cluster ID | 状态 |
|-----|------|-------|-----|------------|------|
| 508624 | 57.1 | 2.1 | 2.9GB | 7 | Rs |
| 508625 | 59.2 | 2.3 | 3.0GB | 9 | Rs |
| 508626 | 61.3 | 2.2 | 2.9GB | 13 | Rs |
| 508627 | 62.4 | 2.3 | 3.1GB | 3 | Rs |
| 508630 | 61.5 | 2.3 | 3.1GB | 13 | Rs |
| 508634 | 58.9 | 2.2 | 3.0GB | 9 | Rs |
| 508636 | 57.6 | 2.2 | 2.9GB | 7 | Rs |
| 508640 | 62.4 | 2.3 | 3.0GB | 3 | Rs |
| 508643 | 48.8 | 1.9 | 2.5GB | 0 | Rs |

**其他工作进程 CPU 使用率**: 16% - 50% 不等

## 二、瓶颈分析

### 2.1 CPU 瓶颈分析

#### 高 CPU 使用进程
```
Cluster 3:  PID 508627 (62.4%), PID 508640 (62.4%) - 2个worker
Cluster 7:  PID 508624 (57.1%), PID 508636 (57.6%) - 2个worker
Cluster 9:  PID 508625 (59.2%), PID 508634 (58.9%) - 2个worker
Cluster 13: PID 508626 (61.3%), PID 508630 (61.5%) - 2个worker
```

**分析**: 
- 部分 cluster 的 worker CPU 使用率高达 60%+
- 存在负载不均衡现象：cluster 12, 14, 5 的 CPU 使用率较低 (16-26%)
- Checkpointer 进程 CPU 使用率为 16.6%，说明有较多的脏页需要写入磁盘

### 2.2 内存瓶颈分析

#### 内存使用分布
```
总内存使用: 约 62GB
- CREATE INDEX 主进程: 28.1GB (21.3%)
- Checkpointer: 14.2GB (10.8%)
- 24个并行 worker: 约 2-3GB/个，总计约 55GB
```

**分析**:
- 内存使用率达到 87% 左右 (54GB/62GB)
- 单个 worker 内存占用 2-3GB，符合预期（队列 + 图构建缓存）
- 内存不是当前瓶颈，但接近上限

### 2.3 IO 瓶颈分析

#### 关键指标
- **Checkpointer 高 CPU**: 16.6%，表明频繁的脏页刷新
- **Walwriter 活动**: 1.0% CPU，WAL 写入正常

**潜在 IO 瓶颈**:
1. 索引构建过程中大量随机写入
2. 图结构更新导致频繁的页面读写
3. 24 个 worker 同时写入可能造成 IO 争用

### 2.4 负载均衡分析

#### Cluster 间负载差异

| Cluster | Worker 数量 | 平均 CPU% | 状态 |
|---------|-------------|-----------|------|
| 0 | 2 | 48-50% | 正常 |
| 1 | 1 | 30.5% | 偏低 |
| 2 | 2 | 45-46% | 正常 |
| 3 | 2 | 62% | **高负载** |
| 4 | 2 | 45-46% | 正常 |
| 5 | 1 | 18.4% | **低负载** |
| 6 | 1 | 32.5% | 偏低 |
| 7 | 2 | 57-58% | **高负载** |
| 8 | 1 | 30.9% | 偏低 |
| 9 | 2 | 59% | **高负载** |
| 10 | 1 | 23.9% | **低负载** |
| 11 | 2 | 46-47% | 正常 |
| 12 | 1 | 16.2% | **低负载** |
| 13 | 2 | 61% | **高负载** |
| 14 | 1 | 26.3% | **低负载** |
| 15 | 1 | 50.3% | 正常 |

**分析**:
- **高负载 Clusters**: 3, 7, 9, 13 (CPU > 55%)
- **低负载 Clusters**: 5, 10, 12, 14 (CPU < 27%)
- **负载不均衡原因**: 
  - 数据分布不均匀（某些 cluster 数据量更大）
  - 某些 cluster 的图结构更复杂，邻居搜索开销更大

## 三、监控脚本

### 3.1 实时监控命令

```bash
# 1. 查看 PostgreSQL 进程整体状态
ps aux | grep postgres | grep -v grep

# 2. 按 CPU 排序查看 worker 进程
ps aux --sort=-%cpu | grep "parallel worker" | head -20

# 3. 按内存排序查看 worker 进程
ps aux --sort=-%mem | grep "parallel worker" | head -20

# 4. 查看系统整体资源使用
top -p $(pgrep -d',' postgres)

# 5. 查看磁盘 IO
iostat -x 1

# 6. 查看 PostgreSQL 统计信息
psql -c "SELECT * FROM pg_stat_activity WHERE state = 'active';"
```

### 3.2 持续监控脚本

```bash
#!/bin/bash
# pg_monitor.sh - PostgreSQL 性能监控脚本

LOG_FILE="pg_monitor_$(date +%Y%m%d_%H%M%S).log"
INTERVAL=5

echo "Starting PostgreSQL monitoring..." | tee -a $LOG_FILE
echo "Timestamp,PID,User,CPU%,MEM%,RSS,Command" | tee -a $LOG_FILE

while true; do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    ps aux | grep postgres | grep -v grep | while read line; do
        echo "$TIMESTAMP,$line" | awk '{print $1","$2","$3","$4","$5","$6","$11}' | tee -a $LOG_FILE
    done
    sleep $INTERVAL
done
```

## 四、优化建议

### 4.1 短期优化（立即生效）

1. **调整并行度**
   - 当前 24 个 worker 可能导致 IO 争用
   - 建议减少到 16 个 worker，观察性能变化

2. **优化内存配置**
   ```sql
   -- 增加 shared_buffers（如果内存允许）
   ALTER SYSTEM SET shared_buffers = '16GB';
   
   -- 增加 work_mem
   ALTER SYSTEM SET work_mem = '256MB';
   
   -- 调整 maintenance_work_mem
   ALTER SYSTEM SET maintenance_work_mem = '4GB';
   ```

3. **优化检查点**
   ```sql
   -- 增加检查点间隔，减少 IO 压力
   ALTER SYSTEM SET checkpoint_completion_target = 0.9;
   ALTER SYSTEM SET max_wal_size = '8GB';
   ```

### 4.2 中期优化（代码层面）

1. **改进负载均衡**
   - 根据 cluster 实际数据量动态分配 worker 数量
   - 高负载 cluster 分配更多 worker

2. **优化队列大小**
   - 当前队列大小 10240 可能过大
   - 根据内存情况调整为 5120 或 2048

3. **批量处理优化**
   - 增加 batch size，减少锁争用
   - 优化图构建算法，减少随机访问

### 4.3 长期优化（架构层面）

1. **分区策略**
   - 根据数据特征优化 K-Means 聚类
   - 确保数据在各个 cluster 间均匀分布

2. **硬件升级**
   - 使用 SSD 替代 HDD，减少 IO 瓶颈
   - 增加内存，减少磁盘交换

3. **并行策略优化**
   - 考虑使用流式处理替代批处理
   - 实现动态负载均衡算法

## 五、监控指标阈值

| 指标 | 正常范围 | 警告阈值 | 危险阈值 |
|------|----------|----------|----------|
| CPU% | < 50% | 50-80% | > 80% |
| 内存% | < 70% | 70-85% | > 85% |
| IO Wait | < 10% | 10-30% | > 30% |
| Worker CPU 差异 | < 20% | 20-40% | > 40% |

## 六、实时监控结果（20:59 - 21:00）

### 6.1 实时统计数据

基于监控脚本的实时采集数据：

| 指标 | 数值 |
|------|------|
| 采样时间 | 2026-03-09 20:59:11 - 20:59:17 |
| PostgreSQL 进程数 | 32 个 |
| 并行 Worker 数 | 24 个 |
| 总 CPU 使用率 | ~1000%（10 核满载）|
| 总内存使用 | ~55GB |

### 6.2 关键发现

#### 🔴 高负载 Cluster（CPU > 55%）
```
Cluster 3:  PID 508627 (62.2%), PID 508640 (62.2%)
Cluster 7:  PID 508624 (56.9%), PID 508636 (57.4%)
Cluster 9:  PID 508625 (59.1%), PID 508634 (58.8%)
Cluster 13: PID 508626 (61.2%), PID 508630 (61.3%)
```

#### 🟡 低负载 Cluster（CPU < 20%）
```
Cluster 5:  PID 508638 (18.3%) - 单 worker
Cluster 12: PID 508631 (16.0%) - 单 worker
```

#### 🟢 正常负载 Cluster（CPU 30-50%）
```
Cluster 0, 2, 4, 6, 8, 10, 11, 14, 15
```

### 6.3 瓶颈确认

1. **CPU 瓶颈**: ✅ 确认
   - 4 个 cluster 的 CPU 使用率超过 55%
   - 系统总 CPU 使用率接近 1000%（10 核满载）
   - 负载不均衡：最高 62% vs 最低 16%

2. **内存瓶颈**: ⚠️ 警告
   - 总内存使用 55GB / 62GB（88%）
   - 仍在安全范围内，但接近上限

3. **IO 瓶颈**: ⚠️ 疑似
   - Checkpointer CPU 16.6%，持续写入
   - 需要进一步监控 `iostat` 确认

## 七、紧急优化建议

### 7.1 立即执行（1 分钟内）

```bash
# 1. 查看实时 IO 状态
iostat -x 1 5

# 2. 查看 PostgreSQL 锁等待
psql -c "SELECT * FROM pg_locks WHERE NOT granted;"

# 3. 查看慢查询
psql -c "SELECT * FROM pg_stat_activity WHERE state = 'active' ORDER BY query_start;"
```

### 7.2 短期优化（5 分钟内）

1. **降低并行度**（如果 IO 瓶颈确认）
   ```sql
   -- 减少 worker 数量到 16
   SET max_parallel_workers = 16;
   ```

2. **优化内存配置**
   ```sql
   -- 增加 work_mem
   SET work_mem = '256MB';
   
   -- 增加 maintenance_work_mem
   SET maintenance_work_mem = '4GB';
   ```

3. **调整检查点**
   ```sql
   -- 降低检查点频率
   SET checkpoint_timeout = '10min';
   SET checkpoint_completion_target = 0.9;
   ```

### 7.3 中期优化（30 分钟内）

1. **重新设计 Cluster 分配策略**
   - 根据数据量动态分配 worker 数量
   - 高数据量 cluster 分配更多 worker

2. **优化队列大小**
   ```sql
   -- 降低队列大小，减少内存压力
   SET diskann.cluster_queue_capacity = 5120;
   ```

## 八、监控脚本使用说明

### 8.1 启动监控
```bash
cd /home/zhangqiang/code/postgres/contrib/pgvectorscale
./scripts/pg_monitor.sh [采样间隔秒数] [监控时长秒数]

# 示例：每 5 秒采样一次，监控 10 分钟
./scripts/pg_monitor.sh 5 600
```

### 8.2 查看实时日志
```bash
# 查看最新日志
tail -f /home/zhangqiang/code/postgres/contrib/pgvectorscale/logs/pg_monitor_*.log

# 查看统计摘要
grep "统计摘要" /home/zhangqiang/code/postgres/contrib/pgvectorscale/logs/pg_monitor_*.log
```

### 8.3 数据分析
```bash
# 使用 Python 分析监控数据
python3 << EOF
import pandas as pd

# 读取最新日志
import glob
log_files = glob.glob('/home/zhangqiang/code/postgres/contrib/pgvectorscale/logs/pg_monitor_*.log')
latest_log = max(log_files, key=os.path.getctime)

df = pd.read_csv(latest_log, skiprows=6)

# 生成分析报告
print("=== 性能分析报告 ===")
print(f"监控时长: {df['timestamp'].nunique()} 个时间点")
print(f"\nCPU 使用统计:")
print(f"  平均: {df['cpu_percent'].mean():.2f}%")
print(f"  最大: {df['cpu_percent'].max():.2f}%")
print(f"  标准差: {df['cpu_percent'].std():.2f}%")

print(f"\n内存使用统计:")
print(f"  平均: {(df['rss'].mean()/1024/1024):.2f} GB")
print(f"  最大: {(df['rss'].max()/1024/1024):.2f} GB")
EOF
```

## 九、下一步行动

1. **✅ 已完成**: 启动监控脚本，收集实时性能数据
2. **🔄 进行中**: 分析监控数据，识别具体瓶颈
3. **⏳ 待执行**: 根据分析结果调整 PostgreSQL 参数
4. **⏳ 待执行**: 验证优化效果，对比性能指标
5. **⏳ 待执行**: 生成最终优化报告

----

**报告生成时间**: 2026-03-09 21:00 CST  
**监控进程**: CREATE INDEX (PID 7562)  
**数据规模**: 10,000,000 向量  
**当前进度**: 58.0% (5,800,000 / 10,000,000)  
**监控脚本**: `/home/zhangqiang/code/postgres/contrib/pgvectorscale/scripts/pg_monitor.sh`  
**数据规模**: 10,000,000 向量  
**当前进度**: 58.0% (5,800,000 / 10,000,000)
