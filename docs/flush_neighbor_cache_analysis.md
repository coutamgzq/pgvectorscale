# flush_neighbor_cache 写入 PostgreSQL Page 流程分析

## 概述

本文档详细分析 `flush_neighbor_cache` 如何将邻居数据写入到 PostgreSQL 的 page，并设计日志方案来验证同一个 cluster 下的多个 worker 是否正确地构造同一个图。

## 完整调用链

```
flush_neighbor_cache
  ↓
storage.set_neighbors_on_disk
  ↓
SbqNode::modify (获取可写节点)
  ↓
WritablePage::modify (获取可写页面)
  ↓
archived.set_neighbors (修改邻居数据)
  ↓
node.commit() (提交修改)
  ↓
WritablePage::commit() (写入 WAL)
  ↓
MarkBufferDirty + GenericXLogFinish (标记脏页并写入 WAL)
```

## 详细流程分析

### 1. flush_neighbor_cache (neighbor_store.rs:170-197)

```rust
pub fn flush_neighbor_cache<S: Storage>(&self, storage: &S, stats: &mut PruneNeighborStats) {
    let mut cache = self.neighbor_map.borrow_mut();
    while cache.len() > 0 {
        // 1. 从 LRU 缓存中弹出最旧的条目
        let (neighbors_of, entry) = cache.pop_lru().unwrap();
        drop(cache);
        
        // 2. 与磁盘数据合并
        let all_neighbors =
            self.reconcile_with_disk_neighbors(neighbors_of, entry.neighbors, storage, stats);

        // 3. 如果邻居数量超过限制，进行剪枝
        let pruned_neighbors = if all_neighbors.len() > self.num_neighbors {
            Graph::prune_neighbors(
                self.max_alpha,
                self.num_neighbors,
                entry.labels.as_ref(),
                all_neighbors,
                storage,
                stats,
            )
        } else {
            all_neighbors
        };

        // 4. 写入到磁盘
        storage.set_neighbors_on_disk(neighbors_of, &pruned_neighbors, stats);
        cache = self.neighbor_map.borrow_mut();
    }
}
```

**关键点**：
- `neighbors_of`: 被设置邻居的节点的 ItemPointer (block_number, offset)
- `pruned_neighbors`: 剪枝后的邻居列表
- 每个邻居包含 `index_pointer_to_neighbor` (指向邻居节点的 ItemPointer)

### 2. storage.set_neighbors_on_disk (sbq/storage.rs:416-431)

```rust
fn set_neighbors_on_disk<S: StatsNodeModify + StatsNodeRead>(
    &self,
    index_pointer: IndexPointer,
    neighbors: &[NeighborWithDistance],
    stats: &mut S,
) {
    let mut cache = self.cache().as_ref().unwrap().borrow_mut();

    // 预加载缓存，避免死锁
    let iter = neighbors
        .iter()
        .map(|n| n.get_index_pointer_to_neighbor())
        .chain(once(index_pointer));
    cache.preload(iter, self, stats);

    // 获取可写节点
    let mut node =
        unsafe { SbqNode::modify(self.index, index_pointer, self.has_labels, stats) };
    let mut archived = node.get_archived_node();
    
    // 设置邻居
    archived.set_neighbors(neighbors, self.num_neighbors);
    
    // 提交修改
    node.commit();
}
```

**关键点**：
- `index_pointer`: 要修改的节点的位置 (block_number, offset)
- `neighbors`: 邻居列表
- `SbqNode::modify` 获取页面的排他锁
- `node.commit()` 提交修改并写入 WAL

### 3. SbqNode::modify (sbq/node.rs:102-113)

```rust
pub unsafe fn modify<'a, S: StatsNodeModify>(
    index: &'a PgRelation,
    index_pointer: ItemPointer,
    has_labels: bool,
    stats: &mut S,
) -> WritableSbqNode<'a> {
    if has_labels {
        WritableSbqNode::Labeled(LabeledSbqNode::modify(index, index_pointer, stats))
    } else {
        WritableSbqNode::Classic(ClassicSbqNode::modify(index, index_pointer, stats))
    }
}
```

**关键点**：
- 根据 `index_pointer` 定位到具体的页面
- 调用 `ClassicSbqNode::modify` 或 `LabeledSbqNode::modify`

### 4. ClassicSbqNode::modify (由 pgvectorscale_derive 宏生成)

```rust
// 伪代码，实际由宏生成
pub unsafe fn modify<'a, S: StatsNodeModify>(
    index: &'a PgRelation,
    index_pointer: ItemPointer,
    stats: &mut S,
) -> WritableClassicSbqNode<'a> {
    // 1. 获取可写页面
    let writable_buffer = index_pointer.modify_bytes(index);
    
    // 2. 返回可写节点包装器
    WritableClassicSbqNode {
        buffer: writable_buffer,
        // ... 其他字段
    }
}
```

**关键点**：
- 调用 `ItemPointer::modify_bytes` 获取可写页面
- 这会获取页面的排他锁

### 5. ItemPointer::modify_bytes (util/mod.rs)

```rust
pub unsafe fn modify_bytes(self, index: &PgRelation) -> WritableBuffer<'_> {
    // 1. 获取可写页面
    let page = WritablePage::modify(index, self.block_number);
    
    // 2. 获取页面中的具体项
    let item_id = PageGetItemId(*page, self.offset);
    let item = PageGetItem(*page, item_id) as *mut u8;
    let len = (*item_id).lp_len();
    
    // 3. 返回可写缓冲区
    WritableBuffer {
        _page: page,
        len,
        ptr: item,
    }
}
```

**关键点**：
- `self.block_number`: 页面号
- `self.offset`: 页面内的偏移量
- `WritablePage::modify` 获取页面的排他锁

### 6. WritablePage::modify (util/page.rs:138-141)

```rust
pub fn modify(index: &'a PgRelation, block: BlockNumber) -> Self {
    let buffer = LockedBufferExclusive::read(index, block);
    Self::modify_with_buffer(index, buffer)
}
```

**关键点**：
- `block`: 页面号
- `LockedBufferExclusive::read` 获取页面的排他锁
- 这确保同一时间只有一个进程可以修改这个页面

### 7. WritablePage::commit (util/page.rs:227-233)

```rust
pub fn commit(mut self) {
    unsafe {
        // 1. 标记缓冲区为脏
        pg_sys::MarkBufferDirty(*self.buffer);
        
        // 2. 写入 WAL 并提交
        pg_sys::GenericXLogFinish(self.state);
    }
    self.committed = true;
}
```

**关键点**：
- `MarkBufferDirty`: 标记页面为脏，后续会被刷写到磁盘
- `GenericXLogFinish`: 写入 WAL 日志，确保崩溃恢复
- 这是真正写入 PostgreSQL page 的地方

## PostgreSQL Page 结构

### Page 布局

```
+------------------+
| Page Header      |  (24 bytes)
+------------------+
| Item Pointers    |  (每个 4 bytes)
| (Line Pointer)   |
+------------------+
| Free Space       |
+------------------+
| Items (Tuples)   |
+------------------+
| Special Space    |  (TsvPageOpaqueData)
+------------------+
```

### ItemPointer 结构

```rust
pub struct ItemPointer {
    pub block_number: pg_sys::BlockNumber,  // 页面号 (4 bytes)
    pub offset: pg_sys::OffsetNumber,       // 页面内偏移 (2 bytes)
}
```

### SbqNode 在 Page 中的存储

```rust
#[repr(C)]
pub struct ArchivedClassicSbqNode {
    pub heap_item_pointer: ArchivedItemPointer,           // 8 bytes
    pub bq_vector: ArchivedVec<ArchivedSbqVectorElement>, // 量化向量
    pub neighbor_index_pointers: ArchivedVec<ArchivedItemPointer>, // 邻居列表
}
```

## 并发写入机制

### 1. 页面级锁

PostgreSQL 使用页面级锁来保护并发写入：

```rust
// 获取排他锁
let buffer = LockedBufferExclusive::read(index, block);

// 修改页面数据
// ...

// 释放锁（在 commit 或 drop 时）
buffer.commit();  // 或 drop(buffer)
```

**关键点**：
- 同一时间只有一个进程可以修改一个页面
- 如果多个 worker 修改同一个页面，它们会串行执行
- 这确保了数据的一致性

### 2. WAL (Write-Ahead Logging)

```rust
// 开始 WAL 记录
let state = pg_sys::GenericXLogStart(index.as_ptr());

// 注册要修改的页面
let page = pg_sys::GenericXLogRegisterBuffer(state, *buffer, 0);

// 修改页面数据
// ...

// 写入 WAL 并提交
pg_sys::GenericXLogFinish(state);
```

**关键点**：
- 所有修改都会先写入 WAL
- 确保崩溃恢复时可以重放修改
- WAL 是 PostgreSQL 事务系统的核心

### 3. 为什么不带 cluster 的并行构建能正确工作？

#### 场景 1: 不同 worker 修改不同页面

```
Worker 1: 修改 Page 10, Item 1
Worker 2: 修改 Page 20, Item 1
→ 可以并行执行，因为修改的是不同的页面
```

#### 场景 2: 不同 worker 修改同一页面

```
Worker 1: 修改 Page 10, Item 1
Worker 2: 修改 Page 10, Item 2
→ 串行执行，因为需要获取同一页面的排他锁
```

#### 场景 3: 不同 worker 修改同一节点

```
Worker 1: 修改 Page 10, Item 1 的邻居列表
Worker 2: 修改 Page 10, Item 1 的邻居列表
→ 串行执行，但可能丢失更新！
```

**问题**：
- 如果 Worker 1 和 Worker 2 同时读取了同一个节点的邻居列表
- Worker 1 添加邻居 A，Worker 2 添加邻居 B
- Worker 1 先写入，Worker 2 后写入
- 最终结果：只有邻居 B，邻居 A 丢失！

**解决方案**：
- `reconcile_with_disk_neighbors` 机制
- 每次写入前，先从磁盘读取最新的邻居列表
- 合并缓存中的邻居和磁盘中的邻居
- 然后写入合并后的结果

```rust
fn reconcile_with_disk_neighbors<S: Storage>(
    &self,
    neighbors_of: ItemPointer,
    cached_neighbors: Vec<NeighborWithDistance>,
    storage: &S,
    stats: &mut PruneNeighborStats,
) -> Vec<NeighborWithDistance> {
    // 1. 从磁盘读取最新的邻居列表
    let disk_neighbors = storage.get_neighbors_with_distances_from_disk(neighbors_of, stats);

    // 2. 合并缓存和磁盘的邻居
    let mut neighbor_map: HashMap<ItemPointer, NeighborWithDistance> = HashMap::new();

    for neighbor in disk_neighbors {
        neighbor_map.insert(neighbor.get_index_pointer_to_neighbor(), neighbor);
    }

    for neighbor in cached_neighbors {
        neighbor_map.insert(neighbor.get_index_pointer_to_neighbor(), neighbor);
    }

    // 3. 返回合并后的邻居列表
    neighbor_map.into_values().collect()
}
```

## 日志方案设计

### 目标

验证同一个 cluster 下的多个 worker 是否正确地构造同一个图，通过日志记录：
1. 哪个 worker (进程名) 写入了哪个页面
2. 写入了哪些邻居的 ItemPointer
3. 验证所有写入同一个 cluster 的 worker 是否属于同一个 cluster

### 日志位置

在 `storage.set_neighbors_on_disk` 函数中添加日志：

```rust
fn set_neighbors_on_disk<S: StatsNodeModify + StatsNodeRead>(
    &self,
    index_pointer: IndexPointer,
    neighbors: &[NeighborWithDistance],
    stats: &mut S,
) {
    // ===== 新增日志 =====
    let worker_name = unsafe {
        std::ffi::CStr::from_ptr(pg_sys::MyBackendType)
            .to_str()
            .unwrap_or("unknown")
    };
    
    let neighbor_tids: Vec<String> = neighbors
        .iter()
        .map(|n| {
            let ip = n.get_index_pointer_to_neighbor();
            format!("({},{})", ip.block_number, ip.offset)
        })
        .collect();
    
    log!(
        "[Worker {}] Writing to page {} offset {}: neighbors = [{}]",
        worker_name,
        index_pointer.block_number,
        index_pointer.offset,
        neighbor_tids.join(", ")
    );
    // ===== 新增日志结束 =====
    
    let mut cache = self.cache().as_ref().unwrap().borrow_mut();

    let iter = neighbors
        .iter()
        .map(|n| n.get_index_pointer_to_neighbor())
        .chain(once(index_pointer));
    cache.preload(iter, self, stats);

    let mut node =
        unsafe { SbqNode::modify(self.index, index_pointer, self.has_labels, stats) };
    let mut archived = node.get_archived_node();
    archived.set_neighbors(neighbors, self.num_neighbors);
    node.commit();
}
```

### 日志内容

每条日志包含：
1. **Worker 名称**: `worker_name` (如 "vectorscale_build_cluster_0"，通过 `set_ps_display` 设置)
2. **写入的页面**: `index_pointer.block_number`
3. **页面内偏移**: `index_pointer.offset`
4. **邻居列表**: 所有邻居的 ItemPointer

**注意**: Worker 名称已经包含了 cluster ID 信息（在 cluster.rs:1217-1221 中设置），因此不需要额外打印 PID。

### 日志示例

```
[Worker vectorscale_build_cluster_0] Writing to page 10 offset 1: neighbors = [(11,1), (12,1), (13,1)]
[Worker vectorscale_build_cluster_0] Writing to page 10 offset 2: neighbors = [(11,2), (12,2), (13,2)]
[Worker vectorscale_build_cluster_0] Writing to page 11 offset 1: neighbors = [(10,1), (12,1), (13,1)]
[Worker vectorscale_build_cluster_0] Writing to page 11 offset 2: neighbors = [(10,2), (12,2), (13,2)]
```

### 验证方法

#### 1. 验证同一 cluster 的 worker

如果同一个 cluster 的多个 worker 正确地构造同一个图，那么：
- 所有写入同一个 cluster 的 worker 应该有相同的 cluster ID（从 worker 名称可以看出）
- Worker 名称格式为 `vectorscale_build_cluster_{cluster_id}`

#### 2. 验证邻居关系

- 如果 Worker 写入节点 X 的邻居包含节点 Y
- 那么应该有 Worker（可能是同一个或其他 worker）写入节点 Y 的邻居包含节点 X
- 这验证了图的对称性

#### 3. 验证页面访问模式

- 如果多个 worker 写入同一个页面，日志会显示串行访问
- 可以通过时间戳验证并发控制

### 增强日志方案

为了更好地验证，可以在日志中添加 cluster ID：

```rust
// 需要从 ConsumerState 传递 cluster_id 到 storage
// 或者在 storage 中存储 cluster_id

log!(
    "[Cluster {} Worker {} PID {}] Writing to page {} offset {}: neighbors = [{}]",
    cluster_id,  // 需要传递
    worker_name,
    worker_pid,
    index_pointer.block_number,
    index_pointer.offset,
    neighbor_tids.join(", ")
);
```

## 实现步骤

### 步骤 1: 在 storage 中添加日志

修改 `sbq/storage.rs` 的 `set_neighbors_on_disk` 函数：

```rust
fn set_neighbors_on_disk<S: StatsNodeModify + StatsNodeRead>(
    &self,
    index_pointer: IndexPointer,
    neighbors: &[NeighborWithDistance],
    stats: &mut S,
) {
    // 添加日志
    log_neighbor_write(index_pointer, neighbors);
    
    // 原有代码
    let mut cache = self.cache().as_ref().unwrap().borrow_mut();
    // ...
}

fn log_neighbor_write(index_pointer: IndexPointer, neighbors: &[NeighborWithDistance]) {
    let worker_name = unsafe {
        std::ffi::CStr::from_ptr(pg_sys::MyBackendType)
            .to_str()
            .unwrap_or("unknown")
    };
    
    let neighbor_tids: Vec<String> = neighbors
        .iter()
        .map(|n| {
            let ip = n.get_index_pointer_to_neighbor();
            format!("({},{})", ip.block_number, ip.offset)
        })
        .collect();
    
    log!(
        "[Worker {}] Writing to page {} offset {}: neighbors = [{}]",
        worker_name,
        index_pointer.block_number,
        index_pointer.offset,
        neighbor_tids.join(", ")
    );
}
```

### 步骤 2: 在 plain/storage.rs 中添加相同的日志

如果使用 plain storage，也需要添加相同的日志。

### 步骤 3: 测试验证

运行测试并检查日志：

```bash
# 运行 cluster 并行构建测试
psql -c "SELECT * FROM test_parallel_cluster_build();"

# 检查日志
tail -f /var/log/postgresql/postgresql-*.log | grep "Writing to page"
```

### 步骤 4: 分析日志

编写脚本分析日志：

```python
import re
from collections import defaultdict

# 解析日志
log_pattern = r'\[Worker ([\w_]+)\] Writing to page (\d+) offset (\d+): neighbors = \[(.*?)\]'
worker_writes = defaultdict(list)

with open('postgresql.log', 'r') as f:
    for line in f:
        match = re.search(log_pattern, line)
        if match:
            worker_name, page, offset, neighbors = match.groups()
            worker_writes[worker_name].append({
                'page': int(page),
                'offset': int(offset),
                'neighbors': neighbors
            })

# 分析每个 worker 的写入模式
for worker_name, writes in worker_writes.items():
    print(f"Worker {worker_name}:")
    print(f"  Total writes: {len(writes)}")
    pages = set(w['page'] for w in writes)
    print(f"  Unique pages: {len(pages)}")
    print(f"  Pages: {sorted(pages)}")
```

## 预期结果

### 正确情况

如果同一个 cluster 的多个 worker 正确地构造同一个图：

```
[Worker vectorscale_build_cluster_0] Writing to page 10 offset 1: neighbors = [(11,1), (12,1)]
[Worker vectorscale_build_cluster_0] Writing to page 10 offset 2: neighbors = [(11,2), (12,2)]
[Worker vectorscale_build_cluster_0] Writing to page 11 offset 1: neighbors = [(10,1), (12,1)]
[Worker vectorscale_build_cluster_0] Writing to page 11 offset 2: neighbors = [(10,2), (12,2)]
```

**特征**：
- Worker 名称包含 cluster ID（如 `vectorscale_build_cluster_0`）
- 邻居关系对称
- 页面访问模式合理

### 错误情况

如果每个 worker 构造独立的图：

```
[Worker vectorscale_build_cluster_0] Writing to page 10 offset 1: neighbors = [(11,1), (12,1)]
[Worker vectorscale_build_cluster_0] Writing to page 11 offset 1: neighbors = [(10,1), (12,1)]
[Worker vectorscale_build_cluster_0] Writing to page 12 offset 1: neighbors = [(10,1), (11,1)]
[Worker vectorscale_build_cluster_1] Writing to page 20 offset 1: neighbors = [(21,1), (22,1)]
[Worker vectorscale_build_cluster_1] Writing to page 21 offset 1: neighbors = [(20,1), (22,1)]
[Worker vectorscale_build_cluster_1] Writing to page 22 offset 1: neighbors = [(20,1), (21,1)]
```

**特征**：
- 不同 cluster 的 worker 写入完全不同的页面范围
- 没有交叉的邻居关系
- 每个 cluster 构造独立的子图

## 总结

### flush_neighbor_cache 写入流程

1. **弹出缓存条目**: 从 LRU 缓存中弹出最旧的条目
2. **合并数据**: 与磁盘数据合并，避免丢失更新
3. **剪枝**: 如果邻居数量超过限制，进行剪枝
4. **获取锁**: 通过 `WritablePage::modify` 获取页面的排他锁
5. **修改数据**: 修改页面中的节点数据
6. **提交**: 通过 `WritablePage::commit` 标记脏页并写入 WAL

### 并发控制机制

1. **页面级锁**: 同一时间只有一个进程可以修改一个页面
2. **WAL**: 所有修改都会先写入 WAL，确保崩溃恢复
3. **Reconcile 机制**: 合并缓存和磁盘数据，避免丢失更新

### 日志方案

在 `storage.set_neighbors_on_disk` 中添加日志，记录：
- Worker 名称（包含 cluster ID）
- 写入的页面和偏移
- 邻居列表

通过分析日志可以验证：
- 同一个 cluster 的多个 worker 是否正确地构造同一个图
- 邻居关系是否对称
- 页面访问模式是否合理

### 下一步

1. 实现日志功能
2. 运行测试并收集日志
3. 分析日志验证正确性
4. 如果发现问题，根据日志定位问题原因
