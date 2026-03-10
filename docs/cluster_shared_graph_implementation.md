# Cluster 下多个 Worker 共享 Graph 实现方案

## 问题概述

当前在 cluster 并行构建中，每个 worker 都创建自己的 `Graph` 对象和 `BuilderNeighborCache`，导致：

1. **缓存不一致**：Worker A 的缓存更新对 Worker B 不可见
2. **基于过时数据的决策**：Worker B 可能基于过时的邻居信息做出搜索决策
3. **最终一致性不足**：虽然 reconcile 机制可以合并数据，但搜索过程已经基于错误信息

## 需要修改的核心函数和内容

### 1. 共享内存结构定义

#### 1.1 新增共享 Graph 结构

**文件**: `pgvectorscale/src/access_method/graph/mod.rs`

```rust
// 新增：支持跨进程共享的 Graph 结构
#[repr(C)]
pub struct SharedGraph {
    // Graph 元数据
    meta_page_offset: usize,  // MetaPage 在共享内存中的偏移量
    neighbor_store_offset: usize,  // NeighborStore 在共享内存中的偏移量
    
    // 并发控制
    mutex: pg_sys::slock_t,  // 保护共享 Graph 的互斥锁
    
    // 统计信息
    total_inserts: AtomicUsize,
    total_neighbor_updates: AtomicUsize,
}

impl SharedGraph {
    /// 计算共享 Graph 所需的内存大小
    pub fn calculate_size(meta_page: &MetaPage, num_clusters: usize) -> usize {
        let meta_page_size = std::mem::size_of::<MetaPage>();
        let neighbor_store_size = SharedBuilderNeighborCache::calculate_size(meta_page, num_clusters);
        
        std::mem::size_of::<SharedGraph>()
            + meta_page_size
            + neighbor_store_size
    }
    
    /// 在共享内存中初始化 SharedGraph
    pub unsafe fn initialize(&self, base_ptr: *mut u8, meta_page: &MetaPage, num_clusters: usize) {
        // 初始化互斥锁
        pg_sys::SpinLockInit(&raw mut self.mutex as *mut _);
        
        // 计算 MetaPage 偏移量
        let meta_page_offset = std::mem::size_of::<SharedGraph>();
        self.meta_page_offset = meta_page_offset;
        
        // 初始化 MetaPage
        let meta_page_ptr = base_ptr.add(meta_page_offset) as *mut MetaPage;
        std::ptr::write(meta_page_ptr, meta_page.clone());
        
        // 计算 NeighborStore 偏移量
        let neighbor_store_offset = meta_page_offset + std::mem::size_of::<MetaPage>();
        self.neighbor_store_offset = neighbor_store_offset;
        
        // 初始化 NeighborStore
        let neighbor_store_ptr = base_ptr.add(neighbor_store_offset) as *mut SharedBuilderNeighborCache;
        SharedBuilderNeighborCache::initialize(neighbor_store_ptr, meta_page, num_clusters);
    }
    
    /// 获取共享的 MetaPage
    pub unsafe fn get_meta_page(&self, base_ptr: *mut u8) -> &mut MetaPage {
        &mut *(base_ptr.add(self.meta_page_offset) as *mut MetaPage)
    }
    
    /// 获取共享的 NeighborStore
    pub unsafe fn get_neighbor_store(&self, base_ptr: *mut u8) -> &mut SharedBuilderNeighborCache {
        &mut *(base_ptr.add(self.neighbor_store_offset) as *mut SharedBuilderNeighborCache)
    }
    
    /// 加锁访问共享 Graph
    pub unsafe fn lock(&self) {
        pg_sys::SpinLockAcquire(&raw const self.mutex as *mut _);
    }
    
    /// 解锁
    pub unsafe fn unlock(&self) {
        pg_sys::SpinLockRelease(&raw const self.mutex as *mut _);
    }
}
```

#### 1.2 新增共享 BuilderNeighborCache 结构

**文件**: `pgvectorscale/src/access_method/graph/neighbor_store.rs`

```rust
// 新增：支持跨进程共享的 BuilderNeighborCache
#[repr(C)]
pub struct SharedBuilderNeighborCache {
    // LRU 缓存头部
    cache_capacity: usize,
    cache_size: AtomicUsize,
    
    // 哈希表（使用开放寻址）
    hash_table_offset: usize,
    hash_table_capacity: usize,
    
    // LRU 链表
    lru_head_offset: usize,
    lru_tail_offset: usize,
    
    // 缓存条目数组
    entries_offset: usize,
    entries_capacity: usize,
    
    // 统计信息
    hits: AtomicUsize,
    misses: AtomicUsize,
    evictions: AtomicUsize,
}

#[repr(C)]
pub struct SharedCacheEntry {
    key: ItemPointer,
    neighbors_offset: usize,  // 指向邻居数组的偏移量
    neighbors_count: usize,
    labels_offset: Option<usize>,
    lru_prev: usize,  // LRU 链表前驱索引
    lru_next: usize,  // LRU 链表后继索引
    hash_next: usize,  // 哈希表链表下一个索引
}

impl SharedBuilderNeighborCache {
    /// 计算共享缓存所需的内存大小
    pub fn calculate_size(meta_page: &MetaPage, num_workers: usize) -> usize {
        let total_memory = maintenance_work_mem_bytes() as f64;
        let memory_budget = (total_memory * 0.8).ceil() as usize;
        let entry_size = NeighborCacheEntry::size(meta_page.get_num_neighbors() as _, meta_page.has_labels());
        let cache_capacity = if num_workers > 0 {
            (memory_budget / entry_size) / num_workers
        } else {
            memory_budget / entry_size
        };
        
        let hash_table_capacity = cache_capacity * 2;  // 负载因子 0.5
        
        std::mem::size_of::<SharedBuilderNeighborCache>()
            + hash_table_capacity * std::mem::size_of::<usize>()  // hash table
            + cache_capacity * std::mem::size_of::<SharedCacheEntry>()  // entries
            + cache_capacity * entry_size  // neighbor data
    }
    
    /// 在共享内存中初始化缓存
    pub unsafe fn initialize(&self, base_ptr: *mut u8, meta_page: &MetaPage, num_workers: usize) {
        let total_memory = maintenance_work_mem_bytes() as f64;
        let memory_budget = (total_memory * 0.8).ceil() as usize;
        let entry_size = NeighborCacheEntry::size(meta_page.get_num_neighbors() as _, meta_page.has_labels());
        let cache_capacity = if num_workers > 0 {
            (memory_budget / entry_size) / num_workers
        } else {
            memory_budget / entry_size
        };
        
        self.cache_capacity = cache_capacity;
        self.cache_size.store(0, Ordering::Relaxed);
        self.hash_table_capacity = cache_capacity * 2;
        
        // 计算各部分的偏移量
        let hash_table_offset = std::mem::size_of::<SharedBuilderNeighborCache>();
        self.hash_table_offset = hash_table_offset;
        
        let entries_offset = hash_table_offset + self.hash_table_capacity * std::mem::size_of::<usize>();
        self.entries_offset = entries_offset;
        self.entries_capacity = cache_capacity;
        
        let neighbors_offset = entries_offset + cache_capacity * std::mem::size_of::<SharedCacheEntry>();
        self.lru_head_offset = neighbors_offset;
        self.lru_tail_offset = neighbors_offset;
        
        // 初始化哈希表
        let hash_table = base_ptr.add(hash_table_offset) as *mut usize;
        for i in 0..self.hash_table_capacity {
            *hash_table.add(i) = usize::MAX;  // 空标记
        }
        
        // 初始化缓存条目
        let entries = base_ptr.add(entries_offset) as *mut SharedCacheEntry;
        for i in 0..cache_capacity {
            (*entries.add(i)).lru_prev = if i > 0 { i - 1 } else { usize::MAX };
            (*entries.add(i)).lru_next = if i < cache_capacity - 1 { i + 1 } else { usize::MAX };
        }
        
        // 初始化统计信息
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
    }
    
    /// 从共享缓存获取邻居（需要加锁）
    pub unsafe fn get_neighbors<S: Storage>(
        &self,
        base_ptr: *mut u8,
        neighbors_of: ItemPointer,
        storage: &S,
        stats: &mut PruneNeighborStats,
    ) -> Vec<NeighborWithDistance> {
        let hash = self.hash_key(neighbors_of);
        let hash_table = base_ptr.add(self.hash_table_offset) as *const usize;
        let entries = base_ptr.add(self.entries_offset) as *mut SharedCacheEntry;
        
        // 在哈希表中查找
        let mut idx = *hash_table.add(hash % self.hash_table_capacity);
        while idx != usize::MAX {
            let entry = &*entries.add(idx);
            if entry.key == neighbors_of {
                // 命中，更新 LRU
                self.lru_update(base_ptr, idx);
                self.hits.fetch_add(1, Ordering::Relaxed);
                
                // 返回缓存的邻居
                let neighbors_data = base_ptr.add(entry.neighbors_offset);
                return self.deserialize_neighbors(neighbors_data, entry.neighbors_count);
            }
            idx = entry.hash_next;
        }
        
        // 未命中，从磁盘读取
        self.misses.fetch_add(1, Ordering::Relaxed);
        storage.get_neighbors_with_distances_from_disk(neighbors_of, stats)
    }
    
    /// 设置邻居到共享缓存（需要加锁）
    pub unsafe fn set_neighbors<S: Storage>(
        &self,
        base_ptr: *mut u8,
        neighbors_of: ItemPointer,
        labels: Option<LabelSet>,
        new_neighbors: Vec<NeighborWithDistance>,
        storage: &S,
        stats: &mut PruneNeighborStats,
    ) {
        // 如果缓存已满，驱逐最旧的条目
        if self.cache_size.load(Ordering::Relaxed) >= self.cache_capacity {
            self.evict_lru(base_ptr, storage, stats);
        }
        
        // 插入新条目
        self.insert_entry(base_ptr, neighbors_of, labels, new_neighbors);
    }
    
    /// LRU 更新
    unsafe fn lru_update(&self, base_ptr: *mut u8, idx: usize) {
        let entries = base_ptr.add(self.entries_offset) as *mut SharedCacheEntry;
        let entry = &mut *entries.add(idx);
        
        // 如果已经在头部，不需要移动
        if entry.lru_prev == usize::MAX {
            return;
        }
        
        // 从当前位置移除
        if entry.lru_prev != usize::MAX {
            (*entries.add(entry.lru_prev)).lru_next = entry.lru_next;
        }
        if entry.lru_next != usize::MAX {
            (*entries.add(entry.lru_next)).lru_prev = entry.lru_prev;
        }
        
        // 移动到头部
        entry.lru_prev = usize::MAX;
        entry.lru_next = self.lru_head_offset;
        if self.lru_head_offset != usize::MAX {
            (*entries.add(self.lru_head_offset)).lru_prev = idx;
        }
        self.lru_head_offset = idx;
        
        if self.lru_tail_offset == usize::MAX {
            self.lru_tail_offset = idx;
        }
    }
    
    /// 驱逐 LRU 条目
    unsafe fn evict_lru<S: Storage>(
        &self,
        base_ptr: *mut u8,
        storage: &S,
        stats: &mut PruneNeighborStats,
    ) {
        if self.lru_tail_offset == usize::MAX {
            return;
        }
        
        let entries = base_ptr.add(self.entries_offset) as *mut SharedCacheEntry;
        let tail_idx = self.lru_tail_offset;
        let entry = &*entries.add(tail_idx);
        
        // 从磁盘读取最新数据并合并
        let disk_neighbors = storage.get_neighbors_with_distances_from_disk(entry.key, stats);
        let cached_neighbors = self.deserialize_neighbors(
            base_ptr.add(entry.neighbors_offset),
            entry.neighbors_count
        );
        
        // 合并邻居
        let merged_neighbors = self.merge_neighbors(disk_neighbors, cached_neighbors);
        
        // 写回磁盘
        storage.set_neighbors_on_disk(entry.key, &merged_neighbors, stats);
        
        // 从哈希表和 LRU 链表中移除
        self.remove_entry(base_ptr, tail_idx);
        
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }
    
    /// 插入新条目
    unsafe fn insert_entry(
        &self,
        base_ptr: *mut u8,
        key: ItemPointer,
        labels: Option<LabelSet>,
        neighbors: Vec<NeighborWithDistance>,
    ) {
        // 找到空闲槽位
        let entries = base_ptr.add(self.entries_offset) as *mut SharedCacheEntry;
        let mut free_idx = usize::MAX;
        for i in 0..self.entries_capacity {
            let entry = &*entries.add(i);
            if entry.key.block_number == pg_sys::InvalidBlockNumber {
                free_idx = i;
                break;
            }
        }
        
        if free_idx == usize::MAX {
            return;  // 缓存已满
        }
        
        // 计算邻居数据的偏移量
        let neighbors_offset = self.entries_offset
            + self.entries_capacity * std::mem::size_of::<SharedCacheEntry>()
            + free_idx * 256;  // 假设每个邻居数据最多 256 字节
        
        // 序列化邻居
        let neighbors_data = base_ptr.add(neighbors_offset);
        self.serialize_neighbors(neighbors_data, &neighbors);
        
        // 插入到哈希表
        let hash = self.hash_key(key);
        let hash_table = base_ptr.add(self.hash_table_offset) as *mut usize;
        let hash_idx = hash % self.hash_table_capacity;
        let entry = &mut *entries.add(free_idx);
        
        entry.key = key;
        entry.neighbors_offset = neighbors_offset;
        entry.neighbors_count = neighbors.len();
        entry.labels_offset = None;  // TODO: 处理 labels
        entry.hash_next = *hash_table.add(hash_idx);
        *hash_table.add(hash_idx) = free_idx;
        
        // 插入到 LRU 头部
        self.lru_insert_head(base_ptr, free_idx);
        
        self.cache_size.fetch_add(1, Ordering::Relaxed);
    }
    
    /// 从哈希表和 LRU 链表中移除条目
    unsafe fn remove_entry(&self, base_ptr: *mut u8, idx: usize) {
        let entries = base_ptr.add(self.entries_offset) as *mut SharedCacheEntry;
        let entry = &*entries.add(idx);
        
        // 从哈希表中移除
        let hash = self.hash_key(entry.key);
        let hash_table = base_ptr.add(self.hash_table_offset) as *mut usize;
        let hash_idx = hash % self.hash_table_capacity;
        
        let mut prev_idx = usize::MAX;
        let mut curr_idx = *hash_table.add(hash_idx);
        while curr_idx != usize::MAX {
            if curr_idx == idx {
                if prev_idx == usize::MAX {
                    *hash_table.add(hash_idx) = (*entries.add(curr_idx)).hash_next;
                } else {
                    (*entries.add(prev_idx)).hash_next = (*entries.add(curr_idx)).hash_next;
                }
                break;
            }
            prev_idx = curr_idx;
            curr_idx = (*entries.add(curr_idx)).hash_next;
        }
        
        // 从 LRU 链表中移除
        if entry.lru_prev != usize::MAX {
            (*entries.add(entry.lru_prev)).lru_next = entry.lru_next;
        }
        if entry.lru_next != usize::MAX {
            (*entries.add(entry.lru_next)).lru_prev = entry.lru_prev;
        }
        
        if self.lru_head_offset == idx {
            self.lru_head_offset = entry.lru_next;
        }
        if self.lru_tail_offset == idx {
            self.lru_tail_offset = entry.lru_prev;
        }
        
        // 标记为空闲
        let entry_mut = &mut *entries.add(idx);
        entry_mut.key = ItemPointer::new(pg_sys::InvalidBlockNumber, pg_sys::InvalidOffsetNumber);
        
        self.cache_size.fetch_sub(1, Ordering::Relaxed);
    }
    
    fn hash_key(&self, key: ItemPointer) -> usize {
        let mut hash = 5381usize;
        hash = hash.wrapping_mul(33).wrapping_add(key.block_number as usize);
        hash = hash.wrapping_mul(33).wrapping_add(key.offset as usize);
        hash
    }
    
    unsafe fn serialize_neighbors(&self, ptr: *mut u8, neighbors: &[NeighborWithDistance]) {
        // TODO: 实现序列化
    }
    
    unsafe fn deserialize_neighbors(&self, ptr: *const u8, count: usize) -> Vec<NeighborWithDistance> {
        // TODO: 实现反序列化
        Vec::new()
    }
    
    fn merge_neighbors(&self, disk: Vec<NeighborWithDistance>, cached: Vec<NeighborWithDistance>) -> Vec<NeighborWithDistance> {
        let mut neighbor_map: std::collections::HashMap<ItemPointer, NeighborWithDistance> = std::collections::HashMap::new();
        
        for neighbor in disk {
            neighbor_map.insert(neighbor.get_index_pointer_to_neighbor(), neighbor);
        }
        
        for neighbor in cached {
            neighbor_map.insert(neighbor.get_index_pointer_to_neighbor(), neighbor);
        }
        
        neighbor_map.into_values().collect()
    }
}
```

### 2. 修改并行构建流程

#### 2.1 修改 `do_parallel_cluster_build` 函数

**文件**: `pgvectorscale/src/access_method/build/parallel_build/cluster.rs`

```rust
fn do_parallel_cluster_build(
    heaprel: pg_sys::Relation,
    indexrel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    heap_relation: &PgRelation,
    index_relation: &PgRelation,
    meta_page: &mut MetaPage,
    workers: usize,
    is_concurrent: bool,
    centroids: &[Vec<f32>],
    _write_stats: WriteStats,
    num_dimensions: usize,
) -> usize {
    // ... 现有代码 ...
    
    // ===== 新增：为每个 cluster 分配共享 Graph =====
    let num_clusters = centroids.len();
    let workers_per_cluster = calculate_workers_per_cluster(&worker_distribution, num_clusters);
    
    // 计算共享 Graph 的总大小
    let shared_graphs_size: usize = (0..num_clusters)
        .map(|cluster_id| {
            let cluster_workers = workers_per_cluster[cluster_id];
            SharedGraph::calculate_size(meta_page, cluster_workers)
        })
        .sum();
    
    // 在共享内存中分配
    parallel::toc_estimate_single_chunk(pcxt, shared_graphs_size);
    // ... 现有代码 ...
    
    // 初始化共享 Graph
    let shared_graphs_ptr = pg_sys::shm_toc_allocate((*pcxt).toc, shared_graphs_size).cast::<u8>();
    let mut shared_graphs_offsets: Vec<usize> = Vec::with_capacity(num_clusters);
    let mut current_offset = 0;
    
    for cluster_id in 0..num_clusters {
        let cluster_workers = workers_per_cluster[cluster_id];
        let graph_size = SharedGraph::calculate_size(meta_page, cluster_workers);
        
        let shared_graph = shared_graphs_ptr.add(current_offset) as *mut SharedGraph;
        std::ptr::write(shared_graph, SharedGraph {
            meta_page_offset: 0,
            neighbor_store_offset: 0,
            mutex: std::mem::zeroed(),
            total_inserts: AtomicUsize::new(0),
            total_neighbor_updates: AtomicUsize::new(0),
        });
        
        // 初始化共享 Graph
        (*shared_graph).initialize(
            shared_graphs_ptr.add(current_offset),
            meta_page,
            cluster_workers,
        );
        
        shared_graphs_offsets.push(current_offset);
        current_offset += graph_size;
    }
    
    // 将共享 Graph 指针插入到共享内存表
    pg_sys::shm_toc_insert(
        (*pcxt).toc,
        SHM_TOC_SHARED_GRAPHS_KEY,
        shared_graphs_ptr.cast(),
    );
    // ===== 新增结束 =====
    
    // ... 现有代码 ...
}
```

#### 2.2 修改 `_vectorscale_build_cluster_consumer_main` 函数

**文件**: `pgvectorscale/src/access_method/build/parallel_build/cluster.rs`

```rust
#[unsafe(no_mangle)]
#[cfg(feature = "build_parallel")]
pub extern "C" fn _vectorscale_build_cluster_consumer_main(
    _seg: *mut pg_sys::dsm_segment,
    shm_toc: *mut pg_sys::shm_toc,
) {
    // ... 现有代码 ...
    
    // ===== 新增：获取共享 Graph =====
    let shared_graphs_base: *mut u8 = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_SHARED_GRAPHS_KEY, true).cast::<u8>()
    };
    
    let shared_graphs_offsets: *const usize = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_SHARED_GRAPHS_OFFSETS_KEY, true).cast::<usize>()
    };
    
    // 获取当前 cluster 的共享 Graph
    let shared_graph_ptr = unsafe {
        shared_graphs_base.add(*shared_graphs_offsets.add(cluster_id)) as *mut SharedGraph
    };
    // ===== 新增结束 =====
    
    // ... 现有代码 ...
    
    // ===== 修改：使用共享 Graph 而不是创建新的 Graph =====
    let mut consumer_state = ConsumerState {
        cluster_id,
        start_idx,
        end_idx,
        is_primary,
        cluster_queues,
        _parallel_shared: parallel_shared,
        _num_dimensions: params.num_dimensions,
        ntuples: 0,
        first_node: None,
        worker_number,
        workers_per_cluster,
        shared_graph: shared_graph_ptr,  // 新增字段
    };
    
    build_cluster_subgraph(
        &mut consumer_state,
        &heap_relation,
        &index_relation,
        &mut meta_page,
        &centroids,
        cluster_start_nodes,
    );
    // ===== 修改结束 =====
}
```

#### 2.3 修改 `ConsumerState` 结构

**文件**: `pgvectorscale/src/access_method/build/parallel_build/cluster.rs`

```rust
struct ConsumerState {
    cluster_id: usize,
    start_idx: usize,
    end_idx: usize,
    is_primary: bool,
    cluster_queues: *mut ClusterQueues,
    _parallel_shared: *mut ParallelShared,
    _num_dimensions: usize,
    ntuples: usize,
    first_node: Option<crate::util::ItemPointer>,
    worker_number: usize,
    workers_per_cluster: usize,
    shared_graph: *mut SharedGraph,  // 新增：指向共享 Graph
}
```

#### 2.4 修改 `build_cluster_subgraph` 函数

**文件**: `pgvectorscale/src/access_method/build/parallel_build/cluster.rs`

```rust
unsafe fn build_cluster_subgraph(
    consumer_state: &mut ConsumerState,
    heap_relation: &PgRelation,
    index_relation: &PgRelation,
    meta_page: &mut MetaPage,
    _centroids: &[Vec<f32>],
    cluster_start_nodes: *mut ClusterStartNodes,
) {
    // ===== 修改：使用共享 Graph =====
    let shared_graph = consumer_state.shared_graph;
    let base_ptr = shared_graph as *mut u8;
    
    // 获取共享的 MetaPage 和 NeighborStore
    let shared_meta_page = (*shared_graph).get_meta_page(base_ptr);
    let shared_neighbor_store = (*shared_graph).get_neighbor_store(base_ptr);
    
    // 创建 Graph 包装器（不拥有数据）
    let mut graph = Graph::new(
        GraphNeighborStore::Shared(shared_neighbor_store),  // 新增：Shared 变体
        shared_meta_page,
    );
    // ===== 修改结束 =====
    
    let mut tape = unsafe { Tape::new(index_relation, PageType::Node) };
    let mut write_stats = WriteStats::default();
    
    // ... 现有代码 ...
}
```

### 3. 修改 GraphNeighborStore 枚举

**文件**: `pgvectorscale/src/access_method/graph/neighbor_store.rs`

```rust
pub enum GraphNeighborStore {
    Builder(BuilderNeighborCache),  // 现有：本地缓存
    Disk,  // 现有：直接访问磁盘
    Shared(&'static mut SharedBuilderNeighborCache),  // 新增：共享缓存
}

impl GraphNeighborStore {
    pub fn get_neighbors_with_full_vector_distances<S: Storage>(
        &self,
        neighbors_of: ItemPointer,
        storage: &S,
        stats: &mut PruneNeighborStats,
    ) -> Vec<NeighborWithDistance> {
        match self {
            GraphNeighborStore::Builder(b) => {
                b.get_neighbors_with_full_vector_distances(neighbors_of, storage, stats)
            }
            GraphNeighborStore::Disk => {
                storage.get_neighbors_with_distances_from_disk(neighbors_of, stats)
            }
            GraphNeighborStore::Shared(shared_cache) => {
                // 新增：从共享缓存获取
                unsafe {
                    let base_ptr = shared_cache as *const _ as *mut u8;
                    shared_cache.get_neighbors(base_ptr, neighbors_of, storage, stats)
                }
            }
        }
    }
    
    pub fn set_neighbors<S: Storage>(
        &self,
        storage: &S,
        neighbors_of: ItemPointer,
        labels: Option<LabelSet>,
        new_neighbors: Vec<NeighborWithDistance>,
        stats: &mut PruneNeighborStats,
    ) {
        match self {
            GraphNeighborStore::Builder(b) => {
                b.set_neighbors(neighbors_of, labels, new_neighbors, storage, stats)
            }
            GraphNeighborStore::Disk => {
                storage.set_neighbors_on_disk(neighbors_of, new_neighbors.as_slice(), stats)
            }
            GraphNeighborStore::Shared(shared_cache) => {
                // 新增：设置到共享缓存
                unsafe {
                    let base_ptr = shared_cache as *const _ as *mut u8;
                    shared_cache.set_neighbors(base_ptr, neighbors_of, labels, new_neighbors, storage, stats);
                }
            }
        }
    }
}
```

### 4. 新增共享内存键

**文件**: `pgvectorscale/src/access_method/build/parallel.rs`

```rust
// 新增：共享 Graph 的键
pub const SHM_TOC_SHARED_GRAPHS_KEY: usize = 100;
pub const SHM_TOC_SHARED_GRAPHS_OFFSETS_KEY: usize = 101;
```

### 5. 辅助函数

#### 5.1 计算每个 cluster 的 worker 数量

**文件**: `pgvectorscale/src/access_method/build/parallel_build/cluster.rs`

```rust
fn calculate_workers_per_cluster(
    worker_distribution: &[Vec<usize>],
    num_clusters: usize,
) -> Vec<usize> {
    (0..num_clusters)
        .map(|cluster_id| worker_distribution[cluster_id].len())
        .collect()
}
```

## 关键修改点总结

### 1. 数据结构修改

| 文件 | 修改内容 | 说明 |
|------|---------|------|
| `graph/mod.rs` | 新增 `SharedGraph` 结构 | 支持跨进程共享的 Graph |
| `graph/neighbor_store.rs` | 新增 `SharedBuilderNeighborCache` 结构 | 支持跨进程共享的缓存 |
| `graph/neighbor_store.rs` | 修改 `GraphNeighborStore` 枚举 | 添加 `Shared` 变体 |
| `parallel.rs` | 新增共享内存键 | 用于在 DSM 中分配共享 Graph |

### 2. 函数修改

| 函数 | 文件 | 修改内容 |
|------|------|---------|
| `do_parallel_cluster_build` | `cluster.rs` | 在共享内存中分配和初始化共享 Graph |
| `_vectorscale_build_cluster_consumer_main` | `cluster.rs` | 获取并使用共享 Graph |
| `build_cluster_subgraph` | `cluster.rs` | 使用共享 Graph 而不是创建新的 |
| `process_cluster_vectors` | `cluster.rs` | 适配共享 Graph 的访问模式 |
| `ConsumerState` | `cluster.rs` | 添加 `shared_graph` 字段 |

### 3. 并发控制机制

1. **SpinLock 保护共享 Graph**
   - 使用 PostgreSQL 的 `SpinLock` 保护共享 Graph 的访问
   - 确保多个 worker 安全地访问共享数据

2. **原子操作**
   - 使用 `AtomicUsize` 保护统计信息
   - 使用 `AtomicBool` 保护标志位

3. **LRU 缓存同步**
   - 所有 worker 共享同一个 LRU 缓存
   - 驱逐时自动与磁盘数据合并

## 实现注意事项

### 1. 内存对齐

- 所有共享内存结构必须使用 `#[repr(C)]` 确保内存布局一致
- 注意 64 位系统上的指针对齐

### 2. 生命周期管理

- 共享 Graph 的生命周期由 DSM 段管理
- 确保在 DSM 释放前清理所有资源

### 3. 锁的粒度

- SpinLock 保护整个 Graph 可能成为性能瓶颈
- 考虑使用更细粒度的锁（如每个缓存条目一个锁）

### 4. 错误处理

- 共享内存分配失败时的降级策略
- 锁竞争超时处理

### 5. 测试策略

1. **单元测试**
   - 测试 `SharedBuilderNeighborCache` 的基本操作
   - 测试并发访问的正确性

2. **集成测试**
   - 测试多个 worker 构建同一个 cluster
   - 测试缓存一致性

3. **性能测试**
   - 对比共享 Graph 和独立 Graph 的性能
   - 测试锁竞争对性能的影响

## 预期效果

1. **正确性**
   - 所有 worker 看到一致的邻居信息
   - 避免基于过时数据的搜索决策

2. **性能**
   - 减少磁盘 I/O（共享缓存）
   - 提高缓存命中率

3. **可扩展性**
   - 支持更多 worker 并行构建同一个 cluster
   - 更好的负载均衡

## 风险和挑战

1. **锁竞争**
   - 多个 worker 访问同一个 Graph 可能导致锁竞争
   - 需要优化锁的粒度和访问模式

2. **内存使用**
   - 共享 Graph 会增加共享内存的使用量
   - 需要合理设置缓存大小

3. **复杂性**
   - 实现复杂度较高
   - 需要仔细处理并发和同步

4. **调试难度**
   - 并发问题难以复现和调试
   - 需要完善的日志和监控
