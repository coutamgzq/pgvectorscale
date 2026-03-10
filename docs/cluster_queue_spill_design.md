# Cluster 队列溢出处理方案设计文档

## 1. 问题背景

### 1.1 当前架构

在 pgvectorscale 的并行索引构建中，Producer 进程扫描 heap 表，将向量数据根据 cluster ID 分发到不同的队列中：

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Producer 进程                                 │
│                                                                      │
│   pg_sys::IndexBuildHeapScan()                                       │
│           │                                                          │
│           ▼                                                          │
│   producer_callback()                                                │
│           │                                                          │
│           ▼                                                          │
│   push_with_batch(cluster_id, heap_tid, vector)                      │
│           │                                                          │
│           ▼                                                          │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│   │ Cluster 0    │  │ Cluster 1    │  │ Cluster N    │              │
│   │ Queue        │  │ Queue        │  │ Queue        │              │
│   │ [████████░░] │  │ [████░░░░░░] │  │ [██████████] │              │
│   │  80% full    │  │  40% full    │  │  100% full   │              │
│   └──────────────┘  └──────────────┘  └──────────────┘              │
│           │                 │                 │                      │
│           ▼                 ▼                 ▼                      │
│      Consumer 0        Consumer 1        Consumer N                  │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.2 队头阻塞问题（Head-of-Line Blocking）

**问题描述**：
- 当某个 Cluster 的队列满了（如 Cluster N），Producer 会阻塞等待（`ConditionVariableSleep`）
- 此时其他 Cluster 的队列可能还有空间（如 Cluster 1 只有 40% 满），但 Producer 无法继续扫描
- 这导致整体吞吐量下降，CPU 资源浪费

**代码位置**：`parallel.rs:L257-302`

```rust
pub unsafe fn push_to_queue(...) -> bool {
    loop {
        let tail = (*header).tail.load(Ordering::Acquire);
        let head = (*header).head.load(Ordering::Acquire);
        let next_tail = (tail + 1) % (*header).capacity;

        if next_tail != head {
            // 队列有空间，写入数据
            ...
            return true;
        }

        // 队列满了，阻塞等待
        pg_sys::ConditionVariableSleep(cv, pg_sys::PG_WAIT_EXTENSION);
    }
}
```

### 1.3 为什么不能简单跳过？

**关键约束**：heap 表只扫描一次
- 如果跳过已满队列的数据，这些数据将丢失
- 必须在扫描期间将所有数据保存到某个地方

## 2. 解决方案：临时文件溢出（Spill to Disk）

### 2.1 核心思想

当某个 Cluster 队列满了时，不阻塞等待，而是：
1. **继续扫描**：Producer 继续扫描 heap 表
2. **溢出到磁盘**：将属于已满队列的数据写入临时文件
3. **动态加载**：当队列有空间时，从临时文件读取数据补充

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Producer 进程（优化后）                       │
│                                                                      │
│   IndexBuildHeapScan()                                               │
│           │                                                          │
│           ▼                                                          │
│   对于每个向量：                                                      │
│           │                                                          │
│           ▼                                                          │
│   计算 cluster_id                                                    │
│           │                                                          │
│     ┌─────┴─────┐                                                    │
│     ▼           ▼                                                    │
│  队列有空间   队列满了                                                 │
│     │           │                                                    │
│     ▼           ▼                                                    │
│  直接入队    写入临时文件                                              │
│              /tmp/pg_vectorscale_spill/                              │
│              ├── cluster_0.spill                                     │
│              ├── cluster_1.spill                                     │
│              └── cluster_N.spill                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 关键设计决策

#### 2.2.1 何时停止继续扫描？

**问题**：如果一直扫描，临时文件可能无限增长（极端情况：存储翻倍）

**解决方案**：**分阶段扫描 + 溢出控制**

**核心策略**：
1. **分阶段扫描**：每扫描 10% 的数据，检查溢出文件大小
2. **溢出上限**：确保溢出文件总大小不超过原始表大小的 10%
3. **等待消费**：如果溢出文件超过阈值，暂停扫描，等待 Consumer 消化

**详细流程**：
```
扫描进度: 0% ──────────────────────────────────────────────> 100%
              │         │         │         │         │
             10%       20%       30%       40%       50%
              │         │         │         │         │
              ▼         ▼         ▼         ▼         ▼
         检查溢出文件大小
              │
         ┌────┴────┐
    <=10%表大小   >10%表大小
         │           │
         ▼           ▼
    继续扫描      暂停扫描
                  等待消费
                     │
                     ▼
               溢出文件 <= 5%
                     │
                     ▼
                继续扫描
```

**优势**：
- **渐进式控制**：避免在 90% 时才发现问题，导致长时间等待
- **资源平衡**：Producer 和 Consumer 工作量更均衡
- **可预测性**：溢出文件大小可控，不会超过表大小的 10%

#### 2.2.2 临时文件格式

**选择**：二进制顺序文件，每条记录固定格式

```rust
/// 临时文件记录格式
#[repr(C)]
struct SpillRecord {
    /// Heap TID (6 bytes)
    heap_tid: pg_sys::ItemPointerData,
    /// 向量维度
    num_dimensions: u32,
    /// 向量数据 (动态长度，紧跟在结构体后)
    // vector_data: [f32; num_dimensions]
}
```

**优点**：
- 顺序读写，性能高
- 无需解析，直接内存映射
- 支持快速追加

#### 2.2.3 文件管理策略

```
临时文件目录结构：
/tmp/pg_vectorscale_spill/
├── index_<oid>/
│   ├── cluster_0.spill      # Cluster 0 的溢出数据
│   ├── cluster_1.spill      # Cluster 1 的溢出数据
│   ├── ...
│   └── cluster_N.spill      # Cluster N 的溢出数据
```

**生命周期**：
1. **创建**：Producer 首次需要溢出时创建
2. **写入**：Producer 持续追加
3. **读取**：Consumer 在队列空闲时读取补充
4. **删除**：构建完成后删除

## 3. 详细实现方案

### 3.1 数据结构

#### 3.1.1 SpillFile - 临时文件管理

```rust
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write, Seek, SeekFrom};
use std::path::PathBuf;

/// 单个 Cluster 的溢出文件管理
pub struct SpillFile {
    /// 文件路径
    path: PathBuf,
    /// 写入句柄（Producer 使用）
    writer: Option<BufWriter<File>>,
    /// 读取句柄（Consumer 使用）
    reader: Option<BufReader<File>>,
    /// 已写入记录数
    records_written: u64,
    /// 已读取记录数
    records_read: u64,
    /// 文件大小（字节）
    file_size: u64,
}

impl SpillFile {
    /// 创建新的溢出文件
    pub fn new(cluster_id: usize, index_oid: u32) -> Result<Self, std::io::Error> {
        let path = format!("/tmp/pg_vectorscale_spill/index_{}/cluster_{}.spill", 
                          index_oid, cluster_id);
        
        // 确保目录存在
        std::fs::create_dir_all(PathBuf::from(&path).parent().unwrap())?;
        
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&path)?;
        
        Ok(Self {
            path: path.into(),
            writer: Some(BufWriter::new(file.try_clone()?)),
            reader: Some(BufReader::new(file)),
            records_written: 0,
            records_read: 0,
            file_size: 0,
        })
    }

    /// 写入一条记录
    pub fn write_record(
        &mut self,
        heap_tid: pg_sys::ItemPointerData,
        vector: &[f32],
    ) -> Result<(), std::io::Error> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "Writer not available")
        })?;

        // 写入 heap_tid
        let tid_bytes = unsafe {
            std::slice::from_raw_parts(
                &heap_tid as *const _ as *const u8,
                std::mem::size_of::<pg_sys::ItemPointerData>(),
            )
        };
        writer.write_all(tid_bytes)?;

        // 写入维度
        let dims = vector.len() as u32;
        writer.write_all(&dims.to_le_bytes())?;

        // 写入向量数据
        let vec_bytes = unsafe {
            std::slice::from_raw_parts(
                vector.as_ptr() as *const u8,
                vector.len() * std::mem::size_of::<f32>(),
            )
        };
        writer.write_all(vec_bytes)?;

        self.records_written += 1;
        self.file_size += (std::mem::size_of::<pg_sys::ItemPointerData>() 
                          + std::mem::size_of::<u32>() 
                          + vector.len() * std::mem::size_of::<f32>()) as u64;

        Ok(())
    }

    /// 读取一条记录
    pub fn read_record(
        &mut self,
        vector_buf: &mut Vec<f32>,
    ) -> Result<Option<pg_sys::ItemPointerData>, std::io::Error> {
        if self.records_read >= self.records_written {
            return Ok(None);
        }

        let reader = self.reader.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "Reader not available")
        })?;

        // 读取 heap_tid
        let mut tid_bytes = [0u8; std::mem::size_of::<pg_sys::ItemPointerData>()];
        if reader.read_exact(&mut tid_bytes).is_err() {
            return Ok(None);
        }
        let heap_tid = unsafe {
            std::ptr::read(tid_bytes.as_ptr() as *const pg_sys::ItemPointerData)
        };

        // 读取维度
        let mut dim_bytes = [0u8; 4];
        reader.read_exact(&mut dim_bytes)?;
        let dims = u32::from_le_bytes(dim_bytes) as usize;

        // 读取向量数据
        vector_buf.resize(dims, 0.0);
        let mut vec_bytes = vec![0u8; dims * std::mem::size_of::<f32>()];
        reader.read_exact(&mut vec_bytes)?;
        
        unsafe {
            std::ptr::copy_nonoverlapping(
                vec_bytes.as_ptr() as *const f32,
                vector_buf.as_mut_ptr(),
                dims,
            );
        }

        self.records_read += 1;
        Ok(Some(heap_tid))
    }

    /// 刷新缓冲区
    pub fn flush(&mut self) -> Result<(), std::io::Error> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    /// 清理文件
    pub fn cleanup(&mut self) -> Result<(), std::io::Error> {
        self.writer = None;
        self.reader = None;
        std::fs::remove_file(&self.path)?;
        Ok(())
    }
}
```

#### 3.1.2 SpillManager - 溢出管理器

```rust
/// 管理所有 Cluster 的溢出文件
pub struct SpillManager {
    /// 每个 Cluster 的溢出文件
    spill_files: Vec<Option<SpillFile>>,
    /// 总溢出记录数
    total_spilled: AtomicUsize,
    /// 总读取记录数
    total_read: AtomicUsize,
    /// 最大允许的溢出比例（相对于原始数据）
    max_spill_ratio: f64,
    /// 停止扫描的阈值（剩余数据比例）
    stop_scan_threshold: f64,
}

impl SpillManager {
    pub fn new(num_clusters: usize, max_spill_ratio: f64, stop_scan_threshold: f64) -> Self {
        Self {
            spill_files: (0..num_clusters).map(|_| None).collect(),
            total_spilled: AtomicUsize::new(0),
            total_read: AtomicUsize::new(0),
            max_spill_ratio,
            stop_scan_threshold,
        }
    }

    /// 检查是否需要溢出到文件
    pub fn should_spill(&self, cluster_id: usize) -> bool {
        // 如果该 cluster 已经有溢出文件，继续溢出
        self.spill_files[cluster_id].is_some()
    }

    /// 溢出一条记录
    pub fn spill_record(
        &mut self,
        cluster_id: usize,
        heap_tid: pg_sys::ItemPointerData,
        vector: &[f32],
        index_oid: u32,
    ) -> Result<(), std::io::Error> {
        // 延迟创建溢出文件
        if self.spill_files[cluster_id].is_none() {
            self.spill_files[cluster_id] = Some(SpillFile::new(cluster_id, index_oid)?);
        }

        let file = self.spill_files[cluster_id].as_mut().unwrap();
        file.write_record(heap_tid, vector)?;
        
        self.total_spilled.fetch_add(1, Ordering::Relaxed);
        
        Ok(())
    }

    /// 从溢出文件读取记录到队列
    pub fn read_to_queue(
        &mut self,
        cluster_id: usize,
        queues: &ClusterQueues,
        base_ptr: *mut u8,
        vector_buf: &mut Vec<f32>,
    ) -> Result<usize, std::io::Error> {
        let Some(file) = self.spill_files[cluster_id].as_mut() else {
            return Ok(0);
        };

        let mut count = 0;
        
        // 尽可能多地读取到队列
        while let Some(heap_tid) = file.read_record(vector_buf)? {
            if queues.push_to_queue(base_ptr, cluster_id, heap_tid, vector_buf) {
                count += 1;
                self.total_read.fetch_add(1, Ordering::Relaxed);
            } else {
                // 队列又满了，停止读取
                break;
            }
        }

        Ok(count)
    }

    /// 检查是否应该暂停扫描（分阶段控制溢出）
    /// 
    /// 每扫描 10% 检查一次，如果溢出文件超过表大小的 10%，则暂停扫描
    pub fn should_pause_scan(&self, scan_progress: f64, table_size: u64) -> bool {
        // 只在检查点触发（每 10%）
        if (scan_progress * 10.0) as usize % 1 != 0 {
            return false;
        }
        
        let spill_size = self.total_spill_size();
        let spill_ratio = spill_size as f64 / table_size as f64;
        
        // 如果溢出超过 10%，暂停扫描
        spill_ratio > self.max_spill_ratio
    }
    
    /// 等待溢出文件被消费到指定比例以下
    pub fn wait_until_spill_ratio(&self, target_ratio: f64, table_size: u64) {
        loop {
            let spill_size = self.total_spill_size();
            let spill_ratio = spill_size as f64 / table_size as f64;
            
            if spill_ratio <= target_ratio {
                break;
            }
            
            // 短暂休眠，避免忙等待
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// 获取总溢出大小
    pub fn total_spill_size(&self) -> u64 {
        self.spill_files
            .iter()
            .filter_map(|f| f.as_ref().map(|f| f.file_size))
            .sum()
    }

    /// 清理所有溢出文件
    pub fn cleanup(&mut self) -> Result<(), std::io::Error> {
        for file in &mut self.spill_files {
            if let Some(f) = file.as_mut() {
                f.cleanup()?;
            }
        }
        Ok(())
    }
}
```

### 3.2 Producer 修改

#### 3.2.1 修改 ProducerState

```rust
struct ProducerState<'a> {
    _parallel_shared: *mut ParallelShared,
    cluster_queues: *mut ClusterQueues,
    cluster_sizes: *mut ClusterSizes,
    centroids: &'a [Vec<f32>],
    ntuples: usize,
    meta_page: MetaPage,
    batch_buffer: BatchBuffer,
    // 新增：溢出管理器
    spill_manager: Option<SpillManager>,
    // 新增：索引 OID
    index_oid: u32,
    // 新增：扫描进度（0.0 - 1.0）
    scan_progress: f64,
    // 新增：总元组数（用于计算进度）
    total_tuples: usize,
}
```

#### 3.2.2 修改 push_with_batch

```rust
impl ProducerState<'_> {
    unsafe fn push_with_batch(
        &mut self,
        cluster_id: usize,
        heap_tid: pg_sys::ItemPointerData,
        vector: &[f32],
    ) {
        // 更新扫描进度
        self.ntuples += 1;
        self.scan_progress = self.ntuples as f64 / self.total_tuples as f64;
        
        // 分阶段检查：每扫描 10% 检查一次溢出文件大小
        if let Some(ref spill_manager) = self.spill_manager {
            let check_interval = 0.1; // 10%
            let last_check_point = ((self.scan_progress - 1.0 / self.total_tuples as f64) / check_interval) as i64;
            let current_check_point = (self.scan_progress / check_interval) as i64;
            
            if current_check_point > last_check_point {
                // 到达新的检查点
                let table_size = estimate_table_size(); // 估算表大小
                
                if spill_manager.should_pause_scan(self.scan_progress, table_size) {
                    // 溢出超过 10%，暂停扫描，等待消费
                    self.flush_batch();
                    spill_manager.wait_until_spill_ratio(0.05, table_size); // 等待至 5%
                }
            }
        }

        // 尝试写入队列
        if self.batch_buffer.cluster_id != cluster_id || self.batch_buffer.is_full() {
            self.flush_batch();
            self.batch_buffer.cluster_id = cluster_id;
        }

        // 检查队列是否已满
        let queues = &*self.cluster_queues;
        let base_ptr = self.cluster_queues as *mut u8;
        
        if queues.available_space(base_ptr, cluster_id) == 0 {
            // 队列满了，尝试溢出到文件
            if let Some(ref mut spill_manager) = self.spill_manager {
                if let Err(e) = spill_manager.spill_record(
                    cluster_id, 
                    heap_tid, 
                    vector,
                    self.index_oid
                ) {
                    warning!("Failed to spill record: {}", e);
                    // 溢出失败，阻塞等待
                    self.flush_batch();
                    queues.wait_for_space(base_ptr, cluster_id);
                    
                    // 重新尝试入队
                    self.batch_buffer.entries.push((heap_tid, vector.to_vec()));
                }
            } else {
                // 没有启用溢出，阻塞等待
                self.flush_batch();
                queues.wait_for_space(base_ptr, cluster_id);
                
                self.batch_buffer.entries.push((heap_tid, vector.to_vec()));
            }
        } else {
            self.batch_buffer.entries.push((heap_tid, vector.to_vec()));
        }

        if !self.cluster_sizes.is_null() {
            (*self.cluster_sizes).increment(cluster_id);
        }
    }

    /// 等待溢出数据被消费（旧方法，保留用于兼容）
    unsafe fn wait_for_spill_consumed(&self) {
        if let Some(ref spill_manager) = self.spill_manager {
            loop {
                let spilled = spill_manager.total_spilled.load(Ordering::Relaxed);
                let read = spill_manager.total_read.load(Ordering::Relaxed);
                
                if spilled == read {
                    break;
                }
                
                // 短暂休眠，避免忙等待
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
}
```

### 3.3 Consumer 修改

#### 3.3.1 修改 Consumer 主循环

```rust
unsafe fn process_cluster_vectors<S: Storage>(
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
    cluster_start_nodes: *mut ClusterStartNodes,
    spill_manager: Option<&mut SpillManager>,  // 新增
) {
    let mut insert_stats = InsertStats::default();
    let mut queue_idx: usize = 0;
    let start_idx = consumer_state.start_idx;
    let end_idx = consumer_state.end_idx;
    let num_dimensions = meta_page.get_num_dimensions_to_index() as usize;
    let mut start_node_set = false;
    let mut vector_buf: Vec<f32> = Vec::new();  // 用于读取溢出文件

    loop {
        // 优先从队列获取数据
        let available = queues.queue_size(base_ptr, cluster_id);
        
        if available == 0 && queues.is_queue_finished(base_ptr, cluster_id) {
            // 队列已完成，尝试从溢出文件读取
            if let Some(ref mut sm) = spill_manager {
                let read = sm.read_to_queue(
                    cluster_id, 
                    queues, 
                    base_ptr, 
                    &mut vector_buf
                ).unwrap_or(0);
                
                if read == 0 {
                    // 溢出文件也读完了，退出
                    break;
                }
                // 继续处理新读取的数据
                continue;
            } else {
                break;
            }
        }

        // 原有处理逻辑...
        // ...
    }
}
```

## 4. 流程图

### 4.1 Producer 流程

```
IndexBuildHeapScan
        │
        ▼
┌─────────────────────┐
│ 对于每个向量         │
└─────────────────────┘
        │
        ▼
计算 cluster_id
        │
        ▼
检查扫描进度 >= 90%?
        │
   ┌────┴────┐
   是        否
   │          │
   ▼          ▼
检查溢出数据  检查队列空间
是否已消费    │
   │      ┌───┴───┐
   ▼      ▼       ▼
┌──────┐ 有空间  已满
│阻塞  │   │       │
│等待  │   ▼       ▼
└──────┘ 直接入队  溢出到文件
              │       │
              ▼       ▼
         正常处理   继续扫描
```

### 4.2 Consumer 流程

```
Consumer 启动
        │
        ▼
┌─────────────────────┐
│ 主循环              │
└─────────────────────┘
        │
        ▼
队列有数据?
        │
   ┌────┴────┐
   是        否
   │          │
   ▼          ▼
处理数据   队列已完成?
   │          │
   │     ┌────┴────┐
   │     是        否
   │     │          │
   │     ▼          ▼
   │  溢出文件    等待数据
   │  有数据?      │
   │     │          │
   │  ┌──┴──┐       │
   │  是     否     │
   │  │      │      │
   │  ▼      ▼      │
   │ 读取到   退出   │
   │ 队列            │
   │  │              │
   └──┴──────────────┘
        │
        ▼
   继续循环
```

## 5. 关键优化点

### 5.1 避免存储翻倍

**策略**：**分阶段扫描 + 10% 溢出上限**

1. **分阶段检查**：每扫描 10% 的数据，检查溢出文件总大小
2. **10% 上限**：确保溢出文件总大小不超过原始表大小的 10%
3. **动态等待**：如果超过 10%，暂停扫描，等待 Consumer 消化至 5% 以下再继续

**实现细节**：
```rust
// Producer 主循环
for (i, tuple) in heap_scan.enumerate() {
    let progress = i as f64 / total_tuples as f64;
    
    // 每 10% 检查一次
    if progress % 0.1 < 0.001 {
        let spill_size = spill_manager.total_spill_size();
        let table_size = get_table_size();
        
        if spill_size > table_size * 0.1 {
            // 超过 10%，等待消费
            spill_manager.wait_until_spill_ratio(0.05); // 等待至 5%
        }
    }
    
    // 处理当前元组...
}
```

**优势**：
- **渐进控制**：避免最后 10% 数据时才发现溢出过多
- **资源平衡**：Producer 和 Consumer 工作量更均衡
- **可预测性**：磁盘使用可控，不会超过表大小的 10%

### 5.2 性能优化

**批量读写**：
- Producer：批量写入溢出文件（减少 I/O 次数）
- Consumer：批量读取溢出文件到队列

**内存映射**：
- 对于大溢出文件，使用 `mmap` 提高读取性能

**压缩**：
- 可选：对溢出文件进行压缩（LZ4/Snappy），减少磁盘 I/O

## 6. 配置参数

```rust
/// GUC 参数
pub struct SpillConfig {
    /// 是否启用溢出功能
    pub enabled: bool,
    /// 最大允许的溢出比例（相对于原始表大小）
    pub max_spill_ratio: f64,  // 默认 0.1 (10%)
    /// 恢复扫描的溢出比例（低于此值继续扫描）
    pub resume_spill_ratio: f64,  // 默认 0.05 (5%)
    /// 检查间隔（扫描进度百分比）
    pub check_interval: f64,  // 默认 0.1 (10%)
    /// 溢出文件目录
    pub spill_directory: String,  // 默认 "/tmp/pg_vectorscale_spill"
    /// 批量写入大小
    pub batch_write_size: usize,  // 默认 1000
}
```

**配置说明**：
- `max_spill_ratio = 0.1`：溢出文件总大小不超过表大小的 10%
- `resume_spill_ratio = 0.05`：当溢出文件降至 5% 以下时恢复扫描
- `check_interval = 0.1`：每扫描 10% 的数据检查一次溢出文件大小

## 7. 错误处理

### 7.1 磁盘空间不足

```rust
if disk_space < required_space {
    // 1. 尝试清理已完全消费的溢出文件
    spill_manager.cleanup_consumed_files()?;
    
    // 2. 如果仍然不足，阻塞等待
    if disk_space < required_space {
        warning!("Disk space low, waiting for consumers...");
        wait_for_consumers();
    }
}
```

### 7.2 文件损坏

```rust
// 使用 CRC32 校验每条记录
let crc = calculate_crc(&record);
writer.write_all(&crc.to_le_bytes())?;

// 读取时验证
let expected_crc = u32::from_le_bytes(crc_bytes);
if calculated_crc != expected_crc {
    return Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "CRC mismatch - spill file corrupted"
    ));
}
```

## 8. 总结

### 8.1 解决的问题

1. **队头阻塞**：Producer 不再因为单个 Cluster 队列满而阻塞整个扫描
2. **资源利用率**：CPU 和 I/O 资源得到更充分利用
3. **可扩展性**：支持更大的数据集（不受内存限制）

### 8.2 权衡

| 优点 | 缺点 |
|------|------|
| 提高吞吐量 | 增加磁盘 I/O |
| 减少内存压力 | 增加代码复杂度 |
| 支持大数据集 | 需要额外的磁盘空间 |
| 更好的资源利用 | 需要监控磁盘空间 |

### 8.3 适用场景

- **大数据集**：1亿+ 向量
- **不均衡分布**：某些 Cluster 数据量远大于其他
- **内存受限**：无法增加队列容量
- **高并发**：多个 Consumer 处理不同 Cluster
