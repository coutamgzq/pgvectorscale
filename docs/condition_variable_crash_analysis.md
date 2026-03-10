# Condition Variable 崩溃问题分析与修复

## 问题概述

在并行集群构建过程中，当队列满时，后台工作进程（消费者）退出时发生核心转储（core dump）。崩溃发生在 `ConditionVariableCancelSleep` 函数中，尝试访问无效的内存地址。

## 崩溃堆栈分析

```
#0  0x000000000096bddb in tas (lock=0x7f4eb52212a8 <error: Cannot access memory at address 0x7f4eb52212a8>)
#1  0x000000000096c2e4 in ConditionVariableCancelSleep () at condition_variable.c:238
#2  0x000000000096c421 in ConditionVariableBroadcast (cv=0x7f56df8c016c) at condition_variable.c:310
#3  0x00000000009646f6 in CleanupProcSignalState (status=0, arg=0) at procsignal.c:240
#4  0x000000000095beb2 in shmem_exit (code=0) at ipc.c:283
#5  0x000000000095bce8 in proc_exit_prepare (code=0) at ipc.c:199
#6  0x000000000095bc3f in proc_exit (code=0) at ipc.c:112
#7  0x00000000008c630a in BackgroundWorkerMain at bgworker.c:851
```

### 关键信息

```
(gdb) p cv
$1 = (ConditionVariable *) 0x7f4eb52212a8
(gdb) p *cv
Cannot access memory at address 0x7f4eb52212a8
```

条件变量地址 `0x7f4eb52212a8` 无法访问，说明内存已被释放或从未正确初始化。

## 根本原因分析

### 1. 并行架构概述

在并行集群构建中，使用了 PostgreSQL 的 Parallel Context 机制：

**主进程（生产者）**：
- 创建 Parallel Context (`CreateParallelContext`)
- 分配 Dynamic Shared Memory (DSM)
- 启动后台工作进程 (`LaunchParallelWorkers`)
- 扫描堆表并将向量分发到各个集群队列
- 等待工作进程完成 (`WaitForParallelWorkersToFinish`)

**后台工作进程（消费者）**：
- 通过 `shm_toc_lookup` 获取共享内存指针
- 从分配的集群队列中读取向量
- 构建子图
- 退出时调用 `proc_exit()`

### 2. 代码结构问题

在 `parallel.rs` 中，`ClusterQueues` 结构体定义如下：

```rust
#[repr(C)]
pub struct ClusterQueues {
    pub num_queues: usize,
    pub queue_headers: [ClusterQueueHeader; 64],
    pub condition_vars: [ConditionVariable; 64],  // 固定大小为64
    pub entry_size: usize,
    pub queue_capacity: usize,
}
```

**问题**：`condition_vars` 数组固定大小为 64，但 `ClusterQueues::new()` 只初始化了 `num_queues` 个条件变量。

### 3. 原始代码（有问题的实现）

```rust
impl ClusterQueues {
    pub fn new(num_queues: usize, queue_capacity: usize, num_dimensions: usize) -> Self {
        let entry_size = std::mem::size_of::<ClusterQueueEntry>() + num_dimensions * std::mem::size_of::<f32>();
        let mut queues = Self {
            num_queues,
            queue_headers: unsafe { std::mem::zeroed() },
            condition_vars: unsafe { std::mem::zeroed() },  // 全部置零，未初始化
            entry_size,
            queue_capacity,
        };

        for i in 0..num_queues {
            queues.queue_headers[i] = ClusterQueueHeader::new(queue_capacity, entry_size);
            unsafe {
                pg_sys::ConditionVariableInit(&mut queues.condition_vars[i]);  // 只初始化前 num_queues 个
            }
        }
        queues
    }
}
```

### 3. PostgreSQL 进程退出流程

当后台工作进程（Background Worker）退出时，PostgreSQL 执行以下清理流程：

```
proc_exit()
  └── proc_exit_prepare()
       └── shmem_exit()
            └── CleanupProcSignalState()
                 └── ConditionVariableBroadcast()  // 尝试广播条件变量
                      └── ConditionVariableCancelSleep()  // 取消睡眠
                           └── SpinLockAcquire(&cv->mutex)  // 访问 cv->mutex
```

### 4. 问题发生的条件

**场景**：假设创建 4 个集群（num_queues = 4）

1. **初始化阶段**：
   - `ClusterQueues::new(4, ...)` 只初始化 `condition_vars[0..3]`
   - `condition_vars[4..63]` 保持为零（未初始化）

2. **运行时**：
   - 生产者（主进程）向队列写入数据
   - 当队列满时，生产者调用 `ConditionVariableSleep()` 等待
   - 消费者（后台工作进程）从队列读取数据

3. **退出阶段**：
   - 消费者完成工作，调用 `proc_exit()` 退出
   - `CleanupProcSignalState()` 被调用
   - 它尝试对所有可能正在等待的条件变量进行广播
   - 由于 `condition_vars` 数组中的某些条目未初始化，`SpinLockAcquire(&cv->mutex)` 访问了无效内存

### 5. 为什么队列满时更容易触发

当队列满时：
- 生产者被阻塞在 `ConditionVariableSleep()` 等待消费者
- 消费者处理完数据后退出
- PostgreSQL 的清理代码检测到进程正在等待条件变量
- 尝试取消睡眠并广播条件变量
- 如果访问到未初始化的条件变量，就会崩溃

### 6. 后台工作进程如何访问条件变量

#### 6.1 共享内存的映射

当主进程创建 Parallel Context 时：

```rust
// 主进程分配 DSM
let pcxt = pg_sys::CreateParallelContext(
    crate::EXTENSION_NAME,
    PARALLEL_BUILD_CLUSTER_CONSUMER_MAIN,
    num_workers as i32,
);

// 在 DSM 中分配 ClusterQueues
let cluster_queues = pg_sys::shm_toc_allocate(
    (*pcxt).toc,
    cluster_queues_size,
).cast::<ClusterQueues>();

// 初始化 ClusterQueues（包括条件变量）
(*cluster_queues) = ClusterQueues::new(num_clusters, DEFAULT_QUEUE_CAPACITY, num_dimensions);
```

DSM（Dynamic Shared Memory）被映射到主进程的地址空间，同时后台工作进程通过 `dsm_attach` 附加到同一个 DSM 段，获得相同的内存视图。

#### 6.2 后台工作进程的视角

后台工作进程启动时：

```rust
#[unsafe(no_mangle)]
pub extern "C" fn _vectorscale_build_cluster_consumer_main(
    _seg: *mut pg_sys::dsm_segment,
    shm_toc: *mut pg_sys::shm_toc,
) {
    // 通过 shm_toc_lookup 获取共享内存中的 ClusterQueues
    let cluster_queues: *mut ClusterQueues = unsafe {
        pg_sys::shm_toc_lookup(shm_toc, SHM_TOC_CLUSTER_QUEUES_KEY, false)
            .cast::<ClusterQueues>()
    };
    
    // 获取该消费者对应的条件变量
    let queues = &mut *cluster_queues;
    let cv = &queues.condition_vars[cluster_id];  // 访问共享内存中的条件变量
    
    // 消费者循环：等待并处理数据
    loop {
        let head = header.head.load(Ordering::Acquire);
        let tail = header.tail.load(Ordering::Acquire);
        
        if head != tail {
            // 处理数据...
            pg_sys::ConditionVariableBroadcast(cv as *const _ as *mut _);
        } else if header.finished.load(Ordering::Acquire) {
            break;
        } else {
            // 队列为空，等待生产者
            pg_sys::ConditionVariableSleep(cv as *const _ as *mut _, pg_sys::PG_WAIT_EXTENSION);
        }
    }
}
```

#### 6.3 关键问题：消费者如何"看到"所有条件变量

虽然每个消费者只使用自己的条件变量（`condition_vars[cluster_id]`），但**整个 `ClusterQueues` 结构体（包括所有 64 个条件变量）都位于共享内存中**。

当消费者附加到 DSM 时，它映射了整个 `ClusterQueues` 结构体，包括：
- `num_queues`（1 个 usize）
- `queue_headers[64]`（64 个 ClusterQueueHeader）
- `condition_vars[64]`（64 个 ConditionVariable）← **这里包含未初始化的条件变量**
- `entry_size` 和 `queue_capacity`
- 队列数据存储区

#### 6.4 崩溃发生的精确时机

**场景**：4 个集群，队列满

1. **主进程（生产者）**：
   - 初始化 `ClusterQueues`，只初始化 `condition_vars[0..3]`
   - `condition_vars[4..63]` 为零（未初始化）
   - 向队列写入数据直到队列满
   - 调用 `ConditionVariableSleep(&condition_vars[0])` 等待

2. **消费者进程（Worker 0）**：
   - 附加到 DSM，看到整个 `ClusterQueues`
   - 从队列读取数据
   - 广播 `condition_vars[0]` 唤醒生产者
   - 继续处理直到队列为空且 `finished=true`
   - 调用 `proc_exit()` 退出

3. **退出清理（关键！）**：
   ```
   proc_exit()
     └── proc_exit_prepare()
          └── shmem_exit()
               └── CleanupProcSignalState()
   ```
   
   `CleanupProcSignalState` 是 PostgreSQL 的通用清理函数，它会：
   - 检查当前进程是否在任何条件变量上等待
   - 尝试取消睡眠并广播相关的条件变量
   - **但它并不限于当前进程使用的条件变量！**

   由于 `ClusterQueues` 位于 DSM 中，且 DSM 在进程退出时会被分离，PostgreSQL 的清理代码会尝试确保所有相关的同步原语都被正确清理。

4. **崩溃发生**：
   - 清理代码尝试访问 `condition_vars[4]`（未初始化）
   - 或者由于内存布局问题，访问了错误的地址
   - `SpinLockAcquire(&cv->mutex)` 访问无效内存
   - 段错误（Segmentation Fault）

#### 6.5 为什么清理代码会访问未使用的条件变量

PostgreSQL 的 `CleanupProcSignalState` 函数设计为通用清理机制：

```c
void CleanupProcSignalState(int status, Datum arg)
{
    // 如果进程正在等待条件变量，取消睡眠
    if (MyProc->waitLock)
    {
        ConditionVariableCancelSleep();
    }
    
    // 广播条件变量以唤醒其他等待者
    // 注意：这里的实现可能会遍历或访问相关的条件变量
}
```

虽然消费者只使用自己的条件变量，但**共享内存中的整个条件变量数组对 PostgreSQL 的清理机制都是可见的**。如果某些条件变量未初始化，清理代码可能在尝试访问它们时崩溃。

此外，由于 DSM 在进程退出时被分离，条件变量的内存地址可能变得无效，进一步增加了崩溃的风险。

## 修复方案

### 修复后的代码

```rust
impl ClusterQueues {
    pub fn new(num_queues: usize, queue_capacity: usize, num_dimensions: usize) -> Self {
        let entry_size = std::mem::size_of::<ClusterQueueEntry>() + num_dimensions * std::mem::size_of::<f32>();
        let mut queues = Self {
            num_queues,
            queue_headers: unsafe { std::mem::zeroed() },
            condition_vars: unsafe { std::mem::zeroed() },
            entry_size,
            queue_capacity,
        };

        for i in 0..num_queues {
            queues.queue_headers[i] = ClusterQueueHeader::new(
                queue_capacity,
                entry_size,
            );
        }
        
        // Initialize ALL condition variables (up to 64) to prevent crashes
        // when PostgreSQL tries to clean up condition variables on process exit
        for i in 0..64 {
            unsafe {
                pg_sys::ConditionVariableInit(&mut queues.condition_vars[i]);
            }
        }
        queues
    }
}
```

### 关键修改

1. **分离初始化循环**：将队列头部和条件变量的初始化分开
2. **初始化所有条件变量**：使用 `for i in 0..64` 初始化全部 64 个条件变量，而不仅仅是 `num_queues` 个
3. **添加注释**：解释为什么需要初始化所有条件变量

### 附加修复

在消费者退出前，显式广播条件变量唤醒可能在等待的生产者：

```rust
// Broadcast to wake up any producer waiting on this queue
// This must be done before the consumer exits to prevent the producer
// from waiting indefinitely on a condition variable in shared memory
// that may be detached when the worker exits
unsafe {
    pg_sys::ConditionVariableBroadcast(cv as *const _ as *mut _);
}
```

## 技术原理

### PostgreSQL Condition Variable 内部结构

```c
typedef struct ConditionVariable
{
    slock_t     mutex;          // 自旋锁
    PGPROC     *waiter;         // 等待进程链表
} ConditionVariable;
```

### ConditionVariableInit 的作用

```c
void ConditionVariableInit(ConditionVariable *cv)
{
    SpinLockInit(&cv->mutex);   // 初始化自旋锁
    cv->waiter = NULL;          // 清空等待链表
}
```

如果条件变量未初始化：
- `mutex` 包含随机值或零
- `SpinLockAcquire(&cv->mutex)` 尝试访问无效内存地址
- 导致段错误（Segmentation Fault）

### Dynamic Shared Memory (DSM) 生命周期

1. **创建**：主进程通过 `dsm_create()` 创建 DSM 段
2. **附加**：工作进程通过 `dsm_attach()` 附加到 DSM
3. **分离**：工作进程退出时自动分离 DSM
4. **销毁**：主进程销毁 DSM 段

**关键点**：当工作进程调用 `proc_exit()` 时，DSM 可能仍然附加，但 PostgreSQL 的清理代码会尝试访问条件变量，这些变量位于 DSM 中。

## 预防措施

1. **数组初始化一致性**：如果结构体包含固定大小的数组，确保初始化所有元素，而不仅仅是使用的部分

2. **资源清理顺序**：在进程退出前，确保：
   - 唤醒所有等待的进程
   - 清理所有同步原语
   - 最后才分离共享内存

3. **防御性编程**：对于共享内存中的同步原语，始终初始化所有可能的实例

## 总结

这个问题的根本原因是：**代码只初始化了部分条件变量，但 PostgreSQL 的进程退出清理代码可能访问到未初始化的条件变量，导致访问无效内存。**

修复方案确保所有 64 个条件变量都被正确初始化，无论实际使用多少个队列，从而防止在进程退出时发生崩溃。
