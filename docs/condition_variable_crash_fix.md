# PostgreSQL Condition Variable Crash 问题分析与修复

## 问题概述

在 pgvectorscale 的并行索引构建过程中，PostgreSQL worker 进程在退出时发生 core dump。崩溃发生在 `ConditionVariableCancelSleep()` 函数中，原因是访问了无效的内存地址。

## 崩溃堆栈

```
#0  0x000000000096bddb in tas (lock=0x7f79ed654d78 <error: Cannot access memory at address 0x7f79ed654d78>)
    at ../../../../src/include/storage/s_lock.h:228
#1  0x000000000096c2e4 in ConditionVariableCancelSleep () at condition_variable.c:238
#2  0x000000000096c421 in ConditionVariableBroadcast (cv=0x7f8217e023d4) at condition_variable.c:310
#3  0x00000000009646f6 in CleanupProcSignalState (status=0, arg=0) at procsignal.c:240
#4  0x000000000095beb2 in shmem_exit (code=0) at ipc.c:283
#5  0x000000000095bce8 in proc_exit_prepare (code=0) at ipc.c:199
#6  0x000000000095bc3f in proc_exit (code=0) at ipc.c:112
#7  0x00000000008c630a in BackgroundWorkerMain (startup_data=0x1eadb80 "parallel worker for PID 3937093", startup_data_len=1472)
    at bgworker.c:851
```

## 根本原因分析

### 1. Condition Variable 机制

PostgreSQL 的 condition variable 使用一个进程级别的静态变量 `cv_sleep_target` 来跟踪当前进程准备睡眠的 condition variable：

```c
// src/backend/storage/lmgr/condition_variable.c
static ConditionVariable *cv_sleep_target = NULL;
```

当进程调用 `ConditionVariableSleep()` 或 `ConditionVariablePrepareToSleep()` 时，`cv_sleep_target` 会被设置为指向相应的 condition variable。

### 2. Worker 进程的执行流程

在 pgvectorscale 的并行索引构建中，worker 进程的执行流程如下：

1. **Worker 启动**：`_vectorscale_build_cluster_consumer_main` 被调用
2. **数据处理**：`build_cluster_subgraph` 循环处理队列中的数据
3. **等待数据**：当队列为空时，调用 `wait_on_cv()` → `ConditionVariableSleep()`
4. **设置 cv_sleep_target**：`ConditionVariableSleep()` 设置 `cv_sleep_target` 指向 `ClusterQueues` 中的 condition variable
5. **Worker 完成**：处理完所有数据后，worker 进程退出

### 3. 进程退出时的清理流程

当 worker 进程调用 `proc_exit()` 时，清理流程如下：

```
proc_exit()
  └── proc_exit_prepare()
       └── shmem_exit()
            ├── before_shmem_exit callbacks
            ├── dsm_backend_shutdown()          <-- 释放动态共享内存（包括 ClusterQueues）
            └── on_shmem_exit callbacks
                 └── CleanupProcSignalState()   <-- 调用 ConditionVariableBroadcast()
                      └── ConditionVariableBroadcast()
                           └── ConditionVariableCancelSleep()  <-- 访问 cv_sleep_target->mutex
```

### 4. 问题发生的时机

问题的关键在于 `dsm_backend_shutdown()` 和 `CleanupProcSignalState()` 的调用顺序：

1. `dsm_backend_shutdown()` 首先被调用，释放了包含 `ClusterQueues` 的动态共享内存
2. 此时 `cv_sleep_target` 仍然指向已释放内存中的 condition variable
3. `CleanupProcSignalState()` 被调用，它调用 `ConditionVariableBroadcast(&slot->pss_barrierCV)`
4. `ConditionVariableBroadcast()` 检查 `if (cv_sleep_target != NULL)`，发现不为 NULL
5. `ConditionVariableBroadcast()` 调用 `ConditionVariableCancelSleep()`
6. `ConditionVariableCancelSleep()` 尝试访问 `cv_sleep_target->mutex`，但此时 `cv_sleep_target` 指向的内存已被释放，导致崩溃

### 5. 为什么 cv_sleep_target 没有被清空

在修复前的代码中，`ConditionVariableCancelSleep()` 的调用位于 `build_cluster_subgraph()` 函数的末尾：

```rust
unsafe fn build_cluster_subgraph(...) {
    // ... 数据处理循环 ...
    
    // Cancel any pending condition variable sleep before exiting
    unsafe {
        pg_sys::ConditionVariableCancelSleep();
    }
}
```

**问题场景**：
- 如果 `build_cluster_subgraph()` 正常返回，`ConditionVariableCancelSleep()` 会被调用，`cv_sleep_target` 被清空
- 但如果 `build_cluster_subgraph()` 中发生 **panic**，Rust 会展开栈，但 `ConditionVariableCancelSleep()` 不会被调用
- 结果 `cv_sleep_target` 仍然指向已释放的内存，导致后续崩溃

## 修复方案

### 解决方案：RAII Guard 模式

使用 Rust 的 RAII（Resource Acquisition Is Initialization）模式，创建一个 guard 结构体，在其 `Drop` 实现中调用 `ConditionVariableCancelSleep()`。

### 修复代码

```rust
// 在文件顶部定义 RAII guard
/// RAII guard to ensure ConditionVariableCancelSleep is called on drop.
/// This is critical to prevent crashes during PostgreSQL's process cleanup.
/// When a worker exits, shmem_exit() first releases DSM segments (including
/// ClusterQueues), then calls on_shmem_exit callbacks including CleanupProcSignalState.
/// If cv_sleep_target still points to a ConditionVariable in the released DSM,
/// ConditionVariableCancelSleep() will access invalid memory.
struct CvSleepGuard;

impl Drop for CvSleepGuard {
    fn drop(&mut self) {
        unsafe {
            pg_sys::ConditionVariableCancelSleep();
        }
    }
}

// 在 _vectorscale_build_cluster_consumer_main 中使用
#[unsafe(no_mangle)]
#[cfg(feature = "build_parallel")]
pub extern "C" fn _vectorscale_build_cluster_consumer_main(
    _seg: *mut pg_sys::dsm_segment,
    shm_toc: *mut pg_sys::shm_toc,
) {
    // ... 前置检查 ...
    
    unsafe {
        // Create the guard to ensure ConditionVariableCancelSleep is called on exit.
        // This must be created before any code that might call ConditionVariableSleep.
        let _cv_guard = CvSleepGuard;
        
        // ... 后续代码（包括 build_cluster_subgraph 调用）...
        
        // CvSleepGuard will automatically call ConditionVariableCancelSleep() when it goes out of scope.
        // This ensures cv_sleep_target is cleared even if a panic occurs.
    }
}
```

### 修复的关键点

1. **RAII 模式**：`CvSleepGuard` 在创建时什么都不做，但在被 drop 时调用 `ConditionVariableCancelSleep()`
2. **提前创建**：在 `_vectorscale_build_cluster_consumer_main` 函数的开始处就创建 `_cv_guard`，确保在任何可能调用 `ConditionVariableSleep()` 的代码之前
3. **自动清理**：无论函数是正常返回还是因 panic 而退出，`_cv_guard` 都会被 drop，从而确保 `cv_sleep_target` 被清空
4. **移除冗余调用**：从 `build_cluster_subgraph()` 中移除了显式的 `ConditionVariableCancelSleep()` 调用

## 验证

### 修复前的代码流程（有问题）

```
_vectorscale_build_cluster_consumer_main()
  └── build_cluster_subgraph()
       ├── loop { ConditionVariableSleep() }  // 设置 cv_sleep_target
       └── ConditionVariableCancelSleep()      // 清空 cv_sleep_target（如果正常返回）
  └── proc_exit()
       └── shmem_exit()
            ├── dsm_backend_shutdown()         // 释放 ClusterQueues
            └── CleanupProcSignalState()
                 └── ConditionVariableBroadcast()
                      └── ConditionVariableCancelSleep()  // 如果 cv_sleep_target 未清空，崩溃
```

**问题**：如果 `build_cluster_subgraph()` panic，`ConditionVariableCancelSleep()` 不会被调用

### 修复后的代码流程（正确）

```
_vectorscale_build_cluster_consumer_main()
  ├── let _cv_guard = CvSleepGuard;           // 创建 guard
  └── build_cluster_subgraph()
       └── loop { ConditionVariableSleep() }   // 设置 cv_sleep_target
  └── // _cv_guard 在这里被 drop
       └── ConditionVariableCancelSleep()      // 清空 cv_sleep_target（无论是否 panic）
  └── proc_exit()
       └── shmem_exit()
            ├── dsm_backend_shutdown()         // 释放 ClusterQueues
            └── CleanupProcSignalState()
                 └── ConditionVariableBroadcast()
                      └── ConditionVariableCancelSleep()  // cv_sleep_target 为 NULL，安全
```

**正确**：无论 `build_cluster_subgraph()` 是否 panic，`_cv_guard` 都会被 drop，确保 `cv_sleep_target` 被清空

## 结论

### 问题本质

这是一个**资源清理顺序**问题。PostgreSQL 的进程退出机制先释放动态共享内存，再调用 cleanup 回调。如果进程级别的静态变量 `cv_sleep_target` 指向已释放的共享内存，后续的 cleanup 代码会访问无效内存。

### 修复原理

使用 Rust 的 RAII 模式确保 `ConditionVariableCancelSleep()` 在函数退出时被调用，无论函数是正常返回还是因 panic 而退出。这样可以确保在共享内存被释放之前，`cv_sleep_target` 已经被清空。

### 影响范围

此修复仅影响 pgvectorscale 的并行索引构建功能，不影响其他功能。修复后，worker 进程在退出时不会再因 condition variable 问题而崩溃。

## 附录：为什么 CleanupProcSignalState 需要调用 ConditionVariableBroadcast

### ProcSignal Barrier 机制

PostgreSQL 使用 **ProcSignal Barrier** 机制来实现全局状态同步。这个机制涉及：

- **`pss_barrierGeneration`**：每个进程的 barrier generation 计数器
- **`pss_barrierCV`**：每个进程的 condition variable，用于等待 barrier
- **`EmitProcSignalBarrier()`**：发送 barrier 信号给所有进程
- **`WaitForProcSignalBarrier()`**：等待所有进程处理完 barrier

### Barrier 的工作流程

```
进程 A (发起者)                    进程 B (参与者)
     |                                  |
     |  EmitProcSignalBarrier()         |
     |--------------------------------->|
     |  1. 设置所有进程的 pss_barrierCheckMask
     |  2. 增加 global generation
     |  3. 发送 SIGUSR1 给所有进程
     |                                  |
     |                                  | 收到信号
     |                                  | ProcessProcSignalBarrier()
     |                                  | 1. 处理 barrier
     |                                  | 2. 增加自己的 pss_barrierGeneration
     |                                  | 3. ConditionVariableBroadcast(&pss_barrierCV)
     |                                  |
     |  WaitForProcSignalBarrier()      |
     |  循环检查每个进程的 generation   |
     |<---------------------------------|
     |  如果 generation < target,       |
     |  ConditionVariableSleep(&pss_barrierCV)
     |  等待被唤醒                      |
     |                                  |
     |<---------------------------------| ConditionVariableBroadcast
     |  被唤醒，继续检查                |
```

### 为什么进程退出时需要 Broadcast

当进程退出时，`CleanupProcSignalState` 执行以下操作：

```c
static void
CleanupProcSignalState(int status, Datum arg)
{
    ProcSignalSlot *slot = MyProcSignalSlot;
    
    // ...
    
    /*
     * Make this slot look like it's absorbed all possible barriers, so that
     * no barrier waits block on it.
     */
    pg_atomic_write_u64(&slot->pss_barrierGeneration, PG_UINT64_MAX);
    ConditionVariableBroadcast(&slot->pss_barrierCV);
    
    slot->pss_pid = 0;
}
```

**原因**：

1. **防止等待进程阻塞**：如果其他进程正在调用 `WaitForProcSignalBarrier()` 等待当前进程处理 barrier，它们会在 `pss_barrierCV` 上睡眠。当前进程退出时，需要唤醒这些等待者，告诉它们"我已经处理完所有 barrier 了"。

2. **设置 generation 为最大值**：`PG_UINT64_MAX` 表示这个进程已经"吸收"了所有可能的 barrier，这样等待者不会再等待这个进程。

3. **避免死锁**：如果不 broadcast，等待者会永远睡眠，导致死锁。

### 具体场景示例

假设有 3 个进程：A（发起 barrier）、B（参与者）、C（参与者）

```
1. A 调用 EmitProcSignalBarrier()
   - 设置 B.pss_barrierCheckMask 和 C.pss_barrierCheckMask
   - 发送 SIGUSR1 给 B 和 C

2. B 收到信号，处理 barrier
   - B.pss_barrierGeneration = 1
   - ConditionVariableBroadcast(&B.pss_barrierCV)

3. C 在 barrier 处理期间崩溃/退出
   - 其他进程可能正在 WaitForProcSignalBarrier() 中等待 C
   - 它们在 C.pss_barrierCV 上睡眠

4. C 调用 CleanupProcSignalState
   - C.pss_barrierGeneration = PG_UINT64_MAX
   - ConditionVariableBroadcast(&C.pss_barrierCV)
   - 唤醒等待 C 的进程
   - 等待者就知道 C 不会再处理 barrier 了，继续检查其他进程
```

### 与 cv_sleep_target 的关系

问题发生在 `ConditionVariableBroadcast` 内部：

```c
void
ConditionVariableBroadcast(ConditionVariable *cv)
{
    // ...
    if (cv_sleep_target != NULL)
        ConditionVariableCancelSleep();  // <-- 这里崩溃
    // ...
}
```

`ConditionVariableBroadcast` 需要确保自己的 `cvWaitLink` 没有被占用，所以它会检查 `cv_sleep_target`。如果进程之前在**其他** condition variable 上准备了睡眠（例如 `ClusterQueues` 中的 CV），`cv_sleep_target` 会指向那个 CV。

**正常流程**：
- 进程调用 `ConditionVariableSleep(&cluster_queues_cv)` 
- `cv_sleep_target = &cluster_queues_cv`
- 进程完成工作，调用 `ConditionVariableCancelSleep()`
- `cv_sleep_target = NULL`
- 进程退出，`CleanupProcSignalState` 调用 `ConditionVariableBroadcast(&pss_barrierCV)`
- `ConditionVariableBroadcast` 检查 `cv_sleep_target == NULL`，跳过 `ConditionVariableCancelSleep`
- 安全完成

**崩溃流程**（修复前）：
- 进程调用 `ConditionVariableSleep(&cluster_queues_cv)`
- `cv_sleep_target = &cluster_queues_cv`
- 进程 panic，`ConditionVariableCancelSleep()` 未被调用
- `cv_sleep_target` 仍然指向 `&cluster_queues_cv`
- `dsm_backend_shutdown()` 释放 `ClusterQueues` 内存
- `CleanupProcSignalState` 调用 `ConditionVariableBroadcast(&pss_barrierCV)`
- `ConditionVariableBroadcast` 检查 `cv_sleep_target != NULL`
- 尝试调用 `ConditionVariableCancelSleep()`，访问 `cv_sleep_target->mutex`
- `cv_sleep_target` 指向已释放内存，崩溃

### 总结

`CleanupProcSignalState` 调用 `ConditionVariableBroadcast` 是为了：

1. **通知等待者**：告诉正在等待当前进程处理 barrier 的其他进程"我已经完成了/退出了"
2. **防止死锁**：避免等待者永远睡眠在 `pss_barrierCV` 上
3. **清理状态**：将 `pss_barrierGeneration` 设置为最大值，表示不再参与 barrier 同步

这是 PostgreSQL 全局状态同步机制的重要组成部分，确保进程退出时不会导致其他进程阻塞。

## 相关文件

- `/home/zhangqiang/code/postgres/contrib/pgvectorscale/pgvectorscale/src/access_method/build/parallel_build/cluster.rs`
- `/home/zhangqiang/code/postgres/src/backend/storage/lmgr/condition_variable.c`
- `/home/zhangqiang/code/postgres/src/backend/storage/ipc/procsignal.c`
- `/home/zhangqiang/code/postgres/src/backend/storage/ipc/ipc.c`
