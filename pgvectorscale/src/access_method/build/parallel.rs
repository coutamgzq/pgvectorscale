use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use pgrx::pg_sys::{self, ConditionVariable, Oid};

pub const SHM_TOC_SHARED_KEY: u64 = 0xD000000000000001;
pub const SHM_TOC_TABLESCANDESC_KEY: u64 = 0xD000000000000002;
pub const SHM_TOC_CLUSTER_QUEUES_KEY: u64 = 0xD000000000000003;
pub const SHM_TOC_CENTROIDS_KEY: u64 = 0xD000000000000004;
pub const SHM_TOC_CLUSTER_START_NODES_KEY: u64 = 0xD000000000000005;

pub fn flush_rate(total_vectors: usize) -> usize {
    let rate = crate::access_method::guc::TSV_PARALLEL_FLUSH_INTERVAL.get();
    let result = (total_vectors as f64 * rate) as usize;
    result.max(1)
}

pub fn initial_start_nodes_count() -> usize {
    crate::access_method::guc::TSV_PARALLEL_INITIAL_START_NODES_COUNT.get() as usize
}

pub unsafe fn cleanup_parallel_context(
    pcxt: *mut pg_sys::ParallelContext,
    snapshot: *mut pg_sys::SnapshotData,
) {
    if crate::util::ports::is_mvcc_snapshot(snapshot) {
        pg_sys::UnregisterSnapshot(snapshot);
    }
    pg_sys::DestroyParallelContext(pcxt);
    pg_sys::ExitParallelMode();
}

pub unsafe fn toc_estimate_single_chunk(pcxt: *mut pg_sys::ParallelContext, size: usize) {
    (*pcxt).estimator.space_for_chunks +=
        crate::util::ports::buffer_align(size);
    (*pcxt).estimator.number_of_keys += 1;
}

#[derive(Debug, Copy, Clone)]
pub struct ParallelSharedParams {
    pub heaprelid: Oid,
    pub indexrelid: Oid,
    pub is_concurrent: bool,
    pub num_clusters: usize,
    pub total_vectors: usize,
    pub num_dimensions: usize,
}

#[derive(Debug)]
pub struct ParallelBuildState {
    pub producer_done: AtomicBool,
    pub producer_ntuples: AtomicUsize,
    pub consumers_finished: AtomicUsize,
    pub initialization_cv: ConditionVariable,
}

#[derive(Debug)]
pub struct ParallelShared {
    pub params: ParallelSharedParams,
    pub build_state: ParallelBuildState,
}

#[derive(Debug)]
pub struct ParallelBuildInfo {
    pub parallel_shared: *mut ParallelShared,
    pub is_initializing_worker: bool,
    pub tablescandesc: *mut pg_sys::ParallelTableScanDescData,
}

#[repr(C)]
#[derive(Debug)]
pub struct ClusterQueueHeader {
    pub head: AtomicUsize,
    pub tail: AtomicUsize,
    pub capacity: usize,
    pub element_size: usize,
    pub finished: AtomicBool,
}

impl ClusterQueueHeader {
    pub fn new(capacity: usize, element_size: usize) -> Self {
        Self {
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            capacity,
            element_size,
            finished: AtomicBool::new(false),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == self.tail.load(Ordering::Acquire)
    }

    pub fn is_full(&self) -> bool {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);
        (tail + 1) % self.capacity == head
    }

    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);
        if tail >= head {
            tail - head
        } else {
            self.capacity - head + tail
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ClusterQueueEntry {
    pub heap_tid: pg_sys::ItemPointerData,
    pub vector_len: u32,
}

pub const MAX_VECTOR_DIMENSIONS: usize = 2000;

#[repr(C)]
pub struct ClusterQueues {
    pub num_queues: usize,
    pub queue_headers: [ClusterQueueHeader; 64],
    pub condition_vars: [ConditionVariable; 64],
}

impl ClusterQueues {
    pub fn new(num_queues: usize, queue_capacity: usize) -> Self {
        let mut queues = Self {
            num_queues,
            queue_headers: unsafe { std::mem::zeroed() },
            condition_vars: unsafe { std::mem::zeroed() },
        };

        for i in 0..num_queues {
            queues.queue_headers[i] = ClusterQueueHeader::new(
                queue_capacity,
                std::mem::size_of::<ClusterQueueEntry>() + MAX_VECTOR_DIMENSIONS * std::mem::size_of::<f32>(),
            );
            unsafe {
                pg_sys::ConditionVariableInit(&mut queues.condition_vars[i]);
            }
        }
        queues
    }

    pub fn get_queue(&self, cluster_id: usize) -> &ClusterQueueHeader {
        &self.queue_headers[cluster_id]
    }

    pub fn get_queue_mut(&mut self, cluster_id: usize) -> &mut ClusterQueueHeader {
        &mut self.queue_headers[cluster_id]
    }

    pub fn get_cv(&self, cluster_id: usize) -> &ConditionVariable {
        &self.condition_vars[cluster_id]
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ClusterStartNode {
    pub cluster_id: u32,
    pub start_node: pg_sys::ItemPointerData,
}

#[repr(C)]
pub struct ClusterStartNodes {
    pub num_clusters: usize,
    pub nodes: [ClusterStartNode; 64],
}

impl ClusterStartNodes {
    pub fn new(num_clusters: usize) -> Self {
        Self {
            num_clusters,
            nodes: unsafe { std::mem::zeroed() },
        }
    }

    pub fn set_start_node(&mut self, cluster_id: usize, start_node: pg_sys::ItemPointerData) {
        if cluster_id < self.num_clusters {
            self.nodes[cluster_id] = ClusterStartNode {
                cluster_id: cluster_id as u32,
                start_node,
            };
        }
    }

    pub fn get_start_node(&self, cluster_id: usize) -> Option<pg_sys::ItemPointerData> {
        if cluster_id < self.num_clusters {
            Some(self.nodes[cluster_id].start_node)
        } else {
            None
        }
    }
}

pub fn calculate_queue_size(num_dimensions: usize, capacity: usize) -> usize {
    let entry_size = std::mem::size_of::<ClusterQueueEntry>() + num_dimensions * std::mem::size_of::<f32>();
    entry_size * capacity + std::mem::size_of::<ClusterQueueHeader>()
}

pub fn calculate_cluster_queues_size(num_clusters: usize, num_dimensions: usize, queue_capacity: usize) -> usize {
    std::mem::size_of::<ClusterQueues>() + 
        num_clusters * calculate_queue_size(num_dimensions, queue_capacity)
}

pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;
