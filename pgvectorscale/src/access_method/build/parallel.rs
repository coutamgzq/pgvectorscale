use std::sync::atomic::{AtomicBool, AtomicUsize};

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
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ClusterQueueEntry {
    pub heap_tid: pg_sys::ItemPointerData,
    pub vector_len: u32,
}

/// ClusterQueues structure with dynamically-sized arrays
/// The actual memory layout is:
/// [ClusterQueues header][queue_headers (num_queues items)][condition_vars (num_queues items)][queue data (num_queues * queue_capacity items)]
#[repr(C)]
pub struct ClusterQueues {
    pub num_queues: usize,
    pub entry_size: usize,       // Size of each entry
    pub queue_capacity: usize,   // Capacity of each queue
    // Flexible array members follow (not declared here, computed via offsets)
    // queue_headers: [ClusterQueueHeader; num_queues]
    // condition_vars: [ConditionVariable; num_queues]
    // queue_data: [u8; num_queues * queue_capacity * entry_size]
}

impl ClusterQueues {
    /// Calculate the total size needed for ClusterQueues with given parameters
    pub fn calculate_size(num_queues: usize, queue_capacity: usize, num_dimensions: usize) -> usize {
        let entry_size = std::mem::size_of::<ClusterQueueEntry>() + num_dimensions * std::mem::size_of::<f32>();
        let header_size = std::mem::size_of::<ClusterQueues>();
        let headers_size = num_queues * std::mem::size_of::<ClusterQueueHeader>();
        let cv_size = num_queues * std::mem::size_of::<ConditionVariable>();
        let data_size = num_queues * queue_capacity * entry_size;
        
        header_size + headers_size + cv_size + data_size
    }

    pub fn new(num_queues: usize, queue_capacity: usize, num_dimensions: usize) -> Self {
        Self {
            num_queues,
            entry_size: std::mem::size_of::<ClusterQueueEntry>() + num_dimensions * std::mem::size_of::<f32>(),
            queue_capacity,
        }
    }

    /// Initialize the ClusterQueues structure in pre-allocated memory
    /// This should be called immediately after allocating memory via shm_toc_allocate
    pub unsafe fn initialize(&self, base_ptr: *mut u8) {
        let header_size = std::mem::size_of::<ClusterQueues>();
        let headers_size = self.num_queues * std::mem::size_of::<ClusterQueueHeader>();
        
        // Initialize queue headers
        let headers_ptr = base_ptr.add(header_size) as *mut ClusterQueueHeader;
        for i in 0..self.num_queues {
            let header_ptr = headers_ptr.add(i);
            *header_ptr = ClusterQueueHeader::new(self.queue_capacity, self.entry_size);
        }
        
        // Initialize condition variables
        let cv_ptr = base_ptr.add(header_size + headers_size) as *mut ConditionVariable;
        for i in 0..self.num_queues {
            let cv = cv_ptr.add(i);
            pg_sys::ConditionVariableInit(cv);
        }
    }

    /// Get pointer to queue headers array
    pub unsafe fn get_headers_ptr(&self, base_ptr: *mut u8) -> *mut ClusterQueueHeader {
        base_ptr.add(std::mem::size_of::<ClusterQueues>()) as *mut ClusterQueueHeader
    }

    /// Get pointer to condition variables array
    pub unsafe fn get_cv_ptr(&self, base_ptr: *mut u8) -> *mut ConditionVariable {
        let header_size = std::mem::size_of::<ClusterQueues>();
        let headers_size = self.num_queues * std::mem::size_of::<ClusterQueueHeader>();
        base_ptr.add(header_size + headers_size) as *mut ConditionVariable
    }

    /// Get pointer to queue data storage
    pub unsafe fn get_data_ptr(&self, base_ptr: *mut u8) -> *mut u8 {
        let header_size = std::mem::size_of::<ClusterQueues>();
        let headers_size = self.num_queues * std::mem::size_of::<ClusterQueueHeader>();
        let cv_size = self.num_queues * std::mem::size_of::<ConditionVariable>();
        base_ptr.add(header_size + headers_size + cv_size)
    }

    /// Get pointer to a specific queue entry
    pub unsafe fn get_entry(&self, base_ptr: *mut u8, cluster_id: usize, index: usize) -> *mut ClusterQueueEntry {
        let data_ptr = self.get_data_ptr(base_ptr);
        data_ptr
            .add(cluster_id * self.entry_size * self.queue_capacity)
            .add(index * self.entry_size)
            .cast::<ClusterQueueEntry>()
    }

    /// Get queue header for a specific cluster
    pub unsafe fn get_header(&self, base_ptr: *mut u8, cluster_id: usize) -> *mut ClusterQueueHeader {
        let headers_ptr = self.get_headers_ptr(base_ptr);
        headers_ptr.add(cluster_id)
    }

    /// Get condition variable for a specific cluster
    pub unsafe fn get_cv(&self, base_ptr: *mut u8, cluster_id: usize) -> *mut ConditionVariable {
        let cv_ptr = self.get_cv_ptr(base_ptr);
        cv_ptr.add(cluster_id)
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
            let start_node = self.nodes[cluster_id].start_node;
            if start_node.ip_posid != 0 && start_node.ip_posid != pg_sys::InvalidOffsetNumber {
                Some(start_node)
            } else {
                None
            }
        } else {
            None
        }
    }
}

/// Default capacity for each queue
pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;
