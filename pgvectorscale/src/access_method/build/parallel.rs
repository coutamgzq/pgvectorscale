use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use pgrx::pg_sys::{self, ConditionVariable, Oid};

pub const SHM_TOC_SHARED_KEY: u64 = 0xD000000000000001;
pub const SHM_TOC_TABLESCANDESC_KEY: u64 = 0xD000000000000002;
pub const SHM_TOC_CLUSTER_QUEUES_KEY: u64 = 0xD000000000000003;
pub const SHM_TOC_CENTROIDS_KEY: u64 = 0xD000000000000004;
pub const SHM_TOC_CLUSTER_START_NODES_KEY: u64 = 0xD000000000000005;
pub const SHM_TOC_CLUSTER_SIZES_KEY: u64 = 0xD000000000000006;
pub const SHM_TOC_WORKER_ASSIGNMENTS_KEY: u64 = 0xD000000000000007;

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
    (*pcxt).estimator.space_for_chunks += crate::util::ports::buffer_align(size);
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
    pub start_nodes_initialized: AtomicBool,
    pub initialization_cv: ConditionVariable,
    /// Condition variable for worker assignments ready notification
    pub assignments_cv: ConditionVariable,
    /// Flag indicating worker assignments are ready
    pub assignments_ready: AtomicBool,
}

#[derive(Debug)]
pub struct ParallelShared {
    pub params: ParallelSharedParams,
    pub build_state: ParallelBuildState,
    /// Pointer to shared MetaPage for cluster builds.
    /// This is used to share the same MetaPage instance across all worker processes.
    #[allow(dead_code)]
    pub meta_page_ptr: *mut crate::access_method::meta_page::MetaPage,
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
    pub entry_size: usize, // Size of each entry
    pub queue_capacity: usize, // Capacity of each queue
                           // Flexible array members follow (not declared here, computed via offsets)
                           // queue_headers: [ClusterQueueHeader; num_queues]
                           // condition_vars: [ConditionVariable; num_queues]
                           // queue_data: [u8; num_queues * queue_capacity * entry_size]
}

impl ClusterQueues {
    /// Calculate the total size needed for ClusterQueues with given parameters
    pub fn calculate_size(
        num_queues: usize,
        queue_capacity: usize,
        num_dimensions: usize,
    ) -> usize {
        let entry_size =
            std::mem::size_of::<ClusterQueueEntry>() + num_dimensions * std::mem::size_of::<f32>();
        let header_size = std::mem::size_of::<ClusterQueues>();
        let headers_size = num_queues * std::mem::size_of::<ClusterQueueHeader>();
        let cv_size = num_queues * std::mem::size_of::<ConditionVariable>();
        let data_size = num_queues * queue_capacity * entry_size;

        header_size + headers_size + cv_size + data_size
    }

    pub fn new(num_queues: usize, queue_capacity: usize, num_dimensions: usize) -> Self {
        Self {
            num_queues,
            entry_size: std::mem::size_of::<ClusterQueueEntry>()
                + num_dimensions * std::mem::size_of::<f32>(),
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

        // Initialize condition variables for each queue
        let cv_ptr = base_ptr.add(header_size + headers_size) as *mut ConditionVariable;
        for i in 0..self.num_queues {
            let cv = cv_ptr.add(i);
            pg_sys::ConditionVariableInit(cv);
        }
    }

    /// Initialize with per-queue capacities
    /// queue_capacities must have length >= num_queues
    pub unsafe fn initialize_with_capacities(&self, base_ptr: *mut u8, queue_capacities: &[usize]) {
        let header_size = std::mem::size_of::<ClusterQueues>();
        let headers_size = self.num_queues * std::mem::size_of::<ClusterQueueHeader>();

        // Initialize queue headers with individual capacities
        let headers_ptr = base_ptr.add(header_size) as *mut ClusterQueueHeader;
        for i in 0..self.num_queues {
            let header_ptr = headers_ptr.add(i);
            let capacity = queue_capacities
                .get(i)
                .copied()
                .unwrap_or(self.queue_capacity);
            *header_ptr = ClusterQueueHeader::new(capacity, self.entry_size);
        }

        // Initialize condition variables for each queue
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
    /// For dynamic capacities, this calculates offset based on actual header capacities
    pub unsafe fn get_entry(
        &self,
        base_ptr: *mut u8,
        cluster_id: usize,
        index: usize,
    ) -> *mut ClusterQueueEntry {
        let data_ptr = self.get_data_ptr(base_ptr);

        // Calculate offset based on actual capacities from headers
        let headers_ptr = self.get_headers_ptr(base_ptr);
        let mut offset = 0;
        for i in 0..cluster_id {
            let header = &*headers_ptr.add(i);
            offset += header.capacity * self.entry_size;
        }

        // Add offset within this cluster's queue
        offset += index * self.entry_size;

        data_ptr.add(offset).cast::<ClusterQueueEntry>()
    }

    /// Get queue header for a specific cluster
    pub unsafe fn get_header(
        &self,
        base_ptr: *mut u8,
        cluster_id: usize,
    ) -> *mut ClusterQueueHeader {
        let headers_ptr = self.get_headers_ptr(base_ptr);
        headers_ptr.add(cluster_id)
    }

    /// Get condition variable for a specific cluster
    pub unsafe fn get_cv(&self, base_ptr: *mut u8, cluster_id: usize) -> *mut ConditionVariable {
        let cv_ptr = self.get_cv_ptr(base_ptr);
        cv_ptr.add(cluster_id)
    }

    /// Push a vector to the queue for a specific cluster
    /// Returns true if successful, false if the heap_tid is invalid
    pub unsafe fn push_to_queue(
        &self,
        base_ptr: *mut u8,
        cluster_id: usize,
        heap_tid: pg_sys::ItemPointerData,
        vector: &[f32],
    ) -> bool {
        if heap_tid.ip_posid == 0 || heap_tid.ip_posid == pg_sys::InvalidOffsetNumber {
            return false;
        }

        let block_num = pgrx::itemptr::item_pointer_get_block_number_no_check(heap_tid);
        if block_num == pg_sys::InvalidBlockNumber {
            return false;
        }

        let header = self.get_header(base_ptr, cluster_id);
        let cv = self.get_cv(base_ptr, cluster_id);

        loop {
            let tail = (*header).tail.load(std::sync::atomic::Ordering::Acquire);
            let head = (*header).head.load(std::sync::atomic::Ordering::Acquire);
            let next_tail = (tail + 1) % (*header).capacity;

            if next_tail != head {
                let entry_ptr = self.get_entry(base_ptr, cluster_id, tail);

                (*entry_ptr).heap_tid = heap_tid;
                (*entry_ptr).vector_len = vector.len() as u32;

                let vector_ptr = (entry_ptr as *mut u8)
                    .add(std::mem::size_of::<ClusterQueueEntry>())
                    as *mut f32;
                std::ptr::copy_nonoverlapping(vector.as_ptr(), vector_ptr, vector.len());

                (*header)
                    .tail
                    .store(next_tail, std::sync::atomic::Ordering::Release);

                pg_sys::ConditionVariableBroadcast(cv as *const _ as *mut _);
                return true;
            }

            pg_sys::ConditionVariableSleep(cv as *const _ as *mut _, pg_sys::PG_WAIT_EXTENSION);
        }
    }

    /// Pop a vector from the queue for a specific cluster
    /// Returns Some((heap_tid, vector_data)) if successful, None if queue is empty
    pub unsafe fn pop_from_queue(
        &self,
        base_ptr: *mut u8,
        cluster_id: usize,
    ) -> Option<(pg_sys::ItemPointerData, Vec<f32>)> {
        let header = self.get_header(base_ptr, cluster_id);
        let cv = self.get_cv(base_ptr, cluster_id);

        let head = (*header).head.load(std::sync::atomic::Ordering::Acquire);
        let tail = (*header).tail.load(std::sync::atomic::Ordering::Acquire);

        if head == tail {
            return None;
        }

        let entry_ptr = self.get_entry(base_ptr, cluster_id, head);

        let heap_tid = (*entry_ptr).heap_tid;
        let vector_len = (*entry_ptr).vector_len as usize;

        if heap_tid.ip_posid == 0 || heap_tid.ip_posid == pg_sys::InvalidOffsetNumber {
            let next_head = (head + 1) % (*header).capacity;
            (*header)
                .head
                .store(next_head, std::sync::atomic::Ordering::Release);
            pg_sys::ConditionVariableBroadcast(cv as *const _ as *mut _);
            return None;
        }

        let block_num = pgrx::itemptr::item_pointer_get_block_number_no_check(heap_tid);
        if block_num == pg_sys::InvalidBlockNumber {
            let next_head = (head + 1) % (*header).capacity;
            (*header)
                .head
                .store(next_head, std::sync::atomic::Ordering::Release);
            pg_sys::ConditionVariableBroadcast(cv as *const _ as *mut _);
            return None;
        }

        let vector_ptr =
            (entry_ptr as *const u8).add(std::mem::size_of::<ClusterQueueEntry>()) as *const f32;
        let vector_data = std::slice::from_raw_parts(vector_ptr, vector_len).to_vec();

        let next_head = (head + 1) % (*header).capacity;
        (*header)
            .head
            .store(next_head, std::sync::atomic::Ordering::Release);
        pg_sys::ConditionVariableBroadcast(cv as *const _ as *mut _);

        Some((heap_tid, vector_data))
    }

    /// Check if queue is finished (producer done and queue empty)
    pub unsafe fn is_queue_finished(&self, base_ptr: *mut u8, cluster_id: usize) -> bool {
        let header = self.get_header(base_ptr, cluster_id);
        let head = (*header).head.load(std::sync::atomic::Ordering::Acquire);
        let tail = (*header).tail.load(std::sync::atomic::Ordering::Acquire);
        let finished = (*header)
            .finished
            .load(std::sync::atomic::Ordering::Acquire);

        finished && head == tail
    }

    /// Wait on condition variable for this cluster's queue
    pub unsafe fn wait_on_cv(&self, base_ptr: *mut u8, cluster_id: usize) {
        let cv = self.get_cv(base_ptr, cluster_id);
        pg_sys::ConditionVariableSleep(cv as *const _ as *mut _, pg_sys::PG_WAIT_EXTENSION);
    }

    /// Mark queue as finished (producer done)
    pub unsafe fn mark_queue_finished(&self, base_ptr: *mut u8, cluster_id: usize) {
        let header = self.get_header(base_ptr, cluster_id);
        (*header)
            .finished
            .store(true, std::sync::atomic::Ordering::Release);

        let cv = self.get_cv(base_ptr, cluster_id);
        pg_sys::ConditionVariableBroadcast(cv as *const _ as *mut _);
    }

    /// Push multiple vectors to the queue for a specific cluster in batch
    /// Returns the number of vectors successfully pushed
    pub unsafe fn push_batch_to_queue(
        &self,
        base_ptr: *mut u8,
        cluster_id: usize,
        entries: &[(pg_sys::ItemPointerData, &[f32])],
    ) -> usize {
        if entries.is_empty() {
            return 0;
        }

        let header = self.get_header(base_ptr, cluster_id);
        let cv = self.get_cv(base_ptr, cluster_id);
        let mut pushed = 0;

        for (heap_tid, vector) in entries {
            if heap_tid.ip_posid == 0 || heap_tid.ip_posid == pg_sys::InvalidOffsetNumber {
                continue;
            }

            let block_num = pgrx::itemptr::item_pointer_get_block_number_no_check(*heap_tid);
            if block_num == pg_sys::InvalidBlockNumber {
                continue;
            }

            loop {
                let tail = (*header).tail.load(std::sync::atomic::Ordering::Acquire);
                let head = (*header).head.load(std::sync::atomic::Ordering::Acquire);
                let next_tail = (tail + 1) % (*header).capacity;

                if next_tail != head {
                    let entry_ptr = self.get_entry(base_ptr, cluster_id, tail);

                    (*entry_ptr).heap_tid = *heap_tid;
                    (*entry_ptr).vector_len = vector.len() as u32;

                    let vector_ptr = (entry_ptr as *mut u8)
                        .add(std::mem::size_of::<ClusterQueueEntry>())
                        as *mut f32;
                    std::ptr::copy_nonoverlapping(vector.as_ptr(), vector_ptr, vector.len());

                    (*header)
                        .tail
                        .store(next_tail, std::sync::atomic::Ordering::Release);

                    pushed += 1;
                    break;
                } else {
                    pg_sys::ConditionVariableSleep(
                        cv as *const _ as *mut _,
                        pg_sys::PG_WAIT_EXTENSION,
                    );
                }
            }
        }

        if pushed > 0 {
            pg_sys::ConditionVariableBroadcast(cv as *const _ as *mut _);
        }

        pushed
    }

    /// Pop multiple vectors from the queue for a specific cluster in batch
    /// Returns the number of vectors popped, stored in the provided buffer
    pub unsafe fn pop_batch_from_queue(
        &self,
        base_ptr: *mut u8,
        cluster_id: usize,
        max_count: usize,
        heap_tids: &mut [pg_sys::ItemPointerData],
        vectors: &mut [Vec<f32>],
    ) -> usize {
        let header = self.get_header(base_ptr, cluster_id);
        let cv = self.get_cv(base_ptr, cluster_id);
        let mut popped = 0;

        while popped < max_count {
            let head = (*header).head.load(std::sync::atomic::Ordering::Acquire);
            let tail = (*header).tail.load(std::sync::atomic::Ordering::Acquire);

            if head == tail {
                break;
            }

            let entry_ptr = self.get_entry(base_ptr, cluster_id, head);
            let heap_tid = (*entry_ptr).heap_tid;
            let vector_len = (*entry_ptr).vector_len as usize;

            let next_head = (head + 1) % (*header).capacity;
            (*header)
                .head
                .store(next_head, std::sync::atomic::Ordering::Release);

            if heap_tid.ip_posid == 0
                || heap_tid.ip_posid == pg_sys::InvalidOffsetNumber
                || pgrx::itemptr::item_pointer_get_block_number_no_check(heap_tid)
                    == pg_sys::InvalidBlockNumber
            {
                continue;
            }

            let vector_ptr = (entry_ptr as *const u8).add(std::mem::size_of::<ClusterQueueEntry>())
                as *const f32;
            let vector_data = std::slice::from_raw_parts(vector_ptr, vector_len).to_vec();

            heap_tids[popped] = heap_tid;
            vectors[popped] = vector_data;
            popped += 1;
        }

        if popped > 0 {
            pg_sys::ConditionVariableBroadcast(cv as *const _ as *mut _);
        }

        popped
    }

    /// Get the number of available entries in the queue
    pub unsafe fn available_space(&self, base_ptr: *mut u8, cluster_id: usize) -> usize {
        let header = self.get_header(base_ptr, cluster_id);
        let head = (*header).head.load(std::sync::atomic::Ordering::Acquire);
        let tail = (*header).tail.load(std::sync::atomic::Ordering::Acquire);
        let capacity = (*header).capacity;

        if tail >= head {
            capacity - (tail - head) - 1
        } else {
            head - tail - 1
        }
    }

    /// Get the number of entries currently in the queue
    pub unsafe fn queue_size(&self, base_ptr: *mut u8, cluster_id: usize) -> usize {
        let header = self.get_header(base_ptr, cluster_id);
        let head = (*header).head.load(std::sync::atomic::Ordering::Acquire);
        let tail = (*header).tail.load(std::sync::atomic::Ordering::Acquire);
        let capacity = (*header).capacity;

        if tail >= head {
            tail - head
        } else {
            capacity - head + tail
        }
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
pub const DEFAULT_QUEUE_CAPACITY: usize = 10240;

/// Maximum number of workers supported
pub const MAX_WORKERS: usize = 64;

/// Worker assignment for dynamic load balancing
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct WorkerAssignment {
    pub cluster_id: usize,
    pub start_idx: usize,
    pub end_idx: usize,
    pub is_primary: bool,
}

impl WorkerAssignment {
    pub fn new(cluster_id: usize, start_idx: usize, end_idx: usize, is_primary: bool) -> Self {
        Self {
            cluster_id,
            start_idx,
            end_idx,
            is_primary,
        }
    }
}

/// Shared worker assignments for all workers
#[repr(C)]
pub struct WorkerAssignments {
    pub assignments: [WorkerAssignment; MAX_WORKERS],
    pub num_assignments: AtomicUsize,
    pub ready: AtomicBool,
}

impl WorkerAssignments {
    pub fn new() -> Self {
        Self {
            assignments: unsafe { std::mem::zeroed() },
            num_assignments: AtomicUsize::new(0),
            ready: AtomicBool::new(false),
        }
    }

    pub fn set_assignment(&mut self, worker_id: usize, assignment: WorkerAssignment) {
        if worker_id < MAX_WORKERS {
            self.assignments[worker_id] = assignment;
            let count = self.num_assignments.load(Ordering::Acquire);
            if worker_id >= count {
                self.num_assignments.store(worker_id + 1, Ordering::Release);
            }
        }
    }

    pub fn get_assignment(&self, worker_id: usize) -> Option<WorkerAssignment> {
        let count = self.num_assignments.load(Ordering::Acquire);
        if worker_id < count {
            Some(self.assignments[worker_id])
        } else {
            None
        }
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}

/// Cluster sizes tracking for dynamic worker allocation
#[repr(C)]
pub struct ClusterSizes {
    pub sizes: [AtomicUsize; 64],
    pub num_clusters: usize,
}

impl ClusterSizes {
    pub fn new(num_clusters: usize) -> Self {
        let mut sizes: [AtomicUsize; 64] = unsafe { std::mem::zeroed() };
        for i in 0..64 {
            sizes[i] = AtomicUsize::new(0);
        }
        Self {
            sizes,
            num_clusters: num_clusters.min(64),
        }
    }

    pub fn increment(&self, cluster_id: usize) {
        if cluster_id < self.num_clusters {
            self.sizes[cluster_id].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn get(&self, cluster_id: usize) -> usize {
        if cluster_id < self.num_clusters {
            self.sizes[cluster_id].load(Ordering::Relaxed)
        } else {
            0
        }
    }

    pub fn get_all(&self) -> Vec<usize> {
        (0..self.num_clusters)
            .map(|i| self.sizes[i].load(Ordering::Relaxed))
            .collect()
    }
}

/// Calculate worker assignments based on cluster sizes
pub fn calculate_worker_assignments(
    cluster_sizes: &[usize],
    num_workers: usize,
) -> Vec<WorkerAssignment> {
    let total_vectors: usize = cluster_sizes.iter().sum();
    if total_vectors == 0 || num_workers == 0 {
        return Vec::new();
    }

    let mut assignments = Vec::new();

    for (cluster_id, &size) in cluster_sizes.iter().enumerate() {
        if size == 0 {
            continue;
        }

        let workers_for_cluster = {
            let ideal = (size as f64 * num_workers as f64 / total_vectors as f64).ceil() as usize;
            ideal.max(1).min(size).min(num_workers - assignments.len())
        };

        if workers_for_cluster == 0 {
            continue;
        }

        let chunk_size = (size + workers_for_cluster - 1) / workers_for_cluster;

        for i in 0..workers_for_cluster {
            let start_idx = i * chunk_size;
            let end_idx = ((i + 1) * chunk_size).min(size);

            assignments.push(WorkerAssignment::new(
                cluster_id,
                start_idx,
                end_idx,
                i == 0,
            ));
        }

        if assignments.len() >= num_workers {
            break;
        }
    }

    assignments
}
