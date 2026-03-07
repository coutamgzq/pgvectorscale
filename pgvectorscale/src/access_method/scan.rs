use std::collections::BinaryHeap;

use pgrx::{pg_sys::InvalidOffsetNumber, *};

use crate::{
    access_method::{
        graph::neighbor_store::GraphNeighborStore, labels::LabeledVector, meta_page::MetaPage,
        sbq::storage::SbqSpeedupStorage,
    },
    util::{
        buffer::PinnedBufferShare, ports::pgstat_count_index_scan, HeapPointer, IndexPointer,
        ItemPointer,
    },
};

use super::{
    distance::DistanceFn,
    graph::{Graph, ListSearchResult},
    labels::LabelSetView,
    plain::{
        storage::{PlainStorage, PlainStorageLsnPrivateData},
        PlainDistanceMeasure,
    },
    sbq::{
        quantize::SbqQuantizer, storage::SbqSpeedupStorageLsnPrivateData, SbqMeans,
        SbqSearchDistanceMeasure,
    },
    stats::QuantizerStats,
    storage::{Storage, StorageType},
};

/* Be very careful not to transfer PgRelations in the state, as they can change between calls. That means we shouldn't be
using lifetimes here. Everything should be owned */
enum StorageState {
    SbqSpeedup(
        SbqQuantizer,
        TSVResponseIterator<SbqSearchDistanceMeasure, SbqSpeedupStorageLsnPrivateData>,
    ),
    Plain(TSVResponseIterator<PlainDistanceMeasure, PlainStorageLsnPrivateData>),
}

/* no lifetime usage here. */
struct TSVScanState {
    storage: *mut StorageState,
    distance_fn: Option<DistanceFn>,
    meta_page: MetaPage,
    last_buffer: Option<PinnedBufferShare>,
}

impl TSVScanState {
    fn new(meta_page: MetaPage) -> Self {
        Self {
            storage: std::ptr::null_mut(),
            distance_fn: None,
            meta_page,
            last_buffer: None,
        }
    }

    fn initialize(
        &mut self,
        index: &PgRelation,
        heap: &PgRelation,
        query: LabeledVector,
        search_list_size: usize,
    ) {
        let meta_page = MetaPage::fetch(index);
        let storage = meta_page.get_storage_type();
        let distance = meta_page.get_distance_function();

        let store_type = match storage {
            StorageType::Plain => {
                let stats = QuantizerStats::default();
                let bq = PlainStorage::load_for_search(index, heap, &meta_page);
                let it =
                    TSVResponseIterator::new(&bq, index, query, search_list_size, meta_page, stats);
                StorageState::Plain(it)
            }
            StorageType::SbqCompression => {
                let mut stats = QuantizerStats::default();
                let quantizer = unsafe { SbqMeans::load(index, &meta_page, &mut stats) };
                let bq = SbqSpeedupStorage::load_for_search(index, heap, &quantizer, &meta_page);
                let it =
                    TSVResponseIterator::new(&bq, index, query, search_list_size, meta_page, stats);
                StorageState::SbqSpeedup(quantizer, it)
            }
        };

        self.storage = PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(store_type);
        self.distance_fn = Some(distance);
    }
}

struct ResortData {
    heap_pointer: HeapPointer,
    index_pointer: IndexPointer,
    distance: f32,
}

impl PartialEq for ResortData {
    fn eq(&self, other: &Self) -> bool {
        self.heap_pointer == other.heap_pointer
    }
}

impl PartialOrd for ResortData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for ResortData {}

impl Ord for ResortData {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        //notice the reverse here. Other is the one that is being compared to self
        //this allows us to have a min heap
        other.distance.total_cmp(&self.distance)
    }
}

struct StreamingStats {
    count: i32,
    mean: f32,
    m2: f32,
    max_distance: f32,
}

impl StreamingStats {
    fn new(_resort_size: usize) -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            max_distance: 0.0,
        }
    }

    fn update_base_stats(&mut self, distance: f32) {
        if distance == 0.0 {
            return;
        }
        self.count += 1;
        let delta = distance - self.mean;
        self.mean += delta / self.count as f32;
        let delta2 = distance - self.mean;
        self.m2 += delta * delta2;
    }

    #[allow(dead_code)]
    fn variance(&self) -> f32 {
        if self.count < 2 {
            return 0.0;
        }
        self.m2 / (self.count - 1) as f32
    }

    fn update(&mut self, distance: f32, diff: f32) {
        //base stats only on first resort_size elements
        self.update_base_stats(diff);
        self.max_distance = self.max_distance.max(distance);
    }
}

/// Result from searching a single cluster
#[derive(Clone, Debug)]
struct ClusterSearchResult {
    heap_pointer: HeapPointer,
    index_pointer: IndexPointer,
    distance: f32,
}

impl PartialEq for ClusterSearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.heap_pointer == other.heap_pointer
    }
}

impl Eq for ClusterSearchResult {}

impl PartialOrd for ClusterSearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ClusterSearchResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Min heap: smaller distance has higher priority
        other.distance.total_cmp(&self.distance)
    }
}

struct TSVResponseIterator<QDM, PD> {
    lsr: ListSearchResult<QDM, PD>,
    search_list_size: usize,
    meta_page: MetaPage,
    quantizer_stats: QuantizerStats,
    resort_size: usize,
    resort_buffer: BinaryHeap<ResortData>,
    streaming_stats: StreamingStats,
    next_calls: i32,
    next_calls_with_resort: i32,
    full_distance_comparisons: i32,
    has_label_filter: bool,
    // Cluster mode fields
    cluster_results: BinaryHeap<ClusterSearchResult>,
    is_cluster_mode: bool,
}

impl<QDM: Clone, PD> TSVResponseIterator<QDM, PD> {
    fn new<S: Storage<QueryDistanceMeasure = QDM, LSNPrivateData = PD>>(
        storage: &S,
        index: &PgRelation,
        query: LabeledVector,
        search_list_size: usize,
        //FIXME?
        _meta_page: MetaPage,
        quantizer_stats: QuantizerStats,
    ) -> Self {
        let mut meta_page = MetaPage::fetch(index);
        let has_label_filter = query.labels().is_some_and(|labels| !labels.is_empty());
        let resort_size = super::guc::TSV_RESORT_SIZE.get() as usize;

        // Check if we're in cluster mode (no start_nodes means cluster build)
        let is_cluster_mode = meta_page.get_start_nodes().is_none();

        if is_cluster_mode {
            // Cluster mode: search each cluster independently and merge results
            let cluster_results = Self::search_all_clusters(
                storage,
                &mut meta_page,
                query,
                search_list_size,
                !has_label_filter,
            );

            Self {
                search_list_size,
                lsr: ListSearchResult::empty(),
                meta_page,
                quantizer_stats,
                resort_size,
                resort_buffer: BinaryHeap::with_capacity(resort_size),
                streaming_stats: StreamingStats::new(resort_size),
                next_calls: 0,
                next_calls_with_resort: 0,
                full_distance_comparisons: 0,
                has_label_filter,
                cluster_results,
                is_cluster_mode: true,
            }
        } else {
            // Non-cluster mode: use original streaming search
            let mut graph = Graph::new(GraphNeighborStore::Disk, &mut meta_page);
            let lsr = graph.greedy_search_streaming_init(query, search_list_size, storage);

            Self {
                search_list_size,
                lsr,
                meta_page,
                quantizer_stats,
                resort_size,
                resort_buffer: BinaryHeap::with_capacity(resort_size),
                streaming_stats: StreamingStats::new(resort_size),
                next_calls: 0,
                next_calls_with_resort: 0,
                full_distance_comparisons: 0,
                has_label_filter,
                cluster_results: BinaryHeap::new(),
                is_cluster_mode: false,
            }
        }
    }

    /// Search all clusters independently and merge results into a single priority queue
    fn search_all_clusters<S: Storage>(
        storage: &S,
        meta_page: &mut MetaPage,
        query: LabeledVector,
        search_list_size: usize,
        no_filter: bool,
    ) -> BinaryHeap<ClusterSearchResult>
    where
        S::QueryDistanceMeasure: Clone,
    {
        let cluster_start_nodes = meta_page.get_all_cluster_start_nodes();
        let num_neighbors = meta_page.get_num_neighbors();
        let queue_size = super::guc::TSV_CLUSTER_SEARCH_QUEUE_SIZE.get() as usize;

        // Pre-collect start nodes to avoid borrow issues
        let start_nodes_vec: Vec<(u32, ItemPointer)> = cluster_start_nodes
            .iter()
            .map(|(&id, &node)| (id, node))
            .collect();

        let mut all_results: BinaryHeap<ClusterSearchResult> = BinaryHeap::new();
        let mut seen_nodes: std::collections::HashSet<ItemPointer> =
            std::collections::HashSet::new();

        // Calculate results per cluster to distribute queue size evenly
        let num_clusters = start_nodes_vec.len().max(1);
        let results_per_cluster = (queue_size / num_clusters).max(10); // At least 10 per cluster

        for (_cluster_id, start_node) in start_nodes_vec {
            // Create a fresh distance measure for each cluster search
            let dm = storage.get_query_distance_measure(query.clone());

            // Create ListSearchResult for this cluster
            let mut lsr = ListSearchResult::new(
                vec![start_node],
                dm,
                None,
                search_list_size,
                num_neighbors,
                &mut GraphNeighborStore::Disk,
                storage,
            );

            let mut graph = Graph::new(GraphNeighborStore::Disk, meta_page);
            let mut cluster_result_count = 0;

            // Search this cluster: repeatedly call greedy_search_iterate and consume results
            // This mimics the behavior of the original next() method
            loop {
                // Expand more nodes in the graph
                graph.greedy_search_iterate(&mut lsr, search_list_size, no_filter, None, storage);

                // Consume one result at a time (like the original next() method)
                match lsr.consume_with_distance(storage) {
                    Some((heap_pointer, index_pointer, distance)) => {
                        // Skip deleted tuples
                        if heap_pointer.offset == InvalidOffsetNumber {
                            continue;
                        }

                        // Deduplicate: skip if we've seen this node before
                        if !seen_nodes.insert(index_pointer) {
                            continue;
                        }

                        all_results.push(ClusterSearchResult {
                            heap_pointer,
                            index_pointer,
                            distance,
                        });

                        cluster_result_count += 1;

                        // Check if we've reached the per-cluster limit or total queue size limit
                        if cluster_result_count >= results_per_cluster
                            || all_results.len() >= queue_size
                        {
                            break;
                        }
                    }
                    None => {
                        // No more results available, need to expand more
                        if lsr.is_empty() {
                            // Truly exhausted
                            break;
                        }
                        // Continue to next iteration to expand more nodes
                        continue;
                    }
                }
            }

            if all_results.len() >= queue_size {
                break;
            }
        }

        all_results
    }
}

impl<QDM, PD> TSVResponseIterator<QDM, PD> {
    fn next<S: Storage<QueryDistanceMeasure = QDM, LSNPrivateData = PD>>(
        &mut self,
        _storage: &S,
    ) -> Option<(HeapPointer, IndexPointer)> {
        self.next_calls += 1;

        // Cluster mode: return results from the pre-computed queue
        if self.is_cluster_mode {
            return self
                .cluster_results
                .pop()
                .map(|r| (r.heap_pointer, r.index_pointer));
        }

        // Non-cluster mode: use original streaming search
        let mut graph = Graph::new(GraphNeighborStore::Disk, &mut self.meta_page);

        /* Iterate until we find a non-deleted tuple */
        loop {
            graph.greedy_search_iterate(
                &mut self.lsr,
                self.search_list_size,
                !self.has_label_filter,
                None,
                _storage,
            );

            let item = self.lsr.consume(_storage);

            match item {
                Some((heap_pointer, index_pointer)) => {
                    if heap_pointer.offset == InvalidOffsetNumber {
                        /* deleted tuple */
                        continue;
                    }
                    return Some((heap_pointer, index_pointer));
                }
                None => {
                    return None;
                }
            }
        }
    }

    fn next_with_resort<S: Storage<QueryDistanceMeasure = QDM, LSNPrivateData = PD>>(
        &mut self,
        scan: &PgBox<pg_sys::IndexScanDescData>,
        _index: &PgRelation,
        storage: &S,
    ) -> Option<(HeapPointer, IndexPointer)> {
        self.next_calls_with_resort += 1;

        // Cluster mode: results already have distances, just return from queue
        if self.is_cluster_mode {
            return self
                .cluster_results
                .pop()
                .map(|r| (r.heap_pointer, r.index_pointer));
        }

        if self.resort_buffer.capacity() == 0 {
            return self.next(storage);
        }

        while self.resort_buffer.len() < self.resort_size {
            match self.next(storage) {
                Some((heap_pointer, index_pointer)) => {
                    self.full_distance_comparisons += 1;
                    let distance = storage.get_full_distance_for_resort(
                        scan,
                        self.lsr.sdm.as_ref().unwrap(),
                        index_pointer,
                        heap_pointer,
                        &self.meta_page,
                        &mut self.lsr.stats,
                    );

                    match distance {
                        None => {
                            /* No entry found in heap */
                            continue;
                        }
                        Some(distance) => {
                            if self.resort_buffer.len() > 1 {
                                self.streaming_stats
                                    .update(distance, distance - self.streaming_stats.max_distance);
                            }

                            self.resort_buffer.push(ResortData {
                                heap_pointer,
                                index_pointer,
                                distance,
                            });
                        }
                    }
                }
                None => {
                    break;
                }
            }
        }

        /*error!(
            "Resort buffer size: {}, mean: {}, variance: {}, max_distance: {}: diff: {}",
            self.resort_buffer.len(),
            self.streaming_stats.mean(),
            self.streaming_stats.variance().sqrt(),
            self.streaming_stats.max_distance,
            self.streaming_stats.max_distance - self.resort_buffer.peek().unwrap().distance
        );*/

        self.resort_buffer
            .pop()
            .map(|rd| (rd.heap_pointer, rd.index_pointer))
    }
}

#[pg_guard]
pub extern "C-unwind" fn ambeginscan(
    index_relation: pg_sys::Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    let mut scandesc: PgBox<pg_sys::IndexScanDescData> = unsafe {
        PgBox::from_pg(pg_sys::RelationGetIndexScan(
            index_relation,
            nkeys,
            norderbys,
        ))
    };
    let indexrel = unsafe { PgRelation::from_pg(index_relation) };
    let meta_page = MetaPage::fetch(&indexrel);

    unsafe {
        pgstat_count_index_scan(index_relation, indexrel);
    }

    let state: TSVScanState = TSVScanState::new(meta_page);
    scandesc.opaque =
        PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(state) as void_mut_ptr;

    scandesc.into_pg()
}

#[pg_guard]
pub extern "C-unwind" fn amrescan(
    scan: pg_sys::IndexScanDesc,
    keys: pg_sys::ScanKey,
    nkeys: ::std::os::raw::c_int,
    orderbys: pg_sys::ScanKey,
    norderbys: ::std::os::raw::c_int,
) {
    assert_eq!(norderbys, 1, "Expected a single order-by key");
    assert!(nkeys == 0 || nkeys == 1, "Expected 0 or 1 keys");

    let mut scan: PgBox<pg_sys::IndexScanDescData> = unsafe { PgBox::from_pg(scan) };
    let indexrel = unsafe { PgRelation::from_pg(scan.indexRelation) };
    let heaprel = unsafe { PgRelation::from_pg(scan.heapRelation) };

    if nkeys > 0 {
        scan.xs_recheck = true;
    }

    let orderby_keys = unsafe {
        std::slice::from_raw_parts(orderbys as *const pg_sys::ScanKeyData, norderbys as _)
    };
    let keys =
        unsafe { std::slice::from_raw_parts(keys as *const pg_sys::ScanKeyData, nkeys as _) };

    let search_list_size = super::guc::TSV_QUERY_SEARCH_LIST_SIZE.get() as usize;

    let state = unsafe { (scan.opaque as *mut TSVScanState).as_mut() }.expect("no scandesc state");

    let query = unsafe { LabeledVector::from_scan_key_data(keys, orderby_keys, &state.meta_page) };

    state.initialize(&indexrel, &heaprel, query, search_list_size);
}

#[pg_guard]
pub extern "C-unwind" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection::Type,
) -> bool {
    let scan: PgBox<pg_sys::IndexScanDescData> = unsafe { PgBox::from_pg(scan) };
    let state = unsafe { (scan.opaque as *mut TSVScanState).as_mut() }.expect("no scandesc state");

    let indexrel = unsafe { PgRelation::from_pg(scan.indexRelation) };
    let heaprel = unsafe { PgRelation::from_pg(scan.heapRelation) };

    let mut storage = unsafe { state.storage.as_mut() }.expect("no storage in state");
    match &mut storage {
        StorageState::SbqSpeedup(quantizer, iter) => {
            let bq = SbqSpeedupStorage::load_for_search(
                &indexrel,
                &heaprel,
                quantizer,
                &state.meta_page,
            );
            let next = iter.next_with_resort(&scan, &indexrel, &bq);
            get_tuple(state, next, scan)
        }
        StorageState::Plain(iter) => {
            let storage = PlainStorage::load_for_search(&indexrel, &heaprel, &state.meta_page);
            let next = if state.meta_page.get_num_dimensions()
                == state.meta_page.get_num_dimensions_to_index()
            {
                /* no need to resort */
                iter.next(&storage)
            } else {
                iter.next_with_resort(&scan, &indexrel, &storage)
            };
            get_tuple(state, next, scan)
        }
    }
}

fn get_tuple(
    state: &mut TSVScanState,
    next: Option<(HeapPointer, IndexPointer)>,
    mut scan: PgBox<pg_sys::IndexScanDescData>,
) -> bool {
    scan.xs_recheckorderby = false;
    match next {
        Some((heap_pointer, index_pointer)) => {
            let tid_to_set = &mut scan.xs_heaptid;
            heap_pointer.to_item_pointer_data(tid_to_set);

            /*
             * An index scan must maintain a pin on the index page holding the
             * item last returned by amgettuple
             *
             * https://www.postgresql.org/docs/current/index-locking.html
             */
            let indexrel = unsafe { PgRelation::from_pg(scan.indexRelation) };
            state.last_buffer = Some(PinnedBufferShare::read(
                &indexrel,
                index_pointer.block_number,
            ));
            true
        }
        None => {
            state.last_buffer = None;
            false
        }
    }
}

#[pg_guard]
pub extern "C-unwind" fn amendscan(scan: pg_sys::IndexScanDesc) {
    let min_level = unsafe {
        let l = pg_sys::log_min_messages;
        let c = pg_sys::client_min_messages;
        std::cmp::min(l, c)
    };
    if min_level <= pg_sys::DEBUG1 as _ {
        let scan: PgBox<pg_sys::IndexScanDescData> = unsafe { PgBox::from_pg(scan) };
        let state =
            unsafe { (scan.opaque as *mut TSVScanState).as_mut() }.expect("no scandesc state");

        let mut storage = unsafe { state.storage.as_mut() }.expect("no storage in state");
        match &mut storage {
            StorageState::SbqSpeedup(_bq, iter) => end_scan::<SbqSpeedupStorage>(iter),
            StorageState::Plain(iter) => end_scan::<PlainStorage>(iter),
        }
    }
}

fn end_scan<S: Storage>(
    iter: &mut TSVResponseIterator<S::QueryDistanceMeasure, S::LSNPrivateData>,
) {
    debug1!(
        "Query stats - reads_index={} reads_heap={} d_total={} d_quantized={} d_full={} next={} resort={} visits={} candidate={}",
        iter.lsr.stats.get_node_reads(),
        iter.lsr.stats.get_node_heap_reads(),
        iter.lsr.stats.get_total_distance_comparisons(),
        iter.lsr.stats.get_quantized_distance_comparisons(),
        iter.full_distance_comparisons,
        iter.next_calls,
        iter.next_calls_with_resort,
        iter.lsr.stats.get_visited_nodes(),
        iter.lsr.stats.get_candidate_nodes(),
    );

    debug_assert_eq!(iter.quantizer_stats.node_reads, 1);
    debug_assert_eq!(iter.quantizer_stats.node_writes, 0);
}
