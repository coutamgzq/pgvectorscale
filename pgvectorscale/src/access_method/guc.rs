use pgrx::{pg_sys::AsPgCStr, *};

pub static TSV_QUERY_SEARCH_LIST_SIZE: GucSetting<i32> = GucSetting::<i32>::new(100);
pub static TSV_RESORT_SIZE: GucSetting<i32> = GucSetting::<i32>::new(50);
pub static TSV_PARALLEL_FLUSH_INTERVAL: GucSetting<f64> = GucSetting::<f64>::new(0.05);
pub static TSV_PARALLEL_INITIAL_START_NODES_COUNT: GucSetting<i32> = GucSetting::<i32>::new(1024);
pub static TSV_MIN_VECTORS_FOR_PARALLEL_BUILD: GucSetting<i32> = GucSetting::<i32>::new(65536);
pub static TSV_FORCE_PARALLEL_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(-1);
pub static TSV_NUM_CLUSTERS: GucSetting<i32> = GucSetting::<i32>::new(0);
pub static TSV_CLUSTERING_MAX_SAMPLE_SIZE: GucSetting<i32> = GucSetting::<i32>::new(100000);
pub static TSV_CLUSTERING_SAMPLE_THRESHOLD: GucSetting<i32> = GucSetting::<i32>::new(1000000);

pub fn init() {
    GucRegistry::define_int_guc(
        unsafe { std::ffi::CStr::from_ptr("diskann.query_search_list_size".as_pg_cstr()) },
        unsafe {
            std::ffi::CStr::from_ptr("The size of the search list used in queries".as_pg_cstr())
        },
        unsafe {
            std::ffi::CStr::from_ptr(
                "Higher value increases recall at the cost of speed.".as_pg_cstr(),
            )
        },
        &TSV_QUERY_SEARCH_LIST_SIZE,
        1,
        70000,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { std::ffi::CStr::from_ptr("diskann.query_rescore".as_pg_cstr()) },
        unsafe {
            std::ffi::CStr::from_ptr(
                "The number of elements rescored (0 to disable rescoring)".as_pg_cstr(),
            )
        },
        unsafe {
            std::ffi::CStr::from_ptr("Rescoring takes the query_rescore number of elements that have the smallest approximate distance, rescores them with the exact distance, returning the closest ones with the exact distance.".as_pg_cstr())
        },
        &TSV_RESORT_SIZE,
        0,
        1000,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        unsafe { std::ffi::CStr::from_ptr("diskann.parallel_flush_interval".as_pg_cstr()) },
        unsafe {
            std::ffi::CStr::from_ptr("The fraction of total vectors processed before flushing neighbor cache in parallel builds".as_pg_cstr())
        },
        unsafe {
            std::ffi::CStr::from_ptr("Controls how often the neighbor cache is flushed during parallel index builds as a fraction of total vectors (0.0-1.0).".as_pg_cstr())
        },
        &TSV_PARALLEL_FLUSH_INTERVAL,
        0.0,
        1.0,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe {
            std::ffi::CStr::from_ptr("diskann.parallel_initial_start_nodes_count".as_pg_cstr())
        },
        unsafe {
            std::ffi::CStr::from_ptr(
                "The number of initial start nodes to process before starting parallel workers"
                    .as_pg_cstr(),
            )
        },
        unsafe {
            std::ffi::CStr::from_ptr("Determines how many nodes the initializing worker processes before other workers begin. Affects parallel build coordination.".as_pg_cstr())
        },
        &TSV_PARALLEL_INITIAL_START_NODES_COUNT,
        1,
        10000,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { std::ffi::CStr::from_ptr("diskann.min_vectors_for_parallel_build".as_pg_cstr()) },
        unsafe {
            std::ffi::CStr::from_ptr(
                "Minimum number of vectors required to enable parallel building".as_pg_cstr(),
            )
        },
        unsafe {
            std::ffi::CStr::from_ptr("If the table has fewer vectors than this threshold, parallel building will be disabled and serial building will be used instead.".as_pg_cstr())
        },
        &TSV_MIN_VECTORS_FOR_PARALLEL_BUILD,
        1,
        i32::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { std::ffi::CStr::from_ptr("diskann.force_parallel_workers".as_pg_cstr()) },
        unsafe {
            std::ffi::CStr::from_ptr(
                "Force a specific number of parallel workers for index builds".as_pg_cstr(),
            )
        },
        unsafe {
            std::ffi::CStr::from_ptr("When set to a positive value, this overrides PostgreSQL's automatic worker count determination. Set to -1 to use automatic determination (default).".as_pg_cstr())
        },
        &TSV_FORCE_PARALLEL_WORKERS,
        -1,
        1024,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { std::ffi::CStr::from_ptr("diskann.num_clusters".as_pg_cstr()) },
        unsafe {
            std::ffi::CStr::from_ptr(
                "Number of clusters for k-means clustering during index build".as_pg_cstr(),
            )
        },
        unsafe {
            std::ffi::CStr::from_ptr("When set to > 0, uses k-means to partition vectors into clusters and builds separate indexes for each cluster. Set to 1 for standard single-index build.".as_pg_cstr())
        },
        &TSV_NUM_CLUSTERS,
        0,
        1024,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { std::ffi::CStr::from_ptr("diskann.clustering_max_sample_size".as_pg_cstr()) },
        unsafe {
            std::ffi::CStr::from_ptr(
                "Maximum number of vectors to sample for k-means clustering".as_pg_cstr(),
            )
        },
        unsafe {
            std::ffi::CStr::from_ptr("When the table size exceeds diskann.clustering_sample_threshold, only this many vectors will be sampled for k-means clustering. Set to 0 to disable sampling and use all vectors.".as_pg_cstr())
        },
        &TSV_CLUSTERING_MAX_SAMPLE_SIZE,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        unsafe { std::ffi::CStr::from_ptr("diskann.clustering_sample_threshold".as_pg_cstr()) },
        unsafe {
            std::ffi::CStr::from_ptr(
                "Threshold for enabling sampling during k-means clustering".as_pg_cstr(),
            )
        },
        unsafe {
            std::ffi::CStr::from_ptr("When the table size exceeds this threshold, sampling will be enabled for k-means clustering. Set to 0 to always enable sampling.".as_pg_cstr())
        },
        &TSV_CLUSTERING_SAMPLE_THRESHOLD,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
}
