use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct OptimizationReport {
    pub profile: String,
    pub methodology: &'static str,
    pub dimensions: Vec<OptimizationDimension>,
}

#[derive(Debug, Serialize)]
pub struct OptimizationDimension {
    pub bucket_count: usize,
    pub row_size: usize,
    pub snapshot_bytes: usize,
    pub query_share_bytes: usize,
    pub selected_rows: usize,
    pub scalar_kernels: Vec<OptimizationKernelResult>,
    pub persistent_indexes: Vec<OptimizationIndexResult>,
    pub batches: Vec<OptimizationBatchResult>,
}

#[derive(Debug, Serialize)]
pub struct OptimizationKernelResult {
    pub name: String,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub speedup_vs_masked: f64,
    pub estimated_data_bytes_read: usize,
    pub effective_gib_per_second: f64,
}

#[derive(Debug, Serialize)]
pub struct OptimizationIndexResult {
    pub group_bits: usize,
    pub build_ms: f64,
    pub index_bytes: usize,
    pub storage_amplification: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub speedup_vs_masked: f64,
    pub selected_combinations: usize,
    pub estimated_data_bytes_read: usize,
}

#[derive(Debug, Serialize)]
pub struct OptimizationBatchResult {
    pub batch_size: usize,
    pub kernels: Vec<OptimizationKernelResult>,
}

#[derive(Debug, Serialize)]
pub struct BenchmarkReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub target_server_counts: Vec<usize>,
    pub server_worker_threads: usize,
    pub methodology: Methodology,
    pub excluded_protocols: Vec<ProtocolExclusion>,
    pub dimensions: Vec<DimensionResult>,
    pub batches: Vec<BatchResult>,
    pub load: Vec<LoadResult>,
}

#[derive(Debug, Serialize)]
pub struct Methodology {
    pub privacy_model: &'static str,
    pub server_model: &'static str,
    pub public_baseline: &'static str,
    pub dimension_samples: &'static str,
    pub batch_samples: &'static str,
    pub load_samples: &'static str,
}

#[derive(Debug, Serialize)]
pub struct ProtocolExclusion {
    pub protocol: &'static str,
    pub reason: &'static str,
}

#[derive(Debug, Serialize)]
pub struct DimensionResult {
    pub bucket_count: usize,
    pub row_size: usize,
    pub snapshot_bytes: usize,
    pub snapshot_build_ms: f64,
    pub process_rss_bytes: Option<usize>,
    pub public_query: PublicQueryResult,
    pub isolated_server: IsolatedServerResult,
    pub topologies: Vec<TopologyResult>,
}

#[derive(Debug, Serialize)]
pub struct PublicQueryResult {
    pub request_bytes: usize,
    pub answer_bytes: usize,
    pub client_request_generation_us: f64,
    pub server_p50_ms: f64,
    pub server_p95_ms: f64,
    pub server_p99_ms: f64,
}

#[derive(Debug, Serialize)]
pub struct IsolatedServerResult {
    pub cold_ms: f64,
    pub warm_p50_ms: f64,
    pub warm_p95_ms: f64,
    pub warm_p99_ms: f64,
    pub throughput_gib_s: f64,
}

#[derive(Debug, Serialize)]
pub struct TopologyResult {
    pub server_count: usize,
    pub privacy_collusion_tolerance: usize,
    pub required_answers: usize,
    pub query_share_bytes_per_server: usize,
    pub total_query_bytes: usize,
    pub answer_share_bytes_per_server: usize,
    pub total_answer_bytes: usize,
    pub client_query_generation_us: f64,
    pub co_located_wall_p50_ms: f64,
    pub co_located_wall_p95_ms: f64,
    pub co_located_wall_p99_ms: f64,
    pub sum_server_elapsed_p50_ms: f64,
    pub client_combine_p50_us: f64,
}

#[derive(Debug, Serialize)]
pub struct BatchResult {
    pub server_count: usize,
    pub privacy_collusion_tolerance: usize,
    pub required_answers: usize,
    pub batch_size: usize,
    pub bucket_count: usize,
    pub row_size: usize,
    pub snapshot_bytes: usize,
    pub total_query_bytes: usize,
    pub total_answer_bytes: usize,
    pub client_query_generation_p50_us: f64,
    pub client_query_generation_per_item_p50_us: f64,
    pub co_located_wall_p50_ms: f64,
    pub co_located_wall_p95_ms: f64,
    pub sum_server_elapsed_p50_ms: f64,
    pub client_combine_p50_us: f64,
    pub logical_queries_per_second: f64,
}

#[derive(Debug, Serialize)]
pub struct LoadResult {
    pub server_count: usize,
    pub clients: usize,
    pub bucket_count: usize,
    pub row_size: usize,
    pub operations: usize,
    pub elapsed_ms: f64,
    pub logical_queries_per_second: f64,
    pub end_to_end_p50_ms: f64,
    pub end_to_end_p95_ms: f64,
    pub end_to_end_p99_ms: f64,
}

#[derive(Debug, Serialize)]
pub struct SinglePassBenchmarkReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: &'static str,
    pub dimensions: Vec<SinglePassDimensionResult>,
}

#[derive(Debug, Serialize)]
pub struct SinglePassDimensionResult {
    pub bucket_count: usize,
    pub row_size: usize,
    pub snapshot_bytes: usize,
    pub snapshot_build_ms: f64,
    pub dense: DenseComparisonResult,
    pub single_pass: Vec<SinglePassVariantResult>,
}

#[derive(Debug, Serialize)]
pub struct DenseComparisonResult {
    pub samples: usize,
    pub expected_rows_read_per_server: usize,
    pub expected_data_bytes_read_per_server: usize,
    pub query_bytes_per_server: usize,
    pub answer_bytes_per_server: usize,
    pub client_query_generation_p50_us: f64,
    pub co_located_wall_p50_ms: f64,
    pub co_located_wall_p95_ms: f64,
    pub sum_server_elapsed_p50_ms: f64,
    pub client_reconstruct_p50_us: f64,
}

#[derive(Debug, Serialize)]
pub struct SinglePassVariantResult {
    pub partition_count_q: usize,
    pub samples: usize,
    pub setup_ms: f64,
    pub client_state_bytes: usize,
    pub client_hint_bytes: usize,
    pub client_permutation_bytes: usize,
    pub client_state_to_snapshot_ratio: f64,
    pub rows_read_per_server: usize,
    pub data_bytes_read_per_server: usize,
    pub query_bytes_per_server: usize,
    pub answer_bytes_per_server: usize,
    pub client_query_generation_p50_us: f64,
    pub co_located_wall_p50_ms: f64,
    pub co_located_wall_p95_ms: f64,
    pub sum_server_elapsed_p50_ms: f64,
    pub client_reconstruct_p50_us: f64,
    pub wall_speedup_vs_dense: f64,
    pub wall_time_reduction_percent: f64,
    pub server_time_speedup_vs_dense: f64,
    pub server_time_reduction_percent: f64,
    pub server_row_access_reduction_factor: f64,
    pub total_query_byte_reduction_factor: f64,
}

#[derive(Debug, Serialize)]
pub struct ColdPathBenchmarkReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: &'static str,
    pub workload: ColdPathWorkload,
    pub legacy_paged_layout: LegacyPagedLayoutResult,
    pub packed_dense: PackedDenseResult,
    pub finite_differences: Vec<FiniteDifferencesResult>,
    pub indexed_decoys: IndexedDecoyResult,
}

#[derive(Debug, Serialize)]
pub struct ColdPathWorkload {
    pub document_count: usize,
    pub distinct_tag_count: usize,
    pub documents_per_tag: usize,
    pub values_per_page: usize,
    pub locator_bytes: usize,
    pub decoy_count: usize,
}

#[derive(Debug, Serialize)]
pub struct LegacyPagedLayoutResult {
    pub description: &'static str,
    pub bucket_count: usize,
    pub bucket_capacity: usize,
    pub row_size: usize,
    pub estimated_snapshot_bytes: usize,
    pub query_bytes_per_server_per_page: usize,
    pub note: &'static str,
}

#[derive(Debug, Serialize)]
pub struct PackedDenseResult {
    pub privacy: &'static str,
    pub build_ms: f64,
    pub distinct_tag_count: usize,
    pub page_count: usize,
    pub bucket_count: usize,
    pub bucket_capacity: usize,
    pub table_load_factor: f64,
    pub page_size: usize,
    pub row_size: usize,
    pub snapshot_bytes_per_server: usize,
    pub cold_client_metadata_bytes: usize,
    pub candidate_bucket_queries_per_tag_page: usize,
    pub expected_rows_processed_per_server: usize,
    pub expected_data_bytes_processed_per_server: usize,
    pub query_bytes_per_server: usize,
    pub response_bytes_per_server: usize,
    pub client_query_generation_p50_us: f64,
    pub co_located_wall_p50_ms: f64,
    pub co_located_wall_p95_ms: f64,
    pub sum_server_elapsed_p50_ms: f64,
    pub client_reconstruct_p50_us: f64,
}

#[derive(Debug, Serialize)]
pub struct FiniteDifferencesResult {
    pub privacy: &'static str,
    pub variables_m: usize,
    pub total_degree_d: usize,
    pub record_capacity: usize,
    pub encoded_storage_bytes_per_server: usize,
    pub storage_amplification_vs_packed_dense: f64,
    pub cloud_rows_per_server_per_candidate: usize,
    pub rows_processed_per_server_per_tag_page: usize,
    pub data_bytes_processed_per_server_per_tag_page: usize,
    pub query_bytes_per_server: usize,
    pub response_bytes_per_server: usize,
    pub measured: bool,
    pub preprocessing_ms: Option<f64>,
    pub client_query_generation_p50_us: Option<f64>,
    pub co_located_wall_p50_ms: Option<f64>,
    pub co_located_wall_p95_ms: Option<f64>,
    pub sum_server_elapsed_p50_ms: Option<f64>,
    pub client_reconstruct_p50_ms: Option<f64>,
    pub note: &'static str,
}

#[derive(Debug, Serialize)]
pub struct IndexedDecoyResult {
    pub privacy: &'static str,
    pub decoy_count: usize,
    pub server_count: usize,
    pub query_bytes: usize,
    pub padded_response_bytes: usize,
    pub client_query_generation_p50_us: f64,
    pub server_lookup_p50_ms: f64,
    pub server_lookup_p95_ms: f64,
    pub client_select_p50_us: f64,
    pub note: &'static str,
}

#[derive(Debug, Serialize)]
pub struct FuseComparisonReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: &'static str,
    pub workload: FuseComparisonWorkload,
    pub encoded_corpus_build_ms: f64,
    pub encoded_corpus_tracked_bytes: usize,
    pub layouts: Vec<FuseLayoutResult>,
    pub raid_pir: RaidPirAssessment,
}

#[derive(Debug, Serialize)]
pub struct FuseComparisonWorkload {
    pub document_count: usize,
    pub distinct_tag_count: usize,
    pub documents_per_tag: usize,
    pub encoded_page_count: usize,
    pub values_per_page: usize,
    pub locator_bytes: usize,
    pub page_bytes: usize,
    pub selected_page: usize,
}

#[derive(Debug, Serialize)]
pub struct FuseLayoutResult {
    pub layout: &'static str,
    pub retrieval_cells_or_candidates: usize,
    pub table_rows: usize,
    pub row_bytes: usize,
    pub table_bytes_per_server: usize,
    pub storage_expansion_vs_encoded_pages: f64,
    pub cold_client_metadata_bytes: usize,
    pub layout_build_ms: f64,
    pub build_attempts: usize,
    pub peak_tracked_build_bytes: usize,
    pub peak_build_memory_note: &'static str,
    pub topologies: Vec<FuseTopologyResult>,
}

#[derive(Debug, Serialize)]
pub struct FuseTopologyResult {
    pub server_count: usize,
    pub privacy_collusion_tolerance: usize,
    pub required_answers: usize,
    pub dense_evaluations_per_server: usize,
    pub expected_rows_xored_per_server: usize,
    pub expected_data_bytes_xored_per_server: usize,
    pub query_bytes_per_server: usize,
    pub total_client_upload_bytes: usize,
    pub response_bytes_per_server: usize,
    pub total_client_download_bytes: usize,
    pub client_query_generation_p50_us: f64,
    pub co_located_wall_p50_ms: f64,
    pub co_located_wall_p95_ms: f64,
    pub sum_server_elapsed_p50_ms: f64,
    pub client_reconstruct_p50_us: f64,
}

#[derive(Debug, Serialize)]
pub struct RaidPirAssessment {
    pub reference: &'static str,
    pub status: &'static str,
    pub scope: &'static str,
    pub potential_benefit: &'static str,
    pub incompatibility: &'static str,
    pub availability_note: &'static str,
    pub recommendation: &'static str,
    pub configurations: Vec<RaidPirConfiguration>,
}

#[derive(Debug, Serialize)]
pub struct RaidPirConfiguration {
    pub server_count_k: usize,
    pub redundancy_r: usize,
    pub maximum_colluding_servers: usize,
    pub table_fraction_per_server: f64,
    pub query_fraction_per_server: f64,
    pub fuse_4_table_bytes_per_server: usize,
    pub note: &'static str,
}

pub(super) fn methodology() -> Methodology {
    Methodology {
        privacy_model: "n XOR shares; any n-1 query shares reveal no row; all n answers are required",
        server_model: "isolated scans use one thread; topology, batch, and load tests give each persistent co-located server a bounded two-worker pool, share one memory bus, and exclude HTTP, TLS, and network latency",
        public_baseline: "same immutable snapshot with no privacy: the server hashes a public key, reads one bucket, and copies the response row; excludes DefraDB GraphQL, HTTP, TLS, and network latency",
        dimension_samples: "100 client share generations; one cold server scan; 3-11 warm/topology evaluations with fresh shares depending on profile and snapshot size",
        batch_samples: "3 quick or 7 full samples; every sample generates fresh shares and each server's bounded pool evaluates independent batch items concurrently",
        load_samples: "one persistent coordinator and two evaluator workers per server; 1, 8, or 32 concurrent clients generate fresh shares and wait for every answer",
    }
}

pub(super) fn excluded_protocols() -> Vec<ProtocolExclusion> {
    vec![
        ProtocolExclusion {
            protocol: "compact-dpf",
            reason: "At 262K 64-byte rows it reduced upload from 32 KiB to 357 B per server but took 11-13 ms/server versus 1.67 ms for Dense XOR; the standard construction is two-party and adding servers requires a different multiparty DPF.",
        },
        ProtocolExclusion {
            protocol: "chalamet-pir",
            reason: "The tested ChalametPIR 0.8 implementation is not phone-viable at the target scale: its client materializes a public matrix that extrapolates to roughly 7.8 GiB at 1M records, before normal app memory. Its compact response and fast server are attractive, but need a substantially different, streaming/mobile implementation.",
        },
        ProtocolExclusion {
            protocol: "path-oram",
            reason: "ORAM hides a mutable sequence of reads and writes by maintaining a client position map and stash and reading, rewriting, and reshuffling tree paths. That solves a broader stateful problem than immutable point retrieval and is not an apples-to-apples snapshot benchmark.",
        },
        ProtocolExclusion {
            protocol: "tee-plus-oram",
            reason: "A TEE alone does not hide host-observable memory access patterns, while adding ORAM retains its state and path-rewrite costs. It also adds hardware trust, attestation, side-channel review, and hardware-specific deployment, conflicting with a server-agnostic edge POC.",
        },
    ]
}
