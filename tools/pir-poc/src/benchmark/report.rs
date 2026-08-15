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
