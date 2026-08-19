//! Common-corpus warm/stateful snapshot benchmark.
//!
//! Unlike the older synthetic SinglePass benchmark, this module runs both
//! protocols over the exact compact MPHF tag-page table used by `bench-mphf`.
//! It deliberately keeps phase CPU, logical table work, and network bytes in
//! separate fields: adding those unlike units would hide the real cold/warm
//! crossover.

use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;

use super::accounting::{
    direct_ratio, unavailable_hardware_counters, AggregateWorkReport, AmortizationHorizon,
    ComparisonScope, DirectComparison, LeakageScope, Metric, PhaseWork, SecurityLabels,
};
use super::{micros, millis, percentile, Profile};
use crate::dense;
use crate::mphf_pages::MphfPageSnapshot;
use crate::single_pass::{
    self, ClientState, ServerAnswer, ServerQuery, GENERATION_ID_BYTES, SERVER_COUNT,
};
use crate::snapshot::SnapshotView;
use crate::tag_pages::{benchmark_page_set, benchmark_tag, TagPageConfig};

const DOCUMENT_COUNT: usize = 1 << 20;
const DISTINCT_TAG_COUNT: usize = 1 << 18;
const QUERIES_PER_CLIENT: [usize; 5] = [1, 2, 10, 100, 1_000];
const CLIENTS_PER_GENERATION: [usize; 3] = [1, 1_000, 1_000_000];
const PARTITION_COUNTS: [usize; 5] = [2, 4, 8, 16, 32];

const METHODOLOGY: &str = "Both paths query the identical populated 1,048,576-document, 262,144-row, 96-byte exact-MPHF table with two non-colluding replicas and return the first page of four 16-byte locators. Dense is stateless and scans one random selector share on each server. SinglePass faithfully uses the existing two-role construction: a client downloads/scans the full table once per immutable generation, derives private permutations and parity hints locally, then each online server copies partition-count Q indexed rows. Each 1/2/10/100/1000-query lifetime is executed directly from fresh client state; these are sequential state mutations, not a server batch. The benchmark is in-process and excludes TLS, network latency, transport-serving CPU, filesystem I/O, allocator metadata, hardware byte counters, and energy.";

const SCOPE_WORKLOAD: &str =
    "one first-page tag lookup over the identical populated 1,048,576-document, 262,144-row, 96-byte exact-MPHF table";
const SCOPE_RESULT: &str = "one first page containing four 16-byte compact locators";
const SCOPE_PARTITION: &str =
    "global immutable snapshot; generation-specific exact MPHF metadata is public";
const PRIVACY: &str =
    "exact information-theoretic query privacy with public generation-specific MPHF metadata";

#[derive(Debug, Serialize)]
pub struct WarmStatefulReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: &'static str,
    pub workload: Workload,
    pub global_build: GlobalBuild,
    pub dense: DenseResult,
    pub single_pass: Vec<SinglePassResult>,
    pub amortized_comparisons: Vec<AmortizedComparison>,
    pub topology_assessment: TopologyAssessment,
    pub correctness: CorrectnessChecks,
    pub production_caveats: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct Workload {
    pub document_count: usize,
    pub distinct_tag_count: usize,
    pub documents_per_tag: usize,
    pub row_count: usize,
    pub row_bytes: usize,
    pub table_bytes_per_server: usize,
    pub values_per_page: usize,
    pub locator_bytes: usize,
    pub public_mphf_metadata_bytes: usize,
    pub generation: String,
}

#[derive(Debug, Serialize)]
pub struct GlobalBuild {
    pub encoded_corpus_build_ms: f64,
    pub mphf_layout_build_ms: f64,
    pub sequential_total_build_ms: f64,
    pub tracked_peak_build_bytes: usize,
    pub build_attempts: usize,
    pub server_replica_count: usize,
    pub replication_payload_bytes: usize,
    pub replication_cpu_and_physical_bytes_note: &'static str,
}

#[derive(Debug, Serialize)]
pub struct DenseResult {
    pub aggregate_work: AggregateWorkReport,
    pub setup: SetupBreakdown,
    pub online: OnlineSummary,
    pub lifetimes: Vec<ClientLifetime>,
}

#[derive(Debug, Serialize)]
pub struct SinglePassResult {
    pub partition_count_q: usize,
    pub aggregate_work: AggregateWorkReport,
    pub setup: SetupBreakdown,
    pub online: OnlineSummary,
    pub maintenance: MaintenanceBreakdown,
    pub lifetimes: Vec<ClientLifetime>,
    pub online_aggregate_server_time_over_dense: DirectComparison,
    pub topology_note: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct SetupBreakdown {
    pub algorithmic_server_setup_ms: Metric<f64>,
    pub transport_serving_server_cpu_ms: Metric<f64>,
    pub public_metadata_client_load_p50_ms: Metric<f64>,
    pub client_preprocessing_p50_ms: Metric<f64>,
    pub client_input_table_scan_bytes: Metric<usize>,
    pub public_metadata_transfer_bytes: usize,
    pub database_transfer_bytes: usize,
    pub server_produced_hint_transfer_bytes: usize,
    pub total_client_setup_download_bytes: usize,
    pub client_hint_state_bytes: usize,
    pub client_permutation_state_bytes: usize,
    pub client_persistent_state_bytes: usize,
    pub note: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct OnlineSummary {
    pub samples: usize,
    pub aggregate_server_p50_ms: f64,
    pub aggregate_server_p95_ms: f64,
    pub co_located_wall_p50_ms: f64,
    pub client_prepare_p50_us: f64,
    pub client_complete_p50_us: f64,
    pub logical_rows_read_per_server: usize,
    pub logical_bytes_read_per_server: usize,
    pub upload_bytes_per_query: usize,
    pub download_bytes_per_query: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClientLifetime {
    pub sequential_queries_per_client: usize,
    pub repetitions: usize,
    pub client_metadata_load_p50_ms: f64,
    pub client_preprocessing_p50_ms: f64,
    pub online_aggregate_server_total_p50_ms: f64,
    pub online_co_located_wall_total_p50_ms: f64,
    pub online_client_prepare_total_p50_ms: f64,
    pub online_client_complete_total_p50_ms: f64,
    pub setup_download_bytes: usize,
    pub online_upload_bytes: usize,
    pub online_download_bytes: usize,
    pub note: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct MaintenanceBreakdown {
    pub online_state_update: &'static str,
    pub measured_with_client_complete: bool,
    pub deterministic_hint_byte_xor_updates_per_query: usize,
    pub maximum_permutation_payload_bytes_touched_per_query: usize,
    pub incremental_snapshot_update: &'static str,
    pub new_generation_server_work: &'static str,
    pub new_generation_client_work: &'static str,
    pub persistence_requirement: &'static str,
}

#[derive(Debug, Serialize)]
pub struct AmortizedComparison {
    pub partition_count_q: usize,
    pub sequential_queries_per_client: usize,
    pub clients_per_generation: usize,
    pub dense: AmortizedCost,
    pub single_pass: AmortizedCost,
    pub measured_algorithm_server_time_comparison: DirectComparison,
    pub client_cpu_single_pass_over_dense: f64,
    pub server_egress_single_pass_over_dense: f64,
    pub note: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct AmortizedCost {
    pub measured_algorithm_server_time_ms_per_useful_query: ServerTimeComponents,
    pub client_cpu_ms_per_useful_query: ClientTimeComponents,
    pub server_egress_bytes_per_useful_query: ByteComponents,
    pub client_upload_bytes_per_useful_query: ByteComponents,
    pub measured_algorithm_server_time_ms_per_generation: f64,
    pub client_cpu_ms_per_generation: f64,
    pub server_egress_bytes_per_generation: u64,
    pub client_upload_bytes_per_generation: u64,
    pub active_client_persistent_state_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ServerTimeComponents {
    pub global_build: f64,
    pub per_client_protocol_setup: f64,
    pub online: f64,
    pub total: f64,
    pub excluded_transport_serving_cpu: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClientTimeComponents {
    pub public_metadata_load: f64,
    pub preprocessing: f64,
    pub online_prepare: f64,
    pub online_complete_and_state_update: f64,
    pub total: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ByteComponents {
    pub setup: f64,
    pub online: f64,
    pub total: f64,
}

#[derive(Debug, Serialize)]
pub struct TopologyAssessment {
    pub exercised_single_pass_server_counts: Vec<usize>,
    pub algebraically_valid_single_pass_server_counts: Vec<usize>,
    pub three_or_more_server_extension: &'static str,
    pub extra_replica_is_not_an_extra_share: &'static str,
    pub blocked_dense_three_server_ratio: BlockedTopologyComparison,
}

#[derive(Debug, Serialize)]
pub struct BlockedTopologyComparison {
    pub directly_comparable: bool,
    pub ratio: Option<f64>,
    pub blocked_by: Vec<&'static str>,
    pub note: &'static str,
}

#[derive(Debug, Serialize)]
pub struct CorrectnessChecks {
    pub present_page_recovered_and_fingerprint_verified: bool,
    pub absent_tag_returns_no_page_after_private_retrieval: bool,
    pub stale_client_state_rejected_before_query: bool,
    pub checked_partition_counts: Vec<usize>,
}

pub fn run(profile: Profile) -> Result<WarmStatefulReport> {
    let config = benchmark_config();
    let corpus_started = Instant::now();
    let page_set = benchmark_page_set(DOCUMENT_COUNT, DISTINCT_TAG_COUNT, &config)?;
    let encoded_corpus_build_ms = millis(corpus_started.elapsed());

    let mphf_started = Instant::now();
    let snapshot = MphfPageSnapshot::from_page_set(&page_set, config.clone())?;
    let mphf_layout_build_ms = millis(mphf_started.elapsed());
    validate_common_corpus(&snapshot)?;

    let sequential_total_build_ms = encoded_corpus_build_ms + mphf_layout_build_ms;
    let public_metadata_bytes = snapshot.manifest.client_metadata_bytes();
    let table_bytes = snapshot.rows().len();
    let replica = ReplicaTable {
        generation: snapshot.manifest.generation,
        rows: Arc::from(snapshot.rows()),
        row_count: snapshot.manifest.page_count,
        row_size: snapshot.manifest.page_size,
    };
    let servers = TwoServerPool::new(replica)?;

    let dense_measurements = benchmark_dense_lifetimes(&snapshot, &servers, profile)?;
    let dense = dense_result(&snapshot, sequential_total_build_ms, &dense_measurements)?;

    let mut single_pass = Vec::with_capacity(PARTITION_COUNTS.len());
    for partition_count in PARTITION_COUNTS {
        let measurements =
            benchmark_single_pass_lifetimes(&snapshot, &servers, partition_count, profile)?;
        single_pass.push(single_pass_result(
            &snapshot,
            sequential_total_build_ms,
            partition_count,
            &measurements,
            &dense,
        )?);
    }

    let correctness = correctness_checks(&snapshot, &servers)?;
    let amortized_comparisons =
        amortized_comparisons(sequential_total_build_ms, &dense, &single_pass)?;

    Ok(WarmStatefulReport {
        protocol: "exact-mphf-dense-vs-two-server-singlepass-warm-lifetimes",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: METHODOLOGY,
        workload: Workload {
            document_count: snapshot.manifest.document_count,
            distinct_tag_count: snapshot.manifest.distinct_tag_count,
            documents_per_tag: snapshot.manifest.document_count
                / snapshot.manifest.distinct_tag_count,
            row_count: snapshot.manifest.page_count,
            row_bytes: snapshot.manifest.page_size,
            table_bytes_per_server: table_bytes,
            values_per_page: snapshot.manifest.values_per_page,
            locator_bytes: snapshot.manifest.max_value_bytes,
            public_mphf_metadata_bytes: public_metadata_bytes,
            generation: snapshot.manifest.generation_hex(),
        },
        global_build: GlobalBuild {
            encoded_corpus_build_ms,
            mphf_layout_build_ms,
            sequential_total_build_ms,
            tracked_peak_build_bytes: snapshot.build_metrics.peak_tracked_bytes,
            build_attempts: snapshot.build_metrics.attempts,
            server_replica_count: SERVER_COUNT,
            replication_payload_bytes: table_bytes * SERVER_COUNT,
            replication_cpu_and_physical_bytes_note: "The benchmark copies one table into a shared in-process replica buffer but does not time production replication, persistence, or physical memory traffic.",
        },
        dense,
        single_pass,
        amortized_comparisons,
        topology_assessment: TopologyAssessment {
            exercised_single_pass_server_counts: vec![SERVER_COUNT],
            algebraically_valid_single_pass_server_counts: vec![SERVER_COUNT],
            three_or_more_server_extension: "The implemented construction has exactly two asymmetric roles: one refresh query and one punctured query. Its recovery and show-and-shuffle equations consume exactly those two answers. No n-server sharing or threshold reconstruction follows by adding replicas.",
            extra_replica_is_not_an_extra_share: "A third machine may mirror either role for availability, but it observes the same role query and does not raise collusion tolerance. Counting it as a third PIR server would invent a different protocol and security proof.",
            blocked_dense_three_server_ratio: BlockedTopologyComparison {
                directly_comparable: false,
                ratio: None,
                blocked_by: vec!["server_count", "collusion_tolerance", "required_answers", "protocol_algebra"],
                note: "A two-server SinglePass timing cannot be divided by a three-server Dense timing as a security-equivalent result. Evaluate a proven multi-server client-preprocessing construction separately.",
            },
        },
        correctness,
        production_caveats: vec![
            "SinglePass setup exposes the entire 25 MiB locator table to the authorized client. The direct comparison is valid only for query privacy within an authorization cohort allowed to receive that projection; it is invalid if non-result locators must remain hidden from the client.",
            "The local setup reads server-owned memory directly. Reported table and metadata transfer bytes model production egress, but transport-serving CPU, network latency, retries, TLS, and filesystem reads are not measured and are excluded from the server-time total.",
            "SinglePass state is mutable and permits only one in-flight query. Persist the post-query state atomically and never roll back or reuse a possibly observed query after an ambiguous failure.",
            "Every immutable generation invalidates both the public MPHF index and SinglePass state. The benchmark rejects stale state before query preparation; production must authenticate the manifest and enforce the same binding at the API boundary.",
            "A full generation rebuild and a full per-client setup are the only measured update path. Incremental inserts into the immutable MPHF/SinglePass state are not implemented.",
            "The MPHF maps absent tags to populated ordinals. A 128-bit page fingerprint is checked only after private retrieval; no public membership test is assumed.",
            "Client state sizes exclude Vec/struct headers, allocator metadata, code, stacks, and transient peak RSS. The permutation representation stores both forward and inverse u32 arrays and is intentionally faithful rather than compressed.",
        ],
    })
}

fn benchmark_config() -> TagPageConfig {
    TagPageConfig {
        bucket_capacity: 4,
        target_load_percent: 90,
        values_per_page: 4,
        max_value_bytes: 16,
    }
}

fn validate_common_corpus(snapshot: &MphfPageSnapshot) -> Result<()> {
    if snapshot.manifest.document_count != DOCUMENT_COUNT
        || snapshot.manifest.distinct_tag_count != DISTINCT_TAG_COUNT
        || snapshot.manifest.page_count != DISTINCT_TAG_COUNT
        || snapshot.manifest.page_size != 96
        || snapshot.rows().len() != DISTINCT_TAG_COUNT * 96
    {
        bail!("warm/stateful benchmark did not build the canonical common corpus");
    }
    Ok(())
}

#[derive(Debug)]
struct ProtocolMeasurements {
    lifetimes: Vec<ClientLifetime>,
    online_server: Vec<Duration>,
    online_wall: Vec<Duration>,
    client_prepare: Vec<Duration>,
    client_complete: Vec<Duration>,
    metadata_load: Vec<Duration>,
    preprocessing: Vec<Duration>,
    client_hint_bytes: usize,
    client_permutation_bytes: usize,
}

fn benchmark_dense_lifetimes(
    snapshot: &MphfPageSnapshot,
    servers: &TwoServerPool,
    profile: Profile,
) -> Result<ProtocolMeasurements> {
    let mut output = ProtocolMeasurements::empty();
    for query_count in QUERIES_PER_CLIENT {
        let repetitions = lifetime_repetitions(profile);
        let mut runs = Vec::with_capacity(repetitions);
        for repetition in 0..repetitions {
            let load_started = Instant::now();
            let client = snapshot.trusted_client_index()?;
            let metadata_load = load_started.elapsed();
            output.metadata_load.push(metadata_load);
            let mut rng = StdRng::seed_from_u64(
                0xd3e5_0000 ^ query_count as u64 ^ ((repetition as u64) << 32),
            );
            let mut totals = LifetimeTotals::default();
            for operation in 0..query_count {
                let tag = benchmark_tag(lifetime_tag_index(operation, repetition));
                let prepare_started = Instant::now();
                let ordinal = client.ordinal(&tag, 0)?;
                let queries = dense::query_shares(
                    ordinal,
                    snapshot.manifest.page_count,
                    SERVER_COUNT,
                    &mut rng,
                )?;
                let prepare = prepare_started.elapsed();
                let evaluated = servers.evaluate_dense(queries)?;
                let complete_started = Instant::now();
                let row = dense::combine(&evaluated.answers)?;
                let page = snapshot
                    .decode_retrieved_page(&row, &tag, 0)?
                    .context("Dense lifetime returned the wrong MPHF page")?;
                if page.values.len() != snapshot.manifest.values_per_page {
                    bail!("Dense lifetime returned the wrong locator count");
                }
                let complete = complete_started.elapsed();
                totals.add(prepare, complete, &evaluated);
                output.push_online(prepare, complete, &evaluated);
            }
            runs.push(LifetimeRun {
                metadata_load,
                preprocessing: Duration::ZERO,
                totals,
            });
        }
        output.lifetimes.push(lifetime_result(
            query_count,
            repetitions,
            &runs,
            snapshot.manifest.client_metadata_bytes(),
            dense::query_size(snapshot.manifest.page_count) * SERVER_COUNT * query_count,
            snapshot.manifest.page_size * SERVER_COUNT * query_count,
            "Directly executed stateless queries after one authenticated MPHF metadata load; queries are sequential only to match the client-lifetime horizon.",
        ));
    }
    Ok(output)
}

fn benchmark_single_pass_lifetimes(
    snapshot: &MphfPageSnapshot,
    servers: &TwoServerPool,
    partition_count: usize,
    profile: Profile,
) -> Result<ProtocolMeasurements> {
    let mut output = ProtocolMeasurements::empty();
    for query_count in QUERIES_PER_CLIENT {
        let repetitions = lifetime_repetitions(profile);
        let mut runs = Vec::with_capacity(repetitions);
        for repetition in 0..repetitions {
            let load_started = Instant::now();
            let client = snapshot.trusted_client_index()?;
            let metadata_load = load_started.elapsed();
            output.metadata_load.push(metadata_load);

            let mut rng = StdRng::seed_from_u64(
                0x516e_0000
                    ^ partition_count as u64
                    ^ ((query_count as u64) << 8)
                    ^ ((repetition as u64) << 40),
            );
            let preprocessing_started = Instant::now();
            let mut state = ClientState::setup(
                snapshot.view(),
                snapshot.manifest.generation,
                partition_count,
                &mut rng,
            )?;
            let preprocessing = preprocessing_started.elapsed();
            output.preprocessing.push(preprocessing);
            output.client_hint_bytes = state.hint_bytes();
            output.client_permutation_bytes = state.permutation_bytes();

            let mut totals = LifetimeTotals::default();
            for operation in 0..query_count {
                let tag = benchmark_tag(lifetime_tag_index(operation, repetition));
                let prepare_started = Instant::now();
                let ordinal = client.ordinal(&tag, 0)?;
                let prepared =
                    state.prepare_query(snapshot.manifest.generation, ordinal, &mut rng)?;
                let prepare = prepare_started.elapsed();
                let queries = prepared.server_queries().clone();
                let evaluated = servers.evaluate_single_pass(queries)?;
                let complete_started = Instant::now();
                let row = state.complete_query(
                    snapshot.manifest.generation,
                    prepared,
                    &evaluated.answers,
                )?;
                let page = snapshot
                    .decode_retrieved_page(&row, &tag, 0)?
                    .context("SinglePass lifetime returned the wrong MPHF page")?;
                if page.values.len() != snapshot.manifest.values_per_page {
                    bail!("SinglePass lifetime returned the wrong locator count");
                }
                let complete = complete_started.elapsed();
                totals.add(prepare, complete, &evaluated);
                output.push_online(prepare, complete, &evaluated);
            }
            runs.push(LifetimeRun {
                metadata_load,
                preprocessing,
                totals,
            });
        }
        output.lifetimes.push(lifetime_result(
            query_count,
            repetitions,
            &runs,
            snapshot.manifest.client_metadata_bytes() + snapshot.rows().len(),
            (GENERATION_ID_BYTES + partition_count * size_of::<u32>())
                * SERVER_COUNT
                * query_count,
            (GENERATION_ID_BYTES + partition_count * snapshot.manifest.page_size)
                * SERVER_COUNT
                * query_count,
            "Directly executed from fresh generation-bound mutable state. Query count is a sequential state lifetime, not a batch; the one-query row is measured directly rather than extrapolated.",
        ));
    }
    Ok(output)
}

fn lifetime_repetitions(profile: Profile) -> usize {
    match profile {
        Profile::Quick => 1,
        Profile::Full => 3,
    }
}

fn lifetime_tag_index(operation: usize, repetition: usize) -> usize {
    (operation * 65_537 + repetition * 7_919 + 1_234) % DISTINCT_TAG_COUNT
}

impl ProtocolMeasurements {
    fn empty() -> Self {
        Self {
            lifetimes: Vec::new(),
            online_server: Vec::new(),
            online_wall: Vec::new(),
            client_prepare: Vec::new(),
            client_complete: Vec::new(),
            metadata_load: Vec::new(),
            preprocessing: Vec::new(),
            client_hint_bytes: 0,
            client_permutation_bytes: 0,
        }
    }

    fn push_online<T>(
        &mut self,
        prepare: Duration,
        complete: Duration,
        evaluated: &ServerEvaluation<T>,
    ) {
        self.client_prepare.push(prepare);
        self.client_complete.push(complete);
        self.online_server.push(evaluated.sum_server_elapsed);
        self.online_wall.push(evaluated.wall);
    }

    fn sort(&mut self) {
        for values in [
            &mut self.online_server,
            &mut self.online_wall,
            &mut self.client_prepare,
            &mut self.client_complete,
            &mut self.metadata_load,
            &mut self.preprocessing,
        ] {
            values.sort_unstable();
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct LifetimeTotals {
    prepare: Duration,
    complete: Duration,
    server: Duration,
    wall: Duration,
}

impl LifetimeTotals {
    fn add<T>(&mut self, prepare: Duration, complete: Duration, evaluated: &ServerEvaluation<T>) {
        self.prepare += prepare;
        self.complete += complete;
        self.server += evaluated.sum_server_elapsed;
        self.wall += evaluated.wall;
    }
}

#[derive(Clone, Copy, Debug)]
struct LifetimeRun {
    metadata_load: Duration,
    preprocessing: Duration,
    totals: LifetimeTotals,
}

fn lifetime_result(
    query_count: usize,
    repetitions: usize,
    runs: &[LifetimeRun],
    setup_download_bytes: usize,
    online_upload_bytes: usize,
    online_download_bytes: usize,
    note: &'static str,
) -> ClientLifetime {
    let mut metadata = runs.iter().map(|run| run.metadata_load).collect::<Vec<_>>();
    let mut preprocessing = runs.iter().map(|run| run.preprocessing).collect::<Vec<_>>();
    let mut server = runs.iter().map(|run| run.totals.server).collect::<Vec<_>>();
    let mut wall = runs.iter().map(|run| run.totals.wall).collect::<Vec<_>>();
    let mut prepare = runs
        .iter()
        .map(|run| run.totals.prepare)
        .collect::<Vec<_>>();
    let mut complete = runs
        .iter()
        .map(|run| run.totals.complete)
        .collect::<Vec<_>>();
    for values in [
        &mut metadata,
        &mut preprocessing,
        &mut server,
        &mut wall,
        &mut prepare,
        &mut complete,
    ] {
        values.sort_unstable();
    }
    ClientLifetime {
        sequential_queries_per_client: query_count,
        repetitions,
        client_metadata_load_p50_ms: millis(percentile(&metadata, 50)),
        client_preprocessing_p50_ms: millis(percentile(&preprocessing, 50)),
        online_aggregate_server_total_p50_ms: millis(percentile(&server, 50)),
        online_co_located_wall_total_p50_ms: millis(percentile(&wall, 50)),
        online_client_prepare_total_p50_ms: millis(percentile(&prepare, 50)),
        online_client_complete_total_p50_ms: millis(percentile(&complete, 50)),
        setup_download_bytes,
        online_upload_bytes,
        online_download_bytes,
        note,
    }
}

fn dense_result(
    snapshot: &MphfPageSnapshot,
    global_build_ms: f64,
    measurements: &ProtocolMeasurements,
) -> Result<DenseResult> {
    let mut measurements = measurements.to_owned_for_sort();
    measurements.sort();
    let row_count = snapshot.manifest.page_count;
    let row_size = snapshot.manifest.page_size;
    let expected_rows = row_count.div_ceil(2);
    let logical_bytes = expected_rows * row_size;
    let query_bytes_per_server = dense::query_size(row_count);
    let online = online_summary(
        &measurements,
        expected_rows,
        logical_bytes,
        query_bytes_per_server * SERVER_COUNT,
        row_size * SERVER_COUNT,
    );
    let setup = SetupBreakdown {
        algorithmic_server_setup_ms: Metric::deterministic(
            0.0,
            "Dense has no per-client server-side preprocessing",
        ),
        transport_serving_server_cpu_ms: Metric::not_measured(
            "metadata/CDN serving CPU and transport are outside the in-process benchmark",
        ),
        public_metadata_client_load_p50_ms: Metric::measured(
            p50_ms(&measurements.metadata_load),
            "authenticated MPHF artifact parse and PtrHash load",
        ),
        client_preprocessing_p50_ms: Metric::not_applicable(
            "Dense retains only the public MPHF index",
        ),
        client_input_table_scan_bytes: Metric::not_applicable(
            "Dense client does not scan or download the PIR table",
        ),
        public_metadata_transfer_bytes: snapshot.manifest.client_metadata_bytes(),
        database_transfer_bytes: 0,
        server_produced_hint_transfer_bytes: 0,
        total_client_setup_download_bytes: snapshot.manifest.client_metadata_bytes(),
        client_hint_state_bytes: 0,
        client_permutation_state_bytes: 0,
        client_persistent_state_bytes: snapshot.manifest.client_metadata_bytes(),
        note: "One generation-specific authenticated public MPHF artifact; no private table or hint transfer.",
    };
    let work = dense_accounting(snapshot, global_build_ms, &setup, &online)?;
    Ok(DenseResult {
        aggregate_work: work,
        setup,
        online,
        lifetimes: measurements.lifetimes,
    })
}

fn single_pass_result(
    snapshot: &MphfPageSnapshot,
    global_build_ms: f64,
    partition_count: usize,
    measurements: &ProtocolMeasurements,
    dense: &DenseResult,
) -> Result<SinglePassResult> {
    let mut measurements = measurements.to_owned_for_sort();
    measurements.sort();
    let row_size = snapshot.manifest.page_size;
    let logical_bytes = partition_count * row_size;
    let online = online_summary(
        &measurements,
        partition_count,
        logical_bytes,
        (GENERATION_ID_BYTES + partition_count * size_of::<u32>()) * SERVER_COUNT,
        (GENERATION_ID_BYTES + partition_count * row_size) * SERVER_COUNT,
    );
    let persistent_state = snapshot
        .manifest
        .client_metadata_bytes()
        .checked_add(measurements.client_hint_bytes)
        .and_then(|bytes| bytes.checked_add(measurements.client_permutation_bytes))
        .and_then(|bytes| bytes.checked_add(GENERATION_ID_BYTES))
        .context("SinglePass client state size overflow")?;
    let setup = SetupBreakdown {
        algorithmic_server_setup_ms: Metric::deterministic(
            0.0,
            "servers use the already published immutable row table; permutations and hints are client-private and client-generated",
        ),
        transport_serving_server_cpu_ms: Metric::not_measured(
            "serving the full table and MPHF artifact is required but its CPU/TLS/filesystem cost is not measured in-process",
        ),
        public_metadata_client_load_p50_ms: Metric::measured(
            p50_ms(&measurements.metadata_load),
            "authenticated MPHF artifact parse and PtrHash load",
        ),
        client_preprocessing_p50_ms: Metric::measured(
            p50_ms(&measurements.preprocessing),
            "client builds private permutations and parity hints from one logical table pass",
        ),
        client_input_table_scan_bytes: Metric::deterministic(
            snapshot.rows().len(),
            "every table row contributes to one client parity hint",
        ),
        public_metadata_transfer_bytes: snapshot.manifest.client_metadata_bytes(),
        database_transfer_bytes: snapshot.rows().len(),
        server_produced_hint_transfer_bytes: 0,
        total_client_setup_download_bytes: snapshot.manifest.client_metadata_bytes()
            + snapshot.rows().len(),
        client_hint_state_bytes: measurements.client_hint_bytes,
        client_permutation_state_bytes: measurements.client_permutation_bytes,
        client_persistent_state_bytes: persistent_state,
        note: "The client downloads/scans the authorized locator table and derives hints locally. There is no server-produced hint transfer in this implementation; treating the database stream as a tiny hint would hide the main cold cost.",
    };
    let maintenance = MaintenanceBreakdown {
        online_state_update: "Each completion applies show-and-shuffle: it mutates parity hints and the forward/inverse permutations before another query may start.",
        measured_with_client_complete: true,
        deterministic_hint_byte_xor_updates_per_query: 2 * (partition_count - 1) * row_size,
        maximum_permutation_payload_bytes_touched_per_query: 4
            * size_of::<u32>()
            * (partition_count - 1),
        incremental_snapshot_update: "not implemented; the measured MPHF table is immutable",
        new_generation_server_work: "rebuild the encoded pages and exact MPHF table, then publish/authenticate a new generation",
        new_generation_client_work: "download the new MPHF metadata and full table, discard old state, and rebuild permutations/hints",
        persistence_requirement: "atomically persist the post-query generation-bound state; after an ambiguous request failure, recover the committed state or discard it rather than rolling back",
    };
    let work = single_pass_accounting(snapshot, global_build_ms, partition_count, &setup, &online)?;
    let online_comparison = direct_ratio(
        "SinglePass online aggregate server time over Dense",
        &dense.aggregate_work,
        &work,
        dense.online.aggregate_server_p50_ms,
        online.aggregate_server_p50_ms,
    );
    if !online_comparison.directly_comparable {
        bail!("two-server SinglePass and Dense should have identical comparison scopes");
    }
    Ok(SinglePassResult {
        partition_count_q: partition_count,
        aggregate_work: work,
        setup,
        online,
        maintenance,
        lifetimes: measurements.lifetimes,
        online_aggregate_server_time_over_dense: online_comparison,
        topology_note: "partition_count_q is the paper/implementation partition count and must be at least two. It is independent of sequential_queries_per_client and is not a batch size.",
    })
}

impl ProtocolMeasurements {
    fn to_owned_for_sort(&self) -> Self {
        Self {
            lifetimes: self.lifetimes.clone(),
            online_server: self.online_server.clone(),
            online_wall: self.online_wall.clone(),
            client_prepare: self.client_prepare.clone(),
            client_complete: self.client_complete.clone(),
            metadata_load: self.metadata_load.clone(),
            preprocessing: self.preprocessing.clone(),
            client_hint_bytes: self.client_hint_bytes,
            client_permutation_bytes: self.client_permutation_bytes,
        }
    }
}

fn p50_ms(values: &[Duration]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        millis(percentile(values, 50))
    }
}

fn online_summary(
    measurements: &ProtocolMeasurements,
    rows_per_server: usize,
    bytes_per_server: usize,
    upload_bytes: usize,
    download_bytes: usize,
) -> OnlineSummary {
    OnlineSummary {
        samples: measurements.online_server.len(),
        aggregate_server_p50_ms: millis(percentile(&measurements.online_server, 50)),
        aggregate_server_p95_ms: millis(percentile(&measurements.online_server, 95)),
        co_located_wall_p50_ms: millis(percentile(&measurements.online_wall, 50)),
        client_prepare_p50_us: micros(percentile(&measurements.client_prepare, 50)),
        client_complete_p50_us: micros(percentile(&measurements.client_complete, 50)),
        logical_rows_read_per_server: rows_per_server,
        logical_bytes_read_per_server: bytes_per_server,
        upload_bytes_per_query: upload_bytes,
        download_bytes_per_query: download_bytes,
    }
}

fn comparison_scope() -> ComparisonScope {
    ComparisonScope {
        workload: SCOPE_WORKLOAD,
        result: SCOPE_RESULT,
        public_partition: SCOPE_PARTITION,
        leakage: LeakageScope::ExactQueryPrivacy,
    }
}

fn security() -> SecurityLabels {
    SecurityLabels {
        privacy: PRIVACY,
        server_count: SERVER_COUNT,
        collusion_tolerance: 1,
        required_answers: SERVER_COUNT,
        assumptions: "the two replicas do not collude; both serve the same authenticated immutable generation; mutable client state, when used, remains synchronized and never rolls back",
        availability: "both server answers are required",
        integrity: "the 128-bit retrieved-page fingerprint rejects absent/wrong rows under the semi-honest model; no malicious-server proof or MAC",
    }
}

fn dense_accounting(
    snapshot: &MphfPageSnapshot,
    global_build_ms: f64,
    setup: &SetupBreakdown,
    online: &OnlineSummary,
) -> Result<AggregateWorkReport> {
    let mut work = AggregateWorkReport::new(
        "two-server-exact-mphf-dense",
        comparison_scope(),
        security(),
    );
    fill_common_build(&mut work, snapshot, global_build_ms);
    work.per_client_setup = PhaseWork::unmeasured(
        "one client loading one immutable generation",
        "metadata serving CPU, transport, and peak client RSS were not measured",
    );
    work.per_client_setup.client_time_ms = setup.public_metadata_client_load_p50_ms.clone();
    work.per_client_setup.client_upload_bytes = Metric::deterministic(0, "public metadata fetch");
    work.per_client_setup.client_download_bytes = Metric::deterministic(
        setup.total_client_setup_download_bytes,
        "authenticated public MPHF metadata",
    );
    work.per_client_setup.network_rounds = Metric::estimated(
        1,
        "one metadata fetch is modelled; no transport was benchmarked",
    );
    work.maintenance = immutable_maintenance();
    fill_online(
        &mut work,
        online,
        "one stateless first-page tag lookup",
        "uniform random Dense shares select half the MPHF rows in expectation",
        1,
    );
    work.client.online_cpu_p50_ms = Metric::estimated(
        (online.client_prepare_p50_us + online.client_complete_p50_us) / 1_000.0,
        "sum of separately sampled MPHF lookup/share preparation and reconstruction/fingerprint medians",
    );
    work.client.persistent_state_bytes = Metric::deterministic(
        setup.client_persistent_state_bytes,
        "authenticated generation-specific MPHF artifact",
    );
    work.client.upload_bytes = Metric::deterministic(
        online.upload_bytes_per_query,
        "one Dense selector share per server",
    );
    work.client.download_bytes = Metric::deterministic(
        online.download_bytes_per_query,
        "one 96-byte answer share per server",
    );
    fill_storage(&mut work, snapshot, setup.client_persistent_state_bytes);
    work.amortization = amortization_horizon("no per-query mutable state");
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    Ok(work)
}

fn single_pass_accounting(
    snapshot: &MphfPageSnapshot,
    global_build_ms: f64,
    partition_count: usize,
    setup: &SetupBreakdown,
    online: &OnlineSummary,
) -> Result<AggregateWorkReport> {
    let mut work = AggregateWorkReport::new(
        "two-server-exact-mphf-singlepass",
        comparison_scope(),
        security(),
    );
    fill_common_build(&mut work, snapshot, global_build_ms);
    work.per_client_setup = PhaseWork::unmeasured(
        "one client preprocessing one immutable generation",
        "transport-serving CPU, network latency, and peak client RSS were not measured",
    );
    work.per_client_setup.aggregate_server_time_ms = setup.algorithmic_server_setup_ms.clone();
    let metadata_load_ms = setup
        .public_metadata_client_load_p50_ms
        .value
        .context("SinglePass MPHF load measurement is missing")?;
    let preprocessing_ms = setup
        .client_preprocessing_p50_ms
        .value
        .context("SinglePass preprocessing measurement is missing")?;
    work.per_client_setup.client_time_ms = Metric::measured(
        metadata_load_ms + preprocessing_ms,
        "sum of separately measured MPHF load and private state construction medians",
    );
    work.per_client_setup.logical_selected_bytes = Metric::deterministic(
        snapshot.rows().len(),
        "client preprocessing consumes every table byte once",
    );
    work.per_client_setup.physical_or_scanned_bytes = Metric::estimated(
        snapshot.rows().len(),
        "one logical client pass; physical/cache/network copies were not counted",
    );
    work.per_client_setup.client_upload_bytes = Metric::deterministic(0, "no setup upload");
    work.per_client_setup.client_download_bytes = Metric::deterministic(
        setup.total_client_setup_download_bytes,
        "one public MPHF artifact plus the full authorized locator table",
    );
    work.per_client_setup.server_scans = Metric::estimated(
        1,
        "one table stream is required per client, although the local client reads shared memory",
    );
    work.per_client_setup.network_rounds = Metric::estimated(
        2,
        "modelled as metadata plus streamed table requests; transport was not benchmarked",
    );
    work.maintenance = PhaseWork::unmeasured(
        "one show-and-shuffle state mutation",
        "client completion includes it, but reconstruction and state mutation were not separately timed",
    );
    work.maintenance.aggregate_server_time_ms =
        Metric::deterministic(0.0, "online state maintenance is client-only");
    work.maintenance.client_download_bytes =
        Metric::deterministic(0, "maintenance consumes the already counted online answers");
    work.maintenance.client_upload_bytes = Metric::deterministic(0, "maintenance adds no messages");
    work.maintenance.network_rounds = Metric::deterministic(0, "included in online round");
    fill_online(
        &mut work,
        online,
        "one sequential stateful first-page tag lookup",
        "each server copies partition-count Q indexed rows",
        0,
    );
    work.online.server_scans = Metric::deterministic(0, "indexed row reads, not a table scan");
    work.client.online_cpu_p50_ms = Metric::estimated(
        (online.client_prepare_p50_us + online.client_complete_p50_us) / 1_000.0,
        "sum of separately measured MPHF lookup/query preparation and completion/show-and-shuffle medians",
    );
    work.client.persistent_state_bytes = Metric::deterministic(
        setup.client_persistent_state_bytes,
        "MPHF metadata, generation binding, parity hints, and forward/inverse permutations",
    );
    work.client.upload_bytes = Metric::deterministic(
        online.upload_bytes_per_query,
        "Q u32 indices to each of two servers",
    );
    work.client.download_bytes = Metric::deterministic(
        online.download_bytes_per_query,
        "Q 96-byte rows from each server",
    );
    fill_storage(&mut work, snapshot, setup.client_persistent_state_bytes);
    work.amortization = amortization_horizon(
        "one client setup supports a sequential state lifetime; show-and-shuffle is included once per online query",
    );
    work.hardware_counters = unavailable_hardware_counters();
    debug_assert_eq!(online.logical_rows_read_per_server, partition_count);
    work.validate()?;
    Ok(work)
}

fn fill_common_build(
    work: &mut AggregateWorkReport,
    snapshot: &MphfPageSnapshot,
    global_build_ms: f64,
) {
    work.global_build.aggregate_server_time_ms = Metric::measured(
        global_build_ms,
        "sequential encoded-corpus plus exact-MPHF layout build wall time; treated as one builder's CPU proxy",
    );
    work.global_build.client_time_ms = Metric::not_applicable("build is server-side");
    work.global_build.logical_selected_bytes = Metric::not_measured(
        "layout construction has multiple unlike passes; encoded payload and final table sizes are reported separately",
    );
    work.global_build.physical_or_scanned_bytes =
        Metric::not_measured("no hardware memory-traffic counter was collected");
    work.global_build.peak_server_ram_bytes = Metric::estimated(
        snapshot.build_metrics.peak_tracked_bytes,
        "algorithm-owned buffers; PtrHash transient workspace and process RSS are excluded",
    );
    work.global_build.client_upload_bytes = Metric::deterministic(0, "server-side build");
    work.global_build.client_download_bytes = Metric::deterministic(0, "server-side build");
    work.global_build.server_scans = Metric::not_measured(
        "corpus generation and MPHF construction passes were not normalized to scans",
    );
    work.global_build.network_rounds = Metric::not_applicable("server-side build");
}

fn immutable_maintenance() -> PhaseWork {
    PhaseWork::not_applicable(
        "incremental immutable-table update",
        "updates publish a new generation and repeat global build/client setup",
    )
}

fn fill_online(
    work: &mut AggregateWorkReport,
    online: &OnlineSummary,
    unit: &'static str,
    logical_note: &'static str,
    scans_per_server: usize,
) {
    for server in &mut work.online.per_server {
        server.server_time_p50_ms = Metric::estimated(
            online.aggregate_server_p50_ms / SERVER_COUNT as f64,
            "aggregate p50 divided evenly; individual-server distributions were not retained",
        );
        server.logical_selected_bytes =
            Metric::estimated(online.logical_bytes_read_per_server, logical_note);
        server.physical_or_scanned_bytes = Metric::not_measured(
            "logical row bytes are known; cache-line and DRAM traffic require hardware counters",
        );
        server.scans = Metric::deterministic(scans_per_server, logical_note);
    }
    work.online.unit = unit;
    work.online.aggregate_server_time_p50_ms = Metric::measured(
        online.aggregate_server_p50_ms,
        "sum of both per-request server elapsed times",
    );
    work.online.max_server_time_p50_ms = Metric::estimated(
        online.co_located_wall_p50_ms,
        "co-located wall p50 includes queue/dispatch overhead",
    );
    work.online.aggregate_logical_selected_bytes = Metric::estimated(
        online.logical_bytes_read_per_server * SERVER_COUNT,
        logical_note,
    );
    work.online.aggregate_physical_or_scanned_bytes =
        Metric::not_measured("no hardware memory-traffic counter was collected");
    work.online.server_scans = Metric::deterministic(scans_per_server * SERVER_COUNT, logical_note);
    work.online.network_rounds = Metric::deterministic(1, "both requests run in parallel");
    work.online.useful_result_bytes = Metric::deterministic(64, "four 16-byte compact locators");
}

fn fill_storage(work: &mut AggregateWorkReport, snapshot: &MphfPageSnapshot, client_bytes: usize) {
    work.persisted_storage.server_bytes_per_server = Metric::deterministic(
        snapshot.rows().len(),
        "one exact-MPHF row table per replica",
    );
    work.persisted_storage.aggregate_server_bytes = Metric::deterministic(
        snapshot.rows().len() * SERVER_COUNT,
        "sum of the two logical replicas; public metadata storage excluded",
    );
    work.persisted_storage.client_bytes = Metric::deterministic(
        client_bytes,
        "generation-specific persistent client payload",
    );
}

fn amortization_horizon(maintenance: &'static str) -> AmortizationHorizon {
    AmortizationHorizon {
        global_build: "all clients and useful queries served by one immutable generation",
        per_client_setup: "1, 2, 10, 100, or 1,000 directly executed sequential queries by one client",
        maintenance,
        assumed_global_queries: None,
        assumed_queries_per_client_setup: None,
        assumed_online_events_per_maintenance: Some(1),
        note: "The enclosing horizon matrix supplies explicit client/query denominators and keeps server time, client time, and bytes separate.",
    }
}

fn amortized_comparisons(
    global_build_ms: f64,
    dense: &DenseResult,
    single_pass: &[SinglePassResult],
) -> Result<Vec<AmortizedComparison>> {
    let mut comparisons = Vec::with_capacity(
        PARTITION_COUNTS.len() * QUERIES_PER_CLIENT.len() * CLIENTS_PER_GENERATION.len(),
    );
    for variant in single_pass {
        for query_count in QUERIES_PER_CLIENT {
            let dense_lifetime = lifetime(&dense.lifetimes, query_count)?;
            let single_lifetime = lifetime(&variant.lifetimes, query_count)?;
            for clients in CLIENTS_PER_GENERATION {
                let dense_cost = amortized_cost(
                    global_build_ms,
                    clients,
                    dense_lifetime,
                    dense.setup.client_persistent_state_bytes,
                )?;
                let single_cost = amortized_cost(
                    global_build_ms,
                    clients,
                    single_lifetime,
                    variant.setup.client_persistent_state_bytes,
                )?;
                let server_comparison = direct_ratio(
                    "SinglePass amortized measured algorithm server time over Dense",
                    &dense.aggregate_work,
                    &variant.aggregate_work,
                    dense_cost
                        .measured_algorithm_server_time_ms_per_useful_query
                        .total,
                    single_cost
                        .measured_algorithm_server_time_ms_per_useful_query
                        .total,
                );
                if !server_comparison.directly_comparable {
                    bail!("amortized two-server comparison unexpectedly changed scope");
                }
                comparisons.push(AmortizedComparison {
                    partition_count_q: variant.partition_count_q,
                    sequential_queries_per_client: query_count,
                    clients_per_generation: clients,
                    client_cpu_single_pass_over_dense: safe_ratio(
                        single_cost.client_cpu_ms_per_useful_query.total,
                        dense_cost.client_cpu_ms_per_useful_query.total,
                    ),
                    server_egress_single_pass_over_dense: safe_ratio(
                        single_cost.server_egress_bytes_per_useful_query.total,
                        dense_cost.server_egress_bytes_per_useful_query.total,
                    ),
                    dense: dense_cost,
                    single_pass: single_cost,
                    measured_algorithm_server_time_comparison: server_comparison,
                    note: "Server-time total adds only server-time phases; client CPU is separate. Server egress and client upload are separate byte totals. Transport-serving CPU, latency, physical memory bytes, and energy remain unmeasured and are not silently folded into time.",
                });
            }
        }
    }
    Ok(comparisons)
}

fn lifetime(lifetimes: &[ClientLifetime], query_count: usize) -> Result<&ClientLifetime> {
    lifetimes
        .iter()
        .find(|lifetime| lifetime.sequential_queries_per_client == query_count)
        .context("missing directly measured client lifetime")
}

fn amortized_cost(
    global_build_ms: f64,
    clients: usize,
    lifetime: &ClientLifetime,
    client_state_bytes: usize,
) -> Result<AmortizedCost> {
    let queries = lifetime.sequential_queries_per_client;
    let total_queries = clients
        .checked_mul(queries)
        .context("amortization query count overflow")?;
    let total_queries_f64 = total_queries as f64;
    let queries_f64 = queries as f64;
    let build_per_query = global_build_ms / total_queries_f64;
    let online_server_per_query = lifetime.online_aggregate_server_total_p50_ms / queries_f64;
    let metadata_per_query = lifetime.client_metadata_load_p50_ms / queries_f64;
    let preprocessing_per_query = lifetime.client_preprocessing_p50_ms / queries_f64;
    let prepare_per_query = lifetime.online_client_prepare_total_p50_ms / queries_f64;
    let complete_per_query = lifetime.online_client_complete_total_p50_ms / queries_f64;
    let setup_egress_per_query = lifetime.setup_download_bytes as f64 / queries_f64;
    let online_egress_per_query = lifetime.online_download_bytes as f64 / queries_f64;
    let online_upload_per_query = lifetime.online_upload_bytes as f64 / queries_f64;

    let server_generation =
        global_build_ms + lifetime.online_aggregate_server_total_p50_ms * clients as f64;
    let client_generation = (lifetime.client_metadata_load_p50_ms
        + lifetime.client_preprocessing_p50_ms
        + lifetime.online_client_prepare_total_p50_ms
        + lifetime.online_client_complete_total_p50_ms)
        * clients as f64;
    let per_client_egress = u64::try_from(lifetime.setup_download_bytes)
        .context("per-client setup egress does not fit u64")?
        .checked_add(
            u64::try_from(lifetime.online_download_bytes)
                .context("per-client online egress does not fit u64")?,
        )
        .context("per-client egress overflow")?;
    let server_egress_generation = per_client_egress
        .checked_mul(clients as u64)
        .context("generation egress overflow")?;
    let upload_generation = u64::try_from(lifetime.online_upload_bytes)
        .context("per-client upload does not fit u64")?
        .checked_mul(clients as u64)
        .context("generation upload overflow")?;
    let active_state = u64::try_from(client_state_bytes)
        .context("client state does not fit u64")?
        .checked_mul(clients as u64)
        .context("active client state overflow")?;

    Ok(AmortizedCost {
        measured_algorithm_server_time_ms_per_useful_query: ServerTimeComponents {
            global_build: build_per_query,
            per_client_protocol_setup: 0.0,
            online: online_server_per_query,
            total: build_per_query + online_server_per_query,
            excluded_transport_serving_cpu: "MPHF/table serving, TLS, filesystem, and network-stack CPU are unmeasured; their bytes are reported as server egress instead.",
        },
        client_cpu_ms_per_useful_query: ClientTimeComponents {
            public_metadata_load: metadata_per_query,
            preprocessing: preprocessing_per_query,
            online_prepare: prepare_per_query,
            online_complete_and_state_update: complete_per_query,
            total: metadata_per_query
                + preprocessing_per_query
                + prepare_per_query
                + complete_per_query,
        },
        server_egress_bytes_per_useful_query: ByteComponents {
            setup: setup_egress_per_query,
            online: online_egress_per_query,
            total: setup_egress_per_query + online_egress_per_query,
        },
        client_upload_bytes_per_useful_query: ByteComponents {
            setup: 0.0,
            online: online_upload_per_query,
            total: online_upload_per_query,
        },
        measured_algorithm_server_time_ms_per_generation: server_generation,
        client_cpu_ms_per_generation: client_generation,
        server_egress_bytes_per_generation: server_egress_generation,
        client_upload_bytes_per_generation: upload_generation,
        active_client_persistent_state_bytes: active_state,
    })
}

fn safe_ratio(candidate: f64, baseline: f64) -> f64 {
    if baseline == 0.0 {
        f64::INFINITY
    } else {
        candidate / baseline
    }
}

fn correctness_checks(
    snapshot: &MphfPageSnapshot,
    servers: &TwoServerPool,
) -> Result<CorrectnessChecks> {
    let present_tag = benchmark_tag(DISTINCT_TAG_COUNT / 3);
    let absent_tag = b"warm-stateful-definitely-absent-tag";
    for partition_count in PARTITION_COUNTS {
        let mut rng = StdRng::seed_from_u64(0x0c01_1ec7 ^ partition_count as u64);
        let client = snapshot.trusted_client_index()?;
        let mut state = ClientState::setup(
            snapshot.view(),
            snapshot.manifest.generation,
            partition_count,
            &mut rng,
        )?;

        let present_ordinal = client.ordinal(&present_tag, 0)?;
        let prepared =
            state.prepare_query(snapshot.manifest.generation, present_ordinal, &mut rng)?;
        let evaluated = servers.evaluate_single_pass(prepared.server_queries().clone())?;
        let row =
            state.complete_query(snapshot.manifest.generation, prepared, &evaluated.answers)?;
        if snapshot
            .decode_retrieved_page(&row, &present_tag, 0)?
            .is_none()
        {
            bail!("SinglePass present-key correctness check failed");
        }

        let absent_ordinal = client.ordinal(absent_tag, 0)?;
        let prepared =
            state.prepare_query(snapshot.manifest.generation, absent_ordinal, &mut rng)?;
        let evaluated = servers.evaluate_single_pass(prepared.server_queries().clone())?;
        let row =
            state.complete_query(snapshot.manifest.generation, prepared, &evaluated.answers)?;
        if snapshot
            .decode_retrieved_page(&row, absent_tag, 0)?
            .is_some()
        {
            bail!("SinglePass absent-key fingerprint check failed");
        }
    }

    let mut rng = StdRng::seed_from_u64(0x57a1e);
    let mut state = ClientState::setup(snapshot.view(), snapshot.manifest.generation, 2, &mut rng)?;
    let mut other_generation = snapshot.manifest.generation;
    other_generation[0] ^= 1;
    if state.prepare_query(other_generation, 0, &mut rng).is_ok() {
        bail!("stale SinglePass state was not rejected");
    }

    Ok(CorrectnessChecks {
        present_page_recovered_and_fingerprint_verified: true,
        absent_tag_returns_no_page_after_private_retrieval: true,
        stale_client_state_rejected_before_query: true,
        checked_partition_counts: PARTITION_COUNTS.to_vec(),
    })
}

#[derive(Clone)]
struct ReplicaTable {
    generation: [u8; 32],
    rows: Arc<[u8]>,
    row_count: usize,
    row_size: usize,
}

impl ReplicaTable {
    fn view(&self) -> SnapshotView<'_> {
        SnapshotView::new(&self.rows, self.row_count, self.row_size)
    }
}

enum ServerRequest {
    Dense(Vec<u8>),
    SinglePass(ServerQuery),
}

struct ServerJob {
    request: ServerRequest,
    response: mpsc::Sender<ServerResponse>,
}

struct ServerResponse {
    server_index: usize,
    answer: std::result::Result<ServerResponsePayload, String>,
    elapsed: Duration,
}

enum ServerResponsePayload {
    Dense(Vec<u8>),
    SinglePass(ServerAnswer),
}

struct ServerEvaluation<T> {
    answers: Vec<T>,
    wall: Duration,
    sum_server_elapsed: Duration,
}

struct TwoServerPool {
    senders: Vec<mpsc::Sender<ServerJob>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl TwoServerPool {
    fn new(table: ReplicaTable) -> Result<Self> {
        let mut senders = Vec::with_capacity(SERVER_COUNT);
        let mut workers = Vec::with_capacity(SERVER_COUNT);
        for server_index in 0..SERVER_COUNT {
            let (sender, receiver) = mpsc::channel::<ServerJob>();
            let table = table.clone();
            workers.push(std::thread::spawn(move || {
                while let Ok(job) = receiver.recv() {
                    let started = Instant::now();
                    let answer = match job.request {
                        ServerRequest::Dense(query) => {
                            dense::answer(table.view(), &query).map(ServerResponsePayload::Dense)
                        }
                        ServerRequest::SinglePass(query) => {
                            single_pass::answer(table.view(), table.generation, &query)
                                .map(ServerResponsePayload::SinglePass)
                        }
                    }
                    .map_err(|error| error.to_string());
                    let _ = job.response.send(ServerResponse {
                        server_index,
                        answer,
                        elapsed: started.elapsed(),
                    });
                }
            }));
            senders.push(sender);
        }
        Ok(Self { senders, workers })
    }

    fn evaluate_dense(&self, queries: Vec<Vec<u8>>) -> Result<ServerEvaluation<Vec<u8>>> {
        if queries.len() != SERVER_COUNT {
            bail!("Dense warm benchmark requires exactly two shares");
        }
        let evaluated = self.evaluate(queries.into_iter().map(ServerRequest::Dense).collect())?;
        evaluated.map_answers(|answer| match answer {
            ServerResponsePayload::Dense(answer) => Ok(answer),
            ServerResponsePayload::SinglePass(_) => bail!("warm benchmark response type mismatch"),
        })
    }

    fn evaluate_single_pass(
        &self,
        queries: [ServerQuery; SERVER_COUNT],
    ) -> Result<ServerEvaluation<ServerAnswer>> {
        let evaluated =
            self.evaluate(queries.into_iter().map(ServerRequest::SinglePass).collect())?;
        evaluated.map_answers(|answer| match answer {
            ServerResponsePayload::SinglePass(answer) => Ok(answer),
            ServerResponsePayload::Dense(_) => bail!("warm benchmark response type mismatch"),
        })
    }

    fn evaluate(
        &self,
        requests: Vec<ServerRequest>,
    ) -> Result<ServerEvaluation<ServerResponsePayload>> {
        if requests.len() != SERVER_COUNT {
            bail!("warm benchmark server request count mismatch");
        }
        let (response_sender, response_receiver) = mpsc::channel();
        let wall_started = Instant::now();
        for (sender, request) in self.senders.iter().zip(requests) {
            sender
                .send(ServerJob {
                    request,
                    response: response_sender.clone(),
                })
                .context("send warm benchmark server request")?;
        }
        drop(response_sender);

        let mut answers = (0..SERVER_COUNT).map(|_| None).collect::<Vec<_>>();
        let mut sum_server_elapsed = Duration::ZERO;
        for _ in 0..SERVER_COUNT {
            let response = response_receiver
                .recv()
                .context("receive warm benchmark server response")?;
            sum_server_elapsed += response.elapsed;
            answers[response.server_index] = Some(response.answer.map_err(anyhow::Error::msg)?);
        }
        Ok(ServerEvaluation {
            answers: answers
                .into_iter()
                .map(|answer| answer.context("warm benchmark server returned no answer"))
                .collect::<Result<Vec<_>>>()?,
            wall: wall_started.elapsed(),
            sum_server_elapsed,
        })
    }
}

impl<T> ServerEvaluation<T> {
    fn map_answers<U>(self, convert: impl Fn(T) -> Result<U>) -> Result<ServerEvaluation<U>> {
        Ok(ServerEvaluation {
            answers: self
                .answers
                .into_iter()
                .map(convert)
                .collect::<Result<Vec<_>>>()?,
            wall: self.wall,
            sum_server_elapsed: self.sum_server_elapsed,
        })
    }
}

impl Drop for TwoServerPool {
    fn drop(&mut self) {
        self.senders.clear();
        for worker in self.workers.drain(..) {
            worker.join().expect("warm benchmark server panicked");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::Record;

    fn small_snapshot() -> MphfPageSnapshot {
        MphfPageSnapshot::benchmark(64, 16, benchmark_config()).unwrap()
    }

    #[test]
    fn generation_bound_state_recovers_present_and_rejects_absent() {
        let snapshot = small_snapshot();
        let mut rng = StdRng::seed_from_u64(7);
        let client = snapshot.trusted_client_index().unwrap();
        let mut state =
            ClientState::setup(snapshot.view(), snapshot.manifest.generation, 4, &mut rng).unwrap();

        let tag = benchmark_tag(3);
        let ordinal = client.ordinal(&tag, 0).unwrap();
        let prepared = state
            .prepare_query(snapshot.manifest.generation, ordinal, &mut rng)
            .unwrap();
        let answers = prepared
            .server_queries()
            .iter()
            .map(|query| {
                single_pass::answer(snapshot.view(), snapshot.manifest.generation, query).unwrap()
            })
            .collect::<Vec<_>>();
        let row = state
            .complete_query(snapshot.manifest.generation, prepared, &answers)
            .unwrap();
        assert!(snapshot
            .decode_retrieved_page(&row, &tag, 0)
            .unwrap()
            .is_some());

        let absent = b"not-present";
        let ordinal = client.ordinal(absent, 0).unwrap();
        let prepared = state
            .prepare_query(snapshot.manifest.generation, ordinal, &mut rng)
            .unwrap();
        let answers = prepared
            .server_queries()
            .iter()
            .map(|query| {
                single_pass::answer(snapshot.view(), snapshot.manifest.generation, query).unwrap()
            })
            .collect::<Vec<_>>();
        let row = state
            .complete_query(snapshot.manifest.generation, prepared, &answers)
            .unwrap();
        assert!(snapshot
            .decode_retrieved_page(&row, absent, 0)
            .unwrap()
            .is_none());
    }

    #[test]
    fn stale_generation_is_rejected_before_state_mutation() {
        let first =
            MphfPageSnapshot::build(vec![Record::new("tag", vec![1u8; 16])], benchmark_config())
                .unwrap();
        let second =
            MphfPageSnapshot::build(vec![Record::new("tag", vec![2u8; 16])], benchmark_config())
                .unwrap();
        assert_ne!(first.manifest.generation, second.manifest.generation);
        let mut rng = StdRng::seed_from_u64(11);
        let mut state =
            ClientState::setup(first.view(), first.manifest.generation, 2, &mut rng).unwrap();
        assert!(state
            .prepare_query(second.manifest.generation, 0, &mut rng)
            .is_err());
        assert!(state
            .prepare_query(first.manifest.generation, 0, &mut rng)
            .is_ok());
    }

    #[test]
    fn partition_q_one_is_rejected_but_one_query_lifetime_is_valid() {
        let snapshot = small_snapshot();
        let mut rng = StdRng::seed_from_u64(13);
        assert!(
            ClientState::setup(snapshot.view(), snapshot.manifest.generation, 1, &mut rng,)
                .is_err()
        );
        assert_eq!(QUERIES_PER_CLIENT[0], 1);
    }

    #[test]
    fn three_server_ratio_is_explicitly_blocked() {
        let guard = TopologyAssessment {
            exercised_single_pass_server_counts: vec![2],
            algebraically_valid_single_pass_server_counts: vec![2],
            three_or_more_server_extension: "not derived",
            extra_replica_is_not_an_extra_share: "mirror only",
            blocked_dense_three_server_ratio: BlockedTopologyComparison {
                directly_comparable: false,
                ratio: None,
                blocked_by: vec!["server_count", "protocol_algebra"],
                note: "test",
            },
        };
        assert!(!guard.blocked_dense_three_server_ratio.directly_comparable);
        assert!(guard.blocked_dense_three_server_ratio.ratio.is_none());
    }
}
