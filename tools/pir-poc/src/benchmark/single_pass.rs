use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rand::{rngs::StdRng, SeedableRng};

use super::accounting::{
    direct_ratio, unavailable_hardware_counters, AggregateWorkReport, AmortizationHorizon,
    ComparisonScope, LeakageScope, Metric, PhaseWork, SecurityLabels,
};
use super::config::sample_count;
use super::local::LocalServerPool;
use super::report::{
    DenseComparisonResult, SinglePassBenchmarkReport, SinglePassDimensionResult,
    SinglePassVariantResult,
};
use super::{micros, millis, percentile, Profile, SERVER_WORKER_THREADS};
use crate::dense;
use crate::single_pass::{
    self, ClientState, ServerAnswer, ServerQuery, GENERATION_ID_BYTES, SERVER_COUNT,
};
use crate::snapshot::Snapshot;

const SINGLE_PASS_QUICK_SAMPLES: usize = 1_000;
const SINGLE_PASS_FULL_SAMPLES: usize = 5_000;

pub fn run(profile: Profile) -> Result<SinglePassBenchmarkReport> {
    let dimensions = dimensions(profile)
        .into_iter()
        .map(|(bucket_count, row_size)| benchmark_dimension(bucket_count, row_size, profile))
        .collect::<Result<Vec<_>>>()?;
    Ok(SinglePassBenchmarkReport {
        protocol: "dense-xor-vs-two-server-single-pass-pir",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: "Both protocols use the identical in-memory snapshot and two persistent co-located server workers. Dense uses its measured two-thread scan evaluator per server. SinglePass setup is one single-threaded pass, then each online server copies Q indexed rows. Timings exclude HTTP, TLS, serialization, persistence, and network latency. SinglePass samples are sequential because one mutable client state permits one in-flight query.",
        dimensions,
    })
}

fn dimensions(profile: Profile) -> Vec<(usize, usize)> {
    match profile {
        Profile::Quick => vec![(1 << 18, 64), (1 << 20, 64), (1 << 22, 64)],
        Profile::Full => vec![(1 << 18, 64), (1 << 20, 64), (1 << 20, 256), (1 << 22, 64)],
    }
}

fn partition_counts(profile: Profile) -> &'static [usize] {
    match profile {
        Profile::Quick => &[16],
        Profile::Full => &[8, 16, 32],
    }
}

fn benchmark_dimension(
    bucket_count: usize,
    row_size: usize,
    profile: Profile,
) -> Result<SinglePassDimensionResult> {
    let build_started = Instant::now();
    let snapshot = Arc::new(Snapshot::benchmark(bucket_count, row_size, 0x0516_e91e)?);
    let snapshot_build_ms = millis(build_started.elapsed());
    let dense = benchmark_dense(Arc::clone(&snapshot), profile, snapshot_build_ms)?;
    let single_pass = partition_counts(profile)
        .iter()
        .copied()
        .map(|partition_count| {
            benchmark_single_pass(Arc::clone(&snapshot), partition_count, profile, &dense)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SinglePassDimensionResult {
        bucket_count,
        row_size,
        snapshot_bytes: snapshot.rows().len(),
        snapshot_build_ms,
        dense,
        single_pass,
    })
}

fn benchmark_dense(
    snapshot: Arc<Snapshot>,
    profile: Profile,
    snapshot_build_ms: f64,
) -> Result<DenseComparisonResult> {
    let samples = sample_count(profile, snapshot.rows().len());
    let servers = LocalServerPool::new(Arc::clone(&snapshot), SERVER_COUNT, SERVER_WORKER_THREADS)?;
    let mut rng = StdRng::seed_from_u64(
        snapshot.manifest.bucket_count as u64 ^ snapshot.manifest.row_size as u64 ^ 0xd3e5e,
    );
    let mut query_generation = Vec::with_capacity(samples);
    let mut wall = Vec::with_capacity(samples);
    let mut server_elapsed = Vec::with_capacity(samples);
    let mut reconstruct = Vec::with_capacity(samples);
    for sample in 0..samples {
        let bucket = benchmark_bucket(sample, snapshot.manifest.bucket_count);
        let query_started = Instant::now();
        let shares = dense::query_shares(
            bucket,
            snapshot.manifest.bucket_count,
            SERVER_COUNT,
            &mut rng,
        )?;
        query_generation.push(query_started.elapsed());
        let evaluation = servers.evaluate(
            shares
                .into_iter()
                .map(|share| vec![share])
                .collect::<Vec<_>>(),
        )?;
        let reconstruct_started = Instant::now();
        let answers = evaluation
            .answers
            .iter()
            .map(|server_answers| server_answers[0].as_slice())
            .collect::<Vec<_>>();
        let row = dense::combine(&answers)?;
        reconstruct.push(reconstruct_started.elapsed());
        if row != snapshot.row(bucket)? {
            bail!("Dense comparison recovered the wrong row");
        }
        wall.push(evaluation.wall);
        server_elapsed.push(evaluation.sum_server_elapsed);
    }
    sort_all(&mut [
        &mut query_generation,
        &mut wall,
        &mut server_elapsed,
        &mut reconstruct,
    ]);
    let expected_rows_read_per_server = snapshot.manifest.bucket_count / 2;
    let expected_data_bytes_read_per_server =
        expected_rows_read_per_server * snapshot.manifest.row_size;
    let query_bytes_per_server = dense::query_size(snapshot.manifest.bucket_count);
    let answer_bytes_per_server = snapshot.manifest.row_size;
    let aggregate_work = dense_accounting(
        snapshot.rows().len(),
        snapshot_build_ms,
        expected_data_bytes_read_per_server,
        query_bytes_per_server,
        answer_bytes_per_server,
        micros(percentile(&query_generation, 50)),
        millis(percentile(&wall, 50)),
        millis(percentile(&server_elapsed, 50)),
        micros(percentile(&reconstruct, 50)),
    )?;
    Ok(DenseComparisonResult {
        aggregate_work,
        samples,
        expected_rows_read_per_server,
        expected_data_bytes_read_per_server: expected_rows_read_per_server
            * snapshot.manifest.row_size,
        query_bytes_per_server,
        answer_bytes_per_server,
        client_query_generation_p50_us: micros(percentile(&query_generation, 50)),
        co_located_wall_p50_ms: millis(percentile(&wall, 50)),
        co_located_wall_p95_ms: millis(percentile(&wall, 95)),
        sum_server_elapsed_p50_ms: millis(percentile(&server_elapsed, 50)),
        client_reconstruct_p50_us: micros(percentile(&reconstruct, 50)),
    })
}

fn benchmark_single_pass(
    snapshot: Arc<Snapshot>,
    partition_count: usize,
    profile: Profile,
    dense: &DenseComparisonResult,
) -> Result<SinglePassVariantResult> {
    let mut rng = StdRng::seed_from_u64(
        snapshot.manifest.bucket_count as u64
            ^ snapshot.manifest.row_size as u64
            ^ partition_count as u64
            ^ 0x0516_e91e,
    );
    let setup_started = Instant::now();
    let generation = snapshot.manifest.generation_id()?;
    let mut state = ClientState::setup(snapshot.view(), generation, partition_count, &mut rng)?;
    let setup_ms = millis(setup_started.elapsed());
    let client_state_bytes = state.payload_bytes();
    let client_hint_bytes = state.hint_bytes();
    let client_permutation_bytes = state.permutation_bytes();
    let servers = SinglePassServerPool::new(Arc::clone(&snapshot))?;

    for sample in 0..16 {
        evaluate_single_pass_query(
            &snapshot,
            &servers,
            &mut state,
            benchmark_bucket(sample, snapshot.manifest.bucket_count),
            &mut rng,
        )?;
    }

    let samples = match profile {
        Profile::Quick => SINGLE_PASS_QUICK_SAMPLES,
        Profile::Full => SINGLE_PASS_FULL_SAMPLES,
    };
    let mut query_generation = Vec::with_capacity(samples);
    let mut wall = Vec::with_capacity(samples);
    let mut server_elapsed = Vec::with_capacity(samples);
    let mut reconstruct = Vec::with_capacity(samples);
    for sample in 0..samples {
        let bucket = benchmark_bucket(sample + 16, snapshot.manifest.bucket_count);
        let measurement =
            evaluate_single_pass_query(&snapshot, &servers, &mut state, bucket, &mut rng)?;
        query_generation.push(measurement.query_generation);
        wall.push(measurement.wall);
        server_elapsed.push(measurement.server_elapsed);
        reconstruct.push(measurement.reconstruct);
    }
    sort_all(&mut [
        &mut query_generation,
        &mut wall,
        &mut server_elapsed,
        &mut reconstruct,
    ]);

    let wall_p50_ms = millis(percentile(&wall, 50));
    let server_p50_ms = millis(percentile(&server_elapsed, 50));
    let query_bytes_per_server = GENERATION_ID_BYTES + partition_count * size_of::<u32>();
    let dense_total_query_bytes = dense.query_bytes_per_server * SERVER_COUNT;
    let single_pass_total_query_bytes = query_bytes_per_server * SERVER_COUNT;
    let aggregate_work = single_pass_accounting(
        snapshot.rows().len(),
        snapshot.manifest.row_size,
        snapshot_build_ms_from_dense(dense),
        setup_ms,
        client_state_bytes,
        partition_count,
        query_bytes_per_server,
        wall_p50_ms,
        server_p50_ms,
        micros(percentile(&query_generation, 50)),
        micros(percentile(&reconstruct, 50)),
    )?;
    let server_comparison = direct_ratio(
        "SinglePass aggregate server time over Dense",
        &dense.aggregate_work,
        &aggregate_work,
        dense.sum_server_elapsed_p50_ms,
        server_p50_ms,
    );
    let wall_comparison = direct_ratio(
        "SinglePass co-located wall time over Dense",
        &dense.aggregate_work,
        &aggregate_work,
        dense.co_located_wall_p50_ms,
        wall_p50_ms,
    );
    if !server_comparison.directly_comparable || !wall_comparison.directly_comparable {
        bail!("SinglePass and Dense accounting scopes do not permit a direct speed comparison");
    }
    Ok(SinglePassVariantResult {
        aggregate_work,
        partition_count_q: partition_count,
        samples,
        setup_ms,
        client_state_bytes,
        client_hint_bytes,
        client_permutation_bytes,
        client_state_to_snapshot_ratio: client_state_bytes as f64 / snapshot.rows().len() as f64,
        rows_read_per_server: partition_count,
        data_bytes_read_per_server: partition_count * snapshot.manifest.row_size,
        query_bytes_per_server,
        answer_bytes_per_server: GENERATION_ID_BYTES + partition_count * snapshot.manifest.row_size,
        client_query_generation_p50_us: micros(percentile(&query_generation, 50)),
        co_located_wall_p50_ms: wall_p50_ms,
        co_located_wall_p95_ms: millis(percentile(&wall, 95)),
        sum_server_elapsed_p50_ms: server_p50_ms,
        client_reconstruct_p50_us: micros(percentile(&reconstruct, 50)),
        wall_speedup_vs_dense: ratio(dense.co_located_wall_p50_ms, wall_p50_ms),
        wall_time_reduction_percent: reduction_percent(dense.co_located_wall_p50_ms, wall_p50_ms),
        server_time_speedup_vs_dense: ratio(dense.sum_server_elapsed_p50_ms, server_p50_ms),
        server_time_reduction_percent: reduction_percent(
            dense.sum_server_elapsed_p50_ms,
            server_p50_ms,
        ),
        server_row_access_reduction_factor: dense.expected_rows_read_per_server as f64
            / partition_count as f64,
        total_query_byte_reduction_factor: dense_total_query_bytes as f64
            / single_pass_total_query_bytes as f64,
    })
}

fn snapshot_build_ms_from_dense(dense: &DenseComparisonResult) -> f64 {
    dense
        .aggregate_work
        .global_build
        .aggregate_server_time_ms
        .value
        .unwrap_or_default()
}

#[allow(clippy::too_many_arguments)]
fn dense_accounting(
    snapshot_bytes: usize,
    snapshot_build_ms: f64,
    logical_bytes_per_server: usize,
    query_bytes_per_server: usize,
    answer_bytes_per_server: usize,
    query_generation_us: f64,
    wall_ms: f64,
    aggregate_server_ms: f64,
    reconstruct_us: f64,
) -> Result<AggregateWorkReport> {
    let security = SecurityLabels {
        privacy: "exact information-theoretic row privacy",
        server_count: SERVER_COUNT,
        collusion_tolerance: SERVER_COUNT - 1,
        required_answers: SERVER_COUNT,
        assumptions: "the two replicas do not collude; both serve the same immutable generation; cryptographically secure randomness; mutable client state, when used, remains synchronized and never rolls back",
        availability: "both server answers are required",
        integrity: "correctness checked by benchmark only; no malicious-server proof",
    };
    let mut work = AggregateWorkReport::new(
        "dense-xor-raw-row",
        ComparisonScope {
            workload: "one lookup over the identical populated synthetic row snapshot",
            result: "one fixed-size raw row",
            public_partition: "global immutable snapshot",
            leakage: LeakageScope::ExactQueryPrivacy,
        },
        security,
    );
    work.global_build.aggregate_server_time_ms =
        Metric::measured(snapshot_build_ms, "one local synthetic snapshot build");
    work.global_build.physical_or_scanned_bytes = Metric::not_measured(
        "snapshot construction writes the table; host memory traffic was not counted",
    );
    work.global_build.server_scans = Metric::deterministic(1, "one snapshot construction pass");
    work.global_build.network_rounds = Metric::not_applicable("build is server-local");
    work.maintenance = PhaseWork::not_applicable(
        "immutable snapshot lifetime",
        "the measured immutable snapshot has no incremental maintenance",
    );
    let physical_per_server = logical_bytes_per_server + query_bytes_per_server;
    for server in &mut work.online.per_server {
        server.server_time_p50_ms = Metric::estimated(
            aggregate_server_ms / SERVER_COUNT as f64,
            "aggregate p50 divided evenly; per-server samples were not retained",
        );
        server.logical_selected_bytes = Metric::estimated(
            logical_bytes_per_server,
            "expected set selector bits times row bytes for a random query share",
        );
        server.physical_or_scanned_bytes = Metric::estimated(
            physical_per_server,
            "selector bytes scanned plus selected row payload; excludes cache-line amplification and was not measured with hardware counters",
        );
        server.scans = Metric::deterministic(1, "one Dense selector scan");
    }
    work.online.unit = "one raw-row lookup";
    work.online.aggregate_server_time_p50_ms = Metric::measured(
        aggregate_server_ms,
        "sum of both measured server elapsed times",
    );
    work.online.max_server_time_p50_ms = Metric::estimated(
        wall_ms,
        "co-located wall p50 is an upper-envelope proxy that includes dispatch overhead",
    );
    work.online.aggregate_logical_selected_bytes = Metric::estimated(
        logical_bytes_per_server * SERVER_COUNT,
        "sum of expected selected bytes across random server shares",
    );
    work.online.aggregate_physical_or_scanned_bytes = Metric::estimated(
        physical_per_server * SERVER_COUNT,
        "sum of estimated selector and payload bytes across servers; no perf counter",
    );
    work.online.server_scans = Metric::deterministic(SERVER_COUNT, "one scan per server");
    work.online.network_rounds = Metric::deterministic(1, "one request/response round");
    work.online.useful_result_bytes =
        Metric::deterministic(answer_bytes_per_server, "one reconstructed raw row");
    work.client.online_cpu_p50_ms = Metric::estimated(
        (query_generation_us + reconstruct_us) / 1_000.0,
        "sum of separately measured query-share generation and reconstruction medians; excludes transport",
    );
    work.client.persistent_state_bytes = Metric::deterministic(0, "stateless client");
    work.client.upload_bytes =
        Metric::deterministic(query_bytes_per_server * SERVER_COUNT, "all query shares");
    work.client.download_bytes =
        Metric::deterministic(answer_bytes_per_server * SERVER_COUNT, "all answer shares");
    work.persisted_storage.server_bytes_per_server =
        Metric::deterministic(snapshot_bytes, "one replicated snapshot per server");
    work.persisted_storage.aggregate_server_bytes = Metric::deterministic(
        snapshot_bytes * SERVER_COUNT,
        "sum across both logical replicas",
    );
    work.persisted_storage.client_bytes = Metric::deterministic(0, "stateless client");
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    Ok(work)
}

#[allow(clippy::too_many_arguments)]
fn single_pass_accounting(
    snapshot_bytes: usize,
    row_size: usize,
    snapshot_build_ms: f64,
    setup_ms: f64,
    client_state_bytes: usize,
    partition_count: usize,
    query_bytes_per_server: usize,
    wall_ms: f64,
    aggregate_server_ms: f64,
    query_generation_us: f64,
    reconstruct_us: f64,
) -> Result<AggregateWorkReport> {
    let security = SecurityLabels {
        privacy: "exact information-theoretic row privacy",
        server_count: SERVER_COUNT,
        collusion_tolerance: 1,
        required_answers: SERVER_COUNT,
        assumptions: "the two replicas do not collude; both serve the same immutable generation; cryptographically secure randomness; mutable client state, when used, remains synchronized and never rolls back",
        availability: "both server answers are required",
        integrity: "correctness checked by benchmark only; no malicious-server proof",
    };
    let mut work = AggregateWorkReport::new(
        "two-server-single-pass-raw-row",
        ComparisonScope {
            workload: "one lookup over the identical populated synthetic row snapshot",
            result: "one fixed-size raw row",
            public_partition: "global immutable snapshot",
            leakage: LeakageScope::ExactQueryPrivacy,
        },
        security,
    );
    work.global_build.aggregate_server_time_ms =
        Metric::measured(snapshot_build_ms, "shared synthetic snapshot build");
    work.global_build.server_scans = Metric::deterministic(1, "one snapshot construction pass");
    work.global_build.network_rounds = Metric::not_applicable("build is server-local");
    work.per_client_setup = PhaseWork::unmeasured(
        "one client state initialization",
        "only fields explicitly replaced below were measured or derived",
    );
    work.per_client_setup.client_time_ms =
        Metric::measured(setup_ms, "client setup over a local in-memory snapshot");
    work.per_client_setup.logical_selected_bytes = Metric::deterministic(
        snapshot_bytes,
        "the client preprocesses every snapshot byte once",
    );
    work.per_client_setup.physical_or_scanned_bytes = Metric::estimated(
        snapshot_bytes,
        "one logical snapshot pass; cache and transport copies were not measured",
    );
    work.per_client_setup.peak_client_ram_bytes = Metric::not_measured(
        "persistent state size is known but peak temporary allocation was not measured",
    );
    work.per_client_setup.client_upload_bytes = Metric::deterministic(0, "no setup upload");
    work.per_client_setup.client_download_bytes = Metric::deterministic(
        snapshot_bytes,
        "production setup must stream one snapshot; local benchmark performs no network transfer",
    );
    work.per_client_setup.server_scans =
        Metric::deterministic(1, "one logical snapshot stream per client setup");
    work.per_client_setup.network_rounds = Metric::estimated(
        1,
        "modelled as one streamed setup transfer; local benchmark has no network",
    );
    work.maintenance = PhaseWork::not_applicable(
        "per-query client state update",
        "show-and-shuffle state maintenance is included in online client reconstruction time",
    );
    let logical_per_server = partition_count * row_size;
    let physical_per_server = logical_per_server + query_bytes_per_server;
    for server in &mut work.online.per_server {
        server.server_time_p50_ms = Metric::estimated(
            aggregate_server_ms / SERVER_COUNT as f64,
            "aggregate p50 divided evenly; per-server samples were not retained",
        );
        server.logical_selected_bytes =
            Metric::deterministic(logical_per_server, "Q indexed rows copied by each server");
        server.physical_or_scanned_bytes = Metric::estimated(
            physical_per_server,
            "query indices plus Q row payloads; excludes cache-line amplification and has no hardware counter",
        );
        server.scans =
            Metric::deterministic(0, "online work performs Q indexed reads, not a table scan");
    }
    work.online.unit = "one stateful raw-row lookup";
    work.online.aggregate_server_time_p50_ms =
        Metric::measured(aggregate_server_ms, "sum of both server elapsed times");
    work.online.max_server_time_p50_ms = Metric::estimated(
        wall_ms,
        "co-located wall p50 is an upper-envelope proxy that includes dispatch overhead",
    );
    work.online.aggregate_logical_selected_bytes =
        Metric::deterministic(logical_per_server * SERVER_COUNT, "sum across servers");
    work.online.aggregate_physical_or_scanned_bytes = Metric::estimated(
        physical_per_server * SERVER_COUNT,
        "sum of estimated online bytes across servers; no perf counter",
    );
    work.online.server_scans = Metric::deterministic(0, "indexed reads only");
    work.online.network_rounds = Metric::deterministic(1, "one request/response round");
    work.online.useful_result_bytes = Metric::deterministic(row_size, "one reconstructed raw row");
    work.client.online_cpu_p50_ms = Metric::estimated(
        (query_generation_us + reconstruct_us) / 1_000.0,
        "sum of separately measured query-preparation and stateful-completion medians; excludes setup and transport",
    );
    work.client.persistent_state_bytes = Metric::deterministic(
        client_state_bytes,
        "serialized hint and permutation payload",
    );
    work.client.upload_bytes =
        Metric::deterministic(query_bytes_per_server * SERVER_COUNT, "both server queries");
    work.client.download_bytes = Metric::deterministic(
        (GENERATION_ID_BYTES + logical_per_server) * SERVER_COUNT,
        "generation ID and Q response rows from both servers",
    );
    work.persisted_storage.server_bytes_per_server =
        Metric::deterministic(snapshot_bytes, "one snapshot per server");
    work.persisted_storage.aggregate_server_bytes =
        Metric::deterministic(snapshot_bytes * SERVER_COUNT, "sum across both servers");
    work.persisted_storage.client_bytes =
        Metric::deterministic(client_state_bytes, "state retained between queries");
    work.amortization = AmortizationHorizon {
        global_build: "all clients and queries using one immutable snapshot",
        per_client_setup: "queries by one client until snapshot refresh or state reset",
        maintenance: "included once in every online query",
        assumed_global_queries: None,
        assumed_queries_per_client_setup: None,
        assumed_online_events_per_maintenance: Some(1),
        note: "Setup is intentionally not folded into online latency; choose an expected queries-per-client horizon before comparing total work.",
    };
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    Ok(work)
}

struct SinglePassMeasurement {
    query_generation: Duration,
    wall: Duration,
    server_elapsed: Duration,
    reconstruct: Duration,
}

fn evaluate_single_pass_query(
    snapshot: &Snapshot,
    servers: &SinglePassServerPool,
    state: &mut ClientState,
    bucket: usize,
    rng: &mut StdRng,
) -> Result<SinglePassMeasurement> {
    let query_started = Instant::now();
    let generation = snapshot.manifest.generation_id()?;
    let prepared = state.prepare_query(generation, bucket, rng)?;
    let query_generation = query_started.elapsed();
    let queries = prepared.server_queries().clone();
    debug_assert_eq!(queries[0].wire_bytes(), queries[1].wire_bytes());
    let evaluation = servers.evaluate(queries)?;
    let reconstruct_started = Instant::now();
    let row = state.complete_query(generation, prepared, &evaluation.answers)?;
    let reconstruct = reconstruct_started.elapsed();
    if row != snapshot.row(bucket)? {
        bail!("SinglePass comparison recovered the wrong row");
    }
    Ok(SinglePassMeasurement {
        query_generation,
        wall: evaluation.wall,
        server_elapsed: evaluation.sum_server_elapsed,
        reconstruct,
    })
}

fn benchmark_bucket(sample: usize, bucket_count: usize) -> usize {
    (sample * 65_537 + 1_234) % bucket_count
}

fn sort_all(values: &mut [&mut Vec<Duration>]) {
    for value in values {
        value.sort_unstable();
    }
}

fn ratio(baseline: f64, candidate: f64) -> f64 {
    if candidate == 0.0 {
        f64::INFINITY
    } else {
        baseline / candidate
    }
}

fn reduction_percent(baseline: f64, candidate: f64) -> f64 {
    if baseline == 0.0 {
        0.0
    } else {
        (1.0 - candidate / baseline) * 100.0
    }
}

struct SinglePassJob {
    query: ServerQuery,
    response: mpsc::Sender<SinglePassResponse>,
}

struct SinglePassResponse {
    server_index: usize,
    answer: std::result::Result<ServerAnswer, String>,
    elapsed: Duration,
}

struct SinglePassEvaluation {
    answers: Vec<ServerAnswer>,
    wall: Duration,
    sum_server_elapsed: Duration,
}

struct SinglePassServerPool {
    senders: Vec<mpsc::Sender<SinglePassJob>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl SinglePassServerPool {
    fn new(snapshot: Arc<Snapshot>) -> Result<Self> {
        let generation = snapshot.manifest.generation_id()?;
        let mut senders = Vec::with_capacity(SERVER_COUNT);
        let mut workers = Vec::with_capacity(SERVER_COUNT);
        for server_index in 0..SERVER_COUNT {
            let (sender, receiver) = mpsc::channel::<SinglePassJob>();
            let snapshot = Arc::clone(&snapshot);
            workers.push(std::thread::spawn(move || {
                while let Ok(job) = receiver.recv() {
                    let started = Instant::now();
                    let answer = single_pass::answer(snapshot.view(), generation, &job.query)
                        .map_err(|error| error.to_string());
                    let _ = job.response.send(SinglePassResponse {
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

    fn evaluate(&self, queries: [ServerQuery; SERVER_COUNT]) -> Result<SinglePassEvaluation> {
        let (response_sender, response_receiver) = mpsc::channel();
        let wall_started = Instant::now();
        for (server, query) in self.senders.iter().zip(queries) {
            server
                .send(SinglePassJob {
                    query,
                    response: response_sender.clone(),
                })
                .context("send SinglePass benchmark query")?;
        }
        drop(response_sender);

        let mut answers = (0..SERVER_COUNT).map(|_| None).collect::<Vec<_>>();
        let mut sum_server_elapsed = Duration::ZERO;
        for _ in 0..SERVER_COUNT {
            let response = response_receiver
                .recv()
                .context("receive SinglePass benchmark response")?;
            sum_server_elapsed += response.elapsed;
            answers[response.server_index] = Some(response.answer.map_err(anyhow::Error::msg)?);
        }
        Ok(SinglePassEvaluation {
            answers: answers
                .into_iter()
                .map(|answer| answer.context("SinglePass server returned no answer"))
                .collect::<Result<Vec<_>>>()?,
            wall: wall_started.elapsed(),
            sum_server_elapsed,
        })
    }
}

impl Drop for SinglePassServerPool {
    fn drop(&mut self) {
        self.senders.clear();
        for worker in self.workers.drain(..) {
            worker.join().expect("SinglePass server worker panicked");
        }
    }
}
