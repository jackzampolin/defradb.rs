use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rand::{rngs::StdRng, SeedableRng};

use super::config::sample_count;
use super::local::LocalServerPool;
use super::report::{
    DenseComparisonResult, SinglePassBenchmarkReport, SinglePassDimensionResult,
    SinglePassVariantResult,
};
use super::{micros, millis, percentile, Profile, SERVER_WORKER_THREADS};
use crate::dense;
use crate::single_pass::{self, ClientState, ServerQuery, SERVER_COUNT};
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
    let dense = benchmark_dense(Arc::clone(&snapshot), profile)?;
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

fn benchmark_dense(snapshot: Arc<Snapshot>, profile: Profile) -> Result<DenseComparisonResult> {
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
    Ok(DenseComparisonResult {
        samples,
        expected_rows_read_per_server,
        expected_data_bytes_read_per_server: expected_rows_read_per_server
            * snapshot.manifest.row_size,
        query_bytes_per_server: dense::query_size(snapshot.manifest.bucket_count),
        answer_bytes_per_server: snapshot.manifest.row_size,
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
    let mut state = ClientState::setup(snapshot.view(), partition_count, &mut rng)?;
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
    let query_bytes_per_server = partition_count * size_of::<u32>();
    let dense_total_query_bytes = dense.query_bytes_per_server * SERVER_COUNT;
    let single_pass_total_query_bytes = query_bytes_per_server * SERVER_COUNT;
    Ok(SinglePassVariantResult {
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
        answer_bytes_per_server: partition_count * snapshot.manifest.row_size,
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
    let prepared = state.prepare_query(bucket, rng)?;
    let query_generation = query_started.elapsed();
    let queries = prepared.server_queries().clone();
    debug_assert_eq!(queries[0].wire_bytes(), queries[1].wire_bytes());
    let evaluation = servers.evaluate(queries)?;
    let reconstruct_started = Instant::now();
    let row = state.complete_query(prepared, &evaluation.answers)?;
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
    answer: std::result::Result<Vec<u8>, String>,
    elapsed: Duration,
}

struct SinglePassEvaluation {
    answers: Vec<Vec<u8>>,
    wall: Duration,
    sum_server_elapsed: Duration,
}

struct SinglePassServerPool {
    senders: Vec<mpsc::Sender<SinglePassJob>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl SinglePassServerPool {
    fn new(snapshot: Arc<Snapshot>) -> Result<Self> {
        let mut senders = Vec::with_capacity(SERVER_COUNT);
        let mut workers = Vec::with_capacity(SERVER_COUNT);
        for server_index in 0..SERVER_COUNT {
            let (sender, receiver) = mpsc::channel::<SinglePassJob>();
            let snapshot = Arc::clone(&snapshot);
            workers.push(std::thread::spawn(move || {
                while let Ok(job) = receiver.recv() {
                    let started = Instant::now();
                    let answer = single_pass::answer(snapshot.view(), &job.query)
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
