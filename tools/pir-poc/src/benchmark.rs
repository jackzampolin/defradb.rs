mod cold;
mod config;
mod endpoints;
mod kernels;
mod local;
mod optimization;
pub mod report;
mod single_pass;

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::dense;
use crate::snapshot::Snapshot;
use anyhow::{bail, Context, Result};
pub use cold::run as run_cold;
use config::{
    batch_sizes, dimensions, sample_count, LOAD_BUCKET_COUNT, LOAD_ROW_SIZE, TARGET_SERVER_COUNTS,
};
pub use endpoints::run as run_endpoints;
use local::LocalServerPool;
pub use optimization::run as run_optimizations;
use rand::{rngs::StdRng, SeedableRng};
pub use report::BenchmarkReport;
use report::{
    excluded_protocols, methodology, BatchResult, DimensionResult, IsolatedServerResult,
    LoadResult, PublicQueryResult, TopologyResult,
};
pub use single_pass::run as run_single_pass;

const SERVER_WORKER_THREADS: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Quick,
    Full,
}

pub fn run(profile: Profile) -> Result<BenchmarkReport> {
    let dimensions = dimensions(profile)
        .into_iter()
        .map(|(buckets, row_size)| benchmark_dimension(buckets, row_size, profile))
        .collect::<Result<Vec<_>>>()?;

    let batch_snapshot = Arc::new(Snapshot::benchmark(
        LOAD_BUCKET_COUNT,
        LOAD_ROW_SIZE,
        0xba7c,
    )?);
    let mut batches = Vec::new();
    for server_count in TARGET_SERVER_COUNTS {
        for batch_size in batch_sizes(profile) {
            batches.push(benchmark_batch(
                Arc::clone(&batch_snapshot),
                server_count,
                batch_size,
                profile,
            )?);
        }
    }

    let mut load = Vec::new();
    for server_count in TARGET_SERVER_COUNTS {
        for clients in [1, 8, 32] {
            load.push(benchmark_load(
                Arc::clone(&batch_snapshot),
                server_count,
                clients,
                profile,
            )?);
        }
    }

    let generated_at_unix_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    Ok(BenchmarkReport {
        protocol: "n-server-dense-xor-pir",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds,
        target_server_counts: TARGET_SERVER_COUNTS.to_vec(),
        server_worker_threads: SERVER_WORKER_THREADS,
        methodology: methodology(),
        excluded_protocols: excluded_protocols(),
        dimensions,
        batches,
        load,
    })
}

fn benchmark_dimension(
    bucket_count: usize,
    row_size: usize,
    profile: Profile,
) -> Result<DimensionResult> {
    let build_started = Instant::now();
    let snapshot = Arc::new(Snapshot::benchmark(bucket_count, row_size, 0x5eed)?);
    let snapshot_build_ms = millis(build_started.elapsed());
    let process_rss_bytes = memory_stats::memory_stats().map(|stats| stats.physical_mem);
    let bucket = bucket_count / 3;
    let public_query = benchmark_public_query(&snapshot)?;
    let mut rng = StdRng::seed_from_u64(bucket_count as u64 ^ row_size as u64);
    let isolated_query = dense::query_shares(bucket, bucket_count, 2, &mut rng)?.remove(0);

    let cold_started = Instant::now();
    std::hint::black_box(dense::answer(snapshot.view(), &isolated_query)?);
    let cold_server_ms = millis(cold_started.elapsed());
    let runs = sample_count(profile, snapshot.rows().len());
    let mut warm = Vec::with_capacity(runs);
    for _ in 0..runs {
        let started = Instant::now();
        std::hint::black_box(dense::answer(snapshot.view(), &isolated_query)?);
        warm.push(started.elapsed());
    }
    warm.sort_unstable();
    let p50 = percentile(&warm, 50);
    let isolated_server = IsolatedServerResult {
        cold_ms: cold_server_ms,
        warm_p50_ms: millis(p50),
        warm_p95_ms: millis(percentile(&warm, 95)),
        warm_p99_ms: millis(percentile(&warm, 99)),
        throughput_gib_s: snapshot.rows().len() as f64 / p50.as_secs_f64() / 1024f64.powi(3),
    };

    let topologies = TARGET_SERVER_COUNTS
        .into_iter()
        .map(|server_count| {
            benchmark_topology(Arc::clone(&snapshot), bucket, server_count, profile)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(DimensionResult {
        bucket_count,
        row_size,
        snapshot_bytes: snapshot.rows().len(),
        snapshot_build_ms,
        process_rss_bytes,
        public_query,
        isolated_server,
        topologies,
    })
}

fn benchmark_public_query(snapshot: &Snapshot) -> Result<PublicQueryResult> {
    const KEY: &[u8] = b"benchmark-public-key";
    const SAMPLES: usize = 10_000;

    let request_started = Instant::now();
    for _ in 0..SAMPLES {
        std::hint::black_box(KEY.to_vec());
    }
    let client_request_generation_us = micros(request_started.elapsed()) / SAMPLES as f64;

    let mut server = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let bucket = crate::snapshot::bucket_for_key(
            std::hint::black_box(KEY),
            snapshot.manifest.bucket_count,
        );
        let answer = snapshot.row(bucket)?.to_vec();
        std::hint::black_box(&answer);
        let elapsed = started.elapsed();
        server.push(elapsed);
    }
    server.sort_unstable();

    Ok(PublicQueryResult {
        request_bytes: KEY.len(),
        answer_bytes: snapshot.manifest.row_size,
        client_request_generation_us,
        server_p50_ms: millis(percentile(&server, 50)),
        server_p95_ms: millis(percentile(&server, 95)),
        server_p99_ms: millis(percentile(&server, 99)),
    })
}

fn benchmark_topology(
    snapshot: Arc<Snapshot>,
    bucket: usize,
    server_count: usize,
    profile: Profile,
) -> Result<TopologyResult> {
    let mut rng = StdRng::seed_from_u64(
        snapshot.manifest.bucket_count as u64
            ^ snapshot.manifest.row_size as u64
            ^ server_count as u64,
    );
    let iterations = 100;
    let query_started = Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(dense::query_shares(
            bucket,
            snapshot.manifest.bucket_count,
            server_count,
            &mut rng,
        )?);
    }
    let client_query_generation_us = micros(query_started.elapsed()) / iterations as f64;

    let runs = sample_count(profile, snapshot.rows().len());
    let servers = LocalServerPool::new(Arc::clone(&snapshot), server_count, SERVER_WORKER_THREADS)?;
    let mut wall = Vec::with_capacity(runs);
    let mut server_elapsed = Vec::with_capacity(runs);
    let mut combine = Vec::with_capacity(runs);
    for _ in 0..runs {
        let per_server_queries = dense::query_shares(
            bucket,
            snapshot.manifest.bucket_count,
            server_count,
            &mut rng,
        )?
        .into_iter()
        .map(|query| vec![query])
        .collect();
        let evaluation = servers.evaluate(per_server_queries)?;
        let combine_started = Instant::now();
        let shares = evaluation
            .answers
            .iter()
            .map(|answers| answers[0].as_slice())
            .collect::<Vec<_>>();
        let recovered = dense::combine(&shares)?;
        combine.push(combine_started.elapsed());
        if recovered != snapshot.row(bucket)? {
            bail!("topology evaluation recovered the wrong row");
        }
        wall.push(evaluation.wall);
        server_elapsed.push(evaluation.sum_server_elapsed);
    }
    wall.sort_unstable();
    server_elapsed.sort_unstable();
    combine.sort_unstable();

    let query_share_bytes = dense::query_size(snapshot.manifest.bucket_count);
    Ok(TopologyResult {
        server_count,
        privacy_collusion_tolerance: server_count - 1,
        required_answers: server_count,
        query_share_bytes_per_server: query_share_bytes,
        total_query_bytes: query_share_bytes * server_count,
        answer_share_bytes_per_server: snapshot.manifest.row_size,
        total_answer_bytes: snapshot.manifest.row_size * server_count,
        client_query_generation_us,
        co_located_wall_p50_ms: millis(percentile(&wall, 50)),
        co_located_wall_p95_ms: millis(percentile(&wall, 95)),
        co_located_wall_p99_ms: millis(percentile(&wall, 99)),
        sum_server_elapsed_p50_ms: millis(percentile(&server_elapsed, 50)),
        client_combine_p50_us: micros(percentile(&combine, 50)),
    })
}

fn benchmark_batch(
    snapshot: Arc<Snapshot>,
    server_count: usize,
    batch_size: usize,
    profile: Profile,
) -> Result<BatchResult> {
    let runs = match profile {
        Profile::Quick => 3,
        Profile::Full => 7,
    };
    let mut rng = StdRng::seed_from_u64((server_count * 1_000 + batch_size) as u64);
    let servers = LocalServerPool::new(Arc::clone(&snapshot), server_count, SERVER_WORKER_THREADS)?;
    let mut query_generation = Vec::with_capacity(runs);
    let mut wall = Vec::with_capacity(runs);
    let mut server_elapsed = Vec::with_capacity(runs);
    let mut combine = Vec::with_capacity(runs);

    for sample in 0..runs {
        let query_started = Instant::now();
        let generated = generate_batch_queries(
            snapshot.manifest.bucket_count,
            server_count,
            batch_size,
            sample,
            &mut rng,
        )?;
        query_generation.push(query_started.elapsed());

        let evaluation = servers.evaluate(generated.per_server_queries)?;
        let combine_started = Instant::now();
        for (query_index, bucket) in generated.buckets.into_iter().enumerate() {
            let shares = evaluation
                .answers
                .iter()
                .map(|server_answers| server_answers[query_index].as_slice())
                .collect::<Vec<_>>();
            let recovered = dense::combine(&shares)?;
            if recovered != snapshot.row(bucket)? {
                bail!("batch evaluation recovered the wrong row");
            }
        }
        combine.push(combine_started.elapsed());
        wall.push(evaluation.wall);
        server_elapsed.push(evaluation.sum_server_elapsed);
    }
    query_generation.sort_unstable();
    wall.sort_unstable();
    server_elapsed.sort_unstable();
    combine.sort_unstable();
    let wall_p50 = percentile(&wall, 50);
    let query_p50 = percentile(&query_generation, 50);

    Ok(BatchResult {
        server_count,
        privacy_collusion_tolerance: server_count - 1,
        required_answers: server_count,
        batch_size,
        bucket_count: snapshot.manifest.bucket_count,
        row_size: snapshot.manifest.row_size,
        snapshot_bytes: snapshot.rows().len(),
        total_query_bytes: dense::query_size(snapshot.manifest.bucket_count)
            * server_count
            * batch_size,
        total_answer_bytes: snapshot.manifest.row_size * server_count * batch_size,
        client_query_generation_p50_us: micros(query_p50),
        client_query_generation_per_item_p50_us: micros(query_p50) / batch_size as f64,
        co_located_wall_p50_ms: millis(wall_p50),
        co_located_wall_p95_ms: millis(percentile(&wall, 95)),
        sum_server_elapsed_p50_ms: millis(percentile(&server_elapsed, 50)),
        client_combine_p50_us: micros(percentile(&combine, 50)),
        logical_queries_per_second: batch_size as f64 / wall_p50.as_secs_f64(),
    })
}

struct GeneratedBatch {
    per_server_queries: Vec<Vec<Vec<u8>>>,
    buckets: Vec<usize>,
}

fn generate_batch_queries(
    bucket_count: usize,
    server_count: usize,
    batch_size: usize,
    sample: usize,
    rng: &mut StdRng,
) -> Result<GeneratedBatch> {
    let mut per_server = (0..server_count)
        .map(|_| Vec::with_capacity(batch_size))
        .collect::<Vec<_>>();
    let mut buckets = Vec::with_capacity(batch_size);
    for query_index in 0..batch_size {
        let bucket = (sample * 65_537 + query_index * 7_919 + 1_234) % bucket_count;
        let shares = dense::query_shares(bucket, bucket_count, server_count, rng)?;
        for (server_queries, share) in per_server.iter_mut().zip(shares) {
            server_queries.push(share);
        }
        buckets.push(bucket);
    }
    Ok(GeneratedBatch {
        per_server_queries: per_server,
        buckets,
    })
}

fn benchmark_load(
    snapshot: Arc<Snapshot>,
    server_count: usize,
    clients: usize,
    profile: Profile,
) -> Result<LoadResult> {
    let operations_per_client = match profile {
        Profile::Quick => 4,
        Profile::Full => 16,
    };

    let servers = LocalServerPool::new(Arc::clone(&snapshot), server_count, SERVER_WORKER_THREADS)?;

    let started = Instant::now();
    let latencies = std::thread::scope(|scope| -> Result<Vec<Duration>> {
        let handles = (0..clients)
            .map(|client_index| {
                let servers = &servers;
                let snapshot = Arc::clone(&snapshot);
                scope.spawn(move || -> Result<Vec<Duration>> {
                    let mut rng = StdRng::seed_from_u64(44 + client_index as u64);
                    let mut durations = Vec::with_capacity(operations_per_client);
                    for operation_index in 0..operations_per_client {
                        let operation = Instant::now();
                        let bucket = (client_index * 4_099 + operation_index * 7_919 + 1_234)
                            % snapshot.manifest.bucket_count;
                        let shares = dense::query_shares(
                            bucket,
                            snapshot.manifest.bucket_count,
                            server_count,
                            &mut rng,
                        )?;
                        let evaluation = servers
                            .evaluate(shares.into_iter().map(|share| vec![share]).collect())?;
                        let answers = evaluation
                            .answers
                            .into_iter()
                            .map(|mut answers| {
                                answers.pop().context("PIR server returned no answer")
                            })
                            .collect::<Result<Vec<_>>>()?;
                        if dense::combine(&answers)? != snapshot.row(bucket)? {
                            bail!("load evaluation recovered the wrong row");
                        }
                        durations.push(operation.elapsed());
                    }
                    Ok(durations)
                })
            })
            .collect::<Vec<_>>();

        let mut latencies = Vec::with_capacity(clients * operations_per_client);
        for handle in handles {
            latencies.extend(handle.join().expect("load client worker panicked")?);
        }
        Ok(latencies)
    })?;
    let elapsed = started.elapsed();
    let mut latencies = latencies;
    latencies.sort_unstable();
    let operations = clients * operations_per_client;
    Ok(LoadResult {
        server_count,
        clients,
        bucket_count: snapshot.manifest.bucket_count,
        row_size: snapshot.manifest.row_size,
        operations,
        elapsed_ms: millis(elapsed),
        logical_queries_per_second: operations as f64 / elapsed.as_secs_f64(),
        end_to_end_p50_ms: millis(percentile(&latencies, 50)),
        end_to_end_p95_ms: millis(percentile(&latencies, 95)),
        end_to_end_p99_ms: millis(percentile(&latencies, 99)),
    })
}

fn percentile(values: &[Duration], percentile: usize) -> Duration {
    let index = ((values.len() - 1) * percentile).div_ceil(100);
    values[index]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}
