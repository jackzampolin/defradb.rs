use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;

use crate::dense;
use crate::snapshot::Snapshot;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    Quick,
    Full,
}

#[derive(Debug, Serialize)]
pub struct BenchmarkReport {
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub dimensions: Vec<DimensionResult>,
    pub concurrency: Vec<ConcurrencyResult>,
}

#[derive(Debug, Serialize)]
pub struct DimensionResult {
    pub bucket_count: usize,
    pub row_size: usize,
    pub snapshot_bytes: usize,
    pub snapshot_build_ms: f64,
    pub process_rss_bytes: Option<usize>,
    pub query_share_bytes: usize,
    pub answer_share_bytes: usize,
    pub query_generation_us: f64,
    pub cold_server_ms: f64,
    pub warm_server_p50_ms: f64,
    pub warm_server_p95_ms: f64,
    pub warm_server_p99_ms: f64,
    pub server_throughput_gib_s: f64,
    pub two_server_wall_ms: f64,
    pub combine_us: f64,
}

#[derive(Debug, Serialize)]
pub struct ConcurrencyResult {
    pub clients: usize,
    pub bucket_count: usize,
    pub row_size: usize,
    pub operations: usize,
    pub elapsed_ms: f64,
    pub operations_per_second: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

pub fn run(profile: Profile) -> Result<BenchmarkReport> {
    let dimensions = dimensions(profile)
        .into_iter()
        .map(|(buckets, row_size)| benchmark_dimension(buckets, row_size, profile))
        .collect::<Result<Vec<_>>>()?;
    let concurrency = [1, 8, 32]
        .into_iter()
        .map(|clients| benchmark_concurrency(clients, profile))
        .collect::<Result<Vec<_>>>()?;
    let generated_at_unix_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    Ok(BenchmarkReport {
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds,
        dimensions,
        concurrency,
    })
}

fn dimensions(profile: Profile) -> Vec<(usize, usize)> {
    match profile {
        Profile::Quick => vec![
            (1 << 10, 64),
            (1 << 10, 256),
            (1 << 10, 1024),
            (1 << 14, 64),
            (1 << 14, 256),
            (1 << 14, 1024),
            (1 << 18, 64),
            (1 << 18, 256),
            (1 << 20, 64),
        ],
        Profile::Full => [10, 14, 18, 20]
            .into_iter()
            .flat_map(|depth| [64, 256, 1024].map(move |row| (1 << depth, row)))
            .collect(),
    }
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
    let mut rng = StdRng::seed_from_u64(bucket_count as u64 ^ row_size as u64);
    let bucket = bucket_count / 3;

    let query_started = Instant::now();
    let iterations = 100;
    let mut queries = None;
    for _ in 0..iterations {
        queries = Some(dense::query_shares(bucket, bucket_count, &mut rng)?);
    }
    let query_generation_us = micros(query_started.elapsed()) / iterations as f64;
    let (query_a, query_b) = queries.context("query generation produced nothing")?;

    let cold_started = Instant::now();
    let cold_answer = dense::answer(&snapshot, &query_a)?;
    let cold_server_ms = millis(cold_started.elapsed());
    let runs = match profile {
        Profile::Quick if snapshot.rows().len() >= 64 * 1024 * 1024 => 3,
        Profile::Quick => 7,
        Profile::Full => 11,
    };
    let mut warm = Vec::with_capacity(runs);
    for _ in 0..runs {
        let started = Instant::now();
        let answer = dense::answer(&snapshot, &query_a)?;
        std::hint::black_box(answer);
        warm.push(started.elapsed());
    }
    warm.sort_unstable();

    let snapshot_a = Arc::clone(&snapshot);
    let snapshot_b = Arc::clone(&snapshot);
    let wall_started = Instant::now();
    let (answer_a, answer_b) = std::thread::scope(|scope| {
        let left = scope.spawn(|| dense::answer(&snapshot_a, &query_a));
        let right = scope.spawn(|| dense::answer(&snapshot_b, &query_b));
        (
            left.join().expect("left PIR server panicked"),
            right.join().expect("right PIR server panicked"),
        )
    });
    let two_server_wall_ms = millis(wall_started.elapsed());
    let answer_a = answer_a?;
    let answer_b = answer_b?;
    let combine_started = Instant::now();
    let recovered = dense::combine(&answer_a, &answer_b)?;
    let combine_us = micros(combine_started.elapsed());
    assert_eq!(recovered, snapshot.row(bucket)?);
    std::hint::black_box(cold_answer);

    let p50 = percentile(&warm, 50);
    let bytes = snapshot.rows().len() as f64;
    let server_throughput_gib_s = bytes / p50.as_secs_f64() / 1024f64.powi(3);
    Ok(DimensionResult {
        bucket_count,
        row_size,
        snapshot_bytes: snapshot.rows().len(),
        snapshot_build_ms,
        process_rss_bytes,
        query_share_bytes: dense::query_size(bucket_count),
        answer_share_bytes: row_size,
        query_generation_us,
        cold_server_ms,
        warm_server_p50_ms: millis(p50),
        warm_server_p95_ms: millis(percentile(&warm, 95)),
        warm_server_p99_ms: millis(percentile(&warm, 99)),
        server_throughput_gib_s,
        two_server_wall_ms,
        combine_us,
    })
}

fn benchmark_concurrency(clients: usize, profile: Profile) -> Result<ConcurrencyResult> {
    let bucket_count = 1 << 14;
    let row_size = 256;
    let snapshot = Arc::new(Snapshot::benchmark(bucket_count, row_size, 0xc011ab)?);
    let operations_per_client = match profile {
        Profile::Quick => 2,
        Profile::Full => 8,
    };
    let mut rng = StdRng::seed_from_u64(44);
    let query = Arc::new(dense::query_shares(1234, bucket_count, &mut rng)?.0);
    let started = Instant::now();
    let latencies = std::thread::scope(|scope| {
        let handles = (0..clients)
            .map(|_| {
                let snapshot = Arc::clone(&snapshot);
                let query = Arc::clone(&query);
                scope.spawn(move || -> Result<Vec<Duration>> {
                    let mut durations = Vec::with_capacity(operations_per_client);
                    for _ in 0..operations_per_client {
                        let operation = Instant::now();
                        std::hint::black_box(dense::answer(&snapshot, &query)?);
                        durations.push(operation.elapsed());
                    }
                    Ok(durations)
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("concurrency worker panicked").unwrap())
            .collect::<Vec<_>>()
    });
    let elapsed = started.elapsed();
    let mut latencies = latencies;
    latencies.sort_unstable();
    let operations = clients * operations_per_client;
    Ok(ConcurrencyResult {
        clients,
        bucket_count,
        row_size,
        operations,
        elapsed_ms: millis(elapsed),
        operations_per_second: operations as f64 / elapsed.as_secs_f64(),
        p50_ms: millis(percentile(&latencies, 50)),
        p95_ms: millis(percentile(&latencies, 95)),
        p99_ms: millis(percentile(&latencies, 99)),
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
