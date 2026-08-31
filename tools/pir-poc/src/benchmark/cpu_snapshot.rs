//! Same-host CPU control for the external InsPIRe AVX2 adapter.

use std::hint::black_box;
use std::time::Instant;

use anyhow::{Context, Result};
use rand::rngs::OsRng;
use serde::Serialize;

use crate::dense::{self, ParallelEvaluator};
use crate::profile::Profile;
use crate::snapshot::Snapshot;

const ENTRIES: usize = 1 << 23;
const USEFUL_ROW_BYTES: usize = 120;
const PHYSICAL_ROW_BYTES: usize = 128;
const SAMPLES: usize = 5;

#[derive(Debug, Serialize)]
pub struct Report {
    schema: &'static str,
    hardware_threads: usize,
    entries: usize,
    useful_row_bytes: usize,
    physical_row_bytes: usize,
    logical_table_bytes: usize,
    physical_table_bytes: usize,
    table_materialize_ms: f64,
    rows: Vec<Row>,
    scope: &'static str,
}

#[derive(Debug, Serialize)]
struct Row {
    threads_per_server: usize,
    batch: usize,
    dense_xor_2: Measurement,
    visible_100: VisibleMeasurement,
}

#[derive(Debug, Serialize)]
struct Measurement {
    first_online_aggregate_server_ms_per_query: f64,
    aggregate_server_p50_ms_per_query: f64,
    parallel_server_p50_ms_per_query: f64,
    client_query_p50_ms_per_query: f64,
    client_recover_p50_ms_per_query: f64,
    aggregate_upload_bytes_per_query: usize,
    aggregate_download_bytes_per_query: usize,
}

#[derive(Debug, Serialize)]
struct VisibleMeasurement {
    server_p50_ms_per_query: f64,
    client_filter_p50_ms_per_query: f64,
    upload_bytes_per_query: usize,
    download_bytes_per_query: usize,
}

pub fn run(profile: Profile) -> Result<Report> {
    let materialize = Instant::now();
    let snapshot = common_snapshot()?;
    let table_materialize_ms = materialize.elapsed().as_secs_f64() * 1_000.0;
    let batches: &[usize] = match profile {
        Profile::Quick => &[1, 8],
        Profile::Full => &[1, 2, 4, 8, 16, 32],
    };
    let thread_counts: &[usize] = match profile {
        Profile::Quick => &[1],
        Profile::Full => &[1, 8],
    };
    let mut rows = Vec::new();
    for &threads in thread_counts {
        let evaluator = ParallelEvaluator::new(threads)?;
        for &batch in batches {
            rows.push(Row {
                threads_per_server: threads,
                batch,
                dense_xor_2: benchmark_dense(&snapshot, &evaluator, batch)?,
                visible_100: benchmark_visible(&snapshot, batch)?,
            });
        }
    }
    Ok(Report {
        schema: "defradb-cpu-snapshot-control-v1",
        hardware_threads: std::thread::available_parallelism().map_or(1, |value| value.get()),
        entries: ENTRIES,
        useful_row_bytes: USEFUL_ROW_BYTES,
        physical_row_bytes: PHYSICAL_ROW_BYTES,
        logical_table_bytes: ENTRIES * USEFUL_ROW_BYTES,
        physical_table_bytes: ENTRIES * PHYSICAL_ROW_BYTES,
        table_materialize_ms,
        rows,
        scope: "p50 of five samples after one warmup; exact common 120-byte records occupy 128 physical bytes with zero padding; two Dense replicas execute sequentially and aggregate is their sum; parallel is the slower replica; visible-100 returns all 100 useful rows; network, RPC, lookup-to-ordinal mapping, and queueing excluded",
    })
}

fn common_snapshot() -> Result<Snapshot> {
    let mut rows = vec![0u8; ENTRIES * PHYSICAL_ROW_BYTES];
    for (ordinal, row) in rows.chunks_exact_mut(PHYSICAL_ROW_BYTES).enumerate() {
        for limb in 0..8u32 {
            let base = ordinal as u32 ^ 0x9e37_79b9u32.wrapping_mul(limb + 1);
            let words = [
                mix32(base ^ 0xa5a5_a5a5),
                mix32(base ^ 0x3c6e_f372),
                mix32(base ^ 0xdaa6_6d2b),
                mix32(base ^ 0x78dd_e6e4),
            ];
            let count = if limb == 7 { 2 } else { 4 };
            for (word, value) in words.into_iter().take(count).enumerate() {
                let offset = limb as usize * 16 + word * 4;
                row[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            }
        }
    }
    Snapshot::research_benchmark_from_rows(rows, PHYSICAL_ROW_BYTES, "defra-common-120-byte-v1")
}

fn mix32(mut value: u32) -> u32 {
    value ^= value >> 16;
    value = value.wrapping_mul(0x7feb_352d);
    value ^= value >> 15;
    value = value.wrapping_mul(0x846c_a68b);
    value ^ (value >> 16)
}

fn benchmark_dense(
    snapshot: &Snapshot,
    evaluator: &ParallelEvaluator,
    batch: usize,
) -> Result<Measurement> {
    let targets = targets(batch);
    let mut rng = OsRng;
    let query_started = Instant::now();
    let per_query = targets
        .iter()
        .map(|&target| dense::query_shares(target, ENTRIES, 2, &mut rng))
        .collect::<Result<Vec<_>>>()?;
    let client_query_ms = query_started.elapsed().as_secs_f64() * 1_000.0;
    let server_queries = transpose(&per_query);

    let first = answer_replicas(snapshot, evaluator, &server_queries)?;
    verify(snapshot, &targets, &first.answers)?;
    let _ = answer_replicas(snapshot, evaluator, &server_queries)?;

    let mut aggregate = Vec::with_capacity(SAMPLES);
    let mut parallel = Vec::with_capacity(SAMPLES);
    let mut last = None;
    for _ in 0..SAMPLES {
        let result = answer_replicas(snapshot, evaluator, &server_queries)?;
        aggregate.push(result.server_ms[0] + result.server_ms[1]);
        parallel.push(result.server_ms[0].max(result.server_ms[1]));
        last = Some(result.answers);
    }
    let answers = last.context("Dense CPU benchmark produced no answer")?;
    let recover_started = Instant::now();
    verify(snapshot, &targets, &answers)?;
    let recover_ms = recover_started.elapsed().as_secs_f64() * 1_000.0;
    Ok(Measurement {
        first_online_aggregate_server_ms_per_query: first.server_ms.iter().sum::<f64>()
            / batch as f64,
        aggregate_server_p50_ms_per_query: median(&mut aggregate) / batch as f64,
        parallel_server_p50_ms_per_query: median(&mut parallel) / batch as f64,
        client_query_p50_ms_per_query: client_query_ms / batch as f64,
        client_recover_p50_ms_per_query: recover_ms / batch as f64,
        aggregate_upload_bytes_per_query: 2 * dense::query_size(ENTRIES),
        aggregate_download_bytes_per_query: 2 * USEFUL_ROW_BYTES,
    })
}

struct ReplicaAnswers {
    answers: Vec<Vec<Vec<u8>>>,
    server_ms: [f64; 2],
}

fn answer_replicas(
    snapshot: &Snapshot,
    evaluator: &ParallelEvaluator,
    queries: &[Vec<&[u8]>],
) -> Result<ReplicaAnswers> {
    let mut answers = Vec::with_capacity(2);
    let mut server_ms = [0.0; 2];
    for server in 0..2 {
        let started = Instant::now();
        answers.push(evaluator.answer_batch(snapshot.view(), &queries[server])?);
        server_ms[server] = started.elapsed().as_secs_f64() * 1_000.0;
    }
    black_box(&answers);
    Ok(ReplicaAnswers { answers, server_ms })
}

fn verify(snapshot: &Snapshot, targets: &[usize], answers: &[Vec<Vec<u8>>]) -> Result<()> {
    for (query, &target) in targets.iter().enumerate() {
        let value = dense::combine(&[&answers[0][query], &answers[1][query]])?;
        anyhow::ensure!(value == snapshot.row(target)?, "Dense CPU answer mismatch");
        black_box(value);
    }
    Ok(())
}

fn benchmark_visible(snapshot: &Snapshot, batch: usize) -> Result<VisibleMeasurement> {
    let targets = targets(batch);
    let candidates = targets
        .iter()
        .map(|&target| {
            (0..100)
                .map(|index| {
                    if index == 0 {
                        target
                    } else {
                        (target + index * 104_729) % ENTRIES
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let run = || -> Result<Vec<u8>> {
        let mut output = Vec::with_capacity(batch * 100 * PHYSICAL_ROW_BYTES);
        for query in &candidates {
            for &candidate in query {
                output.extend_from_slice(snapshot.row(candidate)?);
            }
        }
        Ok(output)
    };
    let _ = run()?;
    let mut server = Vec::with_capacity(SAMPLES);
    let mut response = Vec::new();
    for _ in 0..SAMPLES {
        let started = Instant::now();
        response = run()?;
        server.push(started.elapsed().as_secs_f64() * 1_000.0);
    }
    let filter_started = Instant::now();
    for (query, &target) in targets.iter().enumerate() {
        let start = query * 100 * PHYSICAL_ROW_BYTES;
        anyhow::ensure!(
            &response[start..start + USEFUL_ROW_BYTES]
                == &snapshot.row(target)?[..USEFUL_ROW_BYTES]
        );
    }
    black_box(&response);
    Ok(VisibleMeasurement {
        server_p50_ms_per_query: median(&mut server) / batch as f64,
        client_filter_p50_ms_per_query: filter_started.elapsed().as_secs_f64() * 1_000.0
            / batch as f64,
        upload_bytes_per_query: 100 * size_of::<u64>(),
        download_bytes_per_query: 100 * USEFUL_ROW_BYTES,
    })
}

fn targets(batch: usize) -> Vec<usize> {
    (0..batch)
        .map(|index| (0x12345usize + index * 1_000_003) % ENTRIES)
        .collect()
}

fn transpose(per_query: &[Vec<Vec<u8>>]) -> Vec<Vec<&[u8]>> {
    (0..2)
        .map(|server| {
            per_query
                .iter()
                .map(|shares| shares[server].as_slice())
                .collect()
        })
        .collect()
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}
