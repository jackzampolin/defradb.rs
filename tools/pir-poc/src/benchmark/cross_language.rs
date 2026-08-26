use std::hint::black_box;
use std::time::{Duration, Instant};

use anyhow::Result;
use rand::rngs::OsRng;
use serde::Serialize;

use crate::dense;
use crate::profile::Profile;
use crate::snapshot::Snapshot;

const TABLE_SEED: u64 = 0x4d59_5df4_d0f3_3173;
const TARGET_STEP: u64 = 0x9e37_79b9_7f4a_7c15;
const DECOY_COUNT: usize = 100;
const MIN_CLIENT_SAMPLE: Duration = Duration::from_millis(10);
const MIN_SERVER_SAMPLE: Duration = Duration::from_millis(50);
const MAX_CLIENT_REPEATS: u32 = 1 << 20;

#[derive(Debug, Serialize)]
pub struct Report {
    pub schema: &'static str,
    pub language: &'static str,
    pub implementation: &'static str,
    pub profile: &'static str,
    pub table_seed: u64,
    pub timing: &'static str,
    pub workloads: Vec<WorkloadReport>,
}

#[derive(Debug, Serialize)]
pub struct WorkloadReport {
    pub name: &'static str,
    pub rows: usize,
    pub row_bytes: usize,
    pub batch_size: usize,
    pub samples: usize,
    pub table_checksum_fnv1a64: String,
    pub direct: Measurement,
    pub decoy_100: Measurement,
    pub dense_xor_2: Measurement,
    pub dense_xor_3: Measurement,
}

#[derive(Debug, Serialize)]
pub struct Measurement {
    pub client_query_p50_ms: f64,
    pub server_total_p50_ms: f64,
    pub client_finish_p50_ms: f64,
    pub upload_bytes: usize,
    pub download_bytes: usize,
    pub source_operand_bytes: usize,
}

#[derive(Clone, Copy)]
struct Workload {
    name: &'static str,
    rows: usize,
    row_bytes: usize,
    batch_size: usize,
    samples: usize,
}

pub fn run(profile: Profile) -> Result<Report> {
    let workloads = match profile {
        Profile::Quick => [
            Workload {
                name: "locator",
                rows: 1 << 18,
                row_bytes: 96,
                batch_size: 1,
                samples: 7,
            },
            Workload {
                name: "witness",
                rows: 1 << 15,
                row_bytes: 2_008,
                batch_size: 1,
                samples: 7,
            },
            Workload {
                name: "batch-16",
                rows: 1 << 16,
                row_bytes: 96,
                batch_size: 16,
                samples: 5,
            },
        ],
        Profile::Full => [
            Workload {
                name: "locator",
                rows: 1 << 20,
                row_bytes: 96,
                batch_size: 1,
                samples: 11,
            },
            Workload {
                name: "witness",
                rows: 1 << 16,
                row_bytes: 2_008,
                batch_size: 1,
                samples: 11,
            },
            Workload {
                name: "batch-16",
                rows: 1 << 18,
                row_bytes: 96,
                batch_size: 16,
                samples: 7,
            },
        ],
    };

    Ok(Report {
        schema: "defradb-pir-cross-language-v1",
        language: "rust",
        implementation: env!("CARGO_PKG_VERSION"),
        profile: profile.as_str(),
        table_seed: TABLE_SEED,
        timing: "p50; client phases run >=10ms, server paths >=50ms; replica work is summed",
        workloads: workloads
            .into_iter()
            .map(run_workload)
            .collect::<Result<_>>()?,
    })
}

fn run_workload(workload: Workload) -> Result<WorkloadReport> {
    let snapshot = Snapshot::benchmark(workload.rows, workload.row_bytes, TABLE_SEED)?;
    let checksum = format!("{:016x}", fnv1a64(snapshot.rows()));

    // One unreported pass faults pages and exercises every path before sampling.
    let _ = measure_direct(&snapshot, workload, 0)?;
    let _ = measure_decoys(&snapshot, workload, 0)?;
    let _ = measure_dense(&snapshot, workload, 2, 0)?;
    let _ = measure_dense(&snapshot, workload, 3, 0)?;

    let mut direct = Vec::with_capacity(workload.samples);
    let mut decoys = Vec::with_capacity(workload.samples);
    let mut dense_2 = Vec::with_capacity(workload.samples);
    let mut dense_3 = Vec::with_capacity(workload.samples);
    for sample in 1..=workload.samples {
        direct.push(measure_direct(&snapshot, workload, sample)?);
        decoys.push(measure_decoys(&snapshot, workload, sample)?);
        dense_2.push(measure_dense(&snapshot, workload, 2, sample)?);
        dense_3.push(measure_dense(&snapshot, workload, 3, sample)?);
    }

    Ok(WorkloadReport {
        name: workload.name,
        rows: workload.rows,
        row_bytes: workload.row_bytes,
        batch_size: workload.batch_size,
        samples: workload.samples,
        table_checksum_fnv1a64: checksum,
        direct: median_measurement(&mut direct),
        decoy_100: median_measurement(&mut decoys),
        dense_xor_2: median_measurement(&mut dense_2),
        dense_xor_3: median_measurement(&mut dense_3),
    })
}

fn measure_direct(snapshot: &Snapshot, workload: Workload, sample: usize) -> Result<Sample> {
    let targets = targets(workload, sample);
    let (response, server_total) = measure_server_repeated(|| {
        targets
            .iter()
            .map(|&target| snapshot.row(target).map(<[u8]>::to_vec))
            .collect::<Result<Vec<_>>>()
    })?;

    let (_, client_query) = measure_repeated(|| {
        Ok(targets
            .iter()
            .map(|target| target.to_le_bytes())
            .collect::<Vec<_>>())
    })?;

    let (_, client_finish) = measure_repeated(|| {
        for (answer, &target) in response.iter().zip(&targets) {
            anyhow::ensure!(answer == snapshot.row(target)?, "direct answer mismatch");
        }
        black_box(&response);
        Ok(())
    })?;

    Ok(Sample {
        client_query,
        server_total,
        client_finish,
        upload_bytes: workload.batch_size * size_of::<u64>(),
        download_bytes: workload.batch_size * workload.row_bytes,
        source_operand_bytes: workload.batch_size * workload.row_bytes,
    })
}

fn measure_decoys(snapshot: &Snapshot, workload: Workload, sample: usize) -> Result<Sample> {
    let targets = targets(workload, sample);
    let candidate_sets = decoy_candidates(&targets, workload);

    let (response, server_total) = measure_server_repeated(|| {
        let mut result = Vec::with_capacity(workload.batch_size * DECOY_COUNT * workload.row_bytes);
        for candidates in &candidate_sets {
            for &candidate in candidates {
                result.extend_from_slice(snapshot.row(candidate)?);
            }
        }
        Ok(result)
    })?;

    let (_, client_query) = measure_repeated(|| Ok(decoy_candidates(&targets, workload)))?;

    let (_, client_finish) = measure_repeated(|| {
        for (batch_index, &target) in targets.iter().enumerate() {
            let offset = batch_index * DECOY_COUNT * workload.row_bytes;
            anyhow::ensure!(
                &response[offset..offset + workload.row_bytes] == snapshot.row(target)?,
                "decoy answer mismatch"
            );
        }
        black_box(&response);
        Ok(())
    })?;

    Ok(Sample {
        client_query,
        server_total,
        client_finish,
        upload_bytes: workload.batch_size * DECOY_COUNT * size_of::<u64>(),
        download_bytes: workload.batch_size * DECOY_COUNT * workload.row_bytes,
        source_operand_bytes: workload.batch_size * DECOY_COUNT * workload.row_bytes,
    })
}

fn measure_dense(
    snapshot: &Snapshot,
    workload: Workload,
    server_count: usize,
    sample: usize,
) -> Result<Sample> {
    let targets = targets(workload, sample);
    let mut rng = OsRng;
    let per_query = targets
        .iter()
        .map(|&target| dense::query_shares(target, workload.rows, server_count, &mut rng))
        .collect::<Result<Vec<_>>>()?;
    let queries = transpose_queries(&per_query, server_count);

    let mut server_total = Duration::ZERO;
    let mut answers = Vec::with_capacity(server_count);
    for server_queries in &queries {
        let (server_answers, elapsed) =
            measure_server_repeated(|| dense::answer_batch(snapshot.view(), server_queries))?;
        server_total += elapsed;
        answers.push(server_answers);
    }
    black_box(&answers);

    let (_, client_query) = measure_repeated(|| {
        targets
            .iter()
            .map(|&target| dense::query_shares(target, workload.rows, server_count, &mut rng))
            .collect::<Result<Vec<_>>>()
    })?;

    let (_, client_finish) = measure_repeated(|| {
        for (batch_index, &target) in targets.iter().enumerate() {
            let shares = answers
                .iter()
                .map(|server_answers| server_answers[batch_index].as_slice())
                .collect::<Vec<_>>();
            let answer = dense::combine(&shares)?;
            anyhow::ensure!(answer == snapshot.row(target)?, "Dense XOR answer mismatch");
            black_box(answer);
        }
        Ok(())
    })?;

    Ok(Sample {
        client_query,
        server_total,
        client_finish,
        upload_bytes: server_count * workload.batch_size * dense::query_size(workload.rows),
        download_bytes: server_count * workload.batch_size * workload.row_bytes,
        source_operand_bytes: server_count
            * workload.batch_size
            * workload.rows.div_ceil(2)
            * workload.row_bytes,
    })
}

fn decoy_candidates(targets: &[usize], workload: Workload) -> Vec<Vec<usize>> {
    targets
        .iter()
        .map(|&target| {
            (0..DECOY_COUNT)
                .map(|candidate| {
                    if candidate == 0 {
                        target
                    } else {
                        (target + candidate * 104_729) % workload.rows
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn transpose_queries(per_query: &[Vec<Vec<u8>>], server_count: usize) -> Vec<Vec<&[u8]>> {
    (0..server_count)
        .map(|server| {
            per_query
                .iter()
                .map(|shares| shares[server].as_slice())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn measure_repeated<T>(mut operation: impl FnMut() -> Result<T>) -> Result<(T, Duration)> {
    measure_repeated_for(MIN_CLIENT_SAMPLE, &mut operation)
}

fn measure_server_repeated<T>(mut operation: impl FnMut() -> Result<T>) -> Result<(T, Duration)> {
    let _ = operation()?;
    measure_repeated_for(MIN_SERVER_SAMPLE, &mut operation)
}

fn measure_repeated_for<T>(
    minimum: Duration,
    mut operation: impl FnMut() -> Result<T>,
) -> Result<(T, Duration)> {
    let mut repeats = 1;
    loop {
        let start = Instant::now();
        let mut result = operation()?;
        for _ in 1..repeats {
            result = operation()?;
        }
        let elapsed = start.elapsed();
        black_box(&result);
        if elapsed >= minimum || repeats == MAX_CLIENT_REPEATS {
            return Ok((result, elapsed / repeats));
        }
        repeats = (repeats * 2).min(MAX_CLIENT_REPEATS);
    }
}

fn targets(workload: Workload, sample: usize) -> Vec<usize> {
    (0..workload.batch_size)
        .map(|batch| {
            let ordinal = (sample * workload.batch_size + batch + 1) as u64;
            (TABLE_SEED.wrapping_add(ordinal.wrapping_mul(TARGET_STEP)) as usize) % workload.rows
        })
        .collect()
}

#[derive(Clone, Copy)]
struct Sample {
    client_query: Duration,
    server_total: Duration,
    client_finish: Duration,
    upload_bytes: usize,
    download_bytes: usize,
    source_operand_bytes: usize,
}

fn median_measurement(samples: &mut [Sample]) -> Measurement {
    let middle = samples.len() / 2;
    let median = |duration: fn(&Sample) -> Duration| {
        let mut values = samples.iter().map(duration).collect::<Vec<_>>();
        values.sort_unstable();
        values[middle].as_secs_f64() * 1_000.0
    };
    let sample = samples[0];
    Measurement {
        client_query_p50_ms: median(|sample| sample.client_query),
        server_total_p50_ms: median(|sample| sample.server_total),
        client_finish_p50_ms: median(|sample| sample.client_finish),
        upload_bytes: sample.upload_bytes,
        download_bytes: sample.download_bytes,
        source_operand_bytes: sample.source_operand_bytes,
    }
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_sequence_is_stable() {
        let workload = Workload {
            name: "test",
            rows: 1 << 10,
            row_bytes: 32,
            batch_size: 3,
            samples: 1,
        };
        assert_eq!(targets(workload, 2), vec![518, 539, 560]);
    }

    #[test]
    fn deterministic_corpus_matches_the_go_port() {
        let snapshot = Snapshot::benchmark(32, 32, 1).unwrap();
        assert_eq!(fnv1a64(snapshot.rows()), 0x3cff_aeb6_9428_cab5);
    }
}
