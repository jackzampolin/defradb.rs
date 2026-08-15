use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use rand::{rngs::StdRng, SeedableRng};

use super::kernels::{self, XorIndex};
use super::report::{
    OptimizationBatchResult, OptimizationDimension, OptimizationIndexResult,
    OptimizationKernelResult, OptimizationReport,
};
use super::Profile;
use crate::dense::{self, ParallelEvaluator};
use crate::snapshot::Snapshot;

const METHODOLOGY: &str = "All kernels receive identical random query shares on the same immutable snapshot. Masked is the original constant-work reference. Set-bit kernels skip zero rows in the server-visible random share. SIMD uses runtime-detected AVX2 on x86/x86-64 and a portable byte loop elsewhere. Parallel scans use persistent, explicitly sized worker pools. Persistent indexes precompute every XOR combination for groups of 2-4 rows. Times exclude query generation, networking, pool construction, and index build unless stated.";

pub fn run(profile: Profile) -> Result<OptimizationReport> {
    let dimensions = match profile {
        Profile::Quick => vec![(1 << 20, 64), (1 << 22, 64)],
        Profile::Full => vec![(1 << 20, 64), (1 << 20, 256), (1 << 22, 64)],
    }
    .into_iter()
    .map(|(bucket_count, row_size)| benchmark_dimension(bucket_count, row_size, profile))
    .collect::<Result<Vec<_>>>()?;

    Ok(OptimizationReport {
        profile: format!("{profile:?}").to_lowercase(),
        methodology: METHODOLOGY,
        dimensions,
    })
}

fn benchmark_dimension(
    bucket_count: usize,
    row_size: usize,
    profile: Profile,
) -> Result<OptimizationDimension> {
    let snapshot = Snapshot::benchmark(bucket_count, row_size, 0x0f71_0123)?;
    let mut rng = StdRng::seed_from_u64(bucket_count as u64 ^ row_size as u64);
    let query = dense::query_shares(bucket_count / 3, bucket_count, 2, &mut rng)?.remove(0);
    let selected_rows = count_selected_rows(&query, bucket_count);
    let samples = sample_count(profile, snapshot.rows().len());

    let (masked_times, masked_answer) = measure(samples, || {
        kernels::masked(snapshot.view(), std::slice::from_ref(&query))
            .map(|mut answers| answers.pop().expect("one query produces one answer"))
    })?;
    let masked_p50 = percentile(&masked_times, 50);
    let mut scalar_kernels = vec![kernel_result(
        "masked-reference",
        &masked_times,
        masked_p50,
        snapshot.rows().len(),
    )];

    let (byte_times, byte_answer) = measure(samples, || {
        dense::answer_batch(snapshot.view(), std::slice::from_ref(&query))
            .map(|mut answers| answers.pop().expect("one query produces one answer"))
    })?;
    ensure_equal(&masked_answer, &byte_answer, "set-bits-byte")?;
    scalar_kernels.push(kernel_result(
        "set-bits-byte",
        &byte_times,
        masked_p50,
        selected_rows * row_size,
    ));

    let (word_times, word_answer) = measure(samples, || {
        kernels::words(snapshot.view(), std::slice::from_ref(&query))
            .map(|mut answers| answers.pop().expect("one query produces one answer"))
    })?;
    ensure_equal(&masked_answer, &word_answer, "set-bits-word")?;
    scalar_kernels.push(kernel_result(
        "set-bits-word",
        &word_times,
        masked_p50,
        selected_rows * row_size,
    ));

    let (simd_times, simd_answer) = measure(samples, || {
        kernels::simd(snapshot.view(), std::slice::from_ref(&query))
            .map(|mut answers| answers.pop().expect("one query produces one answer"))
    })?;
    ensure_equal(&masked_answer, &simd_answer, "set-bits-simd")?;
    scalar_kernels.push(kernel_result(
        "set-bits-simd",
        &simd_times,
        masked_p50,
        selected_rows * row_size,
    ));

    let available_threads = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    for thread_count in [2, 4, 8]
        .into_iter()
        .filter(|thread_count| *thread_count <= available_threads)
    {
        let evaluator = ParallelEvaluator::new(thread_count)?;
        let (times, answer) = measure(samples, || evaluator.answer(snapshot.view(), &query))?;
        ensure_equal(&masked_answer, &answer, "parallel-set-bits")?;
        scalar_kernels.push(kernel_result(
            format!("set-bits-parallel-{thread_count}"),
            &times,
            masked_p50,
            selected_rows * row_size,
        ));
    }

    let mut persistent_indexes = Vec::new();
    if bucket_count <= 1 << 20 {
        for group_bits in 2..=4 {
            let build_started = Instant::now();
            let index = XorIndex::build(snapshot.view(), group_bits)?;
            let build_ms = millis(build_started.elapsed());
            let selected_combinations =
                count_selected_combinations(&query, bucket_count, group_bits);
            let (times, answer) = measure(samples, || index.answer(&query))?;
            ensure_equal(&masked_answer, &answer, "persistent-index")?;
            let p50 = percentile(&times, 50);
            persistent_indexes.push(OptimizationIndexResult {
                group_bits,
                build_ms,
                index_bytes: index.storage_bytes(),
                storage_amplification: index.storage_amplification(snapshot.rows().len()),
                p50_ms: millis(p50),
                p95_ms: millis(percentile(&times, 95)),
                speedup_vs_masked: masked_p50.as_secs_f64() / p50.as_secs_f64(),
                selected_combinations,
                estimated_data_bytes_read: selected_combinations * row_size,
            });
        }
    }

    let batch_sizes: &[usize] = match (profile, bucket_count) {
        (Profile::Quick, count) if count > 1 << 20 => &[8, 32],
        (Profile::Quick, _) => &[2, 8, 32],
        (Profile::Full, _) => &[2, 4, 8, 16, 32],
    };
    let batches = batch_sizes
        .iter()
        .copied()
        .map(|batch_size| {
            benchmark_batch(
                &snapshot,
                bucket_count,
                row_size,
                batch_size,
                samples.min(5),
                &mut rng,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(OptimizationDimension {
        bucket_count,
        row_size,
        snapshot_bytes: snapshot.rows().len(),
        query_share_bytes: query.len(),
        selected_rows,
        scalar_kernels,
        persistent_indexes,
        batches,
    })
}

fn benchmark_batch(
    snapshot: &Snapshot,
    bucket_count: usize,
    row_size: usize,
    batch_size: usize,
    samples: usize,
    rng: &mut StdRng,
) -> Result<OptimizationBatchResult> {
    let queries = (0..batch_size)
        .map(|index| {
            dense::query_shares((index * 7_919 + 1_234) % bucket_count, bucket_count, 2, rng)
                .map(|mut shares| shares.remove(0))
        })
        .collect::<Result<Vec<_>>>()?;
    let selected_rows = queries
        .iter()
        .map(|query| count_selected_rows(query, bucket_count))
        .sum::<usize>();

    let (masked_times, reference) =
        measure(samples, || kernels::masked(snapshot.view(), &queries))?;
    let masked_p50 = percentile(&masked_times, 50);
    let mut kernels = vec![kernel_result(
        "masked-reference",
        &masked_times,
        masked_p50,
        snapshot.rows().len() * batch_size,
    )];

    let (set_bit_times, set_bit_answers) =
        measure(samples, || kernels::words(snapshot.view(), &queries))?;
    ensure_equal(&reference, &set_bit_answers, "batch-set-bits")?;
    kernels.push(kernel_result(
        "set-bits-word",
        &set_bit_times,
        masked_p50,
        selected_rows * row_size,
    ));

    let (byte_times, byte_answers) =
        measure(samples, || dense::answer_batch(snapshot.view(), &queries))?;
    ensure_equal(&reference, &byte_answers, "batch-set-bits-byte")?;
    kernels.push(kernel_result(
        "set-bits-byte",
        &byte_times,
        masked_p50,
        selected_rows * row_size,
    ));

    let (simd_times, simd_answers) = measure(samples, || kernels::simd(snapshot.view(), &queries))?;
    ensure_equal(&reference, &simd_answers, "batch-set-bits-simd")?;
    kernels.push(kernel_result(
        "set-bits-simd",
        &simd_times,
        masked_p50,
        selected_rows * row_size,
    ));

    let available_threads = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    let parallel_threads = available_threads.min(4);
    if parallel_threads >= 2 {
        let evaluator = ParallelEvaluator::new(parallel_threads)?;
        let (parallel_times, parallel_answers) = measure(samples, || {
            evaluator.answer_batch(snapshot.view(), &queries)
        })?;
        ensure_equal(&reference, &parallel_answers, "batch-parallel-set-bits")?;
        kernels.push(kernel_result(
            format!("set-bits-parallel-{parallel_threads}"),
            &parallel_times,
            masked_p50,
            selected_rows * row_size,
        ));
    }

    let (table_times, table_answers) = measure(samples, || {
        kernels::four_russians(snapshot.view(), &queries)
    })?;
    ensure_equal(&reference, &table_answers, "batch-four-russians")?;
    let group_count = bucket_count.div_ceil(4);
    let estimated_table_bytes = group_count * (15 + batch_size) * row_size;
    kernels.push(kernel_result(
        "on-the-fly-four-russians",
        &table_times,
        masked_p50,
        estimated_table_bytes,
    ));

    Ok(OptimizationBatchResult {
        batch_size,
        kernels,
    })
}

fn measure<T, F>(samples: usize, mut operation: F) -> Result<(Vec<Duration>, T)>
where
    F: FnMut() -> Result<T>,
{
    std::hint::black_box(operation()?);
    let mut times = Vec::with_capacity(samples);
    let mut last = None;
    for _ in 0..samples {
        let started = Instant::now();
        last = Some(operation()?);
        times.push(started.elapsed());
        std::hint::black_box(last.as_ref());
    }
    times.sort_unstable();
    Ok((times, last.expect("at least one benchmark sample")))
}

fn kernel_result(
    name: impl Into<String>,
    times: &[Duration],
    masked_p50: Duration,
    estimated_data_bytes_read: usize,
) -> OptimizationKernelResult {
    let p50 = percentile(times, 50);
    OptimizationKernelResult {
        name: name.into(),
        p50_ms: millis(p50),
        p95_ms: millis(percentile(times, 95)),
        speedup_vs_masked: masked_p50.as_secs_f64() / p50.as_secs_f64(),
        estimated_data_bytes_read,
        effective_gib_per_second: estimated_data_bytes_read as f64
            / p50.as_secs_f64()
            / 1024f64.powi(3),
    }
}

fn count_selected_rows(query: &[u8], bucket_count: usize) -> usize {
    query
        .iter()
        .enumerate()
        .map(|(index, byte)| {
            let remaining = bucket_count.saturating_sub(index * 8).min(8);
            let mask = if remaining == 8 {
                u8::MAX
            } else {
                (1u8 << remaining) - 1
            };
            (byte & mask).count_ones() as usize
        })
        .sum()
}

fn count_selected_combinations(query: &[u8], bucket_count: usize, group_bits: usize) -> usize {
    (0..bucket_count.div_ceil(group_bits))
        .filter(|group| read_bits(query, group * group_bits, group_bits) != 0)
        .count()
}

fn read_bits(query: &[u8], bit_offset: usize, bit_count: usize) -> usize {
    let byte_index = bit_offset / 8;
    let shift = bit_offset % 8;
    let low = query.get(byte_index).copied().unwrap_or_default() as u16;
    let high = query.get(byte_index + 1).copied().unwrap_or_default() as u16;
    (((low | high << 8) >> shift) as usize) & ((1usize << bit_count) - 1)
}

fn ensure_equal<T: PartialEq>(expected: &T, actual: &T, name: &str) -> Result<()> {
    if expected != actual {
        bail!("{name} produced a different PIR answer")
    }
    Ok(())
}

fn sample_count(profile: Profile, snapshot_bytes: usize) -> usize {
    match (profile, snapshot_bytes) {
        (Profile::Quick, bytes) if bytes >= 256 * 1024 * 1024 => 3,
        (Profile::Quick, _) => 5,
        (Profile::Full, bytes) if bytes >= 256 * 1024 * 1024 => 5,
        (Profile::Full, _) => 11,
    }
}

fn percentile(values: &[Duration], percentile: usize) -> Duration {
    values[((values.len() - 1) * percentile).div_ceil(100)]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
