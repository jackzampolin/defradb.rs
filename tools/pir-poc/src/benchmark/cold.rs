use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rand::seq::SliceRandom;
use rand::{rngs::StdRng, SeedableRng};
use rayon::prelude::*;

use super::report::{
    ColdPathBenchmarkReport, ColdPathWorkload, FiniteDifferencesResult, IndexedDecoyResult,
    LegacyPagedLayoutResult, PackedDenseResult,
};
use super::Profile;
use crate::dense::{self, ParallelEvaluator};
use crate::finite_differences::{self, ClientQuery, EncodedDatabase, Parameters};
use crate::snapshot::page_key;
use crate::tag_pages::{benchmark_tag, TagPageConfig, TagPageSnapshot};

const SERVER_COUNT: usize = 2;
const SERVER_WORKERS: usize = 2;
const CANDIDATE_BUCKETS: usize = 2;
const DECOY_COUNT: usize = 100;
const METHODOLOGY: &str = "One logical lookup targets page zero of a synthetic tag workload. Packed Dense privately retrieves both public cuckoo candidates from two non-colluding servers. Finite-differences PIR preprocesses exactly those packed bucket rows and privately retrieves the same two candidates; every Pareto variant fitting the profile memory limit is executed, while larger variants report analytical costs. Indexed decoys issue 100 ordinary public tag lookups and pad every first-page response. Timings exclude HTTP, TLS, serialization, and network latency.";

pub fn run(profile: Profile) -> Result<ColdPathBenchmarkReport> {
    let (document_count, distinct_tag_count) = match profile {
        Profile::Quick => (1 << 20, 1 << 18),
        Profile::Full => (1 << 22, 1 << 20),
    };
    let config = TagPageConfig {
        bucket_capacity: 4,
        target_load_percent: 90,
        values_per_page: 4,
        max_value_bytes: 16,
    };

    let legacy_paged_layout = legacy_layout(document_count, &config)?;
    let build_started = Instant::now();
    let snapshot = Arc::new(TagPageSnapshot::benchmark(
        document_count,
        distinct_tag_count,
        config.clone(),
    )?);
    let build_ms = millis(build_started.elapsed());
    let target_tag = benchmark_tag(distinct_tag_count / 3);
    let packed_dense =
        benchmark_packed_dense(Arc::clone(&snapshot), &target_tag, profile, build_ms)?;
    let indexed_decoys = benchmark_decoys(&snapshot, distinct_tag_count, profile)?;
    let finite_differences =
        benchmark_finite_differences(Arc::clone(&snapshot), &target_tag, profile, &packed_dense)?;

    Ok(ColdPathBenchmarkReport {
        protocol: "cold-private-tag-lookup-comparison",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: METHODOLOGY,
        workload: ColdPathWorkload {
            document_count,
            distinct_tag_count,
            documents_per_tag: document_count / distinct_tag_count,
            values_per_page: config.values_per_page,
            locator_bytes: config.max_value_bytes,
            decoy_count: DECOY_COUNT,
        },
        legacy_paged_layout,
        packed_dense,
        finite_differences,
        indexed_decoys,
    })
}

fn legacy_layout(document_count: usize, config: &TagPageConfig) -> Result<LegacyPagedLayoutResult> {
    let encoded_key_bytes = page_key(&benchmark_tag(0), 0)?.len();
    let bucket_capacity = 8usize;
    let row_size = bucket_capacity
        .checked_mul(6 + encoded_key_bytes + config.max_value_bytes)
        .context("legacy row size overflow")?;
    let bucket_count = document_count
        .checked_mul(2)
        .context("legacy bucket count overflow")?
        .next_power_of_two();
    Ok(LegacyPagedLayoutResult {
        description: "existing one-hash build_paged layout, configured for compact locators",
        bucket_count,
        bucket_capacity,
        row_size,
        estimated_snapshot_bytes: bucket_count
            .checked_mul(row_size)
            .context("legacy snapshot size overflow")?,
        query_bytes_per_server_per_page: dense::query_size(bucket_count),
        note: "Estimated rather than allocated: the current builder sizes from document records, repeats the page key per value, reserves eight value slots in every bucket, and can fail on an overloaded one-hash bucket.",
    })
}

fn benchmark_packed_dense(
    snapshot: Arc<TagPageSnapshot>,
    target_tag: &[u8],
    profile: Profile,
    build_ms: f64,
) -> Result<PackedDenseResult> {
    let buckets = snapshot.candidate_buckets(target_tag, 0)?;
    let evaluators = (0..SERVER_COUNT)
        .map(|_| ParallelEvaluator::new(SERVER_WORKERS))
        .collect::<Result<Vec<_>>>()?;
    let samples = sample_count(profile);
    let mut rng = StdRng::seed_from_u64(0xc01d_d3e5);
    let mut query_generation = Vec::with_capacity(samples);
    let mut wall = Vec::with_capacity(samples);
    let mut server_elapsed = Vec::with_capacity(samples);
    let mut reconstruction = Vec::with_capacity(samples);

    for _ in 0..samples {
        let query_started = Instant::now();
        let queries = dense_queries(&buckets, snapshot.manifest.bucket_count, &mut rng)?;
        query_generation.push(query_started.elapsed());
        let evaluation = evaluate_dense(&snapshot, &evaluators, &queries)?;
        wall.push(evaluation.wall);
        server_elapsed.push(evaluation.sum_server_elapsed);

        let reconstruction_started = Instant::now();
        let mut found = None;
        for candidate in 0..CANDIDATE_BUCKETS {
            let shares = evaluation
                .answers
                .iter()
                .map(|answers| answers[candidate].as_slice())
                .collect::<Vec<_>>();
            let row = dense::combine(&shares)?;
            if let Some(page) = snapshot.decode_bucket_row(&row, target_tag, 0)? {
                found = Some(page);
            }
        }
        let found = found.context("packed Dense did not recover the target page")?;
        if found.values.len() != snapshot.manifest.values_per_page {
            bail!("packed Dense recovered the wrong page");
        }
        reconstruction.push(reconstruction_started.elapsed());
    }
    sort_durations([
        &mut query_generation,
        &mut wall,
        &mut server_elapsed,
        &mut reconstruction,
    ]);

    let expected_rows = CANDIDATE_BUCKETS * snapshot.manifest.bucket_count.div_ceil(2);
    Ok(PackedDenseResult {
        privacy: "exact tag privacy if the two Dense servers do not collude; both candidate buckets and both server answers are required",
        build_ms,
        distinct_tag_count: snapshot.manifest.distinct_tag_count,
        page_count: snapshot.manifest.page_count,
        bucket_count: snapshot.manifest.bucket_count,
        bucket_capacity: snapshot.manifest.bucket_capacity,
        table_load_factor: snapshot.manifest.load_factor(),
        page_size: snapshot.manifest.page_size,
        row_size: snapshot.manifest.row_size,
        snapshot_bytes_per_server: snapshot.rows().len(),
        cold_client_metadata_bytes: snapshot.manifest.client_metadata_bytes(),
        candidate_bucket_queries_per_tag_page: CANDIDATE_BUCKETS,
        expected_rows_processed_per_server: expected_rows,
        expected_data_bytes_processed_per_server: expected_rows * snapshot.manifest.row_size,
        query_bytes_per_server: CANDIDATE_BUCKETS
            * dense::query_size(snapshot.manifest.bucket_count),
        response_bytes_per_server: CANDIDATE_BUCKETS * snapshot.manifest.row_size,
        client_query_generation_p50_us: micros(percentile(&query_generation, 50)),
        co_located_wall_p50_ms: millis(percentile(&wall, 50)),
        co_located_wall_p95_ms: millis(percentile(&wall, 95)),
        sum_server_elapsed_p50_ms: millis(percentile(&server_elapsed, 50)),
        client_reconstruct_p50_us: micros(percentile(&reconstruction, 50)),
    })
}

struct DenseEvaluation {
    answers: Vec<Vec<Vec<u8>>>,
    wall: Duration,
    sum_server_elapsed: Duration,
}

fn dense_queries(
    buckets: &[usize; CANDIDATE_BUCKETS],
    bucket_count: usize,
    rng: &mut StdRng,
) -> Result<Vec<Vec<Vec<u8>>>> {
    let mut per_server = (0..SERVER_COUNT)
        .map(|_| Vec::with_capacity(CANDIDATE_BUCKETS))
        .collect::<Vec<_>>();
    for bucket in buckets {
        for (server, share) in per_server.iter_mut().zip(dense::query_shares(
            *bucket,
            bucket_count,
            SERVER_COUNT,
            rng,
        )?) {
            server.push(share);
        }
    }
    Ok(per_server)
}

fn evaluate_dense(
    snapshot: &TagPageSnapshot,
    evaluators: &[ParallelEvaluator],
    queries: &[Vec<Vec<u8>>],
) -> Result<DenseEvaluation> {
    let wall_started = Instant::now();
    let results = std::thread::scope(|scope| {
        evaluators
            .iter()
            .zip(queries)
            .map(|(evaluator, queries)| {
                scope.spawn(move || {
                    let started = Instant::now();
                    let answers = evaluator.answer_batch(snapshot.view(), queries)?;
                    Ok::<_, anyhow::Error>((started.elapsed(), answers))
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| handle.join().expect("packed Dense server panicked"))
            .collect::<Result<Vec<_>>>()
    })?;
    let wall = wall_started.elapsed();
    let sum_server_elapsed = results.iter().map(|(elapsed, _)| *elapsed).sum();
    Ok(DenseEvaluation {
        answers: results.into_iter().map(|(_, answers)| answers).collect(),
        wall,
        sum_server_elapsed,
    })
}

fn benchmark_finite_differences(
    snapshot: Arc<TagPageSnapshot>,
    target_tag: &[u8],
    profile: Profile,
    packed_dense: &PackedDenseResult,
) -> Result<Vec<FiniteDifferencesResult>> {
    let parameters = Parameters::pareto_variants(
        snapshot.manifest.bucket_count,
        snapshot.manifest.row_size,
        30,
    )?;
    let memory_limit = match profile {
        Profile::Quick => 512 * 1024 * 1024,
        Profile::Full => 2 * 1024 * 1024 * 1024,
    };
    let mut measurements = Vec::with_capacity(parameters.len());
    for variant in &parameters {
        let measurement = if variant.storage_bytes()? <= memory_limit {
            Some(benchmark_finite_variant(
                &snapshot,
                target_tag,
                profile,
                variant.clone(),
            )?)
        } else {
            None
        };
        measurements.push(measurement);
    }

    parameters
        .into_iter()
        .enumerate()
        .map(|(index, parameters)| {
            let storage = parameters.storage_bytes()?;
            let answer = parameters.answer_bytes()?;
            let measurement = measurements[index].as_ref();
            Ok(FiniteDifferencesResult {
                privacy: "exact information-theoretic privacy against either single server; the two servers colluding can recover the target",
                variables_m: parameters.variables_m,
                total_degree_d: parameters.total_degree_d,
                record_capacity: parameters.capacity,
                encoded_storage_bytes_per_server: storage,
                storage_amplification_vs_packed_dense: storage as f64
                    / packed_dense.snapshot_bytes_per_server as f64,
                cloud_rows_per_server_per_candidate: parameters.cloud_count,
                rows_processed_per_server_per_tag_page: parameters.cloud_count
                    * CANDIDATE_BUCKETS,
                data_bytes_processed_per_server_per_tag_page: answer * CANDIDATE_BUCKETS,
                query_bytes_per_server: parameters.query_bytes_per_server() * CANDIDATE_BUCKETS,
                response_bytes_per_server: answer * CANDIDATE_BUCKETS,
                measured: measurement.is_some(),
                preprocessing_ms: measurement.map(|value| value.preprocessing_ms),
                client_query_generation_p50_us: measurement
                    .map(|value| value.client_query_generation_p50_us),
                co_located_wall_p50_ms: measurement.map(|value| value.co_located_wall_p50_ms),
                co_located_wall_p95_ms: measurement.map(|value| value.co_located_wall_p95_ms),
                sum_server_elapsed_p50_ms: measurement
                    .map(|value| value.sum_server_elapsed_p50_ms),
                client_reconstruct_p50_ms: measurement
                    .map(|value| value.client_reconstruct_p50_ms),
                note: "Server preprocessing is snapshot-wide and reusable by every cold client. The POC stores identical encoded tables logically at both replicas but shares one allocation during the co-located benchmark. Query and response costs include both cuckoo candidates.",
            })
        })
        .collect()
}

struct FiniteMeasurement {
    preprocessing_ms: f64,
    client_query_generation_p50_us: f64,
    co_located_wall_p50_ms: f64,
    co_located_wall_p95_ms: f64,
    sum_server_elapsed_p50_ms: f64,
    client_reconstruct_p50_ms: f64,
}

fn benchmark_finite_variant(
    snapshot: &TagPageSnapshot,
    target_tag: &[u8],
    profile: Profile,
    parameters: Parameters,
) -> Result<FiniteMeasurement> {
    let preprocessing_started = Instant::now();
    let database = Arc::new(EncodedDatabase::encode(
        parameters.clone(),
        snapshot.rows(),
    )?);
    let preprocessing_ms = millis(preprocessing_started.elapsed());
    let cloud = Arc::new(parameters.cloud());
    let server_pools = (0..SERVER_COUNT)
        .map(|index| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(SERVER_WORKERS)
                .thread_name(move |worker| format!("finite-server-{index}-{worker}"))
                .build()
                .context("build finite-differences server pool")
        })
        .collect::<Result<Vec<_>>>()?;
    let buckets = snapshot.candidate_buckets(target_tag, 0)?;
    let samples = sample_count(profile);
    let mut rng = StdRng::seed_from_u64(0xf1d1_2026);
    let mut query_generation = Vec::with_capacity(samples);
    let mut wall = Vec::with_capacity(samples);
    let mut server_elapsed = Vec::with_capacity(samples);
    let mut reconstruction = Vec::with_capacity(samples);

    for _ in 0..samples {
        let query_started = Instant::now();
        let queries = buckets
            .map(|bucket| finite_differences::prepare_query(&parameters, bucket, &mut rng))
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        query_generation.push(query_started.elapsed());

        let evaluation = evaluate_finite(&database, &cloud, &server_pools, &queries)?;
        wall.push(evaluation.wall);
        server_elapsed.push(evaluation.sum_server_elapsed);
        let reconstruction_started = Instant::now();
        let mut found = None;
        for (candidate, query) in queries.iter().copied().enumerate() {
            let answers = [
                evaluation.answers[0][candidate].clone(),
                evaluation.answers[1][candidate].clone(),
            ];
            let row = finite_differences::recover(&parameters, &cloud, query, &answers)?;
            if let Some(page) = snapshot.decode_bucket_row(&row, target_tag, 0)? {
                found = Some(page);
            }
        }
        if found.is_none() {
            bail!("finite-differences PIR did not recover the target tag page");
        }
        reconstruction.push(reconstruction_started.elapsed());
    }
    sort_durations([
        &mut query_generation,
        &mut wall,
        &mut server_elapsed,
        &mut reconstruction,
    ]);
    Ok(FiniteMeasurement {
        preprocessing_ms,
        client_query_generation_p50_us: micros(percentile(&query_generation, 50)),
        co_located_wall_p50_ms: millis(percentile(&wall, 50)),
        co_located_wall_p95_ms: millis(percentile(&wall, 95)),
        sum_server_elapsed_p50_ms: millis(percentile(&server_elapsed, 50)),
        client_reconstruct_p50_ms: millis(percentile(&reconstruction, 50)),
    })
}

struct FiniteEvaluation {
    answers: Vec<Vec<Vec<u8>>>,
    wall: Duration,
    sum_server_elapsed: Duration,
}

fn evaluate_finite(
    database: &EncodedDatabase,
    cloud: &[u64],
    server_pools: &[rayon::ThreadPool],
    queries: &[ClientQuery],
) -> Result<FiniteEvaluation> {
    let wall_started = Instant::now();
    let results = std::thread::scope(|scope| {
        server_pools
            .iter()
            .enumerate()
            .map(|(server, pool)| {
                scope.spawn(move || {
                    let started = Instant::now();
                    let answers = pool.install(|| {
                        queries
                            .par_iter()
                            .map(|query| database.answer(cloud, query.server_queries[server]))
                            .collect::<Result<Vec<_>>>()
                    })?;
                    Ok::<_, anyhow::Error>((started.elapsed(), answers))
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| handle.join().expect("finite-differences server panicked"))
            .collect::<Result<Vec<_>>>()
    })?;
    Ok(FiniteEvaluation {
        wall: wall_started.elapsed(),
        sum_server_elapsed: results.iter().map(|(elapsed, _)| *elapsed).sum(),
        answers: results.into_iter().map(|(_, answers)| answers).collect(),
    })
}

fn benchmark_decoys(
    snapshot: &TagPageSnapshot,
    distinct_tag_count: usize,
    profile: Profile,
) -> Result<IndexedDecoyResult> {
    let target = distinct_tag_count / 3;
    let target_tag = benchmark_tag(target);
    let samples = match profile {
        Profile::Quick => 101,
        Profile::Full => 501,
    };
    let mut rng = StdRng::seed_from_u64(0xdec0_0100);
    let mut generation = Vec::with_capacity(samples);
    let mut server = Vec::with_capacity(samples);
    let mut selection = Vec::with_capacity(samples);
    for sample in 0..samples {
        let generation_started = Instant::now();
        let mut tags = (0..DECOY_COUNT)
            .map(|offset| {
                benchmark_tag((target + sample * DECOY_COUNT + offset) % distinct_tag_count)
            })
            .collect::<Vec<_>>();
        tags[0] = target_tag;
        tags.sort_unstable();
        tags.dedup();
        let mut fill = 1usize;
        while tags.len() < DECOY_COUNT {
            let candidate =
                benchmark_tag((target + distinct_tag_count - fill) % distinct_tag_count);
            if tags.binary_search(&candidate).is_err() {
                tags.push(candidate);
                tags.sort_unstable();
            }
            fill += 1;
        }
        tags.shuffle(&mut rng);
        generation.push(generation_started.elapsed());

        let server_started = Instant::now();
        let responses = tags
            .iter()
            .map(|tag| snapshot.public_lookup(tag).map(|values| (*tag, values)))
            .collect::<Result<Vec<_>>>()?;
        server.push(server_started.elapsed());

        let selection_started = Instant::now();
        let target_response = responses
            .iter()
            .find(|(tag, _)| tag == &target_tag)
            .context("decoy response omitted the target")?;
        if target_response.1.len() != snapshot.manifest.values_per_page {
            bail!("decoy lookup recovered the wrong target values");
        }
        selection.push(selection_started.elapsed());
    }
    sort_durations([&mut generation, &mut server, &mut selection]);
    Ok(IndexedDecoyResult {
        privacy: "weaker candidate-set privacy only: the server learns all 100 requested tags and knows the real tag is one of them",
        decoy_count: DECOY_COUNT,
        server_count: 1,
        query_bytes: DECOY_COUNT * size_of::<u64>(),
        padded_response_bytes: DECOY_COUNT * snapshot.manifest.page_size,
        client_query_generation_p50_us: micros(percentile(&generation, 50)),
        server_lookup_p50_ms: millis(percentile(&server, 50)),
        server_lookup_p95_ms: millis(percentile(&server, 95)),
        client_select_p50_us: micros(percentile(&selection, 50)),
        note: "Repeated requests can be intersected. Sending different decoy sets to two servers exposes the real tag through their intersection; sending the same set adds availability but no privacy.",
    })
}

fn sample_count(profile: Profile) -> usize {
    match profile {
        Profile::Quick => 3,
        Profile::Full => 7,
    }
}

fn sort_durations<const N: usize>(groups: [&mut Vec<Duration>; N]) {
    for values in groups {
        values.sort_unstable();
    }
}

fn percentile(values: &[Duration], percentile: usize) -> Duration {
    values[((values.len() - 1) * percentile).div_ceil(100)]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}
