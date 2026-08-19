use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;

use super::{
    accounting::{
        unavailable_hardware_counters, AggregateWorkReport, AmortizationHorizon, ComparisonScope,
        LeakageScope, Metric, PhaseWork, SecurityLabels,
    },
    perf_gate::ServerPerfPhase,
    Profile,
};
use crate::{
    dense,
    dense_batch::{BatchEvaluator, BatchKernel, BatchMetrics},
    mphf_pages::MphfPageSnapshot,
    snapshot::SnapshotView,
    tag_pages::{benchmark_page_set, benchmark_tag, TagPageConfig},
};

const DOCUMENT_COUNT: usize = 1 << 20;
const DISTINCT_TAG_COUNT: usize = 1 << 18;
const SERVER_COUNTS: [usize; 2] = [2, 3];
const BATCH_SIZES: [usize; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
const MAX_WORKING_BYTES_PER_SERVER: usize = 64 * 1024 * 1024;
const CACHE_BLOCK_ROWS: usize = 2048;
const METHODOLOGY: &str = "Every measurement uses the identical populated 1,048,576-document/262,144-tag exact PtrHash MPHF table: 262,144 rows of 96 bytes, each holding one first page with four 16-byte locators. A batch contains K independent tags and independently generated n-out-of-n random XOR shares; batching changes only one server's local GF(2) evaluation order. Each server runs one single-core evaluator and replicas execute concurrently on this co-located host. Independent is the current query-major dense::answer_batch compatibility path. Shared-row-major visits each eight-row stripe before moving to the next; cache-blocked keeps a 2,048-row source block hot; selector-transposed creates per-stripe query bitmasks; grouped Four-Russians builds a non-persistent subset table per source-row group and performs fixed one-combination answer work per query/group. Every candidate is warmed once, then kernel order rotates by sample so fixed-order thermal/frequency drift does not consistently favor one candidate; every result is reconstructed and fingerprint-checked. Query creation, client reconstruction, server work, and wall time are recorded separately. Network, HTTP/TLS/serialization, queue wait, allocator metadata, physical memory traffic, and energy are excluded.";

#[derive(Debug, Serialize)]
pub struct DenseBatchReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: &'static str,
    pub workload: DenseBatchWorkload,
    pub batch_delay_assumption: BatchDelayAssumption,
    pub dimensions: Vec<DenseBatchDimension>,
    pub production_caveats: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct DenseBatchWorkload {
    pub document_count: usize,
    pub distinct_tag_count: usize,
    pub documents_per_tag: usize,
    pub table_rows: usize,
    pub row_bytes: usize,
    pub table_bytes_per_server: usize,
    pub useful_bytes_per_query: usize,
    pub query_share_bytes_per_query_per_server: usize,
    pub mphf_public_metadata_bytes: usize,
    pub corpus_build_wall_ms: f64,
    pub mphf_build_wall_ms: f64,
    pub mphf_client_metadata_load_ms: f64,
    pub tracked_mphf_peak_build_bytes: usize,
    pub generation: String,
}

#[derive(Debug, Serialize)]
pub struct BatchDelayAssumption {
    pub ready_queries_at_service_start: &'static str,
    pub queue_wait_included_in_latency: bool,
    pub assumed_arrival_rate: Option<f64>,
    pub assumed_max_queue_dwell_ms: Option<f64>,
    pub deployment_note: &'static str,
}

#[derive(Debug, Serialize)]
pub struct DenseBatchDimension {
    pub batch_size: usize,
    pub samples_per_kernel: usize,
    pub topologies: Vec<DenseBatchTopology>,
}

#[derive(Debug, Serialize)]
pub struct DenseBatchTopology {
    pub server_count: usize,
    pub privacy_collusion_tolerance: usize,
    pub required_answers: usize,
    pub client_query_generation_p50_ms: f64,
    pub kernels: Vec<DenseBatchKernelResult>,
    pub lowest_measured_aggregate_server_time_kernel: &'static str,
}

#[derive(Debug, Serialize)]
pub struct DenseBatchKernelResult {
    pub aggregate_work: AggregateWorkReport,
    pub kernel: &'static str,
    pub kernel_parameters: &'static str,
    pub aggregate_server_time_p50_ms: f64,
    pub aggregate_server_time_p95_ms: f64,
    pub per_server_time_p50_ms: Vec<f64>,
    pub per_server_time_p95_ms: Vec<f64>,
    pub co_located_wall_p50_ms: f64,
    pub co_located_wall_p95_ms: f64,
    pub aggregate_server_ms_per_query: f64,
    pub aggregate_server_queries_per_second: f64,
    pub co_located_queries_per_second: f64,
    pub aggregate_server_speedup_vs_independent: f64,
    pub table_ordering_passes_per_server_p50: usize,
    pub unique_selector_bytes_addressed_all_servers: usize,
    pub immutable_source_row_operand_reads_all_servers_p50: usize,
    pub immutable_source_operand_bytes_all_servers_p50: usize,
    pub scratch_row_copies_all_servers_p50: usize,
    pub scratch_row_xors_all_servers_p50: usize,
    pub scratch_write_bytes_all_servers_p50: usize,
    pub answer_row_xors_all_servers_p50: usize,
    pub answer_xor_write_bytes_all_servers_p50: usize,
    pub tracked_peak_transient_working_bytes_per_server: usize,
    pub tracked_peak_transient_working_memory_note: &'static str,
    pub total_client_upload_bytes: usize,
    pub total_client_download_bytes: usize,
    pub client_reconstruct_p50_ms: f64,
}

pub fn run(profile: Profile) -> Result<DenseBatchReport> {
    let profile_label = format!("{profile:?}").to_lowercase();
    let config = benchmark_config();
    let corpus_started = Instant::now();
    let page_set = benchmark_page_set(DOCUMENT_COUNT, DISTINCT_TAG_COUNT, &config)?;
    let corpus_build_wall_ms = millis(corpus_started.elapsed());
    let mphf_started = Instant::now();
    let snapshot = MphfPageSnapshot::from_page_set(&page_set, config.clone())?;
    let mphf_build_wall_ms = millis(mphf_started.elapsed());
    drop(page_set);

    let client_load_started = Instant::now();
    let client = snapshot.trusted_client_index()?;
    let client_metadata_load_ms = millis(client_load_started.elapsed());
    let all_tags = (0..*BATCH_SIZES.last().expect("batch sizes are non-empty"))
        .map(|index| benchmark_tag((index * 7919 + 1234) % DISTINCT_TAG_COUNT))
        .collect::<Vec<_>>();
    let all_ordinals = all_tags
        .iter()
        .map(|tag| client.ordinal(tag, 0))
        .collect::<Result<Vec<_>>>()?;

    let dimensions = BATCH_SIZES
        .into_iter()
        .map(|batch_size| {
            let samples = sample_count(profile, batch_size);
            let mut topologies = Vec::with_capacity(SERVER_COUNTS.len());
            for server_count in SERVER_COUNTS {
                topologies.push(benchmark_topology(
                    &snapshot,
                    &all_tags[..batch_size],
                    &all_ordinals[..batch_size],
                    batch_size,
                    server_count,
                    samples,
                    &profile_label,
                    corpus_build_wall_ms + mphf_build_wall_ms,
                    client_metadata_load_ms,
                )?);
            }
            Ok(DenseBatchDimension {
                batch_size,
                samples_per_kernel: samples,
                topologies,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(DenseBatchReport {
        protocol: "shared-scan-batched-exact-mphf-dense-xor",
        profile: profile_label,
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: METHODOLOGY,
        workload: DenseBatchWorkload {
            document_count: DOCUMENT_COUNT,
            distinct_tag_count: DISTINCT_TAG_COUNT,
            documents_per_tag: DOCUMENT_COUNT / DISTINCT_TAG_COUNT,
            table_rows: snapshot.manifest.page_count,
            row_bytes: snapshot.manifest.page_size,
            table_bytes_per_server: snapshot.rows().len(),
            useful_bytes_per_query: snapshot.manifest.values_per_page
                * snapshot.manifest.max_value_bytes,
            query_share_bytes_per_query_per_server: dense::query_size(
                snapshot.manifest.page_count,
            ),
            mphf_public_metadata_bytes: snapshot.manifest.client_metadata_bytes(),
            corpus_build_wall_ms,
            mphf_build_wall_ms,
            mphf_client_metadata_load_ms: client_metadata_load_ms,
            tracked_mphf_peak_build_bytes: snapshot.build_metrics.peak_tracked_bytes,
            generation: snapshot.manifest.generation_hex(),
        },
        batch_delay_assumption: BatchDelayAssumption {
            ready_queries_at_service_start: "all K independently shared queries are already queued when the timed server evaluation begins",
            queue_wait_included_in_latency: false,
            assumed_arrival_rate: None,
            assumed_max_queue_dwell_ms: None,
            deployment_note: "Batch service time is not user-observed latency. A production scheduler must choose and report a maximum dwell deadline, flush partial batches, and account for the actual arrival process separately.",
        },
        dimensions,
        production_caveats: vec![
            "Batching reduces aggregate server work only when enough independent requests overlap. It adds an arrival-dependent queue delay that this CPU benchmark intentionally does not invent or hide.",
            "Shared traversal preserves each query's independent n-out-of-n XOR sharing. It does not hide source IP, timing, batch membership, or which replicas a client contacted.",
            "The direct shared kernels reorder the same random-share-selected row XORs and primarily target locality; they do not reduce the logical answer arithmetic. Grouped Four-Russians can reduce answer XOR count for larger K but adds fixed scratch-table construction work.",
            "Set-bit kernels have share-Hamming-weight-dependent work. A semi-honest share is uniform and target-independent, but an unauthenticated malicious client can choose an expensive share. Grouped Four-Russians deliberately XORs even the zero combination so its answer work is fixed by K and dimensions.",
            "Tracked working memory includes answer payload and kernel scratch capacity only. It excludes Vec headers, allocator metadata, stacks, code, runtime state, and process RSS.",
            "Logical byte counters are software operand counts, not physical DRAM traffic. Cache lines, write allocation, prefetching, coherence, and energy require hardware counters on the deployment machine.",
            "All replicas are co-located and contend for one host's cores and memory hierarchy. Aggregate server time is the primary work metric; co-located wall time is not a distributed latency prediction.",
            "The lowest measured quick-profile kernel is a screening result, not a production default. Confirm it with the full profile, hardware counters, repeated process-level runs, and target server hardware before setting dispatch thresholds.",
        ],
    })
}

#[allow(clippy::too_many_arguments)]
fn benchmark_topology(
    snapshot: &MphfPageSnapshot,
    tags: &[[u8; 8]],
    ordinals: &[usize],
    batch_size: usize,
    server_count: usize,
    samples: usize,
    profile: &str,
    global_build_wall_ms: f64,
    client_metadata_load_ms: f64,
) -> Result<DenseBatchTopology> {
    let evaluator = BatchEvaluator::new(batch_size, MAX_WORKING_BYTES_PER_SERVER)?;
    let mut rng = StdRng::seed_from_u64(
        0x4241_5443_4800_0000 ^ (batch_size as u64) << 8 ^ server_count as u64,
    );
    let mut prepared = Vec::with_capacity(samples);
    let mut generation_times = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        let mut per_server = vec![Vec::with_capacity(batch_size); server_count];
        for &ordinal in ordinals {
            for (server, share) in per_server.iter_mut().zip(dense::query_shares(
                ordinal,
                snapshot.manifest.page_count,
                server_count,
                &mut rng,
            )?) {
                server.push(share);
            }
        }
        generation_times.push(started.elapsed());
        prepared.push(per_server);
    }
    let generation_p50_ms = duration_p50_ms(&generation_times);

    let kernels = candidate_kernels(batch_size);
    for &(_, _, kernel) in &kernels {
        let warm = evaluate_servers(snapshot.view(), &evaluator, &prepared[0], kernel, None)?;
        reconstruct(snapshot, tags, &warm.answers)?;
    }

    let mut measurements = (0..kernels.len())
        .map(|_| Vec::with_capacity(samples))
        .collect::<Vec<_>>();
    for (sample_index, queries) in prepared.iter().enumerate() {
        for offset in 0..kernels.len() {
            let kernel_index = (sample_index + offset) % kernels.len();
            let (kernel_name, _, kernel) = kernels[kernel_index];
            let perf_phase = ServerPerfPhase::dense_batch(
                profile,
                batch_size,
                server_count,
                kernel_name,
                sample_index,
            )?;
            let evaluation =
                evaluate_servers(snapshot.view(), &evaluator, queries, kernel, perf_phase)?;
            let reconstruct_started = Instant::now();
            reconstruct(snapshot, tags, &evaluation.answers)?;
            let reconstruct_elapsed = reconstruct_started.elapsed();
            measurements[kernel_index].push(KernelMeasurement {
                wall: evaluation.wall,
                server_elapsed: evaluation.server_elapsed,
                metrics: evaluation.metrics,
                reconstruct: reconstruct_elapsed,
                client_online: generation_times[sample_index] + reconstruct_elapsed,
            });
        }
    }

    let mut results = Vec::with_capacity(kernels.len());
    for ((name, parameters, _), measurements) in kernels.into_iter().zip(measurements) {
        results.push(summarize_kernel(
            snapshot,
            batch_size,
            server_count,
            name,
            parameters,
            measurements,
            global_build_wall_ms,
            client_metadata_load_ms,
        )?);
    }

    let independent_ms = results
        .iter()
        .find(|result| result.kernel == "independent-query-major")
        .context("Dense batch independent baseline is missing")?
        .aggregate_server_time_p50_ms;
    for result in &mut results {
        result.aggregate_server_speedup_vs_independent =
            independent_ms / result.aggregate_server_time_p50_ms;
    }
    let lowest = results
        .iter()
        .min_by(|left, right| {
            left.aggregate_server_time_p50_ms
                .total_cmp(&right.aggregate_server_time_p50_ms)
        })
        .context("Dense batch produced no kernel results")?
        .kernel;
    Ok(DenseBatchTopology {
        server_count,
        privacy_collusion_tolerance: server_count - 1,
        required_answers: server_count,
        client_query_generation_p50_ms: generation_p50_ms,
        kernels: results,
        lowest_measured_aggregate_server_time_kernel: lowest,
    })
}

fn candidate_kernels(batch_size: usize) -> Vec<(&'static str, &'static str, BatchKernel)> {
    let mut kernels = vec![
        (
            "independent-query-major",
            "current dense::answer_batch compatibility baseline; K table-ordering passes",
            BatchKernel::Independent,
        ),
        (
            "shared-row-major",
            "one eight-row stripe at a time across K queries",
            BatchKernel::SharedRowMajor,
        ),
        (
            "shared-cache-blocked",
            "2,048 rows (192 KiB) per source block, then K query-major scans inside the block",
            BatchKernel::CacheBlocked {
                rows_per_block: CACHE_BLOCK_ROWS,
            },
        ),
        (
            "shared-selector-transposed",
            "eight row masks over groups of 64 queries, rebuilt per selector byte",
            BatchKernel::SelectorTransposed,
        ),
    ];
    let group_bits: &[usize] = match batch_size {
        1..=8 => &[2],
        16 => &[3, 4],
        32 => &[4, 5],
        64 => &[4, 5, 6],
        _ => &[5, 6],
    };
    for &group_bits in group_bits {
        let (name, parameters) = match group_bits {
            2 => ("grouped-four-russians-g2", "ephemeral 4-row subset table"),
            3 => ("grouped-four-russians-g3", "ephemeral 8-row subset table"),
            4 => ("grouped-four-russians-g4", "ephemeral 16-row subset table"),
            5 => ("grouped-four-russians-g5", "ephemeral 32-row subset table"),
            6 => ("grouped-four-russians-g6", "ephemeral 64-row subset table"),
            _ => unreachable!("candidate group sizes are fixed"),
        };
        kernels.push((
            name,
            parameters,
            BatchKernel::GroupedFourRussians { group_bits },
        ));
    }
    kernels
}

struct ServerEvaluation {
    answers: Vec<Vec<Vec<u8>>>,
    wall: Duration,
    server_elapsed: Vec<Duration>,
    metrics: Vec<BatchMetrics>,
}

fn evaluate_servers(
    snapshot: SnapshotView<'_>,
    evaluator: &BatchEvaluator,
    queries: &[Vec<Vec<u8>>],
    kernel: BatchKernel,
    perf_phase: Option<std::sync::Arc<ServerPerfPhase>>,
) -> Result<ServerEvaluation> {
    let wall_started = Instant::now();
    let evaluations = std::thread::scope(|scope| -> Result<Vec<_>> {
        let handles = queries
            .iter()
            .enumerate()
            .map(|(server_index, server_queries)| {
                let perf_phase = perf_phase.clone();
                scope.spawn(move || {
                    let counters = perf_phase
                        .as_ref()
                        .map(|phase| phase.begin_server(server_index))
                        .transpose()?;
                    let started = Instant::now();
                    let evaluation = evaluator.evaluate(snapshot, server_queries, kernel);
                    let elapsed = started.elapsed();
                    if let Some(counters) = counters {
                        counters.finish()?;
                    }
                    Ok::<_, anyhow::Error>((elapsed, evaluation?))
                })
            })
            .collect::<Vec<_>>();
        let aggregate_control = perf_phase
            .as_ref()
            .map(|phase| phase.start_envelope())
            .transpose()?
            .flatten();
        let evaluations = handles
            .into_iter()
            .map(|handle| handle.join().expect("Dense batch server panicked"))
            .collect::<Result<Vec<_>>>();
        let finish = perf_phase
            .as_ref()
            .map(|phase| phase.finish_envelope(aggregate_control))
            .transpose();
        let evaluations = evaluations?;
        finish?;
        Ok(evaluations)
    })?;
    Ok(ServerEvaluation {
        wall: wall_started.elapsed(),
        server_elapsed: evaluations.iter().map(|(elapsed, _)| *elapsed).collect(),
        metrics: evaluations
            .iter()
            .map(|(_, evaluation)| evaluation.metrics.clone())
            .collect(),
        answers: evaluations
            .into_iter()
            .map(|(_, evaluation)| evaluation.answers)
            .collect(),
    })
}

fn reconstruct(
    snapshot: &MphfPageSnapshot,
    tags: &[[u8; 8]],
    answers: &[Vec<Vec<u8>>],
) -> Result<()> {
    for (query_index, tag) in tags.iter().enumerate() {
        let shares = answers
            .iter()
            .map(|server| server[query_index].as_slice())
            .collect::<Vec<_>>();
        let row = dense::combine(&shares)?;
        let page = snapshot
            .decode_retrieved_page(&row, tag, 0)?
            .context("Dense batch MPHF result failed fingerprint verification")?;
        if page.values.len() != snapshot.manifest.values_per_page {
            bail!("Dense batch recovered the wrong number of locators");
        }
    }
    Ok(())
}

struct KernelMeasurement {
    wall: Duration,
    server_elapsed: Vec<Duration>,
    metrics: Vec<BatchMetrics>,
    reconstruct: Duration,
    client_online: Duration,
}

#[allow(clippy::too_many_arguments)]
fn summarize_kernel(
    snapshot: &MphfPageSnapshot,
    batch_size: usize,
    server_count: usize,
    kernel: &'static str,
    kernel_parameters: &'static str,
    measurements: Vec<KernelMeasurement>,
    global_build_wall_ms: f64,
    client_metadata_load_ms: f64,
) -> Result<DenseBatchKernelResult> {
    let wall = measurements
        .iter()
        .map(|measurement| measurement.wall)
        .collect::<Vec<_>>();
    let aggregate = measurements
        .iter()
        .map(|measurement| measurement.server_elapsed.iter().sum())
        .collect::<Vec<Duration>>();
    let reconstruct = measurements
        .iter()
        .map(|measurement| measurement.reconstruct)
        .collect::<Vec<_>>();
    let client_online = measurements
        .iter()
        .map(|measurement| measurement.client_online)
        .collect::<Vec<_>>();
    let mut per_server = vec![Vec::with_capacity(measurements.len()); server_count];
    for measurement in &measurements {
        for (server, elapsed) in per_server.iter_mut().zip(&measurement.server_elapsed) {
            server.push(*elapsed);
        }
    }
    let aggregate_p50_ms = duration_p50_ms(&aggregate);
    let wall_p50_ms = duration_p50_ms(&wall);
    let per_server_p50_ms = per_server
        .iter()
        .map(|samples| duration_p50_ms(samples))
        .collect::<Vec<_>>();
    let per_server_p95_ms = per_server
        .iter()
        .map(|samples| duration_p95_ms(samples))
        .collect::<Vec<_>>();
    let source_bytes_per_server_p50 = (0..server_count)
        .map(|server_index| {
            let samples = measurements
                .iter()
                .map(|measurement| measurement.metrics[server_index].immutable_source_operand_bytes)
                .collect::<Vec<_>>();
            percentile_usize(&samples, 50)
        })
        .collect::<Vec<_>>();

    let metric_sums = measurements
        .iter()
        .map(|measurement| sum_metrics(&measurement.metrics))
        .collect::<Result<Vec<_>>>()?;
    let table_passes_all = metric_sums
        .iter()
        .map(|metrics| metrics.table_ordering_passes)
        .collect::<Vec<_>>();
    let selector_bytes = metric_sums
        .iter()
        .map(|metrics| metrics.unique_selector_bytes_addressed)
        .collect::<Vec<_>>();
    let source_rows = metric_sums
        .iter()
        .map(|metrics| metrics.immutable_source_row_operand_reads)
        .collect::<Vec<_>>();
    let source_bytes = metric_sums
        .iter()
        .map(|metrics| metrics.immutable_source_operand_bytes)
        .collect::<Vec<_>>();
    let scratch_copies = metric_sums
        .iter()
        .map(|metrics| metrics.scratch_row_copies)
        .collect::<Vec<_>>();
    let scratch_xors = metric_sums
        .iter()
        .map(|metrics| metrics.scratch_row_xors)
        .collect::<Vec<_>>();
    let scratch_bytes = metric_sums
        .iter()
        .map(|metrics| metrics.scratch_write_bytes)
        .collect::<Vec<_>>();
    let answer_rows = metric_sums
        .iter()
        .map(|metrics| metrics.answer_row_xors)
        .collect::<Vec<_>>();
    let answer_bytes = metric_sums
        .iter()
        .map(|metrics| metrics.answer_xor_write_bytes)
        .collect::<Vec<_>>();
    let working_per_server = measurements[0]
        .metrics
        .iter()
        .map(|metrics| metrics.peak_transient_working_bytes)
        .max()
        .unwrap_or_default();
    let query_bytes = dense::query_size(snapshot.manifest.page_count);
    let upload_bytes = batch_size
        .checked_mul(query_bytes)
        .and_then(|bytes| bytes.checked_mul(server_count))
        .context("Dense batch upload byte count overflow")?;
    let download_bytes = batch_size
        .checked_mul(snapshot.manifest.page_size)
        .and_then(|bytes| bytes.checked_mul(server_count))
        .context("Dense batch download byte count overflow")?;
    let table_passes_per_server = percentile_usize(&table_passes_all, 50) / server_count;
    let source_bytes_p50 = percentile_usize(&source_bytes, 50);
    let accounting = accounting(
        snapshot,
        batch_size,
        server_count,
        kernel,
        global_build_wall_ms,
        client_metadata_load_ms,
        &per_server_p50_ms,
        &source_bytes_per_server_p50,
        aggregate_p50_ms,
        wall_p50_ms,
        table_passes_per_server,
        source_bytes_p50,
        working_per_server,
        duration_p50_ms(&client_online),
        upload_bytes,
        download_bytes,
    )?;

    Ok(DenseBatchKernelResult {
        aggregate_work: accounting,
        kernel,
        kernel_parameters,
        aggregate_server_time_p50_ms: aggregate_p50_ms,
        aggregate_server_time_p95_ms: duration_p95_ms(&aggregate),
        per_server_time_p50_ms: per_server_p50_ms,
        per_server_time_p95_ms: per_server_p95_ms,
        co_located_wall_p50_ms: wall_p50_ms,
        co_located_wall_p95_ms: duration_p95_ms(&wall),
        aggregate_server_ms_per_query: aggregate_p50_ms / batch_size as f64,
        aggregate_server_queries_per_second: batch_size as f64 * 1000.0 / aggregate_p50_ms,
        co_located_queries_per_second: batch_size as f64 * 1000.0 / wall_p50_ms,
        aggregate_server_speedup_vs_independent: 1.0,
        table_ordering_passes_per_server_p50: table_passes_per_server,
        unique_selector_bytes_addressed_all_servers: percentile_usize(&selector_bytes, 50),
        immutable_source_row_operand_reads_all_servers_p50: percentile_usize(&source_rows, 50),
        immutable_source_operand_bytes_all_servers_p50: source_bytes_p50,
        scratch_row_copies_all_servers_p50: percentile_usize(&scratch_copies, 50),
        scratch_row_xors_all_servers_p50: percentile_usize(&scratch_xors, 50),
        scratch_write_bytes_all_servers_p50: percentile_usize(&scratch_bytes, 50),
        answer_row_xors_all_servers_p50: percentile_usize(&answer_rows, 50),
        answer_xor_write_bytes_all_servers_p50: percentile_usize(&answer_bytes, 50),
        tracked_peak_transient_working_bytes_per_server: working_per_server,
        tracked_peak_transient_working_memory_note: "deterministic answer payload plus kernel scratch capacity; excludes Vec headers, allocator metadata, stacks, runtime state, and process RSS",
        total_client_upload_bytes: upload_bytes,
        total_client_download_bytes: download_bytes,
        client_reconstruct_p50_ms: duration_p50_ms(&reconstruct),
    })
}

fn sum_metrics(metrics: &[BatchMetrics]) -> Result<BatchMetrics> {
    let mut sum = BatchMetrics::default();
    for metrics in metrics {
        sum.query_count = checked_add(sum.query_count, metrics.query_count)?;
        sum.query_share_bytes = metrics.query_share_bytes;
        sum.table_ordering_passes =
            checked_add(sum.table_ordering_passes, metrics.table_ordering_passes)?;
        sum.unique_selector_bytes_addressed = checked_add(
            sum.unique_selector_bytes_addressed,
            metrics.unique_selector_bytes_addressed,
        )?;
        sum.immutable_source_row_operand_reads = checked_add(
            sum.immutable_source_row_operand_reads,
            metrics.immutable_source_row_operand_reads,
        )?;
        sum.immutable_source_operand_bytes = checked_add(
            sum.immutable_source_operand_bytes,
            metrics.immutable_source_operand_bytes,
        )?;
        sum.scratch_row_copies = checked_add(sum.scratch_row_copies, metrics.scratch_row_copies)?;
        sum.scratch_row_xors = checked_add(sum.scratch_row_xors, metrics.scratch_row_xors)?;
        sum.scratch_write_bytes =
            checked_add(sum.scratch_write_bytes, metrics.scratch_write_bytes)?;
        sum.answer_row_xors = checked_add(sum.answer_row_xors, metrics.answer_row_xors)?;
        sum.answer_xor_write_bytes =
            checked_add(sum.answer_xor_write_bytes, metrics.answer_xor_write_bytes)?;
        sum.materialized_answer_bytes = checked_add(
            sum.materialized_answer_bytes,
            metrics.materialized_answer_bytes,
        )?;
        sum.peak_transient_working_bytes = sum
            .peak_transient_working_bytes
            .max(metrics.peak_transient_working_bytes);
    }
    Ok(sum)
}

fn checked_add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right)
        .context("Dense batch aggregate metric overflow")
}

#[allow(clippy::too_many_arguments)]
fn accounting(
    snapshot: &MphfPageSnapshot,
    batch_size: usize,
    server_count: usize,
    protocol: &'static str,
    _global_build_wall_ms: f64,
    client_metadata_load_ms: f64,
    per_server_p50_ms: &[f64],
    source_bytes_per_server: &[usize],
    aggregate_p50_ms: f64,
    wall_p50_ms: f64,
    table_passes_per_server: usize,
    source_bytes_all_servers: usize,
    working_bytes_per_server: usize,
    client_online_p50_ms: f64,
    upload_bytes: usize,
    download_bytes: usize,
) -> Result<AggregateWorkReport> {
    let mut work = AggregateWorkReport::new(
        protocol,
        ComparisonScope {
            workload: "one ready batch of K independent exact first-page tag lookups over the identical 1M-document MPHF corpus; queue wait excluded",
            result: "K independently fingerprint-validated pages, each containing four fixed-width compact locators",
            public_partition: "global immutable generation; generation-specific PtrHash metadata is public",
            leakage: LeakageScope::ExactQueryPrivacy,
        },
        SecurityLabels {
            privacy: "information-theoretic n-out-of-n XOR query privacy for every independent batch slot",
            server_count,
            collusion_tolerance: server_count - 1,
            required_answers: server_count,
            assumptions: "at least one replica does not collude; all servers use the same authenticated immutable generation; batching does not hide transport timing or membership",
            availability: "all answer shares are required for every batch slot",
            integrity: "128-bit page fingerprint rejects absent/wrong rows in the semi-honest model; no Byzantine proof or MAC",
        },
    );
    work.global_build.aggregate_server_time_ms = Metric::not_measured(
        "aggregate builder CPU time is not measured; corpus plus MPHF build wall is reported outside the accounting schema",
    );
    work.global_build.peak_server_ram_bytes = Metric::estimated(
        snapshot.build_metrics.peak_tracked_bytes,
        "tracked MPHF algorithm buffers; PtrHash transient workspace and runtime RSS are excluded",
    );
    work.global_build.client_time_ms = Metric::not_applicable("global build is server-side");
    work.global_build.client_upload_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.client_download_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.server_scans = Metric::not_measured(
        "layout build passes are not instrumented and are not online Dense scans",
    );
    work.global_build.network_rounds = Metric::not_applicable("build is server-local");
    let mut setup = PhaseWork::unmeasured(
        "one client loading one immutable generation",
        "metadata distribution server work and peak client RSS are not measured",
    );
    setup.client_time_ms = Metric::measured(
        client_metadata_load_ms,
        "validated artifact parsing and PtrHash deserialization",
    );
    setup.client_upload_bytes = Metric::deterministic(0, "public metadata fetch has no PIR upload");
    setup.client_download_bytes = Metric::deterministic(
        snapshot.manifest.client_metadata_bytes(),
        "one authenticated generation-specific public artifact",
    );
    setup.network_rounds = Metric::estimated(1, "one metadata fetch; network latency excluded");
    work.per_client_setup = setup;
    work.maintenance = PhaseWork::not_applicable(
        "immutable snapshot lifetime",
        "a changed snapshot is a new build and authenticated generation",
    );

    for ((server, &time_ms), &source_bytes) in work
        .online
        .per_server
        .iter_mut()
        .zip(per_server_p50_ms)
        .zip(source_bytes_per_server)
    {
        server.server_time_p50_ms =
            Metric::measured(time_ms, "retained per-replica p50 elapsed time");
        server.logical_selected_bytes = Metric::measured(
            source_bytes,
            "software-counted immutable source row operands; scratch and answer writes are reported outside the common schema",
        );
        server.physical_or_scanned_bytes =
            Metric::not_measured("physical cache/DRAM traffic requires hardware counters");
        server.scans = Metric::deterministic(
            table_passes_per_server,
            "query-major compatibility passes or one shared table-ordering traversal",
        );
    }
    work.online.unit = "one ready batch of K independent queries; no queue wait";
    work.online.aggregate_server_time_p50_ms = Metric::measured(
        aggregate_p50_ms,
        "p50 of the per-sample sum of all co-located replica elapsed times",
    );
    work.online.max_server_time_p50_ms = Metric::estimated(
        wall_p50_ms,
        "co-located wall p50 is an upper-envelope proxy including thread dispatch",
    );
    work.online.aggregate_logical_selected_bytes = Metric::measured(
        source_bytes_all_servers,
        "p50 software-counted immutable source row operand bytes across replicas",
    );
    work.online.aggregate_physical_or_scanned_bytes =
        Metric::not_measured("physical cache/DRAM traffic requires hardware counters");
    work.online.server_scans = Metric::deterministic(
        table_passes_per_server * server_count,
        "sum of table-ordering traversals across replicas",
    );
    work.online.network_rounds = Metric::deterministic(1, "one request/response round per replica");
    work.online.useful_result_bytes = Metric::deterministic(
        batch_size * snapshot.manifest.values_per_page * snapshot.manifest.max_value_bytes,
        "K pages times four fixed-width locators; answer-share traffic is counted separately",
    );
    work.client.online_cpu_p50_ms = Metric::measured(
        client_online_p50_ms,
        "same-sample query sharing plus reconstruction/fingerprint validation; MPHF ordinal lookup was precomputed",
    );
    work.client.peak_transient_ram_bytes = Metric::estimated(
        upload_bytes + download_bytes,
        "query and answer payload capacity only; client allocator/runtime/RSS excluded",
    );
    work.client.persistent_state_bytes = Metric::deterministic(
        snapshot.manifest.client_metadata_bytes(),
        "authenticated generation-specific PtrHash artifact",
    );
    work.client.upload_bytes =
        Metric::deterministic(upload_bytes, "K independent shares to every replica");
    work.client.download_bytes =
        Metric::deterministic(download_bytes, "K answer rows from every replica");
    work.persisted_storage.server_bytes_per_server = Metric::deterministic(
        snapshot.rows().len(),
        "one replicated immutable MPHF Dense row table",
    );
    work.persisted_storage.aggregate_server_bytes = Metric::deterministic(
        snapshot.rows().len() * server_count,
        "same immutable row table replicated on every server",
    );
    work.persisted_storage.client_bytes = Metric::deterministic(
        snapshot.manifest.client_metadata_bytes(),
        "authenticated generation-specific PtrHash artifact",
    );
    work.amortization = AmortizationHorizon {
        global_build: "all batches served by one immutable snapshot generation",
        per_client_setup: "all batches issued after one authenticated MPHF metadata load",
        maintenance: "not applicable within an immutable generation",
        assumed_global_queries: None,
        assumed_queries_per_client_setup: None,
        assumed_online_events_per_maintenance: None,
        note: "Online unit is one already-ready K-query batch. Divide aggregate server time by K for work/query. No arrival rate or queue dwell is assumed; deployment must add measured queueing delay and partial-batch flush behavior.",
    };
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    if working_bytes_per_server > MAX_WORKING_BYTES_PER_SERVER {
        bail!("Dense batch working memory exceeded the configured admission limit");
    }
    Ok(work)
}

fn benchmark_config() -> TagPageConfig {
    TagPageConfig {
        bucket_capacity: 4,
        target_load_percent: 90,
        values_per_page: 4,
        max_value_bytes: 16,
    }
}

fn sample_count(profile: Profile, batch_size: usize) -> usize {
    match (profile, batch_size) {
        (Profile::Quick, 1..=16) => 5,
        (Profile::Quick, _) => 3,
        (Profile::Full, 1..=16) => 21,
        (Profile::Full, _) => 11,
    }
}

fn duration_p50_ms(samples: &[Duration]) -> f64 {
    millis(percentile_duration(samples, 50))
}

fn duration_p95_ms(samples: &[Duration]) -> f64 {
    millis(percentile_duration(samples, 95))
}

fn percentile_duration(samples: &[Duration], percentile: usize) -> Duration {
    let mut samples = samples.to_vec();
    samples.sort_unstable();
    samples[percentile_index(samples.len(), percentile)]
}

fn percentile_usize(samples: &[usize], percentile: usize) -> usize {
    let mut samples = samples.to_vec();
    samples.sort_unstable();
    samples[percentile_index(samples.len(), percentile)]
}

fn percentile_index(len: usize, percentile: usize) -> usize {
    assert!(len > 0);
    ((len - 1) * percentile).div_ceil(100)
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_tuning_covers_every_batch_size() {
        for batch_size in BATCH_SIZES {
            let kernels = candidate_kernels(batch_size);
            assert_eq!(kernels[0].2, BatchKernel::Independent);
            assert!(kernels.iter().any(|candidate| matches!(
                candidate.2,
                BatchKernel::GroupedFourRussians { group_bits: 2..=6 }
            )));
        }
    }

    #[test]
    fn percentile_is_nearest_rank_with_zero_based_storage() {
        assert_eq!(percentile_usize(&[9], 95), 9);
        assert_eq!(percentile_usize(&[1, 2, 3, 4, 5], 50), 3);
        assert_eq!(percentile_usize(&[1, 2, 3, 4, 5], 95), 5);
    }

    #[test]
    fn partial_final_group_matches_independent() {
        let row_size = 13;
        let bucket_count = 29;
        let rows = (0..bucket_count * row_size)
            .map(|index| (index as u8).wrapping_mul(17))
            .collect::<Vec<_>>();
        let view = SnapshotView::new(&rows, bucket_count, row_size);
        let queries = vec![vec![0x55, 0xaa, 0xf0, 0x1f], vec![0xff; 4]];
        let evaluator = BatchEvaluator::new(2, 1 << 20).unwrap();
        let reference = evaluator
            .evaluate(view, &queries, BatchKernel::Independent)
            .unwrap();
        for group_bits in 2..=8 {
            assert_eq!(
                evaluator
                    .evaluate(
                        view,
                        &queries,
                        BatchKernel::GroupedFourRussians { group_bits },
                    )
                    .unwrap()
                    .answers,
                reference.answers
            );
        }
    }
}
