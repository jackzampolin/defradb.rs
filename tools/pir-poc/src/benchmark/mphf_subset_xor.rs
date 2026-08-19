use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;

use super::{
    accounting::{
        unavailable_hardware_counters, AggregateWorkReport, AmortizationHorizon, ComparisonScope,
        LeakageScope, Metric, PhaseWork, SecurityLabels,
    },
    Profile,
};
use crate::{
    dense,
    mphf_pages::MphfPageSnapshot,
    snapshot::SnapshotView,
    subset_xor::{SubsetXorAnswer, SubsetXorAnswerMetrics, SubsetXorIndex},
    tag_pages::{benchmark_page_set, benchmark_tag, TagPageConfig},
};

const DOCUMENT_COUNT: usize = 1 << 20;
const DISTINCT_TAG_COUNT: usize = 1 << 18;
const SERVER_COUNTS: [usize; 2] = [2, 3];
const GROUP_SIZES: [usize; 3] = [4, 6, 8];
const MAXIMUM_INDEX_BYTES: usize = 1536 * 1024 * 1024;
const METHODOLOGY: &str = "The benchmark composes the exact PtrHash MPHF page layout with immutable subset-XOR preprocessing. It uses the same fully populated 1,048,576-document/262,144-tag corpus as bench-mphf: one 96-byte page containing four 16-byte locators per tag. A cold client loads authenticated generation-specific MPHF metadata, computes one exact ordinal, and creates fresh n-out-of-n Dense XOR shares. Plain MPHF Dense and every subset-XOR group use one single-core answer pass per co-located replica and the same u64-plus-tail answer XOR kernel; this avoids counting a two-worker server wall time as aggregate CPU work. Wall includes server-thread spawn/join; aggregate server time sums elapsed answer time inside every replica thread. All paths are warmed once and every sample reconstructs and fingerprint-validates the page.";
const METRIC_CLASSIFICATION: &str = "wall, aggregate server elapsed, layout/index build wall, client metadata load/lookup/query/reconstruction times, post-build RSS, and sampled logical row/byte counts are measured. Traffic and persisted sizes are deterministic. Expected row reads and tracked algorithm-owned peak bytes are analytical/estimated. Physical DRAM/cache/storage traffic, energy, aggregate builder CPU time, and client peak RAM are not measured.";
const THREAT_MODEL: &str = "Information-theoretic n-out-of-n XOR query privacy against any n-1 colluding semi-honest replicas. Generation-specific PtrHash metadata is public. All n answers are required; the 128-bit private page fingerprint rejects absent/wrong rows but is not Byzantine verification.";

#[derive(Debug, Serialize)]
pub struct MphfSubsetXorReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: &'static str,
    pub metric_classification: &'static str,
    pub threat_model: &'static str,
    pub measured_samples_per_topology: usize,
    pub workload: MphfSubsetWorkload,
    pub plain_mphf_dense: MphfSubsetLayoutResult,
    pub subset_xor: Vec<MphfSubsetLayoutResult>,
    pub production_caveats: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct MphfSubsetWorkload {
    pub document_count: usize,
    pub distinct_tag_count: usize,
    pub documents_per_tag: usize,
    pub encoded_page_count: usize,
    pub values_per_page: usize,
    pub locator_bytes: usize,
    pub row_bytes: usize,
    pub dense_rows: usize,
    pub dense_table_bytes_per_server: usize,
    pub query_share_bytes_per_server: usize,
    pub answer_bytes_per_server: usize,
    pub useful_result_bytes: usize,
    pub encoded_corpus_build_ms: f64,
    pub mphf_layout_build_ms: f64,
    pub mphf_build_attempts: usize,
    pub mphf_public_metadata_bytes: usize,
    pub mphf_client_load_ms: f64,
    pub mphf_client_lookup_p50_us: f64,
    pub mphf_peak_tracked_build_bytes: usize,
    pub mphf_peak_tracking_note: &'static str,
    pub generation: String,
}

#[derive(Debug, Serialize)]
pub struct MphfSubsetLayoutResult {
    pub layout: &'static str,
    pub group_size: Option<usize>,
    pub subset_index_build_ms_per_replica: Option<f64>,
    pub sequential_mphf_plus_subset_build_wall_ms: f64,
    pub sequential_corpus_plus_mphf_plus_subset_build_wall_ms: f64,
    pub source_table_bytes_per_server: usize,
    pub subset_index_data_bytes_per_server: usize,
    pub subset_index_persisted_bytes_per_server: usize,
    pub total_persisted_bytes_per_server: usize,
    pub total_storage_amplification_vs_plain_mphf: f64,
    pub aggregate_persisted_bytes_two_servers: usize,
    pub aggregate_persisted_bytes_three_servers: usize,
    pub peak_tracked_global_build_bytes: usize,
    pub process_rss_bytes_after_build: Option<usize>,
    pub maximum_subset_rows_changed_by_one_source_row_update: Option<usize>,
    pub topologies: Vec<MphfSubsetTopologyResult>,
}

#[derive(Debug, Serialize)]
pub struct MphfSubsetTopologyResult {
    pub aggregate_work: AggregateWorkReport,
    pub server_count: usize,
    pub privacy_collusion_tolerance: usize,
    pub required_answers: usize,
    pub wall_p50_ms: f64,
    pub wall_p95_ms: f64,
    pub aggregate_server_p50_ms: f64,
    pub aggregate_server_p95_ms: f64,
    pub logical_row_reads_p50: usize,
    pub logical_row_xors_p50: usize,
    pub logical_selected_bytes_p50: usize,
    pub analytical_expected_aggregate_row_reads: f64,
    pub total_client_upload_bytes: usize,
    pub total_client_download_bytes: usize,
    pub client_query_generation_p50_us: f64,
    pub client_reconstruct_p50_us: f64,
}

pub fn run(profile: Profile) -> Result<MphfSubsetXorReport> {
    let config = benchmark_config();
    let samples = match profile {
        Profile::Quick => 7,
        Profile::Full => 31,
    };

    let corpus_started = Instant::now();
    let page_set = benchmark_page_set(DOCUMENT_COUNT, DISTINCT_TAG_COUNT, &config)?;
    let encoded_corpus_build_ms = millis(corpus_started.elapsed());
    let mphf_started = Instant::now();
    let snapshot = MphfPageSnapshot::from_page_set(&page_set, config.clone())?;
    let mphf_layout_build_ms = millis(mphf_started.elapsed());
    drop(page_set);

    let client_load_started = Instant::now();
    let client = snapshot.trusted_client_index()?;
    let mphf_client_load_ms = millis(client_load_started.elapsed());
    let target_tag = benchmark_tag(DISTINCT_TAG_COUNT / 3);
    let ordinal = client.ordinal(&target_tag, 0)?;
    if ordinal != snapshot.ordinal(&target_tag, 0)? {
        bail!("cold MPHF client and source snapshot disagree on the target ordinal");
    }
    let mphf_client_lookup_p50_us = benchmark_lookup(|| client.ordinal(&target_tag, 0))?;
    let useful_result_bytes = snapshot.manifest.values_per_page * snapshot.manifest.max_value_bytes;
    let source_table_bytes = snapshot.rows().len();
    let plain_peak = snapshot.build_metrics.peak_tracked_bytes;
    let plain_rss = memory_stats::memory_stats().map(|stats| stats.physical_mem);

    let plain_topologies = SERVER_COUNTS
        .into_iter()
        .map(|server_count| {
            benchmark_topology(
                &snapshot,
                &target_tag,
                ordinal,
                ServerBackend::Dense(snapshot.view()),
                "ptrhash-exact-mphf-dense-single-core",
                server_count,
                samples,
                source_table_bytes,
                plain_peak,
                snapshot.manifest.client_metadata_bytes(),
                mphf_client_load_ms,
                mphf_client_lookup_p50_us,
                useful_result_bytes,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let plain_mphf_dense = MphfSubsetLayoutResult {
        layout: "plain-mphf-dense",
        group_size: None,
        subset_index_build_ms_per_replica: None,
        sequential_mphf_plus_subset_build_wall_ms: mphf_layout_build_ms,
        sequential_corpus_plus_mphf_plus_subset_build_wall_ms: encoded_corpus_build_ms
            + mphf_layout_build_ms,
        source_table_bytes_per_server: source_table_bytes,
        subset_index_data_bytes_per_server: 0,
        subset_index_persisted_bytes_per_server: 0,
        total_persisted_bytes_per_server: source_table_bytes,
        total_storage_amplification_vs_plain_mphf: 1.0,
        aggregate_persisted_bytes_two_servers: source_table_bytes * 2,
        aggregate_persisted_bytes_three_servers: source_table_bytes * 3,
        peak_tracked_global_build_bytes: plain_peak,
        process_rss_bytes_after_build: plain_rss,
        maximum_subset_rows_changed_by_one_source_row_update: None,
        topologies: plain_topologies,
    };

    let mut subset_xor = Vec::with_capacity(GROUP_SIZES.len());
    for group_size in GROUP_SIZES {
        let estimate = SubsetXorIndex::estimate(snapshot.view(), group_size)?;
        if estimate.index_data_bytes > MAXIMUM_INDEX_BYTES {
            bail!(
                "MPHF subset-XOR g={group_size} needs {} bytes, above the {}-byte guard",
                estimate.index_data_bytes,
                MAXIMUM_INDEX_BYTES
            );
        }
        let build_started = Instant::now();
        let index =
            SubsetXorIndex::build_with_limit(snapshot.view(), group_size, MAXIMUM_INDEX_BYTES)?;
        let subset_build_ms = millis(build_started.elapsed());
        let process_rss = memory_stats::memory_stats().map(|stats| stats.physical_mem);
        let total_persisted_bytes = source_table_bytes
            .checked_add(index.persisted_bytes())
            .context("MPHF subset-XOR persisted size overflow")?;
        let peak_tracked = plain_peak.max(estimate.peak_tracked_bytes);
        let protocol = group_protocol(group_size);
        let topologies = SERVER_COUNTS
            .into_iter()
            .map(|server_count| {
                benchmark_topology(
                    &snapshot,
                    &target_tag,
                    ordinal,
                    ServerBackend::Subset(&index),
                    protocol,
                    server_count,
                    samples,
                    total_persisted_bytes,
                    peak_tracked,
                    snapshot.manifest.client_metadata_bytes(),
                    mphf_client_load_ms,
                    mphf_client_lookup_p50_us,
                    useful_result_bytes,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        subset_xor.push(MphfSubsetLayoutResult {
            layout: protocol,
            group_size: Some(group_size),
            subset_index_build_ms_per_replica: Some(subset_build_ms),
            sequential_mphf_plus_subset_build_wall_ms: mphf_layout_build_ms + subset_build_ms,
            sequential_corpus_plus_mphf_plus_subset_build_wall_ms: encoded_corpus_build_ms
                + mphf_layout_build_ms
                + subset_build_ms,
            source_table_bytes_per_server: source_table_bytes,
            subset_index_data_bytes_per_server: index.index_data_bytes(),
            subset_index_persisted_bytes_per_server: index.persisted_bytes(),
            total_persisted_bytes_per_server: total_persisted_bytes,
            total_storage_amplification_vs_plain_mphf: total_persisted_bytes as f64
                / source_table_bytes as f64,
            aggregate_persisted_bytes_two_servers: total_persisted_bytes * 2,
            aggregate_persisted_bytes_three_servers: total_persisted_bytes * 3,
            peak_tracked_global_build_bytes: peak_tracked,
            process_rss_bytes_after_build: process_rss,
            maximum_subset_rows_changed_by_one_source_row_update: Some(1usize << (group_size - 1)),
            topologies,
        });
    }

    Ok(MphfSubsetXorReport {
        protocol: "ptrhash-exact-mphf-with-subset-xor-preprocessing",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: METHODOLOGY,
        metric_classification: METRIC_CLASSIFICATION,
        threat_model: THREAT_MODEL,
        measured_samples_per_topology: samples,
        workload: MphfSubsetWorkload {
            document_count: DOCUMENT_COUNT,
            distinct_tag_count: DISTINCT_TAG_COUNT,
            documents_per_tag: DOCUMENT_COUNT / DISTINCT_TAG_COUNT,
            encoded_page_count: snapshot.manifest.page_count,
            values_per_page: snapshot.manifest.values_per_page,
            locator_bytes: snapshot.manifest.max_value_bytes,
            row_bytes: snapshot.manifest.page_size,
            dense_rows: snapshot.manifest.page_count,
            dense_table_bytes_per_server: source_table_bytes,
            query_share_bytes_per_server: dense::query_size(snapshot.manifest.page_count),
            answer_bytes_per_server: snapshot.manifest.page_size,
            useful_result_bytes,
            encoded_corpus_build_ms,
            mphf_layout_build_ms,
            mphf_build_attempts: snapshot.build_metrics.attempts,
            mphf_public_metadata_bytes: snapshot.manifest.client_metadata_bytes(),
            mphf_client_load_ms,
            mphf_client_lookup_p50_us,
            mphf_peak_tracked_build_bytes: plain_peak,
            mphf_peak_tracking_note: snapshot.build_metrics.peak_tracking_note,
            generation: snapshot.manifest.generation_hex(),
        },
        plain_mphf_dense,
        subset_xor,
        production_caveats: vec![
            "Co-located replicas share one physical read-only allocation and one memory controller; production replicas would own independent copies on independent hosts.",
            "Aggregate server elapsed is a single-core-per-replica software proxy, not CPU cycles or joules; physical memory and energy counters remain required.",
            "Post-build RSS is not peak RSS and may include allocator retention from layouts built earlier in this process.",
            "Process RSS includes this co-located benchmark's server snapshot, retained public MPHF artifact, and loaded client MPHF; it is not a per-server or per-client allocation measurement.",
            "PtrHash transient construction workspace is not exposed by its API and is excluded from tracked peak memory.",
            "The epserde PtrHash artifact remains POC-only unsafe deserialization; production needs authenticated, size-bounded, stable serialization.",
            "Subset indexes are immutable snapshot artifacts. Updates build, verify, and atomically publish a new generation.",
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

#[derive(Clone, Copy)]
enum ServerBackend<'a> {
    Dense(SnapshotView<'a>),
    Subset(&'a SubsetXorIndex),
}

impl ServerBackend<'_> {
    fn answer(self, query: &[u8]) -> Result<SubsetXorAnswer> {
        match self {
            Self::Dense(snapshot) => dense_answer_with_metrics(snapshot, query),
            Self::Subset(index) => index.answer_with_metrics(query),
        }
    }

    fn analytical_expected_reads_per_server(self) -> f64 {
        match self {
            Self::Dense(snapshot) => snapshot.bucket_count as f64 / 2.0,
            Self::Subset(index) => expected_nonzero_reads(index),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn benchmark_topology(
    snapshot: &MphfPageSnapshot,
    target_tag: &[u8],
    ordinal: usize,
    backend: ServerBackend<'_>,
    protocol: &'static str,
    server_count: usize,
    samples: usize,
    persisted_bytes_per_server: usize,
    peak_tracked_build_bytes: usize,
    client_metadata_bytes: usize,
    client_load_ms: f64,
    client_lookup_p50_us: f64,
    useful_result_bytes: usize,
) -> Result<MphfSubsetTopologyResult> {
    let mut rng = StdRng::seed_from_u64(
        0x5ab5_4d50 ^ ((server_count as u64) << 32) ^ protocol_seed(protocol),
    );
    let warm_queries = dense::query_shares(
        ordinal,
        snapshot.manifest.page_count,
        server_count,
        &mut rng,
    )?;
    let warm = evaluate(backend, &warm_queries)?;
    verify_present(snapshot, target_tag, &warm.answers)?;

    let mut measurements = Vec::with_capacity(samples);
    for _ in 0..samples {
        let query_started = Instant::now();
        let queries = dense::query_shares(
            ordinal,
            snapshot.manifest.page_count,
            server_count,
            &mut rng,
        )?;
        let query_generation = query_started.elapsed();
        let evaluation = evaluate(backend, &queries)?;
        let reconstruct_started = Instant::now();
        verify_present(snapshot, target_tag, &evaluation.answers)?;
        let reconstruct = reconstruct_started.elapsed();
        measurements.push(Sample {
            evaluation,
            query_generation,
            reconstruct,
        });
    }
    let summary = summarize(&measurements);
    let query_bytes_per_server = dense::query_size(snapshot.manifest.page_count);
    let total_upload = query_bytes_per_server * server_count;
    let total_download = snapshot.manifest.page_size * server_count;
    let expected_reads = backend.analytical_expected_reads_per_server() * server_count as f64;
    let aggregate_work = topology_accounting(
        protocol,
        server_count,
        persisted_bytes_per_server,
        peak_tracked_build_bytes,
        client_metadata_bytes,
        client_load_ms,
        client_lookup_p50_us,
        millis(summary.wall_p50),
        millis(summary.aggregate_p50),
        summary.logical_bytes_p50,
        total_upload,
        total_download,
        useful_result_bytes,
        micros(summary.query_generation_p50),
        micros(summary.reconstruct_p50),
    )?;
    Ok(MphfSubsetTopologyResult {
        aggregate_work,
        server_count,
        privacy_collusion_tolerance: server_count - 1,
        required_answers: server_count,
        wall_p50_ms: millis(summary.wall_p50),
        wall_p95_ms: millis(summary.wall_p95),
        aggregate_server_p50_ms: millis(summary.aggregate_p50),
        aggregate_server_p95_ms: millis(summary.aggregate_p95),
        logical_row_reads_p50: summary.row_reads_p50,
        logical_row_xors_p50: summary.row_xors_p50,
        logical_selected_bytes_p50: summary.logical_bytes_p50,
        analytical_expected_aggregate_row_reads: expected_reads,
        total_client_upload_bytes: total_upload,
        total_client_download_bytes: total_download,
        client_query_generation_p50_us: micros(summary.query_generation_p50),
        client_reconstruct_p50_us: micros(summary.reconstruct_p50),
    })
}

fn protocol_seed(protocol: &str) -> u64 {
    protocol
        .bytes()
        .fold(0u64, |seed, byte| seed.rotate_left(5) ^ u64::from(byte))
}

fn group_protocol(group_size: usize) -> &'static str {
    match group_size {
        4 => "ptrhash-exact-mphf-subset-xor-g4",
        6 => "ptrhash-exact-mphf-subset-xor-g6",
        8 => "ptrhash-exact-mphf-subset-xor-g8",
        _ => "ptrhash-exact-mphf-subset-xor",
    }
}

fn expected_nonzero_reads(index: &SubsetXorIndex) -> f64 {
    let full_groups = index.bucket_count() / index.group_size();
    let trailing_bits = index.bucket_count() % index.group_size();
    let full_probability = 1.0 - 1.0 / (1usize << index.group_size()) as f64;
    let trailing_probability = if trailing_bits == 0 {
        0.0
    } else {
        1.0 - 1.0 / (1usize << trailing_bits) as f64
    };
    full_groups as f64 * full_probability + trailing_probability
}

fn evaluate(backend: ServerBackend<'_>, queries: &[Vec<u8>]) -> Result<Evaluation> {
    let wall_started = Instant::now();
    let servers = std::thread::scope(|scope| {
        queries
            .iter()
            .map(|query| {
                scope.spawn(move || -> Result<ServerEvaluation> {
                    let started = Instant::now();
                    let answer = backend.answer(query)?;
                    Ok(ServerEvaluation {
                        elapsed: started.elapsed(),
                        answer,
                    })
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("MPHF subset-XOR server thread panicked"))?
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let wall = wall_started.elapsed();
    let aggregate_server = servers.iter().map(|server| server.elapsed).sum();
    let row_reads = servers
        .iter()
        .map(|server| server.answer.metrics.logical_row_reads)
        .sum();
    let row_xors = servers
        .iter()
        .map(|server| server.answer.metrics.logical_row_xors)
        .sum();
    let logical_bytes = servers
        .iter()
        .map(|server| server.answer.metrics.logical_data_bytes_read)
        .sum();
    Ok(Evaluation {
        wall,
        aggregate_server,
        row_reads,
        row_xors,
        logical_bytes,
        answers: servers
            .into_iter()
            .map(|server| server.answer.bytes)
            .collect(),
    })
}

fn verify_present(
    snapshot: &MphfPageSnapshot,
    target_tag: &[u8],
    answers: &[Vec<u8>],
) -> Result<()> {
    let page = dense::combine(answers)?;
    let decoded = snapshot
        .decode_retrieved_page(&page, target_tag, 0)?
        .context("MPHF subset-XOR did not recover the target page")?;
    if decoded.values.len() != snapshot.manifest.values_per_page {
        bail!("MPHF subset-XOR recovered the wrong number of locators");
    }
    Ok(())
}

fn dense_answer_with_metrics(snapshot: SnapshotView<'_>, query: &[u8]) -> Result<SubsetXorAnswer> {
    let expected = dense::query_size(snapshot.bucket_count);
    if query.len() != expected {
        bail!("query share has {} bytes, expected {expected}", query.len());
    }
    let mut bytes = vec![0u8; snapshot.row_size];
    let mut logical_row_reads = 0usize;
    for (byte_index, query_byte) in query.iter().copied().enumerate() {
        let mut selected = query_byte;
        while selected != 0 {
            let bit_index = selected.trailing_zeros() as usize;
            let bucket = byte_index * 8 + bit_index;
            if bucket < snapshot.bucket_count {
                xor_row(&mut bytes, snapshot.row(bucket)?);
                logical_row_reads += 1;
            }
            selected &= selected - 1;
        }
    }
    Ok(SubsetXorAnswer {
        bytes,
        metrics: SubsetXorAnswerMetrics {
            logical_row_reads,
            logical_row_xors: logical_row_reads,
            logical_data_bytes_read: logical_row_reads * snapshot.row_size,
        },
    })
}

#[inline(always)]
fn xor_row(output: &mut [u8], row: &[u8]) {
    const WORD_BYTES: usize = std::mem::size_of::<u64>();
    let word_bytes = output.len() / WORD_BYTES * WORD_BYTES;
    let mut offset = 0;
    while offset < word_bytes {
        let left = u64::from_ne_bytes(
            output[offset..offset + WORD_BYTES]
                .try_into()
                .expect("fixed word"),
        );
        let right = u64::from_ne_bytes(
            row[offset..offset + WORD_BYTES]
                .try_into()
                .expect("fixed word"),
        );
        output[offset..offset + WORD_BYTES].copy_from_slice(&(left ^ right).to_ne_bytes());
        offset += WORD_BYTES;
    }
    for (left, right) in output[word_bytes..].iter_mut().zip(&row[word_bytes..]) {
        *left ^= *right;
    }
}

struct ServerEvaluation {
    elapsed: Duration,
    answer: SubsetXorAnswer,
}

struct Evaluation {
    wall: Duration,
    aggregate_server: Duration,
    row_reads: usize,
    row_xors: usize,
    logical_bytes: usize,
    answers: Vec<Vec<u8>>,
}

struct Sample {
    evaluation: Evaluation,
    query_generation: Duration,
    reconstruct: Duration,
}

struct Summary {
    wall_p50: Duration,
    wall_p95: Duration,
    aggregate_p50: Duration,
    aggregate_p95: Duration,
    row_reads_p50: usize,
    row_xors_p50: usize,
    logical_bytes_p50: usize,
    query_generation_p50: Duration,
    reconstruct_p50: Duration,
}

fn summarize(samples: &[Sample]) -> Summary {
    let mut wall = samples
        .iter()
        .map(|sample| sample.evaluation.wall)
        .collect::<Vec<_>>();
    let mut aggregate = samples
        .iter()
        .map(|sample| sample.evaluation.aggregate_server)
        .collect::<Vec<_>>();
    let mut row_reads = samples
        .iter()
        .map(|sample| sample.evaluation.row_reads)
        .collect::<Vec<_>>();
    let mut row_xors = samples
        .iter()
        .map(|sample| sample.evaluation.row_xors)
        .collect::<Vec<_>>();
    let mut logical_bytes = samples
        .iter()
        .map(|sample| sample.evaluation.logical_bytes)
        .collect::<Vec<_>>();
    let mut query_generation = samples
        .iter()
        .map(|sample| sample.query_generation)
        .collect::<Vec<_>>();
    let mut reconstruct = samples
        .iter()
        .map(|sample| sample.reconstruct)
        .collect::<Vec<_>>();
    wall.sort_unstable();
    aggregate.sort_unstable();
    row_reads.sort_unstable();
    row_xors.sort_unstable();
    logical_bytes.sort_unstable();
    query_generation.sort_unstable();
    reconstruct.sort_unstable();
    Summary {
        wall_p50: percentile(&wall, 50),
        wall_p95: percentile(&wall, 95),
        aggregate_p50: percentile(&aggregate, 50),
        aggregate_p95: percentile(&aggregate, 95),
        row_reads_p50: percentile(&row_reads, 50),
        row_xors_p50: percentile(&row_xors, 50),
        logical_bytes_p50: percentile(&logical_bytes, 50),
        query_generation_p50: percentile(&query_generation, 50),
        reconstruct_p50: percentile(&reconstruct, 50),
    }
}

#[allow(clippy::too_many_arguments)]
fn topology_accounting(
    protocol: &'static str,
    server_count: usize,
    persisted_bytes_per_server: usize,
    peak_tracked_build_bytes: usize,
    client_metadata_bytes: usize,
    client_load_ms: f64,
    client_lookup_p50_us: f64,
    wall_p50_ms: f64,
    aggregate_server_p50_ms: f64,
    aggregate_logical_selected_bytes: usize,
    total_upload_bytes: usize,
    total_download_bytes: usize,
    useful_result_bytes: usize,
    query_generation_p50_us: f64,
    reconstruct_p50_us: f64,
) -> Result<AggregateWorkReport> {
    let mut work = AggregateWorkReport::new(
        protocol,
        ComparisonScope {
            workload: "one exact-ordinal lookup over the identical populated immutable MPHF page corpus",
            result: "one first page containing four fixed-width compact locators",
            public_partition: "global immutable snapshot; authenticated generation-specific PtrHash metadata is public",
            leakage: LeakageScope::ExactQueryPrivacy,
        },
        SecurityLabels {
            privacy: "information-theoretic n-out-of-n XOR query privacy",
            server_count,
            collusion_tolerance: server_count - 1,
            required_answers: server_count,
            assumptions: "at least one replica does not collude; every replica serves the same authenticated immutable generation",
            availability: "all answer shares are required",
            integrity: "the privately retrieved 128-bit fingerprint rejects absent/wrong rows with high probability but is not a malicious-server proof or MAC",
        },
    );
    work.global_build.aggregate_server_time_ms = Metric::not_measured(
        "MPHF and subset index build wall times are reported separately; aggregate builder CPU time was not measured",
    );
    work.global_build.client_time_ms = Metric::not_applicable("build is server-side");
    work.global_build.logical_selected_bytes =
        Metric::not_measured("builder reads and writes were not instrumented");
    work.global_build.physical_or_scanned_bytes =
        Metric::not_measured("builder hardware byte counters were not collected");
    work.global_build.peak_server_ram_bytes = Metric::estimated(
        peak_tracked_build_bytes,
        "maximum tracked algorithm-owned MPHF/subset buffers; PtrHash transient workspace and runtime overhead are excluded",
    );
    work.global_build.peak_client_ram_bytes = Metric::not_applicable("build is server-side");
    work.global_build.client_upload_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.client_download_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.server_scans =
        Metric::not_measured("layout construction passes were not instrumented");
    work.global_build.network_rounds = Metric::not_applicable("build is server-local");

    let mut setup = PhaseWork::unmeasured(
        "one client loading one immutable generation",
        "metadata distribution server work, physical traffic, and peak client RAM were not measured",
    );
    setup.client_time_ms = Metric::measured(
        client_load_ms,
        "authenticated artifact construction, validation, and PtrHash deserialization",
    );
    setup.client_upload_bytes = Metric::deterministic(0, "public metadata fetch has no PIR upload");
    setup.client_download_bytes = Metric::deterministic(
        client_metadata_bytes,
        "one authenticated generation-specific PtrHash artifact",
    );
    setup.network_rounds = Metric::estimated(
        1,
        "one metadata fetch is assumed; network latency was not benchmarked",
    );
    work.per_client_setup = setup;
    work.maintenance = PhaseWork::not_applicable(
        "immutable snapshot lifetime",
        "updates create and atomically publish a new MPHF/subset generation",
    );

    let average_selected_bytes = aggregate_logical_selected_bytes as f64 / server_count as f64;
    for server in &mut work.online.per_server {
        server.server_time_p50_ms = Metric::estimated(
            aggregate_server_p50_ms / server_count as f64,
            "sum-server p50 divided evenly; individual server samples were not retained",
        );
        server.logical_selected_bytes = Metric::estimated(
            average_selected_bytes.round() as usize,
            "average of the measured aggregate selected-row bytes across replicas",
        );
        server.physical_or_scanned_bytes =
            Metric::not_measured("cache-line, DRAM, and storage traffic require hardware counters");
        server.scans = Metric::deterministic(1, "one exact-ordinal selector traversal");
    }
    work.online.unit = "one exact-ordinal first-page lookup";
    work.online.aggregate_server_time_p50_ms = Metric::measured(
        aggregate_server_p50_ms,
        "sum of measured single-core server elapsed times for the co-located topology",
    );
    work.online.max_server_time_p50_ms = Metric::estimated(
        wall_p50_ms,
        "co-located wall p50 is an upper-envelope proxy including thread spawn/join",
    );
    work.online.aggregate_logical_selected_bytes = Metric::measured(
        aggregate_logical_selected_bytes,
        "median exact selected-row payload count across every replica",
    );
    work.online.aggregate_physical_or_scanned_bytes = Metric::not_measured(
        "logical payload bytes are not substituted for cache-line or DRAM traffic",
    );
    work.online.server_scans =
        Metric::deterministic(server_count, "one selector traversal on every replica");
    work.online.network_rounds = Metric::deterministic(1, "all shares are sent in parallel");
    work.online.useful_result_bytes = Metric::deterministic(
        useful_result_bytes,
        "four fixed-width locators, excluding private page framing",
    );
    work.client.online_cpu_p50_ms = Metric::estimated(
        (client_lookup_p50_us + query_generation_p50_us + reconstruct_p50_us) / 1_000.0,
        "sum of separately measured MPHF lookup, share generation, and reconstruction medians",
    );
    work.client.peak_transient_ram_bytes =
        Metric::not_measured("client process peak RAM was not sampled");
    work.client.persistent_state_bytes = Metric::deterministic(
        client_metadata_bytes,
        "authenticated generation-specific PtrHash artifact retained by the client",
    );
    work.client.upload_bytes = Metric::deterministic(
        total_upload_bytes,
        "one fresh exact-ordinal selector share per replica",
    );
    work.client.download_bytes = Metric::deterministic(
        total_download_bytes,
        "one fixed-width page answer share per replica",
    );
    work.persisted_storage.server_bytes_per_server = Metric::deterministic(
        persisted_bytes_per_server,
        "one MPHF page table plus the persisted subset index when applicable; manifest bytes are excluded",
    );
    work.persisted_storage.aggregate_server_bytes = Metric::deterministic(
        persisted_bytes_per_server * server_count,
        "sum of identical persisted replicas across all servers",
    );
    work.persisted_storage.client_bytes = Metric::deterministic(
        client_metadata_bytes,
        "one generation-specific public PtrHash artifact",
    );
    work.amortization = AmortizationHorizon {
        global_build: "all clients and lookups using one immutable generation",
        per_client_setup: "all lookups by one client before generation refresh",
        maintenance: "not applicable; updates create a new immutable generation",
        assumed_global_queries: None,
        assumed_queries_per_client_setup: None,
        assumed_online_events_per_maintenance: None,
        note: "MPHF/subset builds and client metadata load remain separate from online work; no amortization denominator is assumed.",
    };
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    Ok(work)
}

fn benchmark_lookup<T>(mut lookup: impl FnMut() -> Result<T>) -> Result<f64> {
    const SAMPLES: usize = 1_001;
    let mut durations = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        std::hint::black_box(lookup()?);
        durations.push(started.elapsed());
    }
    durations.sort_unstable();
    Ok(micros(percentile(&durations, 50)))
}

fn percentile<T: Copy>(values: &[T], percentile: usize) -> T {
    values[((values.len() - 1) * percentile).div_ceil(100)]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preprocessed_mphf_recovers_client_ordinal_and_rejects_missing_key() {
        let snapshot = MphfPageSnapshot::benchmark(64, 16, benchmark_config()).unwrap();
        let client = snapshot.trusted_client_index().unwrap();
        let index = SubsetXorIndex::build(snapshot.view(), 4).unwrap();
        let present = benchmark_tag(7);
        let present_ordinal = client.ordinal(&present, 0).unwrap();
        assert_eq!(present_ordinal, snapshot.ordinal(&present, 0).unwrap());
        let queries = dense::query_shares(
            present_ordinal,
            snapshot.manifest.page_count,
            3,
            &mut StdRng::seed_from_u64(7),
        )
        .unwrap();
        let answers = queries
            .iter()
            .map(|query| index.answer(query).unwrap())
            .collect::<Vec<_>>();
        let recovered = dense::combine(&answers).unwrap();
        assert!(snapshot
            .decode_retrieved_page(&recovered, &present, 0)
            .unwrap()
            .is_some());

        let missing = b"definitely-not-in-the-populated-mphf-corpus";
        let missing_ordinal = client.ordinal(missing, 0).unwrap();
        let queries = dense::query_shares(
            missing_ordinal,
            snapshot.manifest.page_count,
            3,
            &mut StdRng::seed_from_u64(9),
        )
        .unwrap();
        let answers = queries
            .iter()
            .map(|query| index.answer(query).unwrap())
            .collect::<Vec<_>>();
        let recovered = dense::combine(&answers).unwrap();
        assert!(snapshot
            .decode_retrieved_page(&recovered, missing, 0)
            .unwrap()
            .is_none());
    }

    #[test]
    fn fair_dense_kernel_matches_reference() {
        let snapshot = MphfPageSnapshot::benchmark(64, 16, benchmark_config()).unwrap();
        let query = dense::query_shares(
            7,
            snapshot.manifest.page_count,
            2,
            &mut StdRng::seed_from_u64(11),
        )
        .unwrap()
        .remove(0);
        assert_eq!(
            dense_answer_with_metrics(snapshot.view(), &query)
                .unwrap()
                .bytes,
            dense::answer(snapshot.view(), &query).unwrap()
        );
    }
}
