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
    dense::{self, ParallelEvaluator},
    fuse_pages::{FuseArity, FusePageSnapshot},
    mphf_pages::MphfPageSnapshot,
    snapshot::SnapshotView,
    tag_pages::{benchmark_page_set, benchmark_tag, TagPageConfig},
};

const SERVER_COUNTS: [usize; 2] = [2, 3];
const SERVER_WORKERS: usize = 2;
const METHODOLOGY: &str = "PtrHash MPHF and Fuse-4 consume the identical pre-encoded, fully populated immutable tag-page corpus: four 16-byte locators per tag and one selected page. PtrHash maps a public page key to one exact compact ordinal; Fuse-4 maps it to four XOR cells. Both issue one Dense XOR evaluation per server. Every topology is warmed once, then timed with fresh cryptographic shares and correctness-checked reconstruction. Co-located servers each own a persistent two-worker evaluator and contend for one memory bus. Timings exclude HTTP, TLS, serialization, metadata download, and network latency.";

#[derive(Clone, Debug, Serialize)]
pub struct MphfComparisonReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: &'static str,
    pub workload: MphfComparisonWorkload,
    pub encoded_corpus_build_ms: f64,
    pub encoded_corpus_tracked_bytes: usize,
    pub layouts: Vec<MphfLayoutResult>,
    pub production_caveats: Vec<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MphfComparisonWorkload {
    pub document_count: usize,
    pub distinct_tag_count: usize,
    pub documents_per_tag: usize,
    pub encoded_page_count: usize,
    pub values_per_page: usize,
    pub locator_bytes: usize,
    pub page_bytes: usize,
    pub selected_page: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct MphfLayoutResult {
    pub layout: &'static str,
    pub lookup: &'static str,
    pub dense_evaluations_per_server: usize,
    pub selector_hamming_weight: usize,
    pub table_rows: usize,
    pub row_bytes: usize,
    pub dense_table_bytes_per_server: usize,
    pub public_index_bytes: usize,
    pub persistent_bytes_per_server: usize,
    pub storage_expansion_vs_encoded_pages: f64,
    pub cold_client_metadata_bytes: usize,
    pub cold_client_index_load_ms: Option<f64>,
    pub client_layout_lookup_p50_us: f64,
    pub absent_key_verification_bits: usize,
    pub public_metadata_privacy_note: &'static str,
    pub layout_build_ms: f64,
    pub build_attempts: usize,
    pub peak_tracked_build_bytes: usize,
    pub peak_build_memory_note: &'static str,
    pub generation: Option<String>,
    pub topologies: Vec<MphfTopologyResult>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MphfTopologyResult {
    pub aggregate_work: AggregateWorkReport,
    pub server_count: usize,
    pub privacy_collusion_tolerance: usize,
    pub required_answers: usize,
    pub expected_rows_xored_per_server: usize,
    pub expected_rows_xored_all_servers: usize,
    pub expected_data_bytes_xored_per_server: usize,
    pub expected_data_bytes_xored_all_servers: usize,
    pub query_bytes_per_server: usize,
    pub total_client_upload_bytes: usize,
    pub response_bytes_per_server: usize,
    pub total_client_download_bytes: usize,
    pub client_query_generation_p50_us: f64,
    pub co_located_wall_p50_ms: f64,
    pub co_located_wall_p95_ms: f64,
    pub sum_server_elapsed_p50_ms: f64,
    pub client_reconstruct_p50_us: f64,
}

pub fn run(profile: Profile) -> Result<MphfComparisonReport> {
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

    let corpus_started = Instant::now();
    let page_set = benchmark_page_set(document_count, distinct_tag_count, &config)?;
    let encoded_corpus_build_ms = millis(corpus_started.elapsed());
    let encoded_corpus_tracked_bytes = page_set.tracked_bytes();
    let encoded_payload_bytes = page_set.pages.len() * config.page_size()?;
    let target_tag = benchmark_tag(distinct_tag_count / 3);

    let mphf_started = Instant::now();
    let mphf = MphfPageSnapshot::from_page_set(&page_set, config.clone())?;
    let mphf_build_ms = millis(mphf_started.elapsed());
    let mphf_layout = benchmark_mphf(
        &mphf,
        &target_tag,
        profile,
        encoded_payload_bytes,
        mphf_build_ms,
        encoded_corpus_build_ms + mphf_build_ms,
    )?;
    drop(mphf);

    let fuse_started = Instant::now();
    let fuse = FusePageSnapshot::from_page_set(&page_set, config.clone(), FuseArity::Four)?;
    let fuse_build_ms = millis(fuse_started.elapsed());
    let fuse_layout = benchmark_fuse(
        &fuse,
        &target_tag,
        profile,
        encoded_payload_bytes,
        fuse_build_ms,
        encoded_corpus_build_ms + fuse_build_ms,
    )?;

    Ok(MphfComparisonReport {
        protocol: "exact-mphf-dense-vs-fuse-4-over-replicated-dense-xor",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: METHODOLOGY,
        workload: MphfComparisonWorkload {
            document_count,
            distinct_tag_count,
            documents_per_tag: document_count / distinct_tag_count,
            encoded_page_count: page_set.pages.len(),
            values_per_page: config.values_per_page,
            locator_bytes: config.max_value_bytes,
            page_bytes: config.page_size()?,
            selected_page: 0,
        },
        encoded_corpus_build_ms,
        encoded_corpus_tracked_bytes,
        layouts: vec![mphf_layout, fuse_layout],
        production_caveats: vec![
            "The MPHF description is key-dependent public metadata. It is not a direct membership oracle, but injectivity constraints over guessed candidate keys and cross-generation differencing can leak set relations; this is a different static-index leakage profile from Fuse-4.",
            "A client must use metadata and server rows from the same immutable generation. The exported artifact appends a domain-separated digest; a client must compare it with an independently authenticated manifest before parsing. The generation digest additionally binds dimensions, MPHF, and rows at publication time.",
            "An MPHF maps absent keys to arbitrary populated ordinals. Acceptance is gated by the encoded page's 128-bit fingerprint after private retrieval.",
            "PtrHash's epserde bytes mirror Rust implementation details and require unsafe deserialization. They are not a stable cross-version, cross-target, untrusted wire format. Production needs a safe versioned representation or a tightly authenticated, pinned-build loader boundary.",
            "The POC retains both a live PtrHash and its exact serialized artifact. A production server only needs Dense rows; clients need one authenticated index artifact per immutable generation.",
            "MPHF tracked peak memory excludes PtrHash's unexposed transient construction workspace; benchmark production builds with process peak RSS and sharded construction before adopting it at 10M+ pages.",
        ],
    })
}

fn benchmark_mphf(
    snapshot: &MphfPageSnapshot,
    target_tag: &[u8],
    profile: Profile,
    encoded_payload_bytes: usize,
    build_ms: f64,
    global_build_ms: f64,
) -> Result<MphfLayoutResult> {
    let client_load_started = Instant::now();
    let client = snapshot.trusted_client_index()?;
    let client_load_ms = millis(client_load_started.elapsed());
    let ordinal = client.ordinal(target_tag, 0)?;
    let layout_lookup_p50_us = benchmark_layout_lookup(|| client.ordinal(target_tag, 0))?;
    let topologies = SERVER_COUNTS
        .into_iter()
        .map(|server_count| {
            benchmark_mphf_topology(
                snapshot,
                target_tag,
                ordinal,
                server_count,
                profile,
                global_build_ms,
                client_load_ms,
                layout_lookup_p50_us,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let public_index_bytes = snapshot.manifest.client_metadata_bytes();
    Ok(MphfLayoutResult {
        layout: "ptrhash-exact-mphf-dense",
        lookup: "one public MPHF ordinal, then one private Dense row",
        dense_evaluations_per_server: 1,
        selector_hamming_weight: 1,
        table_rows: snapshot.manifest.page_count,
        row_bytes: snapshot.manifest.page_size,
        dense_table_bytes_per_server: snapshot.rows().len(),
        public_index_bytes,
        persistent_bytes_per_server: snapshot.rows().len() + public_index_bytes,
        storage_expansion_vs_encoded_pages: snapshot.rows().len() as f64
            / encoded_payload_bytes as f64,
        cold_client_metadata_bytes: snapshot.manifest.client_metadata_bytes(),
        cold_client_index_load_ms: Some(client_load_ms),
        client_layout_lookup_p50_us: layout_lookup_p50_us,
        absent_key_verification_bits: snapshot.manifest.absent_key_verification_bits(),
        public_metadata_privacy_note: "Key-dependent PtrHash structure is public and generation-specific. It is not a direct membership oracle, but injectivity constraints over guessed candidates can leak set relations; result acceptance still requires the privately retrieved 128-bit fingerprint.",
        layout_build_ms: build_ms,
        build_attempts: snapshot.build_metrics.attempts,
        peak_tracked_build_bytes: snapshot.build_metrics.peak_tracked_bytes,
        peak_build_memory_note: snapshot.build_metrics.peak_tracking_note,
        generation: Some(snapshot.manifest.generation_hex()),
        topologies,
    })
}

fn benchmark_fuse(
    snapshot: &FusePageSnapshot,
    target_tag: &[u8],
    profile: Profile,
    encoded_payload_bytes: usize,
    build_ms: f64,
    global_build_ms: f64,
) -> Result<MphfLayoutResult> {
    let cells = snapshot.cells(target_tag, 0)?;
    let layout_lookup_p50_us = benchmark_layout_lookup(|| snapshot.cells(target_tag, 0))?;
    let topologies = SERVER_COUNTS
        .into_iter()
        .map(|server_count| {
            benchmark_fuse_topology(
                snapshot,
                target_tag,
                &cells,
                server_count,
                profile,
                global_build_ms,
                layout_lookup_p50_us,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let public_index_bytes = snapshot.manifest.client_metadata_bytes();
    Ok(MphfLayoutResult {
        layout: "fuse-4-retrieval",
        lookup: "four public Fuse cells XORed by one private multi-hot Dense selector",
        dense_evaluations_per_server: 1,
        selector_hamming_weight: 4,
        table_rows: snapshot.manifest.cell_count,
        row_bytes: snapshot.manifest.page_size,
        dense_table_bytes_per_server: snapshot.rows().len(),
        public_index_bytes,
        persistent_bytes_per_server: snapshot.rows().len() + public_index_bytes,
        storage_expansion_vs_encoded_pages: snapshot.rows().len() as f64
            / encoded_payload_bytes as f64,
        cold_client_metadata_bytes: snapshot.manifest.client_metadata_bytes(),
        cold_client_index_load_ms: None,
        client_layout_lookup_p50_us: layout_lookup_p50_us,
        absent_key_verification_bits: 128,
        public_metadata_privacy_note: "Dimensions and the generation-specific successful peel seed are public; the seed is key-set dependent but no full MPHF structure is downloaded. An absent key is rejected by the privately retrieved 128-bit fingerprint.",
        layout_build_ms: build_ms,
        build_attempts: snapshot.build_metrics.attempts,
        peak_tracked_build_bytes: snapshot.build_metrics.peak_tracked_bytes,
        peak_build_memory_note: "Deterministic algorithm-owned corpus, output, and temporary vectors; excludes allocator metadata, code, thread stacks, and runtime RSS.",
        generation: None,
        topologies,
    })
}

#[allow(clippy::too_many_arguments)]
fn benchmark_mphf_topology(
    snapshot: &MphfPageSnapshot,
    target_tag: &[u8],
    ordinal: usize,
    server_count: usize,
    profile: Profile,
    global_build_ms: f64,
    client_load_ms: f64,
    layout_lookup_p50_us: f64,
) -> Result<MphfTopologyResult> {
    let evaluators = evaluators(server_count)?;
    let mut rng = StdRng::seed_from_u64(0x4d50_4846 ^ server_count as u64);
    let warmup = dense::query_shares(
        ordinal,
        snapshot.manifest.page_count,
        server_count,
        &mut rng,
    )?
    .into_iter()
    .map(|query| vec![query])
    .collect::<Vec<_>>();
    let warmup = evaluate(snapshot.view(), &evaluators, &warmup)?;
    reconstruct_mphf(snapshot, target_tag, warmup.answers)?;

    let samples = sample_count(profile);
    let mut query_generation = Vec::with_capacity(samples);
    let mut wall = Vec::with_capacity(samples);
    let mut server_elapsed = Vec::with_capacity(samples);
    let mut reconstruct = Vec::with_capacity(samples);
    for _ in 0..samples {
        let query_started = Instant::now();
        let queries = dense::query_shares(
            ordinal,
            snapshot.manifest.page_count,
            server_count,
            &mut rng,
        )?
        .into_iter()
        .map(|query| vec![query])
        .collect::<Vec<_>>();
        query_generation.push(query_started.elapsed());
        let evaluated = evaluate(snapshot.view(), &evaluators, &queries)?;
        wall.push(evaluated.wall);
        server_elapsed.push(evaluated.sum_server_elapsed);
        let reconstruct_started = Instant::now();
        reconstruct_mphf(snapshot, target_tag, evaluated.answers)?;
        reconstruct.push(reconstruct_started.elapsed());
    }
    topology_result(
        server_count,
        snapshot.manifest.page_count,
        snapshot.manifest.page_size,
        snapshot.manifest.values_per_page * snapshot.manifest.max_value_bytes,
        snapshot.rows().len(),
        global_build_ms,
        snapshot.build_metrics.peak_tracked_bytes,
        snapshot.manifest.client_metadata_bytes(),
        Some(client_load_ms),
        layout_lookup_p50_us,
        "ptrhash-exact-mphf-dense",
        "global immutable snapshot; generation-specific MPHF structure is public",
        "exact information-theoretic query privacy; public MPHF metadata may leak key-set relations",
        query_generation,
        wall,
        server_elapsed,
        reconstruct,
    )
}

fn benchmark_fuse_topology(
    snapshot: &FusePageSnapshot,
    target_tag: &[u8],
    cells: &[usize],
    server_count: usize,
    profile: Profile,
    global_build_ms: f64,
    layout_lookup_p50_us: f64,
) -> Result<MphfTopologyResult> {
    let evaluators = evaluators(server_count)?;
    let mut rng = StdRng::seed_from_u64(0xf053_4d50 ^ server_count as u64);
    let warmup = dense::query_shares_for_buckets(
        cells,
        snapshot.manifest.cell_count,
        server_count,
        &mut rng,
    )?
    .into_iter()
    .map(|query| vec![query])
    .collect::<Vec<_>>();
    let warmup = evaluate(snapshot.view(), &evaluators, &warmup)?;
    reconstruct_fuse(snapshot, target_tag, warmup.answers)?;

    let samples = sample_count(profile);
    let mut query_generation = Vec::with_capacity(samples);
    let mut wall = Vec::with_capacity(samples);
    let mut server_elapsed = Vec::with_capacity(samples);
    let mut reconstruct = Vec::with_capacity(samples);
    for _ in 0..samples {
        let query_started = Instant::now();
        let queries = dense::query_shares_for_buckets(
            cells,
            snapshot.manifest.cell_count,
            server_count,
            &mut rng,
        )?
        .into_iter()
        .map(|query| vec![query])
        .collect::<Vec<_>>();
        query_generation.push(query_started.elapsed());
        let evaluated = evaluate(snapshot.view(), &evaluators, &queries)?;
        wall.push(evaluated.wall);
        server_elapsed.push(evaluated.sum_server_elapsed);
        let reconstruct_started = Instant::now();
        reconstruct_fuse(snapshot, target_tag, evaluated.answers)?;
        reconstruct.push(reconstruct_started.elapsed());
    }
    topology_result(
        server_count,
        snapshot.manifest.cell_count,
        snapshot.manifest.page_size,
        snapshot.manifest.values_per_page * snapshot.manifest.max_value_bytes,
        snapshot.rows().len(),
        global_build_ms,
        snapshot.build_metrics.peak_tracked_bytes,
        snapshot.manifest.client_metadata_bytes(),
        None,
        layout_lookup_p50_us,
        "fuse-4-retrieval-over-replicated-dense-xor",
        "global immutable snapshot; generation-specific Fuse dimensions and peel seed are public",
        "exact information-theoretic query privacy; public Fuse seed reveals a successful-peel constraint",
        query_generation,
        wall,
        server_elapsed,
        reconstruct,
    )
}

struct Evaluation {
    answers: Vec<Vec<Vec<u8>>>,
    wall: Duration,
    sum_server_elapsed: Duration,
}

fn evaluate(
    snapshot: SnapshotView<'_>,
    evaluators: &[ParallelEvaluator],
    queries: &[Vec<Vec<u8>>],
) -> Result<Evaluation> {
    let wall_started = Instant::now();
    let results = std::thread::scope(|scope| {
        evaluators
            .iter()
            .zip(queries)
            .map(|(evaluator, queries)| {
                scope.spawn(move || {
                    let started = Instant::now();
                    let answers = evaluator.answer_batch(snapshot, queries)?;
                    Ok::<_, anyhow::Error>((started.elapsed(), answers))
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| handle.join().expect("MPHF benchmark server panicked"))
            .collect::<Result<Vec<_>>>()
    })?;
    Ok(Evaluation {
        wall: wall_started.elapsed(),
        sum_server_elapsed: results.iter().map(|(elapsed, _)| *elapsed).sum(),
        answers: results.into_iter().map(|(_, answers)| answers).collect(),
    })
}

fn reconstruct_mphf(
    snapshot: &MphfPageSnapshot,
    target_tag: &[u8],
    answers: Vec<Vec<Vec<u8>>>,
) -> Result<()> {
    let shares = answers
        .iter()
        .map(|server| server[0].as_slice())
        .collect::<Vec<_>>();
    let page = snapshot
        .decode_retrieved_page(&dense::combine(&shares)?, target_tag, 0)?
        .context("MPHF did not recover the selected page")?;
    if page.values.len() != snapshot.manifest.values_per_page {
        bail!("MPHF recovered the wrong page");
    }
    Ok(())
}

fn reconstruct_fuse(
    snapshot: &FusePageSnapshot,
    target_tag: &[u8],
    answers: Vec<Vec<Vec<u8>>>,
) -> Result<()> {
    let shares = answers
        .iter()
        .map(|server| server[0].as_slice())
        .collect::<Vec<_>>();
    let page = snapshot
        .decode_retrieved_page(&dense::combine(&shares)?, target_tag, 0)?
        .context("Fuse-4 did not recover the selected page")?;
    if page.values.len() != snapshot.manifest.values_per_page {
        bail!("Fuse-4 recovered the wrong page");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn topology_result(
    server_count: usize,
    row_count: usize,
    row_size: usize,
    useful_result_bytes: usize,
    table_bytes_per_server: usize,
    _global_build_wall_ms: f64,
    peak_tracked_build_bytes: usize,
    client_metadata_bytes: usize,
    client_index_load_ms: Option<f64>,
    layout_lookup_p50_us: f64,
    protocol: &'static str,
    public_partition: &'static str,
    privacy: &'static str,
    mut query_generation: Vec<Duration>,
    mut wall: Vec<Duration>,
    mut server_elapsed: Vec<Duration>,
    mut reconstruct: Vec<Duration>,
) -> Result<MphfTopologyResult> {
    for samples in [
        &mut query_generation,
        &mut wall,
        &mut server_elapsed,
        &mut reconstruct,
    ] {
        samples.sort_unstable();
    }
    let expected_rows_per_server = row_count.div_ceil(2);
    let expected_bytes_per_server = expected_rows_per_server
        .checked_mul(row_size)
        .context("expected byte work overflow")?;
    let query_bytes_per_server = dense::query_size(row_count);
    let query_generation_p50_us = micros(percentile(&query_generation, 50));
    let wall_p50_ms = millis(percentile(&wall, 50));
    let aggregate_server_p50_ms = millis(percentile(&server_elapsed, 50));
    let reconstruct_p50_us = micros(percentile(&reconstruct, 50));
    let aggregate_work = topology_accounting(
        protocol,
        public_partition,
        privacy,
        server_count,
        table_bytes_per_server,
        peak_tracked_build_bytes,
        client_metadata_bytes,
        client_index_load_ms,
        layout_lookup_p50_us,
        expected_bytes_per_server,
        query_bytes_per_server,
        row_size,
        useful_result_bytes,
        query_generation_p50_us,
        wall_p50_ms,
        aggregate_server_p50_ms,
        reconstruct_p50_us,
    )?;
    Ok(MphfTopologyResult {
        aggregate_work,
        server_count,
        privacy_collusion_tolerance: server_count - 1,
        required_answers: server_count,
        expected_rows_xored_per_server: expected_rows_per_server,
        expected_rows_xored_all_servers: expected_rows_per_server * server_count,
        expected_data_bytes_xored_per_server: expected_bytes_per_server,
        expected_data_bytes_xored_all_servers: expected_bytes_per_server * server_count,
        query_bytes_per_server,
        total_client_upload_bytes: query_bytes_per_server * server_count,
        response_bytes_per_server: row_size,
        total_client_download_bytes: row_size * server_count,
        client_query_generation_p50_us: query_generation_p50_us,
        co_located_wall_p50_ms: wall_p50_ms,
        co_located_wall_p95_ms: millis(percentile(&wall, 95)),
        sum_server_elapsed_p50_ms: aggregate_server_p50_ms,
        client_reconstruct_p50_us: reconstruct_p50_us,
    })
}

#[allow(clippy::too_many_arguments)]
fn topology_accounting(
    protocol: &'static str,
    public_partition: &'static str,
    privacy: &'static str,
    server_count: usize,
    table_bytes_per_server: usize,
    peak_tracked_build_bytes: usize,
    client_metadata_bytes: usize,
    client_index_load_ms: Option<f64>,
    layout_lookup_p50_us: f64,
    expected_data_bytes_per_server: usize,
    query_bytes_per_server: usize,
    response_bytes_per_server: usize,
    useful_result_bytes: usize,
    query_generation_p50_us: f64,
    wall_p50_ms: f64,
    aggregate_server_p50_ms: f64,
    reconstruct_p50_us: f64,
) -> Result<AggregateWorkReport> {
    let mut work = AggregateWorkReport::new(
        protocol,
        ComparisonScope {
            workload: "one lookup over the identical populated immutable tag-page corpus",
            result: "one first page containing four fixed-width compact locators",
            public_partition,
            leakage: LeakageScope::ExactQueryPrivacy,
        },
        SecurityLabels {
            privacy,
            server_count,
            collusion_tolerance: server_count - 1,
            required_answers: server_count,
            assumptions: "at least one replicated Dense XOR server does not collude; every server uses the same authenticated immutable generation",
            availability: "all answer shares are required",
            integrity: "128-bit page fingerprint validates membership/wrong-row rejection under the semi-honest model; it is not a malicious-server proof or MAC",
        },
    );
    work.global_build.aggregate_server_time_ms = Metric::not_measured(
        "aggregate builder CPU time was not measured; layout build wall time is reported by the enclosing result",
    );
    work.global_build.client_time_ms = Metric::not_applicable("build is server-side");
    work.global_build.peak_server_ram_bytes = Metric::estimated(
        peak_tracked_build_bytes,
        "tracked algorithm-owned buffers; PtrHash transient workspace and runtime RSS are excluded",
    );
    work.global_build.client_upload_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.client_download_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.server_scans = Metric::not_measured(
        "layout construction passes were not instrumented; one build is not equivalent to one scan",
    );
    work.global_build.network_rounds = Metric::not_applicable("build is server-local");
    let mut setup = PhaseWork::unmeasured(
        "one client loading one immutable generation",
        "metadata distribution server work, physical traffic, and peak client RAM were not measured",
    );
    setup.client_time_ms = client_index_load_ms.map_or_else(
        || Metric::not_measured("Fuse metadata parse time was not measured"),
        |value| {
            Metric::measured(
                value,
                "validated artifact parsing and full PtrHash deserialization",
            )
        },
    );
    setup.client_upload_bytes = Metric::deterministic(0, "public metadata fetch has no PIR upload");
    setup.client_download_bytes = Metric::deterministic(
        client_metadata_bytes,
        "one authenticated generation-specific public artifact",
    );
    setup.network_rounds = Metric::estimated(
        1,
        "one metadata fetch is assumed; network latency was not benchmarked",
    );
    work.per_client_setup = setup;
    work.maintenance = PhaseWork::not_applicable(
        "immutable snapshot lifetime",
        "a changed snapshot is represented by a new global build and generation",
    );

    let estimated_physical_bytes_per_server = expected_data_bytes_per_server
        .checked_add(query_bytes_per_server)
        .context("estimated online byte work overflow")?;
    for server in &mut work.online.per_server {
        server.server_time_p50_ms = Metric::estimated(
            aggregate_server_p50_ms / server_count as f64,
            "sum-server p50 divided evenly; individual server samples were not retained",
        );
        server.logical_selected_bytes = Metric::estimated(
            expected_data_bytes_per_server,
            "uniform random selector shares select half the rows in expectation",
        );
        server.physical_or_scanned_bytes = Metric::estimated(
            estimated_physical_bytes_per_server,
            "selector bytes plus expected XOR row payload; cache-line and DRAM traffic were not measured",
        );
        server.scans = Metric::deterministic(1, "one Dense selector traversal");
    }
    work.online.unit = "one first-page tag lookup";
    work.online.aggregate_server_time_p50_ms = Metric::measured(
        aggregate_server_p50_ms,
        "sum of measured server elapsed times for the co-located topology",
    );
    work.online.max_server_time_p50_ms = Metric::estimated(
        wall_p50_ms,
        "co-located wall p50 is an upper-envelope proxy including dispatch overhead",
    );
    work.online.aggregate_logical_selected_bytes = Metric::estimated(
        expected_data_bytes_per_server * server_count,
        "sum of expected selected row bytes across all replicated servers",
    );
    work.online.aggregate_physical_or_scanned_bytes = Metric::estimated(
        estimated_physical_bytes_per_server * server_count,
        "sum of selector and expected row bytes; no hardware counter was collected",
    );
    work.online.server_scans = Metric::deterministic(
        server_count,
        "one Dense traversal on every replicated server",
    );
    work.online.network_rounds = Metric::deterministic(1, "all shares are sent in parallel");
    work.online.useful_result_bytes = Metric::deterministic(
        useful_result_bytes,
        "four fixed-width locators, excluding the private page framing",
    );
    work.client.online_cpu_p50_ms = Metric::estimated(
        (layout_lookup_p50_us + query_generation_p50_us + reconstruct_p50_us) / 1_000.0,
        "sum of separately sampled layout-lookup, Dense-share-generation, and reconstruction medians",
    );
    work.client.peak_transient_ram_bytes = Metric::not_measured(
        "client process peak RAM was not sampled; the public artifact size is reported separately",
    );
    work.client.persistent_state_bytes = Metric::deterministic(
        client_metadata_bytes,
        "authenticated public metadata retained for one immutable generation",
    );
    work.client.upload_bytes = Metric::deterministic(
        query_bytes_per_server * server_count,
        "one Dense selector share per server",
    );
    work.client.download_bytes = Metric::deterministic(
        response_bytes_per_server * server_count,
        "one answer page share per server",
    );
    work.persisted_storage.server_bytes_per_server =
        Metric::deterministic(table_bytes_per_server, "one replicated Dense table");
    work.persisted_storage.aggregate_server_bytes = Metric::deterministic(
        table_bytes_per_server * server_count,
        "sum of Dense table replicas across all servers",
    );
    work.persisted_storage.client_bytes = Metric::deterministic(
        client_metadata_bytes,
        "one generation-specific public metadata artifact",
    );
    work.amortization = AmortizationHorizon {
        global_build: "all clients and lookups using one immutable generation",
        per_client_setup: "all lookups by one client before generation refresh",
        maintenance: "not applicable; updates create a new immutable generation",
        assumed_global_queries: None,
        assumed_queries_per_client_setup: None,
        assumed_online_events_per_maintenance: None,
        note: "Build wall time and client metadata load remain separate from online work; no amortization denominator is assumed.",
    };
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    Ok(work)
}

fn benchmark_layout_lookup<T>(mut lookup: impl FnMut() -> Result<T>) -> Result<f64> {
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

fn evaluators(server_count: usize) -> Result<Vec<ParallelEvaluator>> {
    (0..server_count)
        .map(|_| ParallelEvaluator::new(SERVER_WORKERS))
        .collect()
}

fn sample_count(profile: Profile) -> usize {
    match profile {
        Profile::Quick => 7,
        Profile::Full => 31,
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
