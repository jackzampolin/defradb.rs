use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;

use super::Profile;
use crate::{
    dense::{self, ParallelEvaluator},
    fuse_pages::{FuseArity, FusePageSnapshot},
    mphf_pages::MphfPageSnapshot,
    ribbon_pages::{RibbonConfig, RibbonPageSnapshot},
    snapshot::SnapshotView,
    tag_pages::{benchmark_page_set, benchmark_tag, TagPageConfig, TagPageSnapshot},
};

const SERVER_COUNTS: [usize; 2] = [2, 3];
const SERVER_WORKERS: usize = 2;
const RIBBON_CONFIG: RibbonConfig = RibbonConfig {
    width: 64,
    overhead_percent: 10,
};
const METHODOLOGY: &str = "PtrHash exact Dense, Standard Ribbon, Fuse-4, and packed cuckoo consume the identical pre-encoded populated tag-page corpus and return one first page containing four 16-byte locators. Standard Ribbon follows the primary paper's incremental banded GF(2) solver and back substitution at width 64 and epsilon 10%. MPHF, Ribbon, and Fuse issue one Dense evaluation; cuckoo issues two independent evaluations. Every topology is warmed once, timed with fresh cryptographic shares, and correctness checked. Co-located servers each own two persistent workers and contend for one memory bus; HTTP, TLS, serialization, metadata transfer, and network latency are excluded.";
const PEAK_NOTE: &str = "Deterministic algorithm-owned buffers, including the common encoded corpus, output, and explicit temporary vectors; allocator/runtime overhead is excluded. PtrHash's unexposed construction workspace is additionally excluded.";

#[derive(Clone, Debug, Serialize)]
pub struct RibbonComparisonReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: &'static str,
    pub workload: RibbonWorkload,
    pub encoded_corpus_build_ms: f64,
    pub encoded_corpus_tracked_bytes: usize,
    pub layouts: Vec<RibbonLayoutResult>,
    pub bumped_ribbon: BumpedRibbonAssessment,
}

#[derive(Clone, Debug, Serialize)]
pub struct RibbonWorkload {
    pub document_count: usize,
    pub distinct_tag_count: usize,
    pub documents_per_tag: usize,
    pub encoded_page_count: usize,
    pub encoded_payload_bytes: usize,
    pub values_per_page: usize,
    pub locator_bytes: usize,
    pub page_bytes: usize,
    pub selected_page: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct RibbonLayoutResult {
    pub layout: &'static str,
    pub construction: &'static str,
    pub public_selector: &'static str,
    pub dense_evaluations_per_server: usize,
    pub selector_hamming_weight: usize,
    pub table_rows: usize,
    pub row_bytes: usize,
    pub dense_table_bytes_per_server: usize,
    pub public_metadata_bytes: usize,
    pub persistent_bytes_per_server: usize,
    pub storage_expansion_vs_encoded_pages: f64,
    pub absent_key_verification_bits: usize,
    pub layout_build_ms: f64,
    pub global_build_wall_ms: f64,
    pub build_attempts: usize,
    pub peak_tracked_build_bytes: usize,
    pub peak_build_memory_note: &'static str,
    pub generation: Option<String>,
    pub client_metadata_load_ms: Option<f64>,
    pub client_layout_lookup_p50_us: f64,
    pub topologies: Vec<RibbonTopologyResult>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RibbonTopologyResult {
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
    pub client_share_generation_p50_us: f64,
    pub co_located_wall_p50_ms: f64,
    pub co_located_wall_p95_ms: f64,
    pub sum_server_elapsed_p50_ms: f64,
    pub client_reconstruct_p50_us: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct BumpedRibbonAssessment {
    pub reference: &'static str,
    pub official_implementation: &'static str,
    pub status: &'static str,
    pub why_not_measured_as_standard_ribbon: &'static str,
    pub required_client_metadata: &'static str,
    pub pir_consequence: &'static str,
    pub integration_record: &'static str,
    pub next_experiment: &'static str,
}

pub fn run(profile: Profile) -> Result<RibbonComparisonReport> {
    let (document_count, distinct_tag_count) = match profile {
        Profile::Quick => (1 << 20, 1 << 18),
        Profile::Full => (1 << 22, 1 << 20),
    };
    let page_config = TagPageConfig {
        bucket_capacity: 4,
        target_load_percent: 90,
        values_per_page: 4,
        max_value_bytes: 16,
    };
    let corpus_started = Instant::now();
    let page_set = benchmark_page_set(document_count, distinct_tag_count, &page_config)?;
    let encoded_corpus_build_ms = millis(corpus_started.elapsed());
    let encoded_corpus_tracked_bytes = page_set.tracked_bytes();
    let encoded_payload_bytes = page_set.pages.len() * page_config.page_size()?;
    let target_tag = benchmark_tag(distinct_tag_count / 3);
    let mut layouts = Vec::with_capacity(4);

    let started = Instant::now();
    let mphf = MphfPageSnapshot::from_page_set(&page_set, page_config.clone())?;
    let build_ms = millis(started.elapsed());
    layouts.push(benchmark_mphf(
        &mphf,
        &target_tag,
        profile,
        encoded_payload_bytes,
        build_ms,
        encoded_corpus_build_ms + build_ms,
    )?);
    drop(mphf);

    let started = Instant::now();
    let ribbon = RibbonPageSnapshot::from_page_set(&page_set, page_config.clone(), RIBBON_CONFIG)?;
    let build_ms = millis(started.elapsed());
    layouts.push(benchmark_ribbon(
        &ribbon,
        &target_tag,
        profile,
        encoded_payload_bytes,
        build_ms,
        encoded_corpus_build_ms + build_ms,
    )?);
    drop(ribbon);

    let started = Instant::now();
    let fuse = FusePageSnapshot::from_page_set(&page_set, page_config.clone(), FuseArity::Four)?;
    let build_ms = millis(started.elapsed());
    layouts.push(benchmark_fuse(
        &fuse,
        &target_tag,
        profile,
        encoded_payload_bytes,
        build_ms,
        encoded_corpus_build_ms + build_ms,
    )?);
    drop(fuse);

    let started = Instant::now();
    let cuckoo = TagPageSnapshot::from_page_set(&page_set, page_config.clone())?;
    let build_ms = millis(started.elapsed());
    layouts.push(benchmark_cuckoo(
        &cuckoo,
        &target_tag,
        profile,
        encoded_payload_bytes,
        build_ms,
        encoded_corpus_build_ms + build_ms,
    )?);

    Ok(RibbonComparisonReport {
        protocol: "standard-ribbon-layout-tournament-over-replicated-dense-xor",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: METHODOLOGY,
        workload: RibbonWorkload {
            document_count,
            distinct_tag_count,
            documents_per_tag: document_count / distinct_tag_count,
            encoded_page_count: page_set.pages.len(),
            encoded_payload_bytes,
            values_per_page: page_config.values_per_page,
            locator_bytes: page_config.max_value_bytes,
            page_bytes: page_config.page_size()?,
            selected_page: 0,
        },
        encoded_corpus_build_ms,
        encoded_corpus_tracked_bytes,
        layouts,
        bumped_ribbon: BumpedRibbonAssessment {
            reference: "Dillinger, H\u{fc}bschle-Schneider, Sanders, and Walzer, Fast Succinct Retrieval and Approximate Membership Using Ribbon (SEA 2022), https://arxiv.org/abs/2109.01892",
            official_implementation: "https://github.com/lorenzhs/BuRR (Apache-2.0 C++; vendored submodules, compile-time result width, recursive bump layers)",
            status: "analytically evaluated; not labeled or timed as an in-process Rust implementation",
            why_not_measured_as_standard_ribbon: "BuRR overloads a primary Ribbon layer, bumps deterministic chunks into recursive backyard layers, and routes queries using explicit bump metadata. Dropping failed equations or timing only the first layer would be incorrect.",
            required_client_metadata: "The client must know each layer's seed/dimensions plus per-bucket bump-routing metadata in order to derive the right layer and Ribbon equation from the public key.",
            pir_consequence: "A faithful lookup selects one Ribbon equation in exactly one routed layer. Layers may be concatenated into one Dense table for one multi-hot PIR request, but the public bump route is key-dependent metadata and has a different leakage/artifact profile from Standard Ribbon's constant manifest.",
            integration_record: "The official implementation is a research C++ header/library centered on fixed-width integer retrieval values and recursive template configuration. This POC stores 96-byte pages and needs exposed route/row indices for Dense selector construction; an FFI wrapper around QueryRetrieval would reveal the value publicly inside the client process but does not expose the selected cells. Reimplementing only its surface API would not establish fidelity.",
            next_experiment: "Add a separately reviewed C++ adapter that exports the exact bump route and equation cells for arbitrary fixed-size page stripes, validate it against QueryRetrieval, then benchmark concatenated-layer Dense selectors. Keep its public routing metadata and build memory explicit.",
        },
    })
}

fn benchmark_mphf(
    snapshot: &MphfPageSnapshot,
    target_tag: &[u8],
    profile: Profile,
    encoded_payload_bytes: usize,
    layout_build_ms: f64,
    global_build_wall_ms: f64,
) -> Result<RibbonLayoutResult> {
    let load_started = Instant::now();
    let client = snapshot.trusted_client_index()?;
    let load_ms = millis(load_started.elapsed());
    let ordinal = client.ordinal(target_tag, 0)?;
    let lookup_us = benchmark_lookup(|| client.ordinal(target_tag, 0))?;
    let selectors = vec![vec![ordinal]];
    let topologies = benchmark_topologies(snapshot.view(), &selectors, profile, |answers| {
        let retrieved = combine_answer(answers, 0)?;
        snapshot
            .decode_retrieved_page(&retrieved, target_tag, 0)?
            .context("MPHF failed to recover the selected page")?;
        Ok(())
    })?;
    let metadata = snapshot.manifest.client_metadata_bytes();
    Ok(RibbonLayoutResult {
        layout: "ptrhash-exact-mphf-dense",
        construction: "production PtrHash MPHF with exact remap",
        public_selector: "one exact ordinal from generation-specific public MPHF metadata",
        dense_evaluations_per_server: 1,
        selector_hamming_weight: 1,
        table_rows: snapshot.manifest.page_count,
        row_bytes: snapshot.manifest.page_size,
        dense_table_bytes_per_server: snapshot.rows().len(),
        public_metadata_bytes: metadata,
        persistent_bytes_per_server: snapshot.rows().len() + metadata,
        storage_expansion_vs_encoded_pages: snapshot.rows().len() as f64
            / encoded_payload_bytes as f64,
        absent_key_verification_bits: snapshot.manifest.absent_key_verification_bits(),
        layout_build_ms,
        global_build_wall_ms,
        build_attempts: snapshot.build_metrics.attempts,
        peak_tracked_build_bytes: snapshot.build_metrics.peak_tracked_bytes,
        peak_build_memory_note: PEAK_NOTE,
        generation: Some(snapshot.manifest.generation_hex()),
        client_metadata_load_ms: Some(load_ms),
        client_layout_lookup_p50_us: lookup_us,
        topologies,
    })
}

fn benchmark_ribbon(
    snapshot: &RibbonPageSnapshot,
    target_tag: &[u8],
    profile: Profile,
    encoded_payload_bytes: usize,
    layout_build_ms: f64,
    global_build_wall_ms: f64,
) -> Result<RibbonLayoutResult> {
    let cells = snapshot.cells(target_tag, 0)?;
    let lookup_us = benchmark_lookup(|| snapshot.cells(target_tag, 0))?;
    let selectors = vec![cells.clone()];
    let topologies = benchmark_topologies(snapshot.view(), &selectors, profile, |answers| {
        let retrieved = combine_answer(answers, 0)?;
        snapshot
            .decode_retrieved_page(&retrieved, target_tag, 0)?
            .context("Standard Ribbon failed to recover the selected page")?;
        Ok(())
    })?;
    let metadata = snapshot.manifest.client_metadata_bytes();
    Ok(RibbonLayoutResult {
        layout: "standard-ribbon-64-epsilon-10pct",
        construction: "primary Standard Ribbon Algorithms 1-3 over GF(2)",
        public_selector: "one contiguous width-64 equation derived from public key and constant generation manifest",
        dense_evaluations_per_server: 1,
        selector_hamming_weight: cells.len(),
        table_rows: snapshot.manifest.cell_count,
        row_bytes: snapshot.manifest.page_size,
        dense_table_bytes_per_server: snapshot.rows().len(),
        public_metadata_bytes: metadata,
        persistent_bytes_per_server: snapshot.rows().len() + metadata,
        storage_expansion_vs_encoded_pages: snapshot.rows().len() as f64
            / encoded_payload_bytes as f64,
        absent_key_verification_bits: snapshot.manifest.absent_key_verification_bits(),
        layout_build_ms,
        global_build_wall_ms,
        build_attempts: snapshot.build_metrics.attempts,
        peak_tracked_build_bytes: snapshot.build_metrics.peak_tracked_bytes,
        peak_build_memory_note: PEAK_NOTE,
        generation: Some(snapshot.manifest.generation_hex()),
        client_metadata_load_ms: None,
        client_layout_lookup_p50_us: lookup_us,
        topologies,
    })
}

fn benchmark_fuse(
    snapshot: &FusePageSnapshot,
    target_tag: &[u8],
    profile: Profile,
    encoded_payload_bytes: usize,
    layout_build_ms: f64,
    global_build_wall_ms: f64,
) -> Result<RibbonLayoutResult> {
    let cells = snapshot.cells(target_tag, 0)?;
    let lookup_us = benchmark_lookup(|| snapshot.cells(target_tag, 0))?;
    let selectors = vec![cells.clone()];
    let topologies = benchmark_topologies(snapshot.view(), &selectors, profile, |answers| {
        let retrieved = combine_answer(answers, 0)?;
        snapshot
            .decode_retrieved_page(&retrieved, target_tag, 0)?
            .context("Fuse-4 failed to recover the selected page")?;
        Ok(())
    })?;
    let metadata = snapshot.manifest.client_metadata_bytes();
    Ok(RibbonLayoutResult {
        layout: "fuse-4-retrieval",
        construction: "four-wise spatially coupled peelable static function",
        public_selector:
            "four cells derived from public key and constant generation dimensions/seed",
        dense_evaluations_per_server: 1,
        selector_hamming_weight: cells.len(),
        table_rows: snapshot.manifest.cell_count,
        row_bytes: snapshot.manifest.page_size,
        dense_table_bytes_per_server: snapshot.rows().len(),
        public_metadata_bytes: metadata,
        persistent_bytes_per_server: snapshot.rows().len() + metadata,
        storage_expansion_vs_encoded_pages: snapshot.rows().len() as f64
            / encoded_payload_bytes as f64,
        absent_key_verification_bits: 128,
        layout_build_ms,
        global_build_wall_ms,
        build_attempts: snapshot.build_metrics.attempts,
        peak_tracked_build_bytes: snapshot.build_metrics.peak_tracked_bytes,
        peak_build_memory_note: PEAK_NOTE,
        generation: None,
        client_metadata_load_ms: None,
        client_layout_lookup_p50_us: lookup_us,
        topologies,
    })
}

fn benchmark_cuckoo(
    snapshot: &TagPageSnapshot,
    target_tag: &[u8],
    profile: Profile,
    encoded_payload_bytes: usize,
    layout_build_ms: f64,
    global_build_wall_ms: f64,
) -> Result<RibbonLayoutResult> {
    let buckets = snapshot.candidate_buckets(target_tag, 0)?;
    let lookup_us = benchmark_lookup(|| snapshot.candidate_buckets(target_tag, 0))?;
    let selectors = buckets
        .into_iter()
        .map(|bucket| vec![bucket])
        .collect::<Vec<_>>();
    let topologies = benchmark_topologies(snapshot.view(), &selectors, profile, |answers| {
        let mut found = false;
        for candidate in 0..2 {
            let retrieved = combine_answer(answers, candidate)?;
            found |= snapshot
                .decode_bucket_row(&retrieved, target_tag, 0)?
                .is_some();
        }
        if !found {
            bail!("packed cuckoo failed to recover the selected page");
        }
        Ok(())
    })?;
    let metadata = snapshot.manifest.client_metadata_bytes();
    Ok(RibbonLayoutResult {
        layout: "packed-cuckoo",
        construction: "two-choice cuckoo table with four page slots per bucket",
        public_selector:
            "two independent candidate buckets derived from public key and constant manifest",
        dense_evaluations_per_server: 2,
        selector_hamming_weight: 2,
        table_rows: snapshot.manifest.bucket_count,
        row_bytes: snapshot.manifest.row_size,
        dense_table_bytes_per_server: snapshot.rows().len(),
        public_metadata_bytes: metadata,
        persistent_bytes_per_server: snapshot.rows().len() + metadata,
        storage_expansion_vs_encoded_pages: snapshot.rows().len() as f64
            / encoded_payload_bytes as f64,
        absent_key_verification_bits: 128,
        layout_build_ms,
        global_build_wall_ms,
        build_attempts: snapshot.build_metrics.attempts,
        peak_tracked_build_bytes: snapshot.build_metrics.peak_tracked_bytes,
        peak_build_memory_note: PEAK_NOTE,
        generation: None,
        client_metadata_load_ms: None,
        client_layout_lookup_p50_us: lookup_us,
        topologies,
    })
}

fn benchmark_topologies(
    snapshot: SnapshotView<'_>,
    selectors: &[Vec<usize>],
    profile: Profile,
    verify: impl Fn(&[Vec<Vec<u8>>]) -> Result<()>,
) -> Result<Vec<RibbonTopologyResult>> {
    SERVER_COUNTS
        .into_iter()
        .map(|server_count| benchmark_topology(snapshot, selectors, server_count, profile, &verify))
        .collect()
}

fn benchmark_topology(
    snapshot: SnapshotView<'_>,
    selectors: &[Vec<usize>],
    server_count: usize,
    profile: Profile,
    verify: &impl Fn(&[Vec<Vec<u8>>]) -> Result<()>,
) -> Result<RibbonTopologyResult> {
    let evaluators = (0..server_count)
        .map(|_| ParallelEvaluator::new(SERVER_WORKERS))
        .collect::<Result<Vec<_>>>()?;
    let mut rng = StdRng::seed_from_u64(
        0x5249_4242 ^ snapshot.bucket_count as u64 ^ selectors.len() as u64 ^ server_count as u64,
    );
    let warmup_queries = queries(selectors, snapshot.bucket_count, server_count, &mut rng)?;
    let warmup = evaluate(snapshot, &evaluators, &warmup_queries)?;
    verify(&warmup.answers)?;

    let samples = match profile {
        Profile::Quick => 7,
        Profile::Full => 31,
    };
    let mut query_generation = Vec::with_capacity(samples);
    let mut wall = Vec::with_capacity(samples);
    let mut server_elapsed = Vec::with_capacity(samples);
    let mut reconstruct = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        let generated = queries(selectors, snapshot.bucket_count, server_count, &mut rng)?;
        query_generation.push(started.elapsed());
        let evaluated = evaluate(snapshot, &evaluators, &generated)?;
        wall.push(evaluated.wall);
        server_elapsed.push(evaluated.sum_server_elapsed);
        let started = Instant::now();
        verify(&evaluated.answers)?;
        reconstruct.push(started.elapsed());
    }
    for values in [
        &mut query_generation,
        &mut wall,
        &mut server_elapsed,
        &mut reconstruct,
    ] {
        values.sort_unstable();
    }

    let evaluations = selectors.len();
    let expected_rows_per_server = evaluations * snapshot.bucket_count.div_ceil(2);
    let expected_bytes_per_server = expected_rows_per_server * snapshot.row_size;
    let query_bytes_per_server = evaluations * dense::query_size(snapshot.bucket_count);
    let response_bytes_per_server = evaluations * snapshot.row_size;
    Ok(RibbonTopologyResult {
        server_count,
        privacy_collusion_tolerance: server_count - 1,
        required_answers: server_count,
        expected_rows_xored_per_server: expected_rows_per_server,
        expected_rows_xored_all_servers: expected_rows_per_server * server_count,
        expected_data_bytes_xored_per_server: expected_bytes_per_server,
        expected_data_bytes_xored_all_servers: expected_bytes_per_server * server_count,
        query_bytes_per_server,
        total_client_upload_bytes: query_bytes_per_server * server_count,
        response_bytes_per_server,
        total_client_download_bytes: response_bytes_per_server * server_count,
        client_share_generation_p50_us: micros(percentile(&query_generation, 50)),
        co_located_wall_p50_ms: millis(percentile(&wall, 50)),
        co_located_wall_p95_ms: millis(percentile(&wall, 95)),
        sum_server_elapsed_p50_ms: millis(percentile(&server_elapsed, 50)),
        client_reconstruct_p50_us: micros(percentile(&reconstruct, 50)),
    })
}

fn queries(
    selectors: &[Vec<usize>],
    row_count: usize,
    server_count: usize,
    rng: &mut StdRng,
) -> Result<Vec<Vec<Vec<u8>>>> {
    let mut per_server = (0..server_count)
        .map(|_| Vec::with_capacity(selectors.len()))
        .collect::<Vec<_>>();
    for selector in selectors {
        let shares = dense::query_shares_for_buckets(selector, row_count, server_count, rng)?;
        for (server, share) in per_server.iter_mut().zip(shares) {
            server.push(share);
        }
    }
    Ok(per_server)
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
            .map(|handle| handle.join().expect("Ribbon benchmark server panicked"))
            .collect::<Result<Vec<_>>>()
    })?;
    Ok(Evaluation {
        wall: wall_started.elapsed(),
        sum_server_elapsed: results.iter().map(|(elapsed, _)| *elapsed).sum(),
        answers: results.into_iter().map(|(_, answers)| answers).collect(),
    })
}

fn combine_answer(answers: &[Vec<Vec<u8>>], evaluation: usize) -> Result<Vec<u8>> {
    dense::combine(
        &answers
            .iter()
            .map(|server| server[evaluation].as_slice())
            .collect::<Vec<_>>(),
    )
}

fn benchmark_lookup<T>(mut lookup: impl FnMut() -> Result<T>) -> Result<f64> {
    const SAMPLES: usize = 1_001;
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        std::hint::black_box(lookup()?);
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    Ok(micros(percentile(&samples, 50)))
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
