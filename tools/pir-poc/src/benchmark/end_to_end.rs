//! End-to-end private-result experiment.
//!
//! Locator-only PIR numbers are easy to misread: returning a CID privately is
//! not a private document lookup if the next fetch reveals that CID.  This
//! benchmark therefore compares two complete strict-private immutable-snapshot
//! designs and one workload-identical weaker baseline:
//!
//! * exact-MPHF Dense rows containing the requested fixed-width projections;
//! * exact-MPHF locator pages followed by a second, privately batched Dense
//!   retrieval from a compact ordinal document table.
//! * ordinary MPHF lookup for one visible target plus 99 visible decoys, with
//!   every candidate returning the same padded continuation schedule.
//!
//! The locator-only phase is retained solely as a labelled diagnostic.  Every
//! continuation page and every second-stage selector is included in traffic,
//! scan, and aggregate-server-work accounting.

use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use serde::Serialize;

use super::{
    accounting::{
        unavailable_hardware_counters, AggregateWorkReport, ComparisonScope, LeakageScope, Metric,
        PhaseWork, SecurityLabels,
    },
    Profile,
};
use crate::{
    dense,
    dense_batch::{BatchEvaluator, BatchKernel},
    mphf_pages::MphfPageSnapshot,
    snapshot::{page_key, SnapshotView},
    tag_pages::{benchmark_tag, encode_page, EncodedPage, EncodedPageSet, TagPageConfig},
};

const DOCUMENT_COUNT: usize = 1 << 20;
const PROJECTION_SLOTS: [usize; 4] = [32, 96, 256, 1_024];
const TAG_FANOUTS: [usize; 5] = [1, 4, 16, 100, 1_000];
const SERVER_COUNTS: [usize; 2] = [2, 3];
const INLINE_PAGE_BUDGET: usize = 64 * 1_024;
const MAX_LOCATOR_VALUES_PER_PAGE: usize = 256;
const LOCATOR_BYTES: usize = 24;
const PAGE_HEADER_BYTES: usize = 24;
const VALUE_LENGTH_BYTES: usize = 2;
const QUICK_SAMPLES: usize = 3;
const FULL_SAMPLES: usize = 7;
const MAX_MEASURED_TABLE_BYTES: usize = 384 * 1_024 * 1_024;
const MAX_MEASURED_AGGREGATE_LOGICAL_BYTES: usize = 2 * 1_024 * 1_024 * 1_024;
const MAX_MEASURED_UPLOAD_BYTES: usize = 128 * 1_024 * 1_024;
const PUBLIC_INDEX_CANDIDATES: usize = 100;
const CORPUS_DOMAIN: &[u8] = b"defradb-pir-end-to-end-corpus-v1";
const COMPOSITION_DOMAIN: &[u8] = b"defradb-pir-end-to-end-composition-v1";

const METHODOLOGY: &str = "One deterministic 1,048,576-document corpus is represented in two complete strict-private forms and one deliberately weaker but workload-identical public-index baseline. Inline stores exact fixed-width projection slots in generation-bound PtrHash MPHF pages. Two-stage stores padded 24-byte (ordinal, 128-bit document fingerprint) locators in MPHF pages and then issues one private Dense selector per padded ordinal against a separate immutable projection table. The fair decoy baseline reuses the exact inline table, sends one target tag plus 99 visible present decoy tags, performs the same fixed padded continuation schedule for every candidate, and returns every complete fixed-size page. Tags use fanouts 1, 4, 16, 100, and 1,000, publicly padded to 1, 4, 16, 128, and 1,024. Inline cardinality is power-of-two padded subject to a 64-KiB page budget. Locator page capacity adapts to the same policy class up to 256 slots, so the five classes use 1, 4, 16, 128, and 256 slots. All continuation pages and dummy second-stage requests are charged. Replicas run concurrently, and every same-table private query batch uses the same shared row-major evaluator in one server thread: one table-ordering pass for inline, one for locator-only, and two for the complete two-stage result. A small resource-bounded cross-section is measured on the real million-row tables; the rest of the full sweep reports exact deterministic dimensions, traffic, and expected random-share XOR work with timing explicitly unmeasured.";

#[derive(Clone, Debug, Serialize)]
pub struct EndToEndReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: &'static str,
    pub workload: EndToEndWorkload,
    pub scenarios: Vec<EndToEndScenario>,
    pub break_even: Vec<BreakEven>,
    pub conclusions: Vec<&'static str>,
    pub production_caveats: Vec<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
pub struct EndToEndWorkload {
    pub document_count: usize,
    pub projection_slot_bytes: Vec<usize>,
    pub tag_fanouts: Vec<usize>,
    pub server_counts: Vec<usize>,
    pub inline_page_budget_bytes: usize,
    pub locator_slot_bytes: usize,
    pub maximum_locator_values_per_page: usize,
    pub public_index_candidate_count: usize,
    pub measured_cell_rule: &'static str,
    pub measured_resource_gate: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct EndToEndScenario {
    pub projection_slot_bytes: usize,
    pub requested_tag_fanout: usize,
    pub target_tag_result_count: usize,
    pub padded_result_count: usize,
    pub distinct_tag_count: usize,
    pub inline: ApproachResult,
    pub public_index_100_decoy: PublicIndexDecoyResult,
    pub private_two_stage: ApproachResult,
    pub locator_only_diagnostic: ApproachResult,
}

#[derive(Clone, Debug, Serialize)]
pub struct PublicIndexDecoyResult {
    pub approach: &'static str,
    pub result_scope: &'static str,
    pub privacy_scope: &'static str,
    pub cardinality_leakage: &'static str,
    pub timing_leakage: &'static str,
    pub privacy_equivalence_warning: &'static str,
    pub target_candidates: usize,
    pub decoy_candidates: usize,
    pub padded_result_count_per_candidate: usize,
    pub requested_pages_per_candidate: usize,
    pub ordinary_mphf_row_lookups: usize,
    pub table_rows: usize,
    pub row_bytes: usize,
    pub table_bytes: usize,
    pub server_index_bytes: Option<usize>,
    pub reused_inline_generation: Option<String>,
    pub topology: PublicIndexDecoyTopology,
}

#[derive(Clone, Debug, Serialize)]
pub struct PublicIndexDecoyTopology {
    pub aggregate_work: AggregateWorkReport,
    pub server_count: usize,
    pub measured: bool,
    pub measurement_skip_reason: Option<&'static str>,
    pub ordinary_mphf_row_lookups: usize,
    pub network_rounds: usize,
    pub raw_visible_tag_upload_bytes: usize,
    pub fixed_padded_page_download_bytes: usize,
    pub useful_target_projection_bytes: usize,
    pub deterministic_server_row_bytes: usize,
    pub server_p50_ms: Option<f64>,
    pub server_p95_ms: Option<f64>,
    pub client_decode_p50_ms: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ApproachResult {
    pub approach: &'static str,
    pub result_scope: &'static str,
    pub measured_layout: bool,
    pub layout_measurement_skip_reason: Option<&'static str>,
    pub page_value_capacity: usize,
    pub requested_continuation_pages: usize,
    pub primary_table_rows: usize,
    pub primary_row_bytes: usize,
    pub primary_table_bytes_per_server: usize,
    pub secondary_table_rows: usize,
    pub secondary_row_bytes: usize,
    pub secondary_table_bytes_per_server: usize,
    pub total_table_bytes_per_server: usize,
    pub public_client_metadata_bytes: Option<usize>,
    pub build_wall_ms: Option<f64>,
    pub peak_tracked_build_bytes: Option<usize>,
    pub generation_binding: Option<String>,
    pub topologies: Vec<EndToEndTopology>,
}

#[derive(Clone, Debug, Serialize)]
pub struct EndToEndTopology {
    pub aggregate_work: AggregateWorkReport,
    pub server_count: usize,
    pub measured: bool,
    pub measurement_skip_reason: Option<&'static str>,
    pub dense_requests_per_server: usize,
    pub table_passes_per_server: usize,
    pub network_rounds: usize,
    pub query_bytes_per_server: usize,
    pub total_client_upload_bytes: usize,
    pub response_bytes_per_server: usize,
    pub total_client_download_bytes: usize,
    pub useful_result_bytes: usize,
    pub expected_logical_xor_bytes_all_servers: usize,
    pub actual_logical_xor_bytes_p50_all_servers: Option<usize>,
    pub aggregate_server_p50_ms: Option<f64>,
    pub aggregate_server_p95_ms: Option<f64>,
    pub co_located_wall_p50_ms: Option<f64>,
    pub co_located_wall_p95_ms: Option<f64>,
    pub client_combine_decode_p50_ms: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct BreakEven {
    pub projection_slot_bytes: usize,
    pub tag_fanout: usize,
    pub server_count: usize,
    pub private_two_stage_over_inline_expected_server_work: f64,
    pub private_two_stage_over_inline_upload: f64,
    pub private_two_stage_over_inline_download: f64,
    pub private_two_stage_over_inline_server_storage: f64,
    pub measured_server_time_ratio: Option<f64>,
    pub locator_only_excluded_from_ratio_reason: &'static str,
}

#[derive(Clone, Copy)]
enum PageValue {
    Projection,
    Locator,
}

#[derive(Default)]
struct Samples {
    aggregate_server: Vec<Duration>,
    wall: Vec<Duration>,
    client: Vec<Duration>,
    actual_logical_bytes: Vec<usize>,
}

struct Evaluation {
    answers: Vec<Vec<Vec<u8>>>,
    wall: Duration,
    per_server: Vec<Duration>,
    actual_logical_bytes: usize,
}

#[derive(Clone, Copy)]
struct LayoutDimensions {
    distinct_tags: usize,
    target_values: usize,
    padded_values: usize,
    pages: usize,
    page_capacity: usize,
    page_size: usize,
    target_pages: usize,
    table_bytes: usize,
}

pub fn run(profile: Profile) -> Result<EndToEndReport> {
    let mut scenarios = Vec::new();
    let mut break_even = Vec::new();
    for projection_bytes in PROJECTION_SLOTS {
        for fanout in TAG_FANOUTS {
            let scenario = benchmark_scenario(profile, projection_bytes, fanout)?;
            for server_count in SERVER_COUNTS {
                let inline = topology(&scenario.inline, server_count)?;
                let two_stage = topology(&scenario.private_two_stage, server_count)?;
                break_even.push(BreakEven {
                    projection_slot_bytes: projection_bytes,
                    tag_fanout: fanout,
                    server_count,
                    private_two_stage_over_inline_expected_server_work:
                        two_stage.expected_logical_xor_bytes_all_servers as f64
                            / inline.expected_logical_xor_bytes_all_servers as f64,
                    private_two_stage_over_inline_upload:
                        two_stage.total_client_upload_bytes as f64
                            / inline.total_client_upload_bytes as f64,
                    private_two_stage_over_inline_download:
                        two_stage.total_client_download_bytes as f64
                            / inline.total_client_download_bytes as f64,
                    private_two_stage_over_inline_server_storage:
                        scenario.private_two_stage.total_table_bytes_per_server as f64
                            / scenario.inline.total_table_bytes_per_server as f64,
                    measured_server_time_ratio: inline
                        .aggregate_server_p50_ms
                        .zip(two_stage.aggregate_server_p50_ms)
                        .map(|(inline, two_stage)| two_stage / inline),
                    locator_only_excluded_from_ratio_reason: "A locator is not a useful end-to-end private result: a subsequent ordinary CID fetch reveals the selected document.",
                });
            }
            scenarios.push(scenario);
        }
    }

    Ok(EndToEndReport {
        protocol: "end-to-end-private-result-and-fair-decoy-comparison-v2",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: METHODOLOGY,
        workload: EndToEndWorkload {
            document_count: DOCUMENT_COUNT,
            projection_slot_bytes: PROJECTION_SLOTS.to_vec(),
            tag_fanouts: TAG_FANOUTS.to_vec(),
            server_counts: SERVER_COUNTS.to_vec(),
            inline_page_budget_bytes: INLINE_PAGE_BUDGET,
            locator_slot_bytes: LOCATOR_BYTES,
            maximum_locator_values_per_page: MAX_LOCATOR_VALUES_PER_PAGE,
            public_index_candidate_count: PUBLIC_INDEX_CANDIDATES,
            measured_cell_rule: "measure projection=96 B across every fanout plus fanout=1 at 32 and 256 B; all twenty cells retain exact analytical accounting",
            measured_resource_gate: "skip timing when either persistent table exceeds 384 MiB, expected aggregate XOR work exceeds 2 GiB, or upload exceeds 128 MiB; skipped timing is never interpolated",
        },
        scenarios,
        break_even,
        conclusions: vec![
            "For one fixed projection schema, inline MPHF pages minimize total server work: the two-stage design adds the locator scan and repeats the million-row document-table XOR once per match.",
            "Shared row-major private batching reduces document-table ordering passes to one, but it does not erase per-query selector tests and output XORs; aggregate logical XOR work remains proportional to the padded result count.",
            "The two-stage layout remains useful when projections cannot be fixed at snapshot build time, when several indexes share one document table, or when storage/update modularity is worth higher online work.",
            "The fair 100-decoy public-index baseline can be much faster because it performs ordinary point lookups, but it reveals the exact candidate set to one server and is never a privacy-equivalent speedup over exact PIR.",
        ],
        production_caveats: vec![
            "This benchmark has no HTTP, TLS, serialization, or WAN latency. Co-located wall time is not a network latency prediction.",
            "PtrHash metadata is generation-specific, key-dependent public metadata. Clients must authenticate it before parsing and must bind it to both tag pages and the document-table generation.",
            "The padded policy hides the selected tag's online result count, not dataset-wide dimensions or set relations potentially exposed by public MPHF metadata. That leakage must be reviewed separately.",
            "The POC uses semi-honest XOR shares and 128-bit content fingerprints. Fingerprints detect wrong rows and absent tags but are not a malicious-server proof or MAC.",
            "The document stage uses the shared row-major Dense batch evaluator. Its one table-ordering pass is reported separately from cryptographic request count; physical cache/DRAM bytes still require hardware counters.",
            "Ordinals are stable only within one immutable generation. Tombstones must remain explicit fixed-size rows until the next authenticated compaction so locator ordinals cannot silently retarget another document.",
            "Production must publish and authenticate the cardinality policy in the manifest, require clients to issue the full fixed page/document schedule for present, lower-fanout, and absent tags, and reject result counts above the class instead of silently enlarging a request.",
            "A normal public CID fetch after locator PIR is deliberately excluded: it leaks the selected CID and is not an end-to-end private result.",
            "The decoy baseline fixes request count, page schedule, and response size within one public cardinality class, but cache state, tag popularity, correlated requests, and biased decoy selection can still identify the target. A production decoy sampler and longitudinal attack evaluation are outside this POC.",
        ],
    })
}

fn benchmark_scenario(
    profile: Profile,
    projection_bytes: usize,
    requested_fanout: usize,
) -> Result<EndToEndScenario> {
    let inline_capacity = padded_inline_capacity(requested_fanout, projection_bytes)?;
    let inline_dims = dimensions(projection_bytes, requested_fanout, inline_capacity)?;
    let locator_capacity = requested_fanout
        .next_power_of_two()
        .min(MAX_LOCATOR_VALUES_PER_PAGE);
    let locator_dims = dimensions(LOCATOR_BYTES, requested_fanout, locator_capacity)?;
    let selected_cell =
        projection_bytes == 96 || (requested_fanout == 1 && projection_bytes <= 256);
    let layout_gate = selected_cell
        && inline_dims.table_bytes <= MAX_MEASURED_TABLE_BYTES
        && DOCUMENT_COUNT
            .checked_mul(projection_bytes)
            .context("document table size overflow")?
            <= MAX_MEASURED_TABLE_BYTES;

    let (inline, decoy, two_stage, locator_only) = if layout_gate {
        benchmark_built_layouts(
            profile,
            projection_bytes,
            requested_fanout,
            inline_dims,
            locator_dims,
        )?
    } else {
        analytical_layouts(
            profile,
            projection_bytes,
            requested_fanout,
            inline_dims,
            locator_dims,
            if selected_cell {
                "layout exceeded the quick-run 384-MiB persistent-table build gate"
            } else {
                "outside the deliberately measured cross-section; exact deterministic accounting is retained"
            },
        )?
    };

    Ok(EndToEndScenario {
        projection_slot_bytes: projection_bytes,
        requested_tag_fanout: requested_fanout,
        target_tag_result_count: inline_dims.target_values,
        padded_result_count: inline_dims.padded_values,
        distinct_tag_count: inline_dims.distinct_tags,
        inline,
        public_index_100_decoy: decoy,
        private_two_stage: two_stage,
        locator_only_diagnostic: locator_only,
    })
}

fn benchmark_built_layouts(
    profile: Profile,
    projection_bytes: usize,
    requested_fanout: usize,
    inline_dims: LayoutDimensions,
    locator_dims: LayoutDimensions,
) -> Result<(
    ApproachResult,
    PublicIndexDecoyResult,
    ApproachResult,
    ApproachResult,
)> {
    let inline_started = Instant::now();
    let inline_pages = build_page_set(
        projection_bytes,
        requested_fanout,
        inline_dims.page_capacity,
        PageValue::Projection,
    )?;
    let inline_config = page_config(projection_bytes, inline_dims.page_capacity);
    let inline_snapshot = MphfPageSnapshot::from_page_set(&inline_pages, inline_config)?;
    drop(inline_pages);
    let inline_build_ms = millis(inline_started.elapsed());
    validate_absent(&inline_snapshot)?;
    let inline_metadata = inline_snapshot.manifest.client_metadata_bytes();
    let inline_generation = inline_snapshot.manifest.generation_hex();
    let inline_peak = inline_snapshot.build_metrics.peak_tracked_bytes;
    let mut inline_topologies = Vec::new();
    for servers in SERVER_COUNTS {
        let shape = inline_shape(inline_dims, servers)?;
        let measure = timing_allowed(&shape, servers);
        let samples = measure.then(|| {
            benchmark_inline(
                profile,
                &inline_snapshot,
                projection_bytes,
                inline_dims.target_values,
                inline_dims.padded_values,
                servers,
            )
        });
        inline_topologies.push(make_topology(
            "mphf-inline-projection-shared-row-major-dense-xor",
            "all fixed-width private projection slots for one tag, with declared cardinality padding",
            servers,
            shape,
            samples.transpose()?,
            (!measure).then_some("expected work or upload exceeded the quick-run timing gate"),
            inline_dims.table_bytes,
            inline_metadata,
            Some(inline_build_ms),
            Some(inline_peak),
        )?);
    }
    let inline = ApproachResult {
        approach: "mphf-inline-projection",
        result_scope: "complete private fixed-width projection set",
        measured_layout: true,
        layout_measurement_skip_reason: None,
        page_value_capacity: inline_dims.page_capacity,
        requested_continuation_pages: inline_dims.target_pages,
        primary_table_rows: inline_dims.pages,
        primary_row_bytes: inline_dims.page_size,
        primary_table_bytes_per_server: inline_dims.table_bytes,
        secondary_table_rows: 0,
        secondary_row_bytes: 0,
        secondary_table_bytes_per_server: 0,
        total_table_bytes_per_server: inline_dims.table_bytes,
        public_client_metadata_bytes: Some(inline_metadata),
        build_wall_ms: Some(inline_build_ms),
        peak_tracked_build_bytes: Some(inline_peak),
        generation_binding: Some(inline_generation.clone()),
        topologies: inline_topologies,
    };
    let decoy = make_built_decoy_result(
        profile,
        &inline_snapshot,
        projection_bytes,
        inline_dims,
        inline_metadata,
        inline_generation.clone(),
        inline_build_ms,
        inline_peak,
    )?;
    drop(inline_snapshot);

    let two_started = Instant::now();
    let locator_pages = build_page_set(
        projection_bytes,
        requested_fanout,
        locator_dims.page_capacity,
        PageValue::Locator,
    )?;
    let locator_snapshot = MphfPageSnapshot::from_page_set(
        &locator_pages,
        page_config(LOCATOR_BYTES, locator_dims.page_capacity),
    )?;
    drop(locator_pages);
    validate_absent(&locator_snapshot)?;
    let documents = build_document_rows(projection_bytes)?;
    let document_generation = blake3::hash(&documents);
    let mut composition = blake3::Hasher::new();
    composition.update(COMPOSITION_DOMAIN);
    composition.update(&locator_snapshot.manifest.generation);
    composition.update(document_generation.as_bytes());
    composition.update(&(projection_bytes as u64).to_le_bytes());
    let composition_generation = composition.finalize().to_hex().to_string();
    let two_build_ms = millis(two_started.elapsed());
    let two_peak = locator_snapshot
        .build_metrics
        .peak_tracked_bytes
        .checked_add(documents.len())
        .context("two-stage peak tracking overflow")?;
    let metadata = locator_snapshot.manifest.client_metadata_bytes() + 32;
    let mut two_topologies = Vec::new();
    let mut locator_topologies = Vec::new();
    for servers in SERVER_COUNTS {
        let locator_shape = locator_shape(locator_dims, servers)?;
        let two_shape = two_stage_shape(locator_dims, projection_bytes, servers)?;
        let measure_locator = timing_allowed(&locator_shape, servers);
        let measure_two = timing_allowed(&two_shape, servers);
        let (two_samples, locator_samples) = if measure_two {
            let (two, locator) = benchmark_two_stage(
                profile,
                &locator_snapshot,
                &documents,
                projection_bytes,
                locator_dims.target_values,
                locator_dims.padded_values,
                servers,
            )?;
            (Some(two), Some(locator))
        } else if measure_locator {
            (
                None,
                Some(benchmark_locator_only(
                    profile,
                    &locator_snapshot,
                    projection_bytes,
                    locator_dims.target_values,
                    locator_dims.padded_values,
                    servers,
                )?),
            )
        } else {
            (None, None)
        };
        two_topologies.push(make_topology(
            "mphf-locator-plus-private-shared-row-major-document-dense-xor",
            "all fixed-width private projection slots for one tag, with declared cardinality padding",
            servers,
            two_shape,
            two_samples,
            (!measure_two).then_some("expected work or upload exceeded the quick-run timing gate"),
            locator_dims.table_bytes + documents.len(),
            metadata,
            Some(two_build_ms),
            Some(two_peak),
        )?);
        locator_topologies.push(make_topology(
            "mphf-locator-only-diagnostic",
            "opaque locator/ordinal page only; NOT an end-to-end private document result",
            servers,
            locator_shape,
            locator_samples,
            (!measure_locator)
                .then_some("expected work or upload exceeded the quick-run timing gate"),
            locator_dims.table_bytes,
            locator_snapshot.manifest.client_metadata_bytes(),
            Some(two_build_ms),
            Some(locator_snapshot.build_metrics.peak_tracked_bytes),
        )?);
    }
    let two_stage = ApproachResult {
        approach: "mphf-locator-plus-private-batched-document-retrieval",
        result_scope: "complete private fixed-width projection set",
        measured_layout: true,
        layout_measurement_skip_reason: None,
        page_value_capacity: locator_dims.page_capacity,
        requested_continuation_pages: locator_dims.target_pages,
        primary_table_rows: locator_dims.pages,
        primary_row_bytes: locator_dims.page_size,
        primary_table_bytes_per_server: locator_dims.table_bytes,
        secondary_table_rows: DOCUMENT_COUNT,
        secondary_row_bytes: projection_bytes,
        secondary_table_bytes_per_server: documents.len(),
        total_table_bytes_per_server: locator_dims.table_bytes + documents.len(),
        public_client_metadata_bytes: Some(metadata),
        build_wall_ms: Some(two_build_ms),
        peak_tracked_build_bytes: Some(two_peak),
        generation_binding: Some(composition_generation),
        topologies: two_topologies,
    };
    let locator_only = ApproachResult {
        approach: "mphf-locator-only-diagnostic",
        result_scope: "locator bytes only; deliberately not compared as a useful private result",
        measured_layout: true,
        layout_measurement_skip_reason: None,
        page_value_capacity: locator_dims.page_capacity,
        requested_continuation_pages: locator_dims.target_pages,
        primary_table_rows: locator_dims.pages,
        primary_row_bytes: locator_dims.page_size,
        primary_table_bytes_per_server: locator_dims.table_bytes,
        secondary_table_rows: 0,
        secondary_row_bytes: 0,
        secondary_table_bytes_per_server: 0,
        total_table_bytes_per_server: locator_dims.table_bytes,
        public_client_metadata_bytes: Some(locator_snapshot.manifest.client_metadata_bytes()),
        build_wall_ms: Some(two_build_ms),
        peak_tracked_build_bytes: Some(locator_snapshot.build_metrics.peak_tracked_bytes),
        generation_binding: Some(locator_snapshot.manifest.generation_hex()),
        topologies: locator_topologies,
    };
    Ok((inline, decoy, two_stage, locator_only))
}

fn analytical_layouts(
    _profile: Profile,
    projection_bytes: usize,
    _requested_fanout: usize,
    inline_dims: LayoutDimensions,
    locator_dims: LayoutDimensions,
    skip_reason: &'static str,
) -> Result<(
    ApproachResult,
    PublicIndexDecoyResult,
    ApproachResult,
    ApproachResult,
)> {
    let document_bytes = DOCUMENT_COUNT
        .checked_mul(projection_bytes)
        .context("document table size overflow")?;
    let inline_topologies = SERVER_COUNTS
        .into_iter()
        .map(|servers| {
            make_topology(
                "mphf-inline-projection-shared-row-major-dense-xor",
                "all fixed-width private projection slots for one tag, with declared cardinality padding",
                servers,
                inline_shape(inline_dims, servers)?,
                None,
                Some(skip_reason),
                inline_dims.table_bytes,
                0,
                None,
                None,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let two_topologies = SERVER_COUNTS
        .into_iter()
        .map(|servers| {
            make_topology(
                "mphf-locator-plus-private-shared-row-major-document-dense-xor",
                "all fixed-width private projection slots for one tag, with declared cardinality padding",
                servers,
                two_stage_shape(locator_dims, projection_bytes, servers)?,
                None,
                Some(skip_reason),
                locator_dims.table_bytes + document_bytes,
                0,
                None,
                None,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let locator_topologies = SERVER_COUNTS
        .into_iter()
        .map(|servers| {
            make_topology(
                "mphf-locator-only-diagnostic",
                "opaque locator/ordinal page only; NOT an end-to-end private document result",
                servers,
                locator_shape(locator_dims, servers)?,
                None,
                Some(skip_reason),
                locator_dims.table_bytes,
                0,
                None,
                None,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let decoy = make_analytical_decoy_result(projection_bytes, inline_dims, skip_reason)?;

    Ok((
        ApproachResult {
            approach: "mphf-inline-projection",
            result_scope: "complete private fixed-width projection set",
            measured_layout: false,
            layout_measurement_skip_reason: Some(skip_reason),
            page_value_capacity: inline_dims.page_capacity,
            requested_continuation_pages: inline_dims.target_pages,
            primary_table_rows: inline_dims.pages,
            primary_row_bytes: inline_dims.page_size,
            primary_table_bytes_per_server: inline_dims.table_bytes,
            secondary_table_rows: 0,
            secondary_row_bytes: 0,
            secondary_table_bytes_per_server: 0,
            total_table_bytes_per_server: inline_dims.table_bytes,
            public_client_metadata_bytes: None,
            build_wall_ms: None,
            peak_tracked_build_bytes: None,
            generation_binding: None,
            topologies: inline_topologies,
        },
        decoy,
        ApproachResult {
            approach: "mphf-locator-plus-private-batched-document-retrieval",
            result_scope: "complete private fixed-width projection set",
            measured_layout: false,
            layout_measurement_skip_reason: Some(skip_reason),
            page_value_capacity: locator_dims.page_capacity,
            requested_continuation_pages: locator_dims.target_pages,
            primary_table_rows: locator_dims.pages,
            primary_row_bytes: locator_dims.page_size,
            primary_table_bytes_per_server: locator_dims.table_bytes,
            secondary_table_rows: DOCUMENT_COUNT,
            secondary_row_bytes: projection_bytes,
            secondary_table_bytes_per_server: document_bytes,
            total_table_bytes_per_server: locator_dims.table_bytes + document_bytes,
            public_client_metadata_bytes: None,
            build_wall_ms: None,
            peak_tracked_build_bytes: None,
            generation_binding: None,
            topologies: two_topologies,
        },
        ApproachResult {
            approach: "mphf-locator-only-diagnostic",
            result_scope:
                "locator bytes only; deliberately not compared as a useful private result",
            measured_layout: false,
            layout_measurement_skip_reason: Some(skip_reason),
            page_value_capacity: locator_dims.page_capacity,
            requested_continuation_pages: locator_dims.target_pages,
            primary_table_rows: locator_dims.pages,
            primary_row_bytes: locator_dims.page_size,
            primary_table_bytes_per_server: locator_dims.table_bytes,
            secondary_table_rows: 0,
            secondary_row_bytes: 0,
            secondary_table_bytes_per_server: 0,
            total_table_bytes_per_server: locator_dims.table_bytes,
            public_client_metadata_bytes: None,
            build_wall_ms: None,
            peak_tracked_build_bytes: None,
            generation_binding: None,
            topologies: locator_topologies,
        },
    ))
}

#[derive(Clone, Copy)]
struct DecoyShape {
    ordinary_mphf_row_lookups: usize,
    raw_visible_tag_upload_bytes: usize,
    fixed_padded_page_download_bytes: usize,
    useful_target_projection_bytes: usize,
    deterministic_server_row_bytes: usize,
}

#[allow(clippy::too_many_arguments)]
fn make_built_decoy_result(
    profile: Profile,
    snapshot: &MphfPageSnapshot,
    projection_bytes: usize,
    dimensions: LayoutDimensions,
    server_index_bytes: usize,
    generation: String,
    build_wall_ms: f64,
    peak_build_bytes: usize,
) -> Result<PublicIndexDecoyResult> {
    let shape = decoy_shape(dimensions, projection_bytes)?;
    let measure = decoy_timing_allowed(&shape);
    let samples = if measure {
        Some(benchmark_public_index_decoys(
            profile,
            snapshot,
            projection_bytes,
            dimensions.target_values,
            dimensions.target_pages,
        )?)
    } else {
        None
    };
    let topology = make_decoy_topology(
        shape,
        samples,
        (!measure).then_some("decoy row bytes exceeded the declared quick-run timing gate"),
        dimensions.table_bytes,
        server_index_bytes,
        Some(build_wall_ms),
        Some(peak_build_bytes),
    )?;
    Ok(decoy_result(
        dimensions,
        Some(server_index_bytes),
        Some(generation),
        topology,
    ))
}

fn make_analytical_decoy_result(
    projection_bytes: usize,
    dimensions: LayoutDimensions,
    skip_reason: &'static str,
) -> Result<PublicIndexDecoyResult> {
    let shape = decoy_shape(dimensions, projection_bytes)?;
    let topology = make_decoy_topology(
        shape,
        None,
        Some(skip_reason),
        dimensions.table_bytes,
        0,
        None,
        None,
    )?;
    Ok(decoy_result(dimensions, None, None, topology))
}

fn decoy_result(
    dimensions: LayoutDimensions,
    server_index_bytes: Option<usize>,
    generation: Option<String>,
    topology: PublicIndexDecoyTopology,
) -> PublicIndexDecoyResult {
    PublicIndexDecoyResult {
        approach: "ordinary-mphf-public-index-100-visible-candidates",
        result_scope: "the complete padded inline projection pages for one target and 99 decoy tags",
        privacy_scope: "weaker 1-in-100 candidate-set privacy against one server; all candidate tags are visible",
        cardinality_leakage: "the public padded fanout class and all 100 candidate identities are visible; every candidate follows the same fixed page schedule",
        timing_leakage: "request count and response bytes are fixed within the class, but ordinary point-lookup cache and popularity timing are not made oblivious",
        privacy_equivalence_warning: "performance ratios against exact PIR are descriptive only; this candidate-set scheme is not a privacy-equivalent acceleration",
        target_candidates: 1,
        decoy_candidates: PUBLIC_INDEX_CANDIDATES - 1,
        padded_result_count_per_candidate: dimensions.padded_values,
        requested_pages_per_candidate: dimensions.target_pages,
        ordinary_mphf_row_lookups: topology.ordinary_mphf_row_lookups,
        table_rows: dimensions.pages,
        row_bytes: dimensions.page_size,
        table_bytes: dimensions.table_bytes,
        server_index_bytes,
        reused_inline_generation: generation,
        topology,
    }
}

fn decoy_shape(dimensions: LayoutDimensions, projection_bytes: usize) -> Result<DecoyShape> {
    let lookups = PUBLIC_INDEX_CANDIDATES
        .checked_mul(dimensions.target_pages)
        .context("decoy lookup count overflow")?;
    let response_bytes = lookups
        .checked_mul(dimensions.page_size)
        .context("decoy response bytes overflow")?;
    Ok(DecoyShape {
        ordinary_mphf_row_lookups: lookups,
        raw_visible_tag_upload_bytes: PUBLIC_INDEX_CANDIDATES
            .checked_mul(size_of_benchmark_tag())
            .context("decoy upload bytes overflow")?,
        fixed_padded_page_download_bytes: response_bytes,
        useful_target_projection_bytes: dimensions
            .target_values
            .checked_mul(projection_bytes)
            .context("decoy useful bytes overflow")?,
        deterministic_server_row_bytes: response_bytes,
    })
}

fn size_of_benchmark_tag() -> usize {
    benchmark_tag(0).len()
}

fn decoy_timing_allowed(shape: &DecoyShape) -> bool {
    shape.deterministic_server_row_bytes <= MAX_MEASURED_AGGREGATE_LOGICAL_BYTES
        && shape.raw_visible_tag_upload_bytes <= MAX_MEASURED_UPLOAD_BYTES
}

fn make_decoy_topology(
    shape: DecoyShape,
    samples: Option<Samples>,
    skip_reason: Option<&'static str>,
    table_bytes: usize,
    server_index_bytes: usize,
    build_wall_ms: Option<f64>,
    peak_build_bytes: Option<usize>,
) -> Result<PublicIndexDecoyTopology> {
    let summary = samples.map(Samples::summarize).transpose()?;
    let work = decoy_accounting(
        shape,
        summary.as_ref(),
        table_bytes,
        server_index_bytes,
        build_wall_ms,
        peak_build_bytes,
    )?;
    Ok(PublicIndexDecoyTopology {
        aggregate_work: work,
        server_count: 1,
        measured: summary.is_some(),
        measurement_skip_reason: summary.is_none().then_some(
            skip_reason.unwrap_or("decoy timing was not selected for this analytical sweep cell"),
        ),
        ordinary_mphf_row_lookups: shape.ordinary_mphf_row_lookups,
        network_rounds: 1,
        raw_visible_tag_upload_bytes: shape.raw_visible_tag_upload_bytes,
        fixed_padded_page_download_bytes: shape.fixed_padded_page_download_bytes,
        useful_target_projection_bytes: shape.useful_target_projection_bytes,
        deterministic_server_row_bytes: shape.deterministic_server_row_bytes,
        server_p50_ms: summary
            .as_ref()
            .map(|summary| millis(summary.aggregate_server_p50)),
        server_p95_ms: summary
            .as_ref()
            .map(|summary| millis(summary.aggregate_server_p95)),
        client_decode_p50_ms: summary.as_ref().map(|summary| millis(summary.client_p50)),
    })
}

#[allow(clippy::too_many_arguments)]
fn decoy_accounting(
    shape: DecoyShape,
    samples: Option<&SampleSummary>,
    table_bytes: usize,
    server_index_bytes: usize,
    build_wall_ms: Option<f64>,
    peak_build_bytes: Option<usize>,
) -> Result<AggregateWorkReport> {
    let mut work = AggregateWorkReport::new(
        "ordinary-mphf-public-index-100-visible-candidates",
        ComparisonScope {
            workload: "one target plus 99 decoy tag lookups over the identical inline table and padded-cardinality schedule",
            result: "all complete fixed-width padded pages for every visible candidate; one candidate is useful",
            public_partition: "global immutable snapshot and public padded fanout class",
            leakage: LeakageScope::CandidateSet {
                candidates: PUBLIC_INDEX_CANDIDATES,
            },
        },
        SecurityLabels {
            privacy: "weaker candidate-set privacy only: the server sees all 100 exact candidate tags",
            server_count: 1,
            collusion_tolerance: 0,
            required_answers: 1,
            assumptions: "a production candidate sampler must make the user's target indistinguishable from the 99 decoys under longitudinal traffic analysis; this deterministic corpus is not such a sampler",
            availability: "the one public-index server must answer",
            integrity: "128-bit page fingerprints detect absent/wrong rows; no malicious-server proof or MAC",
        },
    );
    work.global_build.aggregate_server_time_ms = build_wall_ms.map_or_else(
        || Metric::not_measured("inline layout was not built for this analytical cell"),
        |value| {
            Metric::estimated(
                value,
                "the exact inline table build is reused; single-builder wall time is not aggregate CPU",
            )
        },
    );
    work.global_build.client_time_ms = Metric::not_applicable("immutable build is server-side");
    work.global_build.logical_selected_bytes =
        Metric::not_measured("builder bytes were not instrumented as online row reads");
    work.global_build.physical_or_scanned_bytes =
        Metric::not_measured("build hardware bytes were not collected");
    work.global_build.peak_server_ram_bytes = peak_build_bytes.map_or_else(
        || Metric::not_measured("inline layout was not built for this analytical cell"),
        |value| {
            Metric::estimated(
                value,
                "reused inline builder-owned peak; allocator and PtrHash transient workspace excluded",
            )
        },
    );
    work.global_build.peak_client_ram_bytes = Metric::not_applicable("build is server-side");
    work.global_build.client_upload_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.client_download_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.server_scans = Metric::not_measured("builder passes were not collected");
    work.global_build.network_rounds = Metric::not_applicable("build is server-local");
    work.per_client_setup = PhaseWork::not_applicable(
        "client setup",
        "the server performs ordinary MPHF lookup; the client keeps no public MPHF state",
    );
    work.maintenance = PhaseWork::not_applicable(
        "immutable snapshot lifetime",
        "changes publish a new authenticated inline generation",
    );
    let server = work
        .online
        .per_server
        .first_mut()
        .context("decoy accounting has no server")?;
    server.server_time_p50_ms = samples.map_or_else(
        || Metric::not_measured("decoy timing was skipped by the resource gate"),
        |sample| {
            Metric::measured(
                millis(sample.aggregate_server_p50),
                "single-thread sequential MPHF lookup and fixed-row-copy elapsed time",
            )
        },
    );
    server.logical_selected_bytes = Metric::deterministic(
        shape.deterministic_server_row_bytes,
        "one fixed-width inline row returned for every candidate continuation page",
    );
    server.physical_or_scanned_bytes =
        Metric::not_measured("MPHF/cache/DRAM physical bytes were not collected");
    server.scans = Metric::deterministic(0, "ordinary MPHF point lookups perform no Dense scan");
    work.online.unit = "one complete fixed-schedule 100-candidate lookup";
    work.online.aggregate_server_time_p50_ms = samples.map_or_else(
        || Metric::not_measured("decoy timing was skipped by the resource gate"),
        |sample| {
            Metric::measured(
                millis(sample.aggregate_server_p50),
                "one single-thread public-index server",
            )
        },
    );
    work.online.max_server_time_p50_ms = samples.map_or_else(
        || Metric::not_measured("decoy timing was skipped by the resource gate"),
        |sample| {
            Metric::measured(
                millis(sample.wall_p50),
                "one server; wall equals server elapsed",
            )
        },
    );
    work.online.aggregate_logical_selected_bytes = Metric::deterministic(
        shape.deterministic_server_row_bytes,
        "sum of all fixed page rows copied for the 100 visible candidates",
    );
    work.online.aggregate_physical_or_scanned_bytes =
        Metric::not_measured("hardware counters were not collected");
    work.online.server_scans = Metric::deterministic(0, "ordinary point lookups, not a table scan");
    work.online.network_rounds =
        Metric::deterministic(1, "all visible tags are submitted together");
    work.online.useful_result_bytes = Metric::deterministic(
        shape.useful_target_projection_bytes,
        "only the target candidate's actual projections are useful; all decoy pages are returned",
    );
    work.client.online_cpu_p50_ms = samples.map_or_else(
        || Metric::not_measured("decoy client decode timing was skipped"),
        |sample| {
            Metric::measured(
                millis(sample.client_p50),
                "candidate request materialization plus all page fingerprint/projection checks",
            )
        },
    );
    work.client.peak_transient_ram_bytes = Metric::deterministic(
        shape
            .raw_visible_tag_upload_bytes
            .checked_add(shape.fixed_padded_page_download_bytes)
            .context("decoy transient bytes overflow")?,
        "raw tag and fixed response buffers; transport/allocator framing excluded",
    );
    work.client.persistent_state_bytes = Metric::deterministic(0, "no client MPHF state");
    work.client.upload_bytes = Metric::deterministic(
        shape.raw_visible_tag_upload_bytes,
        "100 visible eight-byte synthetic tags; transport framing excluded",
    );
    work.client.download_bytes = Metric::deterministic(
        shape.fixed_padded_page_download_bytes,
        "all complete fixed-size page rows for all candidates",
    );
    let server_storage = table_bytes
        .checked_add(server_index_bytes)
        .context("decoy server storage overflow")?;
    work.persisted_storage.server_bytes_per_server = if server_index_bytes == 0 {
        Metric::estimated(
            table_bytes,
            "inline table only; unbuilt MPHF bytes unavailable",
        )
    } else {
        Metric::deterministic(
            server_storage,
            "inline table plus exact server MPHF artifact",
        )
    };
    work.persisted_storage.aggregate_server_bytes = if server_index_bytes == 0 {
        Metric::estimated(
            table_bytes,
            "one inline table; unbuilt MPHF bytes unavailable",
        )
    } else {
        Metric::deterministic(server_storage, "one public-index server")
    };
    work.persisted_storage.client_bytes =
        Metric::deterministic(0, "server computes the MPHF lookup");
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    Ok(work)
}

#[derive(Clone, Copy)]
struct TopologyShape {
    requests_per_server: usize,
    table_passes_per_server: usize,
    network_rounds: usize,
    query_bytes_per_server: usize,
    response_bytes_per_server: usize,
    useful_result_bytes: usize,
    expected_logical_bytes_per_server: usize,
}

fn inline_shape(dimensions: LayoutDimensions, servers: usize) -> Result<TopologyShape> {
    let query_bytes_per_server = dimensions
        .target_pages
        .checked_mul(dense::query_size(dimensions.pages))
        .context("inline query bytes overflow")?;
    let response_bytes_per_server = dimensions
        .target_pages
        .checked_mul(dimensions.page_size)
        .context("inline response bytes overflow")?;
    let expected_logical_bytes_per_server = dimensions
        .pages
        .div_ceil(2)
        .checked_mul(dimensions.page_size)
        .and_then(|bytes| bytes.checked_mul(dimensions.target_pages))
        .context("inline logical work overflow")?;
    let _ = servers;
    Ok(TopologyShape {
        requests_per_server: dimensions.target_pages,
        table_passes_per_server: 1,
        network_rounds: 1,
        query_bytes_per_server,
        response_bytes_per_server,
        useful_result_bytes: dimensions.target_values
            * ((dimensions.page_size - PAGE_HEADER_BYTES) / dimensions.page_capacity
                - VALUE_LENGTH_BYTES),
        expected_logical_bytes_per_server,
    })
}

fn locator_shape(dimensions: LayoutDimensions, _servers: usize) -> Result<TopologyShape> {
    Ok(TopologyShape {
        requests_per_server: dimensions.target_pages,
        table_passes_per_server: 1,
        network_rounds: 1,
        query_bytes_per_server: dimensions
            .target_pages
            .checked_mul(dense::query_size(dimensions.pages))
            .context("locator query bytes overflow")?,
        response_bytes_per_server: dimensions
            .target_pages
            .checked_mul(dimensions.page_size)
            .context("locator response bytes overflow")?,
        useful_result_bytes: dimensions.target_values * LOCATOR_BYTES,
        expected_logical_bytes_per_server: dimensions
            .pages
            .div_ceil(2)
            .checked_mul(dimensions.page_size)
            .and_then(|bytes| bytes.checked_mul(dimensions.target_pages))
            .context("locator logical work overflow")?,
    })
}

fn two_stage_shape(
    locator_dimensions: LayoutDimensions,
    projection_bytes: usize,
    _servers: usize,
) -> Result<TopologyShape> {
    let locator = locator_shape(locator_dimensions, 2)?;
    let document_query_bytes = locator_dimensions
        .padded_values
        .checked_mul(dense::query_size(DOCUMENT_COUNT))
        .context("document query bytes overflow")?;
    let document_response_bytes = locator_dimensions
        .padded_values
        .checked_mul(projection_bytes)
        .context("document response bytes overflow")?;
    let document_logical = DOCUMENT_COUNT
        .div_ceil(2)
        .checked_mul(projection_bytes)
        .and_then(|bytes| bytes.checked_mul(locator_dimensions.padded_values))
        .context("document logical work overflow")?;
    Ok(TopologyShape {
        requests_per_server: locator.requests_per_server + locator_dimensions.padded_values,
        table_passes_per_server: locator.table_passes_per_server + 1,
        network_rounds: 2,
        query_bytes_per_server: locator.query_bytes_per_server + document_query_bytes,
        response_bytes_per_server: locator.response_bytes_per_server + document_response_bytes,
        useful_result_bytes: locator_dimensions.target_values * projection_bytes,
        expected_logical_bytes_per_server: locator.expected_logical_bytes_per_server
            + document_logical,
    })
}

fn timing_allowed(shape: &TopologyShape, server_count: usize) -> bool {
    shape.expected_logical_bytes_per_server * server_count <= MAX_MEASURED_AGGREGATE_LOGICAL_BYTES
        && shape.query_bytes_per_server * server_count <= MAX_MEASURED_UPLOAD_BYTES
}

#[allow(clippy::too_many_arguments)]
fn make_topology(
    protocol: &'static str,
    result_scope: &'static str,
    server_count: usize,
    shape: TopologyShape,
    samples: Option<Samples>,
    skip_reason: Option<&'static str>,
    table_bytes_per_server: usize,
    client_metadata_bytes: usize,
    build_wall_ms: Option<f64>,
    peak_build_bytes: Option<usize>,
) -> Result<EndToEndTopology> {
    let expected_all = shape
        .expected_logical_bytes_per_server
        .checked_mul(server_count)
        .context("aggregate expected work overflow")?;
    let upload = shape
        .query_bytes_per_server
        .checked_mul(server_count)
        .context("aggregate upload overflow")?;
    let download = shape
        .response_bytes_per_server
        .checked_mul(server_count)
        .context("aggregate download overflow")?;
    let summaries = samples.map(Samples::summarize).transpose()?;
    let work = accounting(
        protocol,
        result_scope,
        server_count,
        shape,
        summaries.as_ref(),
        table_bytes_per_server,
        client_metadata_bytes,
        build_wall_ms,
        peak_build_bytes,
    )?;
    Ok(EndToEndTopology {
        aggregate_work: work,
        server_count,
        measured: summaries.is_some(),
        measurement_skip_reason: summaries.is_none().then_some(
            skip_reason.unwrap_or("timing was not selected for this exact analytical sweep cell"),
        ),
        dense_requests_per_server: shape.requests_per_server,
        table_passes_per_server: shape.table_passes_per_server,
        network_rounds: shape.network_rounds,
        query_bytes_per_server: shape.query_bytes_per_server,
        total_client_upload_bytes: upload,
        response_bytes_per_server: shape.response_bytes_per_server,
        total_client_download_bytes: download,
        useful_result_bytes: shape.useful_result_bytes,
        expected_logical_xor_bytes_all_servers: expected_all,
        actual_logical_xor_bytes_p50_all_servers: summaries
            .as_ref()
            .map(|summary| summary.actual_logical_bytes_p50),
        aggregate_server_p50_ms: summaries
            .as_ref()
            .map(|summary| millis(summary.aggregate_server_p50)),
        aggregate_server_p95_ms: summaries
            .as_ref()
            .map(|summary| millis(summary.aggregate_server_p95)),
        co_located_wall_p50_ms: summaries.as_ref().map(|summary| millis(summary.wall_p50)),
        co_located_wall_p95_ms: summaries.as_ref().map(|summary| millis(summary.wall_p95)),
        client_combine_decode_p50_ms: summaries.as_ref().map(|summary| millis(summary.client_p50)),
    })
}

struct SampleSummary {
    aggregate_server_p50: Duration,
    aggregate_server_p95: Duration,
    wall_p50: Duration,
    wall_p95: Duration,
    client_p50: Duration,
    actual_logical_bytes_p50: usize,
}

impl Samples {
    fn summarize(mut self) -> Result<SampleSummary> {
        if self.aggregate_server.is_empty()
            || self.wall.is_empty()
            || self.client.is_empty()
            || self.actual_logical_bytes.is_empty()
        {
            bail!("measured sample set is empty");
        }
        self.aggregate_server.sort_unstable();
        self.wall.sort_unstable();
        self.client.sort_unstable();
        self.actual_logical_bytes.sort_unstable();
        Ok(SampleSummary {
            aggregate_server_p50: percentile(&self.aggregate_server, 50),
            aggregate_server_p95: percentile(&self.aggregate_server, 95),
            wall_p50: percentile(&self.wall, 50),
            wall_p95: percentile(&self.wall, 95),
            client_p50: percentile(&self.client, 50),
            actual_logical_bytes_p50: percentile_usize(&self.actual_logical_bytes, 50),
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn accounting(
    protocol: &'static str,
    result_scope: &'static str,
    server_count: usize,
    shape: TopologyShape,
    samples: Option<&SampleSummary>,
    table_bytes_per_server: usize,
    client_metadata_bytes: usize,
    build_wall_ms: Option<f64>,
    peak_build_bytes: Option<usize>,
) -> Result<AggregateWorkReport> {
    let mut work = AggregateWorkReport::new(
        protocol,
        ComparisonScope {
            workload: "one tag lookup over the same 1,048,576-document immutable corpus and declared padded-cardinality policy",
            result: result_scope,
            public_partition: "global immutable snapshot; no public time window or collection subpartition",
            leakage: LeakageScope::ExactQueryPrivacy,
        },
        SecurityLabels {
            privacy: "exact information-theoretic query privacy against any n-1 colluding semi-honest replicas",
            server_count,
            collusion_tolerance: server_count - 1,
            required_answers: server_count,
            assumptions: "at least one replica does not collude; all replicas serve the same authenticated tag/document generation; fixed padded request schedule is enforced client-side",
            availability: "all answer shares are required",
            integrity: "128-bit fingerprints verify tag pages and document ordinals; this is not malicious-server integrity",
        },
    );
    work.global_build.aggregate_server_time_ms = build_wall_ms.map_or_else(
        || Metric::not_measured("layout was not built for this analytical sweep cell"),
        |value| {
            Metric::estimated(
                value,
                "measured single-builder wall time; aggregate builder CPU was not instrumented",
            )
        },
    );
    work.global_build.client_time_ms = Metric::not_applicable("immutable build is server-side");
    work.global_build.logical_selected_bytes =
        Metric::not_measured("builder passes are not comparable to online selected-row bytes");
    work.global_build.physical_or_scanned_bytes =
        Metric::not_measured("build physical bytes were not collected");
    work.global_build.peak_server_ram_bytes = peak_build_bytes.map_or_else(
        || Metric::not_measured("layout was not built for this analytical sweep cell"),
        |value| {
            Metric::estimated(
                value,
                "algorithm-owned buffers; allocator, PtrHash transient workspace, code, and stacks excluded",
            )
        },
    );
    work.global_build.peak_client_ram_bytes = Metric::not_applicable("build is server-side");
    work.global_build.client_upload_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.client_download_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.server_scans =
        Metric::not_measured("layout construction passes were not instrumented as online scans");
    work.global_build.network_rounds = Metric::not_applicable("build is server-local");

    let mut setup = PhaseWork::unmeasured(
        "one client loading one immutable generation",
        "metadata parse CPU and peak RAM were not separately sampled",
    );
    setup.client_upload_bytes = Metric::deterministic(0, "public metadata fetch has no PIR upload");
    setup.client_download_bytes = if client_metadata_bytes == 0 {
        Metric::not_measured("PtrHash metadata size is unavailable for an unbuilt analytical cell")
    } else {
        Metric::deterministic(
            client_metadata_bytes,
            "authenticated generation-specific PtrHash artifact and composition digest",
        )
    };
    setup.network_rounds = Metric::estimated(1, "one authenticated metadata fetch assumed");
    work.per_client_setup = setup;
    work.maintenance = PhaseWork::not_applicable(
        "immutable snapshot lifetime",
        "changes publish a new authenticated generation; explicit tombstones remain until compaction",
    );

    for server in &mut work.online.per_server {
        server.server_time_p50_ms = samples.map_or_else(
            || Metric::not_measured("timing was skipped by the declared quick-run gate"),
            |sample| {
                Metric::estimated(
                    millis(sample.aggregate_server_p50) / server_count as f64,
                    "aggregate server median divided evenly; per-server samples were not retained",
                )
            },
        );
        server.logical_selected_bytes = Metric::estimated(
            shape.expected_logical_bytes_per_server,
            "uniform random XOR selector shares select half the table rows in expectation for every charged request",
        );
        server.physical_or_scanned_bytes = Metric::not_measured(
            "logical XOR bytes are not substituted for hardware/cache/DRAM traffic",
        );
        server.scans = Metric::deterministic(
            shape.table_passes_per_server,
            "table-ordering passes; the shared row-major document batch evaluates every padded selector in one pass",
        );
    }
    work.online.unit = "one complete padded-cardinality tag result";
    work.online.aggregate_server_time_p50_ms = samples.map_or_else(
        || Metric::not_measured("timing was skipped by the declared quick-run gate"),
        |sample| {
            Metric::measured(
                millis(sample.aggregate_server_p50),
                "sum of elapsed single-core evaluation time across co-located replicas",
            )
        },
    );
    work.online.max_server_time_p50_ms = samples.map_or_else(
        || Metric::not_measured("timing was skipped by the declared quick-run gate"),
        |sample| {
            Metric::measured(
                millis(sample.wall_p50),
                "co-located wall time; excludes network latency",
            )
        },
    );
    work.online.aggregate_logical_selected_bytes = Metric::estimated(
        shape.expected_logical_bytes_per_server * server_count,
        "sum of expected selected-row XOR bytes over all charged requests and replicas",
    );
    work.online.aggregate_physical_or_scanned_bytes = Metric::not_measured(
        "hardware counters were not collected; shared-scan physical reads are a separate experiment",
    );
    work.online.server_scans = Metric::deterministic(
        shape.table_passes_per_server * server_count,
        "table-ordering passes on every replica; request count is reported separately",
    );
    work.online.network_rounds = Metric::deterministic(
        shape.network_rounds,
        "all shares within a stage are sent in parallel",
    );
    work.online.useful_result_bytes = Metric::deterministic(
        shape.useful_result_bytes,
        "actual useful projection bytes; padding, headers, locators, and dummy results excluded",
    );
    work.client.online_cpu_p50_ms = samples.map_or_else(
        || Metric::not_measured("client generation/combine/decode timing was skipped"),
        |sample| {
            Metric::measured(
                millis(sample.client_p50),
                "share generation plus answer combine, fingerprint, ordinal, and dummy filtering",
            )
        },
    );
    work.client.peak_transient_ram_bytes = Metric::not_measured(
        "query/answer buffers are deterministic in size but allocator peak was not sampled",
    );
    work.client.persistent_state_bytes = if client_metadata_bytes == 0 {
        Metric::not_measured("metadata unavailable for an unbuilt analytical cell")
    } else {
        Metric::deterministic(
            client_metadata_bytes,
            "one authenticated immutable-generation index",
        )
    };
    work.client.upload_bytes = Metric::deterministic(
        shape.query_bytes_per_server * server_count,
        "every Dense selector share, including dummy padded document selectors",
    );
    work.client.download_bytes = Metric::deterministic(
        shape.response_bytes_per_server * server_count,
        "every answer share, including page framing and dummy padded document answers",
    );
    work.persisted_storage.server_bytes_per_server = Metric::deterministic(
        table_bytes_per_server,
        "tag rows plus the document table when the protocol has a private second stage",
    );
    work.persisted_storage.aggregate_server_bytes = Metric::deterministic(
        table_bytes_per_server * server_count,
        "fully replicated immutable tables",
    );
    work.persisted_storage.client_bytes = if client_metadata_bytes == 0 {
        Metric::not_measured("metadata unavailable for an unbuilt analytical cell")
    } else {
        Metric::deterministic(client_metadata_bytes, "authenticated public MPHF metadata")
    };
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    Ok(work)
}

fn benchmark_public_index_decoys(
    profile: Profile,
    snapshot: &MphfPageSnapshot,
    projection_bytes: usize,
    useful_count: usize,
    requested_pages: usize,
) -> Result<Samples> {
    if snapshot.manifest.distinct_tag_count < PUBLIC_INDEX_CANDIDATES {
        bail!("decoy benchmark needs at least 100 present tags in the cardinality class");
    }
    let candidates = decoy_candidate_tags();
    let warm = public_index_decoy_lookup(snapshot, &candidates, requested_pages)?;
    verify_public_index_decoys(
        snapshot,
        &candidates,
        projection_bytes,
        useful_count,
        requested_pages,
        &warm,
    )?;

    let expected_row_bytes = PUBLIC_INDEX_CANDIDATES
        .checked_mul(requested_pages)
        .and_then(|lookups| lookups.checked_mul(snapshot.manifest.page_size))
        .context("decoy logical row bytes overflow")?;
    let mut samples = Samples::default();
    for _ in 0..sample_count(profile) {
        let client_started = Instant::now();
        let request = candidates.clone();
        let request_client = client_started.elapsed();
        let server_started = Instant::now();
        let response = public_index_decoy_lookup(snapshot, &request, requested_pages)?;
        let server_elapsed = server_started.elapsed();
        let client_started = Instant::now();
        verify_public_index_decoys(
            snapshot,
            &request,
            projection_bytes,
            useful_count,
            requested_pages,
            &response,
        )?;
        samples.aggregate_server.push(server_elapsed);
        samples.wall.push(server_elapsed);
        samples
            .client
            .push(request_client + client_started.elapsed());
        samples.actual_logical_bytes.push(expected_row_bytes);
    }
    Ok(samples)
}

fn public_index_decoy_lookup(
    snapshot: &MphfPageSnapshot,
    candidates: &[[u8; 8]],
    requested_pages: usize,
) -> Result<Vec<Vec<u8>>> {
    let response_count = candidates
        .len()
        .checked_mul(requested_pages)
        .context("decoy response count overflow")?;
    let mut rows = Vec::with_capacity(response_count);
    for tag in candidates {
        // Do not read total_pages and stop early: the public cardinality class
        // fixes this exact schedule for every target and decoy candidate.
        for page in 0..requested_pages {
            let ordinal = snapshot.ordinal(tag, page)?;
            rows.push(snapshot.row(ordinal)?.to_vec());
        }
    }
    Ok(rows)
}

fn verify_public_index_decoys(
    snapshot: &MphfPageSnapshot,
    candidates: &[[u8; 8]],
    projection_bytes: usize,
    useful_count: usize,
    requested_pages: usize,
    rows: &[Vec<u8>],
) -> Result<()> {
    let expected_rows = candidates
        .len()
        .checked_mul(requested_pages)
        .context("decoy expected response count overflow")?;
    if rows.len() != expected_rows {
        bail!("decoy server returned the wrong fixed response count");
    }
    for (candidate_index, tag) in candidates.iter().enumerate() {
        let mut values = Vec::with_capacity(useful_count);
        let response_base = candidate_index
            .checked_mul(requested_pages)
            .context("decoy response offset overflow")?;
        for page in 0..requested_pages {
            let response_index = response_base
                .checked_add(page)
                .context("decoy response index overflow")?;
            let row = &rows[response_index];
            if let Some(decoded) = snapshot.decode_retrieved_page(row, tag, page)? {
                values.extend(decoded.values);
            }
        }
        if values.len() != useful_count {
            bail!(
                "decoy candidate {candidate_index} returned {} values, expected {useful_count}",
                values.len()
            );
        }
        let tag_index = usize::try_from(u64::from_le_bytes(*tag))
            .context("decoy synthetic tag index does not fit usize")?;
        let (first_ordinal, candidate_values) = benchmark_tag_ordinal_range(
            snapshot.manifest.document_count,
            snapshot.manifest.distinct_tag_count,
            tag_index,
        )?;
        if candidate_values != useful_count {
            bail!(
                "decoy candidate {candidate_index} is outside the target padded-cardinality class"
            );
        }
        for (offset, value) in values.iter().enumerate() {
            let expected_ordinal = first_ordinal
                .checked_add(offset)
                .context("decoy expected ordinal overflow")?;
            let expected = projection_value(expected_ordinal, projection_bytes)?;
            if *value != expected {
                bail!("decoy candidate {candidate_index} projection {offset} mismatched");
            }
        }
    }
    Ok(())
}

fn decoy_candidate_tags() -> Vec<[u8; 8]> {
    let mut candidates = (0..PUBLIC_INDEX_CANDIDATES)
        .map(benchmark_tag)
        .collect::<Vec<_>>();
    // The public request carries no target marker and does not put the target
    // in a conventional first position. This fixed seed keeps the benchmark
    // reproducible; it is explicitly not a production-quality decoy sampler.
    candidates.shuffle(&mut StdRng::seed_from_u64(0x0dec_0100));
    if candidates.first() == Some(&benchmark_tag(0)) {
        candidates.swap(0, 1);
    }
    candidates
}

fn benchmark_tag_ordinal_range(
    document_count: usize,
    distinct_tag_count: usize,
    tag_index: usize,
) -> Result<(usize, usize)> {
    if distinct_tag_count == 0 || tag_index >= distinct_tag_count {
        bail!("decoy tag index is outside the deterministic corpus");
    }
    let base = document_count / distinct_tag_count;
    let extra = document_count % distinct_tag_count;
    let first = tag_index
        .checked_mul(base)
        .and_then(|ordinal| ordinal.checked_add(tag_index.min(extra)))
        .context("decoy first ordinal overflow")?;
    Ok((first, base + usize::from(tag_index < extra)))
}

fn benchmark_inline(
    profile: Profile,
    snapshot: &MphfPageSnapshot,
    projection_bytes: usize,
    useful_count: usize,
    padded_count: usize,
    server_count: usize,
) -> Result<Samples> {
    let client = snapshot.trusted_client_index()?;
    let page_count = padded_count.div_ceil(snapshot.manifest.values_per_page);
    let tag = benchmark_tag(0);
    let ordinals = (0..page_count)
        .map(|page| client.ordinal(&tag, page))
        .collect::<Result<Vec<_>>>()?;
    let mut rng =
        StdRng::seed_from_u64(0xe2e0_1000 ^ projection_bytes as u64 ^ server_count as u64);
    let warm_queries = query_batch(
        &ordinals,
        snapshot.manifest.page_count,
        server_count,
        &mut rng,
    )?;
    let warm = evaluate_shared(snapshot.view(), &warm_queries)?;
    verify_inline(
        snapshot,
        &tag,
        useful_count,
        page_count,
        projection_bytes,
        warm.answers,
    )?;

    let mut samples = Samples::default();
    for _ in 0..sample_count(profile) {
        let client_started = Instant::now();
        let queries = query_batch(
            &ordinals,
            snapshot.manifest.page_count,
            server_count,
            &mut rng,
        )?;
        let query_client = client_started.elapsed();
        let evaluated = evaluate_shared(snapshot.view(), &queries)?;
        let client_started = Instant::now();
        verify_inline(
            snapshot,
            &tag,
            useful_count,
            page_count,
            projection_bytes,
            evaluated.answers,
        )?;
        samples.client.push(query_client + client_started.elapsed());
        samples
            .aggregate_server
            .push(evaluated.per_server.iter().copied().sum());
        samples.wall.push(evaluated.wall);
        samples
            .actual_logical_bytes
            .push(evaluated.actual_logical_bytes);
    }
    Ok(samples)
}

fn benchmark_locator_only(
    profile: Profile,
    snapshot: &MphfPageSnapshot,
    projection_bytes: usize,
    useful_count: usize,
    padded_count: usize,
    server_count: usize,
) -> Result<Samples> {
    let (_, locator) = benchmark_locator_samples(
        profile,
        snapshot,
        projection_bytes,
        useful_count,
        padded_count,
        server_count,
        None,
    )?;
    Ok(locator)
}

fn benchmark_two_stage(
    profile: Profile,
    locator_snapshot: &MphfPageSnapshot,
    documents: &[u8],
    projection_bytes: usize,
    useful_count: usize,
    padded_count: usize,
    server_count: usize,
) -> Result<(Samples, Samples)> {
    benchmark_locator_samples(
        profile,
        locator_snapshot,
        projection_bytes,
        useful_count,
        padded_count,
        server_count,
        Some(documents),
    )
}

#[allow(clippy::too_many_arguments)]
fn benchmark_locator_samples(
    profile: Profile,
    locator_snapshot: &MphfPageSnapshot,
    projection_bytes: usize,
    useful_count: usize,
    padded_count: usize,
    server_count: usize,
    documents: Option<&[u8]>,
) -> Result<(Samples, Samples)> {
    let client = locator_snapshot.trusted_client_index()?;
    let tag = benchmark_tag(0);
    let requested_pages = padded_count.div_ceil(locator_snapshot.manifest.values_per_page);
    let page_ordinals = (0..requested_pages)
        .map(|page| client.ordinal(&tag, page))
        .collect::<Result<Vec<_>>>()?;
    let mut rng =
        StdRng::seed_from_u64(0xe2e0_2000 ^ projection_bytes as u64 ^ server_count as u64);

    // Warm every measured phase once, with the exact fixed padded schedule.
    let warm_locator_queries = query_batch(
        &page_ordinals,
        locator_snapshot.manifest.page_count,
        server_count,
        &mut rng,
    )?;
    let warm_locator = evaluate_shared(locator_snapshot.view(), &warm_locator_queries)?;
    let locators = decode_locators(
        locator_snapshot,
        &tag,
        projection_bytes,
        useful_count,
        padded_count,
        warm_locator.answers,
    )?;
    if let Some(documents) = documents {
        let ordinals = locators
            .iter()
            .map(|locator| locator.ordinal)
            .collect::<Vec<_>>();
        let warm_document_queries = query_batch(&ordinals, DOCUMENT_COUNT, server_count, &mut rng)?;
        let warm_documents = evaluate_shared(
            SnapshotView::new(documents, DOCUMENT_COUNT, projection_bytes),
            &warm_document_queries,
        )?;
        verify_documents(
            documents,
            projection_bytes,
            useful_count,
            &locators,
            warm_documents.answers,
        )?;
    }

    let mut combined_samples = Samples::default();
    let mut locator_samples = Samples::default();
    for _ in 0..sample_count(profile) {
        let locator_client_started = Instant::now();
        let locator_queries = query_batch(
            &page_ordinals,
            locator_snapshot.manifest.page_count,
            server_count,
            &mut rng,
        )?;
        let locator_query_client = locator_client_started.elapsed();
        let locator_evaluated = evaluate_shared(locator_snapshot.view(), &locator_queries)?;
        let locator_client_started = Instant::now();
        let locators = decode_locators(
            locator_snapshot,
            &tag,
            projection_bytes,
            useful_count,
            padded_count,
            locator_evaluated.answers,
        )?;
        let locator_client = locator_query_client + locator_client_started.elapsed();
        let locator_server: Duration = locator_evaluated.per_server.iter().copied().sum();
        locator_samples.aggregate_server.push(locator_server);
        locator_samples.wall.push(locator_evaluated.wall);
        locator_samples.client.push(locator_client);
        locator_samples
            .actual_logical_bytes
            .push(locator_evaluated.actual_logical_bytes);

        if let Some(documents) = documents {
            let document_client_started = Instant::now();
            let ordinals = locators
                .iter()
                .map(|locator| locator.ordinal)
                .collect::<Vec<_>>();
            let document_queries = query_batch(&ordinals, DOCUMENT_COUNT, server_count, &mut rng)?;
            let document_query_client = document_client_started.elapsed();
            let document_evaluated = evaluate_shared(
                SnapshotView::new(documents, DOCUMENT_COUNT, projection_bytes),
                &document_queries,
            )?;
            let document_client_started = Instant::now();
            verify_documents(
                documents,
                projection_bytes,
                useful_count,
                &locators,
                document_evaluated.answers,
            )?;
            let document_client = document_query_client + document_client_started.elapsed();
            let document_server: Duration = document_evaluated.per_server.iter().copied().sum();
            combined_samples
                .aggregate_server
                .push(locator_server + document_server);
            combined_samples
                .wall
                .push(locator_evaluated.wall + document_evaluated.wall);
            combined_samples
                .client
                .push(locator_client + document_client);
            combined_samples.actual_logical_bytes.push(
                locator_evaluated.actual_logical_bytes + document_evaluated.actual_logical_bytes,
            );
        }
    }
    Ok((combined_samples, locator_samples))
}

fn evaluate_shared(snapshot: SnapshotView<'_>, queries: &[Vec<Vec<u8>>]) -> Result<Evaluation> {
    let max_queries = queries.iter().map(Vec::len).max().unwrap_or(1).max(1);
    let max_working_bytes = max_queries
        .checked_mul(snapshot.row_size)
        .and_then(|bytes| bytes.checked_add(snapshot.row_size))
        .context("shared batch working-memory limit overflow")?;
    let wall_started = Instant::now();
    let results = std::thread::scope(|scope| {
        queries
            .iter()
            .map(|server_queries| {
                scope.spawn(move || {
                    let evaluator = BatchEvaluator::new(max_queries, max_working_bytes)?;
                    let started = Instant::now();
                    let evaluated = evaluator.evaluate(
                        snapshot,
                        server_queries,
                        BatchKernel::SharedRowMajor,
                    )?;
                    Ok::<_, anyhow::Error>((
                        started.elapsed(),
                        evaluated.answers,
                        evaluated.metrics.immutable_source_operand_bytes,
                    ))
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .expect("end-to-end shared-batch server panicked")
            })
            .collect::<Result<Vec<_>>>()
    })?;
    Ok(Evaluation {
        wall: wall_started.elapsed(),
        per_server: results.iter().map(|(elapsed, _, _)| *elapsed).collect(),
        actual_logical_bytes: results.iter().map(|(_, _, bytes)| *bytes).sum(),
        answers: results.into_iter().map(|(_, answers, _)| answers).collect(),
    })
}

#[cfg(test)]
fn selected_bits(query: &[u8], row_count: usize) -> usize {
    query
        .iter()
        .enumerate()
        .map(|(byte_index, &byte)| {
            let remaining = row_count.saturating_sub(byte_index * 8).min(8);
            let mask = if remaining == 8 {
                u8::MAX
            } else {
                ((1u16 << remaining) - 1) as u8
            };
            (byte & mask).count_ones() as usize
        })
        .sum()
}

fn query_batch(
    ordinals: &[usize],
    row_count: usize,
    server_count: usize,
    rng: &mut StdRng,
) -> Result<Vec<Vec<Vec<u8>>>> {
    let mut by_server = (0..server_count)
        .map(|_| Vec::with_capacity(ordinals.len()))
        .collect::<Vec<_>>();
    for &ordinal in ordinals {
        let shares = dense::query_shares(ordinal, row_count, server_count, rng)?;
        for (server, share) in by_server.iter_mut().zip(shares) {
            server.push(share);
        }
    }
    Ok(by_server)
}

fn combine_query_answers(answers: &[Vec<Vec<u8>>], query_index: usize) -> Result<Vec<u8>> {
    let shares = answers
        .iter()
        .map(|server| {
            server
                .get(query_index)
                .map(Vec::as_slice)
                .context("server returned too few answers")
        })
        .collect::<Result<Vec<_>>>()?;
    dense::combine(&shares)
}

fn verify_inline(
    snapshot: &MphfPageSnapshot,
    tag: &[u8],
    useful_count: usize,
    requested_pages: usize,
    projection_bytes: usize,
    answers: Vec<Vec<Vec<u8>>>,
) -> Result<()> {
    let mut values = Vec::with_capacity(useful_count);
    for page in 0..requested_pages {
        let answer = combine_query_answers(&answers, page)?;
        if let Some(decoded) = snapshot.decode_retrieved_page(&answer, tag, page)? {
            values.extend(decoded.values);
        }
    }
    if values.len() != useful_count {
        bail!(
            "inline retrieval recovered {} values, expected {useful_count}",
            values.len()
        );
    }
    for (ordinal, value) in values.iter().enumerate() {
        if *value != projection_value(ordinal, projection_bytes)? {
            bail!("inline projection mismatch at ordinal {ordinal}");
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct Locator {
    ordinal: usize,
    fingerprint: [u8; 16],
    dummy: bool,
}

fn decode_locators(
    snapshot: &MphfPageSnapshot,
    tag: &[u8],
    projection_bytes: usize,
    useful_count: usize,
    padded_count: usize,
    answers: Vec<Vec<Vec<u8>>>,
) -> Result<Vec<Locator>> {
    let requested_pages = padded_count.div_ceil(snapshot.manifest.values_per_page);
    let mut locators = Vec::with_capacity(padded_count);
    for page in 0..requested_pages {
        let answer = combine_query_answers(&answers, page)?;
        if let Some(decoded) = snapshot.decode_retrieved_page(&answer, tag, page)? {
            for encoded in decoded.values {
                locators.push(parse_locator(&encoded, false)?);
            }
        }
    }
    if locators.len() != useful_count {
        bail!(
            "locator retrieval recovered {} values, expected {useful_count}",
            locators.len()
        );
    }
    for (expected, locator) in locators.iter().enumerate() {
        if locator.ordinal != expected
            || locator.fingerprint
                != projection_fingerprint(&projection_value(expected, projection_bytes)?)
        {
            bail!("locator mismatch at result {expected}");
        }
    }
    pad_locators(tag, &mut locators, padded_count, projection_bytes)?;
    Ok(locators)
}

fn pad_locators(
    tag: &[u8],
    locators: &mut Vec<Locator>,
    padded_count: usize,
    projection_bytes: usize,
) -> Result<()> {
    if locators.len() > padded_count {
        bail!("useful locator count exceeds the public padded-cardinality policy");
    }
    while locators.len() < padded_count {
        let padding_index = locators.len();
        let ordinal = dummy_ordinal(tag, padding_index);
        let value = projection_value(ordinal, projection_bytes)?;
        locators.push(Locator {
            ordinal,
            fingerprint: projection_fingerprint(&value),
            dummy: true,
        });
    }
    Ok(())
}

fn verify_documents(
    documents: &[u8],
    projection_bytes: usize,
    useful_count: usize,
    locators: &[Locator],
    answers: Vec<Vec<Vec<u8>>>,
) -> Result<()> {
    if locators.len() != answers.first().map_or(0, Vec::len) {
        bail!("document answer count does not match padded locator count");
    }
    let mut useful_seen = 0;
    for (index, locator) in locators.iter().enumerate() {
        let answer = combine_query_answers(&answers, index)?;
        if projection_fingerprint(&answer) != locator.fingerprint {
            bail!("document fingerprint mismatch at padded result {index}");
        }
        let ordinal = u64::from_le_bytes(answer[..8].try_into()?) as usize;
        if ordinal != locator.ordinal {
            bail!("document ordinal mismatch at padded result {index}");
        }
        let expected = &documents[ordinal * projection_bytes..(ordinal + 1) * projection_bytes];
        if answer != expected {
            bail!("document bytes mismatch at padded result {index}");
        }
        if !locator.dummy {
            useful_seen += 1;
        }
    }
    if useful_seen != useful_count {
        bail!("document retrieval retained {useful_seen} useful values, expected {useful_count}");
    }
    Ok(())
}

fn dimensions(
    value_bytes: usize,
    requested_fanout: usize,
    page_capacity: usize,
) -> Result<LayoutDimensions> {
    let distinct_tags = DOCUMENT_COUNT.div_ceil(requested_fanout);
    let base = DOCUMENT_COUNT / distinct_tags;
    let extra = DOCUMENT_COUNT % distinct_tags;
    let target_values = base + usize::from(extra != 0);
    let padded_values = requested_fanout.next_power_of_two();
    let pages = (0..distinct_tags).try_fold(0usize, |total, tag_index| {
        let values = base + usize::from(tag_index < extra);
        total
            .checked_add(values.div_ceil(page_capacity))
            .context("page count overflow")
    })?;
    let page_size = PAGE_HEADER_BYTES
        .checked_add(
            page_capacity
                .checked_mul(VALUE_LENGTH_BYTES + value_bytes)
                .context("page value area overflow")?,
        )
        .context("page size overflow")?;
    Ok(LayoutDimensions {
        distinct_tags,
        target_values,
        padded_values,
        pages,
        page_capacity,
        page_size,
        target_pages: padded_values.div_ceil(page_capacity),
        table_bytes: pages
            .checked_mul(page_size)
            .context("table size overflow")?,
    })
}

fn padded_inline_capacity(fanout: usize, projection_bytes: usize) -> Result<usize> {
    let maximum = INLINE_PAGE_BUDGET
        .checked_sub(PAGE_HEADER_BYTES)
        .context("inline page budget smaller than header")?
        / (VALUE_LENGTH_BYTES + projection_bytes);
    if maximum == 0 {
        bail!("projection slot does not fit the inline page budget");
    }
    let maximum_power_of_two = 1usize << (usize::BITS - 1 - maximum.leading_zeros());
    Ok(fanout.next_power_of_two().min(maximum_power_of_two))
}

fn page_config(value_bytes: usize, values_per_page: usize) -> TagPageConfig {
    TagPageConfig {
        bucket_capacity: 1,
        target_load_percent: 90,
        values_per_page,
        max_value_bytes: value_bytes,
    }
}

fn build_page_set(
    projection_bytes: usize,
    requested_fanout: usize,
    values_per_page: usize,
    value_kind: PageValue,
) -> Result<EncodedPageSet> {
    let value_bytes = match value_kind {
        PageValue::Projection => projection_bytes,
        PageValue::Locator => LOCATOR_BYTES,
    };
    let config = page_config(value_bytes, values_per_page);
    let distinct_tags = DOCUMENT_COUNT.div_ceil(requested_fanout);
    let base_values = DOCUMENT_COUNT / distinct_tags;
    let extra_values = DOCUMENT_COUNT % distinct_tags;
    let maximum_values = base_values + usize::from(extra_values != 0);
    let maximum_pages_per_tag = maximum_values.div_ceil(values_per_page);
    let expected_pages = (0..distinct_tags).try_fold(0usize, |total, tag_index| {
        let count = base_values + usize::from(tag_index < extra_values);
        total
            .checked_add(count.div_ceil(values_per_page))
            .context("page count overflow")
    })?;
    let mut pages = Vec::with_capacity(expected_pages);
    let mut first_ordinal = 0usize;
    for tag_index in 0..distinct_tags {
        let tag = benchmark_tag(tag_index);
        let value_count = base_values + usize::from(tag_index < extra_values);
        let total_pages = value_count.div_ceil(values_per_page);
        for page_index in 0..total_pages {
            let first_value = page_index * values_per_page;
            let values_on_page = (value_count - first_value).min(values_per_page);
            let values = (0..values_on_page)
                .map(|offset| {
                    let ordinal = first_ordinal + first_value + offset;
                    match value_kind {
                        PageValue::Projection => projection_value(ordinal, projection_bytes),
                        PageValue::Locator => locator_value(ordinal, projection_bytes),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let key = page_key(&tag, page_index)?;
            pages.push(EncodedPage {
                bytes: encode_page(&key, total_pages, &values, &config)?,
                key,
            });
        }
        first_ordinal += value_count;
    }
    if first_ordinal != DOCUMENT_COUNT || pages.len() != expected_pages {
        bail!("synthetic page builder did not consume the exact corpus");
    }
    Ok(EncodedPageSet {
        document_count: DOCUMENT_COUNT,
        distinct_tag_count: distinct_tags,
        maximum_pages_per_tag,
        pages,
    })
}

fn build_document_rows(projection_bytes: usize) -> Result<Vec<u8>> {
    let mut rows = Vec::with_capacity(
        DOCUMENT_COUNT
            .checked_mul(projection_bytes)
            .context("document table size overflow")?,
    );
    for ordinal in 0..DOCUMENT_COUNT {
        rows.extend_from_slice(&projection_value(ordinal, projection_bytes)?);
    }
    Ok(rows)
}

fn projection_value(ordinal: usize, size: usize) -> Result<Vec<u8>> {
    if size < LOCATOR_BYTES + 1 {
        bail!("projection slots need at least 25 bytes for ordinal, fingerprint, and payload");
    }
    let ordinal = u64::try_from(ordinal).context("ordinal does not fit u64")?;
    let mut value = vec![0u8; size];
    value[..8].copy_from_slice(&ordinal.to_le_bytes());
    let mut state = ordinal
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(size as u64)
        .wrapping_add(1);
    for byte in &mut value[24..] {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    let fingerprint = projection_fingerprint(&value);
    value[8..24].copy_from_slice(&fingerprint);
    Ok(value)
}

fn projection_fingerprint(value: &[u8]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CORPUS_DOMAIN);
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(&value[..8]);
    if value.len() > 24 {
        hasher.update(&value[24..]);
    }
    let mut fingerprint = [0u8; 16];
    fingerprint.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    fingerprint
}

fn locator_value(ordinal: usize, projection_bytes: usize) -> Result<Vec<u8>> {
    let projection = projection_value(ordinal, projection_bytes)?;
    let mut locator = vec![0u8; LOCATOR_BYTES];
    locator[..8].copy_from_slice(&u64::try_from(ordinal)?.to_le_bytes());
    locator[8..].copy_from_slice(&projection_fingerprint(&projection));
    Ok(locator)
}

fn parse_locator(value: &[u8], dummy: bool) -> Result<Locator> {
    if value.len() != LOCATOR_BYTES {
        bail!(
            "locator has {} bytes, expected {LOCATOR_BYTES}",
            value.len()
        );
    }
    let ordinal = usize::try_from(u64::from_le_bytes(value[..8].try_into()?))
        .context("locator ordinal does not fit usize")?;
    if ordinal >= DOCUMENT_COUNT {
        bail!("locator ordinal is outside the document table");
    }
    Ok(Locator {
        ordinal,
        fingerprint: value[8..].try_into()?,
        dummy,
    })
}

fn dummy_ordinal(tag: &[u8], padding_index: usize) -> usize {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"defradb-pir-end-to-end-dummy-ordinal-v1");
    hasher.update(tag);
    hasher.update(&(padding_index as u64).to_le_bytes());
    let prefix = u64::from_le_bytes(hasher.finalize().as_bytes()[..8].try_into().unwrap());
    prefix as usize % DOCUMENT_COUNT
}

fn validate_absent(snapshot: &MphfPageSnapshot) -> Result<()> {
    let absent = b"absent-end-to-end-tag";
    for page in 0..snapshot.manifest.maximum_pages_per_tag.max(1) {
        let ordinal = snapshot.ordinal(absent, page)?;
        if snapshot
            .decode_retrieved_page(snapshot.row(ordinal)?, absent, page)?
            .is_some()
        {
            bail!("absent tag unexpectedly passed the 128-bit page fingerprint");
        }
    }
    Ok(())
}

fn topology(approach: &ApproachResult, server_count: usize) -> Result<&EndToEndTopology> {
    approach
        .topologies
        .iter()
        .find(|topology| topology.server_count == server_count)
        .context("missing topology")
}

fn sample_count(profile: Profile) -> usize {
    match profile {
        Profile::Quick => QUICK_SAMPLES,
        Profile::Full => FULL_SAMPLES,
    }
}

fn percentile(values: &[Duration], percentile: usize) -> Duration {
    values[((values.len() - 1) * percentile).div_ceil(100)]
}

fn percentile_usize(values: &[usize], percentile: usize) -> usize {
    values[((values.len() - 1) * percentile).div_ceil(100)]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::Record;

    fn test_config(values_per_page: usize) -> TagPageConfig {
        page_config(LOCATOR_BYTES, values_per_page)
    }

    #[test]
    fn fixed_schedule_hides_present_lower_and_absent_cardinality() {
        let projection_bytes = 32;
        let mut records = Vec::new();
        for ordinal in 0..5 {
            records.push(Record::new(
                b"maximum",
                locator_value(ordinal, projection_bytes).unwrap(),
            ));
        }
        for ordinal in 5..7 {
            records.push(Record::new(
                b"lower",
                locator_value(ordinal, projection_bytes).unwrap(),
            ));
        }
        let snapshot = MphfPageSnapshot::build(records, test_config(4)).unwrap();
        let client = snapshot.trusted_client_index().unwrap();
        let padded_count: usize = 8;
        let scheduled_pages = padded_count.div_ceil(4);

        for (tag, expected_values) in [
            (b"maximum".as_slice(), 5usize),
            (b"lower".as_slice(), 2usize),
            (b"absent".as_slice(), 0usize),
        ] {
            let ordinals = (0..scheduled_pages)
                .map(|page| client.ordinal(tag, page).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(ordinals.len(), 2, "every policy member issues two pages");
            let mut found = Vec::new();
            for (page, ordinal) in ordinals.into_iter().enumerate() {
                if let Some(decoded) = snapshot
                    .decode_retrieved_page(snapshot.row(ordinal).unwrap(), tag, page)
                    .unwrap()
                {
                    found.extend(decoded.values);
                }
            }
            assert_eq!(found.len(), expected_values);
            let mut parsed = found
                .iter()
                .map(|value| parse_locator(value, false).unwrap())
                .collect::<Vec<_>>();
            pad_locators(tag, &mut parsed, padded_count, projection_bytes).unwrap();
            assert_eq!(parsed.len(), padded_count);
            assert_eq!(
                parsed.iter().filter(|locator| !locator.dummy).count(),
                expected_values
            );
            assert_eq!(
                parsed.iter().filter(|locator| locator.dummy).count(),
                padded_count - expected_values
            );
        }
    }

    #[test]
    fn duplicate_and_fixed_width_tombstone_rows_survive_build() {
        let duplicate = projection_value(0, 32).unwrap();
        let mut tombstone = projection_value(1, 32).unwrap();
        tombstone[24] = 0xff;
        let fingerprint = projection_fingerprint(&tombstone);
        tombstone[8..24].copy_from_slice(&fingerprint);
        let snapshot = MphfPageSnapshot::build(
            vec![
                Record::new(b"tag", &duplicate),
                Record::new(b"tag", &duplicate),
                Record::new(b"tag", &tombstone),
            ],
            page_config(32, 4),
        )
        .unwrap();
        let values = snapshot.public_lookup(b"tag").unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(
            values.iter().filter(|value| *value == &duplicate).count(),
            2
        );
        assert!(values.contains(&tombstone));
        assert_eq!(tombstone.len(), duplicate.len());
    }

    #[test]
    fn document_fingerprint_binds_ordinal_and_payload() {
        let first = projection_value(7, 96).unwrap();
        let second = projection_value(8, 96).unwrap();
        assert_ne!(
            projection_fingerprint(&first),
            projection_fingerprint(&second)
        );
        assert_eq!(&first[8..24], projection_fingerprint(&first));
    }

    #[test]
    fn logical_bit_count_ignores_selector_padding_bits() {
        assert_eq!(selected_bits(&[0xff, 0xff], 9), 9);
        assert_eq!(selected_bits(&[0x55, 0xff], 9), 5);
    }

    #[test]
    fn public_index_decoys_return_every_candidates_complete_fixed_page() {
        let projection_bytes = 32;
        let useful_count = 3;
        let requested_pages = 2;
        let config = page_config(projection_bytes, 2);
        let mut pages = Vec::new();
        for tag_index in 0..PUBLIC_INDEX_CANDIDATES {
            let tag = benchmark_tag(tag_index);
            for page in 0..requested_pages {
                let first = page * config.values_per_page;
                let values = (first..useful_count)
                    .take(config.values_per_page)
                    .map(|offset| {
                        projection_value(tag_index * useful_count + offset, projection_bytes)
                            .unwrap()
                    })
                    .collect::<Vec<_>>();
                let key = page_key(&tag, page).unwrap();
                pages.push(EncodedPage {
                    bytes: encode_page(&key, requested_pages, &values, &config).unwrap(),
                    key,
                });
            }
        }
        let page_set = EncodedPageSet {
            document_count: PUBLIC_INDEX_CANDIDATES * useful_count,
            distinct_tag_count: PUBLIC_INDEX_CANDIDATES,
            maximum_pages_per_tag: requested_pages,
            pages,
        };
        let snapshot = MphfPageSnapshot::from_page_set(&page_set, config).unwrap();
        let candidates = decoy_candidate_tags();
        assert_ne!(candidates[0], benchmark_tag(0));
        assert!(candidates.contains(&benchmark_tag(0)));
        let rows = public_index_decoy_lookup(&snapshot, &candidates, requested_pages).unwrap();
        assert_eq!(rows.len(), PUBLIC_INDEX_CANDIDATES * requested_pages);
        assert!(rows
            .iter()
            .all(|row| row.len() == snapshot.manifest.page_size));
        verify_public_index_decoys(
            &snapshot,
            &candidates,
            projection_bytes,
            useful_count,
            requested_pages,
            &rows,
        )
        .unwrap();
    }
}
