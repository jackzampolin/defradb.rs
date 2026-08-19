//! Guarded 10M+ populated-page benchmark for the exact MPHF Dense layout.
//!
//! The default command is preflight-only.  A real multi-gigabyte PtrHash/page
//! build requires explicit `execute` mode and must pass every configured guard.

use std::{
    collections::BTreeSet,
    mem::size_of,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use rand::{rngs::StdRng, SeedableRng};
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
    mphf_pages::MphfPageSnapshot,
    tag_pages::{benchmark_tag, benchmark_value, EncodedPage, TagPageConfig},
};

const PAGE_FRAMING_BYTES: usize = 26;
const DEFAULT_MPHF_BITS_PER_PAGE_PREFLIGHT: usize = 4;
const ESTIMATED_PAGE_KEY_ALLOCATION_BYTES: usize = 32;
const ESTIMATED_PTRHASH_TRANSIENT_HEADROOM_PER_PAGE: usize = 32;
const SERVER_COUNTS: [usize; 2] = [2, 3];
const MAX_TIMING_SAMPLES: usize = 31;
const MAX_INLINE_PROJECTION_BYTES: usize = u16::MAX as usize;
const MIB: usize = 1 << 20;

#[derive(Clone, Debug, Serialize)]
pub struct ProductionScaleConfig {
    pub populated_tag_pages: usize,
    pub row_bytes: Vec<usize>,
    pub execute: bool,
    pub server_counts: Vec<usize>,
    pub max_table_bytes: usize,
    pub max_estimated_build_bytes: usize,
    pub max_timed_aggregate_logical_bytes: usize,
    pub max_client_upload_bytes: usize,
    pub samples: usize,
}

impl ProductionScaleConfig {
    pub fn for_profile(profile: Profile) -> Self {
        match profile {
            Profile::Quick => Self {
                populated_tag_pages: 10_000_000,
                row_bytes: vec![32, 96],
                execute: false,
                server_counts: SERVER_COUNTS.to_vec(),
                max_table_bytes: 1_024 * MIB,
                max_estimated_build_bytes: 2_048 * MIB,
                // Worst case, not the expected half-selected-row case: one
                // warm-up plus three samples over three 320 MB replicas.
                max_timed_aggregate_logical_bytes: 4_096 * MIB,
                max_client_upload_bytes: 64 * MIB,
                samples: 3,
            },
            Profile::Full => Self {
                populated_tag_pages: 25_000_000,
                row_bytes: vec![32, 96, 256, 1_024],
                execute: false,
                server_counts: SERVER_COUNTS.to_vec(),
                max_table_bytes: 4_096 * MIB,
                max_estimated_build_bytes: 6_144 * MIB,
                // Admits the 32-byte row at 25M pages and three replicas, but
                // refuses the wider rows unless the operator raises the cap.
                max_timed_aggregate_logical_bytes: 24_576 * MIB,
                max_client_upload_bytes: 256 * MIB,
                samples: 7,
            },
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ProductionScaleReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: Vec<&'static str>,
    pub config: ProductionScaleConfig,
    pub layouts: Vec<ProductionScaleLayout>,
    pub production_caveats: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct ProductionScaleLayout {
    pub populated_tag_pages: usize,
    pub row_bytes: usize,
    pub inline_projection_bytes: usize,
    pub preflight: ProductionScalePreflight,
    pub built: bool,
    pub build_skip_reasons: Vec<String>,
    pub table_bytes_per_server: usize,
    pub public_mphf_bytes: Option<usize>,
    pub public_mphf_preflight_bytes: usize,
    pub build_wall_ms: Option<f64>,
    pub peak_tracked_build_bytes: Option<usize>,
    pub generation: Option<String>,
    pub cold_client_index_load_ms: Option<f64>,
    pub client_ordinal_lookup_p50_us: Option<f64>,
    pub topologies: Vec<ProductionScaleTopology>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProductionScalePreflight {
    pub valid: bool,
    pub validation_error: Option<String>,
    pub table_bytes_per_server: usize,
    pub encoded_page_corpus_bytes: usize,
    pub key_vector_bytes: usize,
    pub ordered_rows_bytes: usize,
    pub occupancy_bytes: usize,
    pub public_mphf_estimate_bytes: usize,
    pub ptrhash_transient_headroom_bytes: usize,
    pub estimated_peak_build_bytes: usize,
    pub peak_estimate_note: &'static str,
    pub query_share_bytes_per_server: usize,
    pub maximum_client_upload_bytes: usize,
    pub maximum_expected_aggregate_logical_bytes: usize,
    pub maximum_timing_job_logical_bytes: usize,
    pub table_guard_passed: bool,
    pub build_guard_passed: bool,
    pub upload_guard_passed: bool,
    pub timing_work_guard_passed: bool,
}

#[derive(Debug, Serialize)]
pub struct ProductionScaleTopology {
    pub aggregate_work: AggregateWorkReport,
    pub server_count: usize,
    pub timed: bool,
    pub timing_skip_reasons: Vec<String>,
    pub expected_rows_xored_per_server: usize,
    pub expected_rows_xored_all_servers: usize,
    pub deterministic_selector_bits_examined_per_server: usize,
    pub deterministic_selector_bits_examined_all_servers: usize,
    pub deterministic_table_bytes_addressable_per_server: usize,
    pub deterministic_table_bytes_addressable_all_servers: usize,
    pub expected_logical_bytes_per_server: usize,
    pub expected_logical_bytes_all_servers: usize,
    pub actual_logical_bytes_p50_all_servers: Option<usize>,
    pub query_bytes_per_server: usize,
    pub total_client_upload_bytes: usize,
    pub response_bytes_per_server: usize,
    pub total_client_download_bytes: usize,
    pub aggregate_server_p50_ms: Option<f64>,
    pub aggregate_server_p95_ms: Option<f64>,
    pub co_located_wall_p50_ms: Option<f64>,
    pub co_located_wall_p95_ms: Option<f64>,
    pub client_query_combine_p50_ms: Option<f64>,
}

#[derive(Default)]
struct TimingSamples {
    aggregate_server: Vec<Duration>,
    wall: Vec<Duration>,
    client: Vec<Duration>,
    actual_logical_bytes: Vec<usize>,
}

struct TimingSummary {
    aggregate_server_p50: Duration,
    aggregate_server_p95: Duration,
    wall_p50: Duration,
    wall_p95: Duration,
    client_p50: Duration,
    actual_logical_bytes_p50: usize,
}

#[derive(Clone, Copy)]
struct BuiltArtifacts {
    public_mphf_bytes: usize,
    peak_tracked_build_bytes: usize,
    cold_client_index_load_ms: f64,
}

pub fn run_cli(profile: Profile, args: &[String]) -> Result<ProductionScaleReport> {
    if args.len() > 6 {
        bail!("too many production-scale arguments");
    }
    let mut config = ProductionScaleConfig::for_profile(profile);
    if let Some(mode) = args.first() {
        match mode.as_str() {
            "preflight" => config.execute = false,
            "execute" => config.execute = true,
            _ => bail!("production-scale mode must be preflight or execute"),
        }
    }
    if let Some(pages) = args.get(1) {
        config.populated_tag_pages = pages
            .parse()
            .context("production-scale PAGES must be an integer")?;
    }
    if let Some(widths) = args.get(2) {
        config.row_bytes = widths
            .split(',')
            .map(|width| {
                width
                    .trim()
                    .parse()
                    .context("production-scale ROW_BYTES must be comma-separated integers")
            })
            .collect::<Result<Vec<_>>>()?;
    }
    if let Some(max_build_mib) = args.get(3) {
        config.max_estimated_build_bytes = parse_mib(max_build_mib, "MAX_BUILD_MIB")?;
    }
    if let Some(max_table_mib) = args.get(4) {
        config.max_table_bytes = parse_mib(max_table_mib, "MAX_TABLE_MIB")?;
    }
    if let Some(max_work_mib) = args.get(5) {
        config.max_timed_aggregate_logical_bytes = parse_mib(max_work_mib, "MAX_TIMED_WORK_MIB")?;
    }
    run(profile, config)
}

pub fn run(profile: Profile, config: ProductionScaleConfig) -> Result<ProductionScaleReport> {
    validate_config(&config)?;
    let layouts = config
        .row_bytes
        .iter()
        .copied()
        .map(|row_bytes| benchmark_layout(&config, row_bytes))
        .collect::<Result<Vec<_>>>()?;
    Ok(ProductionScaleReport {
        protocol: "production-scale-exact-mphf-inline-dense-xor-v1",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: vec![
            "Rows are populated tag pages, not documents. Exactly one tag, one page, and one fixed-width inline projection slot are materialized per populated MPHF ordinal.",
            "The exact PtrHash MPHF has no empty Dense rows. A cold client loads generation-bound public metadata, computes one ordinal, and privately retrieves one row from every replica.",
            "Preflight computes table, explicit builder buffers, query traffic, and expected random-share XOR work with checked arithmetic before any large allocation.",
            "Preflight-only is the default. Execute mode still refuses construction or timing when a configured guard fails; the timing gate charges the worst-case selected-row work of its warm-up plus every sample.",
            "Deterministic work fields count selector positions examined and the complete addressable table. Expected XOR-byte fields are explicitly probabilistic because uniformly random XOR shares select half the rows in expectation; execute mode also reports exact seeded selected-row bytes.",
            "Each server is single-threaded inside one request. Replicas run concurrently; aggregate server elapsed is the sum of replica elapsed times and co-located wall is reported separately.",
        ],
        config,
        layouts,
        production_caveats: vec![
            "The build-memory estimate includes explicit EncodedPage/key/row/occupancy buffers plus conservative PtrHash headroom, but PtrHash does not expose transient allocation. Passing the guard is not proof against allocator or implementation spikes.",
            "The MPHF preflight artifact estimate uses four bits per populated page and is labelled estimated. Execute mode reports the exact serialized artifact size.",
            "PtrHash epserde metadata is build-specific and unsafe to deserialize before authenticating and size-bounding the exact generation artifact.",
            "MPHF metadata is key-dependent public state. It does not directly reveal the selected tag, but guessed-key injectivity and cross-generation set relations require a separate leakage review.",
            "Timings exclude HTTP, TLS, serialization, WAN latency, filesystem persistence, NUMA placement, and hardware counters. Production 10M+ runs should pin replicas and collect cycles, instructions, cache misses, RSS, and energy.",
            "All answer shares are required. Adding a third replica raises aggregate work by roughly 50% over two replicas while tolerating collusion of any two only under the n-out-of-n Dense XOR model.",
        ],
    })
}

fn benchmark_layout(
    config: &ProductionScaleConfig,
    row_bytes: usize,
) -> Result<ProductionScaleLayout> {
    let preflight = preflight(config, row_bytes);
    let mut build_skip_reasons = Vec::new();
    if !config.execute {
        build_skip_reasons
            .push("mode is preflight; pass execute to permit large allocations".into());
    }
    if !preflight.valid {
        build_skip_reasons.push(
            preflight
                .validation_error
                .clone()
                .unwrap_or_else(|| "preflight validation failed".into()),
        );
    }
    if !preflight.table_guard_passed {
        build_skip_reasons.push(format!(
            "table needs {} bytes, above {}-byte table guard",
            preflight.table_bytes_per_server, config.max_table_bytes
        ));
    }
    if !preflight.build_guard_passed {
        build_skip_reasons.push(format!(
            "estimated build peak is {} bytes, above {}-byte build guard",
            preflight.estimated_peak_build_bytes, config.max_estimated_build_bytes
        ));
    }
    let should_build = build_skip_reasons.is_empty();
    if !should_build {
        let topologies = config
            .server_counts
            .iter()
            .copied()
            .map(|servers| {
                analytical_topology(
                    config,
                    &preflight,
                    row_bytes,
                    servers,
                    None,
                    None,
                    timing_skip_reasons(config, &preflight, row_bytes, servers),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(ProductionScaleLayout {
            populated_tag_pages: config.populated_tag_pages,
            row_bytes,
            inline_projection_bytes: row_bytes.saturating_sub(PAGE_FRAMING_BYTES),
            table_bytes_per_server: preflight.table_bytes_per_server,
            public_mphf_preflight_bytes: preflight.public_mphf_estimate_bytes,
            preflight,
            built: false,
            build_skip_reasons,
            public_mphf_bytes: None,
            build_wall_ms: None,
            peak_tracked_build_bytes: None,
            generation: None,
            cold_client_index_load_ms: None,
            client_ordinal_lookup_p50_us: None,
            topologies,
        });
    }

    let tag_config = TagPageConfig {
        bucket_capacity: 1,
        target_load_percent: 90,
        values_per_page: 1,
        max_value_bytes: row_bytes - PAGE_FRAMING_BYTES,
    };
    let build_started = Instant::now();
    let snapshot = MphfPageSnapshot::benchmark(
        config.populated_tag_pages,
        config.populated_tag_pages,
        tag_config,
    )?;
    let build_wall_ms = millis(build_started.elapsed());
    if snapshot.manifest.page_count != config.populated_tag_pages
        || snapshot.manifest.page_size != row_bytes
    {
        bail!("production-scale MPHF build did not preserve requested dimensions");
    }
    validate_absent(&snapshot)?;
    let client_load_started = Instant::now();
    let client = snapshot.trusted_client_index()?;
    let cold_client_index_load_ms = millis(client_load_started.elapsed());
    let target_index = config.populated_tag_pages / 3;
    let target_tag = benchmark_tag(target_index);
    let expected_value = benchmark_value(target_index, 0, row_bytes - PAGE_FRAMING_BYTES);
    let ordinal = client.ordinal(&target_tag, 0)?;
    let client_ordinal_lookup_p50_us =
        benchmark_lookup(config.samples, || client.ordinal(&target_tag, 0))?;
    let artifacts = BuiltArtifacts {
        public_mphf_bytes: snapshot.manifest.client_metadata_bytes(),
        peak_tracked_build_bytes: snapshot.build_metrics.peak_tracked_bytes,
        cold_client_index_load_ms,
    };
    let topologies = config
        .server_counts
        .iter()
        .copied()
        .map(|servers| {
            let reasons = timing_skip_reasons(config, &preflight, row_bytes, servers);
            let timed = reasons.is_empty();
            let timing = if timed {
                Some(measure_topology(
                    &snapshot,
                    &target_tag,
                    &expected_value,
                    ordinal,
                    servers,
                    config.samples,
                )?)
            } else {
                None
            };
            analytical_topology(
                config,
                &preflight,
                row_bytes,
                servers,
                Some(artifacts),
                timing,
                reasons,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ProductionScaleLayout {
        populated_tag_pages: config.populated_tag_pages,
        row_bytes,
        inline_projection_bytes: row_bytes - PAGE_FRAMING_BYTES,
        table_bytes_per_server: snapshot.rows().len(),
        public_mphf_bytes: Some(artifacts.public_mphf_bytes),
        public_mphf_preflight_bytes: preflight.public_mphf_estimate_bytes,
        build_wall_ms: Some(build_wall_ms),
        peak_tracked_build_bytes: Some(artifacts.peak_tracked_build_bytes),
        generation: Some(snapshot.manifest.generation_hex()),
        cold_client_index_load_ms: Some(cold_client_index_load_ms),
        client_ordinal_lookup_p50_us: Some(client_ordinal_lookup_p50_us),
        preflight,
        built: true,
        build_skip_reasons,
        topologies,
    })
}

fn preflight(config: &ProductionScaleConfig, row_bytes: usize) -> ProductionScalePreflight {
    match checked_preflight(config, row_bytes) {
        Ok(preflight) => preflight,
        Err(error) => ProductionScalePreflight {
            valid: false,
            validation_error: Some(error.to_string()),
            table_bytes_per_server: 0,
            encoded_page_corpus_bytes: 0,
            key_vector_bytes: 0,
            ordered_rows_bytes: 0,
            occupancy_bytes: 0,
            public_mphf_estimate_bytes: 0,
            ptrhash_transient_headroom_bytes: 0,
            estimated_peak_build_bytes: 0,
            peak_estimate_note: peak_estimate_note(),
            query_share_bytes_per_server: 0,
            maximum_client_upload_bytes: 0,
            maximum_expected_aggregate_logical_bytes: 0,
            maximum_timing_job_logical_bytes: 0,
            table_guard_passed: false,
            build_guard_passed: false,
            upload_guard_passed: false,
            timing_work_guard_passed: false,
        },
    }
}

fn checked_preflight(
    config: &ProductionScaleConfig,
    row_bytes: usize,
) -> Result<ProductionScalePreflight> {
    if row_bytes <= PAGE_FRAMING_BYTES {
        bail!("row width must exceed the 26-byte page framing");
    }
    let rows = config.populated_tag_pages;
    let table_bytes = checked_mul(rows, row_bytes, "Dense table")?;
    let encoded_page_corpus_bytes = checked_mul(
        rows,
        size_of::<EncodedPage>()
            .checked_add(ESTIMATED_PAGE_KEY_ALLOCATION_BYTES)
            .and_then(|bytes| bytes.checked_add(row_bytes))
            .context("encoded page estimate overflow")?,
        "encoded page corpus",
    )?;
    let key_vector_bytes = checked_mul(rows, size_of::<u64>() * 2, "two key vectors")?;
    let occupancy_bytes = checked_mul(rows, size_of::<bool>(), "occupancy bitmap")?;
    let public_mphf_estimate_bytes = rows
        .checked_mul(DEFAULT_MPHF_BITS_PER_PAGE_PREFLIGHT)
        .context("MPHF bit estimate overflow")?
        .div_ceil(8);
    let serialized_mphf_copies = checked_mul(
        public_mphf_estimate_bytes,
        2,
        "serialized MPHF retained copies",
    )?;
    let ptrhash_transient_headroom_bytes = checked_mul(
        rows,
        ESTIMATED_PTRHASH_TRANSIENT_HEADROOM_PER_PAGE,
        "PtrHash transient headroom",
    )?;
    let estimated_peak_build_bytes = [
        encoded_page_corpus_bytes,
        key_vector_bytes,
        table_bytes,
        occupancy_bytes,
        serialized_mphf_copies,
        ptrhash_transient_headroom_bytes,
    ]
    .into_iter()
    .try_fold(0usize, |total, value| {
        total
            .checked_add(value)
            .context("build peak estimate overflow")
    })?;
    let query_bytes = dense::query_size(rows);
    let max_servers = *config
        .server_counts
        .iter()
        .max()
        .context("no configured server counts")?;
    let maximum_client_upload_bytes = checked_mul(query_bytes, max_servers, "client upload")?;
    let expected_bytes_per_server = checked_mul(rows.div_ceil(2), row_bytes, "expected XOR bytes")?;
    let maximum_expected_aggregate_logical_bytes = checked_mul(
        expected_bytes_per_server,
        max_servers,
        "aggregate expected XOR bytes",
    )?;
    let maximum_selected_bytes_per_query =
        checked_mul(table_bytes, max_servers, "maximum selected bytes")?;
    let timing_requests = config
        .samples
        .checked_add(1)
        .context("timing warm-up/sample count overflow")?;
    let maximum_timing_job_logical_bytes = checked_mul(
        maximum_selected_bytes_per_query,
        timing_requests,
        "maximum timing-job selected bytes",
    )?;
    Ok(ProductionScalePreflight {
        valid: true,
        validation_error: None,
        table_bytes_per_server: table_bytes,
        encoded_page_corpus_bytes,
        key_vector_bytes,
        ordered_rows_bytes: table_bytes,
        occupancy_bytes,
        public_mphf_estimate_bytes,
        ptrhash_transient_headroom_bytes,
        estimated_peak_build_bytes,
        peak_estimate_note: peak_estimate_note(),
        query_share_bytes_per_server: query_bytes,
        maximum_client_upload_bytes,
        maximum_expected_aggregate_logical_bytes,
        maximum_timing_job_logical_bytes,
        table_guard_passed: table_bytes <= config.max_table_bytes,
        build_guard_passed: estimated_peak_build_bytes <= config.max_estimated_build_bytes,
        upload_guard_passed: maximum_client_upload_bytes <= config.max_client_upload_bytes,
        timing_work_guard_passed: maximum_timing_job_logical_bytes
            <= config.max_timed_aggregate_logical_bytes,
    })
}

fn analytical_topology(
    config: &ProductionScaleConfig,
    preflight: &ProductionScalePreflight,
    row_bytes: usize,
    server_count: usize,
    artifacts: Option<BuiltArtifacts>,
    timing: Option<TimingSummary>,
    mut reasons: Vec<String>,
) -> Result<ProductionScaleTopology> {
    let expected_rows_per_server = config.populated_tag_pages.div_ceil(2);
    let expected_rows_all = checked_mul(expected_rows_per_server, server_count, "expected rows")?;
    let expected_bytes_per_server =
        checked_mul(expected_rows_per_server, row_bytes, "expected bytes")?;
    let expected_bytes_all = checked_mul(
        expected_bytes_per_server,
        server_count,
        "aggregate expected bytes",
    )?;
    let query_bytes = dense::query_size(config.populated_tag_pages);
    let upload = checked_mul(query_bytes, server_count, "aggregate upload")?;
    let download = checked_mul(row_bytes, server_count, "aggregate download")?;
    let selector_bits_all = checked_mul(
        config.populated_tag_pages,
        server_count,
        "aggregate selector positions",
    )?;
    let addressable_table_all = checked_mul(
        preflight.table_bytes_per_server,
        server_count,
        "aggregate addressable table bytes",
    )?;
    if !preflight.valid {
        reasons.push(
            preflight
                .validation_error
                .clone()
                .unwrap_or_else(|| "preflight validation failed".into()),
        );
    }
    if artifacts.is_none() {
        reasons.push("exact MPHF layout was not built after preflight".into());
    }
    reasons.sort();
    reasons.dedup();
    let timed = timing.is_some();
    let work = topology_accounting(
        row_bytes,
        server_count,
        preflight,
        artifacts,
        expected_bytes_per_server,
        expected_bytes_all,
        upload,
        download,
        timing.as_ref(),
    )?;
    Ok(ProductionScaleTopology {
        aggregate_work: work,
        server_count,
        timed,
        timing_skip_reasons: if timed { Vec::new() } else { reasons },
        expected_rows_xored_per_server: expected_rows_per_server,
        expected_rows_xored_all_servers: expected_rows_all,
        deterministic_selector_bits_examined_per_server: config.populated_tag_pages,
        deterministic_selector_bits_examined_all_servers: selector_bits_all,
        deterministic_table_bytes_addressable_per_server: preflight.table_bytes_per_server,
        deterministic_table_bytes_addressable_all_servers: addressable_table_all,
        expected_logical_bytes_per_server: expected_bytes_per_server,
        expected_logical_bytes_all_servers: expected_bytes_all,
        actual_logical_bytes_p50_all_servers: timing
            .as_ref()
            .map(|timing| timing.actual_logical_bytes_p50),
        query_bytes_per_server: query_bytes,
        total_client_upload_bytes: upload,
        response_bytes_per_server: row_bytes,
        total_client_download_bytes: download,
        aggregate_server_p50_ms: timing
            .as_ref()
            .map(|timing| millis(timing.aggregate_server_p50)),
        aggregate_server_p95_ms: timing
            .as_ref()
            .map(|timing| millis(timing.aggregate_server_p95)),
        co_located_wall_p50_ms: timing.as_ref().map(|timing| millis(timing.wall_p50)),
        co_located_wall_p95_ms: timing.as_ref().map(|timing| millis(timing.wall_p95)),
        client_query_combine_p50_ms: timing.as_ref().map(|timing| millis(timing.client_p50)),
    })
}

#[allow(clippy::too_many_arguments)]
fn topology_accounting(
    row_bytes: usize,
    server_count: usize,
    preflight: &ProductionScalePreflight,
    artifacts: Option<BuiltArtifacts>,
    expected_bytes_per_server: usize,
    expected_bytes_all_servers: usize,
    total_upload_bytes: usize,
    total_download_bytes: usize,
    timing: Option<&TimingSummary>,
) -> Result<AggregateWorkReport> {
    let table_bytes = preflight.table_bytes_per_server;
    let mut work = AggregateWorkReport::new(
        "exact-mphf-inline-projection-dense-xor-production-scale",
        ComparisonScope {
            workload: "one private lookup over the configured populated-tag-page production-scale immutable generation",
            result: "one fixed-width inline projection page including framing",
            public_partition: "global immutable snapshot; no time-window leakage",
            leakage: LeakageScope::ExactQueryPrivacy,
        },
        SecurityLabels {
            privacy: "exact information-theoretic n-server Dense XOR query privacy against any n-1 colluding semi-honest replicas",
            server_count,
            collusion_tolerance: server_count - 1,
            required_answers: server_count,
            assumptions: "at least one replica does not collude; all replicas serve one authenticated MPHF/table generation",
            availability: "all answer shares are required",
            integrity: "128-bit page fingerprint rejects absent/wrong rows; no malicious-server proof or MAC",
        },
    );
    work.global_build.aggregate_server_time_ms = Metric::not_measured(
        "build wall is reported by the enclosing layout when executed; aggregate builder CPU was not collected",
    );
    work.global_build.client_time_ms = Metric::not_applicable("build is server-side");
    work.global_build.logical_selected_bytes = Metric::estimated(
        preflight.encoded_page_corpus_bytes,
        "preflight encoded-page corpus estimate; construction performs additional key/MPHF passes",
    );
    work.global_build.physical_or_scanned_bytes =
        Metric::not_measured("build hardware bytes not collected");
    work.global_build.peak_server_ram_bytes = if let Some(artifacts) = artifacts {
        Metric::measured(
            artifacts.peak_tracked_build_bytes,
            "peak explicitly tracked by the exact MPHF builder; unexposed PtrHash workspace remains excluded",
        )
    } else {
        Metric::estimated(
            preflight.estimated_peak_build_bytes,
            "checked preflight estimate including explicit buffers and conservative PtrHash headroom",
        )
    };
    work.global_build.peak_client_ram_bytes = Metric::not_applicable("build is server-side");
    work.global_build.client_upload_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.client_download_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.server_scans = Metric::not_measured("builder passes are not online scans");
    work.global_build.network_rounds = Metric::not_applicable("build is server-local");
    let mut setup = PhaseWork::unmeasured(
        "one cold client loading one immutable MPHF generation",
        "exact artifact size/load time are only available when the layout is built",
    );
    setup.client_time_ms = artifacts.map_or_else(
        || Metric::not_measured("MPHF client metadata was not materialized"),
        |artifacts| {
            Metric::measured(
                artifacts.cold_client_index_load_ms,
                "trusted parse and construction of the generation-bound client index",
            )
        },
    );
    setup.client_upload_bytes = Metric::deterministic(0, "public artifact fetch has no PIR upload");
    setup.client_download_bytes = artifacts.map_or_else(
        || {
            Metric::estimated(
                preflight.public_mphf_estimate_bytes,
                "four-bit-per-page preflight estimate",
            )
        },
        |artifacts| {
            Metric::measured(
                artifacts.public_mphf_bytes,
                "exact generation-bound public client artifact",
            )
        },
    );
    setup.network_rounds = Metric::estimated(1, "one authenticated metadata fetch assumed");
    work.per_client_setup = setup;
    work.maintenance = PhaseWork::not_applicable(
        "immutable generation lifetime",
        "changes publish a new generation rather than mutate queried ordinals",
    );
    for server in &mut work.online.per_server {
        server.server_time_p50_ms = timing.map_or_else(
            || Metric::not_measured("timing was blocked by execute mode or a resource guard"),
            |timing| {
                Metric::estimated(
                    millis(timing.aggregate_server_p50) / server_count as f64,
                    "aggregate server median divided evenly; per-server samples not retained",
                )
            },
        );
        server.logical_selected_bytes = Metric::estimated(
            expected_bytes_per_server,
            "uniform random selector shares select half of populated rows in expectation",
        );
        server.physical_or_scanned_bytes = Metric::not_measured("cache/DRAM traffic not measured");
        server.scans = Metric::deterministic(1, "one Dense selector traversal");
    }
    work.online.unit = "one exact MPHF inline projection lookup";
    work.online.aggregate_server_time_p50_ms = timing.map_or_else(
        || Metric::not_measured("timing was blocked by execute mode or a resource guard"),
        |timing| {
            Metric::measured(
                millis(timing.aggregate_server_p50),
                "sum of single-threaded elapsed times across concurrently executed replicas",
            )
        },
    );
    work.online.max_server_time_p50_ms = timing.map_or_else(
        || Metric::not_measured("timing was blocked by execute mode or a resource guard"),
        |timing| Metric::measured(millis(timing.wall_p50), "co-located replica wall median"),
    );
    work.online.aggregate_logical_selected_bytes = Metric::estimated(
        expected_bytes_all_servers,
        "sum of expected selected-row bytes across replicas",
    );
    work.online.aggregate_physical_or_scanned_bytes =
        Metric::not_measured("hardware counters absent");
    work.online.server_scans = Metric::deterministic(server_count, "one traversal per replica");
    work.online.network_rounds = Metric::deterministic(1, "all query shares sent in parallel");
    work.online.useful_result_bytes = Metric::deterministic(
        row_bytes - PAGE_FRAMING_BYTES,
        "one fixed-width inline projection value excluding page framing",
    );
    work.client.online_cpu_p50_ms = timing.map_or_else(
        || Metric::not_measured("client query/combine timing unavailable"),
        |timing| {
            Metric::measured(
                millis(timing.client_p50),
                "fresh share generation plus combine/decode",
            )
        },
    );
    work.client.peak_transient_ram_bytes = Metric::deterministic(
        total_upload_bytes
            .checked_add(total_download_bytes)
            .context("client transient byte count overflow")?,
        "query and answer buffers only; allocator/metadata state excluded",
    );
    work.client.persistent_state_bytes = artifacts.map_or_else(
        || {
            Metric::estimated(
                preflight.public_mphf_estimate_bytes,
                "four-bit-per-page MPHF preflight estimate",
            )
        },
        |artifacts| {
            Metric::measured(
                artifacts.public_mphf_bytes,
                "exact loaded generation-bound public MPHF artifact",
            )
        },
    );
    work.client.upload_bytes =
        Metric::deterministic(total_upload_bytes, "one Dense selector share per replica");
    work.client.download_bytes = Metric::deterministic(
        total_download_bytes,
        "one fixed-width answer share per replica",
    );
    let metadata_bytes = artifacts
        .map(|artifacts| artifacts.public_mphf_bytes)
        .unwrap_or(preflight.public_mphf_estimate_bytes);
    let storage_per_server = table_bytes
        .checked_add(metadata_bytes)
        .context("per-server persisted storage overflow")?;
    let aggregate_storage = checked_mul(storage_per_server, server_count, "aggregate storage")?;
    work.persisted_storage.server_bytes_per_server = if artifacts.is_some() {
        Metric::measured(
            storage_per_server,
            "compact Dense rows plus one exact public MPHF artifact copy per replica",
        )
    } else {
        Metric::estimated(
            storage_per_server,
            "compact Dense rows plus the preflight public MPHF estimate",
        )
    };
    work.persisted_storage.aggregate_server_bytes = if artifacts.is_some() {
        Metric::measured(
            aggregate_storage,
            "fully replicated Dense rows and public MPHF artifact",
        )
    } else {
        Metric::estimated(
            aggregate_storage,
            "fully replicated Dense rows plus preflight public MPHF estimates",
        )
    };
    work.persisted_storage.client_bytes = artifacts.map_or_else(
        || {
            Metric::estimated(
                preflight.public_mphf_estimate_bytes,
                "four-bit-per-page MPHF preflight estimate",
            )
        },
        |artifacts| {
            Metric::measured(
                artifacts.public_mphf_bytes,
                "exact generation-bound client metadata",
            )
        },
    );
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    Ok(work)
}

fn measure_topology(
    snapshot: &MphfPageSnapshot,
    target_tag: &[u8],
    expected_value: &[u8],
    ordinal: usize,
    server_count: usize,
    samples: usize,
) -> Result<TimingSummary> {
    let mut rng = StdRng::seed_from_u64(
        0x10_000_000 ^ snapshot.manifest.page_count as u64 ^ server_count as u64,
    );
    let warm_queries = dense::query_shares(
        ordinal,
        snapshot.manifest.page_count,
        server_count,
        &mut rng,
    )?;
    let warm_answers = evaluate_replicas(snapshot, &warm_queries)?;
    verify_answer(snapshot, target_tag, expected_value, warm_answers.answers)?;

    let mut timings = TimingSamples::default();
    for _ in 0..samples {
        let client_started = Instant::now();
        let queries = dense::query_shares(
            ordinal,
            snapshot.manifest.page_count,
            server_count,
            &mut rng,
        )?;
        let query_client = client_started.elapsed();
        let evaluated = evaluate_replicas(snapshot, &queries)?;
        let client_started = Instant::now();
        verify_answer(snapshot, target_tag, expected_value, evaluated.answers)?;
        timings.client.push(query_client + client_started.elapsed());
        timings
            .aggregate_server
            .push(evaluated.per_server.iter().copied().sum());
        timings.wall.push(evaluated.wall);
        timings
            .actual_logical_bytes
            .push(evaluated.actual_logical_bytes);
    }
    timings.summarize()
}

struct ReplicaEvaluation {
    answers: Vec<Vec<u8>>,
    per_server: Vec<Duration>,
    wall: Duration,
    actual_logical_bytes: usize,
}

fn evaluate_replicas(
    snapshot: &MphfPageSnapshot,
    query_shares: &[Vec<u8>],
) -> Result<ReplicaEvaluation> {
    let view = snapshot.view();
    let wall_started = Instant::now();
    let results = std::thread::scope(|scope| {
        query_shares
            .iter()
            .map(|query| {
                scope.spawn(move || {
                    let started = Instant::now();
                    dense::answer(view, query).map(|answer| (started.elapsed(), answer))
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .expect("production-scale Dense replica panicked")
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let selected_rows = query_shares
        .iter()
        .map(|query| selected_bits(query, snapshot.manifest.page_count))
        .sum::<usize>();
    Ok(ReplicaEvaluation {
        wall: wall_started.elapsed(),
        actual_logical_bytes: checked_mul(
            selected_rows,
            snapshot.manifest.page_size,
            "actual selected row bytes",
        )?,
        per_server: results.iter().map(|(elapsed, _)| *elapsed).collect(),
        answers: results.into_iter().map(|(_, answer)| answer).collect(),
    })
}

fn verify_answer(
    snapshot: &MphfPageSnapshot,
    target_tag: &[u8],
    expected_value: &[u8],
    answers: Vec<Vec<u8>>,
) -> Result<()> {
    let page = snapshot
        .decode_retrieved_page(&dense::combine(&answers)?, target_tag, 0)?
        .context("production-scale MPHF answer failed its page fingerprint")?;
    if page.total_pages != 1 || page.values.len() != 1 || page.values[0] != expected_value {
        bail!("production-scale MPHF recovered the wrong inline projection bytes");
    }
    Ok(())
}

fn validate_absent(snapshot: &MphfPageSnapshot) -> Result<()> {
    let absent = b"production-scale-absent-tag";
    let ordinal = snapshot.ordinal(absent, 0)?;
    if snapshot
        .decode_retrieved_page(snapshot.row(ordinal)?, absent, 0)?
        .is_some()
    {
        bail!("absent production-scale tag passed the 128-bit fingerprint");
    }
    Ok(())
}

fn benchmark_lookup<F, T>(samples: usize, mut lookup: F) -> Result<f64>
where
    F: FnMut() -> Result<T>,
{
    let runs = samples.max(7);
    let mut timings = Vec::with_capacity(runs);
    for _ in 0..runs {
        let started = Instant::now();
        std::hint::black_box(lookup()?);
        timings.push(started.elapsed());
    }
    timings.sort_unstable();
    Ok(micros(percentile(&timings, 50)))
}

impl TimingSamples {
    fn summarize(mut self) -> Result<TimingSummary> {
        if self.aggregate_server.is_empty()
            || self.wall.is_empty()
            || self.client.is_empty()
            || self.actual_logical_bytes.is_empty()
        {
            bail!("production-scale timing sample set is empty");
        }
        self.aggregate_server.sort_unstable();
        self.wall.sort_unstable();
        self.client.sort_unstable();
        self.actual_logical_bytes.sort_unstable();
        Ok(TimingSummary {
            aggregate_server_p50: percentile(&self.aggregate_server, 50),
            aggregate_server_p95: percentile(&self.aggregate_server, 95),
            wall_p50: percentile(&self.wall, 50),
            wall_p95: percentile(&self.wall, 95),
            client_p50: percentile(&self.client, 50),
            actual_logical_bytes_p50: percentile_usize(&self.actual_logical_bytes, 50),
        })
    }
}

fn timing_skip_reasons(
    config: &ProductionScaleConfig,
    preflight: &ProductionScalePreflight,
    row_bytes: usize,
    server_count: usize,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if !config.execute {
        reasons.push("mode is preflight; timing requires execute".into());
    }
    let upload = preflight
        .query_share_bytes_per_server
        .saturating_mul(server_count);
    if upload > config.max_client_upload_bytes {
        reasons.push(format!(
            "client upload {upload} exceeds {}-byte guard",
            config.max_client_upload_bytes
        ));
    }
    // This is deliberately a hard maximum rather than the half-selected-row
    // expectation: every share may contain all bits, and the gate charges the
    // untimed warm-up plus every reported sample.
    let logical = config
        .populated_tag_pages
        .checked_mul(row_bytes)
        .and_then(|bytes| bytes.checked_mul(server_count))
        .and_then(|bytes| bytes.checked_mul(config.samples.saturating_add(1)))
        .unwrap_or(usize::MAX);
    if logical > config.max_timed_aggregate_logical_bytes {
        reasons.push(format!(
            "expected aggregate logical work {logical} exceeds {}-byte timing guard",
            config.max_timed_aggregate_logical_bytes
        ));
    }
    reasons
}

fn selected_bits(query: &[u8], rows: usize) -> usize {
    query
        .iter()
        .enumerate()
        .map(|(byte_index, &byte)| {
            let remaining = rows.saturating_sub(byte_index * 8).min(8);
            let mask = if remaining == 8 {
                u8::MAX
            } else {
                ((1u16 << remaining) - 1) as u8
            };
            (byte & mask).count_ones() as usize
        })
        .sum()
}

fn validate_config(config: &ProductionScaleConfig) -> Result<()> {
    if config.populated_tag_pages < 10_000_000 {
        bail!("production-scale benchmark requires at least 10,000,000 populated tag pages");
    }
    if config.row_bytes.is_empty() {
        bail!("production-scale benchmark needs at least one row width");
    }
    if config.server_counts.is_empty()
        || config
            .server_counts
            .iter()
            .any(|servers| !SERVER_COUNTS.contains(servers))
    {
        bail!("production-scale Dense benchmark supports only two or three servers");
    }
    if config
        .server_counts
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .len()
        != config.server_counts.len()
    {
        bail!("production-scale server counts must not contain duplicates");
    }
    if config
        .row_bytes
        .iter()
        .any(|row_bytes| *row_bytes <= PAGE_FRAMING_BYTES)
    {
        bail!("production-scale row widths must exceed the 26-byte page framing");
    }
    if config
        .row_bytes
        .iter()
        .any(|row_bytes| row_bytes.saturating_sub(PAGE_FRAMING_BYTES) > MAX_INLINE_PROJECTION_BYTES)
    {
        bail!("production-scale inline projections must fit a 16-bit encoded length");
    }
    if config
        .row_bytes
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .len()
        != config.row_bytes.len()
    {
        bail!("production-scale row widths must not contain duplicates");
    }
    if config.max_table_bytes == 0
        || config.max_estimated_build_bytes == 0
        || config.max_timed_aggregate_logical_bytes == 0
        || config.max_client_upload_bytes == 0
        || config.samples == 0
    {
        bail!("production-scale guards and sample count must be non-zero");
    }
    if config.samples > MAX_TIMING_SAMPLES {
        bail!("production-scale sample count exceeds the bounded timing limit");
    }
    Ok(())
}

fn parse_mib(value: &str, name: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .with_context(|| format!("production-scale {name} must be an integer"))?
        .checked_mul(MIB)
        .with_context(|| format!("production-scale {name} overflows bytes"))
}

fn checked_mul(left: usize, right: usize, label: &str) -> Result<usize> {
    left.checked_mul(right)
        .with_context(|| format!("{label} size overflow"))
}

fn peak_estimate_note() -> &'static str {
    "Checked sum of EncodedPage headers, estimated 32-byte page-key allocations, encoded page rows, two u64 key vectors, ordered Dense rows, one-byte occupancy entries, two estimated serialized MPHF copies, and 32 bytes/page conservative PtrHash transient headroom. Excludes allocator metadata, runtime stacks/code, and unexposed PtrHash allocations beyond that headroom."
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

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quick_config(row_bytes: usize) -> ProductionScaleConfig {
        let mut config = ProductionScaleConfig::for_profile(Profile::Quick);
        config.row_bytes = vec![row_bytes];
        config
    }

    #[test]
    fn ten_million_page_preflight_charges_the_complete_timing_job() {
        let config = quick_config(32);
        let preflight = checked_preflight(&config, 32).unwrap();
        assert_eq!(preflight.table_bytes_per_server, 320_000_000);
        assert_eq!(preflight.query_share_bytes_per_server, 1_250_000);
        assert_eq!(preflight.maximum_client_upload_bytes, 3_750_000);
        assert_eq!(
            preflight.maximum_expected_aggregate_logical_bytes,
            480_000_000
        );
        assert_eq!(preflight.maximum_timing_job_logical_bytes, 3_840_000_000);
        assert!(preflight.table_guard_passed);
        assert!(preflight.build_guard_passed);
        assert!(preflight.upload_guard_passed);
        assert!(preflight.timing_work_guard_passed);
    }

    #[test]
    fn wider_layout_is_refused_by_the_default_build_and_timing_guards() {
        let config = quick_config(96);
        let preflight = checked_preflight(&config, 96).unwrap();
        assert_eq!(preflight.table_bytes_per_server, 960_000_000);
        assert!(!preflight.build_guard_passed);
        assert!(!preflight.timing_work_guard_passed);
    }

    #[test]
    fn analytical_two_and_three_server_work_is_explicit() {
        let config = quick_config(32);
        let preflight = checked_preflight(&config, 32).unwrap();
        let two = analytical_topology(&config, &preflight, 32, 2, None, None, Vec::new()).unwrap();
        let three =
            analytical_topology(&config, &preflight, 32, 3, None, None, Vec::new()).unwrap();
        assert_eq!(two.expected_logical_bytes_all_servers, 320_000_000);
        assert_eq!(three.expected_logical_bytes_all_servers, 480_000_000);
        assert_eq!(two.total_client_upload_bytes, 2_500_000);
        assert_eq!(three.total_client_upload_bytes, 3_750_000);
        assert_eq!(
            three.deterministic_selector_bits_examined_all_servers,
            30_000_000
        );
    }

    #[test]
    fn configuration_rejects_non_production_or_ambiguous_dimensions() {
        let mut config = quick_config(32);
        config.populated_tag_pages = 9_999_999;
        assert!(validate_config(&config).is_err());

        let mut config = quick_config(32);
        config.server_counts = vec![2, 4];
        assert!(validate_config(&config).is_err());

        let mut config = quick_config(32);
        config.row_bytes = vec![32, 32];
        assert!(validate_config(&config).is_err());

        let config = quick_config(PAGE_FRAMING_BYTES + MAX_INLINE_PROJECTION_BYTES + 1);
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn selected_bit_accounting_excludes_padding_bits() {
        assert_eq!(selected_bits(&[0xff, 0xff], 10), 10);
        assert_eq!(selected_bits(&[0b1111_1111, 0b1111_1100], 10), 8);
    }
}
