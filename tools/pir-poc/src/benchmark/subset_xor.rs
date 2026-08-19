use std::{
    collections::BTreeMap,
    io::{self, Write},
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
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
    snapshot::SnapshotView,
    subset_xor::{SubsetXorAnswer, SubsetXorEstimate, SubsetXorIndex},
    tag_pages::{benchmark_tag, TagPageConfig, TagPageSnapshot},
};

const DOCUMENT_COUNT: usize = 1 << 20;
const DISTINCT_TAG_COUNT: usize = 1 << 18;
const SERVER_COUNTS: [usize; 2] = [2, 3];
const CANDIDATE_BUCKETS: usize = 2;
const METHODOLOGY: &str = "The benchmark builds the existing fully populated 1M-document packed cuckoo tag-page corpus (four 16-byte locators per tag, four 96-byte pages per 384-byte PIR bucket row), then builds an immutable subset-XOR index over exactly those rows. Every production server would hold the same persisted index; the co-located benchmark shares one read-only allocation across its server threads and reports logical storage per replica and across two/three replicas. Each logical tag-page lookup privately retrieves both public cuckoo candidates using fresh n-out-of-n XOR shares. Dense and subset-XOR use the same u64-plus-tail answer-row XOR kernel, and both collect their logical counters in the answer pass. Co-located server threads contend for one memory system; wall time includes thread launch/join, while aggregate server time is the sum of measured answer time inside each server thread. Measured selector-derived row-read, row-XOR, data-read, upload, and download totals are summed over every server and both candidate buckets. Query generation, persistence I/O, HTTP, TLS, serialization, and network latency are excluded. Logical data-read bytes count only selected source/subset-row payloads, not selector bytes, cache-line overfetch, address computation, or other physical memory traffic; the implicit zero subset performs no index read or answer XOR.";
const METRIC_CLASSIFICATION: &str = "wall/aggregate server timings and post-build process RSS are measured; logical row/byte counts are exact counts from each sampled selector; expected non-zero reads and tracked peak allocation are analytical; persisted size is exactly verified for built indexes and estimated for allocation-guarded indexes. RSS is an after-build snapshot, not peak RSS, and includes allocator/runtime/corpus state.";
const THREAT_MODEL: &str = "Information-theoretic n-out-of-n XOR query privacy against any n-1 colluding semi-honest replicas. All n answers are required for reconstruction; this benchmark provides neither Byzantine answer integrity nor failure recovery.";
const IMMUTABLE_NOTE: &str = "Build beside the immutable source snapshot, persist and verify the index, then atomically publish both. A one-row patch would touch 2^(group_size-1) subset rows, but this POC deliberately rebuilds sealed snapshots so readers cannot observe a mixed source/index generation. A production manifest should bind the index to the source snapshot digest.";

#[derive(Debug, Serialize)]
pub struct SubsetXorBenchmarkReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: &'static str,
    pub metric_classification: &'static str,
    pub threat_model: &'static str,
    pub immutable_update_and_rebuild: &'static str,
    pub measured_samples_per_topology: usize,
    pub workload: SubsetXorWorkload,
    pub dense_baseline: Vec<DenseTopologyResult>,
    pub indexes: Vec<SubsetXorIndexResult>,
}

#[derive(Debug, Serialize)]
pub struct SubsetXorWorkload {
    pub document_count: usize,
    pub distinct_tag_count: usize,
    pub documents_per_tag: usize,
    pub encoded_page_count: usize,
    pub bucket_count: usize,
    pub row_size: usize,
    pub source_snapshot_bytes_per_server: usize,
    pub source_snapshot_build_ms: f64,
    pub source_snapshot_peak_tracked_bytes: usize,
    pub query_share_bytes_per_candidate_per_server: usize,
    pub candidate_bucket_queries_per_tag_page: usize,
    pub answer_bytes_per_candidate_per_server: usize,
}

#[derive(Debug, Serialize)]
pub struct DenseTopologyResult {
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
    pub logical_data_bytes_read_p50: usize,
    pub total_query_bytes_per_tag_page: usize,
    pub total_answer_bytes_per_tag_page: usize,
}

#[derive(Debug, Serialize)]
pub struct SubsetXorIndexResult {
    pub group_size: usize,
    pub built: bool,
    pub skip_reason: Option<String>,
    pub group_count: usize,
    pub stored_combination_rows: usize,
    pub index_data_bytes_per_server: usize,
    pub persisted_bytes_per_server: usize,
    pub index_only_persisted_storage_amplification: f64,
    pub persisted_source_plus_index_bytes_per_server: usize,
    pub source_plus_index_storage_amplification: f64,
    pub aggregate_persisted_bytes_two_servers: usize,
    pub aggregate_persisted_bytes_three_servers: usize,
    pub aggregate_source_plus_index_bytes_two_servers: usize,
    pub aggregate_source_plus_index_bytes_three_servers: usize,
    pub build_ms_per_replica: Option<f64>,
    pub peak_tracked_bytes_source_plus_one_index: usize,
    pub process_rss_bytes_after_build: Option<usize>,
    pub maximum_subset_rows_changed_by_one_source_row_update: usize,
    pub topologies: Vec<SubsetXorTopologyResult>,
}

#[derive(Debug, Serialize)]
pub struct SubsetXorTopologyResult {
    pub aggregate_work: AggregateWorkReport,
    pub server_count: usize,
    pub privacy_collusion_tolerance: usize,
    pub required_answers: usize,
    pub wall_p50_ms: f64,
    pub wall_p95_ms: f64,
    pub aggregate_server_p50_ms: f64,
    pub aggregate_server_p95_ms: f64,
    pub wall_speedup_vs_dense: f64,
    pub aggregate_server_speedup_vs_dense: f64,
    pub logical_row_reads_p50: usize,
    pub logical_row_xors_p50: usize,
    pub logical_data_bytes_read_p50: usize,
    pub analytical_expected_aggregate_nonzero_row_reads: f64,
    pub total_query_bytes_per_tag_page: usize,
    pub total_answer_bytes_per_tag_page: usize,
}

pub fn run(profile: Profile) -> Result<SubsetXorBenchmarkReport> {
    let config = TagPageConfig {
        bucket_capacity: 4,
        target_load_percent: 90,
        values_per_page: 4,
        max_value_bytes: 16,
    };
    let source_started = Instant::now();
    let snapshot = TagPageSnapshot::benchmark(DOCUMENT_COUNT, DISTINCT_TAG_COUNT, config)?;
    let source_snapshot_build_ms = millis(source_started.elapsed());
    let target_tag = benchmark_tag(DISTINCT_TAG_COUNT / 3);
    let buckets = snapshot.candidate_buckets(&target_tag, 0)?;
    let samples = match profile {
        Profile::Quick => 7,
        Profile::Full => 21,
    };

    let dense_baseline = SERVER_COUNTS
        .into_iter()
        .map(|server_count| benchmark_dense(&snapshot, &target_tag, buckets, server_count, samples))
        .collect::<Result<Vec<_>>>()?;
    let dense_by_servers = dense_baseline
        .iter()
        .map(|result| (result.server_count, result))
        .collect::<BTreeMap<_, _>>();

    // The exact corpus needs about 850 MiB for g=8 and about 2.7 GiB for g=10.
    // Exercise g=8 on the 8 GiB benchmark host, but leave g=10 analytical.
    let maximum_index_bytes = 1536 * 1024 * 1024;
    let mut indexes = Vec::new();
    for group_size in [2, 4, 6, 8, 10] {
        let estimate = SubsetXorIndex::estimate(snapshot.view(), group_size)?;
        if estimate.index_data_bytes > maximum_index_bytes {
            indexes.push(skipped_index(
                estimate,
                format!(
                    "estimated index is {} bytes, above the profile's {}-byte allocation guard",
                    estimate.index_data_bytes, maximum_index_bytes
                ),
            ));
            continue;
        }

        let build_started = Instant::now();
        let index =
            SubsetXorIndex::build_with_limit(snapshot.view(), group_size, maximum_index_bytes)?;
        let build_ms = millis(build_started.elapsed());
        verify_persisted_size(&index)?;
        let process_rss_bytes_after_build =
            memory_stats::memory_stats().map(|stats| stats.physical_mem);
        let topologies = SERVER_COUNTS
            .into_iter()
            .map(|server_count| {
                benchmark_index(
                    &snapshot,
                    &target_tag,
                    buckets,
                    &index,
                    server_count,
                    samples,
                    dense_by_servers[&server_count],
                )
            })
            .collect::<Result<Vec<_>>>()?;
        indexes.push(SubsetXorIndexResult {
            group_size,
            built: true,
            skip_reason: None,
            group_count: index.group_count(),
            stored_combination_rows: index.stored_combination_rows(),
            index_data_bytes_per_server: index.index_data_bytes(),
            persisted_bytes_per_server: index.persisted_bytes(),
            index_only_persisted_storage_amplification: index.storage_amplification(),
            persisted_source_plus_index_bytes_per_server: snapshot.rows().len()
                + index.persisted_bytes(),
            source_plus_index_storage_amplification: 1.0 + index.storage_amplification(),
            aggregate_persisted_bytes_two_servers: index.persisted_bytes() * 2,
            aggregate_persisted_bytes_three_servers: index.persisted_bytes() * 3,
            aggregate_source_plus_index_bytes_two_servers: (snapshot.rows().len()
                + index.persisted_bytes())
                * 2,
            aggregate_source_plus_index_bytes_three_servers: (snapshot.rows().len()
                + index.persisted_bytes())
                * 3,
            build_ms_per_replica: Some(build_ms),
            peak_tracked_bytes_source_plus_one_index: estimate.peak_tracked_bytes,
            process_rss_bytes_after_build,
            maximum_subset_rows_changed_by_one_source_row_update: 1usize << (group_size - 1),
            topologies,
        });
    }

    Ok(SubsetXorBenchmarkReport {
        protocol: "bim-subset-xor-over-packed-dense",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: METHODOLOGY,
        metric_classification: METRIC_CLASSIFICATION,
        threat_model: THREAT_MODEL,
        immutable_update_and_rebuild: IMMUTABLE_NOTE,
        measured_samples_per_topology: samples,
        workload: SubsetXorWorkload {
            document_count: DOCUMENT_COUNT,
            distinct_tag_count: DISTINCT_TAG_COUNT,
            documents_per_tag: DOCUMENT_COUNT / DISTINCT_TAG_COUNT,
            encoded_page_count: snapshot.manifest.page_count,
            bucket_count: snapshot.manifest.bucket_count,
            row_size: snapshot.manifest.row_size,
            source_snapshot_bytes_per_server: snapshot.rows().len(),
            source_snapshot_build_ms,
            source_snapshot_peak_tracked_bytes: snapshot.build_metrics.peak_tracked_bytes,
            query_share_bytes_per_candidate_per_server: dense::query_size(
                snapshot.manifest.bucket_count,
            ),
            candidate_bucket_queries_per_tag_page: CANDIDATE_BUCKETS,
            answer_bytes_per_candidate_per_server: snapshot.manifest.row_size,
        },
        dense_baseline,
        indexes,
    })
}

fn skipped_index(estimate: SubsetXorEstimate, reason: String) -> SubsetXorIndexResult {
    SubsetXorIndexResult {
        group_size: estimate.group_size,
        built: false,
        skip_reason: Some(reason),
        group_count: estimate.group_count,
        stored_combination_rows: estimate.stored_combination_rows,
        index_data_bytes_per_server: estimate.index_data_bytes,
        persisted_bytes_per_server: estimate.persisted_bytes,
        index_only_persisted_storage_amplification: estimate.storage_amplification(),
        persisted_source_plus_index_bytes_per_server: estimate.bucket_count * estimate.row_size
            + estimate.persisted_bytes,
        source_plus_index_storage_amplification: 1.0 + estimate.storage_amplification(),
        aggregate_persisted_bytes_two_servers: estimate.persisted_bytes * 2,
        aggregate_persisted_bytes_three_servers: estimate.persisted_bytes * 3,
        aggregate_source_plus_index_bytes_two_servers: (estimate.bucket_count * estimate.row_size
            + estimate.persisted_bytes)
            * 2,
        aggregate_source_plus_index_bytes_three_servers: (estimate.bucket_count
            * estimate.row_size
            + estimate.persisted_bytes)
            * 3,
        build_ms_per_replica: None,
        peak_tracked_bytes_source_plus_one_index: estimate.peak_tracked_bytes,
        process_rss_bytes_after_build: None,
        maximum_subset_rows_changed_by_one_source_row_update: 1usize << (estimate.group_size - 1),
        topologies: Vec::new(),
    }
}

fn benchmark_dense(
    snapshot: &TagPageSnapshot,
    target_tag: &[u8],
    buckets: [usize; CANDIDATE_BUCKETS],
    server_count: usize,
    samples: usize,
) -> Result<DenseTopologyResult> {
    let mut rng = StdRng::seed_from_u64(0xd35e_0000 | server_count as u64);
    let warm_queries = generate_queries(
        buckets,
        snapshot.manifest.bucket_count,
        server_count,
        &mut rng,
    )?;
    let warm = evaluate_dense(snapshot, &warm_queries)?;
    verify_answers(snapshot, target_tag, buckets, &warm.answers)?;

    let mut measurements = Vec::with_capacity(samples);
    for _ in 0..samples {
        let queries = generate_queries(
            buckets,
            snapshot.manifest.bucket_count,
            server_count,
            &mut rng,
        )?;
        let evaluation = evaluate_dense(snapshot, &queries)?;
        verify_answers(snapshot, target_tag, buckets, &evaluation.answers)?;
        measurements.push(evaluation);
    }
    let summary = summarize(&measurements);
    let total_query_bytes =
        dense::query_size(snapshot.manifest.bucket_count) * server_count * CANDIDATE_BUCKETS;
    let total_answer_bytes = snapshot.manifest.row_size * server_count * CANDIDATE_BUCKETS;
    let aggregate_work = topology_accounting(
        "packed-cuckoo-dense-xor",
        server_count,
        snapshot.rows().len(),
        snapshot.build_metrics.peak_tracked_bytes,
        millis(summary.wall_p50),
        millis(summary.aggregate_p50),
        summary.data_bytes_p50,
        total_query_bytes,
        total_answer_bytes,
        snapshot.manifest.values_per_page * snapshot.manifest.max_value_bytes,
    )?;
    Ok(DenseTopologyResult {
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
        logical_data_bytes_read_p50: summary.data_bytes_p50,
        total_query_bytes_per_tag_page: total_query_bytes,
        total_answer_bytes_per_tag_page: total_answer_bytes,
    })
}

fn benchmark_index(
    snapshot: &TagPageSnapshot,
    target_tag: &[u8],
    buckets: [usize; CANDIDATE_BUCKETS],
    index: &SubsetXorIndex,
    server_count: usize,
    samples: usize,
    dense: &DenseTopologyResult,
) -> Result<SubsetXorTopologyResult> {
    let mut rng = StdRng::seed_from_u64(
        0x5ab5_0000 | ((index.group_size() as u64) << 8) | server_count as u64,
    );
    let warm_queries = generate_queries(
        buckets,
        snapshot.manifest.bucket_count,
        server_count,
        &mut rng,
    )?;
    let warm = evaluate_index(index, &warm_queries)?;
    verify_answers(snapshot, target_tag, buckets, &warm.answers)?;

    let mut measurements = Vec::with_capacity(samples);
    for _ in 0..samples {
        let queries = generate_queries(
            buckets,
            snapshot.manifest.bucket_count,
            server_count,
            &mut rng,
        )?;
        let evaluation = evaluate_index(index, &queries)?;
        verify_answers(snapshot, target_tag, buckets, &evaluation.answers)?;
        measurements.push(evaluation);
    }
    let summary = summarize(&measurements);
    let wall_ms = millis(summary.wall_p50);
    let aggregate_ms = millis(summary.aggregate_p50);
    let total_query_bytes =
        dense::query_size(snapshot.manifest.bucket_count) * server_count * CANDIDATE_BUCKETS;
    let total_answer_bytes = snapshot.manifest.row_size * server_count * CANDIDATE_BUCKETS;
    let aggregate_work = topology_accounting(
        group_protocol(index.group_size()),
        server_count,
        snapshot.rows().len() + index.persisted_bytes(),
        snapshot
            .build_metrics
            .peak_tracked_bytes
            .max(snapshot.rows().len() + index.index_data_bytes()),
        wall_ms,
        aggregate_ms,
        summary.data_bytes_p50,
        total_query_bytes,
        total_answer_bytes,
        snapshot.manifest.values_per_page * snapshot.manifest.max_value_bytes,
    )?;
    Ok(SubsetXorTopologyResult {
        aggregate_work,
        server_count,
        privacy_collusion_tolerance: server_count - 1,
        required_answers: server_count,
        wall_p50_ms: wall_ms,
        wall_p95_ms: millis(summary.wall_p95),
        aggregate_server_p50_ms: aggregate_ms,
        aggregate_server_p95_ms: millis(summary.aggregate_p95),
        wall_speedup_vs_dense: dense.wall_p50_ms / wall_ms,
        aggregate_server_speedup_vs_dense: dense.aggregate_server_p50_ms / aggregate_ms,
        logical_row_reads_p50: summary.row_reads_p50,
        logical_row_xors_p50: summary.row_xors_p50,
        logical_data_bytes_read_p50: summary.data_bytes_p50,
        analytical_expected_aggregate_nonzero_row_reads: expected_nonzero_reads(index)
            * server_count as f64
            * CANDIDATE_BUCKETS as f64,
        total_query_bytes_per_tag_page: total_query_bytes,
        total_answer_bytes_per_tag_page: total_answer_bytes,
    })
}

fn group_protocol(group_size: usize) -> &'static str {
    match group_size {
        2 => "packed-cuckoo-subset-xor-g2",
        4 => "packed-cuckoo-subset-xor-g4",
        6 => "packed-cuckoo-subset-xor-g6",
        8 => "packed-cuckoo-subset-xor-g8",
        10 => "packed-cuckoo-subset-xor-g10",
        _ => "packed-cuckoo-subset-xor",
    }
}

#[allow(clippy::too_many_arguments)]
fn topology_accounting(
    protocol: &'static str,
    server_count: usize,
    persisted_bytes_per_server: usize,
    tracked_peak_build_bytes: usize,
    wall_p50_ms: f64,
    aggregate_server_p50_ms: f64,
    aggregate_logical_selected_bytes: usize,
    total_query_bytes: usize,
    total_answer_bytes: usize,
    useful_result_bytes: usize,
) -> Result<AggregateWorkReport> {
    let mut work = AggregateWorkReport::new(
        protocol,
        ComparisonScope {
            workload: "one lookup over the identical populated immutable packed-cuckoo tag-page corpus",
            result: "one first page containing four fixed-width compact locators",
            public_partition: "global immutable snapshot; cuckoo dimensions and table seed are public",
            leakage: LeakageScope::ExactQueryPrivacy,
        },
        SecurityLabels {
            privacy: "information-theoretic n-out-of-n XOR query privacy",
            server_count,
            collusion_tolerance: server_count - 1,
            required_answers: server_count,
            assumptions: "at least one replica does not collude; every replica serves the same authenticated immutable generation",
            availability: "all answer shares are required",
            integrity: "the 128-bit page fingerprint rejects a wrong row with high probability but is not a malicious-server proof or MAC",
        },
    );
    work.global_build.aggregate_server_time_ms = Metric::not_measured(
        "source/index build wall times are reported by the enclosing result; aggregate builder CPU time was not measured",
    );
    work.global_build.client_time_ms = Metric::not_applicable("build is server-side");
    work.global_build.logical_selected_bytes =
        Metric::not_measured("builder reads and writes were not instrumented");
    work.global_build.physical_or_scanned_bytes =
        Metric::not_measured("builder hardware byte counters were not collected");
    work.global_build.peak_server_ram_bytes = Metric::estimated(
        tracked_peak_build_bytes,
        "analytical/tracked algorithm-owned buffers; allocator and runtime overhead are excluded",
    );
    work.global_build.peak_client_ram_bytes = Metric::not_applicable("build is server-side");
    work.global_build.client_upload_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.client_download_bytes = Metric::deterministic(0, "build is server-local");
    work.global_build.server_scans =
        Metric::not_measured("layout construction passes were not instrumented");
    work.global_build.network_rounds = Metric::not_applicable("build is server-local");
    work.per_client_setup = PhaseWork::unmeasured(
        "one client loading one immutable generation",
        "the small public cuckoo manifest distribution and parsing costs were not measured",
    );
    work.per_client_setup.client_upload_bytes =
        Metric::deterministic(0, "public metadata fetch has no PIR upload");
    work.maintenance = PhaseWork::not_applicable(
        "immutable snapshot lifetime",
        "updates build and atomically publish a new immutable source/index generation",
    );

    let average_logical_bytes_per_server =
        aggregate_logical_selected_bytes as f64 / server_count as f64;
    for server in &mut work.online.per_server {
        server.server_time_p50_ms = Metric::estimated(
            aggregate_server_p50_ms / server_count as f64,
            "sum-server p50 divided evenly; individual server samples were not retained",
        );
        server.logical_selected_bytes = Metric::estimated(
            average_logical_bytes_per_server.round() as usize,
            "average of the measured aggregate selected-row bytes across replicas",
        );
        server.physical_or_scanned_bytes =
            Metric::not_measured("cache-line, DRAM, and storage traffic require hardware counters");
        server.scans = Metric::deterministic(
            CANDIDATE_BUCKETS,
            "one selector traversal for each public cuckoo candidate",
        );
    }
    work.online.unit = "one first-page tag lookup";
    work.online.aggregate_server_time_p50_ms = Metric::measured(
        aggregate_server_p50_ms,
        "sum of measured server elapsed times for the co-located topology",
    );
    work.online.max_server_time_p50_ms = Metric::estimated(
        wall_p50_ms,
        "co-located wall p50 is an upper-envelope proxy that includes thread dispatch/join",
    );
    work.online.aggregate_logical_selected_bytes = Metric::measured(
        aggregate_logical_selected_bytes,
        "median exact selected-row payload count across every server and both candidates",
    );
    work.online.aggregate_physical_or_scanned_bytes = Metric::not_measured(
        "logical payload bytes are not substituted for cache-line or DRAM traffic",
    );
    work.online.server_scans = Metric::deterministic(
        server_count * CANDIDATE_BUCKETS,
        "two candidate selector traversals on every replicated server",
    );
    work.online.network_rounds = Metric::deterministic(1, "all shares are sent in parallel");
    work.online.useful_result_bytes = Metric::deterministic(
        useful_result_bytes,
        "four fixed-width locators, excluding private page framing and false candidate",
    );
    work.client.online_cpu_p50_ms = Metric::not_measured(
        "query generation and reconstruction were excluded from this server-work benchmark",
    );
    work.client.peak_transient_ram_bytes =
        Metric::not_measured("client process peak RAM was not sampled");
    work.client.persistent_state_bytes = Metric::not_measured(
        "the small generation-specific cuckoo manifest was not serialized separately",
    );
    work.client.upload_bytes = Metric::deterministic(
        total_query_bytes,
        "one fresh Dense selector share per server for each of two candidates",
    );
    work.client.download_bytes = Metric::deterministic(
        total_answer_bytes,
        "one fixed-width answer share per server for each of two candidates",
    );
    work.persisted_storage.server_bytes_per_server = Metric::deterministic(
        persisted_bytes_per_server,
        "source rows plus the persisted subset index when applicable; manifest bytes are excluded",
    );
    work.persisted_storage.aggregate_server_bytes = Metric::deterministic(
        persisted_bytes_per_server * server_count,
        "sum of identical persisted replicas across all servers",
    );
    work.persisted_storage.client_bytes = Metric::not_measured(
        "the small generation-specific cuckoo manifest was not serialized separately",
    );
    work.amortization = AmortizationHorizon {
        global_build: "all clients and lookups using one immutable generation",
        per_client_setup: "all lookups by one client before generation refresh",
        maintenance: "not applicable; updates create a new immutable generation",
        assumed_global_queries: None,
        assumed_queries_per_client_setup: None,
        assumed_online_events_per_maintenance: None,
        note: "Build and setup remain separate from online work; no amortization denominator is assumed.",
    };
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    Ok(work)
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

fn generate_queries(
    buckets: [usize; CANDIDATE_BUCKETS],
    bucket_count: usize,
    server_count: usize,
    rng: &mut StdRng,
) -> Result<Vec<Vec<Vec<u8>>>> {
    let mut per_server = (0..server_count)
        .map(|_| Vec::with_capacity(CANDIDATE_BUCKETS))
        .collect::<Vec<_>>();
    for bucket in buckets {
        for (server_queries, share) in per_server.iter_mut().zip(dense::query_shares(
            bucket,
            bucket_count,
            server_count,
            rng,
        )?) {
            server_queries.push(share);
        }
    }
    Ok(per_server)
}

fn evaluate_dense(
    snapshot: &TagPageSnapshot,
    per_server_queries: &[Vec<Vec<u8>>],
) -> Result<Evaluation> {
    std::thread::scope(|scope| {
        let wall_started = Instant::now();
        let handles = per_server_queries
            .iter()
            .map(|queries| {
                scope.spawn(move || -> Result<ServerEvaluation> {
                    let started = Instant::now();
                    let answers = queries
                        .iter()
                        .map(|query| dense_answer_with_metrics(snapshot.view(), query))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(ServerEvaluation {
                        elapsed: started.elapsed(),
                        answers,
                    })
                })
            })
            .collect::<Vec<_>>();
        finish_evaluation(handles, wall_started)
    })
}

fn evaluate_index(
    index: &SubsetXorIndex,
    per_server_queries: &[Vec<Vec<u8>>],
) -> Result<Evaluation> {
    std::thread::scope(|scope| {
        let wall_started = Instant::now();
        let handles = per_server_queries
            .iter()
            .map(|queries| {
                scope.spawn(move || -> Result<ServerEvaluation> {
                    let started = Instant::now();
                    let answers = queries
                        .iter()
                        .map(|query| index.answer_with_metrics(query))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(ServerEvaluation {
                        elapsed: started.elapsed(),
                        answers,
                    })
                })
            })
            .collect::<Vec<_>>();
        finish_evaluation(handles, wall_started)
    })
}

fn finish_evaluation<'scope>(
    handles: Vec<std::thread::ScopedJoinHandle<'scope, Result<ServerEvaluation>>>,
    wall_started: Instant,
) -> Result<Evaluation> {
    let servers = handles
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("subset-XOR server thread panicked"))?
        })
        .collect::<Result<Vec<_>>>()?;
    let wall = wall_started.elapsed();
    let aggregate_server = servers.iter().map(|server| server.elapsed).sum();
    let mut row_reads = 0;
    let mut row_xors = 0;
    let mut data_bytes = 0;
    let answers = servers
        .into_iter()
        .map(|server| {
            server
                .answers
                .into_iter()
                .map(|answer| {
                    row_reads += answer.metrics.logical_row_reads;
                    row_xors += answer.metrics.logical_row_xors;
                    data_bytes += answer.metrics.logical_data_bytes_read;
                    answer.bytes
                })
                .collect::<Vec<_>>()
        })
        .collect();
    Ok(Evaluation {
        wall,
        aggregate_server,
        row_reads,
        row_xors,
        data_bytes,
        answers,
    })
}

fn verify_answers(
    snapshot: &TagPageSnapshot,
    target_tag: &[u8],
    buckets: [usize; CANDIDATE_BUCKETS],
    answers: &[Vec<Vec<u8>>],
) -> Result<()> {
    if answers.len() < 2 {
        bail!("subset-XOR verification requires at least two server answers");
    }
    let mut matched_page = false;
    for candidate in 0..CANDIDATE_BUCKETS {
        let shares = answers
            .iter()
            .map(|server| server[candidate].as_slice())
            .collect::<Vec<_>>();
        let recovered = dense::combine(&shares)?;
        if recovered != snapshot.view().row(buckets[candidate])? {
            bail!("subset-XOR reconstructed the wrong packed bucket");
        }
        if snapshot
            .decode_bucket_row(&recovered, target_tag, 0)?
            .is_some()
        {
            matched_page = true;
        }
    }
    if !matched_page {
        bail!("subset-XOR lookup did not recover the target tag page");
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
                benchmark_xor_row(&mut bytes, snapshot.row(bucket)?);
                logical_row_reads += 1;
            }
            selected &= selected - 1;
        }
    }
    Ok(SubsetXorAnswer {
        bytes,
        metrics: crate::subset_xor::SubsetXorAnswerMetrics {
            logical_row_reads,
            logical_row_xors: logical_row_reads,
            logical_data_bytes_read: logical_row_reads * snapshot.row_size,
        },
    })
}

#[inline(always)]
fn benchmark_xor_row(output: &mut [u8], row: &[u8]) {
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

fn summarize(measurements: &[Evaluation]) -> Summary {
    let mut wall = measurements
        .iter()
        .map(|measurement| measurement.wall)
        .collect::<Vec<_>>();
    let mut aggregate = measurements
        .iter()
        .map(|measurement| measurement.aggregate_server)
        .collect::<Vec<_>>();
    let mut row_reads = measurements
        .iter()
        .map(|measurement| measurement.row_reads)
        .collect::<Vec<_>>();
    let mut row_xors = measurements
        .iter()
        .map(|measurement| measurement.row_xors)
        .collect::<Vec<_>>();
    let mut data_bytes = measurements
        .iter()
        .map(|measurement| measurement.data_bytes)
        .collect::<Vec<_>>();
    wall.sort_unstable();
    aggregate.sort_unstable();
    row_reads.sort_unstable();
    row_xors.sort_unstable();
    data_bytes.sort_unstable();
    Summary {
        wall_p50: percentile(&wall, 50),
        wall_p95: percentile(&wall, 95),
        aggregate_p50: percentile(&aggregate, 50),
        aggregate_p95: percentile(&aggregate, 95),
        row_reads_p50: percentile(&row_reads, 50),
        row_xors_p50: percentile(&row_xors, 50),
        data_bytes_p50: percentile(&data_bytes, 50),
    }
}

fn percentile<T: Copy>(values: &[T], percentile: usize) -> T {
    values[((values.len() - 1) * percentile).div_ceil(100)]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn verify_persisted_size(index: &SubsetXorIndex) -> Result<()> {
    let mut writer = CountingWriter::default();
    index.write_to(&mut writer)?;
    if writer.bytes != index.persisted_bytes() {
        bail!(
            "subset-XOR persisted {} bytes, expected {}",
            writer.bytes,
            index.persisted_bytes()
        );
    }
    Ok(())
}

#[derive(Default)]
struct CountingWriter {
    bytes: usize,
}

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("persisted byte count overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct ServerEvaluation {
    elapsed: Duration,
    answers: Vec<SubsetXorAnswer>,
}

struct Evaluation {
    wall: Duration,
    aggregate_server: Duration,
    row_reads: usize,
    row_xors: usize,
    data_bytes: usize,
    answers: Vec<Vec<Vec<u8>>>,
}

struct Summary {
    wall_p50: Duration,
    wall_p95: Duration,
    aggregate_p50: Duration,
    aggregate_p95: Duration,
    row_reads_p50: usize,
    row_xors_p50: usize,
    data_bytes_p50: usize,
}

#[cfg(test)]
mod tests {
    use rand::RngCore;

    use super::*;

    #[test]
    fn counting_writer_matches_fragmented_writes() {
        let mut writer = CountingWriter::default();
        writer.write_all(&[1, 2, 3]).unwrap();
        writer.write_all(&[4, 5]).unwrap();
        assert_eq!(writer.bytes, 5);
    }

    #[test]
    fn fair_dense_kernel_matches_reference_for_arbitrary_row_widths() {
        let mut rng = StdRng::seed_from_u64(0xd35e_fa17);
        for (bucket_count, row_size) in [(3, 1), (17, 13), (33, 65)] {
            let mut rows = vec![0u8; bucket_count * row_size];
            rng.fill_bytes(&mut rows);
            let snapshot = SnapshotView::new(&rows, bucket_count, row_size);
            let mut query = vec![0u8; dense::query_size(bucket_count)];
            rng.fill_bytes(&mut query);
            assert_eq!(
                dense_answer_with_metrics(snapshot, &query).unwrap().bytes,
                dense::answer(snapshot, &query).unwrap()
            );
        }
    }
}
