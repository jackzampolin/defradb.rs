use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rand::{rngs::StdRng, SeedableRng};

use super::accounting::{
    unavailable_hardware_counters, AggregateWorkReport, AmortizationHorizon, ComparisonScope,
    LeakageScope, Metric, PhaseWork, SecurityLabels,
};
use super::{
    report::{
        FuseComparisonReport, FuseComparisonWorkload, FuseLayoutResult, FuseTopologyResult,
        RaidPirAssessment, RaidPirConfiguration,
    },
    Profile,
};
use crate::{
    dense::{self, ParallelEvaluator},
    fuse_pages::{FuseArity, FusePageSnapshot},
    snapshot::SnapshotView,
    tag_pages::{benchmark_page_set, benchmark_tag, TagPageConfig, TagPageSnapshot},
};

const SERVER_COUNTS: [usize; 2] = [2, 3];
const SERVER_WORKERS: usize = 2;
const CUCKOO_CANDIDATES: usize = 2;
const METHODOLOGY: &str = "All layouts consume the same pre-encoded, fully populated immutable tag-page corpus: four 16-byte locators per tag and one selected page. Packed cuckoo performs two independent Dense XOR evaluations; Fuse performs one multi-hot Dense XOR evaluation selecting three or four cells. Every topology is warmed once, then timed with fresh cryptographic shares and correctness-checked reconstruction. Servers are co-located, each owns a persistent two-worker evaluator, and contend for one memory bus. Timings exclude HTTP, TLS, serialization, and network latency. Peak build memory is deterministic algorithm-owned memory (corpus, output and temporary vectors), excluding allocator/runtime overhead.";
const PEAK_MEMORY_NOTE: &str = "Deterministic peak of algorithm-owned buffers, including the common encoded corpus and final table; excludes allocator metadata, code, thread stacks, and runtime RSS.";

pub fn run(profile: Profile) -> Result<FuseComparisonReport> {
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

    let cuckoo_started = Instant::now();
    let cuckoo = TagPageSnapshot::from_page_set(&page_set, config.clone())?;
    let cuckoo_build_ms = millis(cuckoo_started.elapsed());
    let mut layouts = vec![benchmark_cuckoo(
        &cuckoo,
        &target_tag,
        profile,
        encoded_payload_bytes,
        cuckoo_build_ms,
        encoded_corpus_build_ms + cuckoo_build_ms,
    )?];
    drop(cuckoo);

    for arity in [FuseArity::Three, FuseArity::Four] {
        let build_started = Instant::now();
        let snapshot = FusePageSnapshot::from_page_set(&page_set, config.clone(), arity)?;
        let build_ms = millis(build_started.elapsed());
        layouts.push(benchmark_fuse(
            &snapshot,
            &target_tag,
            profile,
            encoded_payload_bytes,
            build_ms,
            encoded_corpus_build_ms + build_ms,
            arity,
        )?);
    }

    let fuse_4_table_bytes = layouts
        .iter()
        .find(|layout| layout.layout == "fuse-4")
        .context("Fuse-4 result is missing")?
        .table_bytes_per_server;
    Ok(FuseComparisonReport {
        protocol: "static-fuse-retrieval-over-replicated-dense-xor",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: METHODOLOGY,
        workload: FuseComparisonWorkload {
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
        layouts,
        raid_pir: RaidPirAssessment {
            reference: "Demmler, Herzberg, and Schneider, RAID-PIR (CCSW 2014), https://encrypto.de/papers/DHS14.pdf",
            status: "evaluated separately after the layout comparison; not implemented",
            scope: "RAID-PIR is a database-distribution protocol, not a Fuse placement variant. It would shard encoded data across servers instead of replicating this Dense XOR table on every server.",
            potential_benefit: "With k servers and redundancy r, each server stores and queries r/k of the table while privacy tolerates at most r-1 colluding servers. It helps only when k is larger than the desired collusion threshold plus one.",
            incompatibility: "The current n-share Dense XOR proof and evaluator require every server to hold the same rows. RAID-PIR needs a new query/answer protocol and cannot be obtained by simply striping these rows.",
            availability_note: "The paper explicitly says its constructions are not robust to server failures. It describes higher-layer recovery and accountability, but that is not one-server failure tolerance in the base protocol.",
            recommendation: "Do not implement RAID-PIR for the current 2- or 3-server, any-one-honest privacy target: r must equal k, so every server still stores the complete table. Revisit it only with at least four servers or a weaker collusion target, as a separate protocol experiment.",
            configurations: raid_configurations(fuse_4_table_bytes),
        },
    })
}

fn raid_configurations(fuse_4_table_bytes: usize) -> Vec<RaidPirConfiguration> {
    [(3, 2), (3, 3), (4, 2), (4, 3), (5, 3)]
        .into_iter()
        .map(|(server_count_k, redundancy_r)| {
            let fraction = redundancy_r as f64 / server_count_k as f64;
            RaidPirConfiguration {
                server_count_k,
                redundancy_r,
                maximum_colluding_servers: redundancy_r - 1,
                table_fraction_per_server: fraction,
                query_fraction_per_server: fraction,
                fuse_4_table_bytes_per_server: (fuse_4_table_bytes * redundancy_r)
                    .div_ceil(server_count_k),
                note: if server_count_k == redundancy_r {
                    "maximum collusion tolerance for this server count; no per-server distribution saving"
                } else {
                    "lower per-server storage/work, paid for with more servers than the collusion threshold requires"
                },
            }
        })
        .collect()
}

fn benchmark_cuckoo(
    snapshot: &TagPageSnapshot,
    target_tag: &[u8],
    profile: Profile,
    encoded_payload_bytes: usize,
    build_ms: f64,
    global_build_ms: f64,
) -> Result<FuseLayoutResult> {
    let buckets = snapshot.candidate_buckets(target_tag, 0)?;
    let topologies = SERVER_COUNTS
        .into_iter()
        .map(|server_count| {
            benchmark_cuckoo_topology(
                snapshot,
                target_tag,
                buckets,
                server_count,
                profile,
                global_build_ms,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(FuseLayoutResult {
        layout: "packed-cuckoo",
        retrieval_cells_or_candidates: CUCKOO_CANDIDATES,
        table_rows: snapshot.manifest.bucket_count,
        row_bytes: snapshot.manifest.row_size,
        table_bytes_per_server: snapshot.rows().len(),
        storage_expansion_vs_encoded_pages: snapshot.rows().len() as f64
            / encoded_payload_bytes as f64,
        cold_client_metadata_bytes: snapshot.manifest.client_metadata_bytes(),
        layout_build_ms: build_ms,
        build_attempts: snapshot.build_metrics.attempts,
        peak_tracked_build_bytes: snapshot.build_metrics.peak_tracked_bytes,
        peak_build_memory_note: PEAK_MEMORY_NOTE,
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
    arity: FuseArity,
) -> Result<FuseLayoutResult> {
    let cells = snapshot.cells(target_tag, 0)?;
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
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(FuseLayoutResult {
        layout: arity.label(),
        retrieval_cells_or_candidates: arity.value(),
        table_rows: snapshot.manifest.cell_count,
        row_bytes: snapshot.manifest.page_size,
        table_bytes_per_server: snapshot.rows().len(),
        storage_expansion_vs_encoded_pages: snapshot.rows().len() as f64
            / encoded_payload_bytes as f64,
        cold_client_metadata_bytes: snapshot.manifest.client_metadata_bytes(),
        layout_build_ms: build_ms,
        build_attempts: snapshot.build_metrics.attempts,
        peak_tracked_build_bytes: snapshot.build_metrics.peak_tracked_bytes,
        peak_build_memory_note: PEAK_MEMORY_NOTE,
        topologies,
    })
}

fn benchmark_cuckoo_topology(
    snapshot: &TagPageSnapshot,
    target_tag: &[u8],
    buckets: [usize; 2],
    server_count: usize,
    profile: Profile,
    build_ms: f64,
) -> Result<FuseTopologyResult> {
    let evaluators = evaluators(server_count)?;
    let mut rng = StdRng::seed_from_u64(0xc0c0_0000 ^ server_count as u64);
    let warmup = cuckoo_queries(
        buckets,
        snapshot.manifest.bucket_count,
        server_count,
        &mut rng,
    )?;
    let warmup = evaluate(
        snapshot.rows(),
        snapshot.manifest.bucket_count,
        snapshot.manifest.row_size,
        &evaluators,
        &warmup,
    )?;
    reconstruct_cuckoo(snapshot, target_tag, warmup.answers)?;

    let samples = sample_count(profile);
    let mut query_generation = Vec::with_capacity(samples);
    let mut wall = Vec::with_capacity(samples);
    let mut server_elapsed = Vec::with_capacity(samples);
    let mut reconstruct = Vec::with_capacity(samples);
    for _ in 0..samples {
        let query_started = Instant::now();
        let queries = cuckoo_queries(
            buckets,
            snapshot.manifest.bucket_count,
            server_count,
            &mut rng,
        )?;
        query_generation.push(query_started.elapsed());
        let evaluated = evaluate(
            snapshot.rows(),
            snapshot.manifest.bucket_count,
            snapshot.manifest.row_size,
            &evaluators,
            &queries,
        )?;
        wall.push(evaluated.wall);
        server_elapsed.push(evaluated.sum_server_elapsed);
        let reconstruct_started = Instant::now();
        reconstruct_cuckoo(snapshot, target_tag, evaluated.answers)?;
        reconstruct.push(reconstruct_started.elapsed());
    }
    topology_result(
        server_count,
        CUCKOO_CANDIDATES,
        snapshot.manifest.bucket_count,
        snapshot.manifest.row_size,
        CUCKOO_CANDIDATES * dense::query_size(snapshot.manifest.bucket_count),
        CUCKOO_CANDIDATES * snapshot.manifest.row_size,
        build_ms,
        snapshot.manifest.client_metadata_bytes(),
        snapshot.manifest.values_per_page * snapshot.manifest.max_value_bytes,
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
    build_ms: f64,
) -> Result<FuseTopologyResult> {
    let evaluators = evaluators(server_count)?;
    let mut rng = StdRng::seed_from_u64(0xf053_0000 ^ server_count as u64);
    let warmup = fuse_queries(cells, snapshot.manifest.cell_count, server_count, &mut rng)?;
    let warmup = evaluate(
        snapshot.rows(),
        snapshot.manifest.cell_count,
        snapshot.manifest.page_size,
        &evaluators,
        &warmup,
    )?;
    reconstruct_fuse(snapshot, target_tag, warmup.answers)?;

    let samples = sample_count(profile);
    let mut query_generation = Vec::with_capacity(samples);
    let mut wall = Vec::with_capacity(samples);
    let mut server_elapsed = Vec::with_capacity(samples);
    let mut reconstruct = Vec::with_capacity(samples);
    for _ in 0..samples {
        let query_started = Instant::now();
        let queries = fuse_queries(cells, snapshot.manifest.cell_count, server_count, &mut rng)?;
        query_generation.push(query_started.elapsed());
        let evaluated = evaluate(
            snapshot.rows(),
            snapshot.manifest.cell_count,
            snapshot.manifest.page_size,
            &evaluators,
            &queries,
        )?;
        wall.push(evaluated.wall);
        server_elapsed.push(evaluated.sum_server_elapsed);
        let reconstruct_started = Instant::now();
        reconstruct_fuse(snapshot, target_tag, evaluated.answers)?;
        reconstruct.push(reconstruct_started.elapsed());
    }
    topology_result(
        server_count,
        1,
        snapshot.manifest.cell_count,
        snapshot.manifest.page_size,
        dense::query_size(snapshot.manifest.cell_count),
        snapshot.manifest.page_size,
        build_ms,
        snapshot.manifest.client_metadata_bytes(),
        snapshot.manifest.values_per_page * snapshot.manifest.max_value_bytes,
        query_generation,
        wall,
        server_elapsed,
        reconstruct,
    )
}

fn cuckoo_queries(
    buckets: [usize; 2],
    row_count: usize,
    server_count: usize,
    rng: &mut StdRng,
) -> Result<Vec<Vec<Vec<u8>>>> {
    let mut per_server = (0..server_count)
        .map(|_| Vec::with_capacity(CUCKOO_CANDIDATES))
        .collect::<Vec<_>>();
    for bucket in buckets {
        for (queries, share) in
            per_server
                .iter_mut()
                .zip(dense::query_shares(bucket, row_count, server_count, rng)?)
        {
            queries.push(share);
        }
    }
    Ok(per_server)
}

fn fuse_queries(
    cells: &[usize],
    row_count: usize,
    server_count: usize,
    rng: &mut StdRng,
) -> Result<Vec<Vec<Vec<u8>>>> {
    Ok(
        dense::query_shares_for_buckets(cells, row_count, server_count, rng)?
            .into_iter()
            .map(|share| vec![share])
            .collect(),
    )
}

struct Evaluation {
    answers: Vec<Vec<Vec<u8>>>,
    wall: Duration,
    sum_server_elapsed: Duration,
}

fn evaluate(
    rows: &[u8],
    row_count: usize,
    row_size: usize,
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
                    let answers = evaluator
                        .answer_batch(SnapshotView::new(rows, row_count, row_size), queries)?;
                    Ok::<_, anyhow::Error>((started.elapsed(), answers))
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| handle.join().expect("Fuse benchmark server panicked"))
            .collect::<Result<Vec<_>>>()
    })?;
    Ok(Evaluation {
        wall: wall_started.elapsed(),
        sum_server_elapsed: results.iter().map(|(elapsed, _)| *elapsed).sum(),
        answers: results.into_iter().map(|(_, answers)| answers).collect(),
    })
}

fn reconstruct_cuckoo(
    snapshot: &TagPageSnapshot,
    target_tag: &[u8],
    answers: Vec<Vec<Vec<u8>>>,
) -> Result<()> {
    let mut found = None;
    for candidate in 0..CUCKOO_CANDIDATES {
        let shares = answers
            .iter()
            .map(|server| server[candidate].as_slice())
            .collect::<Vec<_>>();
        if let Some(page) = snapshot.decode_bucket_row(&dense::combine(&shares)?, target_tag, 0)? {
            found = Some(page);
        }
    }
    let found = found.context("packed cuckoo did not recover the selected page")?;
    if found.values.len() != snapshot.manifest.values_per_page {
        bail!("packed cuckoo recovered the wrong page");
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
        .context("Fuse did not recover the selected page")?;
    if page.values.len() != snapshot.manifest.values_per_page {
        bail!("Fuse recovered the wrong page");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn topology_result(
    server_count: usize,
    evaluations: usize,
    row_count: usize,
    row_size: usize,
    query_bytes_per_server: usize,
    response_bytes_per_server: usize,
    build_ms: f64,
    client_metadata_bytes: usize,
    useful_result_bytes: usize,
    mut query_generation: Vec<Duration>,
    mut wall: Vec<Duration>,
    mut server_elapsed: Vec<Duration>,
    mut reconstruct: Vec<Duration>,
) -> Result<FuseTopologyResult> {
    for samples in [
        &mut query_generation,
        &mut wall,
        &mut server_elapsed,
        &mut reconstruct,
    ] {
        samples.sort_unstable();
    }
    let expected_rows = evaluations * row_count.div_ceil(2);
    let aggregate_work = fuse_topology_accounting(
        server_count,
        row_count * row_size,
        build_ms,
        client_metadata_bytes,
        evaluations,
        expected_rows * row_size,
        query_bytes_per_server,
        response_bytes_per_server,
        useful_result_bytes,
        micros(percentile(&query_generation, 50)),
        millis(percentile(&wall, 50)),
        millis(percentile(&server_elapsed, 50)),
        micros(percentile(&reconstruct, 50)),
    )?;
    Ok(FuseTopologyResult {
        aggregate_work,
        server_count,
        privacy_collusion_tolerance: server_count - 1,
        required_answers: server_count,
        dense_evaluations_per_server: evaluations,
        expected_rows_xored_per_server: expected_rows,
        expected_data_bytes_xored_per_server: expected_rows * row_size,
        query_bytes_per_server,
        total_client_upload_bytes: query_bytes_per_server * server_count,
        response_bytes_per_server,
        total_client_download_bytes: response_bytes_per_server * server_count,
        client_query_generation_p50_us: micros(percentile(&query_generation, 50)),
        co_located_wall_p50_ms: millis(percentile(&wall, 50)),
        co_located_wall_p95_ms: millis(percentile(&wall, 95)),
        sum_server_elapsed_p50_ms: millis(percentile(&server_elapsed, 50)),
        client_reconstruct_p50_us: micros(percentile(&reconstruct, 50)),
    })
}

#[allow(clippy::too_many_arguments)]
fn fuse_topology_accounting(
    server_count: usize,
    table_bytes_per_server: usize,
    build_ms: f64,
    client_metadata_bytes: usize,
    evaluations_per_server: usize,
    logical_bytes_per_server: usize,
    query_bytes_per_server: usize,
    response_bytes_per_server: usize,
    useful_result_bytes: usize,
    query_generation_us: f64,
    wall_ms: f64,
    aggregate_server_ms: f64,
    reconstruct_us: f64,
) -> Result<AggregateWorkReport> {
    let mut work = AggregateWorkReport::new(
        "static-layout-over-replicated-dense-xor",
        ComparisonScope {
            workload: "one lookup over the identical populated immutable tag-page corpus",
            result: "one first page containing four fixed-width compact locators",
            public_partition: "global immutable snapshot",
            leakage: LeakageScope::ExactQueryPrivacy,
        },
        SecurityLabels {
            privacy: "exact information-theoretic tag privacy",
            server_count,
            collusion_tolerance: server_count - 1,
            required_answers: server_count,
            assumptions: "at least one replicated Dense XOR server does not collude",
            availability: "all answer shares are required",
            integrity: "page fingerprint checks corruption; no malicious-server proof",
        },
    );
    work.global_build.aggregate_server_time_ms = Metric::measured(
        build_ms,
        "static layout build over the shared encoded corpus",
    );
    work.global_build.server_scans =
        Metric::not_measured("static-layout construction passes were not instrumented");
    work.global_build.network_rounds = Metric::not_applicable("build is server-local");
    work.maintenance = PhaseWork::not_applicable(
        "immutable snapshot lifetime",
        "the static layout is rebuilt for a new snapshot",
    );
    let physical_per_server = logical_bytes_per_server + query_bytes_per_server;
    for server in &mut work.online.per_server {
        server.server_time_p50_ms = Metric::estimated(
            aggregate_server_ms / server_count as f64,
            "aggregate p50 divided evenly; per-server samples were not retained",
        );
        server.logical_selected_bytes = Metric::estimated(
            logical_bytes_per_server,
            "expected set selector bits times row bytes for a random query share",
        );
        server.physical_or_scanned_bytes = Metric::estimated(
            physical_per_server,
            "selector scan plus expected XOR row payload; cache-line and DRAM traffic were not measured",
        );
        server.scans = Metric::deterministic(
            evaluations_per_server,
            "Dense selector traversals performed by this server",
        );
    }
    work.online.unit = "one first-page tag lookup";
    work.online.aggregate_server_time_p50_ms =
        Metric::measured(aggregate_server_ms, "sum of measured server elapsed times");
    work.online.max_server_time_p50_ms = Metric::estimated(
        wall_ms,
        "co-located wall p50 is an upper-envelope proxy including dispatch overhead",
    );
    work.online.aggregate_logical_selected_bytes = Metric::estimated(
        logical_bytes_per_server * server_count,
        "sum of expected selected bytes across random server shares",
    );
    work.online.aggregate_physical_or_scanned_bytes = Metric::estimated(
        physical_per_server * server_count,
        "sum of estimated selector and payload bytes; no hardware counter",
    );
    work.online.server_scans = Metric::deterministic(
        evaluations_per_server * server_count,
        "sum of Dense selector traversals",
    );
    work.online.network_rounds = Metric::deterministic(1, "all shares are batched in one round");
    work.online.useful_result_bytes =
        Metric::deterministic(useful_result_bytes, "four fixed-width compact locators");
    work.client.online_cpu_p50_ms = Metric::estimated(
        (query_generation_us + reconstruct_us) / 1_000.0,
        "sum of separately measured share-generation and reconstruction medians",
    );
    work.client.persistent_state_bytes =
        Metric::deterministic(client_metadata_bytes, "constant public layout metadata");
    work.client.upload_bytes =
        Metric::deterministic(query_bytes_per_server * server_count, "all query shares");
    work.client.download_bytes = Metric::deterministic(
        response_bytes_per_server * server_count,
        "all answer shares",
    );
    work.persisted_storage.server_bytes_per_server =
        Metric::deterministic(table_bytes_per_server, "one static table replica");
    work.persisted_storage.aggregate_server_bytes = Metric::deterministic(
        table_bytes_per_server * server_count,
        "sum across replicated servers",
    );
    work.persisted_storage.client_bytes =
        Metric::deterministic(client_metadata_bytes, "constant public layout metadata");
    work.amortization = AmortizationHorizon {
        global_build: "all clients and lookups using one immutable layout",
        per_client_setup: "not applicable beyond constant public metadata",
        maintenance: "layout lifetime",
        assumed_global_queries: None,
        assumed_queries_per_client_setup: None,
        assumed_online_events_per_maintenance: None,
        note: "Layout build is reported separately and is not folded into online work.",
    };
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    Ok(work)
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
