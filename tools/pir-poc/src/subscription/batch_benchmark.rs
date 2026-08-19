//! Batch and concurrency exploration for live Compact DPF subscriptions.

use std::{
    collections::HashMap,
    mem::size_of,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;

use super::{
    batch_eval::{
        shares_for_event_subscription, CompactBatchEvaluation, CompactBatchEvaluator,
        CompactBatchKernel, DecoyBatchKernel, IndexedDecoyBatchServer,
    },
    combine_compact, compact_registration, CompactSubscriptionServer, SubscriptionId,
};
use crate::benchmark::{
    accounting::{
        unavailable_hardware_counters, AggregateWorkReport, ComparisonScope, LeakageScope, Metric,
        PhaseWork, SecurityLabels,
    },
    Profile,
};

const BUCKET_COUNT: usize = 1 << 16;
const SUBSCRIBER_COUNTS: [usize; 4] = [1, 100, 1_000, 10_000];
const EVENT_BATCHES: [usize; 4] = [1, 8, 64, 1_024];
const CANDIDATES: usize = 100;
const PARALLEL_SHARDS: usize = 4;
const TIMED_POINT_EVALUATION_LIMIT: usize = 65_536;
const SUBSCRIPTION_ID_BYTES: usize = 16;
const COMPACT_NOTIFICATION_SHARE_BYTES: usize = SUBSCRIPTION_ID_BYTES + 16;
const DECOY_CANDIDATE_BYTES: usize = size_of::<u32>();

#[derive(Debug, Serialize)]
pub struct LiveBatchBenchmarkReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: Vec<&'static str>,
    pub domain: LiveBatchDomain,
    pub workloads: Vec<LiveBatchWorkload>,
    pub prg_batching: PrgBatchingAssessment,
    pub three_server: CompactThreeServerAssessment,
    pub production_caveats: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct LiveBatchDomain {
    pub bucket_count: usize,
    pub depth: usize,
    pub subscriber_counts: Vec<usize>,
    pub event_batch_sizes: Vec<usize>,
    pub compact_servers: usize,
    pub decoy_candidates: usize,
    pub timed_gate: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamCase {
    HitOne,
    Miss,
    Uniform,
    ZipfLike,
}

#[derive(Debug, Serialize)]
pub struct LiveBatchWorkload {
    pub stream_case: StreamCase,
    pub distribution_note: &'static str,
    pub subscriptions: usize,
    pub events: usize,
    pub useful_target_matches: usize,
    pub useful_matches_per_event: f64,
    pub compact_key_bytes_per_server: usize,
    pub compact_registration_bytes_total: usize,
    pub compact_registration_build_ms: f64,
    pub compact_server_state_bytes_each: usize,
    pub compact_preprocessing_ms_sum_servers: f64,
    pub compact_kernels: Vec<CompactKernelResult>,
    pub sequential_elapsed_winner: Option<&'static str>,
    pub parallel_latency_is_separate_lane: bool,
    pub indexed_decoy: IndexedDecoyResult,
    pub privacy_comparison: LivePrivacyComparison,
}

#[derive(Debug, Serialize)]
pub struct LivePrivacyComparison {
    pub compact_point_evaluations_per_event_all_servers: usize,
    pub compact_tree_level_expansions_per_event_all_servers: usize,
    pub compact_fixed_wire_bytes_per_event_all_servers: usize,
    pub indexed_decoy_best_lookups_per_event: f64,
    pub indexed_decoy_candidate_bytes_per_event: f64,
    pub direct_elapsed_ratio_permitted: bool,
    pub reason: &'static str,
}

#[derive(Debug, Serialize)]
pub struct CompactKernelResult {
    pub aggregate_work: AggregateWorkReport,
    pub kernel: &'static str,
    pub timed: bool,
    pub timing_skip_reason: Option<&'static str>,
    pub eligible_for_aggregate_elapsed_ranking: bool,
    pub aggregate_time_note: &'static str,
    pub point_evaluations_all_servers: usize,
    pub point_evaluations_per_event: f64,
    pub point_evaluations_per_useful_match: Option<f64>,
    pub dpf_tree_level_expansions_all_servers: usize,
    pub dpf_tree_level_expansions_per_event: f64,
    pub dpf_tree_level_expansions_per_useful_match: Option<f64>,
    pub key_bytes_processed_all_servers: usize,
    pub key_bytes_processed_per_event: f64,
    pub key_bytes_processed_per_useful_match: Option<f64>,
    pub table_ordering_passes_all_servers: usize,
    pub response_share_bytes_all_servers: usize,
    pub wire_response_bytes_all_servers: usize,
    pub peak_materialized_output_bytes_all_servers: usize,
    pub aggregate_server_p50_ms: Option<f64>,
    pub aggregate_server_p95_ms: Option<f64>,
    pub wall_p50_ms: Option<f64>,
    pub wall_p95_ms: Option<f64>,
    pub client_combine_filter_p50_ms: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct IndexedDecoyResult {
    pub privacy: &'static str,
    pub candidates_per_subscription: usize,
    pub registration_bytes: usize,
    pub index_memberships: usize,
    pub distinct_index_buckets: usize,
    pub estimated_server_state_bytes: usize,
    pub candidate_notifications: usize,
    pub useful_target_matches: usize,
    pub false_positive_notifications: usize,
    pub kernels: Vec<DecoyKernelResult>,
}

#[derive(Debug, Serialize)]
pub struct DecoyKernelResult {
    pub aggregate_work: AggregateWorkReport,
    pub kernel: &'static str,
    pub timed: bool,
    pub timing_skip_reason: Option<&'static str>,
    pub parallel_shards: usize,
    pub index_lookups: usize,
    pub index_lookups_per_event: f64,
    pub index_lookups_per_useful_match: Option<f64>,
    pub candidate_notifications: usize,
    pub notification_bytes: usize,
    pub server_p50_ms: Option<f64>,
    pub server_p95_ms: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct PrgBatchingAssessment {
    pub implemented: Vec<&'static str>,
    pub not_implemented: &'static str,
    pub reason: &'static str,
    pub production_experiment: &'static str,
}

#[derive(Debug, Serialize)]
pub struct CompactThreeServerAssessment {
    pub status: &'static str,
    pub reason: &'static str,
    pub rejected_shortcut: &'static str,
    pub valid_directions: Vec<&'static str>,
}

struct Setup {
    servers: [CompactSubscriptionServer; 2],
    targets_by_id: HashMap<SubscriptionId, usize>,
    ids: Vec<SubscriptionId>,
    key_bytes_per_server: usize,
    registration_build_ms: f64,
    decoys: IndexedDecoyBatchServer,
}

#[derive(Default)]
struct CompactTimings {
    aggregate_server: Vec<Duration>,
    wall: Vec<Duration>,
    client: Vec<Duration>,
}

#[derive(Default)]
struct DecoyTimings {
    server: Vec<Duration>,
}

pub fn run(profile: Profile) -> Result<LiveBatchBenchmarkReport> {
    let mut workloads = Vec::new();
    for stream_case in [
        StreamCase::HitOne,
        StreamCase::Miss,
        StreamCase::Uniform,
        StreamCase::ZipfLike,
    ] {
        for subscriptions in SUBSCRIBER_COUNTS {
            let targets = targets(stream_case, subscriptions);
            let setup = build_setup(&targets)?;
            let left = CompactBatchEvaluator::new(&setup.servers[0], PARALLEL_SHARDS)?;
            let right = CompactBatchEvaluator::new(&setup.servers[1], PARALLEL_SHARDS)?;
            let preprocessing_ms =
                millis(left.preprocessing_elapsed() + right.preprocessing_elapsed());
            for event_count in EVENT_BATCHES {
                let events = events(stream_case, event_count);
                workloads.push(benchmark_workload(
                    profile,
                    stream_case,
                    &setup,
                    &left,
                    &right,
                    &events,
                    preprocessing_ms,
                )?);
            }
        }
    }
    Ok(LiveBatchBenchmarkReport {
        protocol: "two-server-compact-dpf-live-batch-optimization-v1",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: vec![
            "All workloads use the identical 65,536-bucket domain. Compact DPF and indexed decoys receive the identical target subscriptions and event stream.",
            "Compact DPF always evaluates every subscription for every event on both non-colluding servers and emits fixed output shares; hit and miss have identical deterministic work and output size.",
            "The compatibility, key-preprocessed, subscription-major, cache-blocked, and parallel kernels preserve byte-exact DPF shares. Kernel order changes only server-local allocation and traversal.",
            "Aggregate-work ranking uses deterministic point evaluations, DPF tree-level expansions, and logical key bytes. Sequential elapsed times may rank sequential kernels; parallel Rayon elapsed is latency/throughput only because worker CPU time is not collected.",
            "The indexed 100-decoy comparator performs public inverted-index lookups and has candidate-set privacy only. It is not directly security-comparable with Compact DPF.",
            "Rows outside the timing gate retain exact deterministic work/state/traffic accounting with timing explicitly absent; no interpolation is used.",
        ],
        domain: LiveBatchDomain {
            bucket_count: BUCKET_COUNT,
            depth: BUCKET_COUNT.trailing_zeros() as usize,
            subscriber_counts: SUBSCRIBER_COUNTS.to_vec(),
            event_batch_sizes: EVENT_BATCHES.to_vec(),
            compact_servers: 2,
            decoy_candidates: CANDIDATES,
            timed_gate: "time only workloads with subscriptions * events <= 65,536; the 10K/1K large matrix remains exact analytical accounting",
        },
        workloads,
        prg_batching: PrgBatchingAssessment {
            implemented: vec![
                "encode every distinct event input once per batch instead of once per subscription",
                "pre-sort key references once per immutable subscription set",
                "event-major, subscription-major, cache-blocked, and bounded parallel traversal",
                "deduplicate repeated public event buckets for the weaker decoy index",
            ],
            not_implemented: "SIMD/multi-buffer AES expansion across independent Compact DPF roots",
            reason: "fss-rs exposes scalar eval_point and scalar PRG expansion. Independent subscription roots cannot share cryptographic expansion results; a real speedup needs a new constant-time multi-key AES/PRG backend, not memoization of secret state.",
            production_experiment: "add a pinned, audited AES-NI/ARMv8 multi-buffer PRG interface and compare instructions, cycles, cache misses, and energy at equal point-evaluation counts before changing the wire protocol",
        },
        three_server: CompactThreeServerAssessment {
            status: "not a drop-in extension",
            reason: "the selected fss-rs Compact DPF is a two-party construction whose keys, correction words, and combine rule are defined for exactly two parties",
            rejected_shortcut: "duplicating one of the two key shares on a third server adds neither three-party privacy nor one-server fault tolerance and can invalidate the security argument",
            valid_directions: vec![
                "benchmark a reviewed three-party or threshold multi-party DPF/FSS construction as a different protocol",
                "run independently keyed two-server pairs for availability, counting doubled registration/evaluation work and pair-level trust assumptions",
                "retain Dense XOR live keys when arbitrary n-out-of-n sharing is more important than Compact DPF key size",
            ],
        },
        production_caveats: vec![
            "Each Compact DPF server share is pseudorandom and cannot be filtered locally. Hiding matches therefore requires fixed output padding: one identifier and one 16-byte share per subscription per event per server, or a separately proven private aggregation protocol.",
            "Returning only combined matches requires a trusted/non-colluding combiner and leaks batch match count and timing unless notifications are padded and released on a fixed schedule.",
            "Parallel wall time is not aggregate work. Sequential kernels sum single-threaded replica elapsed; parallel kernels expose latency only, while every kernel separately reports deterministic point evaluations, tree levels, key bytes, and output bytes.",
            "Zipf-like results use a declared finite synthetic rank distribution (exponent 1.2 over 4,096 hot ranks); they are not asserted to model Shinzo production traffic.",
            "Registration churn, persistent-store writes, network framing, TLS, event decoding, notification payload delivery, and mobile client energy need separate measurements.",
        ],
    })
}

fn build_setup(targets: &[usize]) -> Result<Setup> {
    let started = Instant::now();
    let mut rng = StdRng::seed_from_u64(0x1a1e_d0f0_u64 ^ targets.len() as u64);
    let mut servers = [
        CompactSubscriptionServer::new(0, BUCKET_COUNT)?,
        CompactSubscriptionServer::new(1, BUCKET_COUNT)?,
    ];
    let mut targets_by_id = HashMap::with_capacity(targets.len());
    let mut ids = Vec::with_capacity(targets.len());
    let mut key_bytes_per_server = None;
    let mut decoys = IndexedDecoyBatchServer::default();
    for (index, &target) in targets.iter().enumerate() {
        let registration = compact_registration(target, BUCKET_COUNT, &mut rng)?;
        let key_bytes = registration.server_keys[0].len();
        if registration.server_keys[1].len() != key_bytes {
            anyhow::bail!("Compact DPF server key sizes differ");
        }
        if key_bytes_per_server
            .replace(key_bytes)
            .is_some_and(|previous| previous != key_bytes)
        {
            anyhow::bail!("Compact DPF key size changed within one domain");
        }
        for (server, key) in servers.iter_mut().zip(&registration.server_keys) {
            server.register(registration.id, key)?;
        }
        let candidates = decoy_candidates(target, index);
        decoys.register(registration.id, &candidates)?;
        targets_by_id.insert(registration.id, target);
        ids.push(registration.id);
    }
    Ok(Setup {
        servers,
        targets_by_id,
        ids,
        key_bytes_per_server: key_bytes_per_server.unwrap_or(0),
        registration_build_ms: millis(started.elapsed()),
        decoys,
    })
}

#[allow(clippy::too_many_arguments)]
fn benchmark_workload(
    profile: Profile,
    stream_case: StreamCase,
    setup: &Setup,
    left: &CompactBatchEvaluator<'_>,
    right: &CompactBatchEvaluator<'_>,
    events: &[usize],
    preprocessing_ms: f64,
) -> Result<LiveBatchWorkload> {
    let subscriptions = setup.ids.len();
    let useful_matches = useful_match_count(&setup.targets_by_id, events);
    let timed = subscriptions
        .checked_mul(events.len())
        .is_some_and(|evaluations| evaluations <= TIMED_POINT_EVALUATION_LIMIT);
    let compact_kernels = [
        CompactBatchKernel::ExistingEventMajor,
        CompactBatchKernel::PreprocessedEventMajor,
        CompactBatchKernel::SubscriptionMajor,
        CompactBatchKernel::CacheBlocked {
            subscription_block: 256,
            event_block: 16,
        },
        CompactBatchKernel::ParallelEventShards,
    ]
    .into_iter()
    .map(|kernel| {
        benchmark_compact_kernel(
            profile,
            setup,
            left,
            right,
            events,
            useful_matches,
            kernel,
            timed,
        )
    })
    .collect::<Result<Vec<_>>>()?;
    let sequential_elapsed_winner = compact_kernels
        .iter()
        .filter(|kernel| kernel.eligible_for_aggregate_elapsed_ranking)
        .filter_map(|kernel| {
            kernel
                .aggregate_server_p50_ms
                .map(|elapsed| (kernel.kernel, elapsed))
        })
        .min_by(|left, right| left.1.total_cmp(&right.1))
        .map(|(kernel, _)| kernel);

    let decoy_baseline = setup
        .decoys
        .evaluate(events, DecoyBatchKernel::EventMajor)?;
    let candidate_notifications = decoy_baseline.matching_notifications;
    let decoy_kernels = [
        DecoyBatchKernel::EventMajor,
        DecoyBatchKernel::DeduplicatedEvents,
        DecoyBatchKernel::ParallelEventShards,
    ]
    .into_iter()
    .map(|kernel| {
        benchmark_decoy_kernel(
            profile,
            setup,
            events,
            useful_matches,
            candidate_notifications,
            kernel,
            timed,
        )
    })
    .collect::<Result<Vec<_>>>()?;
    let best_decoy_lookups = decoy_kernels
        .iter()
        .map(|kernel| kernel.index_lookups)
        .min()
        .unwrap_or(0);

    Ok(LiveBatchWorkload {
        stream_case,
        distribution_note: distribution_note(stream_case),
        subscriptions,
        events: events.len(),
        useful_target_matches: useful_matches,
        useful_matches_per_event: useful_matches as f64 / events.len() as f64,
        compact_key_bytes_per_server: setup.key_bytes_per_server,
        compact_registration_bytes_total: setup.key_bytes_per_server * subscriptions * 2,
        compact_registration_build_ms: setup.registration_build_ms,
        compact_server_state_bytes_each: setup.key_bytes_per_server * subscriptions,
        compact_preprocessing_ms_sum_servers: preprocessing_ms,
        compact_kernels,
        sequential_elapsed_winner,
        parallel_latency_is_separate_lane: true,
        indexed_decoy: IndexedDecoyResult {
            privacy: "weaker 100-candidate-set privacy; the server learns all candidates and the exact event bucket",
            candidates_per_subscription: CANDIDATES,
            registration_bytes: subscriptions * CANDIDATES * DECOY_CANDIDATE_BYTES,
            index_memberships: setup.decoys.memberships(),
            distinct_index_buckets: setup.decoys.distinct_buckets(),
            estimated_server_state_bytes: setup.decoys.estimated_state_bytes(),
            candidate_notifications,
            useful_target_matches: useful_matches,
            false_positive_notifications: candidate_notifications.saturating_sub(useful_matches),
            kernels: decoy_kernels,
        },
        privacy_comparison: LivePrivacyComparison {
            compact_point_evaluations_per_event_all_servers: subscriptions * 2,
            compact_tree_level_expansions_per_event_all_servers: subscriptions
                * 2
                * BUCKET_COUNT.trailing_zeros() as usize,
            compact_fixed_wire_bytes_per_event_all_servers: subscriptions
                * 2
                * COMPACT_NOTIFICATION_SHARE_BYTES,
            indexed_decoy_best_lookups_per_event: best_decoy_lookups as f64
                / events.len() as f64,
            indexed_decoy_candidate_bytes_per_event: candidate_notifications as f64
                * SUBSCRIPTION_ID_BYTES as f64
                / events.len() as f64,
            direct_elapsed_ratio_permitted: false,
            reason: "Compact DPF has computational two-server target privacy under the AES-based PRG/DPF construction and fixed padded outputs; indexed decoys reveal a 100-candidate set and exact event bucket, so elapsed ratios are descriptive only and cannot select a privacy-equivalent winner.",
        },
    })
}

#[allow(clippy::too_many_arguments)]
fn benchmark_compact_kernel(
    profile: Profile,
    setup: &Setup,
    left: &CompactBatchEvaluator<'_>,
    right: &CompactBatchEvaluator<'_>,
    events: &[usize],
    useful_matches: usize,
    kernel: CompactBatchKernel,
    timed: bool,
) -> Result<CompactKernelResult> {
    let subscriptions = setup.ids.len();
    let evaluations_per_server = subscriptions
        .checked_mul(events.len())
        .context("Compact DPF evaluation count overflow")?;
    let point_evaluations = evaluations_per_server * 2;
    let tree_expansions = point_evaluations * BUCKET_COUNT.trailing_zeros() as usize;
    let key_bytes_processed = setup.key_bytes_per_server * point_evaluations;
    let passes_per_server = compact_passes(kernel, subscriptions, events.len());
    let response_share_bytes = evaluations_per_server * 16 * 2;
    let wire_response_bytes = evaluations_per_server * COMPACT_NOTIFICATION_SHARE_BYTES * 2;
    let timings = if timed {
        Some(measure_compact(
            profile, setup, left, right, events, kernel,
        )?)
    } else {
        None
    };
    let summary = timings.map(CompactTimings::summarize).transpose()?;
    let aggregate_elapsed_valid = kernel != CompactBatchKernel::ParallelEventShards;
    let work = compact_accounting(
        kernel.name(),
        setup,
        events.len(),
        useful_matches,
        point_evaluations,
        tree_expansions,
        key_bytes_processed,
        passes_per_server,
        wire_response_bytes,
        aggregate_elapsed_valid,
        summary.as_ref(),
    )?;
    Ok(CompactKernelResult {
        aggregate_work: work,
        kernel: kernel.name(),
        timed,
        timing_skip_reason: (!timed).then_some(
            "subscriptions * events exceeds 65,536; exact deterministic accounting retained",
        ),
        eligible_for_aggregate_elapsed_ranking: aggregate_elapsed_valid,
        aggregate_time_note: if aggregate_elapsed_valid {
            "single-threaded inside each replica; sum of two server elapsed times is an aggregate elapsed proxy"
        } else {
            "bounded four-shard Rayon wall time hides worker CPU; use only for latency/throughput, not aggregate-work ranking"
        },
        point_evaluations_all_servers: point_evaluations,
        point_evaluations_per_event: point_evaluations as f64 / events.len() as f64,
        point_evaluations_per_useful_match: per_match(point_evaluations, useful_matches),
        dpf_tree_level_expansions_all_servers: tree_expansions,
        dpf_tree_level_expansions_per_event: tree_expansions as f64 / events.len() as f64,
        dpf_tree_level_expansions_per_useful_match: per_match(tree_expansions, useful_matches),
        key_bytes_processed_all_servers: key_bytes_processed,
        key_bytes_processed_per_event: key_bytes_processed as f64 / events.len() as f64,
        key_bytes_processed_per_useful_match: per_match(key_bytes_processed, useful_matches),
        table_ordering_passes_all_servers: passes_per_server * 2,
        response_share_bytes_all_servers: response_share_bytes,
        wire_response_bytes_all_servers: wire_response_bytes,
        peak_materialized_output_bytes_all_servers: response_share_bytes,
        aggregate_server_p50_ms: if aggregate_elapsed_valid {
            summary
                .as_ref()
                .map(|summary| millis(summary.aggregate_server_p50))
        } else {
            None
        },
        aggregate_server_p95_ms: if aggregate_elapsed_valid {
            summary
                .as_ref()
                .map(|summary| millis(summary.aggregate_server_p95))
        } else {
            None
        },
        wall_p50_ms: summary.as_ref().map(|summary| millis(summary.wall_p50)),
        wall_p95_ms: summary.as_ref().map(|summary| millis(summary.wall_p95)),
        client_combine_filter_p50_ms: summary.as_ref().map(|summary| millis(summary.client_p50)),
    })
}

fn measure_compact(
    profile: Profile,
    setup: &Setup,
    left: &CompactBatchEvaluator<'_>,
    right: &CompactBatchEvaluator<'_>,
    events: &[usize],
    kernel: CompactBatchKernel,
) -> Result<CompactTimings> {
    let warm_left = left.evaluate(events, kernel)?;
    let warm_right = right.evaluate(events, kernel)?;
    verify_compact(setup, events, &warm_left, &warm_right)?;

    let mut timings = CompactTimings::default();
    for _ in 0..sample_count(profile) {
        let wall_started = Instant::now();
        let (left_result, right_result) = std::thread::scope(|scope| {
            let left_handle = scope.spawn(|| {
                let started = Instant::now();
                left.evaluate(events, kernel)
                    .map(|result| (started.elapsed(), result))
            });
            let right_handle = scope.spawn(|| {
                let started = Instant::now();
                right
                    .evaluate(events, kernel)
                    .map(|result| (started.elapsed(), result))
            });
            (
                left_handle
                    .join()
                    .expect("left Compact DPF server panicked"),
                right_handle
                    .join()
                    .expect("right Compact DPF server panicked"),
            )
        });
        let (left_elapsed, left_result) = left_result?;
        let (right_elapsed, right_result) = right_result?;
        timings.wall.push(wall_started.elapsed());
        timings.aggregate_server.push(left_elapsed + right_elapsed);
        let client_started = Instant::now();
        verify_compact(setup, events, &left_result, &right_result)?;
        timings.client.push(client_started.elapsed());
    }
    Ok(timings)
}

fn verify_compact(
    setup: &Setup,
    events: &[usize],
    left: &CompactBatchEvaluation,
    right: &CompactBatchEvaluation,
) -> Result<()> {
    if left.subscription_ids != right.subscription_ids {
        anyhow::bail!("Compact DPF server subscription ordering differs");
    }
    for (event_index, &event) in events.iter().enumerate() {
        for (subscription_index, id) in left.subscription_ids.iter().enumerate() {
            let shares =
                shares_for_event_subscription(left, right, event_index, subscription_index)?;
            let matches = combine_compact(&shares)?;
            let expected = setup
                .targets_by_id
                .get(id)
                .is_some_and(|target| *target == event);
            if matches != expected {
                anyhow::bail!(
                    "Compact DPF batch mismatch for event {event_index}, subscription {subscription_index}"
                );
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn compact_accounting(
    kernel: &'static str,
    setup: &Setup,
    events: usize,
    useful_matches: usize,
    point_evaluations: usize,
    tree_expansions: usize,
    key_bytes_processed: usize,
    passes_per_server: usize,
    wire_response_bytes: usize,
    aggregate_elapsed_valid: bool,
    timing: Option<&CompactSummary>,
) -> Result<AggregateWorkReport> {
    let subscriptions = setup.ids.len();
    let mut work = AggregateWorkReport::new(
        kernel,
        ComparisonScope {
            workload: "identical live event batch over the same 65,536-bucket domain and registered subscription targets",
            result: "exact match boolean for every subscription/event pair under fixed output padding",
            public_partition: "one public live collection/event stream; hit/miss and declared distribution are separate cases",
            leakage: LeakageScope::ExactQueryPrivacy,
        },
        SecurityLabels {
            privacy: "computational two-party Compact DPF subscription-point privacy against either non-colluding semi-honest server under the AES-based PRG and selected DPF construction",
            server_count: 2,
            collusion_tolerance: 1,
            required_answers: 2,
            assumptions: "the two Compact DPF servers do not collude; both use the same domain and registered key generation",
            availability: "both output shares are required",
            integrity: "correctness checked in-process; no malicious-server proof or authenticated output share",
        },
    );
    work.global_build = PhaseWork::not_applicable(
        "global build",
        "live Compact DPF has per-subscription registration rather than a global snapshot build",
    );
    let mut setup_phase = PhaseWork::unmeasured(
        "registration of the full subscription set",
        "registration wall time is reported outside AggregateWorkReport; aggregate client/server CPU split was not instrumented",
    );
    setup_phase.logical_selected_bytes = Metric::deterministic(
        setup.key_bytes_per_server * subscriptions * 2,
        "two encoded server keys per subscription",
    );
    setup_phase.client_upload_bytes = Metric::deterministic(
        setup.key_bytes_per_server * subscriptions * 2,
        "one Compact DPF key sent to each server",
    );
    setup_phase.client_download_bytes = Metric::deterministic(0, "registration has no result body");
    setup_phase.network_rounds = Metric::estimated(1, "registrations can be batched per server");
    work.per_client_setup = setup_phase;
    work.maintenance = PhaseWork::not_applicable(
        "event-independent subscription state",
        "events do not mutate Compact DPF keys; register/unregister is a setup change",
    );
    for server in &mut work.online.per_server {
        server.server_time_p50_ms = if aggregate_elapsed_valid {
            timing.map_or_else(
                || Metric::not_measured("workload exceeded the declared timing gate"),
                |timing| {
                    Metric::estimated(
                        millis(timing.aggregate_server_p50) / 2.0,
                        "aggregate two-server median divided evenly; individual samples not retained",
                    )
                },
            )
        } else {
            Metric::not_measured(
                "parallel worker CPU time was not collected; Rayon wall is not aggregate server work",
            )
        };
        server.logical_selected_bytes = Metric::deterministic(
            key_bytes_processed / 2,
            "encoded DPF key bytes logically consumed across every point evaluation; cache reuse is not subtracted",
        );
        server.physical_or_scanned_bytes = Metric::not_measured(
            "cache/DRAM traffic requires hardware counters and is not inferred from key size",
        );
        server.scans = Metric::deterministic(
            passes_per_server,
            "kernel-specific subscription/event ordering passes",
        );
    }
    work.online.unit = "one declared event batch";
    work.online.aggregate_server_time_p50_ms = if aggregate_elapsed_valid {
        timing.map_or_else(
            || Metric::not_measured("workload exceeded the declared timing gate"),
            |timing| {
                Metric::measured(
                    millis(timing.aggregate_server_p50),
                    "sum of two single-threaded server elapsed times",
                )
            },
        )
    } else {
        Metric::not_measured(
            "parallel worker CPU time was not collected; use deterministic point/PRG/key work and wall latency separately",
        )
    };
    work.online.max_server_time_p50_ms = timing.map_or_else(
        || Metric::not_measured("workload exceeded the declared timing gate"),
        |timing| Metric::measured(millis(timing.wall_p50), "co-located two-server wall median"),
    );
    work.online.aggregate_logical_selected_bytes = Metric::deterministic(
        key_bytes_processed,
        "aggregate encoded-key bytes logically consumed by all point evaluations",
    );
    work.online.aggregate_physical_or_scanned_bytes =
        Metric::not_measured("physical/cache/DRAM traffic was not collected");
    work.online.server_scans = Metric::deterministic(
        passes_per_server * 2,
        "sum of kernel ordering passes across both servers",
    );
    work.online.network_rounds = Metric::deterministic(
        1,
        "both fixed padded output-share batches can be delivered in parallel",
    );
    work.online.useful_result_bytes = Metric::deterministic(
        useful_matches * SUBSCRIPTION_ID_BYTES,
        "matching subscription identifiers only; event payload delivery excluded",
    );
    work.client.online_cpu_p50_ms = timing.map_or_else(
        || Metric::not_measured("combine/filter timing was skipped"),
        |timing| {
            Metric::measured(
                millis(timing.client_p50),
                "combine and filter every output pair",
            )
        },
    );
    work.client.peak_transient_ram_bytes = Metric::deterministic(
        subscriptions * events * 16 * 2,
        "two packed 16-byte output-share arrays; allocator overhead excluded",
    );
    work.client.persistent_state_bytes = Metric::not_measured(
        "client subscription bookkeeping representation is outside this server benchmark",
    );
    work.client.upload_bytes =
        Metric::deterministic(0, "live events originate at the server/event bus");
    work.client.download_bytes = Metric::deterministic(
        wire_response_bytes,
        "fixed identifier plus 16-byte share for every subscription/event/server",
    );
    work.persisted_storage.server_bytes_per_server = Metric::deterministic(
        setup.key_bytes_per_server * subscriptions,
        "one encoded Compact DPF key per registered subscription",
    );
    work.persisted_storage.aggregate_server_bytes = Metric::deterministic(
        setup.key_bytes_per_server * subscriptions * 2,
        "two non-colluding Compact DPF key stores",
    );
    work.persisted_storage.client_bytes =
        Metric::not_measured("client registration state excluded");
    work.hardware_counters = unavailable_hardware_counters();
    let _ = (point_evaluations, tree_expansions);
    work.validate()?;
    Ok(work)
}

#[allow(clippy::too_many_arguments)]
fn benchmark_decoy_kernel(
    profile: Profile,
    setup: &Setup,
    events: &[usize],
    useful_matches: usize,
    expected_notifications: usize,
    kernel: DecoyBatchKernel,
    timed: bool,
) -> Result<DecoyKernelResult> {
    let reference = setup.decoys.evaluate(events, kernel)?;
    if reference.matching_notifications != expected_notifications {
        anyhow::bail!("indexed decoy kernels returned different notification counts");
    }
    let timing_allowed = timed && kernel != DecoyBatchKernel::ParallelEventShards;
    let timings = if timing_allowed {
        let mut timings = DecoyTimings::default();
        for _ in 0..sample_count(profile) {
            let started = Instant::now();
            let result = setup.decoys.evaluate(events, kernel)?;
            timings.server.push(started.elapsed());
            if result.notifications != reference.notifications {
                anyhow::bail!("indexed decoy kernel changed exact event outputs");
            }
        }
        Some(timings.summarize()?)
    } else {
        None
    };
    let notification_bytes = expected_notifications * SUBSCRIPTION_ID_BYTES;
    let work = decoy_accounting(
        kernel.name(),
        setup,
        events.len(),
        useful_matches,
        reference.index_lookups,
        expected_notifications,
        notification_bytes,
        timings.as_ref(),
    )?;
    Ok(DecoyKernelResult {
        aggregate_work: work,
        kernel: kernel.name(),
        timed: timing_allowed,
        timing_skip_reason: (!timing_allowed).then_some(if timed {
            "bounded four-shard decoy pool is correctness-only; pool construction and worker CPU make elapsed incomparable"
        } else {
            "subscriptions * events exceeds the declared timing gate"
        }),
        parallel_shards: reference.parallel_shards,
        index_lookups: reference.index_lookups,
        index_lookups_per_event: reference.index_lookups as f64 / events.len() as f64,
        index_lookups_per_useful_match: per_match(reference.index_lookups, useful_matches),
        candidate_notifications: expected_notifications,
        notification_bytes,
        server_p50_ms: timings.as_ref().map(|timing| millis(timing.server_p50)),
        server_p95_ms: timings.as_ref().map(|timing| millis(timing.server_p95)),
    })
}

#[allow(clippy::too_many_arguments)]
fn decoy_accounting(
    kernel: &'static str,
    setup: &Setup,
    events: usize,
    useful_matches: usize,
    lookups: usize,
    candidate_notifications: usize,
    notification_bytes: usize,
    timing: Option<&DecoySummary>,
) -> Result<AggregateWorkReport> {
    let subscriptions = setup.ids.len();
    let mut work = AggregateWorkReport::new(
        kernel,
        ComparisonScope {
            workload: "identical live event batch over the same 65,536-bucket domain and registered subscription targets",
            result: "candidate notifications containing useful matches plus decoy false positives",
            public_partition: "one public live collection/event stream; exact event bucket visible to the index server",
            leakage: LeakageScope::CandidateSet {
                candidates: CANDIDATES,
            },
        },
        SecurityLabels {
            privacy: "weaker one-server 100-candidate-set obfuscation; not cryptographic query privacy",
            server_count: 1,
            collusion_tolerance: 0,
            required_answers: 1,
            assumptions: "decoys are plausibly indistinguishable to the server under an external traffic model",
            availability: "one index server answer is required",
            integrity: "ordinary index correctness; no private-verification proof",
        },
    );
    work.global_build = PhaseWork::not_applicable(
        "global build",
        "live inverted-index state is maintained by subscription registrations",
    );
    let mut setup_phase = PhaseWork::unmeasured(
        "registration of the full candidate set",
        "index build wall time is included in the enclosing registration setup, not isolated here",
    );
    setup_phase.logical_selected_bytes = Metric::deterministic(
        subscriptions * CANDIDATES * DECOY_CANDIDATE_BYTES,
        "100 public u32 bucket candidates per subscription",
    );
    setup_phase.client_upload_bytes = Metric::deterministic(
        subscriptions * CANDIDATES * DECOY_CANDIDATE_BYTES,
        "all target/decoy candidate buckets are revealed to the index server",
    );
    setup_phase.client_download_bytes = Metric::deterministic(0, "registration has no result body");
    setup_phase.network_rounds = Metric::estimated(1, "registrations can be batched");
    work.per_client_setup = setup_phase;
    work.maintenance = PhaseWork::not_applicable(
        "event-independent inverted index",
        "events do not mutate registrations; register/unregister changes index memberships",
    );
    let logical_handles = candidate_notifications * SUBSCRIPTION_ID_BYTES;
    let server = &mut work.online.per_server[0];
    server.server_time_p50_ms = timing.map_or_else(
        || Metric::not_measured("workload exceeded the declared timing gate"),
        |timing| Metric::measured(millis(timing.server_p50), "single-server batch median"),
    );
    server.logical_selected_bytes = Metric::deterministic(
        logical_handles,
        "candidate subscription identifier handles returned by index lookups",
    );
    server.physical_or_scanned_bytes = Metric::not_measured("HashMap/cache traffic not measured");
    server.scans = Metric::deterministic(lookups, "public inverted-index lookups");
    work.online.unit = "one declared event batch";
    work.online.aggregate_server_time_p50_ms = timing.map_or_else(
        || Metric::not_measured("workload exceeded the declared timing gate"),
        |timing| Metric::measured(millis(timing.server_p50), "single-server batch median"),
    );
    work.online.max_server_time_p50_ms = timing.map_or_else(
        || Metric::not_measured("workload exceeded the declared timing gate"),
        |timing| Metric::measured(millis(timing.server_p50), "single-server batch median"),
    );
    work.online.aggregate_logical_selected_bytes = Metric::deterministic(
        logical_handles,
        "candidate identifiers including decoy false positives",
    );
    work.online.aggregate_physical_or_scanned_bytes =
        Metric::not_measured("hardware counters absent");
    work.online.server_scans = Metric::deterministic(lookups, "public inverted-index lookups");
    work.online.network_rounds = Metric::deterministic(1, "one candidate-notification batch");
    work.online.useful_result_bytes = Metric::deterministic(
        useful_matches * SUBSCRIPTION_ID_BYTES,
        "true target subscription identifiers only; false positives excluded",
    );
    work.client.online_cpu_p50_ms = Metric::not_measured(
        "client false-positive filtering and payload delivery were not implemented",
    );
    work.client.peak_transient_ram_bytes =
        Metric::not_measured("client candidate filtering excluded");
    work.client.persistent_state_bytes = Metric::not_measured("client candidate map excluded");
    work.client.upload_bytes = Metric::deterministic(0, "events originate at the server/event bus");
    work.client.download_bytes =
        Metric::deterministic(notification_bytes, "candidate subscription identifiers");
    work.persisted_storage.server_bytes_per_server = Metric::estimated(
        setup.decoys.estimated_state_bytes(),
        "HashMap slots and vector capacities; allocator metadata excluded",
    );
    work.persisted_storage.aggregate_server_bytes = Metric::estimated(
        setup.decoys.estimated_state_bytes(),
        "one indexed-decoy server",
    );
    work.persisted_storage.client_bytes = Metric::not_measured("client candidate map excluded");
    work.hardware_counters = unavailable_hardware_counters();
    let _ = events;
    work.validate()?;
    Ok(work)
}

struct CompactSummary {
    aggregate_server_p50: Duration,
    aggregate_server_p95: Duration,
    wall_p50: Duration,
    wall_p95: Duration,
    client_p50: Duration,
}

impl CompactTimings {
    fn summarize(mut self) -> Result<CompactSummary> {
        if self.aggregate_server.is_empty() || self.wall.is_empty() || self.client.is_empty() {
            anyhow::bail!("Compact DPF timing sample set is empty");
        }
        self.aggregate_server.sort_unstable();
        self.wall.sort_unstable();
        self.client.sort_unstable();
        Ok(CompactSummary {
            aggregate_server_p50: percentile(&self.aggregate_server, 50),
            aggregate_server_p95: percentile(&self.aggregate_server, 95),
            wall_p50: percentile(&self.wall, 50),
            wall_p95: percentile(&self.wall, 95),
            client_p50: percentile(&self.client, 50),
        })
    }
}

struct DecoySummary {
    server_p50: Duration,
    server_p95: Duration,
}

impl DecoyTimings {
    fn summarize(mut self) -> Result<DecoySummary> {
        if self.server.is_empty() {
            anyhow::bail!("indexed decoy timing sample set is empty");
        }
        self.server.sort_unstable();
        Ok(DecoySummary {
            server_p50: percentile(&self.server, 50),
            server_p95: percentile(&self.server, 95),
        })
    }
}

fn targets(stream_case: StreamCase, count: usize) -> Vec<usize> {
    match stream_case {
        StreamCase::HitOne | StreamCase::Miss => (0..count)
            .map(|index| {
                if index == 0 {
                    1
                } else {
                    index % (BUCKET_COUNT - 1) + 1
                }
            })
            .enumerate()
            .map(|(index, bucket)| {
                if stream_case == StreamCase::HitOne && index == 0 {
                    0
                } else {
                    bucket
                }
            })
            .collect(),
        StreamCase::Uniform => (0..count)
            .map(|index| uniform_bucket(index as u64, 0x51a7_0001))
            .collect(),
        StreamCase::ZipfLike => {
            let zipf = ZipfSampler::new(4_096, 1.2);
            (0..count)
                .map(|index| zipf.sample(hash64(index as u64 ^ 0x51a7_2001)))
                .collect()
        }
    }
}

fn events(stream_case: StreamCase, count: usize) -> Vec<usize> {
    match stream_case {
        StreamCase::HitOne | StreamCase::Miss => vec![0; count],
        StreamCase::Uniform => (0..count)
            .map(|index| uniform_bucket(index as u64, 0xe7e1_0002))
            .collect(),
        StreamCase::ZipfLike => {
            let zipf = ZipfSampler::new(4_096, 1.2);
            (0..count)
                .map(|index| zipf.sample(hash64(index as u64 ^ 0xe7e1_2002)))
                .collect()
        }
    }
}

fn distribution_note(stream_case: StreamCase) -> &'static str {
    match stream_case {
        StreamCase::HitOne => "every event is bucket zero and exactly one subscription targets zero; one useful match per event",
        StreamCase::Miss => "every event is bucket zero and every target is non-zero; zero useful matches",
        StreamCase::Uniform => "targets and events are independent deterministic uniform hashes over all 65,536 buckets",
        StreamCase::ZipfLike => "targets and events are independent deterministic finite Zipf rank samples with exponent 1.2 over 4,096 hot buckets",
    }
}

fn useful_match_count(targets: &HashMap<SubscriptionId, usize>, events: &[usize]) -> usize {
    let mut histogram = HashMap::<usize, usize>::new();
    for target in targets.values() {
        *histogram.entry(*target).or_default() += 1;
    }
    events
        .iter()
        .map(|event| histogram.get(event).copied().unwrap_or(0))
        .sum()
}

fn decoy_candidates(target: usize, subscription_index: usize) -> Vec<usize> {
    // An odd stride permutes the power-of-two domain, guaranteeing that the
    // first 100 entries are distinct while retaining the true target at j=0.
    let stride = ((subscription_index * 2 + 1) % BUCKET_COUNT) | 1;
    (0..CANDIDATES)
        .map(|candidate| (target + candidate * stride) % BUCKET_COUNT)
        .collect()
}

fn compact_passes(kernel: CompactBatchKernel, subscriptions: usize, events: usize) -> usize {
    match kernel {
        CompactBatchKernel::ExistingEventMajor
        | CompactBatchKernel::PreprocessedEventMajor
        | CompactBatchKernel::ParallelEventShards => events,
        CompactBatchKernel::SubscriptionMajor => subscriptions,
        CompactBatchKernel::CacheBlocked {
            subscription_block,
            event_block: _,
        } => subscriptions.div_ceil(subscription_block),
    }
}

fn per_match(work: usize, matches: usize) -> Option<f64> {
    (matches != 0).then_some(work as f64 / matches as f64)
}

fn uniform_bucket(index: u64, seed: u64) -> usize {
    hash64(index ^ seed) as usize & (BUCKET_COUNT - 1)
}

fn hash64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

struct ZipfSampler {
    cumulative: Vec<f64>,
    total: f64,
}

impl ZipfSampler {
    fn new(ranks: usize, exponent: f64) -> Self {
        let mut total = 0.0;
        let cumulative = (1..=ranks)
            .map(|rank| {
                total += 1.0 / (rank as f64).powf(exponent);
                total
            })
            .collect();
        Self { cumulative, total }
    }

    fn sample(&self, random: u64) -> usize {
        let unit = random as f64 / u64::MAX as f64;
        let target = unit * self.total;
        self.cumulative
            .partition_point(|value| *value < target)
            .min(self.cumulative.len() - 1)
    }
}

fn sample_count(profile: Profile) -> usize {
    match profile {
        Profile::Quick => 3,
        Profile::Full => 9,
    }
}

fn percentile(values: &[Duration], percentile: usize) -> Duration {
    values[((values.len() - 1) * percentile).div_ceil(100)]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_setup() -> Setup {
        build_setup(&[0, 2, 2, 7]).unwrap()
    }

    #[test]
    fn every_compact_kernel_preserves_exact_outputs() {
        let setup = small_setup();
        let left = CompactBatchEvaluator::new(&setup.servers[0], 2).unwrap();
        let right = CompactBatchEvaluator::new(&setup.servers[1], 2).unwrap();
        let events = [0, 1, 2, 7];
        let kernels = [
            CompactBatchKernel::ExistingEventMajor,
            CompactBatchKernel::PreprocessedEventMajor,
            CompactBatchKernel::SubscriptionMajor,
            CompactBatchKernel::CacheBlocked {
                subscription_block: 2,
                event_block: 2,
            },
            CompactBatchKernel::ParallelEventShards,
        ];
        let mut baseline = None;
        for kernel in kernels {
            let left_result = left.evaluate(&events, kernel).unwrap();
            let right_result = right.evaluate(&events, kernel).unwrap();
            verify_compact(&setup, &events, &left_result, &right_result).unwrap();
            if let Some((baseline_left, baseline_right)) = &baseline {
                assert_eq!(&left_result.values, baseline_left);
                assert_eq!(&right_result.values, baseline_right);
            } else {
                baseline = Some((left_result.values, right_result.values));
            }
        }
    }

    #[test]
    fn compact_hit_and_miss_have_identical_fixed_work_and_output_size() {
        let setup = small_setup();
        let evaluator = CompactBatchEvaluator::new(&setup.servers[0], 2).unwrap();
        let hit = evaluator
            .evaluate(&[0], CompactBatchKernel::PreprocessedEventMajor)
            .unwrap();
        let miss = evaluator
            .evaluate(&[99], CompactBatchKernel::PreprocessedEventMajor)
            .unwrap();
        assert_eq!(hit.metrics, miss.metrics);
        assert_eq!(hit.values.len(), setup.ids.len());
        assert_eq!(miss.values.len(), setup.ids.len());
        assert_eq!(
            hit.metrics.point_evaluations,
            setup.ids.len(),
            "one server evaluates every subscription for either event"
        );
    }

    #[test]
    fn indexed_decoy_kernels_preserve_event_order_and_outputs() {
        let setup = small_setup();
        let events = [0, 0, 2, 19, 7, 2];
        let baseline = setup
            .decoys
            .evaluate(&events, DecoyBatchKernel::EventMajor)
            .unwrap();
        for kernel in [
            DecoyBatchKernel::DeduplicatedEvents,
            DecoyBatchKernel::ParallelEventShards,
        ] {
            let candidate = setup.decoys.evaluate(&events, kernel).unwrap();
            assert_eq!(candidate.notifications, baseline.notifications);
            assert_eq!(
                candidate.matching_notifications,
                baseline.matching_notifications
            );
        }
    }

    #[test]
    fn hit_miss_and_distribution_cases_have_declared_semantics() {
        for count in [1, 100, 1_000, 10_000] {
            let hit_targets = targets(StreamCase::HitOne, count);
            assert_eq!(hit_targets.iter().filter(|target| **target == 0).count(), 1);
            assert!(targets(StreamCase::Miss, count)
                .iter()
                .all(|target| *target != 0));
            assert!(targets(StreamCase::Uniform, count)
                .iter()
                .all(|target| *target < BUCKET_COUNT));
            assert!(targets(StreamCase::ZipfLike, count)
                .iter()
                .all(|target| *target < 4_096));
        }
    }

    #[test]
    fn decoy_candidates_are_unique_and_include_target() {
        for index in [0, 1, 99, 9_999] {
            let candidates = decoy_candidates(1234, index);
            assert_eq!(candidates.len(), CANDIDATES);
            assert_eq!(candidates[0], 1234);
            let mut sorted = candidates;
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(sorted.len(), CANDIDATES);
        }
    }
}
