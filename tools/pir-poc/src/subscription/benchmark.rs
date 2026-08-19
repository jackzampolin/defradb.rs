use std::collections::HashMap;
use std::mem::size_of;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;

use super::{
    combine_compact, compact_registration, dense_registration, evaluate_dense,
    CompactSubscriptionServer, OUTPUT_BYTES,
};
use crate::benchmark::accounting::{
    direct_ratio, unavailable_hardware_counters, AggregateWorkReport, AmortizationHorizon,
    ComparisonScope, DirectComparison, LeakageScope, Metric, PhaseWork, SecurityLabels,
};
use crate::benchmark::Profile;

const CANDIDATE_TAGS_PER_SUBSCRIPTION: usize = 100;
const DECOY_WIRE_BUCKET_BYTES: usize = size_of::<u32>();
const INDEX_EVENT_STREAM_BUCKETS: usize = 65_536;
const SUBSCRIPTION_ID_BYTES: usize = 16;
const COMPACT_NOTIFICATION_SHARE_BYTES: usize = SUBSCRIPTION_ID_BYTES + OUTPUT_BYTES;

#[derive(Debug, Serialize)]
pub struct SubscriptionBenchmarkReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: Vec<&'static str>,
    pub dimensions: Vec<SubscriptionDimension>,
    pub three_server: ThreeServerAssessment,
}

#[derive(Debug, Serialize)]
pub struct SubscriptionDimension {
    pub bucket_count: usize,
    pub compact_dpf_key_bytes_per_server: usize,
    pub compact_dpf_total_registration_bytes: usize,
    pub compact_dpf_response_bytes_per_server_per_event: usize,
    pub compact_dpf_client_keygen_p50_us: f64,
    pub compact_dpf_match_eval_p50_us: Vec<f64>,
    pub compact_dpf_miss_eval_p50_us: Vec<f64>,
    pub compact_dpf_client_combine_p50_us: f64,
    pub compact_dpf_fanout: Vec<FanoutResult>,
    pub dense_three_server_key_bytes_per_server: usize,
    pub dense_three_server_total_registration_bytes: usize,
    pub dense_three_server_client_keygen_ms: f64,
    pub dense_three_server_response_bytes_per_server_per_event: usize,
    pub dense_three_server_hot_eval_p50_ns: Vec<f64>,
    pub dense_three_server_aggregate_work: AggregateWorkReport,
}

#[derive(Debug, Serialize)]
pub struct FanoutResult {
    pub aggregate_work: AggregateWorkReport,
    pub subscriptions: usize,
    pub server_eval_p50_ms: Vec<f64>,
    pub subscriptions_evaluated_per_second: Vec<f64>,
    pub response_bytes_per_server_per_event: usize,
    pub total_response_bytes_per_event: usize,
    pub indexed_decoys: IndexedDecoyFanoutResult,
}

#[derive(Debug, Serialize)]
pub struct IndexedDecoyFanoutResult {
    pub aggregate_work: AggregateWorkReport,
    pub privacy: &'static str,
    pub candidate_tags_per_subscription: usize,
    pub registration_bytes_per_subscription: usize,
    pub index_build_ms: f64,
    pub index_memberships: usize,
    pub distinct_index_buckets: usize,
    pub estimated_index_bytes: usize,
    pub matching_event_notifications: usize,
    pub missing_event_notifications: usize,
    pub matching_event_p50_ns: f64,
    pub missing_event_p50_ns: f64,
    pub matching_events_per_second: f64,
    pub missing_events_per_second: f64,
    pub expected_notifications_per_uniform_event: f64,
    pub compact_dpf_sum_server_work_p50_ms: f64,
    pub server_work_comparison: DirectComparison,
}

#[derive(Debug, Serialize)]
pub struct ThreeServerAssessment {
    pub implemented_baseline: &'static str,
    pub privacy: &'static str,
    pub availability: &'static str,
    pub compact_dpf_status: &'static str,
    pub rejected_shortcut: &'static str,
    pub production_direction: &'static str,
}

pub fn run(profile: Profile) -> Result<SubscriptionBenchmarkReport> {
    let dimensions = dimensions(profile)
        .into_iter()
        .map(|bucket_count| benchmark_dimension(bucket_count, profile))
        .collect::<Result<Vec<_>>>()?;
    Ok(SubscriptionBenchmarkReport {
        protocol: "compact-dpf-live-subscriptions",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: vec![
            "Release build; all measurements are in-process and exclude network latency.",
            "Compact DPF evaluates one point per registered subscription for every event.",
            "Fanout measures a whole server event pass, including result-vector allocation.",
            "The 100-candidate decoy baseline is a one-server public inverted index: registration inserts one target and 99 decoy bucket memberships per logical subscription, then each event performs one hash-index lookup rather than 100 lookups per subscription.",
            "Decoy hit measurements rotate over up to 65,536 distinct indexed buckets and clone one matching internal subscription handle; misses rotate over up to 65,536 absent buckets and return an empty vector. Both exclude event decoding, transport, persistence, and delivery of the event payload.",
            "Decoy index bytes estimate Rust HashMap slots, control bytes, and membership-vector capacity, but excludes allocator metadata. Its deterministic collision-free registrations model one matching subscriber and maximize distinct index buckets.",
            "The dense three-server result is a hot single-key lower bound: one indexed bit read per server and event; it does not model a multi-gigabyte subscription-key working set.",
            "Results are medians; black_box prevents benchmark result elimination.",
        ],
        dimensions,
        three_server: ThreeServerAssessment {
            implemented_baseline: "n-server dense XOR one-hot subscription shares (measured with 3 servers)",
            privacy: "the target remains private if at least one of the n servers does not collude",
            availability: "all n result shares are required; this baseline improves collusion tolerance, not availability",
            compact_dpf_status: "the selected fss-rs construction is intrinsically two-party; true 3-party DPF requires a different construction and implementation",
            rejected_shortcut: "placing independent 2-party DPF pairs on AB, AC, and BC lets any colluding pair reconstruct one complete key pair",
            production_direction: "keep a server-count-agnostic interface; adopt an audited threshold/multi-party DPF only after its trust, key-size, and evaluation tradeoffs are benchmarked",
        },
    })
}

fn dimensions(profile: Profile) -> Vec<usize> {
    match profile {
        Profile::Quick => vec![1 << 20, 1 << 22],
        Profile::Full => vec![1 << 20, 1 << 22, 1 << 24],
    }
}

fn benchmark_dimension(bucket_count: usize, profile: Profile) -> Result<SubscriptionDimension> {
    let target = bucket_count / 3;
    let miss = target + 1;
    let keygen_samples = match profile {
        Profile::Quick => 31,
        Profile::Full => 101,
    };
    let point_samples = match profile {
        Profile::Quick => 101,
        Profile::Full => 1_001,
    };
    let mut rng = StdRng::seed_from_u64(bucket_count as u64 ^ 0xd0f5);

    let mut keygen = Vec::with_capacity(keygen_samples);
    let mut registration = None;
    for _ in 0..keygen_samples {
        let started = Instant::now();
        let generated = compact_registration(target, bucket_count, &mut rng)?;
        keygen.push(started.elapsed());
        registration = Some(generated);
    }
    let registration = registration.expect("at least one key-generation sample");
    keygen.sort_unstable();
    let compact_key_bytes = registration.server_keys[0].len();
    if registration.server_keys[1].len() != compact_key_bytes {
        bail!("Compact DPF key shares have different encoded sizes");
    }

    let mut servers = [
        CompactSubscriptionServer::new(0, bucket_count)?,
        CompactSubscriptionServer::new(1, bucket_count)?,
    ];
    for (server, key) in servers.iter_mut().zip(&registration.server_keys) {
        server.register(registration.id, key)?;
    }

    let mut match_times = [
        Vec::with_capacity(point_samples),
        Vec::with_capacity(point_samples),
    ];
    let mut miss_times = [
        Vec::with_capacity(point_samples),
        Vec::with_capacity(point_samples),
    ];
    let mut combine_times = Vec::with_capacity(point_samples);
    for _ in 0..point_samples {
        let mut shares = Vec::with_capacity(2);
        for (server_index, server) in servers.iter().enumerate() {
            let started = Instant::now();
            shares.push(std::hint::black_box(
                server.evaluate_one(registration.id, target)?,
            ));
            match_times[server_index].push(started.elapsed());
        }
        let started = Instant::now();
        if !std::hint::black_box(combine_compact(&shares)?) {
            bail!("Compact DPF benchmark target did not match");
        }
        combine_times.push(started.elapsed());

        for (server_index, server) in servers.iter().enumerate() {
            let started = Instant::now();
            std::hint::black_box(server.evaluate_one(registration.id, miss)?);
            miss_times[server_index].push(started.elapsed());
        }
    }
    for times in match_times.iter_mut().chain(miss_times.iter_mut()) {
        times.sort_unstable();
    }
    combine_times.sort_unstable();
    let compact_keygen_p50_us = micros(percentile(&keygen, 50));
    let compact_combine_p50_us = micros(percentile(&combine_times, 50));

    let fanout_counts: &[usize] = match profile {
        Profile::Quick => &[1, 100, 1_000],
        Profile::Full => &[1, 1_000, 10_000],
    };
    let fanout = fanout_counts
        .iter()
        .map(|&count| {
            benchmark_fanout(
                bucket_count,
                target,
                count,
                compact_key_bytes,
                compact_keygen_p50_us,
                compact_combine_p50_us,
                profile,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    let dense_keygen_started = Instant::now();
    let dense = dense_registration(target, bucket_count, 3, &mut rng)?;
    let dense_keygen_ms = millis(dense_keygen_started.elapsed());
    let dense_ns_per_eval = dense
        .server_keys
        .iter()
        .map(|key| benchmark_dense_evaluation(key, bucket_count, profile))
        .collect::<Result<Vec<_>>>()?;
    let dense_three_server_aggregate_work = dense_live_accounting(
        dense.server_keys[0].len(),
        dense_keygen_ms,
        &dense_ns_per_eval,
    )?;

    Ok(SubscriptionDimension {
        bucket_count,
        compact_dpf_key_bytes_per_server: compact_key_bytes,
        compact_dpf_total_registration_bytes: compact_key_bytes * 2,
        compact_dpf_response_bytes_per_server_per_event: COMPACT_NOTIFICATION_SHARE_BYTES,
        compact_dpf_client_keygen_p50_us: compact_keygen_p50_us,
        compact_dpf_match_eval_p50_us: match_times
            .iter()
            .map(|times| micros(percentile(times, 50)))
            .collect(),
        compact_dpf_miss_eval_p50_us: miss_times
            .iter()
            .map(|times| micros(percentile(times, 50)))
            .collect(),
        compact_dpf_client_combine_p50_us: compact_combine_p50_us,
        compact_dpf_fanout: fanout,
        dense_three_server_key_bytes_per_server: dense.server_keys[0].len(),
        dense_three_server_total_registration_bytes: dense.server_keys[0].len() * 3,
        dense_three_server_client_keygen_ms: dense_keygen_ms,
        dense_three_server_response_bytes_per_server_per_event: 1,
        dense_three_server_hot_eval_p50_ns: dense_ns_per_eval,
        dense_three_server_aggregate_work,
    })
}

fn benchmark_dense_evaluation(key: &[u8], bucket_count: usize, profile: Profile) -> Result<f64> {
    const EVALUATIONS_PER_SAMPLE: usize = 10_000;
    let samples = match profile {
        Profile::Quick => 31,
        Profile::Full => 101,
    };
    let mut timings = Vec::with_capacity(samples);
    for sample in 0..samples {
        let started = Instant::now();
        for index in 0..EVALUATIONS_PER_SAMPLE {
            let bucket = (sample * 65_537 + index * 7_919) & (bucket_count - 1);
            std::hint::black_box(evaluate_dense(
                std::hint::black_box(key),
                std::hint::black_box(bucket),
                bucket_count,
            )?);
        }
        timings.push(started.elapsed());
    }
    timings.sort_unstable();
    Ok(nanos(percentile(&timings, 50)) / EVALUATIONS_PER_SAMPLE as f64)
}

fn benchmark_fanout(
    bucket_count: usize,
    event_bucket: usize,
    subscriptions: usize,
    compact_key_bytes: usize,
    compact_keygen_p50_us: f64,
    compact_combine_p50_us: f64,
    profile: Profile,
) -> Result<FanoutResult> {
    let mut rng = StdRng::seed_from_u64(bucket_count as u64 ^ subscriptions as u64 ^ 0xfa40);
    let mut servers = [
        CompactSubscriptionServer::new(0, bucket_count)?,
        CompactSubscriptionServer::new(1, bucket_count)?,
    ];
    for index in 0..subscriptions {
        let target = (event_bucket + index * 7_919) % bucket_count;
        let registration = compact_registration(target, bucket_count, &mut rng)?;
        for (server, key) in servers.iter_mut().zip(&registration.server_keys) {
            server.register(registration.id, key)?;
        }
    }
    let samples = match profile {
        Profile::Quick => 11,
        Profile::Full => 31,
    };
    let mut timings = [Vec::with_capacity(samples), Vec::with_capacity(samples)];
    for (server_index, server) in servers.iter().enumerate() {
        for _ in 0..samples {
            let started = Instant::now();
            let shares = server.evaluate_event(event_bucket)?;
            std::hint::black_box(shares);
            timings[server_index].push(started.elapsed());
        }
        timings[server_index].sort_unstable();
    }
    let medians = timings
        .iter()
        .map(|times| percentile(times, 50))
        .collect::<Vec<_>>();
    let aggregate_work = compact_dpf_live_accounting(
        subscriptions,
        compact_key_bytes,
        compact_keygen_p50_us,
        compact_combine_p50_us,
        &medians,
    )?;
    let indexed_decoys = benchmark_indexed_decoys(
        bucket_count,
        event_bucket,
        subscriptions,
        profile,
        &medians,
        &aggregate_work,
    )?;
    Ok(FanoutResult {
        aggregate_work,
        subscriptions,
        server_eval_p50_ms: medians.iter().copied().map(millis).collect(),
        subscriptions_evaluated_per_second: medians
            .iter()
            .map(|duration| subscriptions as f64 / duration.as_secs_f64())
            .collect(),
        response_bytes_per_server_per_event: subscriptions * COMPACT_NOTIFICATION_SHARE_BYTES,
        total_response_bytes_per_event: subscriptions * COMPACT_NOTIFICATION_SHARE_BYTES * 2,
        indexed_decoys,
    })
}

struct IndexedDecoyServer {
    subscriptions_by_bucket: HashMap<usize, Vec<u32>>,
    memberships: usize,
}

impl IndexedDecoyServer {
    fn with_capacity(memberships: usize) -> Self {
        Self {
            subscriptions_by_bucket: HashMap::with_capacity(memberships),
            memberships: 0,
        }
    }

    fn register(&mut self, subscription: u32, buckets: impl IntoIterator<Item = usize>) {
        for bucket in buckets {
            self.subscriptions_by_bucket
                .entry(bucket)
                .or_default()
                .push(subscription);
            self.memberships += 1;
        }
    }

    fn evaluate_event(&self, event_bucket: usize) -> Vec<u32> {
        self.subscriptions_by_bucket
            .get(&event_bucket)
            .cloned()
            .unwrap_or_default()
    }

    fn estimated_index_bytes(&self) -> usize {
        let table_bytes = self.subscriptions_by_bucket.capacity()
            * (size_of::<usize>() + size_of::<Vec<u32>>() + 1);
        let membership_bytes = self
            .subscriptions_by_bucket
            .values()
            .map(|subscriptions| subscriptions.capacity() * size_of::<u32>())
            .sum::<usize>();
        table_bytes + membership_bytes
    }
}

fn benchmark_indexed_decoys(
    bucket_count: usize,
    event_bucket: usize,
    subscriptions: usize,
    profile: Profile,
    compact_medians: &[Duration],
    compact_work: &AggregateWorkReport,
) -> Result<IndexedDecoyFanoutResult> {
    let memberships = subscriptions * CANDIDATE_TAGS_PER_SUBSCRIPTION;
    if memberships >= bucket_count {
        bail!(
            "decoy benchmark needs {memberships} distinct memberships outside one event bucket, but the domain has only {bucket_count} buckets"
        );
    }

    let build_started = Instant::now();
    let mut server = IndexedDecoyServer::with_capacity(memberships);
    for subscription in 0..subscriptions {
        let first_ordinal = subscription * CANDIDATE_TAGS_PER_SUBSCRIPTION;
        let buckets = (0..CANDIDATE_TAGS_PER_SUBSCRIPTION).map(|candidate| {
            let ordinal = first_ordinal + candidate;
            if ordinal == 0 {
                event_bucket
            } else {
                non_event_bucket(event_bucket, ordinal - 1, bucket_count)
            }
        });
        server.register(
            u32::try_from(subscription).expect("benchmark subscription count fits in u32"),
            buckets,
        );
    }
    let index_build_ms = millis(build_started.elapsed());
    let matching_buckets = server
        .subscriptions_by_bucket
        .keys()
        .copied()
        .take(INDEX_EVENT_STREAM_BUCKETS)
        .collect::<Vec<_>>();
    let missing_buckets = (0..bucket_count)
        .filter(|bucket| !server.subscriptions_by_bucket.contains_key(bucket))
        .take(INDEX_EVENT_STREAM_BUCKETS)
        .collect::<Vec<_>>();
    let matching_notifications = server.evaluate_event(event_bucket).len();
    let missing_notifications = server.evaluate_event(missing_buckets[0]).len();
    if matching_notifications != 1 || missing_notifications != 0 {
        bail!("indexed-decoy benchmark constructed an invalid hit/miss workload");
    }

    let matching_median = benchmark_indexed_events(&server, &matching_buckets, profile);
    let missing_median = benchmark_indexed_events(&server, &missing_buckets, profile);
    let compact_sum = compact_medians.iter().sum::<Duration>();
    let aggregate_work = indexed_decoy_live_accounting(
        subscriptions,
        memberships,
        server.estimated_index_bytes(),
        index_build_ms,
        matching_notifications,
        matching_median,
    )?;
    let server_work_comparison = direct_ratio(
        "indexed-decoy server work over compact-DPF server work",
        compact_work,
        &aggregate_work,
        millis(compact_sum),
        millis(matching_median),
    );

    Ok(IndexedDecoyFanoutResult {
        aggregate_work,
        privacy: "candidate-set privacy only: the server sees all 100 buckets and which candidate generated each notification",
        candidate_tags_per_subscription: CANDIDATE_TAGS_PER_SUBSCRIPTION,
        registration_bytes_per_subscription: CANDIDATE_TAGS_PER_SUBSCRIPTION
            * DECOY_WIRE_BUCKET_BYTES,
        index_build_ms,
        index_memberships: server.memberships,
        distinct_index_buckets: server.subscriptions_by_bucket.len(),
        estimated_index_bytes: server.estimated_index_bytes(),
        matching_event_notifications: matching_notifications,
        missing_event_notifications: missing_notifications,
        matching_event_p50_ns: nanos(matching_median),
        missing_event_p50_ns: nanos(missing_median),
        matching_events_per_second: 1.0 / matching_median.as_secs_f64(),
        missing_events_per_second: 1.0 / missing_median.as_secs_f64(),
        expected_notifications_per_uniform_event: memberships as f64 / bucket_count as f64,
        compact_dpf_sum_server_work_p50_ms: millis(compact_sum),
        server_work_comparison,
    })
}

fn live_result_scope(leakage: LeakageScope) -> ComparisonScope {
    ComparisonScope {
        workload: "one event evaluated against the identical registered subscription set",
        result: "the set of matching internal subscription identifiers",
        public_partition: "one public event bucket",
        leakage,
    }
}

fn compact_dpf_live_accounting(
    subscriptions: usize,
    key_bytes_per_server: usize,
    keygen_p50_us: f64,
    combine_p50_us: f64,
    server_medians: &[Duration],
) -> Result<AggregateWorkReport> {
    let server_count = server_medians.len();
    let mut work = AggregateWorkReport::new(
        "compact-dpf-live-subscriptions",
        live_result_scope(LeakageScope::ExactQueryPrivacy),
        SecurityLabels {
            privacy: "exact subscription-point privacy under the DPF security assumption",
            server_count,
            collusion_tolerance: 1,
            required_answers: server_count,
            assumptions: "the two DPF servers do not collude and the PRG remains secure",
            availability: "both output shares are required",
            integrity: "no malicious-server verification",
        },
    );
    work.global_build = PhaseWork::not_applicable(
        "global database build",
        "live DPF evaluates registered keys and has no snapshot-wide build",
    );
    work.per_client_setup = PhaseWork::unmeasured(
        "registration of the benchmark subscription set",
        "registration insertion time and peak RAM were not measured",
    );
    work.per_client_setup.client_time_ms = Metric::estimated(
        keygen_p50_us * subscriptions as f64 / 1_000.0,
        "one measured median key generation multiplied by subscription count",
    );
    work.per_client_setup.logical_selected_bytes = Metric::deterministic(
        key_bytes_per_server * server_count * subscriptions,
        "all generated server key shares",
    );
    work.per_client_setup.client_upload_bytes = Metric::deterministic(
        key_bytes_per_server * server_count * subscriptions,
        "registration key shares uploaded once",
    );
    work.per_client_setup.client_download_bytes =
        Metric::deterministic(0, "registration has no measured response payload");
    work.per_client_setup.server_scans = Metric::deterministic(
        0,
        "registration appends keys; it does not scan subscriptions",
    );
    work.per_client_setup.network_rounds = Metric::deterministic(1, "one registration round");
    work.maintenance = PhaseWork::unmeasured(
        "subscription change",
        "deregistration and key replacement were not benchmarked",
    );
    let mut aggregate_ms = 0.0;
    let mut max_ms = 0.0f64;
    for (server, median) in work.online.per_server.iter_mut().zip(server_medians) {
        let elapsed_ms = millis(*median);
        aggregate_ms += elapsed_ms;
        max_ms = max_ms.max(elapsed_ms);
        let logical = key_bytes_per_server * subscriptions;
        server.server_time_p50_ms =
            Metric::measured(elapsed_ms, "one server's full event-pass median");
        server.logical_selected_bytes =
            Metric::deterministic(logical, "registered DPF key bytes evaluated for the event");
        server.physical_or_scanned_bytes = Metric::not_measured(
            "DPF evaluation includes HashMap traversal, PRG expansion, and output allocation; physical bytes require hardware counters",
        );
        server.scans =
            Metric::deterministic(1, "one sequential pass over all registered subscriptions");
    }
    let logical_aggregate = key_bytes_per_server * subscriptions * server_count;
    work.online.unit = "one event against the registered subscription set";
    work.online.aggregate_server_time_p50_ms = Metric::estimated(
        aggregate_ms,
        "sum of separately measured per-server medians, not a joint-sample median",
    );
    work.online.max_server_time_p50_ms =
        Metric::estimated(max_ms, "maximum of separately measured per-server medians");
    work.online.aggregate_logical_selected_bytes =
        Metric::deterministic(logical_aggregate, "sum of DPF key bytes across servers");
    work.online.aggregate_physical_or_scanned_bytes = Metric::not_measured(
        "DPF HashMap, PRG, allocator, cache-line, and DRAM traffic require hardware counters",
    );
    work.online.server_scans =
        Metric::deterministic(server_count, "one subscription pass per server");
    work.online.network_rounds =
        Metric::deterministic(1, "one server-originated notification delivery round");
    work.online.useful_result_bytes = Metric::deterministic(
        SUBSCRIPTION_ID_BYTES,
        "the constructed fanout workload has exactly one matching subscription identifier",
    );
    work.client.online_cpu_p50_ms = Metric::estimated(
        combine_p50_us * subscriptions as f64 / 1_000.0,
        "one measured share-combine median multiplied by subscription count; identifier correlation and transport decoding were not measured",
    );
    work.client.persistent_state_bytes =
        Metric::not_measured("client subscription bookkeeping was not instrumented");
    work.client.upload_bytes = Metric::deterministic(0, "events originate at servers");
    work.client.download_bytes = Metric::estimated(
        subscriptions * COMPACT_NOTIFICATION_SHARE_BYTES * server_count,
        "one 16-byte subscription identifier and 16-byte output share per subscription and server; framing is not implemented",
    );
    work.persisted_storage.server_bytes_per_server = Metric::deterministic(
        key_bytes_per_server * subscriptions,
        "registered DPF key shares",
    );
    work.persisted_storage.aggregate_server_bytes = Metric::deterministic(
        key_bytes_per_server * subscriptions * server_count,
        "sum of DPF key shares across servers",
    );
    work.persisted_storage.client_bytes =
        Metric::not_measured("client subscription bookkeeping was not instrumented");
    work.amortization = AmortizationHorizon {
        global_build: "not applicable",
        per_client_setup: "events delivered during the registered subscription lifetime",
        maintenance: "events between subscription changes",
        assumed_global_queries: None,
        assumed_queries_per_client_setup: None,
        assumed_online_events_per_maintenance: None,
        note: "Registration is separated from per-event work; choose an expected event lifetime before amortizing it.",
    };
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    Ok(work)
}

fn indexed_decoy_live_accounting(
    subscriptions: usize,
    memberships: usize,
    index_bytes: usize,
    index_build_ms: f64,
    matching_notifications: usize,
    matching_median: Duration,
) -> Result<AggregateWorkReport> {
    let mut work = AggregateWorkReport::new(
        "indexed-100-decoy-live-subscriptions",
        live_result_scope(LeakageScope::CandidateSet {
            candidates: CANDIDATE_TAGS_PER_SUBSCRIPTION,
        }),
        SecurityLabels {
            privacy: "candidate-set privacy only",
            server_count: 1,
            collusion_tolerance: 0,
            required_answers: 1,
            assumptions: "each target is hidden only among its registered public candidates",
            availability: "one index server answer is required",
            integrity: "ordinary unauthenticated index notification",
        },
    );
    work.global_build = PhaseWork::not_applicable(
        "global database build",
        "the live inverted index is populated by registrations",
    );
    work.per_client_setup = PhaseWork::unmeasured(
        "registration of the benchmark subscription set",
        "only aggregate index build time and deterministic registration bytes were recorded",
    );
    work.per_client_setup.aggregate_server_time_ms =
        Metric::measured(index_build_ms, "build all benchmark index memberships");
    work.per_client_setup.logical_selected_bytes = Metric::deterministic(
        memberships * DECOY_WIRE_BUCKET_BYTES,
        "all public candidate bucket memberships",
    );
    work.per_client_setup.client_upload_bytes = Metric::deterministic(
        subscriptions * CANDIDATE_TAGS_PER_SUBSCRIPTION * DECOY_WIRE_BUCKET_BYTES,
        "100 public bucket candidates per subscription",
    );
    work.per_client_setup.client_download_bytes =
        Metric::deterministic(0, "registration has no measured response payload");
    work.per_client_setup.server_scans =
        Metric::deterministic(0, "hash-index insertions, not a table scan");
    work.per_client_setup.network_rounds = Metric::deterministic(1, "one registration round");
    work.maintenance = PhaseWork::unmeasured(
        "subscription change",
        "membership removal and replacement were not benchmarked",
    );
    let online_ms = millis(matching_median);
    let logical = matching_notifications * size_of::<u32>();
    let server = &mut work.online.per_server[0];
    server.server_time_p50_ms = Metric::measured(online_ms, "matching hash-index event median");
    server.logical_selected_bytes =
        Metric::deterministic(logical, "matching internal subscription handles");
    server.physical_or_scanned_bytes = Metric::not_measured(
        "HashMap probes and allocator/cache traffic require hardware counters",
    );
    server.scans = Metric::deterministic(0, "one indexed hash lookup, not a scan");
    work.online.unit = "one matching event against the registered subscription set";
    work.online.aggregate_server_time_p50_ms = Metric::measured(online_ms, "single index server");
    work.online.max_server_time_p50_ms = Metric::measured(online_ms, "single index server");
    work.online.aggregate_logical_selected_bytes =
        Metric::deterministic(logical, "matching handles returned by the index");
    work.online.aggregate_physical_or_scanned_bytes =
        Metric::not_measured("no perf counter was available for HashMap and allocator traffic");
    work.online.server_scans = Metric::deterministic(0, "indexed lookup only");
    work.online.network_rounds =
        Metric::deterministic(1, "one server-originated notification delivery round");
    work.online.useful_result_bytes =
        Metric::deterministic(logical, "matching internal subscription identifiers");
    work.client.online_cpu_p50_ms =
        Metric::not_measured("client notification handling was excluded");
    work.client.persistent_state_bytes =
        Metric::not_measured("client subscription bookkeeping was excluded");
    work.client.upload_bytes = Metric::deterministic(0, "events originate at the server");
    work.client.download_bytes =
        Metric::deterministic(logical, "matching internal subscription identifiers");
    work.persisted_storage.server_bytes_per_server = Metric::estimated(
        index_bytes,
        "HashMap slots and vector capacity; allocator metadata excluded",
    );
    work.persisted_storage.aggregate_server_bytes =
        Metric::estimated(index_bytes, "single index server");
    work.persisted_storage.client_bytes =
        Metric::not_measured("client subscription bookkeeping was excluded");
    work.amortization = AmortizationHorizon {
        global_build: "not applicable",
        per_client_setup: "events delivered during the registered subscription lifetime",
        maintenance: "events between subscription changes",
        assumed_global_queries: None,
        assumed_queries_per_client_setup: None,
        assumed_online_events_per_maintenance: None,
        note: "Registration/index build is not folded into per-event work.",
    };
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    Ok(work)
}

fn dense_live_accounting(
    key_bytes_per_server: usize,
    keygen_ms: f64,
    server_p50_ns: &[f64],
) -> Result<AggregateWorkReport> {
    let server_count = server_p50_ns.len();
    let mut work = AggregateWorkReport::new(
        "dense-xor-live-subscription-key",
        ComparisonScope {
            workload: "one event evaluated against one registered subscription",
            result: "one logical match boolean",
            public_partition: "one public event bucket",
            leakage: LeakageScope::ExactQueryPrivacy,
        },
        SecurityLabels {
            privacy: "exact information-theoretic subscription-point privacy",
            server_count,
            collusion_tolerance: server_count - 1,
            required_answers: server_count,
            assumptions: "at least one server does not collude",
            availability: "all output shares are required",
            integrity: "no malicious-server verification",
        },
    );
    work.global_build = PhaseWork::not_applicable(
        "global database build",
        "live Dense evaluates registered selector shares",
    );
    work.per_client_setup = PhaseWork::unmeasured(
        "one subscription registration",
        "registration insertion and transient RAM were not measured",
    );
    work.per_client_setup.client_time_ms =
        Metric::measured(keygen_ms, "generate all Dense selector shares");
    work.per_client_setup.logical_selected_bytes =
        Metric::deterministic(key_bytes_per_server * server_count, "all selector shares");
    work.per_client_setup.client_upload_bytes = Metric::deterministic(
        key_bytes_per_server * server_count,
        "one full selector share per server",
    );
    work.per_client_setup.client_download_bytes =
        Metric::deterministic(0, "registration has no measured response payload");
    work.per_client_setup.server_scans = Metric::deterministic(0, "key registration only");
    work.per_client_setup.network_rounds = Metric::deterministic(1, "one registration round");
    work.maintenance = PhaseWork::unmeasured(
        "subscription change",
        "selector removal and replacement were not benchmarked",
    );
    let mut aggregate_ms = 0.0;
    let mut max_ms = 0.0f64;
    for (server, nanoseconds) in work.online.per_server.iter_mut().zip(server_p50_ns) {
        let milliseconds = nanoseconds / 1_000_000.0;
        aggregate_ms += milliseconds;
        max_ms = max_ms.max(milliseconds);
        server.server_time_p50_ms =
            Metric::measured(milliseconds, "one indexed selector-bit read median");
        server.logical_selected_bytes = Metric::deterministic(1, "one packed selector byte read");
        server.physical_or_scanned_bytes = Metric::estimated(
            1,
            "logical byte read only; actual cache-line and working-set traffic was not measured",
        );
        server.scans = Metric::deterministic(0, "one indexed bit test, not a selector scan");
    }
    work.online.unit = "one event against one registered subscription";
    work.online.aggregate_server_time_p50_ms = Metric::estimated(
        aggregate_ms,
        "sum of separately measured per-server medians, not a joint-sample median",
    );
    work.online.max_server_time_p50_ms =
        Metric::estimated(max_ms, "maximum of separately measured per-server medians");
    work.online.aggregate_logical_selected_bytes =
        Metric::deterministic(server_count, "one selector byte per server");
    work.online.aggregate_physical_or_scanned_bytes = Metric::estimated(
        server_count,
        "logical byte reads only; no cache-line or hardware-counter measurement",
    );
    work.online.server_scans = Metric::deterministic(0, "indexed reads only");
    work.online.network_rounds =
        Metric::deterministic(1, "one server-originated notification delivery round");
    work.online.useful_result_bytes = Metric::deterministic(1, "one match boolean");
    work.client.online_cpu_p50_ms =
        Metric::not_measured("three-share client combine was not benchmarked in this baseline");
    work.client.persistent_state_bytes = Metric::deterministic(0, "no mutable client PIR state");
    work.client.upload_bytes = Metric::deterministic(0, "events originate at servers");
    work.client.download_bytes =
        Metric::deterministic(server_count, "one output byte share per server");
    work.persisted_storage.server_bytes_per_server =
        Metric::deterministic(key_bytes_per_server, "one Dense selector share");
    work.persisted_storage.aggregate_server_bytes = Metric::deterministic(
        key_bytes_per_server * server_count,
        "sum of selector shares across servers",
    );
    work.persisted_storage.client_bytes = Metric::deterministic(0, "no mutable PIR state");
    work.amortization = AmortizationHorizon {
        global_build: "not applicable",
        per_client_setup: "events delivered during one subscription lifetime",
        maintenance: "events between subscription changes",
        assumed_global_queries: None,
        assumed_queries_per_client_setup: None,
        assumed_online_events_per_maintenance: None,
        note: "Registration upload and stored selector scale with the public bucket domain; online is one indexed bit read.",
    };
    work.hardware_counters = unavailable_hardware_counters();
    work.validate()?;
    Ok(work)
}

fn non_event_bucket(event_bucket: usize, ordinal: usize, bucket_count: usize) -> usize {
    let bucket = ordinal % (bucket_count - 1);
    if bucket >= event_bucket {
        bucket + 1
    } else {
        bucket
    }
}

fn benchmark_indexed_events(
    server: &IndexedDecoyServer,
    event_buckets: &[usize],
    profile: Profile,
) -> Duration {
    let samples = match profile {
        Profile::Quick => 31,
        Profile::Full => 101,
    };
    let mut timings = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        for event_bucket in event_buckets {
            std::hint::black_box(server.evaluate_event(std::hint::black_box(*event_bucket)));
        }
        timings.push(started.elapsed());
    }
    timings.sort_unstable();
    percentile(&timings, 50) / u32::try_from(event_buckets.len()).expect("event stream fits in u32")
}

fn percentile(values: &[Duration], percentile: usize) -> Duration {
    let index = ((values.len() - 1) * percentile).div_ceil(100);
    values[index]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn nanos(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexed_decoy_server_returns_only_matching_subscriptions() {
        let mut server = IndexedDecoyServer::with_capacity(6);
        server.register(7, [3, 5, 8]);
        server.register(9, [5, 13, 21]);

        assert_eq!(server.evaluate_event(3), vec![7]);
        assert_eq!(server.evaluate_event(5), vec![7, 9]);
        assert!(server.evaluate_event(34).is_empty());
        assert_eq!(server.memberships, 6);
    }

    #[test]
    fn non_event_bucket_enumerates_every_other_bucket_once() {
        let event_bucket = 3;
        let mut buckets = (0..7)
            .map(|ordinal| non_event_bucket(event_bucket, ordinal, 8))
            .collect::<Vec<_>>();
        buckets.sort_unstable();

        assert_eq!(buckets, vec![0, 1, 2, 4, 5, 6, 7]);
    }

    #[test]
    fn compact_accounting_includes_identifiers_needed_to_correlate_shares() {
        let work = compact_dpf_live_accounting(
            2,
            100,
            1.0,
            1.0,
            &[Duration::from_micros(10), Duration::from_micros(12)],
        )
        .unwrap();

        assert_eq!(
            work.client.download_bytes.value,
            Some(2 * COMPACT_NOTIFICATION_SHARE_BYTES * 2)
        );
        assert_eq!(
            work.client.download_bytes.evidence,
            crate::benchmark::accounting::Evidence::Estimated
        );
        assert_eq!(
            work.online.useful_result_bytes.value,
            Some(SUBSCRIPTION_ID_BYTES)
        );
        serde_json::to_string(&work).unwrap();
    }
}
