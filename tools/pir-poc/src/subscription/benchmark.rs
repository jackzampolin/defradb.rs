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
use crate::benchmark::Profile;

const CANDIDATE_TAGS_PER_SUBSCRIPTION: usize = 100;
const DECOY_WIRE_BUCKET_BYTES: usize = size_of::<u32>();
const INDEX_EVENT_STREAM_BUCKETS: usize = 65_536;

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
}

#[derive(Debug, Serialize)]
pub struct FanoutResult {
    pub subscriptions: usize,
    pub server_eval_p50_ms: Vec<f64>,
    pub subscriptions_evaluated_per_second: Vec<f64>,
    pub response_bytes_per_server_per_event: usize,
    pub total_response_bytes_per_event: usize,
    pub indexed_decoys: IndexedDecoyFanoutResult,
}

#[derive(Debug, Serialize)]
pub struct IndexedDecoyFanoutResult {
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
    pub compact_dpf_to_decoy_server_work_ratio: f64,
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

    let fanout_counts: &[usize] = match profile {
        Profile::Quick => &[1, 100, 1_000],
        Profile::Full => &[1, 1_000, 10_000],
    };
    let fanout = fanout_counts
        .iter()
        .map(|&count| benchmark_fanout(bucket_count, target, count, profile))
        .collect::<Result<Vec<_>>>()?;

    let dense_keygen_started = Instant::now();
    let dense = dense_registration(target, bucket_count, 3, &mut rng)?;
    let dense_keygen_ms = millis(dense_keygen_started.elapsed());
    let dense_ns_per_eval = dense
        .server_keys
        .iter()
        .map(|key| benchmark_dense_evaluation(key, bucket_count, profile))
        .collect::<Result<Vec<_>>>()?;

    Ok(SubscriptionDimension {
        bucket_count,
        compact_dpf_key_bytes_per_server: compact_key_bytes,
        compact_dpf_total_registration_bytes: compact_key_bytes * 2,
        compact_dpf_response_bytes_per_server_per_event: 16,
        compact_dpf_client_keygen_p50_us: micros(percentile(&keygen, 50)),
        compact_dpf_match_eval_p50_us: match_times
            .iter()
            .map(|times| micros(percentile(times, 50)))
            .collect(),
        compact_dpf_miss_eval_p50_us: miss_times
            .iter()
            .map(|times| micros(percentile(times, 50)))
            .collect(),
        compact_dpf_client_combine_p50_us: micros(percentile(&combine_times, 50)),
        compact_dpf_fanout: fanout,
        dense_three_server_key_bytes_per_server: dense.server_keys[0].len(),
        dense_three_server_total_registration_bytes: dense.server_keys[0].len() * 3,
        dense_three_server_client_keygen_ms: dense_keygen_ms,
        dense_three_server_response_bytes_per_server_per_event: 1,
        dense_three_server_hot_eval_p50_ns: dense_ns_per_eval,
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
    let indexed_decoys =
        benchmark_indexed_decoys(bucket_count, event_bucket, subscriptions, profile, &medians)?;
    Ok(FanoutResult {
        subscriptions,
        server_eval_p50_ms: medians.iter().copied().map(millis).collect(),
        subscriptions_evaluated_per_second: medians
            .iter()
            .map(|duration| subscriptions as f64 / duration.as_secs_f64())
            .collect(),
        response_bytes_per_server_per_event: subscriptions * OUTPUT_BYTES,
        total_response_bytes_per_event: subscriptions * OUTPUT_BYTES * 2,
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
    let compact_to_decoy_ratio = compact_sum.as_secs_f64() / matching_median.as_secs_f64();

    Ok(IndexedDecoyFanoutResult {
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
        compact_dpf_to_decoy_server_work_ratio: compact_to_decoy_ratio,
    })
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
}
