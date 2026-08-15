use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;

use super::{
    combine_compact, compact_registration, dense_registration, evaluate_dense,
    CompactSubscriptionServer,
};
use crate::benchmark::Profile;

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
    Ok(FanoutResult {
        subscriptions,
        server_eval_p50_ms: medians.iter().copied().map(millis).collect(),
        subscriptions_evaluated_per_second: medians
            .iter()
            .map(|duration| subscriptions as f64 / duration.as_secs_f64())
            .collect(),
    })
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
