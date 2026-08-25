use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;

use super::{active_nullifier, billion_tag, ohttp, Profile};
use crate::subscription::{combine_compact, compact_registration, CompactSubscriptionServer};

const DECOYS: usize = 100;
const EVENT_CAPSULE_BYTES: usize = 188;
const SHINZO_DOMAIN: usize = 1 << 16;

#[derive(Clone, Debug, Serialize)]
pub struct ProtocolResult {
    pub protocol: String,
    pub privacy: String,
    pub server_count: usize,
    pub required_answers: usize,
    pub server_p50_ms: f64,
    pub client_p50_ms: f64,
    pub upload_bytes: usize,
    pub download_bytes: usize,
    pub returned_items: usize,
    pub client_processed_items: usize,
    pub ignored_without_processing: usize,
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct UseCaseComparison {
    pub use_case: String,
    pub strict: ProtocolResult,
    pub decoy_100: ProtocolResult,
    pub strict_server_over_decoy: f64,
    pub decoy_download_over_strict: f64,
    pub decision: String,
}

#[derive(Debug, Serialize)]
pub struct SelectedUseCaseBenchmarkReport {
    pub schema: &'static str,
    pub profile: &'static str,
    pub policy: Vec<&'static str>,
    pub comparisons: Vec<UseCaseComparison>,
    pub active_nullifier: active_nullifier::ActiveNullifierReport,
    pub billion_document_tag: billion_tag::BillionTagReport,
    pub shinzo: ShinzoMeasurement,
    pub ohttp_origin_transport: ohttp::OhttpTransportBenchmarkReport,
}

#[derive(Clone, Debug, Serialize)]
pub struct ShinzoMeasurement {
    pub bucket_domain: usize,
    pub registration_key_bytes: usize,
    pub strict_match_verified: bool,
    pub strict_miss_verified: bool,
    pub strict: ProtocolResult,
    pub decoy_100: ProtocolResult,
}

pub fn run(profile: Profile) -> Result<SelectedUseCaseBenchmarkReport> {
    let active = active_nullifier::run(profile)?;
    let tag = billion_tag::run(profile)?;
    let shinzo = benchmark_shinzo(profile)?;
    let ohttp_origin_transport = ohttp::run(profile)?;

    let active_comparison = UseCaseComparison {
        use_case: "active Shieldd nullifier witness".to_owned(),
        strict: ProtocolResult {
            protocol: active.strict_private_query.protocol.to_owned(),
            privacy: active.strict_private_query.privacy.to_owned(),
            server_count: 2,
            required_answers: 2,
            server_p50_ms: active.strict_private_query.server_p50_ms,
            client_p50_ms: active.strict_private_query.client_p50_ms,
            upload_bytes: active.strict_private_query.upload_bytes,
            download_bytes: active.strict_private_query.download_bytes,
            returned_items: active.strict_private_query.returned_witnesses,
            client_processed_items: active.strict_private_query.client_processed_witnesses,
            ignored_without_processing: 0,
            notes: vec![format!(
                "immutable delta build: {:.3} ms and {} bytes/replica",
                active
                    .committed_block_update
                    .immutable_delta_build_p50_ms,
                active
                    .committed_block_update
                    .immutable_delta_payload_bytes_per_replica
            )],
        },
        decoy_100: ProtocolResult {
            protocol: active.decoy_100_query.protocol.to_owned(),
            privacy: active.decoy_100_query.privacy.to_owned(),
            server_count: 1,
            required_answers: 1,
            server_p50_ms: active.decoy_100_query.server_p50_ms,
            client_p50_ms: active.decoy_100_query.client_p50_ms,
            upload_bytes: active.decoy_100_query.upload_bytes,
            download_bytes: active.decoy_100_query.download_bytes,
            returned_items: active.decoy_100_query.returned_witnesses,
            client_processed_items: active.decoy_100_query.client_processed_witnesses,
            ignored_without_processing: active.decoy_100_query.returned_witnesses
                - active.decoy_100_query.client_processed_witnesses,
            notes: vec!["the client parses only its known target witness".to_owned()],
        },
        strict_server_over_decoy: active.comparison.strict_server_over_decoy,
        decoy_download_over_strict: active.comparison.decoy_download_over_strict,
        decision: "use immutable base/deltas for live updates; strict PIR when candidate-set leakage is unacceptable"
            .to_owned(),
    };

    let tag_comparison = UseCaseComparison {
        use_case: "one-billion-document tag at 0.01% selectivity".to_owned(),
        strict: ProtocolResult {
            protocol: tag.strict_pir.protocol.to_owned(),
            privacy: tag.strict_pir.privacy.to_owned(),
            server_count: 2,
            required_answers: 2,
            server_p50_ms: tag.strict_pir.full_workload_aggregate_server_ms,
            client_p50_ms: tag.strict_pir.full_workload_client_combine_ms
                + tag.strict_pir.full_workload_client_aead_ms,
            upload_bytes: tag.strict_pir.upload_bytes,
            download_bytes: tag.strict_pir.download_bytes,
            returned_items: tag.strict_pir.measured_client_aead_decryptions,
            client_processed_items: tag.strict_pir.measured_client_aead_decryptions,
            ignored_without_processing: 0,
            notes: vec!["one selector is reused over every fixed continuation stripe".to_owned()],
        },
        decoy_100: ProtocolResult {
            protocol: tag.decoy_100.protocol.to_owned(),
            privacy: tag.decoy_100.privacy.to_owned(),
            server_count: 1,
            required_answers: 1,
            server_p50_ms: tag.decoy_100.full_workload_server_copy_ms,
            client_p50_ms: tag.decoy_100.full_workload_client_aead_ms,
            upload_bytes: tag.decoy_100.upload_bytes,
            download_bytes: usize::try_from(tag.decoy_100.download_bytes)?,
            returned_items: tag.decoy_100.returned_encrypted_values,
            client_processed_items: tag.decoy_100.measured_client_aead_decryptions,
            ignored_without_processing: tag.decoy_100.ignored_without_decryption,
            notes: vec!["non-target ciphertext slots are discarded without AEAD".to_owned()],
        },
        strict_server_over_decoy: tag.comparison.strict_server_over_decoy,
        decoy_download_over_strict: tag.comparison.decoy_download_over_strict,
        decision: "use public immutable windows when acceptable; choose strict mode for bandwidth/strong privacy and decoys for minimum server work"
            .to_owned(),
    };

    let shinzo_comparison = UseCaseComparison {
        use_case: "Shinzo live wallet subscription".to_owned(),
        strict: shinzo.strict.clone(),
        decoy_100: shinzo.decoy_100.clone(),
        strict_server_over_decoy: shinzo.strict.server_p50_ms
            / shinzo.decoy_100.server_p50_ms.max(f64::EPSILON),
        decoy_download_over_strict: shinzo.decoy_100.download_bytes as f64
            / shinzo.strict.download_bytes as f64,
        decision: "retain Compact DPF; its current point evaluation is already below transport and delivery overhead"
            .to_owned(),
    };

    Ok(SelectedUseCaseBenchmarkReport {
        schema: "defradb-pir-selected-use-cases-v2",
        profile: profile.as_str(),
        policy: vec![
            "strict and decoy modes share one immutable serving table",
            "strict/decoy ratios are descriptive only because their leakage differs",
            "all results use fixed public result schedules and generation-bound manifests",
            "client decoy work includes only the target slot; other rows are ignored",
        ],
        comparisons: vec![active_comparison, tag_comparison, shinzo_comparison],
        active_nullifier: active,
        billion_document_tag: tag,
        shinzo,
        ohttp_origin_transport,
    })
}

fn benchmark_shinzo(profile: Profile) -> Result<ShinzoMeasurement> {
    let samples = match profile {
        Profile::Quick => 101,
        Profile::Full => 1_001,
    };
    let target = 23_417;
    let miss = target + 1;
    let mut rng = StdRng::seed_from_u64(0x5348_494e_5a4f);
    let registration = compact_registration(target, SHINZO_DOMAIN, &mut rng)?;
    let mut servers = [
        CompactSubscriptionServer::new(0, SHINZO_DOMAIN)?,
        CompactSubscriptionServer::new(1, SHINZO_DOMAIN)?,
    ];
    for (server, key) in servers.iter_mut().zip(&registration.server_keys) {
        server.register(registration.id, key)?;
    }
    let mut strict_server = Vec::with_capacity(samples);
    let mut strict_client = Vec::with_capacity(samples);
    let mut strict_match_verified = false;
    let mut strict_miss_verified = false;
    for sample in 0..samples {
        let bucket = if sample % 2 == 0 { target } else { miss };
        let started = Instant::now();
        let left = servers[0].evaluate_one(registration.id, bucket)?;
        let right = servers[1].evaluate_one(registration.id, bucket)?;
        strict_server.push(started.elapsed());
        let client_started = Instant::now();
        let matched = combine_compact(&[left, right])?;
        strict_client.push(client_started.elapsed());
        if bucket == target {
            strict_match_verified |= matched;
        } else {
            strict_miss_verified |= !matched;
        }
    }
    if !strict_match_verified || !strict_miss_verified {
        bail!("Compact DPF selected-use-case verification failed");
    }

    let mut decoy_index = BTreeMap::<usize, Vec<usize>>::new();
    for candidate in 0..DECOYS {
        let bucket = if candidate == 37 {
            target
        } else {
            (target + candidate * 613 + 1) % SHINZO_DOMAIN
        };
        decoy_index.entry(bucket).or_default().push(candidate);
    }
    let mut decoy_server = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        black_box(decoy_index.get(&target));
        decoy_server.push(started.elapsed());
    }
    strict_server.sort_unstable();
    strict_client.sort_unstable();
    decoy_server.sort_unstable();
    let key_bytes = registration.server_keys.iter().map(Vec::len).sum::<usize>();
    let strict = ProtocolResult {
        protocol: "two-server Compact DPF".to_owned(),
        privacy: "computational target privacy under the AES-PRG DPF construction and two non-colluding hosts".to_owned(),
        server_count: 2,
        required_answers: 2,
        server_p50_ms: millis(median(&strict_server)),
        client_p50_ms: millis(median(&strict_client)),
        upload_bytes: key_bytes,
        download_bytes: 2 * (16 + 16) + EVENT_CAPSULE_BYTES,
        returned_items: 1,
        client_processed_items: 1,
        ignored_without_processing: 0,
        notes: vec!["event framing must remain fixed for both matches and misses".to_owned()],
    };
    let decoy = ProtocolResult {
        protocol: "one-server indexed 100-wallet candidates".to_owned(),
        privacy:
            "candidate-set privacy only; all wallets and longitudinal intersections are visible"
                .to_owned(),
        server_count: 1,
        required_answers: 1,
        server_p50_ms: millis(median(&decoy_server)),
        client_p50_ms: 0.0,
        upload_bytes: DECOYS * 20,
        download_bytes: 16 + EVENT_CAPSULE_BYTES,
        returned_items: 1,
        client_processed_items: 1,
        ignored_without_processing: 0,
        notes: vec![
            "one inverted-index event lookup; registration exposes 100 candidates".to_owned(),
        ],
    };
    Ok(ShinzoMeasurement {
        bucket_domain: SHINZO_DOMAIN,
        registration_key_bytes: key_bytes,
        strict_match_verified,
        strict_miss_verified,
        strict,
        decoy_100: decoy,
    })
}

fn median(values: &[Duration]) -> Duration {
    values[values.len() / 2]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
