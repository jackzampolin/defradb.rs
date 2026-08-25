use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde::Serialize;

use crate::ohttp_transport::{
    spawn_ohttp_replica, OhttpUseCaseClient, OriginTransportConfig, PaddingStrategy,
    RelayMetricsSnapshot,
};
use crate::selected::{
    EncryptedTagBuildRecord, NullifierBuildRecord, PocLimits, TableUseCase, UseCaseBuildInput,
    UseCaseStore,
};
use crate::selected_http::{spawn_selected, UseCaseClient};
use crate::verification::{build_demo_witnesses, encrypt_projection, verify_nullifier_witness};

const DEMO_OPERATOR_KEY: [u8; 32] = [0x44; 32];
const DEMO_PROJECTION_KEY: [u8; 32] = [0x50; 32];
const TOR_SOCKS_ENV: &str = "PIR_POC_TOR_SOCKS_URL";
const TRANSPORT_QUERY_SAMPLES: usize = 11;

#[derive(Debug, Serialize)]
pub struct SelectedDemoReport {
    pub schema: &'static str,
    pub generation: String,
    pub nullifier_witness_bytes: usize,
    pub encrypted_tag_values: usize,
    pub decoy_returned_rows: usize,
    pub decoy_processed_rows: usize,
    pub decoy_ignored_without_decoding: usize,
    pub shinzo_match: bool,
    pub shinzo_miss: bool,
    pub endpoints: Vec<&'static str>,
    pub ohttp_origin_hiding: OhttpDemoReport,
    pub transport_comparison: Vec<TransportObservation>,
}

#[derive(Debug, Serialize)]
pub struct OhttpDemoReport {
    pub topology: &'static str,
    pub padding: PaddingStrategy,
    pub nullifier_lookup_verified: bool,
    pub encrypted_tag_lookup_verified: bool,
    pub shinzo_live_match_verified: bool,
    pub replica_0_relay: RelayMetricsSnapshot,
    pub replica_1_relay: RelayMetricsSnapshot,
}

#[derive(Debug, Serialize)]
pub struct TransportObservation {
    pub path: &'static str,
    pub status: String,
    pub server_count: usize,
    pub hides_query: bool,
    pub hides_origin_from_provider: bool,
    pub setup_ms: Option<f64>,
    pub verified_query_p50_ms: Option<f64>,
    pub query_samples: usize,
    pub note: &'static str,
}

pub fn demo_input() -> Result<UseCaseBuildInput> {
    let nullifier_values = (1..=128)
        .map(|value| {
            let mut nullifier = [0; 32];
            nullifier[0] = value;
            nullifier
        })
        .collect::<Vec<_>>();
    let (root, witnesses) = build_demo_witnesses(&nullifier_values)?;
    let nullifiers = nullifier_values
        .iter()
        .zip(witnesses)
        .map(|(nullifier, (position, witness))| NullifierBuildRecord {
            nullifier_hex: hex::encode(nullifier),
            position,
            witness_base64: STANDARD.encode(witness),
        })
        .collect();
    let mut rng = StdRng::seed_from_u64(0x0050_524f_4a45_4354);
    let encrypted_tags = (0..100)
        .map(|value| {
            let tag = format!("tag-{value}").into_bytes();
            let plaintext = format!("projection-{value}").into_bytes();
            Ok(EncryptedTagBuildRecord {
                tag_base64: STANDARD.encode(&tag),
                encrypted_values_base64: vec![STANDARD.encode(encrypt_projection(
                    &DEMO_PROJECTION_KEY,
                    42,
                    &root,
                    &tag,
                    0,
                    &plaintext,
                    &mut rng,
                )?)],
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(UseCaseBuildInput {
        generation_height: 42,
        generation_root_hex: hex::encode(root),
        nullifiers,
        encrypted_tags,
        shinzo_bucket_count: 1 << 16,
        limits: PocLimits::default(),
    })
}

pub async fn run() -> Result<SelectedDemoReport> {
    let input = demo_input()?;
    let left = Arc::new(UseCaseStore::build(input.clone(), &DEMO_OPERATOR_KEY, 0)?);
    let right = Arc::new(UseCaseStore::build(input, &DEMO_OPERATOR_KEY, 1)?);
    let left_server = spawn_selected(Arc::clone(&left), "127.0.0.1:0").await?;
    let right_server = spawn_selected(Arc::clone(&right), "127.0.0.1:0").await?;
    let urls = [
        format!("http://{}", left_server.address),
        format!("http://{}", right_server.address),
    ];
    let mut nullifier = [0; 32];
    nullifier[0] = 7;
    let direct_setup = Instant::now();
    let client = UseCaseClient::connect(&urls, &DEMO_OPERATOR_KEY).await?;
    let direct_setup_ms = elapsed_ms(direct_setup);
    let mut direct_query_samples = Vec::with_capacity(TRANSPORT_QUERY_SAMPLES);
    let mut witness = None;
    for _ in 0..TRANSPORT_QUERY_SAMPLES {
        let query = Instant::now();
        witness = client.verified_nullifier_lookup(&nullifier).await?;
        direct_query_samples.push(elapsed_ms(query));
    }
    let witness = witness.ok_or_else(|| anyhow::anyhow!("demo nullifier was not found"))?;
    let direct_query_p50_ms = median(&mut direct_query_samples);
    let tag_values = client
        .verified_tag_lookup(b"tag-37", &DEMO_PROJECTION_KEY)
        .await?
        .ok_or_else(|| anyhow::anyhow!("demo encrypted tag was not found"))?;
    let candidates = (0..100)
        .map(|value| format!("tag-{value}").into_bytes())
        .collect::<Vec<_>>();
    let decoy = client
        .decoy_lookup(TableUseCase::EncryptedTag, b"tag-37", &candidates)
        .await?;
    let shinzo_match = client.subscribe_and_evaluate(12_345, 12_345).await?;
    let shinzo_miss = client.subscribe_and_evaluate(23_456, 23_457).await?;

    // A separate relay and gateway per replica avoids giving one relay both
    // Dense selector shares.  Loopback HTTP is used only by this executable;
    // production deployment requires HTTPS on both OHTTP hops.
    let left_ohttp = spawn_ohttp_replica(left, &DEMO_OPERATOR_KEY, "replica-0", 1).await?;
    let right_ohttp = spawn_ohttp_replica(right, &DEMO_OPERATOR_KEY, "replica-1", 2).await?;
    let ohttp_padding = PaddingStrategy::Fixed {
        request_bytes: 4_096,
        response_bytes: 65_536,
    };
    let ohttp_setup = Instant::now();
    let ohttp_client = OhttpUseCaseClient::connect(
        &[left_ohttp.relay_url(), right_ohttp.relay_url()],
        &DEMO_OPERATOR_KEY,
        ohttp_padding,
    )
    .await?;
    let ohttp_setup_ms = elapsed_ms(ohttp_setup);
    let mut ohttp_query_samples = Vec::with_capacity(TRANSPORT_QUERY_SAMPLES);
    let mut ohttp_nullifier = None;
    for _ in 0..TRANSPORT_QUERY_SAMPLES {
        let query = Instant::now();
        ohttp_nullifier = ohttp_client.verified_nullifier_lookup(&nullifier).await?;
        ohttp_query_samples.push(elapsed_ms(query));
    }
    let ohttp_nullifier =
        ohttp_nullifier.ok_or_else(|| anyhow::anyhow!("OHTTP demo nullifier was not found"))?;
    let ohttp_query_p50_ms = median(&mut ohttp_query_samples);
    let ohttp_tag = ohttp_client
        .verified_tag_lookup(b"tag-37", &DEMO_PROJECTION_KEY)
        .await?
        .ok_or_else(|| anyhow::anyhow!("OHTTP demo tag was not found"))?;
    let ohttp_shinzo = ohttp_client.subscribe_and_evaluate(34_567, 34_567).await?;

    let public_setup = Instant::now();
    let public_client = UseCaseClient::connect_decoy(&urls[0], &DEMO_OPERATOR_KEY).await?;
    let public_setup_ms = elapsed_ms(public_setup);
    let mut public_query_samples = Vec::with_capacity(TRANSPORT_QUERY_SAMPLES);
    for _ in 0..TRANSPORT_QUERY_SAMPLES {
        let query = Instant::now();
        let public = public_client
            .decoy_lookup(TableUseCase::Nullifier, &nullifier, &[nullifier.to_vec()])
            .await?;
        let public_witness = public
            .values
            .and_then(|mut values| (values.len() == 1).then(|| values.remove(0)))
            .ok_or_else(|| anyhow::anyhow!("visible demo lookup returned no canonical witness"))?;
        verify_nullifier_witness(
            &nullifier,
            &public_witness,
            &client
                .metadata
                .manifest
                .manifest
                .active_generation
                .manifest
                .root,
        )?;
        public_query_samples.push(elapsed_ms(query));
    }
    let public_query_p50_ms = median(&mut public_query_samples);

    let mut transport_comparison = vec![
        TransportObservation {
            path: "visible direct HTTP",
            status: "verified".to_owned(),
            server_count: 1,
            hides_query: false,
            hides_origin_from_provider: false,
            setup_ms: Some(public_setup_ms),
            verified_query_p50_ms: Some(public_query_p50_ms),
            query_samples: TRANSPORT_QUERY_SAMPLES,
            note: "one visible candidate through the public/decoy endpoint",
        },
        TransportObservation {
            path: "PIR direct HTTP",
            status: "verified".to_owned(),
            server_count: 2,
            hides_query: true,
            hides_origin_from_provider: false,
            setup_ms: Some(direct_setup_ms),
            verified_query_p50_ms: Some(direct_query_p50_ms),
            query_samples: TRANSPORT_QUERY_SAMPLES,
            note: "Dense XOR hides the target but both replicas see the wallet address",
        },
        TransportObservation {
            path: "PIR OHTTP",
            status: "verified".to_owned(),
            server_count: 2,
            hides_query: true,
            hides_origin_from_provider: true,
            setup_ms: Some(ohttp_setup_ms),
            verified_query_p50_ms: Some(ohttp_query_p50_ms),
            query_samples: TRANSPORT_QUERY_SAMPLES,
            note: "independent OHTTP relay/gateway path per PIR replica",
        },
    ];
    transport_comparison.push(
        tor_observation(
            &[left_ohttp.relay_url(), right_ohttp.relay_url()],
            ohttp_padding,
            &nullifier,
        )
        .await,
    );

    Ok(SelectedDemoReport {
        schema: "defradb-pir-selected-demo-v2",
        generation: hex::encode(client.metadata.manifest.manifest.body_digest),
        nullifier_witness_bytes: witness.len(),
        encrypted_tag_values: tag_values.len(),
        decoy_returned_rows: decoy.returned_rows,
        decoy_processed_rows: decoy.processed_rows,
        decoy_ignored_without_decoding: decoy.ignored_without_decoding,
        shinzo_match,
        shinzo_miss: !shinzo_miss,
        endpoints: vec![
            "/v1/nullifier/private",
            "/v1/nullifier/decoy",
            "/v1/tag/private",
            "/v1/tag/decoy",
            "/v1/shinzo/register",
            "/v1/shinzo/event",
        ],
        ohttp_origin_hiding: OhttpDemoReport {
            topology: "client -> independent relay -> replica gateway, once per PIR replica",
            padding: ohttp_padding,
            nullifier_lookup_verified: ohttp_nullifier == witness,
            encrypted_tag_lookup_verified: ohttp_tag == tag_values,
            shinzo_live_match_verified: ohttp_shinzo,
            replica_0_relay: left_ohttp.metrics.snapshot(),
            replica_1_relay: right_ohttp.metrics.snapshot(),
        },
        transport_comparison,
    })
}

async fn tor_observation(
    relay_urls: &[String],
    padding: PaddingStrategy,
    nullifier: &[u8; 32],
) -> TransportObservation {
    let Ok(proxy_url) = std::env::var(TOR_SOCKS_ENV) else {
        return TransportObservation {
            path: "PIR Tor + OHTTP",
            status: format!("not run: set {TOR_SOCKS_ENV}=socks5h://127.0.0.1:9050"),
            server_count: 2,
            hides_query: true,
            hides_origin_from_provider: true,
            setup_ms: None,
            verified_query_p50_ms: None,
            query_samples: 0,
            note: "requires a real Tor/Arti SOCKS listener; the POC does not fake Tor numbers",
        };
    };
    let transports = vec![
        OriginTransportConfig::TorSocks5 {
            proxy_url: proxy_url.clone(),
        },
        OriginTransportConfig::TorSocks5 { proxy_url },
    ];
    let setup = Instant::now();
    let client = OhttpUseCaseClient::connect_with_transport_configs(
        relay_urls,
        &DEMO_OPERATOR_KEY,
        padding,
        &transports,
    )
    .await;
    let setup_ms = elapsed_ms(setup);
    let Ok(client) = client else {
        return TransportObservation {
            path: "PIR Tor + OHTTP",
            status: "failed to connect through configured Tor SOCKS listener".to_owned(),
            server_count: 2,
            hides_query: true,
            hides_origin_from_provider: true,
            setup_ms: Some(setup_ms),
            verified_query_p50_ms: None,
            query_samples: 0,
            note: "local loopback relays may not be reachable through a Tor exit; deploy remote HTTPS relays for a real run",
        };
    };
    let mut samples = Vec::with_capacity(TRANSPORT_QUERY_SAMPLES);
    let mut verified = true;
    for _ in 0..TRANSPORT_QUERY_SAMPLES {
        let query = Instant::now();
        verified &= matches!(
            client.verified_nullifier_lookup(nullifier).await,
            Ok(Some(_))
        );
        samples.push(elapsed_ms(query));
    }
    let verified_query_p50_ms = median(&mut samples);
    TransportObservation {
        path: "PIR Tor + OHTTP",
        status: if verified {
            "verified".to_owned()
        } else {
            "query failed verification".to_owned()
        },
        server_count: 2,
        hides_query: true,
        hides_origin_from_provider: true,
        setup_ms: Some(setup_ms),
        verified_query_p50_ms: Some(verified_query_p50_ms),
        query_samples: TRANSPORT_QUERY_SAMPLES,
        note:
            "Tor hides the wallet from the OHTTP relay; OHTTP keeps PIR bytes opaque to Tor relays",
    }
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000.0
}

fn median(samples: &mut [f64]) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}
