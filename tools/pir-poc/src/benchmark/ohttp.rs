use std::time::Duration;

use anyhow::{Context, Result};
use serde::Serialize;

use super::Profile;
use crate::ohttp_transport::{measure_ohttp_crypto, OhttpCryptoObservation, PaddingStrategy};

const ACTIVE_NULLIFIER_REQUEST_BYTES_PER_REPLICA: usize = 541_241;
const ACTIVE_NULLIFIER_RESPONSE_BYTES_PER_REPLICA: usize = 35_816;
const BILLION_TAG_REQUEST_BYTES_PER_REPLICA: usize = 1_250;
const BILLION_TAG_RESPONSE_BYTES_PER_REPLICA: usize = 19_428_008;
const COMPACT_DPF_REGISTRATION_BYTES_PER_REPLICA: usize = 320;
const COMPACT_DPF_EVENT_BYTES_PER_REPLICA: usize = 126;

#[derive(Clone, Debug, Serialize)]
pub struct OhttpTransportMeasurement {
    pub workload: &'static str,
    pub padding: PaddingStrategy,
    pub samples: usize,
    pub application_request_bytes: usize,
    pub binary_http_request_bytes: usize,
    pub encrypted_request_bytes: usize,
    pub application_response_bytes: usize,
    pub binary_http_response_bytes: usize,
    pub encrypted_response_bytes: usize,
    pub request_wire_over_application: f64,
    pub response_wire_over_application: f64,
    pub client_encode_encrypt_p50_ms: f64,
    pub gateway_decrypt_decode_p50_ms: f64,
    pub gateway_encode_encrypt_p50_ms: f64,
    pub client_decrypt_decode_p50_ms: f64,
    pub total_client_crypto_codec_p50_ms: f64,
    pub total_gateway_crypto_codec_p50_ms: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct OhttpTransportBenchmarkReport {
    pub scope: &'static str,
    pub measurements: Vec<OhttpTransportMeasurement>,
    pub conclusions: Vec<&'static str>,
}

pub fn run(profile: Profile) -> Result<OhttpTransportBenchmarkReport> {
    let samples = match profile {
        Profile::Quick => 3,
        Profile::Full => 11,
    };
    let mut measurements = Vec::new();
    for (workload, request, response) in [
        (
            "Compact DPF representative: registration-sized request and event-sized response",
            COMPACT_DPF_REGISTRATION_BYTES_PER_REPLICA,
            COMPACT_DPF_EVENT_BYTES_PER_REPLICA,
        ),
        (
            "active-nullifier Dense XOR share per replica",
            ACTIVE_NULLIFIER_REQUEST_BYTES_PER_REPLICA,
            ACTIVE_NULLIFIER_RESPONSE_BYTES_PER_REPLICA,
        ),
        (
            "1B-document tag Dense XOR share per replica",
            BILLION_TAG_REQUEST_BYTES_PER_REPLICA,
            BILLION_TAG_RESPONSE_BYTES_PER_REPLICA,
        ),
    ] {
        measurements.push(measure(
            workload,
            request,
            response,
            PaddingStrategy::None,
            samples,
        )?);
        measurements.push(measure(
            workload,
            request,
            response,
            PaddingStrategy::PowerOfTwo { minimum_bytes: 256 },
            samples,
        )?);
    }
    measurements.push(measure(
        "Compact DPF fixed envelope",
        COMPACT_DPF_REGISTRATION_BYTES_PER_REPLICA,
        COMPACT_DPF_EVENT_BYTES_PER_REPLICA,
        PaddingStrategy::Fixed {
            request_bytes: 1_024,
            response_bytes: 1_024,
        },
        samples,
    )?);
    Ok(OhttpTransportBenchmarkReport {
        scope: "RFC 9458 HPKE + Binary HTTP only; excludes PIR evaluation and network latency",
        measurements,
        conclusions: vec![
            "OHTTP changes origin privacy, not PIR server scan work",
            "fixed envelopes equalize success/error sizes but must be route-specific at production scale",
            "power-of-two padding is operationally simple but leaks a public size class",
            "two-server PIR needs independent relay/gateway paths; one shared relay can correlate both shares",
        ],
    })
}

fn measure(
    workload: &'static str,
    request_bytes: usize,
    response_bytes: usize,
    padding: PaddingStrategy,
    samples: usize,
) -> Result<OhttpTransportMeasurement> {
    let observations = measure_ohttp_crypto(request_bytes, response_bytes, padding, samples)
        .with_context(|| format!("benchmark OHTTP workload {workload}"))?;
    let first = observations.first().context("missing OHTTP observation")?;
    let mut client_encode = durations(&observations, |value| value.client_encode_and_encrypt);
    let mut gateway_decode = durations(&observations, |value| value.gateway_decrypt_and_decode);
    let mut gateway_encode = durations(&observations, |value| value.gateway_encode_and_encrypt);
    let mut client_decode = durations(&observations, |value| value.client_decrypt_and_decode);
    client_encode.sort_unstable();
    gateway_decode.sort_unstable();
    gateway_encode.sort_unstable();
    client_decode.sort_unstable();
    let client_encode_p50 = median(&client_encode);
    let gateway_decode_p50 = median(&gateway_decode);
    let gateway_encode_p50 = median(&gateway_encode);
    let client_decode_p50 = median(&client_decode);
    Ok(OhttpTransportMeasurement {
        workload,
        padding,
        samples,
        application_request_bytes: request_bytes,
        binary_http_request_bytes: first.binary_http_request_bytes,
        encrypted_request_bytes: first.encrypted_request_bytes,
        application_response_bytes: response_bytes,
        binary_http_response_bytes: first.binary_http_response_bytes,
        encrypted_response_bytes: first.encrypted_response_bytes,
        request_wire_over_application: ratio(first.encrypted_request_bytes, request_bytes),
        response_wire_over_application: ratio(first.encrypted_response_bytes, response_bytes),
        client_encode_encrypt_p50_ms: millis(client_encode_p50),
        gateway_decrypt_decode_p50_ms: millis(gateway_decode_p50),
        gateway_encode_encrypt_p50_ms: millis(gateway_encode_p50),
        client_decrypt_decode_p50_ms: millis(client_decode_p50),
        total_client_crypto_codec_p50_ms: millis(client_encode_p50 + client_decode_p50),
        total_gateway_crypto_codec_p50_ms: millis(gateway_decode_p50 + gateway_encode_p50),
    })
}

fn durations(
    values: &[OhttpCryptoObservation],
    field: impl Fn(&OhttpCryptoObservation) -> Duration,
) -> Vec<Duration> {
    values.iter().map(field).collect()
}

fn median(values: &[Duration]) -> Duration {
    values[values.len() / 2]
}

fn millis(value: Duration) -> f64 {
    value.as_secs_f64() * 1_000.0
}

fn ratio(wire: usize, application: usize) -> f64 {
    wire as f64 / application.max(1) as f64
}
