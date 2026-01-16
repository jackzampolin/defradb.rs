//! JWT token encoding functions.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

use crate::error::Error;
use crate::{FullIdentity, Result};

use super::claims::IdentityClaims;
use super::der;

pub(crate) const EDDSA_ALG: &str = "EdDSA";
pub(crate) const ES256K_ALG: &str = "ES256K";

fn build_signing_input(alg: &str, claims: &IdentityClaims) -> Result<String> {
    let header = serde_json::json!({
        "alg": alg,
        "typ": "JWT"
    });
    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());

    let claims_json = serde_json::to_string(claims)
        .map_err(|e| Error::TokenEncoding(format!("failed to serialize claims: {}", e)))?;
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims_json.as_bytes());

    Ok(format!("{}.{}", header_b64, claims_b64))
}

pub(crate) fn encode_ed25519<I: FullIdentity>(
    claims: &IdentityClaims,
    identity: &I,
) -> Result<String> {
    let signing_input = build_signing_input(EDDSA_ALG, claims)?;

    let signature = identity
        .sign(signing_input.as_bytes())
        .map_err(|e| Error::TokenEncoding(format!("signing failed: {}", e)))?;

    let sig_b64 = URL_SAFE_NO_PAD.encode(&signature);
    Ok(format!("{}.{}", signing_input, sig_b64))
}

pub(crate) fn encode_secp256k1<I: FullIdentity>(
    claims: &IdentityClaims,
    identity: &I,
) -> Result<String> {
    let signing_input = build_signing_input(ES256K_ALG, claims)?;

    let signature = identity
        .sign(signing_input.as_bytes())
        .map_err(|e| Error::TokenEncoding(format!("signing failed: {}", e)))?;

    let raw_sig = der::der_to_raw(&signature)?;
    let sig_b64 = URL_SAFE_NO_PAD.encode(&raw_sig);
    Ok(format!("{}.{}", signing_input, sig_b64))
}
