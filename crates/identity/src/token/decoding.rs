//! JWT token decoding and verification functions.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

use crypto::{public_key_from_bytes, KeyType};

use crate::error::Error;
use crate::Result;

use super::claims::IdentityClaims;
use super::der;

pub(crate) fn decode_ed25519(token: &str) -> Result<IdentityClaims> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(Error::TokenDecoding("invalid JWT format".to_string()));
    }

    let claims_bytes = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| Error::TokenDecoding(format!("invalid payload base64: {}", e)))?;

    let claims: IdentityClaims = serde_json::from_slice(&claims_bytes)
        .map_err(|e| Error::TokenDecoding(format!("invalid claims JSON: {}", e)))?;

    let pub_key_bytes = hex::decode(&claims.sub).map_err(|e| Error::InvalidClaimValue {
        claim: "sub".to_string(),
        reason: format!("invalid hex: {}", e),
    })?;

    // Ed25519 signatures are raw 64 bytes
    let sig_bytes = URL_SAFE_NO_PAD
        .decode(parts[2])
        .map_err(|e| Error::TokenDecoding(format!("invalid signature base64: {}", e)))?;

    let public_key = public_key_from_bytes(KeyType::Ed25519, &pub_key_bytes).map_err(|e| {
        Error::InvalidClaimValue {
            claim: "sub".to_string(),
            reason: format!("invalid public key: {}", e),
        }
    })?;

    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let verified = public_key
        .verify(signing_input.as_bytes(), &sig_bytes)
        .map_err(|e| Error::TokenDecoding(format!("verification error: {}", e)))?;

    if !verified {
        return Err(Error::TokenDecoding(
            "signature verification failed".to_string(),
        ));
    }

    Ok(claims)
}

pub(crate) fn decode_secp256k1(token: &str) -> Result<IdentityClaims> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(Error::TokenDecoding("invalid JWT format".to_string()));
    }

    let claims_bytes = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| Error::TokenDecoding(format!("invalid payload base64: {}", e)))?;

    let claims: IdentityClaims = serde_json::from_slice(&claims_bytes)
        .map_err(|e| Error::TokenDecoding(format!("invalid claims JSON: {}", e)))?;

    let pub_key_bytes = hex::decode(&claims.sub).map_err(|e| Error::InvalidClaimValue {
        claim: "sub".to_string(),
        reason: format!("invalid hex: {}", e),
    })?;

    let sig_bytes = URL_SAFE_NO_PAD
        .decode(parts[2])
        .map_err(|e| Error::TokenDecoding(format!("invalid signature base64: {}", e)))?;

    // JWT uses raw R||S format, convert back to DER for verification
    let der_sig = der::raw_to_der(&sig_bytes)?;

    let public_key = public_key_from_bytes(KeyType::Secp256k1, &pub_key_bytes).map_err(|e| {
        Error::InvalidClaimValue {
            claim: "sub".to_string(),
            reason: format!("invalid public key: {}", e),
        }
    })?;

    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let verified = public_key
        .verify(signing_input.as_bytes(), &der_sig)
        .map_err(|e| Error::TokenDecoding(format!("verification error: {}", e)))?;

    if !verified {
        return Err(Error::TokenDecoding(
            "signature verification failed".to_string(),
        ));
    }

    Ok(claims)
}

pub(crate) fn parse_algorithm(token: &str) -> Result<String> {
    let header_part = token
        .split('.')
        .next()
        .ok_or_else(|| Error::TokenDecoding("invalid JWT format".to_string()))?;

    let header_bytes = URL_SAFE_NO_PAD
        .decode(header_part)
        .map_err(|e| Error::TokenDecoding(format!("invalid header base64: {}", e)))?;

    let header: serde_json::Value = serde_json::from_slice(&header_bytes)
        .map_err(|e| Error::TokenDecoding(format!("invalid header JSON: {}", e)))?;

    header["alg"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| Error::MissingClaim("alg".to_string()))
}
