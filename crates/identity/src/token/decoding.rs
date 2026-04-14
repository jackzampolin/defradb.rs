//! JWT token decoding and verification functions.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

use crypto::{public_key_from_bytes, KeyType};

use crate::error::Error;
use crate::Result;

use super::claims::IdentityClaims;
use super::der;

struct JwtParts<'a> {
    header: &'a str,
    payload: &'a str,
    signature: &'a str,
}

fn parse_jwt(token: &str) -> Result<JwtParts<'_>> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(Error::TokenDecoding("invalid JWT format".to_string()));
    }
    Ok(JwtParts {
        header: parts[0],
        payload: parts[1],
        signature: parts[2],
    })
}

fn decode_claims(payload_b64: &str) -> Result<IdentityClaims> {
    let claims_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .map_err(|e| Error::TokenDecoding(format!("invalid payload base64: {}", e)))?;

    serde_json::from_slice(&claims_bytes)
        .map_err(|e| Error::TokenDecoding(format!("invalid claims JSON: {}", e)))
}

fn decode_public_key_from_claims(
    claims: &IdentityClaims,
    key_type: KeyType,
) -> Result<Box<dyn crypto::keys::PublicKey>> {
    let pub_key_bytes = hex::decode(&claims.sub).map_err(|e| Error::InvalidClaimValue {
        claim: "sub".to_string(),
        reason: format!("invalid hex: {}", e),
    })?;

    public_key_from_bytes(key_type, &pub_key_bytes).map_err(|e| Error::InvalidClaimValue {
        claim: "sub".to_string(),
        reason: format!("invalid public key: {}", e),
    })
}

fn decode_signature(sig_b64: &str) -> Result<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(sig_b64)
        .map_err(|e| Error::TokenDecoding(format!("invalid signature base64: {}", e)))
}

fn verify_signature(
    public_key: &dyn crypto::keys::PublicKey,
    signing_input: &str,
    signature: &[u8],
) -> Result<()> {
    public_key
        .verify(signing_input.as_bytes(), signature)
        .map(|_| ())
        .map_err(|e| Error::TokenDecoding(format!("verification error: {}", e)))
}

pub(crate) fn decode_ed25519(token: &str) -> Result<IdentityClaims> {
    let jwt = parse_jwt(token)?;
    let claims = decode_claims(jwt.payload)?;
    let public_key = decode_public_key_from_claims(&claims, KeyType::Ed25519)?;
    let signature = decode_signature(jwt.signature)?;

    let signing_input = format!("{}.{}", jwt.header, jwt.payload);
    verify_signature(public_key.as_ref(), &signing_input, &signature)?;

    Ok(claims)
}

pub(crate) fn decode_secp256k1(token: &str) -> Result<IdentityClaims> {
    let jwt = parse_jwt(token)?;
    let claims = decode_claims(jwt.payload)?;
    let public_key = decode_public_key_from_claims(&claims, KeyType::Secp256k1)?;
    let raw_signature = decode_signature(jwt.signature)?;
    let der_signature = der::k256_raw_to_der(&raw_signature)?;

    let signing_input = format!("{}.{}", jwt.header, jwt.payload);
    verify_signature(public_key.as_ref(), &signing_input, &der_signature)?;

    Ok(claims)
}

pub(crate) fn decode_secp256r1(token: &str) -> Result<IdentityClaims> {
    let jwt = parse_jwt(token)?;
    let claims = decode_claims(jwt.payload)?;
    let public_key = decode_public_key_from_claims(&claims, KeyType::Secp256r1)?;
    let raw_signature = decode_signature(jwt.signature)?;
    // ES256 JWT signature is raw R||S (64 bytes), convert to DER for verification.
    let der_signature = der::p256_raw_to_der(&raw_signature)?;

    let signing_input = format!("{}.{}", jwt.header, jwt.payload);
    verify_signature(public_key.as_ref(), &signing_input, &der_signature)?;

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
