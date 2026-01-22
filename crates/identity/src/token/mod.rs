//! JWT bearer token generation and verification.
//!
//! This module provides functions for creating and verifying JWT bearer tokens
//! for identity authentication. Tokens are signed using the identity's private key
//! with EdDSA (for Ed25519) or ES256K (for secp256k1).
//!
//! Both algorithms are implemented manually since the jsonwebtoken crate requires
//! specific key formats (PKCS#8 DER) that differ from our crypto library's raw format.

mod claims;
mod decoding;
mod der;
mod encoding;
mod identity;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crypto::{public_key_from_bytes, KeyType};

use crate::did::Did;
use crate::error::Error;
use crate::key_type::IdentityKeyType;
use crate::{FullIdentity, Result};

/// Default clock skew tolerance in seconds.
/// This allows for minor time differences between token issuer and verifier.
pub const DEFAULT_CLOCK_SKEW_SECONDS: u64 = 60;

pub use claims::IdentityClaims;
use decoding::{decode_ed25519, decode_secp256k1, parse_algorithm};
use encoding::{encode_ed25519, encode_secp256k1, EDDSA_ALG, ES256K_ALG};
pub use identity::TokenIdentity;

/// Generates a new JWT bearer token for the given identity.
///
/// # Parameters
/// * `identity` - The full identity (with private key) to generate the token for
/// * `duration` - How long the token should be valid
/// * `audience` - Optional audience claim for the token
/// * `authorized_account` - Optional authorized account to include in the token
///
/// # Returns
/// The JWT token as a byte vector.
///
/// # Errors
/// Returns an error if token encoding fails.
pub fn new_token<I: FullIdentity>(
    identity: &I,
    duration: Duration,
    audience: Option<String>,
    authorized_account: Option<String>,
) -> Result<Vec<u8>> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::TokenEncoding(format!("system time error: {}", e)))?;

    let exp = now + duration;
    let pub_key = identity.pub_key();
    let did = identity.did()?;
    let key_type = pub_key.key_type();

    let identity_key_type = IdentityKeyType::try_from(key_type)?;

    let claims = IdentityClaims {
        sub: pub_key.to_hex_string(),
        iss: did.to_string(),
        exp: exp.as_secs(),
        nbf: now.as_secs(),
        iat: now.as_secs(),
        aud: audience.map(|a| vec![a]),
        authorized_account,
        key_type: identity_key_type.to_string(),
    };

    let token = match key_type {
        KeyType::Ed25519 => encode_ed25519(&claims, identity)?,
        KeyType::Secp256k1 => encode_secp256k1(&claims, identity)?,
        KeyType::Secp256r1 => return Err(Error::UnsupportedKeyType(KeyType::Secp256r1)),
    };

    Ok(token.into_bytes())
}

/// Verifies a bearer token against an expected audience.
///
/// Uses `DEFAULT_CLOCK_SKEW_SECONDS` (60 seconds) for time tolerance.
/// For custom tolerance, use `verify_auth_token_with_skew`.
///
/// # Parameters
/// * `identity` - The token identity to verify
/// * `expected_audience` - The expected audience value
///
/// # Returns
/// Ok(()) if verification succeeds.
///
/// # Errors
/// Returns an error if:
/// * The token is not yet valid (nbf is in the future, accounting for clock skew)
/// * The token has expired (accounting for clock skew)
/// * The audience doesn't match
pub fn verify_auth_token(identity: &TokenIdentity, expected_audience: &str) -> Result<()> {
    verify_auth_token_with_skew(identity, expected_audience, DEFAULT_CLOCK_SKEW_SECONDS)
}

/// Verifies a bearer token against an expected audience with custom clock skew tolerance.
///
/// Clock skew tolerance accounts for minor time differences between the token issuer
/// and verifier systems. In distributed systems, clocks may not be perfectly synchronized.
///
/// # Parameters
/// * `identity` - The token identity to verify
/// * `expected_audience` - The expected audience value
/// * `clock_skew_seconds` - Tolerance in seconds for time differences (0 for strict checking)
///
/// # Returns
/// Ok(()) if verification succeeds.
///
/// # Errors
/// Returns an error if:
/// * The token is not yet valid (nbf is in the future beyond tolerance)
/// * The token has expired (beyond tolerance)
/// * The audience doesn't match
pub fn verify_auth_token_with_skew(
    identity: &TokenIdentity,
    expected_audience: &str,
    clock_skew_seconds: u64,
) -> Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::TokenDecoding(format!("system time error: {}", e)))?
        .as_secs();

    // Allow clock skew tolerance for nbf (not-before) check
    // Token is invalid if nbf is more than clock_skew_seconds in the future
    if identity.claims.nbf > now.saturating_add(clock_skew_seconds) {
        return Err(Error::TokenNotYetValid {
            nbf: identity.claims.nbf,
            now,
        });
    }

    // Allow clock skew tolerance for exp (expiration) check
    // Token is expired if exp is more than clock_skew_seconds in the past
    if identity.claims.exp.saturating_add(clock_skew_seconds) < now {
        return Err(Error::TokenExpired {
            exp: identity.claims.exp,
            now,
        });
    }

    if let Some(ref audiences) = identity.claims.aud {
        if !audiences.contains(&expected_audience.to_string()) {
            return Err(Error::AudienceMismatch {
                expected: expected_audience.to_string(),
                actual: audiences.clone(),
            });
        }
    } else {
        return Err(Error::AudienceMismatch {
            expected: expected_audience.to_string(),
            actual: vec![],
        });
    }

    Ok(())
}

/// Extracts an identity from a JWT bearer token.
///
/// # Parameters
/// * `data` - The JWT token bytes
///
/// # Returns
/// A TokenIdentity that can be used for verification but not signing.
///
/// # Errors
/// Returns an error if:
/// * The token cannot be decoded (invalid base64 or UTF-8)
/// * The claims cannot be deserialized (malformed JSON or missing fields)
/// * The signature verification fails
/// * The header algorithm doesn't match the key_type claim
/// * The issuer claim doesn't match the DID derived from the public key
/// * The key type is unsupported
/// * The public key cannot be reconstructed from the sub claim
pub fn from_token(data: &[u8]) -> Result<TokenIdentity> {
    let token_str = std::str::from_utf8(data)
        .map_err(|e| Error::TokenDecoding(format!("invalid UTF-8: {}", e)))?;

    // ES256K isn't in jsonwebtoken's Algorithm enum, so we parse manually
    let header_alg = parse_algorithm(token_str)?;

    let claims: IdentityClaims = match header_alg.as_str() {
        "EdDSA" => decode_ed25519(token_str)?,
        "ES256K" => decode_secp256k1(token_str)?,
        alg => {
            return Err(Error::TokenDecoding(format!(
                "unsupported algorithm: {}",
                alg
            )))
        }
    };

    let key_type: IdentityKeyType = claims.key_type.parse()?;

    let expected_alg = match key_type {
        IdentityKeyType::Ed25519 => EDDSA_ALG,
        IdentityKeyType::Secp256k1 => ES256K_ALG,
    };
    if header_alg != expected_alg {
        return Err(Error::TokenDecoding(format!(
            "algorithm mismatch: header specifies '{}' but key_type claim is '{}' (expected '{}')",
            header_alg, claims.key_type, expected_alg
        )));
    }

    let public_key = public_key_from_bytes(
        key_type.to_crypto_key_type(),
        &hex::decode(&claims.sub).map_err(|e| Error::InvalidClaimValue {
            claim: "sub".to_string(),
            reason: format!("invalid hex: {}", e),
        })?,
    )
    .map_err(|e| Error::InvalidClaimValue {
        claim: "sub".to_string(),
        reason: format!("invalid public key: {}", e),
    })?;

    let did_string = public_key.did().map_err(|e| Error::InvalidClaimValue {
        claim: "sub".to_string(),
        reason: format!("failed to derive DID: {}", e),
    })?;
    let did = Did::new_unchecked(did_string);

    let did_str = did.to_string();
    if claims.iss != did_str {
        return Err(Error::InvalidClaimValue {
            claim: "iss".to_string(),
            reason: format!(
                "issuer '{}' does not match DID derived from public key '{}'",
                claims.iss, did_str
            ),
        });
    }

    Ok(TokenIdentity {
        public_key,
        did,
        bearer_token: token_str.to_string(),
        authorized_account: claims.authorized_account.clone(),
        key_type,
        claims,
    })
}
