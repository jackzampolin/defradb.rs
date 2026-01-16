//! JWT bearer token generation and verification.
//!
//! This module provides functions for creating and verifying JWT bearer tokens
//! for identity authentication. Tokens are signed using the identity's private key
//! with EdDSA (for Ed25519) or ES256K (for secp256k1).
//!
//! Note: Both EdDSA and ES256K are implemented manually since the jsonwebtoken crate
//! requires specific key formats (PKCS#8 DER) that differ from our crypto library's
//! raw key format.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};

use crypto::keys::PublicKey;
use crypto::{public_key_from_bytes, KeyType};

use crate::did::Did;
use crate::error::Error;
use crate::key_type::IdentityKeyType;
use crate::{FullIdentity, Identity, Result};

/// EdDSA algorithm name for Ed25519.
const EDDSA_ALG: &str = "EdDSA";

/// ES256K algorithm name for secp256k1.
const ES256K_ALG: &str = "ES256K";

/// JWT claims structure for identity tokens.
///
/// Contains standard JWT claims plus custom claims for identity-specific data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityClaims {
    /// Subject: hex-encoded public key
    pub sub: String,

    /// Issuer: DID of the identity
    pub iss: String,

    /// Expiration time (Unix timestamp)
    pub exp: u64,

    /// Not before time (Unix timestamp)
    pub nbf: u64,

    /// Issued at time (Unix timestamp)
    pub iat: u64,

    /// Audience (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aud: Option<Vec<String>>,

    /// Authorized account (optional)
    #[serde(rename = "authorized_account", skip_serializing_if = "Option::is_none")]
    pub authorized_account: Option<String>,

    /// Key type (ed25519 or secp256k1)
    pub key_type: String,
}

/// An identity extracted from a JWT token.
///
/// TokenIdentity only has access to the public key and cannot sign new tokens
/// since it doesn't have the private key.
pub struct TokenIdentity {
    public_key: Box<dyn PublicKey>,
    did: Did,
    bearer_token: String,
    authorized_account: Option<String>,
    key_type: IdentityKeyType,
    claims: IdentityClaims,
}

impl TokenIdentity {
    /// Returns the bearer token string.
    pub fn bearer_token(&self) -> &str {
        &self.bearer_token
    }

    /// Returns the authorized account if present.
    pub fn authorized_account(&self) -> Option<&str> {
        self.authorized_account.as_deref()
    }

    /// Returns the key type of this identity.
    pub fn key_type(&self) -> IdentityKeyType {
        self.key_type
    }

    /// Returns the claims from the token.
    pub fn claims(&self) -> &IdentityClaims {
        &self.claims
    }
}

impl Identity for TokenIdentity {
    fn pub_key(&self) -> &dyn PublicKey {
        self.public_key.as_ref()
    }

    fn did(&self) -> Result<Did> {
        Ok(self.did.clone())
    }
}

impl std::fmt::Debug for TokenIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenIdentity")
            .field("did", &self.did)
            .field("key_type", &self.key_type)
            .field("authorized_account", &self.authorized_account)
            .finish_non_exhaustive()
    }
}

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
        KeyType::Ed25519 => encode_ed25519_token(&claims, identity)?,
        KeyType::Secp256k1 => encode_secp256k1_token(&claims, identity)?,
        KeyType::Secp256r1 => return Err(Error::UnsupportedKeyType(KeyType::Secp256r1)),
    };

    Ok(token.into_bytes())
}

/// Verifies a bearer token against an expected audience.
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
/// * The token has expired
/// * The audience doesn't match
pub fn verify_auth_token(identity: &TokenIdentity, expected_audience: &str) -> Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::TokenDecoding(format!("system time error: {}", e)))?
        .as_secs();

    if identity.claims.exp < now {
        return Err(Error::TokenExpired);
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
/// * The token cannot be decoded
/// * Required claims are missing
/// * The key type is unsupported
/// * The public key cannot be reconstructed
pub fn from_token(data: &[u8]) -> Result<TokenIdentity> {
    let token_str = std::str::from_utf8(data)
        .map_err(|e| Error::TokenDecoding(format!("invalid UTF-8: {}", e)))?;

    // Parse the header manually to handle ES256K which isn't in jsonwebtoken's Algorithm enum
    let header_alg = parse_jwt_algorithm(token_str)?;

    let claims: IdentityClaims = match header_alg.as_str() {
        "EdDSA" => decode_ed25519_token(token_str)?,
        "ES256K" => decode_secp256k1_token(token_str)?,
        alg => {
            return Err(Error::TokenDecoding(format!(
                "unsupported algorithm: {}",
                alg
            )))
        }
    };

    let key_type: IdentityKeyType = claims.key_type.parse()?;

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

    Ok(TokenIdentity {
        public_key,
        did,
        bearer_token: token_str.to_string(),
        authorized_account: claims.authorized_account.clone(),
        key_type,
        claims,
    })
}

fn encode_ed25519_token<I: FullIdentity>(claims: &IdentityClaims, identity: &I) -> Result<String> {
    // Create header for EdDSA
    let header = serde_json::json!({
        "alg": EDDSA_ALG,
        "typ": "JWT"
    });
    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());

    // Encode claims
    let claims_json = serde_json::to_string(claims)
        .map_err(|e| Error::TokenEncoding(format!("failed to serialize claims: {}", e)))?;
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims_json.as_bytes());

    // Create signing input
    let signing_input = format!("{}.{}", header_b64, claims_b64);

    // Sign with Ed25519 (raw 64-byte signature)
    let signature = identity
        .sign(signing_input.as_bytes())
        .map_err(|e| Error::TokenEncoding(format!("signing failed: {}", e)))?;

    let sig_b64 = URL_SAFE_NO_PAD.encode(&signature);

    Ok(format!("{}.{}", signing_input, sig_b64))
}

fn encode_secp256k1_token<I: FullIdentity>(
    claims: &IdentityClaims,
    identity: &I,
) -> Result<String> {
    // Create header for ES256K
    let header = serde_json::json!({
        "alg": ES256K_ALG,
        "typ": "JWT"
    });
    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());

    // Encode claims
    let claims_json = serde_json::to_string(claims)
        .map_err(|e| Error::TokenEncoding(format!("failed to serialize claims: {}", e)))?;
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims_json.as_bytes());

    // Create signing input
    let signing_input = format!("{}.{}", header_b64, claims_b64);

    // Sign with secp256k1
    let signature = identity
        .sign(signing_input.as_bytes())
        .map_err(|e| Error::TokenEncoding(format!("signing failed: {}", e)))?;

    // Convert DER signature to raw R||S format (64 bytes)
    let raw_sig = der_signature_to_raw(&signature)?;

    let sig_b64 = URL_SAFE_NO_PAD.encode(&raw_sig);

    Ok(format!("{}.{}", signing_input, sig_b64))
}

fn decode_ed25519_token(token: &str) -> Result<IdentityClaims> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(Error::TokenDecoding("invalid JWT format".to_string()));
    }

    // Decode claims from payload
    let claims_bytes = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| Error::TokenDecoding(format!("invalid payload base64: {}", e)))?;

    let claims: IdentityClaims = serde_json::from_slice(&claims_bytes)
        .map_err(|e| Error::TokenDecoding(format!("invalid claims JSON: {}", e)))?;

    // Get the public key from claims
    let pub_key_bytes = hex::decode(&claims.sub).map_err(|e| Error::InvalidClaimValue {
        claim: "sub".to_string(),
        reason: format!("invalid hex: {}", e),
    })?;

    // Decode signature (raw 64-byte Ed25519 signature)
    let sig_bytes = URL_SAFE_NO_PAD
        .decode(parts[2])
        .map_err(|e| Error::TokenDecoding(format!("invalid signature base64: {}", e)))?;

    // Verify signature using the public key
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

fn decode_secp256k1_token(token: &str) -> Result<IdentityClaims> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(Error::TokenDecoding("invalid JWT format".to_string()));
    }

    // Decode claims from payload
    let claims_bytes = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| Error::TokenDecoding(format!("invalid payload base64: {}", e)))?;

    let claims: IdentityClaims = serde_json::from_slice(&claims_bytes)
        .map_err(|e| Error::TokenDecoding(format!("invalid claims JSON: {}", e)))?;

    // Get the public key from claims
    let pub_key_bytes = hex::decode(&claims.sub).map_err(|e| Error::InvalidClaimValue {
        claim: "sub".to_string(),
        reason: format!("invalid hex: {}", e),
    })?;

    // Decode signature
    let sig_bytes = URL_SAFE_NO_PAD
        .decode(parts[2])
        .map_err(|e| Error::TokenDecoding(format!("invalid signature base64: {}", e)))?;

    // Convert raw R||S signature back to DER for verification
    let der_sig = raw_signature_to_der(&sig_bytes)?;

    // Verify signature using the public key
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

/// Parse the algorithm from a JWT header.
fn parse_jwt_algorithm(token: &str) -> Result<String> {
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

/// Convert DER-encoded ECDSA signature to raw R||S format (64 bytes).
fn der_signature_to_raw(der: &[u8]) -> Result<Vec<u8>> {
    // DER format: 0x30 <len> 0x02 <r_len> <r> 0x02 <s_len> <s>
    if der.len() < 8 || der[0] != 0x30 {
        return Err(Error::TokenEncoding("invalid DER signature".to_string()));
    }

    let mut pos = 2; // Skip 0x30 and length byte

    // Handle multi-byte length
    if der[1] & 0x80 != 0 {
        let len_bytes = (der[1] & 0x7f) as usize;
        pos += len_bytes;
    }

    // Parse R
    if der[pos] != 0x02 {
        return Err(Error::TokenEncoding(
            "invalid DER signature: expected INTEGER tag for R".to_string(),
        ));
    }
    pos += 1;
    let r_len = der[pos] as usize;
    pos += 1;

    let r_start = pos;
    let r_end = r_start + r_len;
    pos = r_end;

    // Parse S
    if der[pos] != 0x02 {
        return Err(Error::TokenEncoding(
            "invalid DER signature: expected INTEGER tag for S".to_string(),
        ));
    }
    pos += 1;
    let s_len = der[pos] as usize;
    pos += 1;

    let s_start = pos;
    let s_end = s_start + s_len;

    // Extract R and S, removing leading zeros
    let mut r = &der[r_start..r_end];
    let mut s = &der[s_start..s_end];

    // Remove leading zeros (DER INTEGER padding)
    while r.len() > 32 && r[0] == 0 {
        r = &r[1..];
    }
    while s.len() > 32 && s[0] == 0 {
        s = &s[1..];
    }

    // Pad to 32 bytes if necessary
    let mut result = vec![0u8; 64];
    let r_offset = 32 - r.len().min(32);
    let s_offset = 32 - s.len().min(32);

    result[r_offset..32].copy_from_slice(&r[..r.len().min(32)]);
    result[32 + s_offset..64].copy_from_slice(&s[..s.len().min(32)]);

    Ok(result)
}

/// Convert raw R||S signature (64 bytes) to DER-encoded ECDSA signature.
fn raw_signature_to_der(raw: &[u8]) -> Result<Vec<u8>> {
    if raw.len() != 64 {
        return Err(Error::TokenDecoding(format!(
            "invalid raw signature length: expected 64, got {}",
            raw.len()
        )));
    }

    let r = &raw[0..32];
    let s = &raw[32..64];

    // Encode as DER INTEGER, adding leading zero if high bit is set
    fn encode_der_integer(bytes: &[u8]) -> Vec<u8> {
        // Skip leading zeros
        let mut start = 0;
        while start < bytes.len() - 1 && bytes[start] == 0 {
            start += 1;
        }
        let trimmed = &bytes[start..];

        let mut result = Vec::new();
        result.push(0x02); // INTEGER tag

        // Add leading zero if high bit is set (to ensure positive number)
        if trimmed[0] & 0x80 != 0 {
            result.push((trimmed.len() + 1) as u8);
            result.push(0x00);
        } else {
            result.push(trimmed.len() as u8);
        }
        result.extend_from_slice(trimmed);
        result
    }

    let r_der = encode_der_integer(r);
    let s_der = encode_der_integer(s);

    let mut result = Vec::new();
    result.push(0x30); // SEQUENCE tag
    result.push((r_der.len() + s_der.len()) as u8);
    result.extend_from_slice(&r_der);
    result.extend_from_slice(&s_der);

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RawIdentity;
    use crypto::{generate_ed25519, generate_secp256k1};

    #[test]
    fn test_new_token_ed25519() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let token = new_token(
            &identity,
            Duration::from_secs(3600),
            Some("test-audience".to_string()),
            Some("account123".to_string()),
        )
        .unwrap();

        assert!(!token.is_empty());

        let token_str = std::str::from_utf8(&token).unwrap();
        assert!(
            token_str.contains('.'),
            "JWT should have header.payload.signature format"
        );
    }

    #[test]
    fn test_new_token_secp256k1() {
        let private_key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let token = new_token(
            &identity,
            Duration::from_secs(3600),
            Some("test-audience".to_string()),
            None,
        )
        .unwrap();

        assert!(!token.is_empty());

        let token_str = std::str::from_utf8(&token).unwrap();
        assert!(token_str.contains('.'));
    }

    #[test]
    fn test_from_token_ed25519() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();
        let original_did = identity.did().unwrap();

        let token = new_token(
            &identity,
            Duration::from_secs(3600),
            Some("test-audience".to_string()),
            Some("account123".to_string()),
        )
        .unwrap();

        let token_identity = from_token(&token).unwrap();

        assert_eq!(token_identity.did().unwrap(), original_did);
        assert_eq!(token_identity.key_type(), IdentityKeyType::Ed25519);
        assert_eq!(token_identity.authorized_account(), Some("account123"));
    }

    #[test]
    fn test_from_token_secp256k1() {
        let private_key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();
        let original_did = identity.did().unwrap();

        let token = new_token(
            &identity,
            Duration::from_secs(3600),
            Some("test-audience".to_string()),
            None,
        )
        .unwrap();

        let token_identity = from_token(&token).unwrap();

        assert_eq!(token_identity.did().unwrap(), original_did);
        assert_eq!(token_identity.key_type(), IdentityKeyType::Secp256k1);
        assert_eq!(token_identity.authorized_account(), None);
    }

    #[test]
    fn test_verify_auth_token_valid_audience() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let token = new_token(
            &identity,
            Duration::from_secs(3600),
            Some("expected-audience".to_string()),
            None,
        )
        .unwrap();

        let token_identity = from_token(&token).unwrap();
        let result = verify_auth_token(&token_identity, "expected-audience");

        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_auth_token_wrong_audience() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let token = new_token(
            &identity,
            Duration::from_secs(3600),
            Some("actual-audience".to_string()),
            None,
        )
        .unwrap();

        let token_identity = from_token(&token).unwrap();
        let result = verify_auth_token(&token_identity, "wrong-audience");

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::AudienceMismatch { .. }
        ));
    }

    #[test]
    fn test_verify_auth_token_missing_audience() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let token = new_token(&identity, Duration::from_secs(3600), None, None).unwrap();

        let token_identity = from_token(&token).unwrap();
        let result = verify_auth_token(&token_identity, "any-audience");

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::AudienceMismatch { .. }
        ));
    }

    #[test]
    fn test_expired_token() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        // Create a token that expires immediately (0 duration)
        let token = new_token(
            &identity,
            Duration::from_secs(0),
            Some("audience".to_string()),
            None,
        )
        .unwrap();

        // Wait a moment to ensure expiration
        std::thread::sleep(Duration::from_millis(1100));

        let token_identity = from_token(&token).unwrap();
        let result = verify_auth_token(&token_identity, "audience");

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::TokenExpired));
    }

    #[test]
    fn test_roundtrip_ed25519() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();
        let original_pub_key = identity.pub_key().raw();

        let token = new_token(
            &identity,
            Duration::from_secs(3600),
            Some("roundtrip-test".to_string()),
            Some("test-account".to_string()),
        )
        .unwrap();

        let token_identity = from_token(&token).unwrap();

        // Verify public key matches
        assert_eq!(token_identity.pub_key().raw(), original_pub_key);

        // Verify verification succeeds
        let result = verify_auth_token(&token_identity, "roundtrip-test");
        assert!(result.is_ok());

        // Verify claims are preserved
        assert_eq!(token_identity.authorized_account(), Some("test-account"));
    }

    #[test]
    fn test_roundtrip_secp256k1() {
        let private_key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();
        let original_pub_key = identity.pub_key().raw();

        let token = new_token(
            &identity,
            Duration::from_secs(3600),
            Some("roundtrip-test".to_string()),
            None,
        )
        .unwrap();

        let token_identity = from_token(&token).unwrap();

        // Verify public key matches
        assert_eq!(token_identity.pub_key().raw(), original_pub_key);

        // Verify verification succeeds
        let result = verify_auth_token(&token_identity, "roundtrip-test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_token_format() {
        let result = from_token(b"not-a-valid-jwt");
        assert!(result.is_err());
    }

    #[test]
    fn test_token_claims_content() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();
        let expected_did = identity.did().unwrap();
        let expected_pub_key_hex = identity.pub_key().to_hex_string();

        let token = new_token(
            &identity,
            Duration::from_secs(3600),
            Some("test-aud".to_string()),
            Some("my-account".to_string()),
        )
        .unwrap();

        let token_identity = from_token(&token).unwrap();
        let claims = token_identity.claims();

        assert_eq!(claims.sub, expected_pub_key_hex);
        assert_eq!(claims.iss, expected_did.to_string());
        assert_eq!(claims.aud, Some(vec!["test-aud".to_string()]));
        assert_eq!(claims.authorized_account, Some("my-account".to_string()));
        assert_eq!(claims.key_type, "ed25519");
        assert!(claims.exp > claims.iat);
        assert_eq!(claims.nbf, claims.iat);
    }

    #[test]
    fn test_token_identity_debug() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let token = new_token(&identity, Duration::from_secs(3600), None, None).unwrap();

        let token_identity = from_token(&token).unwrap();
        let debug_str = format!("{:?}", token_identity);

        assert!(debug_str.contains("TokenIdentity"));
        assert!(debug_str.contains("did:key:"));
        assert!(debug_str.contains("Ed25519"));
    }
}
