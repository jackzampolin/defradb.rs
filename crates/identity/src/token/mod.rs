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
/// # Parameters
/// * `identity` - The token identity to verify
/// * `expected_audience` - The expected audience value
///
/// # Returns
/// Ok(()) if verification succeeds.
///
/// # Errors
/// Returns an error if:
/// * The token is not yet valid (nbf is in the future)
/// * The token has expired
/// * The audience doesn't match
pub fn verify_auth_token(identity: &TokenIdentity, expected_audience: &str) -> Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::TokenDecoding(format!("system time error: {}", e)))?
        .as_secs();

    if identity.claims.nbf > now {
        return Err(Error::TokenNotYetValid {
            nbf: identity.claims.nbf,
            now,
        });
    }

    if identity.claims.exp < now {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Identity, RawIdentity};
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
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
        assert!(token_str.contains('.'));
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

        let token = new_token(
            &identity,
            Duration::from_secs(3600),
            Some("audience".to_string()),
            None,
        )
        .unwrap();

        let mut token_identity = from_token(&token).unwrap();
        // Manipulate claims to simulate expiration without sleeping
        token_identity.claims.exp = 0;

        let result = verify_auth_token(&token_identity, "audience");

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::TokenExpired { .. }));
    }

    #[test]
    fn test_token_not_yet_valid() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let token = new_token(
            &identity,
            Duration::from_secs(3600),
            Some("audience".to_string()),
            None,
        )
        .unwrap();

        let mut token_identity = from_token(&token).unwrap();
        // Set nbf far in the future
        token_identity.claims.nbf = u64::MAX;

        let result = verify_auth_token(&token_identity, "audience");

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::TokenNotYetValid { .. }
        ));
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

        assert_eq!(token_identity.pub_key().raw(), original_pub_key);
        let result = verify_auth_token(&token_identity, "roundtrip-test");
        assert!(result.is_ok());
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

        assert_eq!(token_identity.pub_key().raw(), original_pub_key);
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

    // Security tests

    #[test]
    fn test_tampered_signature_rejected_ed25519() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let token = new_token(&identity, Duration::from_secs(3600), None, None).unwrap();
        let token_str = String::from_utf8(token).unwrap();

        let parts: Vec<&str> = token_str.split('.').collect();
        let sig_bytes = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        let mut tampered_sig = sig_bytes.clone();
        tampered_sig[0] ^= 0xFF;
        tampered_sig[10] ^= 0xFF;
        let tampered_sig_b64 = URL_SAFE_NO_PAD.encode(&tampered_sig);
        let tampered_token = format!("{}.{}.{}", parts[0], parts[1], tampered_sig_b64);

        let result = from_token(tampered_token.as_bytes());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::TokenDecoding(ref msg) if msg.contains("signature verification failed")),
            "Expected signature verification failure, got: {:?}",
            err
        );
    }

    #[test]
    fn test_tampered_signature_rejected_secp256k1() {
        let private_key = generate_secp256k1().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let token = new_token(&identity, Duration::from_secs(3600), None, None).unwrap();
        let token_str = String::from_utf8(token).unwrap();

        let parts: Vec<&str> = token_str.split('.').collect();
        let sig_bytes = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        let mut tampered_sig = sig_bytes.clone();
        tampered_sig[0] ^= 0xFF;
        tampered_sig[10] ^= 0xFF;
        let tampered_sig_b64 = URL_SAFE_NO_PAD.encode(&tampered_sig);
        let tampered_token = format!("{}.{}.{}", parts[0], parts[1], tampered_sig_b64);

        let result = from_token(tampered_token.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_tampered_payload_rejected() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let token = new_token(
            &identity,
            Duration::from_secs(3600),
            Some("original-audience".to_string()),
            None,
        )
        .unwrap();
        let token_str = String::from_utf8(token).unwrap();

        let parts: Vec<&str> = token_str.split('.').collect();
        let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).unwrap();
        let mut claims: serde_json::Value = serde_json::from_slice(&payload_bytes).unwrap();
        claims["aud"] = serde_json::json!(["tampered-audience"]);
        let tampered_payload = URL_SAFE_NO_PAD.encode(serde_json::to_string(&claims).unwrap());
        let tampered_token = format!("{}.{}.{}", parts[0], tampered_payload, parts[2]);

        let result = from_token(tampered_token.as_bytes());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::TokenDecoding(ref msg) if msg.contains("signature verification failed")),
            "Expected signature verification failure, got: {:?}",
            err
        );
    }

    #[test]
    fn test_wrong_signer_rejected() {
        let attacker_key = generate_ed25519().unwrap();
        let attacker_identity = RawIdentity::from_private_key(attacker_key).unwrap();

        let victim_key = generate_ed25519().unwrap();
        let victim_identity = RawIdentity::from_private_key(victim_key).unwrap();

        let attacker_token =
            new_token(&attacker_identity, Duration::from_secs(3600), None, None).unwrap();
        let attacker_token_str = String::from_utf8(attacker_token).unwrap();
        let parts: Vec<&str> = attacker_token_str.split('.').collect();

        let victim_pub_key_hex = victim_identity.pub_key().to_hex_string();
        let victim_did = victim_identity.did().unwrap();

        let fake_claims = IdentityClaims {
            sub: victim_pub_key_hex,
            iss: victim_did.to_string(),
            exp: 9999999999,
            nbf: 0,
            iat: 0,
            aud: None,
            authorized_account: None,
            key_type: "ed25519".to_string(),
        };
        let fake_payload = URL_SAFE_NO_PAD.encode(serde_json::to_string(&fake_claims).unwrap());
        let forged_token = format!("{}.{}.{}", parts[0], fake_payload, parts[2]);

        let result = from_token(forged_token.as_bytes());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::TokenDecoding(ref msg) if msg.contains("signature verification failed")),
            "Expected signature verification failure, got: {:?}",
            err
        );
    }

    #[test]
    fn test_algorithm_mismatch_rejected() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        let token = new_token(&identity, Duration::from_secs(3600), None, None).unwrap();
        let token_str = String::from_utf8(token).unwrap();

        let parts: Vec<&str> = token_str.split('.').collect();
        let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).unwrap();
        let mut claims: serde_json::Value = serde_json::from_slice(&payload_bytes).unwrap();
        // Change key_type to secp256k1 while header says EdDSA
        claims["key_type"] = serde_json::json!("secp256k1");
        let modified_payload = URL_SAFE_NO_PAD.encode(serde_json::to_string(&claims).unwrap());
        let modified_token = format!("{}.{}.{}", parts[0], modified_payload, parts[2]);

        let result = from_token(modified_token.as_bytes());
        assert!(result.is_err());
        let err = result.unwrap_err();
        // Should fail at signature verification (before algorithm check) or algorithm mismatch
        assert!(
            matches!(err, Error::TokenDecoding(ref msg) if msg.contains("signature") || msg.contains("algorithm mismatch")),
            "Expected signature or algorithm error, got: {:?}",
            err
        );
    }

    #[test]
    fn test_issuer_mismatch_rejected() {
        let private_key = generate_ed25519().unwrap();
        let identity = RawIdentity::from_private_key(private_key).unwrap();

        // Create another identity to get a different DID
        let other_key = generate_ed25519().unwrap();
        let other_identity = RawIdentity::from_private_key(other_key).unwrap();
        let other_did = other_identity.did().unwrap();

        let token = new_token(&identity, Duration::from_secs(3600), None, None).unwrap();
        let token_str = String::from_utf8(token).unwrap();

        let parts: Vec<&str> = token_str.split('.').collect();
        let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).unwrap();
        let mut claims: serde_json::Value = serde_json::from_slice(&payload_bytes).unwrap();
        // Change iss to a different DID
        claims["iss"] = serde_json::json!(other_did.to_string());
        let modified_payload = URL_SAFE_NO_PAD.encode(serde_json::to_string(&claims).unwrap());
        let modified_token = format!("{}.{}.{}", parts[0], modified_payload, parts[2]);

        let result = from_token(modified_token.as_bytes());
        assert!(result.is_err());
        let err = result.unwrap_err();
        // Should fail at signature verification (payload was modified)
        assert!(
            matches!(err, Error::TokenDecoding(ref msg) if msg.contains("signature")),
            "Expected signature error, got: {:?}",
            err
        );
    }

    #[test]
    fn test_unsupported_algorithm() {
        let header = serde_json::json!({
            "alg": "RS256",
            "typ": "JWT"
        });
        let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
        let payload_b64 = URL_SAFE_NO_PAD.encode(b"{}");
        let sig_b64 = URL_SAFE_NO_PAD.encode(&[0u8; 64]);
        let token = format!("{}.{}.{}", header_b64, payload_b64, sig_b64);

        let result = from_token(token.as_bytes());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::TokenDecoding(ref msg) if msg.contains("unsupported algorithm"))
        );
    }

    #[test]
    fn test_invalid_utf8_token() {
        let result = from_token(&[0xFF, 0xFE, 0x00, 0x01]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::TokenDecoding(ref msg) if msg.contains("invalid UTF-8")));
    }

    #[test]
    fn test_invalid_base64_header() {
        let result = from_token(b"!!!invalid-base64!!!.payload.signature");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::TokenDecoding(ref msg) if msg.contains("base64")));
    }
}
