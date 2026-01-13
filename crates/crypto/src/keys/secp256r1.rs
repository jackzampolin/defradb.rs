//! secp256r1 (P-256) public key implementations
//!
//! secp256r1 (also known as P-256 or prime256v1) is a NIST standard elliptic curve.
//! This module provides read-only public key support for verifying signatures from
//! JavaScript clients. Private key operations are handled by the JS clients.
//!
//! - Public keys: 33 bytes (compressed format with 0x02/0x03 prefix) or 65 bytes (uncompressed)

use p256::ecdsa::{Signature, VerifyingKey};
use p256::EncodedPoint;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use defra_core::Result;

use crate::error::crypto_error;
use crate::keys::{Key, PublicKey};
use crate::types::KeyType;

/// secp256r1 (P-256) public key wrapper
///
/// This implementation only supports public key operations (verification).
/// Private keys are managed by JavaScript clients.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Secp256r1PublicKey {
    #[serde(with = "secp256r1_public_key_serde")]
    key: VerifyingKey,
    /// Cached compressed bytes for efficient serialization
    #[serde(skip)]
    compressed_bytes: Vec<u8>,
}

impl PartialEq for Secp256r1PublicKey {
    fn eq(&self, other: &Self) -> bool {
        // Compare only the key, not the cached compressed_bytes which may be empty after deserialization
        self.key == other.key
    }
}

impl Eq for Secp256r1PublicKey {}

impl Secp256r1PublicKey {
    /// Create a new secp256r1 public key from raw bytes
    ///
    /// Supports both compressed (33 bytes) and uncompressed (65 bytes) formats.
    /// Internally stores the compressed form for consistency with Go implementation.
    ///
    /// # Parameters
    /// * `bytes` - Public key bytes (33 or 65 bytes)
    ///
    /// # Returns
    /// * `Ok(Secp256r1PublicKey)` if the key is valid
    /// * `Err` if the key is invalid (wrong length, empty, or invalid point)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(crypto_error("secp256r1 public key cannot be empty"));
        }

        // EncodedPoint handles both compressed (33) and uncompressed (65) formats
        let point = EncodedPoint::from_bytes(bytes)
            .map_err(|e| crypto_error(format!("invalid secp256r1 public key encoding: {}", e)))?;
        let key = VerifyingKey::from_encoded_point(&point)
            .map_err(|_| crypto_error("invalid secp256r1 public key: not on curve"))?;

        // Pre-compute and cache compressed bytes
        let compressed_bytes = key.to_encoded_point(true).as_bytes().to_vec();

        Ok(Self {
            key,
            compressed_bytes,
        })
    }

    /// Get the underlying p256 verifying key
    pub fn underlying(&self) -> &VerifyingKey {
        &self.key
    }
}

impl Key for Secp256r1PublicKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Secp256r1 {
            return false;
        }
        // Use constant-time comparison to prevent timing attacks
        let self_raw = self.raw();
        let other_raw = other.raw();
        if self_raw.len() != other_raw.len() {
            return false;
        }
        self_raw.ct_eq(&other_raw).into()
    }

    fn raw(&self) -> Vec<u8> {
        // Return cached compressed format (33 bytes with 0x02/0x03 prefix)
        self.compressed_bytes.clone()
    }

    fn key_type(&self) -> KeyType {
        KeyType::Secp256r1
    }
}

impl PublicKey for Secp256r1PublicKey {
    fn verify(&self, data: &[u8], signature: &[u8]) -> Result<bool> {
        use p256::ecdsa::signature::DigestVerifier;

        // Parse DER-encoded signature
        let sig = match Signature::from_der(signature) {
            Ok(s) => s,
            Err(_) => return Ok(false),
        };

        // Hash the message with SHA-256 first
        // Go's ecdsa.Sign takes a pre-computed hash, so we must also pre-hash
        // and use verify_digest (DigestVerifier) to match Go behavior
        let digest = Sha256::new_with_prefix(data);

        // Verify signature using pre-hashed digest
        match self.key.verify_digest(digest, &sig) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    fn did(&self) -> Result<String> {
        // Use uncompressed format (65 bytes) for DID generation to match Go implementation
        // Go uses ecdhPubKey.Bytes() which produces the full [0x04 | x | y] format
        let uncompressed = self.key.to_encoded_point(false);
        crate::did::create_did_key(KeyType::Secp256r1, uncompressed.as_bytes())
    }
}

// Custom serde module for VerifyingKey
mod secp256r1_public_key_serde {
    use p256::ecdsa::VerifyingKey;
    use p256::EncodedPoint;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(key: &VerifyingKey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize as compressed format
        let bytes = key.to_encoded_point(true);
        serializer.serialize_bytes(bytes.as_bytes())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<VerifyingKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        let point = EncodedPoint::from_bytes(&bytes)
            .map_err(|e| serde::de::Error::custom(e.to_string()))?;
        VerifyingKey::from_encoded_point(&point).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Generate a valid P-256 key for testing
    fn generate_test_key() -> (Vec<u8>, Vec<u8>) {
        use p256::ecdsa::SigningKey;
        use rand::rngs::OsRng;

        let signing_key = SigningKey::random(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        let compressed = verifying_key.to_encoded_point(true).as_bytes().to_vec();
        let uncompressed = verifying_key.to_encoded_point(false).as_bytes().to_vec();

        (compressed, uncompressed)
    }

    #[test]
    fn test_secp256r1_key_type() {
        let (compressed, _) = generate_test_key();
        let public_key = Secp256r1PublicKey::from_bytes(&compressed).unwrap();
        assert_eq!(public_key.key_type(), KeyType::Secp256r1);
    }

    #[test]
    fn test_secp256r1_public_key_compressed() {
        let (compressed, _) = generate_test_key();
        let public_key = Secp256r1PublicKey::from_bytes(&compressed).unwrap();
        let raw = public_key.raw();

        // Should be compressed format (33 bytes)
        assert_eq!(raw.len(), 33);
        // First byte should be 0x02 or 0x03
        assert!(raw[0] == 0x02 || raw[0] == 0x03);
    }

    #[test]
    fn test_secp256r1_public_key_from_uncompressed() {
        let (compressed, uncompressed) = generate_test_key();

        // Parse uncompressed format
        let public_key_uncompressed = Secp256r1PublicKey::from_bytes(&uncompressed).unwrap();

        // Parse compressed format
        let public_key_compressed = Secp256r1PublicKey::from_bytes(&compressed).unwrap();

        // Both should produce the same compressed output
        assert_eq!(public_key_uncompressed.raw(), public_key_compressed.raw());
        assert_eq!(public_key_uncompressed.raw().len(), 33);
    }

    #[test]
    fn test_secp256r1_invalid_key_lengths() {
        // Invalid public key length
        let public_key = Secp256r1PublicKey::from_bytes(&[0u8; 10]);
        assert!(public_key.is_err());
    }

    #[test]
    fn test_secp256r1_nil_key() {
        let public_key = Secp256r1PublicKey::from_bytes(&[]);
        assert!(public_key.is_err());
    }

    #[test]
    fn test_secp256r1_key_equality() {
        let (compressed, _) = generate_test_key();
        let key1 = Secp256r1PublicKey::from_bytes(&compressed).unwrap();
        let key2 = Secp256r1PublicKey::from_bytes(&compressed).unwrap();
        assert!(key1.equal(&key2 as &dyn Key));
    }

    #[test]
    fn test_secp256r1_verify_signature() {
        use p256::ecdsa::{signature::DigestSigner, SigningKey};
        use rand::rngs::OsRng;

        // Generate a key pair
        let signing_key = SigningKey::random(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        // Create our wrapper
        let compressed = verifying_key.to_encoded_point(true).as_bytes().to_vec();
        let public_key = Secp256r1PublicKey::from_bytes(&compressed).unwrap();

        // Sign a message using sign_digest to match Go's ecdsa.Sign behavior
        // Go's ecdsa.Sign takes a pre-computed hash, so we use DigestSigner
        let message = b"test message";
        let digest = Sha256::new_with_prefix(message);
        let signature: Signature = signing_key.sign_digest(digest);

        // Verify with our wrapper
        let valid = public_key
            .verify(message, &signature.to_der().as_bytes())
            .unwrap();
        assert!(valid);

        // Verify with wrong message
        let wrong_message = b"wrong message";
        let valid = public_key
            .verify(wrong_message, &signature.to_der().as_bytes())
            .unwrap();
        assert!(!valid);
    }

    #[test]
    fn test_secp256r1_invalid_der_signatures() {
        use p256::ecdsa::SigningKey;
        use rand::rngs::OsRng;

        // Generate a key pair for testing
        let signing_key = SigningKey::random(&mut OsRng);
        let verifying_key = signing_key.verifying_key();
        let compressed = verifying_key.to_encoded_point(true).as_bytes().to_vec();
        let public_key = Secp256r1PublicKey::from_bytes(&compressed).unwrap();
        let message = b"test";

        // Empty signature
        let result = public_key.verify(message, &[]);
        assert!(
            result.is_ok() && !result.unwrap(),
            "Empty signature should return false"
        );

        // Invalid DER: wrong sequence tag (should be 0x30)
        let invalid_der = vec![0xFF, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01];
        let result = public_key.verify(message, &invalid_der);
        assert!(
            result.is_ok() && !result.unwrap(),
            "Invalid DER tag should return false"
        );

        // Invalid DER: length exceeds data
        let invalid_der = vec![0x30, 0xFF, 0x02, 0x01, 0x01];
        let result = public_key.verify(message, &invalid_der);
        assert!(
            result.is_ok() && !result.unwrap(),
            "Invalid DER length should return false"
        );

        // Too short to be valid DER
        let invalid_der = vec![0x30, 0x02];
        let result = public_key.verify(message, &invalid_der);
        assert!(
            result.is_ok() && !result.unwrap(),
            "Truncated DER should return false"
        );

        // Single byte (not valid DER)
        let invalid_der = vec![0x30];
        let result = public_key.verify(message, &invalid_der);
        assert!(
            result.is_ok() && !result.unwrap(),
            "Single byte should return false"
        );
    }

    #[test]
    fn test_secp256r1_verify_tampered_signature() {
        use p256::ecdsa::{signature::DigestSigner, SigningKey};
        use rand::rngs::OsRng;

        // Generate a key pair for testing
        let signing_key = SigningKey::random(&mut OsRng);
        let verifying_key = signing_key.verifying_key();
        let compressed = verifying_key.to_encoded_point(true).as_bytes().to_vec();
        let public_key = Secp256r1PublicKey::from_bytes(&compressed).unwrap();

        let message = b"test message";
        let digest = Sha256::new_with_prefix(message);
        let signature: Signature = signing_key.sign_digest(digest);
        let mut signature_bytes = signature.to_der().as_bytes().to_vec();

        // Tamper with the signature
        if let Some(last) = signature_bytes.last_mut() {
            *last ^= 0xFF;
        }

        // Should fail verification with tampered signature
        let result = public_key.verify(message, &signature_bytes).unwrap();
        assert!(!result, "Tampered signature should fail verification");
    }
}
