//! secp256r1 (P-256) public key implementations
//!
//! secp256r1 (also known as P-256 or prime256v1) is a NIST standard elliptic curve.
//! This module provides read-only public key support for verifying signatures from
//! JavaScript clients. Private key operations are handled by the JS clients.
//!
//! - Public keys: 33 bytes (compressed format with 0x02/0x03 prefix) or 65 bytes (uncompressed)

use p256::ecdsa::{signature::Verifier, Signature, VerifyingKey};
use p256::EncodedPoint;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use defra_core::Result;

use crate::keys::{Key, PublicKey};
use crate::types::KeyType;

/// secp256r1 (P-256) public key wrapper
///
/// This implementation only supports public key operations (verification).
/// Private keys are managed by JavaScript clients.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Secp256r1PublicKey {
    #[serde(with = "secp256r1_public_key_serde")]
    key: VerifyingKey,
    /// Cached compressed bytes for efficient serialization
    #[serde(skip)]
    compressed_bytes: Vec<u8>,
}

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
    /// * `Some(Secp256r1PublicKey)` if the key is valid
    /// * `None` if the key is invalid
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        // EncodedPoint handles both compressed (33) and uncompressed (65) formats
        let point = EncodedPoint::from_bytes(bytes).ok()?;
        let key = VerifyingKey::from_encoded_point(&point).ok()?;

        // Pre-compute and cache compressed bytes
        let compressed_bytes = key.to_encoded_point(true).as_bytes().to_vec();

        Some(Self {
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
        self.raw() == other.raw()
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
        // Parse DER-encoded signature
        let sig = match Signature::from_der(signature) {
            Ok(s) => s,
            Err(_) => return Ok(false),
        };

        // Hash the message with SHA-256
        let hash = Sha256::digest(data);

        // Verify signature
        match self.key.verify(&hash, &sig) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    fn did(&self) -> Result<String> {
        crate::did::create_did_key(KeyType::Secp256r1, &self.raw())
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
        let point =
            EncodedPoint::from_bytes(&bytes).map_err(|e| serde::de::Error::custom(e.to_string()))?;
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
        assert!(public_key.is_none());
    }

    #[test]
    fn test_secp256r1_nil_key() {
        let public_key = Secp256r1PublicKey::from_bytes(&[]);
        assert!(public_key.is_none());
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
        use p256::ecdsa::{signature::Signer, SigningKey};
        use rand::rngs::OsRng;

        // Generate a key pair
        let signing_key = SigningKey::random(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        // Create our wrapper
        let compressed = verifying_key.to_encoded_point(true).as_bytes().to_vec();
        let public_key = Secp256r1PublicKey::from_bytes(&compressed).unwrap();

        // Sign a message
        let message = b"test message";
        let hash = Sha256::digest(message);
        let signature: Signature = signing_key.sign(&hash);

        // Verify with our wrapper
        let valid = public_key.verify(message, &signature.to_der().as_bytes()).unwrap();
        assert!(valid);

        // Verify with wrong message
        let wrong_message = b"wrong message";
        let valid = public_key
            .verify(wrong_message, &signature.to_der().as_bytes())
            .unwrap();
        assert!(!valid);
    }
}
