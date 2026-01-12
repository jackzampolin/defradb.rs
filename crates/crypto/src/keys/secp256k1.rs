//! secp256k1 key implementations
//!
//! secp256k1 is the elliptic curve used by Bitcoin, Ethereum, and other blockchains.
//! - Public keys: 33 bytes (compressed format with 0x02/0x03 prefix)
//! - Private keys: 32 bytes
//! - Signatures: DER-encoded ECDSA signatures

use k256::ecdsa::{signature::Signer, signature::Verifier, Signature, SigningKey, VerifyingKey};
use k256::EncodedPoint;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use defra_core::Result;

use crate::keys::{Key, PrivateKey, PublicKey};
use crate::types::{KeyType, SECP256K1_PRIVATE_KEY_SIZE};

/// secp256k1 private key wrapper
#[derive(Clone)]
pub struct Secp256k1PrivateKey {
    key: SigningKey,
}

impl Secp256k1PrivateKey {
    /// Create a new secp256k1 private key from raw bytes
    ///
    /// # Parameters
    /// * `bytes` - 32-byte secp256k1 private key
    ///
    /// # Returns
    /// * `Some(Secp256k1PrivateKey)` if the key is valid
    /// * `None` if the key is invalid (wrong length or nil)
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        if bytes.len() != SECP256K1_PRIVATE_KEY_SIZE {
            return None;
        }

        let key = SigningKey::from_slice(bytes).ok()?;
        Some(Self { key })
    }

    /// Get the underlying k256 signing key
    pub fn underlying(&self) -> &SigningKey {
        &self.key
    }
}

impl Key for Secp256k1PrivateKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Secp256k1 {
            return false;
        }
        self.raw() == other.raw()
    }

    fn raw(&self) -> Vec<u8> {
        self.key.to_bytes().to_vec()
    }

    fn key_type(&self) -> KeyType {
        KeyType::Secp256k1
    }
}

impl PrivateKey for Secp256k1PrivateKey {
    fn sign(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Hash the message with SHA-256 (required for ECDSA)
        let hash = Sha256::digest(data);

        // Sign the hash
        let signature: Signature = self.key.sign(&hash);

        // Return DER-encoded signature (critical for compatibility with Go implementation)
        Ok(signature.to_der().as_bytes().to_vec())
    }

    fn public_key(&self) -> Box<dyn PublicKey> {
        Box::new(Secp256k1PublicKey {
            key: *self.key.verifying_key(),
        })
    }
}

/// secp256k1 public key wrapper
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secp256k1PublicKey {
    #[serde(with = "secp256k1_public_key_serde")]
    key: VerifyingKey,
}

impl Secp256k1PublicKey {
    /// Create a new secp256k1 public key from raw bytes
    ///
    /// Supports both compressed (33 bytes) and uncompressed (65 bytes) formats.
    ///
    /// # Parameters
    /// * `bytes` - Public key bytes (33 or 65 bytes)
    ///
    /// # Returns
    /// * `Some(Secp256k1PublicKey)` if the key is valid
    /// * `None` if the key is invalid
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        // EncodedPoint handles both compressed (33) and uncompressed (65) formats
        let point = EncodedPoint::from_bytes(bytes).ok()?;
        let key = VerifyingKey::from_encoded_point(&point).ok()?;

        Some(Self { key })
    }

    /// Get the underlying k256 verifying key
    pub fn underlying(&self) -> &VerifyingKey {
        &self.key
    }
}

impl Key for Secp256k1PublicKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Secp256k1 {
            return false;
        }
        self.raw() == other.raw()
    }

    fn raw(&self) -> Vec<u8> {
        // Always return compressed format (33 bytes with 0x02/0x03 prefix)
        self.key.to_encoded_point(true).as_bytes().to_vec()
    }

    fn key_type(&self) -> KeyType {
        KeyType::Secp256k1
    }
}

impl PublicKey for Secp256k1PublicKey {
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
        crate::did::create_did_key(KeyType::Secp256k1, &self.raw())
    }
}

// Custom serde module for VerifyingKey
mod secp256k1_public_key_serde {
    use k256::ecdsa::VerifyingKey;
    use k256::EncodedPoint;
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

    #[test]
    fn test_secp256k1_key_type() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[1u8; 32]).unwrap();
        assert_eq!(private_key.key_type(), KeyType::Secp256k1);

        let public_key = private_key.public_key();
        assert_eq!(public_key.key_type(), KeyType::Secp256k1);
    }

    #[test]
    fn test_secp256k1_raw_bytes() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[2u8; 32]).unwrap();
        let raw = private_key.raw();
        assert_eq!(raw.len(), 32);
    }

    #[test]
    fn test_secp256k1_public_key_compressed() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[3u8; 32]).unwrap();
        let public_key = private_key.public_key();
        let raw = public_key.raw();

        // Should be compressed format (33 bytes)
        assert_eq!(raw.len(), 33);
        // First byte should be 0x02 or 0x03
        assert!(raw[0] == 0x02 || raw[0] == 0x03);
    }

    #[test]
    fn test_secp256k1_sign_verify() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[4u8; 32]).unwrap();
        let message = b"test message";

        let signature = private_key.sign(message).unwrap();
        // DER signatures vary in length but are typically 70-72 bytes
        assert!(signature.len() >= 70 && signature.len() <= 73);

        let public_key = private_key.public_key();
        let valid = public_key.verify(message, &signature).unwrap();
        assert!(valid);
    }

    #[test]
    fn test_secp256k1_verify_wrong_message() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[5u8; 32]).unwrap();
        let message = b"test message";
        let signature = private_key.sign(message).unwrap();

        let public_key = private_key.public_key();
        let wrong_message = b"wrong message";
        let valid = public_key.verify(wrong_message, &signature).unwrap();
        assert!(!valid);
    }

    #[test]
    fn test_secp256k1_invalid_signature() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[6u8; 32]).unwrap();
        let public_key = private_key.public_key();

        let message = b"test message";
        let invalid_sig = vec![0u8; 64];
        let valid = public_key.verify(message, &invalid_sig).unwrap();
        assert!(!valid);
    }

    #[test]
    fn test_secp256k1_invalid_key_lengths() {
        // Invalid private key length
        let private_key = Secp256k1PrivateKey::from_bytes(&[0u8; 16]);
        assert!(private_key.is_none());

        // Invalid public key length
        let public_key = Secp256k1PublicKey::from_bytes(&[0u8; 10]);
        assert!(public_key.is_none());
    }

    #[test]
    fn test_secp256k1_nil_keys() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[]);
        assert!(private_key.is_none());

        let public_key = Secp256k1PublicKey::from_bytes(&[]);
        assert!(public_key.is_none());
    }

    #[test]
    fn test_secp256k1_key_equality() {
        let key1 = Secp256k1PrivateKey::from_bytes(&[7u8; 32]).unwrap();
        let key2 = Secp256k1PrivateKey::from_bytes(&[7u8; 32]).unwrap();
        assert!(key1.equal(&key2 as &dyn Key));

        let key3 = Secp256k1PrivateKey::from_bytes(&[8u8; 32]).unwrap();
        assert!(!key1.equal(&key3 as &dyn Key));
    }

    #[test]
    fn test_secp256k1_public_key_from_compressed() {
        // Create a compressed public key (33 bytes starting with 0x02 or 0x03)
        let private_key = Secp256k1PrivateKey::from_bytes(&[9u8; 32]).unwrap();
        let compressed = private_key.public_key().raw();

        // Should be able to parse compressed format
        let public_key = Secp256k1PublicKey::from_bytes(&compressed);
        assert!(public_key.is_some());
    }
}
