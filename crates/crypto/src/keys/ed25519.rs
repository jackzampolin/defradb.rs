//! Ed25519 key implementations
//!
//! Ed25519 is a modern, high-security elliptic curve signature scheme.
//! - Public keys: 32 bytes
//! - Private keys: 64 bytes (includes public key)
//! - Signatures: 64 bytes

use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey, PUBLIC_KEY_LENGTH};
use serde::{Deserialize, Serialize};

use defra_core::Result;

use crate::error::signature_verification_failed;
use crate::keys::{Key, PrivateKey, PublicKey};
use crate::types::KeyType;

/// Ed25519 private key wrapper
#[derive(Clone)]
pub struct Ed25519PrivateKey {
    key: SigningKey,
}

impl Ed25519PrivateKey {
    /// Create a new Ed25519 private key from raw bytes
    ///
    /// # Parameters
    /// * `bytes` - 64-byte Ed25519 private key
    ///
    /// # Returns
    /// * `Some(Ed25519PrivateKey)` if the key is valid
    /// * `None` if the key is invalid (wrong length or nil)
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        // Ed25519 private keys are 64 bytes (32-byte seed + 32-byte public key)
        // to match the Go implementation format
        if bytes.len() != 64 {
            return None;
        }

        // ed25519-dalek expects 32 bytes for SigningKey (the seed)
        // The first 32 bytes are the seed
        let seed: [u8; 32] = bytes[..32].try_into().ok()?;
        let key = SigningKey::from_bytes(&seed);

        Some(Self { key })
    }

    /// Get the underlying ed25519-dalek signing key
    pub fn underlying(&self) -> &SigningKey {
        &self.key
    }
}

impl Key for Ed25519PrivateKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Ed25519 {
            return false;
        }
        self.raw() == other.raw()
    }

    fn raw(&self) -> Vec<u8> {
        // Return 64 bytes: 32-byte seed + 32-byte public key (to match Go implementation)
        let seed = self.key.to_bytes();
        let public = self.key.verifying_key().to_bytes();
        let mut result = Vec::with_capacity(64);
        result.extend_from_slice(&seed);
        result.extend_from_slice(&public);
        result
    }

    fn key_type(&self) -> KeyType {
        KeyType::Ed25519
    }
}

impl PrivateKey for Ed25519PrivateKey {
    fn sign(&self, data: &[u8]) -> Result<Vec<u8>> {
        let signature = self.key.sign(data);
        Ok(signature.to_bytes().to_vec())
    }

    fn public_key(&self) -> Box<dyn PublicKey> {
        Box::new(Ed25519PublicKey {
            key: self.key.verifying_key(),
        })
    }
}

/// Ed25519 public key wrapper
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ed25519PublicKey {
    #[serde(with = "ed25519_public_key_serde")]
    key: VerifyingKey,
}

impl Ed25519PublicKey {
    /// Create a new Ed25519 public key from raw bytes
    ///
    /// # Parameters
    /// * `bytes` - 32-byte Ed25519 public key
    ///
    /// # Returns
    /// * `Some(Ed25519PublicKey)` if the key is valid
    /// * `None` if the key is invalid (wrong length or nil)
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        if bytes.len() != PUBLIC_KEY_LENGTH {
            return None;
        }

        let key_bytes: [u8; PUBLIC_KEY_LENGTH] = bytes.try_into().ok()?;
        let key = VerifyingKey::from_bytes(&key_bytes).ok()?;

        Some(Self { key })
    }

    /// Get the underlying ed25519-dalek verifying key
    pub fn underlying(&self) -> &VerifyingKey {
        &self.key
    }
}

impl Key for Ed25519PublicKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Ed25519 {
            return false;
        }
        self.raw() == other.raw()
    }

    fn raw(&self) -> Vec<u8> {
        self.key.to_bytes().to_vec()
    }

    fn key_type(&self) -> KeyType {
        KeyType::Ed25519
    }
}

impl PublicKey for Ed25519PublicKey {
    fn verify(&self, data: &[u8], signature: &[u8]) -> Result<bool> {
        if signature.len() != 64 {
            return Ok(false);
        }

        let sig_bytes: [u8; 64] = signature
            .try_into()
            .map_err(|_| signature_verification_failed())?;

        let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);

        match self.key.verify(data, &signature) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    fn did(&self) -> Result<String> {
        crate::did::create_did_key(KeyType::Ed25519, &self.raw())
    }
}

// Custom serde module for VerifyingKey
mod ed25519_public_key_serde {
    use ed25519_dalek::VerifyingKey;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(key: &VerifyingKey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&key.to_bytes())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<VerifyingKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        if bytes.len() != 32 {
            return Err(serde::de::Error::custom("invalid Ed25519 public key length"));
        }
        let key_bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| serde::de::Error::custom("invalid Ed25519 public key bytes"))?;
        VerifyingKey::from_bytes(&key_bytes).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ed25519_key_generation() {
        let private_key = Ed25519PrivateKey::from_bytes(&[0u8; 64]);
        assert!(private_key.is_some());
    }

    #[test]
    fn test_ed25519_key_type() {
        let private_key = Ed25519PrivateKey::from_bytes(&[1u8; 64]).unwrap();
        assert_eq!(private_key.key_type(), KeyType::Ed25519);

        let public_key = private_key.public_key();
        assert_eq!(public_key.key_type(), KeyType::Ed25519);
    }

    #[test]
    fn test_ed25519_raw_bytes() {
        let private_key = Ed25519PrivateKey::from_bytes(&[2u8; 64]).unwrap();
        let raw = private_key.raw();
        assert_eq!(raw.len(), 64);
    }

    #[test]
    fn test_ed25519_public_key_from_private() {
        let private_key = Ed25519PrivateKey::from_bytes(&[3u8; 64]).unwrap();
        let public_key = private_key.public_key();
        let raw = public_key.raw();
        assert_eq!(raw.len(), 32);
    }

    #[test]
    fn test_ed25519_sign_verify() {
        let private_key = Ed25519PrivateKey::from_bytes(&[4u8; 64]).unwrap();
        let message = b"test message";

        let signature = private_key.sign(message).unwrap();
        assert_eq!(signature.len(), 64);

        let public_key = private_key.public_key();
        let valid = public_key.verify(message, &signature).unwrap();
        assert!(valid);
    }

    #[test]
    fn test_ed25519_verify_wrong_message() {
        let private_key = Ed25519PrivateKey::from_bytes(&[5u8; 64]).unwrap();
        let message = b"test message";
        let signature = private_key.sign(message).unwrap();

        let public_key = private_key.public_key();
        let wrong_message = b"wrong message";
        let valid = public_key.verify(wrong_message, &signature).unwrap();
        assert!(!valid);
    }

    #[test]
    fn test_ed25519_invalid_key_lengths() {
        // Invalid private key length
        let private_key = Ed25519PrivateKey::from_bytes(&[0u8; 32]);
        assert!(private_key.is_none());

        // Invalid public key length
        let public_key = Ed25519PublicKey::from_bytes(&[0u8; 16]);
        assert!(public_key.is_none());
    }

    #[test]
    fn test_ed25519_nil_keys() {
        let private_key = Ed25519PrivateKey::from_bytes(&[]);
        assert!(private_key.is_none());

        let public_key = Ed25519PublicKey::from_bytes(&[]);
        assert!(public_key.is_none());
    }

    #[test]
    fn test_ed25519_key_equality() {
        let key1 = Ed25519PrivateKey::from_bytes(&[6u8; 64]).unwrap();
        let key2 = Ed25519PrivateKey::from_bytes(&[6u8; 64]).unwrap();
        assert!(key1.equal(&key2 as &dyn Key));

        let key3 = Ed25519PrivateKey::from_bytes(&[7u8; 64]).unwrap();
        assert!(!key1.equal(&key3 as &dyn Key));
    }
}
