//! secp256r1 (P-256) key implementations
//!
//! secp256r1 (also known as P-256 or prime256v1) is a NIST standard elliptic curve.
//! Used by browser identity via Web Crypto API.
//!
//! - Private keys: 32 bytes
//! - Public keys: 33 bytes (compressed) or 65 bytes (uncompressed)
//! - Signatures: DER-encoded ECDSA signatures

use p256::ecdsa::{signature::DigestSigner, Signature, SigningKey, VerifyingKey};
use p256::EncodedPoint;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use defra_core::Result;

use crate::error::crypto_error;
use crate::keys::{Key, PrivateKey, PublicKey};
use crate::types::{KeyType, SECP256R1_PRIVATE_KEY_SIZE};

/// secp256r1 (P-256) private key wrapper
#[derive(Clone)]
pub struct Secp256r1PrivateKey {
    key: SigningKey,
}

impl PartialEq for Secp256r1PrivateKey {
    fn eq(&self, other: &Self) -> bool {
        self.key.to_bytes().ct_eq(&other.key.to_bytes()).into()
    }
}

impl Eq for Secp256r1PrivateKey {}

impl std::fmt::Debug for Secp256r1PrivateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Secp256r1PrivateKey")
            .field("key_type", &KeyType::Secp256r1)
            .finish_non_exhaustive()
    }
}

impl Secp256r1PrivateKey {
    /// Create a new secp256r1 private key from raw bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(crypto_error("secp256r1 private key cannot be empty"));
        }

        if bytes.len() != SECP256R1_PRIVATE_KEY_SIZE {
            return Err(crypto_error(format!(
                "secp256r1 private key must be {} bytes, got {} bytes",
                SECP256R1_PRIVATE_KEY_SIZE,
                bytes.len()
            )));
        }

        let key = SigningKey::from_slice(bytes)
            .map_err(|e| crypto_error(format!("invalid secp256r1 private key: {}", e)))?;
        Ok(Self { key })
    }

    /// Get the underlying p256 signing key
    pub fn underlying(&self) -> &SigningKey {
        &self.key
    }
}

impl Key for Secp256r1PrivateKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Secp256r1 {
            return false;
        }
        let self_raw = self.raw();
        let other_raw = other.raw();
        if self_raw.len() != other_raw.len() {
            return false;
        }
        self_raw.ct_eq(&other_raw).into()
    }

    fn raw(&self) -> Vec<u8> {
        self.key.to_bytes().to_vec()
    }

    fn key_type(&self) -> KeyType {
        KeyType::Secp256r1
    }
}

impl PrivateKey for Secp256r1PrivateKey {
    fn sign(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut hasher = Sha256::new();
        hasher.update(data);

        let signature: Signature = self.key.sign_digest(hasher);

        Ok(signature.to_der().as_bytes().to_vec())
    }

    fn public_key(&self) -> Box<dyn PublicKey> {
        let verifying_key = *self.key.verifying_key();
        let compressed_bytes = verifying_key.to_encoded_point(true).as_bytes().to_vec();
        Box::new(Secp256r1PublicKey {
            key: verifying_key,
            compressed_bytes,
        })
    }
}

/// secp256r1 (P-256) public key wrapper
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
