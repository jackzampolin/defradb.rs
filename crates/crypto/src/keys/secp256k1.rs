//! secp256k1 key implementations
//!
//! secp256k1 is the elliptic curve used by Bitcoin, Ethereum, and other blockchains.
//! - Public keys: 33 bytes (compressed format with 0x02/0x03 prefix)
//! - Private keys: 32 bytes
//! - Signatures: DER-encoded ECDSA signatures

use k256::ecdsa::{
    signature::DigestSigner, signature::DigestVerifier, Signature, SigningKey, VerifyingKey,
};
use k256::EncodedPoint;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use defra_core::Result;

use crate::error::crypto_error;
use crate::keys::{Key, PrivateKey, PublicKey};
use crate::types::{KeyType, SECP256K1_PRIVATE_KEY_SIZE};

/// secp256k1 private key wrapper
#[derive(Clone)]
pub struct Secp256k1PrivateKey {
    key: SigningKey,
    raw_bytes: Zeroizing<Vec<u8>>,
    public_key: Secp256k1PublicKey,
}

impl PartialEq for Secp256k1PrivateKey {
    fn eq(&self, other: &Self) -> bool {
        // Compare private keys using constant-time comparison to prevent timing attacks
        self.key.to_bytes().ct_eq(&other.key.to_bytes()).into()
    }
}

impl Eq for Secp256k1PrivateKey {}

impl std::fmt::Debug for Secp256k1PrivateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't print key material for security
        f.debug_struct("Secp256k1PrivateKey")
            .field("key_type", &KeyType::Secp256k1)
            .finish_non_exhaustive()
    }
}

impl Secp256k1PrivateKey {
    /// Create a new secp256k1 private key from raw bytes
    ///
    /// # Parameters
    /// * `bytes` - 32-byte secp256k1 private key
    ///
    /// # Returns
    /// * `Ok(Secp256k1PrivateKey)` if the key is valid
    /// * `Err` if the key is invalid (wrong length, empty, or invalid scalar)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(crypto_error("secp256k1 private key cannot be empty"));
        }

        if bytes.len() != SECP256K1_PRIVATE_KEY_SIZE {
            return Err(crypto_error(format!(
                "secp256k1 private key must be {} bytes, got {} bytes",
                SECP256K1_PRIVATE_KEY_SIZE,
                bytes.len()
            )));
        }

        let key = SigningKey::from_slice(bytes)
            .map_err(|e| crypto_error(format!("invalid secp256k1 private key: {}", e)))?;
        let public_key = Secp256k1PublicKey::from_verifying_key(*key.verifying_key());
        Ok(Self {
            key,
            raw_bytes: Zeroizing::new(bytes.to_vec()),
            public_key,
        })
    }

    /// Get the underlying k256 signing key
    pub fn underlying(&self) -> &SigningKey {
        &self.key
    }

    /// Derive the corresponding secp256k1 public key without dynamic dispatch.
    pub fn to_public_key(&self) -> Secp256k1PublicKey {
        self.public_key.clone()
    }
}

impl Key for Secp256k1PrivateKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Secp256k1 {
            return false;
        }
        // Use constant-time comparison to prevent timing attacks
        let self_raw = self.raw();
        let other_raw = other.raw();
        if self_raw.len() != other_raw.len() {
            return false;
        }
        self_raw.ct_eq(other_raw).into()
    }

    fn raw(&self) -> &[u8] {
        &self.raw_bytes
    }

    fn key_type(&self) -> KeyType {
        KeyType::Secp256k1
    }
}

impl PrivateKey for Secp256k1PrivateKey {
    fn sign(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Hash the message with SHA-256 (required for ECDSA)
        // Use DigestSigner to sign pre-hashed data (matches Go behavior)
        let mut hasher = Sha256::new();
        hasher.update(data);

        // Sign the pre-hashed digest using DigestSigner trait
        let signature: Signature = self.key.sign_digest(hasher);

        // Return DER-encoded signature (X.690 Distinguished Encoding Rules)
        // DER is the standard format for Bitcoin/blockchain ECDSA signatures because:
        // 1. Self-describing format with type and length fields
        // 2. Deterministic serialization (unlike raw r,s which have variable length)
        // 3. Required for DefraDB Go implementation compatibility
        Ok(signature.to_der().as_bytes().to_vec())
    }

    fn public_key(&self) -> &dyn PublicKey {
        &self.public_key
    }
}

/// secp256k1 public key wrapper
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Secp256k1PublicKey {
    key: VerifyingKey,
    raw_bytes: Vec<u8>,
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
    /// * `Ok(Secp256k1PublicKey)` if the key is valid
    /// * `Err` if the key is invalid (wrong length, empty, or invalid point)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(crypto_error("secp256k1 public key cannot be empty"));
        }

        // EncodedPoint handles both compressed (33) and uncompressed (65) formats
        let point = EncodedPoint::from_bytes(bytes)
            .map_err(|e| crypto_error(format!("invalid secp256k1 public key encoding: {}", e)))?;
        let key = VerifyingKey::from_encoded_point(&point)
            .map_err(|_| crypto_error("invalid secp256k1 public key: not on curve"))?;

        Ok(Self::from_verifying_key(key))
    }

    /// Get the underlying k256 verifying key
    pub fn underlying(&self) -> &VerifyingKey {
        &self.key
    }

    fn from_verifying_key(key: VerifyingKey) -> Self {
        let raw_bytes = key.to_encoded_point(true).as_bytes().to_vec();
        Self { key, raw_bytes }
    }
}

impl Key for Secp256k1PublicKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Secp256k1 {
            return false;
        }
        // Use constant-time comparison to prevent timing attacks
        let self_raw = self.raw();
        let other_raw = other.raw();
        if self_raw.len() != other_raw.len() {
            return false;
        }
        self_raw.ct_eq(other_raw).into()
    }

    fn raw(&self) -> &[u8] {
        &self.raw_bytes
    }

    fn key_type(&self) -> KeyType {
        KeyType::Secp256k1
    }
}

impl Serialize for Secp256k1PublicKey {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.raw_bytes)
    }
}

impl<'de> Deserialize<'de> for Secp256k1PublicKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}

impl PublicKey for Secp256k1PublicKey {
    fn verify(&self, data: &[u8], signature: &[u8]) -> Result<bool> {
        // Parse DER-encoded signature
        let sig = Signature::from_der(signature)
            .map_err(|e| crypto_error(format!("invalid secp256k1 DER signature: {}", e)))?;

        // Preserve the caller-provided DER signature as-is so secp256k1
        // verification enforces canonical low-S semantics and matches Go.
        // Hash the message with SHA-256 using DigestVerifier trait
        // This matches Go behavior which signs/verifies against SHA256(message)
        let mut hasher = Sha256::new();
        hasher.update(data);

        // Verify signature against the pre-hashed digest
        self.key
            .verify_digest(hasher, &sig)
            .map(|_| true)
            .map_err(|e| crypto_error(format!("secp256k1 signature verification failed: {}", e)))
    }

    fn did(&self) -> Result<String> {
        // Use uncompressed format (65 bytes) for DID generation to match Go implementation
        // Go uses SerializeUncompressed() which produces the full [0x04 | x | y] format
        let uncompressed = self.key.to_encoded_point(false);
        crate::did::create_did_key(KeyType::Secp256k1, uncompressed.as_bytes())
    }
}

/// Extract the uncompressed x and y coordinates from a secp256k1 private key.
///
/// Returns (x, y) where each is a 32-byte big-endian representation of the
/// affine coordinate on the secp256k1 curve. Used for JWK export.
pub fn secp256k1_private_key_to_xy(private_bytes: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let signing_key = SigningKey::from_slice(private_bytes)
        .map_err(|e| crypto_error(format!("invalid secp256k1 private key: {}", e)))?;
    let point = signing_key.verifying_key().to_encoded_point(false);
    let x = point
        .x()
        .ok_or_else(|| crypto_error("secp256k1: missing x coordinate"))?
        .to_vec();
    let y = point
        .y()
        .ok_or_else(|| crypto_error("secp256k1: missing y coordinate"))?
        .to_vec();
    Ok((x, y))
}
