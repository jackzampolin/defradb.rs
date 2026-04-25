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
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use defra_core::Result;

use crate::error::crypto_error;
use crate::keys::{Key, PrivateKey, PublicKey};
use crate::types::{KeyType, SECP256R1_PRIVATE_KEY_SIZE};

/// secp256r1 (P-256) private key wrapper
#[derive(Clone)]
pub struct Secp256r1PrivateKey {
    key: SigningKey,
    raw_bytes: Zeroizing<Vec<u8>>,
    public_key: Secp256r1PublicKey,
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
        let public_key = Secp256r1PublicKey::from_verifying_key(*key.verifying_key());
        Ok(Self {
            key,
            raw_bytes: Zeroizing::new(bytes.to_vec()),
            public_key,
        })
    }

    /// Get the underlying p256 signing key
    pub fn underlying(&self) -> &SigningKey {
        &self.key
    }

    /// Derive the corresponding secp256r1 public key without dynamic dispatch.
    pub fn to_public_key(&self) -> Secp256r1PublicKey {
        self.public_key.clone()
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
        self_raw.ct_eq(other_raw).into()
    }

    fn raw(&self) -> &[u8] {
        &self.raw_bytes
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

        // Normalize S to low-S form for compatibility with Go verifiers and
        // to prevent signature malleability (ECDSA signatures are malleable without this).
        let signature = signature.normalize_s().unwrap_or(signature);

        Ok(signature.to_der().as_bytes().to_vec())
    }

    fn public_key(&self) -> &dyn PublicKey {
        &self.public_key
    }
}

/// secp256r1 (P-256) public key wrapper
#[derive(Clone, Debug)]
pub struct Secp256r1PublicKey {
    key: VerifyingKey,
    raw_bytes: Vec<u8>,
}

impl PartialEq for Secp256r1PublicKey {
    fn eq(&self, other: &Self) -> bool {
        self.raw_bytes.ct_eq(&other.raw_bytes).into()
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

        Ok(Self::from_verifying_key(key))
    }

    /// Get the underlying p256 verifying key
    pub fn underlying(&self) -> &VerifyingKey {
        &self.key
    }

    fn from_verifying_key(key: VerifyingKey) -> Self {
        let raw_bytes = key.to_encoded_point(true).as_bytes().to_vec();
        Self { key, raw_bytes }
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
        self_raw.ct_eq(other_raw).into()
    }

    fn raw(&self) -> &[u8] {
        &self.raw_bytes
    }

    fn key_type(&self) -> KeyType {
        KeyType::Secp256r1
    }
}

impl Serialize for Secp256r1PublicKey {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.raw_bytes)
    }
}

impl<'de> Deserialize<'de> for Secp256r1PublicKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}

impl PublicKey for Secp256r1PublicKey {
    fn verify(&self, data: &[u8], signature: &[u8]) -> Result<bool> {
        use p256::ecdsa::signature::DigestVerifier;

        // Parse DER-encoded signature
        let sig = Signature::from_der(signature)
            .map_err(|e| crypto_error(format!("invalid secp256r1 DER signature: {}", e)))?;

        // Normalize S to low-S form for compatibility with various signers
        let sig = sig.normalize_s().unwrap_or(sig);

        // Hash the message with SHA-256 first
        // Go's ecdsa.Sign takes a pre-computed hash, so we must also pre-hash
        // and use verify_digest (DigestVerifier) to match Go behavior
        let digest = Sha256::new_with_prefix(data);

        // Verify signature using pre-hashed digest
        self.key
            .verify_digest(digest, &sig)
            .map(|_| true)
            .map_err(|e| crypto_error(format!("secp256r1 signature verification failed: {}", e)))
    }

    fn did(&self) -> Result<String> {
        // Use uncompressed format (65 bytes) for DID generation to match Go implementation
        // Go uses ecdhPubKey.Bytes() which produces the full [0x04 | x | y] format
        let uncompressed = self.key.to_encoded_point(false);
        crate::did::create_did_key(KeyType::Secp256r1, uncompressed.as_bytes())
    }
}

/// Extract the uncompressed x and y coordinates from a secp256r1 private key.
///
/// Returns (x, y) where each is a 32-byte big-endian representation. Used for JWK export.
pub fn secp256r1_private_key_to_xy(private_bytes: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let signing_key = SigningKey::from_slice(private_bytes)
        .map_err(|e| crypto_error(format!("invalid secp256r1 private key: {}", e)))?;
    let point = signing_key.verifying_key().to_encoded_point(false);
    let x = point
        .x()
        .ok_or_else(|| crypto_error("secp256r1: missing x coordinate"))?
        .to_vec();
    let y = point
        .y()
        .ok_or_else(|| crypto_error("secp256r1: missing y coordinate"))?
        .to_vec();
    Ok((x, y))
}
