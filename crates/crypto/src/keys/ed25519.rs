//! Ed25519 key implementations
//!
//! Ed25519 is a modern, high-security elliptic curve signature scheme.
//! - Public keys: 32 bytes
//! - Private keys: 64 bytes (includes public key)
//! - Signatures: 64 bytes

use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey, PUBLIC_KEY_LENGTH};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};

use defra_core::Result;

use crate::error::{crypto_error, signature_verification_failed};
use crate::keys::{Key, PrivateKey, PublicKey};
use crate::types::KeyType;

/// Ed25519 private key wrapper
///
/// # Private Key Format
///
/// Ed25519 private keys in this implementation use a **64-byte representation**:
/// - **Bytes 0-31**: 32-byte seed (the actual private scalar)
/// - **Bytes 32-63**: 32-byte public key (derived from the seed)
///
/// This format matches the Go implementation and is compatible with many Ed25519
/// libraries. The 64-byte format allows storing both the private seed and public
/// key together for efficient operations.
///
/// When creating a key from bytes using `from_bytes()`, exactly 64 bytes must be
/// provided. The first 32 bytes are used as the seed to reconstruct the signing key.
///
/// # Compatibility Note
///
/// This 64-byte format is standard in many implementations including Go's `crypto/ed25519`,
/// libsodium's `crypto_sign_keypair`, and PyNaCl. Some libraries use only 32-byte seeds;
/// when interoperating with those, use only the first 32 bytes of this format.
#[derive(Clone)]
pub struct Ed25519PrivateKey {
    key: SigningKey,
    raw_bytes: Zeroizing<Vec<u8>>,
    public_key: Ed25519PublicKey,
}

impl PartialEq for Ed25519PrivateKey {
    fn eq(&self, other: &Self) -> bool {
        // Compare private keys using constant-time comparison to prevent timing attacks
        // Compare the 32-byte seed (not the full 64-byte representation which includes public key)
        self.key.to_bytes().ct_eq(&other.key.to_bytes()).into()
    }
}

impl Eq for Ed25519PrivateKey {}

impl std::fmt::Debug for Ed25519PrivateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't print key material for security
        f.debug_struct("Ed25519PrivateKey")
            .field("key_type", &KeyType::Ed25519)
            .finish_non_exhaustive()
    }
}

impl Ed25519PrivateKey {
    /// Create a new Ed25519 private key from raw bytes
    ///
    /// # Parameters
    /// * `bytes` - 64-byte Ed25519 private key
    ///
    /// # Returns
    /// * `Ok(Ed25519PrivateKey)` if the key is valid
    /// * `Err` if the key is invalid (wrong length or empty)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(crypto_error("Ed25519 private key cannot be empty"));
        }

        // Ed25519 private keys stored as 64 bytes (32-byte seed + 32-byte public key)
        // NOTE: RFC 8032 defines Ed25519 private keys as 32 bytes (just the seed).
        // We use 64-byte format for compatibility with DefraDB Go implementation,
        // which stores both seed and derived public key together.
        if bytes.len() != 64 {
            return Err(crypto_error(format!(
                "Ed25519 private key must be 64 bytes, got {} bytes",
                bytes.len()
            )));
        }

        // ed25519-dalek expects 32 bytes for SigningKey (the seed)
        // The first 32 bytes are the seed
        let mut seed: [u8; 32] = bytes[..32]
            .try_into()
            .map_err(|_| crypto_error("failed to extract Ed25519 seed from key bytes"))?;
        let key = SigningKey::from_bytes(&seed);

        // Validate that the provided public key (bytes 32-63) matches the derived public key
        // This catches malformed keys where the public key portion doesn't match the seed
        let derived_public = key.verifying_key().to_bytes();
        let provided_public = &bytes[32..64];
        if derived_public != provided_public {
            seed.zeroize();
            return Err(crypto_error(
                "Ed25519 public key portion does not match derived key from seed",
            ));
        }

        seed.zeroize();
        let public_key = Ed25519PublicKey::from_verifying_key(key.verifying_key());
        Ok(Self {
            key,
            raw_bytes: Zeroizing::new(bytes.to_vec()),
            public_key,
        })
    }

    /// Get the underlying ed25519-dalek signing key
    pub fn underlying(&self) -> &SigningKey {
        &self.key
    }

    /// Derive the corresponding Ed25519 public key without dynamic dispatch.
    pub fn to_public_key(&self) -> Ed25519PublicKey {
        self.public_key.clone()
    }
}

impl Key for Ed25519PrivateKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Ed25519 {
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

    fn raw(&self) -> &[u8] {
        &self.raw_bytes
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

    fn public_key(&self) -> &dyn PublicKey {
        &self.public_key
    }
}

/// Ed25519 public key wrapper
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ed25519PublicKey {
    key: VerifyingKey,
    raw_bytes: [u8; PUBLIC_KEY_LENGTH],
}

impl Ed25519PublicKey {
    /// Create a new Ed25519 public key from raw bytes
    ///
    /// # Parameters
    /// * `bytes` - 32-byte Ed25519 public key
    ///
    /// # Returns
    /// * `Ok(Ed25519PublicKey)` if the key is valid
    /// * `Err` if the key is invalid (wrong length, empty, or invalid point)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(crypto_error("Ed25519 public key cannot be empty"));
        }

        if bytes.len() != PUBLIC_KEY_LENGTH {
            return Err(crypto_error(format!(
                "Ed25519 public key must be {} bytes, got {} bytes",
                PUBLIC_KEY_LENGTH,
                bytes.len()
            )));
        }

        let key_bytes: [u8; PUBLIC_KEY_LENGTH] = bytes
            .try_into()
            .map_err(|_| crypto_error("failed to convert Ed25519 public key bytes"))?;
        let key = VerifyingKey::from_bytes(&key_bytes)
            .map_err(|e| crypto_error(format!("invalid Ed25519 public key: {}", e)))?;

        Ok(Self {
            key,
            raw_bytes: key_bytes,
        })
    }

    /// Get the underlying ed25519-dalek verifying key
    pub fn underlying(&self) -> &VerifyingKey {
        &self.key
    }

    fn from_verifying_key(key: VerifyingKey) -> Self {
        Self {
            raw_bytes: key.to_bytes(),
            key,
        }
    }
}

impl Key for Ed25519PublicKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Ed25519 {
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

    fn raw(&self) -> &[u8] {
        &self.raw_bytes
    }

    fn key_type(&self) -> KeyType {
        KeyType::Ed25519
    }
}

impl Serialize for Ed25519PublicKey {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.raw_bytes)
    }
}

impl<'de> Deserialize<'de> for Ed25519PublicKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
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

/// Reconstruct a 64-byte Ed25519 key (seed || pubkey) from a 32-byte seed.
///
/// Ed25519 private keys in this codebase use the 64-byte representation
/// (32-byte seed + 32-byte derived public key). This function takes just the
/// seed and derives the full keypair bytes. Used for JWK import.
pub fn ed25519_key_from_seed(seed: &[u8]) -> Result<Vec<u8>> {
    let mut seed_array: [u8; 32] = seed
        .try_into()
        .map_err(|_| crypto_error("ed25519 seed must be exactly 32 bytes"))?;
    let signing_key = SigningKey::from_bytes(&seed_array);
    let verifying_key = signing_key.verifying_key();
    let mut full_key = Vec::with_capacity(64);
    full_key.extend_from_slice(seed);
    full_key.extend_from_slice(verifying_key.as_bytes());
    seed_array.zeroize();
    Ok(full_key)
}
