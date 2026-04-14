//! Cryptographic key management
//!
//! This module provides trait definitions and implementations for various
//! cryptographic key types (secp256k1, Ed25519, secp256r1).

#[cfg(not(target_arch = "wasm32"))]
pub mod bls;
pub mod ed25519;
pub mod generation;
pub mod secp256k1;
pub mod secp256r1;

/// Base trait for all cryptographic keys
///
/// Provides common operations for key comparison, serialization, and type information.
pub trait Key: defra_core::thread_bounds::MaybeSendSync {
    /// Check if this key equals another key (byte-level comparison)
    fn equal(&self, other: &dyn Key) -> bool;

    /// Get the raw bytes of this key
    fn raw(&self) -> &[u8];

    /// Get an owned copy of the raw bytes when required by external APIs.
    fn raw_owned(&self) -> Vec<u8> {
        self.raw().to_vec()
    }

    /// Get the hex-encoded string representation of this key
    fn to_hex_string(&self) -> String {
        hex::encode(self.raw())
    }

    /// Get the key type
    fn key_type(&self) -> crate::types::KeyType;
}

/// Public key operations
pub trait PublicKey: Key {
    /// Verify a signature over data using this public key.
    ///
    /// Returns `Ok(true)` on success and `Err` for malformed signatures or
    /// cryptographic verification failures.
    fn verify(&self, data: &[u8], signature: &[u8]) -> defra_core::Result<bool>;

    /// Generate a DID key representation of this public key
    fn did(&self) -> defra_core::Result<String>;
}

/// Private key operations
pub trait PrivateKey: Key {
    /// Sign the given data
    fn sign(&self, data: &[u8]) -> defra_core::Result<Vec<u8>>;

    /// Get the corresponding public key
    fn public_key(&self) -> &dyn PublicKey;
}
