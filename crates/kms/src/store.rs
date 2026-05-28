//! Persistent storage for DEK material AND its on-disk Encryption block bytes.
//!
//! M1 ships `MemoryKeyStore`. Future milestones add `KeyringKeyStore` (M3),
//! `EnclaveKeyStore` (M5), `ThresholdKeyStore` (M6). All share this trait.

use async_trait::async_trait;

use crate::error::Result;
use crate::types::{EncryptionCid, KeyScope};

/// One persisted entry: the raw 32-byte DEK plus the canonical
/// `Encryption` block bytes (CBOR-serialized
/// `defra_core::Encryption`).
///
/// The KMS keeps the block bytes alongside the key because the wire
/// protocol ECIES-wraps the *block bytes* (not just the key) for delivery
/// to a peer. Bundling both here means `KeyStore::get` can serve a
/// `serve_request` path without re-reading the blockstore.
#[derive(Debug, Clone)]
pub struct StoredKey {
    /// The 32-byte AES-256 DEK in plaintext.
    pub key: [u8; 32],
    /// The CBOR-encoded `defra_core::Encryption` block whose CID is the
    /// `EncryptionCid` this `StoredKey` is keyed by.
    pub block_bytes: Vec<u8>,
}

/// Pluggable storage for DEKs + their on-disk block bytes.
#[async_trait]
pub trait KeyStore: Send + Sync {
    /// Persist a DEK + block bytes under the content-addressed CID.
    /// Idempotent: re-putting the same CID overwrites.
    async fn put(&self, cid: EncryptionCid, stored: StoredKey) -> Result<()>;

    /// Retrieve a stored DEK + block bytes by CID. None if not held locally.
    async fn get(&self, cid: &EncryptionCid) -> Result<Option<StoredKey>>;

    /// Generate a fresh DEK for the given scope. Returns the CID of the
    /// persisted block + the `StoredKey` containing the plain key and block bytes.
    async fn generate(&self, scope: &KeyScope) -> Result<(EncryptionCid, StoredKey)>;

    /// Remove a DEK from local storage. No-op if absent.
    async fn delete(&self, cid: &EncryptionCid) -> Result<()>;

    /// List all CIDs held locally. Backends that don't support listing
    /// (e.g. OS keyring) return `Error::Unsupported`.
    async fn list(&self) -> Result<Vec<EncryptionCid>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    fn assert_object_safe<T: ?Sized + Send + Sync>() {}

    #[test]
    fn key_store_is_object_safe() {
        assert_object_safe::<dyn KeyStore>();
    }
}
