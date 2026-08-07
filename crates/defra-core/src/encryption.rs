//! Encryption configuration for CRDT delta encryption.
//!
//! Provides the EncryptionConfig type and thread-local storage for passing
//! encryption config from the query layer to the block builder layer.
//!
//! # Key generation model
//!
//! A write that explicitly requests encryption generates a fresh random
//! AES-256 key via [`generate_encryption_key`]. The key is stored alongside
//! its ciphertext in a separate `Encryption` block (see
//! [`crate::block_signature::Encryption`]), and the data block links to it
//! via its `encryption: Option<Cid>` field. Decryption loads the
//! `Encryption` block by CID and reads the key directly — no master key
//! is needed at the receiver.
//!
//! A write that carries no config does not turn encryption off: the block
//! writer inherits the previous block's key and `Encryption` link, so a field
//! created encrypted stays encrypted under one key for its whole history until
//! a new explicit request rotates it. The DAG, not any process-local state, is
//! what records that a document is encrypted — see `db-blocks`'
//! `inherited_encryption`, mirroring Go's `determineBlockEncryption` in
//! `internal/core/block/store.go`.
//!
//! Inheritance is per field and requires a previous head. A field first
//! written by an update has none, so on a document encrypted as a whole it
//! falls back to the document-level policy recorded by the composite block's
//! encryption link, which mints it a key rather than leaving it in the clear.
//! Go writes that field in plaintext instead (its head set is per-field,
//! `internal/core/block/store.go`); this is a deliberate divergence.
//!
//! Reusing one key across a field's history is sound because every delta is
//! sealed with a fresh random 96-bit nonce (see `crypto::encryption::nonce`).
//! That uniqueness is load-bearing under this model: a deterministic or
//! counter-based nonce would repeat a (key, nonce) pair across updates.
//!
//! This matches Go DefraDB's `internal/encryption/encryptor.go` exactly.
//! The previous Rust implementation derived keys deterministically from
//! `SHA-256(field_name || doc_id || master_key)`, which was both wire-
//! incompatible with Go (different key bytes) and cryptographically
//! weaker (one master-key compromise revealed every past, present, and
//! future document). See #651 for the audit trail.
//!
//! Deterministic encryption keys are retained only for Go-compatibility tests
//! that assert exact encrypted CIDs. Release builds require
//! `DEFRA_ALLOW_DETERMINISTIC_TEST_CRYPTO=1` before the hidden test switch can
//! be enabled; production deployments must never set that variable.

use rand::RngCore;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use zeroize::{Zeroize, ZeroizeOnDrop};

const DETERMINISTIC_TEST_CRYPTO_ENV: &str = "DEFRA_ALLOW_DETERMINISTIC_TEST_CRYPTO";

/// Wrapper for encryption key bytes with zeroization on drop.
///
/// Uses `zeroize::ZeroizeOnDrop` to clear key material when dropped, with
/// a memory fence after zeroing. This is the RustCrypto ecosystem standard
/// and replaces a previous manual `unsafe { write_volatile }` implementation.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct EncryptionKey(Vec<u8>);

impl EncryptionKey {
    /// Create a new encryption key from raw bytes.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Access the raw key bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EncryptionKey([REDACTED; {} bytes])", self.0.len())
    }
}

impl From<Vec<u8>> for EncryptionKey {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<EncryptionKey> for Vec<u8> {
    fn from(key: EncryptionKey) -> Self {
        key.0.clone()
    }
}

impl AsRef<[u8]> for EncryptionKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Encryption policy for a CRDT document mutation.
///
/// Carries which fields should be encrypted; does NOT carry any key
/// material. A write that supplies this config generates a fresh random key
/// via [`generate_encryption_key`] and stores it in the per-block
/// `Encryption` metadata; a write without one inherits the previous block's
/// key instead of dropping to plaintext.
#[derive(Debug, Clone, Default)]
pub struct EncryptionConfig {
    pub encrypt_doc: bool,
    pub encrypt_fields: Vec<String>,
}

impl EncryptionConfig {
    /// Check if a field should be encrypted.
    pub fn should_encrypt_field(&self, field_name: &str) -> bool {
        self.encrypt_doc || self.encrypt_fields.iter().any(|f| f == field_name)
    }

    /// Check if a field should use its own field-level encryption key.
    pub fn should_encrypt_individual_field(&self, field_name: &str) -> bool {
        self.encrypt_fields.iter().any(|f| f == field_name)
    }
}

#[doc(hidden)]
static USE_DETERMINISTIC_ENCRYPTION_KEY: AtomicBool = AtomicBool::new(false);

const TEST_ENCRYPTION_KEY: &str = "examplekey1234567890examplekey12";

/// Generate a fresh random 32-byte AES-256 key from the OS RNG.
///
/// Matches Go's `internal/encryption/encryptor.go::generateEncryptionKey`:
/// each write gets a unique random key. The key is stored alongside the
/// ciphertext in a separate `Encryption` block (see
/// [`crate::block_signature::Encryption`]), and the data block points
/// at it via the `encryption: Option<Cid>` link.
///
/// Uses [`rand::rngs::OsRng`] which reads from the OS-provided
/// cryptographically secure random source (`getrandom` on Linux,
/// `SecRandomCopyBytes` on macOS, `BCryptGenRandom` on Windows).
pub fn generate_encryption_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut key);
    key
}

/// Generate an AES-256 key for a document field.
///
/// `doc_key` is the node-local document identity (the encoded DocRef —
/// collection short ID + doc short ID), matching Go's post-#4838
/// encryptor cache key. Production builds use fresh random keys. When
/// FFI runs under the Go integration test binary, this mirrors Go's
/// `generateTestEncryptionKey` so expected encrypted deltas and CIDs
/// are reproducible.
pub fn generate_encryption_key_for(doc_key: &[u8], field_name: Option<&str>) -> [u8; 32] {
    if USE_DETERMINISTIC_ENCRYPTION_KEY.load(Ordering::Acquire) {
        return generate_deterministic_encryption_key(doc_key, field_name);
    }
    generate_encryption_key()
}

#[doc(hidden)]
pub fn set_deterministic_encryption_key(enabled: bool) {
    USE_DETERMINISTIC_ENCRYPTION_KEY.store(
        enabled && deterministic_test_crypto_allowed(),
        Ordering::Release,
    );
}

#[doc(hidden)]
pub fn deterministic_encryption_key_enabled() -> bool {
    USE_DETERMINISTIC_ENCRYPTION_KEY.load(Ordering::Acquire)
}

fn generate_deterministic_encryption_key(doc_key: &[u8], field_name: Option<&str>) -> [u8; 32] {
    let mut material = Vec::new();
    material.extend_from_slice(field_name.unwrap_or("").as_bytes());
    material.extend_from_slice(doc_key);
    material.extend_from_slice(TEST_ENCRYPTION_KEY.as_bytes());
    let mut key = [0u8; 32];
    key.copy_from_slice(&material[..32]);
    key
}

fn deterministic_test_crypto_allowed() -> bool {
    cfg!(debug_assertions)
        || matches!(
            std::env::var(DETERMINISTIC_TEST_CRYPTO_ENV).as_deref(),
            Ok("1")
        )
}

thread_local! {
    static CURRENT_ENCRYPTION_CONFIG: RefCell<Option<EncryptionConfig>> = const { RefCell::new(None) };
}

/// Set the encryption config for the current thread.
pub fn set_encryption_config(config: Option<EncryptionConfig>) {
    CURRENT_ENCRYPTION_CONFIG.with(|c| {
        *c.borrow_mut() = config;
    });
}

/// Get the current encryption config for this thread.
pub fn get_encryption_config() -> Option<EncryptionConfig> {
    CURRENT_ENCRYPTION_CONFIG.with(|c| c.borrow().clone())
}

#[cfg(test)]
mod tests {
    use super::{generate_encryption_key, EncryptionKey};
    use zeroize::Zeroize;

    #[test]
    fn generate_encryption_key_is_random() {
        let k1 = generate_encryption_key();
        let k2 = generate_encryption_key();
        assert_ne!(k1, k2);
        assert_eq!(k1.len(), 32);
    }

    #[test]
    fn encryption_key_zeroizes_buffer() {
        let mut key = EncryptionKey::new(vec![0xAB; 32]);
        assert_eq!(key.as_bytes(), &[0xAB; 32]);
        key.zeroize();
        // `Zeroize for Vec<u8>` zeroes the elements and truncates length to 0.
        assert!(key.as_bytes().is_empty());
    }
}
