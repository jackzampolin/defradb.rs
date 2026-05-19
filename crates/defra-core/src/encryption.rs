//! Encryption configuration for CRDT delta encryption.
//!
//! Provides the EncryptionConfig type and thread-local storage for passing
//! encryption config from the query layer to the block builder layer.
//!
//! # Key generation model
//!
//! Each encrypted write generates a fresh random AES-256 key via
//! [`generate_encryption_key`]. The key is stored alongside its ciphertext
//! in a separate `Encryption` block (see
//! [`crate::block_signature::Encryption`]), and the data block links to it
//! via its `encryption: Option<Cid>` field. Decryption loads the
//! `Encryption` block by CID and reads the key directly — no master key
//! is needed at the receiver.
//!
//! This matches Go DefraDB's `internal/encryption/encryptor.go` exactly.
//! The previous Rust implementation derived keys deterministically from
//! `SHA-256(field_name || doc_id || master_key)`, which was both wire-
//! incompatible with Go (different key bytes) and cryptographically
//! weaker (one master-key compromise revealed every past, present, and
//! future document). See #651 for the audit trail.

use rand::RngCore;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use zeroize::{Zeroize, ZeroizeOnDrop};

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
/// material. Each encrypted write generates a fresh random key via
/// [`generate_encryption_key`] and stores it in the per-block
/// `Encryption` metadata.
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
/// Production builds use fresh random keys. When FFI runs under the Go
/// integration test binary, this mirrors Go's `generateTestEncryptionKey` so
/// expected encrypted deltas and CIDs are reproducible.
pub fn generate_encryption_key_for(doc_id: &str, field_name: Option<&str>) -> [u8; 32] {
    if USE_DETERMINISTIC_ENCRYPTION_KEY.load(Ordering::Acquire) {
        return generate_deterministic_encryption_key(doc_id, field_name);
    }
    generate_encryption_key()
}

#[doc(hidden)]
pub fn set_deterministic_encryption_key(enabled: bool) {
    USE_DETERMINISTIC_ENCRYPTION_KEY.store(enabled, Ordering::Release);
}

#[doc(hidden)]
pub fn deterministic_encryption_key_enabled() -> bool {
    USE_DETERMINISTIC_ENCRYPTION_KEY.load(Ordering::Acquire)
}

fn generate_deterministic_encryption_key(doc_id: &str, field_name: Option<&str>) -> [u8; 32] {
    let material = format!(
        "{}{}{}",
        field_name.unwrap_or(""),
        doc_id,
        TEST_ENCRYPTION_KEY
    );
    let bytes = material.as_bytes();
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes[..32]);
    key
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

/// Global per-document encryption store.
/// Maps docID → EncryptionConfig so updates can re-apply encryption
/// that was set during document creation.
static DOC_ENCRYPTION_STORE: std::sync::OnceLock<Mutex<HashMap<String, EncryptionConfig>>> =
    std::sync::OnceLock::new();

fn doc_encryption_store() -> &'static Mutex<HashMap<String, EncryptionConfig>> {
    DOC_ENCRYPTION_STORE.get_or_init(|| Mutex::new(HashMap::new()))
}

const MAX_DOC_ENCRYPTION_ENTRIES: usize = 10_000;

/// Store encryption config for a document.
pub fn store_doc_encryption(doc_id: &str, config: EncryptionConfig) {
    if let Ok(mut store) = doc_encryption_store().lock() {
        if store.len() >= MAX_DOC_ENCRYPTION_ENTRIES {
            store.clear();
        }
        store.insert(doc_id.to_string(), config);
    }
}

/// Retrieve stored encryption config for a document.
pub fn get_doc_encryption(doc_id: &str) -> Option<EncryptionConfig> {
    doc_encryption_store()
        .lock()
        .ok()
        .and_then(|store| store.get(doc_id).cloned())
}

/// Clear all stored encryption configs (for node cleanup).
pub fn clear_doc_encryption_store() {
    if let Ok(mut store) = doc_encryption_store().lock() {
        store.clear();
    }
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
