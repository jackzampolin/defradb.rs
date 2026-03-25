//! Encryption configuration for CRDT delta encryption.
//!
//! Provides the EncryptionConfig type and thread-local storage for passing
//! encryption config from the query layer to the block builder layer.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Mutex;

/// Wrapper for encryption key bytes with zeroization on drop.
///
/// Ensures key material is cleared from memory when no longer needed.
#[derive(Clone)]
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

impl Drop for EncryptionKey {
    fn drop(&mut self) {
        for byte in self.0.iter_mut() {
            // Use write_volatile to prevent the compiler from optimizing away the zeroing.
            // SAFETY: We are writing to a valid, properly aligned mutable reference.
            unsafe {
                std::ptr::write_volatile(byte, 0);
            }
        }
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

/// Encryption configuration for CRDT delta encryption.
#[derive(Debug, Clone)]
pub struct EncryptionConfig {
    pub encrypt_doc: bool,
    pub encrypt_fields: Vec<String>,
    pub encryption_key: Vec<u8>,
}

impl EncryptionConfig {
    /// Check if a field should be encrypted.
    pub fn should_encrypt_field(&self, field_name: &str) -> bool {
        self.encrypt_doc || self.encrypt_fields.iter().any(|f| f == field_name)
    }

    /// Derive the encryption key for a specific field and document.
    ///
    /// Key derivation: `(fieldName + docID + masterKey)[:32]`
    /// - Doc-level: fieldName = "" (empty string)
    /// - Field-level: fieldName = specific field name
    pub fn derive_key(&self, field_name: &str, doc_id: &str) -> [u8; 32] {
        let field = if self.encrypt_fields.iter().any(|f| f == field_name) {
            field_name
        } else {
            "" // doc-level
        };
        let mut key_material = Vec::new();
        key_material.extend_from_slice(field.as_bytes());
        key_material.extend_from_slice(doc_id.as_bytes());
        key_material.extend_from_slice(&self.encryption_key);
        let mut key = [0u8; 32];
        let len = key_material.len().min(32);
        key[..len].copy_from_slice(&key_material[..len]);
        key
    }
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
