//! Encryption configuration for CRDT delta encryption.
//!
//! Provides the EncryptionConfig type and thread-local storage for passing
//! encryption config from the query layer to the block builder layer.

use sha2::{Digest, Sha256};
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
    /// Key derivation: `SHA-256(fieldName + docID + masterKey)`
    /// - Doc-level: fieldName = "" (empty string)
    /// - Field-level: fieldName = specific field name
    ///
    /// Uses SHA-256 to ensure all input material (including the master key)
    /// contributes to the derived key regardless of field name or doc ID length.
    pub fn derive_key(&self, field_name: &str, doc_id: &str) -> [u8; 32] {
        let field = if self.encrypt_fields.iter().any(|f| f == field_name) {
            field_name
        } else {
            "" // doc-level
        };
        let mut hasher = Sha256::new();
        hasher.update(field.as_bytes());
        hasher.update(doc_id.as_bytes());
        hasher.update(&self.encryption_key);
        hasher.finalize().into()
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

#[cfg(test)]
mod tests {
    use super::EncryptionConfig;

    #[test]
    fn derive_key_mixes_in_master_key_for_long_doc_ids() {
        let doc_id = "bae-c94acbfa-1234-5678-90ab-cdef12345678";
        let config_a = EncryptionConfig {
            encrypt_doc: true,
            encrypt_fields: vec![],
            encryption_key: b"first-master-key-material-123456".to_vec(),
        };
        let config_b = EncryptionConfig {
            encrypt_doc: true,
            encrypt_fields: vec![],
            encryption_key: b"second-master-key-material654321".to_vec(),
        };

        assert_ne!(config_a.derive_key("", doc_id), config_b.derive_key("", doc_id));
    }
}
