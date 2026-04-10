//! Encryption configuration for CRDT delta encryption.
//!
//! Provides the EncryptionConfig type and thread-local storage for passing
//! encryption config from the query layer to the block builder layer.

use rand::RngCore;
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

    fn effective_field_name(&self, field_name: &str) -> Option<String> {
        if self.encrypt_fields.iter().any(|f| f == field_name) {
            Some(field_name.to_string())
        } else {
            None
        }
    }

    /// Return the key used for the given doc/field pair, generating one if needed.
    ///
    /// This mirrors Go's document encryptor behavior:
    /// - doc-level encryption reuses one key per document
    /// - field-level encryption reuses one key per `(doc, field)` pair
    /// - production keys are generated randomly instead of being derived
    pub fn get_or_generate_key(&self, field_name: &str, doc_id: &str) -> [u8; 32] {
        let field = self.effective_field_name(field_name);
        let cache_key = (doc_id.to_string(), field);

        let mut store = doc_encryption_key_store()
            .lock()
            .expect("doc encryption key store poisoned");

        if store.len() >= MAX_DOC_ENCRYPTION_ENTRIES {
            store.clear();
        }

        if let Some(existing) = store.get(&cache_key) {
            return *existing;
        }

        let mut key = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut key);
        store.insert(cache_key, key);
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

static DOC_ENCRYPTION_KEY_STORE: std::sync::OnceLock<
    Mutex<HashMap<(String, Option<String>), [u8; 32]>>,
> = std::sync::OnceLock::new();

fn doc_encryption_store() -> &'static Mutex<HashMap<String, EncryptionConfig>> {
    DOC_ENCRYPTION_STORE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn doc_encryption_key_store() -> &'static Mutex<HashMap<(String, Option<String>), [u8; 32]>> {
    DOC_ENCRYPTION_KEY_STORE.get_or_init(|| Mutex::new(HashMap::new()))
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
    if let Ok(mut store) = doc_encryption_key_store().lock() {
        store.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::EncryptionConfig;

    #[test]
    fn generated_key_is_reused_for_same_doc_and_field() {
        super::clear_doc_encryption_store();
        let config = EncryptionConfig {
            encrypt_doc: true,
            encrypt_fields: vec![],
            encryption_key: Vec::new(),
        };

        let key_a = config.get_or_generate_key("", "bae-c94acbfa-1234-5678-90ab-cdef12345678");
        let key_b = config.get_or_generate_key("", "bae-c94acbfa-1234-5678-90ab-cdef12345678");

        assert_eq!(key_a, key_b);
    }

    #[test]
    fn generated_keys_differ_for_distinct_field_scopes() {
        super::clear_doc_encryption_store();
        let config = EncryptionConfig {
            encrypt_doc: false,
            encrypt_fields: vec!["name".to_string(), "email".to_string()],
            encryption_key: Vec::new(),
        };

        let name_key = config.get_or_generate_key("name", "doc1");
        let email_key = config.get_or_generate_key("email", "doc1");
        let doc_level_key = config.get_or_generate_key("other", "doc1");

        assert_ne!(name_key, email_key);
        assert_ne!(name_key, doc_level_key);
        assert_ne!(email_key, doc_level_key);
    }
}
