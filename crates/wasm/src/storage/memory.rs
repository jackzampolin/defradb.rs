//! WASM-compatible in-memory storage.
//!
//! A simple key-value store that works in WASM without tokio.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::error::{Result, WasmError};

/// Simple in-memory key-value store for WASM.
///
/// Uses standard library synchronization primitives instead of tokio.
/// All operations are synchronous from the Rust perspective, but can
/// be called from async JavaScript code.
#[derive(Clone)]
pub struct WasmMemoryStore {
    data: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,
    closed: Arc<RwLock<bool>>,
}

impl WasmMemoryStore {
    /// Create a new empty memory store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            closed: Arc::new(RwLock::new(false)),
        }
    }

    /// Check if the store is closed.
    pub async fn is_closed(&self) -> bool {
        *self.closed.read().unwrap()
    }

    /// Close the store.
    pub async fn close(&self) -> Result<()> {
        let mut closed = self.closed.write().unwrap();
        *closed = true;
        Ok(())
    }

    /// Get a value by key.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if *self.closed.read().unwrap() {
            return Err(WasmError::Closed);
        }
        Ok(self.data.read().unwrap().get(key).cloned())
    }

    /// Set a value for a key.
    pub fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        if *self.closed.read().unwrap() {
            return Err(WasmError::Closed);
        }
        self.data.write().unwrap().insert(key.to_string(), value);
        Ok(())
    }

    /// Delete a key.
    pub fn delete(&self, key: &str) -> Result<()> {
        if *self.closed.read().unwrap() {
            return Err(WasmError::Closed);
        }
        self.data.write().unwrap().remove(key);
        Ok(())
    }

    /// Check if a key exists.
    pub fn has(&self, key: &str) -> Result<bool> {
        if *self.closed.read().unwrap() {
            return Err(WasmError::Closed);
        }
        Ok(self.data.read().unwrap().contains_key(key))
    }

    /// Get all keys with a given prefix.
    pub fn keys_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        if *self.closed.read().unwrap() {
            return Err(WasmError::Closed);
        }
        let data = self.data.read().unwrap();
        Ok(data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    /// Clear all data.
    pub fn clear(&self) -> Result<()> {
        if *self.closed.read().unwrap() {
            return Err(WasmError::Closed);
        }
        self.data.write().unwrap().clear();
        Ok(())
    }
}

impl Default for WasmMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let store = WasmMemoryStore::new();

        // Set and get
        store.set("key1", b"value1".to_vec()).unwrap();
        assert_eq!(store.get("key1").unwrap(), Some(b"value1".to_vec()));

        // Has
        assert!(store.has("key1").unwrap());
        assert!(!store.has("nonexistent").unwrap());

        // Delete
        store.delete("key1").unwrap();
        assert!(!store.has("key1").unwrap());
    }

    #[test]
    fn test_prefix_keys() {
        let store = WasmMemoryStore::new();

        store.set("users:1", b"alice".to_vec()).unwrap();
        store.set("users:2", b"bob".to_vec()).unwrap();
        store.set("posts:1", b"hello".to_vec()).unwrap();

        let user_keys = store.keys_with_prefix("users:").unwrap();
        assert_eq!(user_keys.len(), 2);
        assert!(user_keys.contains(&"users:1".to_string()));
        assert!(user_keys.contains(&"users:2".to_string()));
    }
}
