//! Storage backends for the WASM client.
//!
//! Supports:
//! - Memory: In-memory storage (data lost on page refresh)
//! - IndexedDB: Persistent browser storage

mod indexeddb;
mod memory;

pub use indexeddb::WasmIndexedDbStore;
pub use memory::WasmMemoryStore;

use crate::bindings::StorageType;
use crate::error::Result;

/// Create a storage backend based on the configuration.
pub async fn create_store(storage_type: StorageType, db_name: Option<&str>) -> Result<WasmStore> {
    match storage_type {
        StorageType::Memory => Ok(WasmStore::Memory(WasmMemoryStore::new())),
        StorageType::IndexedDb => {
            let name = db_name.unwrap_or("defradb");
            let store = WasmIndexedDbStore::open(name).await?;
            Ok(WasmStore::IndexedDb(store))
        }
    }
}

/// Unified storage type for the WASM client.
pub enum WasmStore {
    Memory(WasmMemoryStore),
    IndexedDb(WasmIndexedDbStore),
}

impl WasmStore {
    /// Check if the store is closed.
    pub async fn is_closed(&self) -> bool {
        match self {
            WasmStore::Memory(store) => store.is_closed().await,
            WasmStore::IndexedDb(store) => store.is_closed(),
        }
    }

    /// Close the store.
    pub async fn close(&self) -> Result<()> {
        match self {
            WasmStore::Memory(store) => store.close().await,
            WasmStore::IndexedDb(store) => store.close().await,
        }
    }

    /// Get a value by key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self {
            WasmStore::Memory(store) => {
                // Memory store uses string keys
                let key_str = String::from_utf8_lossy(key);
                store.get(&key_str)
            }
            WasmStore::IndexedDb(store) => store.get(key).await,
        }
    }

    /// Set a value for a key.
    pub async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        match self {
            WasmStore::Memory(store) => {
                let key_str = String::from_utf8_lossy(key);
                store.set(&key_str, value.to_vec())
            }
            WasmStore::IndexedDb(store) => store.set(key, value).await,
        }
    }

    /// Delete a key.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        match self {
            WasmStore::Memory(store) => {
                let key_str = String::from_utf8_lossy(key);
                store.delete(&key_str)
            }
            WasmStore::IndexedDb(store) => store.delete(key).await,
        }
    }

    /// Check if a key exists.
    pub async fn has(&self, key: &[u8]) -> Result<bool> {
        match self {
            WasmStore::Memory(store) => {
                let key_str = String::from_utf8_lossy(key);
                store.has(&key_str)
            }
            WasmStore::IndexedDb(store) => store.has(key).await,
        }
    }

    /// Get all keys with a given prefix.
    pub async fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        match self {
            WasmStore::Memory(store) => {
                let prefix_str = String::from_utf8_lossy(prefix);
                let keys = store.keys_with_prefix(&prefix_str)?;
                Ok(keys.into_iter().map(|k| k.into_bytes()).collect())
            }
            WasmStore::IndexedDb(store) => store.keys_with_prefix(prefix).await,
        }
    }

    /// Clear all data in the store.
    pub async fn clear(&self) -> Result<()> {
        match self {
            WasmStore::Memory(store) => store.clear(),
            WasmStore::IndexedDb(store) => store.clear().await,
        }
    }

    /// Get the underlying memory store if this is a memory store.
    pub fn as_memory(&self) -> Option<&WasmMemoryStore> {
        match self {
            WasmStore::Memory(store) => Some(store),
            WasmStore::IndexedDb(_) => None,
        }
    }

    /// Get the underlying IndexedDB store if this is an IndexedDB store.
    pub fn as_indexeddb(&self) -> Option<&WasmIndexedDbStore> {
        match self {
            WasmStore::Memory(_) => None,
            WasmStore::IndexedDb(store) => Some(store),
        }
    }
}
