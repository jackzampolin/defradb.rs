//! LevelDB storage backend for WASM client.
//!
//! Wraps the storage crate's LevelDbStore to provide a simple key-value interface.
//! Currently uses rusty-leveldb's in-memory environment; OPFS persistence planned.

use crate::error::{Result, WasmError};
use storage::corekv::{Dropable, Iterator as CoreKvIterator, IterOptions};
use storage::{LevelDbStore, Reader, Store, Writer};

/// LevelDB-backed storage for the WASM client.
///
/// This provides a simple key-value interface on top of the full corekv::Store
/// implementation from the storage crate.
pub struct WasmLevelDbStore {
    store: LevelDbStore,
    closed: bool,
}

impl WasmLevelDbStore {
    /// Open or create a LevelDB database.
    ///
    /// The path is used as a namespace identifier. In WASM without OPFS,
    /// this currently uses an in-memory environment.
    pub fn open(name: &str) -> Result<Self> {
        let store = LevelDbStore::open(name).map_err(|e| WasmError::Storage(e.to_string()))?;
        Ok(Self {
            store,
            closed: false,
        })
    }

    /// Check if the store is closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Close the store.
    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.store
            .close()
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;
        self.closed = true;
        Ok(())
    }

    /// Get a value by key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.closed {
            return Err(WasmError::Closed);
        }

        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;

        let result = txn
            .get(key)
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;

        txn.discard();
        Ok(result)
    }

    /// Set a value for a key.
    pub async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.closed {
            return Err(WasmError::Closed);
        }

        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;

        txn.set(key, value)
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;

        txn.commit()
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;

        Ok(())
    }

    /// Delete a key.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        if self.closed {
            return Err(WasmError::Closed);
        }

        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;

        txn.delete(key)
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;

        txn.commit()
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;

        Ok(())
    }

    /// Check if a key exists.
    pub async fn has(&self, key: &[u8]) -> Result<bool> {
        if self.closed {
            return Err(WasmError::Closed);
        }

        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;

        let result = txn
            .has(key)
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;

        txn.discard();
        Ok(result)
    }

    /// Get all keys with a given prefix.
    pub async fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        if self.closed {
            return Err(WasmError::Closed);
        }

        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;

        let iter_options = IterOptions::new().with_prefix(prefix.to_vec());

        let mut iter = txn
            .iterator(iter_options)
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;

        let mut keys = Vec::new();
        while let Ok(Some(kv)) = iter.next().await {
            keys.push(kv.key);
        }

        txn.discard();
        Ok(keys)
    }

    /// Clear all data in the store.
    pub async fn clear(&self) -> Result<()> {
        if self.closed {
            return Err(WasmError::Closed);
        }

        self.store
            .drop_all()
            .await
            .map_err(|e| WasmError::Storage(e.to_string()))?;

        Ok(())
    }
}
