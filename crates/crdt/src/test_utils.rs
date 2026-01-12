//! Test utilities for CRDT testing
//!
//! Provides shared test infrastructure including an in-memory store implementation.

use async_trait::async_trait;
use defra_core::{store::Store, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// In-memory store for testing
/// Duplicated per module to keep tests self-contained and avoid cross-crate test dependencies
pub struct MemoryStore {
    data: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.lock().await.get(key).cloned())
    }

    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.data.lock().await.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        self.data.lock().await.remove(key);
        Ok(())
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        Ok(self.data.lock().await.contains_key(key))
    }
}
