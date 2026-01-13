//! Test utilities for CRDT testing
//!
//! Provides shared test infrastructure including:
//! - MemoryStore: Basic in-memory store
//! - FailingStore: Configurable store that fails on specific operations
//! - OperationCountingStore: Store that tracks operation counts

use async_trait::async_trait;
use defra_core::{store::Store, Error, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
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

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
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

/// Configuration for which operations should fail
#[derive(Debug, Clone, Default)]
pub struct FailureConfig {
    /// Fail get operations after this many successful calls
    pub fail_get_after: Option<usize>,
    /// Fail set operations after this many successful calls
    pub fail_set_after: Option<usize>,
    /// Fail delete operations after this many successful calls
    pub fail_delete_after: Option<usize>,
    /// Fail has operations after this many successful calls
    pub fail_has_after: Option<usize>,
    /// Only fail on keys matching this prefix
    pub fail_key_prefix: Option<Vec<u8>>,
}

/// Store that can be configured to fail on specific operations
/// Used for testing crash recovery and error handling
pub struct FailingStore {
    inner: MemoryStore,
    config: Arc<Mutex<FailureConfig>>,
    get_count: AtomicUsize,
    set_count: AtomicUsize,
    delete_count: AtomicUsize,
    has_count: AtomicUsize,
}

impl FailingStore {
    pub fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            config: Arc::new(Mutex::new(FailureConfig::default())),
            get_count: AtomicUsize::new(0),
            set_count: AtomicUsize::new(0),
            delete_count: AtomicUsize::new(0),
            has_count: AtomicUsize::new(0),
        }
    }

    pub fn with_config(config: FailureConfig) -> Self {
        Self {
            inner: MemoryStore::new(),
            config: Arc::new(Mutex::new(config)),
            get_count: AtomicUsize::new(0),
            set_count: AtomicUsize::new(0),
            delete_count: AtomicUsize::new(0),
            has_count: AtomicUsize::new(0),
        }
    }

    /// Update the failure configuration
    pub async fn set_config(&self, config: FailureConfig) {
        *self.config.lock().await = config;
    }

    /// Reset all operation counts
    pub fn reset_counts(&self) {
        self.get_count.store(0, Ordering::SeqCst);
        self.set_count.store(0, Ordering::SeqCst);
        self.delete_count.store(0, Ordering::SeqCst);
        self.has_count.store(0, Ordering::SeqCst);
    }

    /// Get the number of set operations performed
    pub fn set_count(&self) -> usize {
        self.set_count.load(Ordering::SeqCst)
    }

    /// Get the number of get operations performed
    pub fn get_count(&self) -> usize {
        self.get_count.load(Ordering::SeqCst)
    }

    fn should_fail_for_key(&self, key: &[u8], prefix: &Option<Vec<u8>>) -> bool {
        match prefix {
            Some(p) => key.starts_with(p),
            None => true,
        }
    }
}

impl Default for FailingStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Store for FailingStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let count = self.get_count.fetch_add(1, Ordering::SeqCst);
        let config = self.config.lock().await;

        if let Some(fail_after) = config.fail_get_after {
            if count >= fail_after && self.should_fail_for_key(key, &config.fail_key_prefix) {
                return Err(Error::Storage("simulated get failure".into()));
            }
        }
        drop(config);

        self.inner.get(key).await
    }

    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let count = self.set_count.fetch_add(1, Ordering::SeqCst);
        let config = self.config.lock().await;

        if let Some(fail_after) = config.fail_set_after {
            if count >= fail_after && self.should_fail_for_key(key, &config.fail_key_prefix) {
                return Err(Error::Storage("simulated set failure".into()));
            }
        }
        drop(config);

        self.inner.set(key, value).await
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        let count = self.delete_count.fetch_add(1, Ordering::SeqCst);
        let config = self.config.lock().await;

        if let Some(fail_after) = config.fail_delete_after {
            if count >= fail_after && self.should_fail_for_key(key, &config.fail_key_prefix) {
                return Err(Error::Storage("simulated delete failure".into()));
            }
        }
        drop(config);

        self.inner.delete(key).await
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        let count = self.has_count.fetch_add(1, Ordering::SeqCst);
        let config = self.config.lock().await;

        if let Some(fail_after) = config.fail_has_after {
            if count >= fail_after && self.should_fail_for_key(key, &config.fail_key_prefix) {
                return Err(Error::Storage("simulated has failure".into()));
            }
        }
        drop(config);

        self.inner.has(key).await
    }
}

/// Store that counts operations for verification
pub struct OperationCountingStore {
    inner: MemoryStore,
    get_count: AtomicUsize,
    set_count: AtomicUsize,
    delete_count: AtomicUsize,
    has_count: AtomicUsize,
}

impl OperationCountingStore {
    pub fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            get_count: AtomicUsize::new(0),
            set_count: AtomicUsize::new(0),
            delete_count: AtomicUsize::new(0),
            has_count: AtomicUsize::new(0),
        }
    }

    pub fn get_count(&self) -> usize {
        self.get_count.load(Ordering::SeqCst)
    }

    pub fn set_count(&self) -> usize {
        self.set_count.load(Ordering::SeqCst)
    }

    pub fn delete_count(&self) -> usize {
        self.delete_count.load(Ordering::SeqCst)
    }

    pub fn has_count(&self) -> usize {
        self.has_count.load(Ordering::SeqCst)
    }

    pub fn total_operations(&self) -> usize {
        self.get_count() + self.set_count() + self.delete_count() + self.has_count()
    }
}

impl Default for OperationCountingStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Store for OperationCountingStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_count.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key).await
    }

    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.set_count.fetch_add(1, Ordering::SeqCst);
        self.inner.set(key, value).await
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_count.fetch_add(1, Ordering::SeqCst);
        self.inner.delete(key).await
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        self.has_count.fetch_add(1, Ordering::SeqCst);
        self.inner.has(key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_failing_store_set_failure() {
        let store = FailingStore::with_config(FailureConfig {
            fail_set_after: Some(1),
            ..Default::default()
        });

        // First set should succeed
        store.set(b"key1", b"value1").await.unwrap();

        // Second set should fail
        let result = store.set(b"key2", b"value2").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("simulated"));
    }

    #[tokio::test]
    async fn test_failing_store_key_prefix_filter() {
        let store = FailingStore::with_config(FailureConfig {
            fail_set_after: Some(0),
            fail_key_prefix: Some(b"/nonces/".to_vec()),
            ..Default::default()
        });

        // Set to non-matching key should succeed
        store.set(b"/data/value", b"value1").await.unwrap();

        // Set to matching key should fail
        let result = store.set(b"/nonces/123", b"marker").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_operation_counting_store() {
        let store = OperationCountingStore::new();

        store.set(b"key1", b"value1").await.unwrap();
        store.set(b"key2", b"value2").await.unwrap();
        store.get(b"key1").await.unwrap();
        store.has(b"key3").await.unwrap();

        assert_eq!(store.set_count(), 2);
        assert_eq!(store.get_count(), 1);
        assert_eq!(store.has_count(), 1);
        assert_eq!(store.total_operations(), 4);
    }
}
