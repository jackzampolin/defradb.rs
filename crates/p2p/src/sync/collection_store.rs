//! Persistent storage for P2P collection subscriptions.
//!
//! Mirrors Go DefraDB's systemstore `/p2p/collection/` key structure
//! for storing which collections are subscribed for P2P sync.

use crate::error::{Error, Result};
use async_trait::async_trait;
use std::sync::Arc;
use storage::corekv::{Iterator, IterOptions, Key, Reader, Store};

/// Marker byte for collection subscription (matches Go's marker = byte(0xff))
const COLLECTION_MARKER: u8 = 0xff;

/// Prefix for P2P collection keys
const P2P_COLLECTION_PREFIX: &[u8] = b"/p2p/collection/";

/// Key for P2P collection subscriptions.
///
/// Structure: /p2p/collection/{collectionID}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct P2PCollectionKey {
    pub collection_id: String,
}

impl P2PCollectionKey {
    /// Create a new P2PCollectionKey
    pub fn new(collection_id: impl Into<String>) -> Self {
        Self {
            collection_id: collection_id.into(),
        }
    }

    /// Get the prefix for all P2P collection keys
    pub fn prefix() -> &'static [u8] {
        P2P_COLLECTION_PREFIX
    }

    /// Parse a collection ID from a key
    pub fn parse_collection_id(key: &[u8]) -> Option<String> {
        if key.starts_with(P2P_COLLECTION_PREFIX) {
            let id_bytes = &key[P2P_COLLECTION_PREFIX.len()..];
            String::from_utf8(id_bytes.to_vec()).ok()
        } else {
            None
        }
    }
}

impl Key for P2PCollectionKey {
    fn bytes(&self) -> Vec<u8> {
        let mut key = P2P_COLLECTION_PREFIX.to_vec();
        key.extend(self.collection_id.as_bytes());
        key
    }

    fn to_string(&self) -> String {
        format!("/p2p/collection/{}", self.collection_id)
    }
}

/// Trait for P2P collection storage operations.
#[async_trait]
pub trait P2PCollectionStorage: Send + Sync {
    /// Add a collection subscription to persistent storage.
    async fn add_collection(&self, collection_id: &str) -> Result<()>;

    /// Remove a collection subscription from persistent storage.
    async fn remove_collection(&self, collection_id: &str) -> Result<()>;

    /// Get all subscribed collection IDs from persistent storage.
    async fn get_all_collections(&self) -> Result<Vec<String>>;

    /// Check if a collection is subscribed.
    async fn is_subscribed(&self, collection_id: &str) -> Result<bool>;
}

/// Implementation of P2P collection storage backed by a key-value store.
pub struct P2PCollectionStore<S: Store> {
    store: Arc<S>,
}

impl<S: Store> P2PCollectionStore<S> {
    /// Create a new P2P collection store.
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S: Store + 'static> P2PCollectionStorage for P2PCollectionStore<S> {
    async fn add_collection(&self, collection_id: &str) -> Result<()> {
        let key = P2PCollectionKey::new(collection_id);
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        txn.set(&key.bytes(), &[COLLECTION_MARKER])
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        txn.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn remove_collection(&self, collection_id: &str) -> Result<()> {
        let key = P2PCollectionKey::new(collection_id);
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        txn.delete(&key.bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        txn.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn get_all_collections(&self) -> Result<Vec<String>> {
        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut collections = Vec::new();

        // Iterate over all keys with the P2P collection prefix
        let opts = IterOptions::new().with_prefix(P2P_COLLECTION_PREFIX.to_vec());
        let mut iter = txn
            .iterator(opts)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            if let Some(collection_id) = P2PCollectionKey::parse_collection_id(&kv.key) {
                collections.push(collection_id);
            }
        }

        iter.close()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(collections)
    }

    async fn is_subscribed(&self, collection_id: &str) -> Result<bool> {
        let key = P2PCollectionKey::new(collection_id);
        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let value = txn
            .get(&key.bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(value.is_some())
    }
}

/// No-op implementation for when persistent storage is not available.
pub struct NoOpCollectionStorage;

#[async_trait]
impl P2PCollectionStorage for NoOpCollectionStorage {
    async fn add_collection(&self, _collection_id: &str) -> Result<()> {
        Ok(())
    }

    async fn remove_collection(&self, _collection_id: &str) -> Result<()> {
        Ok(())
    }

    async fn get_all_collections(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn is_subscribed(&self, _collection_id: &str) -> Result<bool> {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_p2p_collection_key() {
        let key = P2PCollectionKey::new("users");
        assert_eq!(key.bytes(), b"/p2p/collection/users");
        assert_eq!(key.to_string(), "/p2p/collection/users");
    }

    #[test]
    fn test_parse_collection_id() {
        let id = P2PCollectionKey::parse_collection_id(b"/p2p/collection/users");
        assert_eq!(id, Some("users".to_string()));

        let id = P2PCollectionKey::parse_collection_id(b"/other/key");
        assert_eq!(id, None);
    }
}
