//! Persistent storage for P2P collection subscriptions.
//!
//! Mirrors Go DefraDB's systemstore `/p2p/collection/` key structure
//! for storing which collections are subscribed for P2P sync.

use crate::error::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use storage::corekv::{IterOptions, Key, Reader, Store};
use storage::keys::systemstore::P2PCollectionKey;
use storage::stores::Systemstore;

/// Marker byte for collection subscription (matches Go's marker = byte(0xff))
const COLLECTION_MARKER: u8 = 0xff;

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
    systemstore: Systemstore<S>,
}

impl<S: Store> P2PCollectionStore<S> {
    /// Create a new P2P collection store.
    pub fn new(store: Arc<S>) -> Self {
        Self {
            systemstore: Systemstore::new(store),
        }
    }
}

#[async_trait]
impl<S: Store + 'static> P2PCollectionStorage for P2PCollectionStore<S> {
    async fn add_collection(&self, collection_id: &str) -> Result<()> {
        let key = P2PCollectionKey::new(collection_id);
        let mut txn = self
            .systemstore
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
            .systemstore
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
            .systemstore
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut collections = Vec::new();

        // Iterate over all keys with the P2P collection prefix
        let opts = IterOptions::new().with_prefix(P2PCollectionKey::p2p_collection_prefix());
        let mut iter = txn
            .iterator(opts)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            if let Some(collection_id) = parse_collection_id(&kv.key) {
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
            .systemstore
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

fn parse_collection_id(key: &[u8]) -> Option<String> {
    let prefix = P2PCollectionKey::p2p_collection_prefix();
    key.strip_prefix(prefix.as_slice())
        .and_then(|id_bytes| String::from_utf8(id_bytes.to_vec()).ok())
        .filter(|id| !id.is_empty())
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
        let id = parse_collection_id(b"/p2p/collection/users");
        assert_eq!(id, Some("users".to_string()));

        let id = parse_collection_id(b"/other/key");
        assert_eq!(id, None);
    }

    #[tokio::test]
    async fn stores_collection_state_in_systemstore_namespace() {
        use storage::RegolithStore;

        let store = Arc::new(RegolithStore::in_memory().unwrap());
        let p2p_store = P2PCollectionStore::new(store.clone());

        p2p_store.add_collection("users").await.unwrap();

        let systemstore = Systemstore::new(store.clone());
        let system_txn = systemstore.new_txn(true).await.unwrap();
        assert_eq!(
            system_txn
                .get(&P2PCollectionKey::new("users").bytes())
                .await
                .unwrap(),
            Some(Bytes::from(vec![COLLECTION_MARKER]))
        );

        let root_txn = store.new_txn(true).await.unwrap();
        assert_eq!(
            root_txn
                .get(&P2PCollectionKey::new("users").bytes())
                .await
                .unwrap(),
            None,
            "Go-compatible P2P collection state must live in Systemstore, not the raw root store"
        );
    }

    #[tokio::test]
    async fn loads_collection_state_written_like_go_systemstore() {
        use storage::RegolithStore;

        let store = Arc::new(RegolithStore::in_memory().unwrap());
        let systemstore = Systemstore::new(store.clone());
        let mut system_txn = systemstore.new_txn(false).await.unwrap();
        system_txn
            .set(
                &P2PCollectionKey::new("users").bytes(),
                &[COLLECTION_MARKER],
            )
            .await
            .unwrap();
        system_txn.commit().await.unwrap();

        let p2p_store = P2PCollectionStore::new(store);

        assert!(p2p_store.is_subscribed("users").await.unwrap());
        assert_eq!(
            p2p_store.get_all_collections().await.unwrap(),
            vec!["users".to_string()]
        );
    }
}
