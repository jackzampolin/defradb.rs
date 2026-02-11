/// Peerstore - Peer and replication metadata
///
/// The Peerstore handles storage of replicator configuration, replication
/// retry tracking, and search engine retry tracking for P2P operations.
use crate::corekv::{IterOptions, Key, Reader, Result, Store, Txn, Writer};
use crate::keys::peerstore::ReplicatorKey;
use crate::namespace::{Namespace, NamespacedStore};
use async_trait::async_trait;
use std::sync::Arc;
use tracing;

/// Peerstore provides storage for peer and replication metadata
pub struct Peerstore<S: Store> {
    store: NamespacedStore<S>,
}

impl<S: Store> Peerstore<S> {
    /// Create a new Peerstore
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store: NamespacedStore::new(store, Namespace::Peerstore),
        }
    }

    /// Store a replicator's configuration.
    ///
    /// The peer_id is used as the key, and the data is stored as raw bytes.
    /// The caller is responsible for serialization (typically CBOR).
    pub async fn set_replicator(&self, peer_id: &str, data: &[u8]) -> Result<()> {
        let key = ReplicatorKey::new(peer_id);
        let mut txn = self.store.new_txn(false).await?;
        txn.set(&key.bytes(), data).await?;
        txn.commit().await
    }

    /// Get a replicator's configuration by peer ID.
    ///
    /// Returns None if the replicator doesn't exist.
    pub async fn get_replicator(&self, peer_id: &str) -> Result<Option<Vec<u8>>> {
        let key = ReplicatorKey::new(peer_id);
        let txn = self.store.new_txn(true).await?;
        txn.get(&key.bytes()).await
    }

    /// Delete a replicator's configuration.
    pub async fn delete_replicator(&self, peer_id: &str) -> Result<()> {
        let key = ReplicatorKey::new(peer_id);
        let mut txn = self.store.new_txn(false).await?;
        txn.delete(&key.bytes()).await?;
        txn.commit().await
    }

    /// Get all stored replicator configurations.
    ///
    /// Returns a list of (peer_id, data) pairs. Keys that don't match the
    /// expected format are logged and skipped.
    pub async fn get_all_replicators(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let prefix = ReplicatorKey::replicator_prefix();
        let txn = self.store.new_txn(true).await?;
        let opts = IterOptions::new().with_prefix(prefix);
        let mut iter = txn.iterator(opts).await?;

        let mut results = Vec::new();
        while let Some(pair) = iter.next().await? {
            // Parse the key using ReplicatorKey::from_bytes for safe extraction
            if let Some(key) = ReplicatorKey::from_bytes(&pair.key) {
                results.push((key.replicator_id().to_string(), pair.value));
            } else {
                let key_str = String::from_utf8_lossy(&pair.key);
                tracing::warn!(
                    key = %key_str,
                    "Skipping replicator entry with unexpected key format"
                );
            }
        }

        Ok(results)
    }

    /// Check if a replicator exists.
    pub async fn has_replicator(&self, peer_id: &str) -> Result<bool> {
        let key = ReplicatorKey::new(peer_id);
        let txn = self.store.new_txn(true).await?;
        txn.has(&key.bytes()).await
    }

    /// Store P2P collection subscriptions (persists across restarts).
    pub async fn set_p2p_collections(&self, data: &[u8]) -> Result<()> {
        let mut txn = self.store.new_txn(false).await?;
        txn.set(b"/p2p/collections", data).await?;
        txn.commit().await
    }

    /// Load stored P2P collection subscriptions.
    pub async fn get_p2p_collections(&self) -> Result<Option<Vec<u8>>> {
        let txn = self.store.new_txn(true).await?;
        txn.get(b"/p2p/collections").await
    }

    /// Store P2P document subscriptions (persists across restarts).
    pub async fn set_p2p_documents(&self, data: &[u8]) -> Result<()> {
        let mut txn = self.store.new_txn(false).await?;
        txn.set(b"/p2p/documents", data).await?;
        txn.commit().await
    }

    /// Load stored P2P document subscriptions.
    pub async fn get_p2p_documents(&self) -> Result<Option<Vec<u8>>> {
        let txn = self.store.new_txn(true).await?;
        txn.get(b"/p2p/documents").await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store> Store for Peerstore<S> {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        self.store.new_txn(readonly).await
    }

    async fn close(&self) -> Result<()> {
        self.store.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::MemoryStore;
    use crate::corekv::Key;
    use crate::keys::peerstore::ReplicatorKey;

    #[tokio::test]
    async fn test_peerstore_basic() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);

        let key = ReplicatorKey::new("replicator_1");

        // Write
        let mut txn = peerstore.new_txn(false).await.unwrap();
        txn.set(&key.bytes(), b"replicator_config").await.unwrap();
        txn.commit().await.unwrap();

        // Read
        let txn = peerstore.new_txn(true).await.unwrap();
        let value = txn.get(&key.bytes()).await.unwrap();
        assert_eq!(value, Some(b"replicator_config".to_vec()));
    }

    #[tokio::test]
    async fn test_set_get_replicator() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);

        let peer_id = "QmTestPeer123";
        let data = b"replicator_config_data";

        // Set
        peerstore.set_replicator(peer_id, data).await.unwrap();

        // Get
        let result = peerstore.get_replicator(peer_id).await.unwrap();
        assert_eq!(result, Some(data.to_vec()));

        // Has
        assert!(peerstore.has_replicator(peer_id).await.unwrap());
        assert!(!peerstore.has_replicator("nonexistent").await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_replicator() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);

        let peer_id = "QmTestPeer123";
        let data = b"replicator_config_data";

        // Set
        peerstore.set_replicator(peer_id, data).await.unwrap();
        assert!(peerstore.has_replicator(peer_id).await.unwrap());

        // Delete
        peerstore.delete_replicator(peer_id).await.unwrap();
        assert!(!peerstore.has_replicator(peer_id).await.unwrap());

        // Get returns None
        let result = peerstore.get_replicator(peer_id).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_get_all_replicators() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);

        // Add multiple replicators
        peerstore.set_replicator("peer1", b"config1").await.unwrap();
        peerstore.set_replicator("peer2", b"config2").await.unwrap();
        peerstore.set_replicator("peer3", b"config3").await.unwrap();

        // Get all
        let all = peerstore.get_all_replicators().await.unwrap();
        assert_eq!(all.len(), 3);

        // Check they're all present (order may vary)
        let peer_ids: Vec<&str> = all.iter().map(|(id, _)| id.as_str()).collect();
        assert!(peer_ids.contains(&"peer1"));
        assert!(peer_ids.contains(&"peer2"));
        assert!(peer_ids.contains(&"peer3"));
    }

    #[tokio::test]
    async fn test_get_all_replicators_empty() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);

        let all = peerstore.get_all_replicators().await.unwrap();
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn test_update_replicator() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);

        let peer_id = "QmTestPeer123";

        // Set initial
        peerstore
            .set_replicator(peer_id, b"config_v1")
            .await
            .unwrap();
        let result = peerstore.get_replicator(peer_id).await.unwrap();
        assert_eq!(result, Some(b"config_v1".to_vec()));

        // Update
        peerstore
            .set_replicator(peer_id, b"config_v2")
            .await
            .unwrap();
        let result = peerstore.get_replicator(peer_id).await.unwrap();
        assert_eq!(result, Some(b"config_v2".to_vec()));

        // Still only one replicator
        let all = peerstore.get_all_replicators().await.unwrap();
        assert_eq!(all.len(), 1);
    }
}
