/// Peerstore - Peer and replication metadata
///
/// The Peerstore handles storage of replicator configuration, replication
/// retry tracking, and search engine retry tracking for P2P operations.
use crate::corekv::{IterOptions, Key, Reader, Result, Store, Txn, Writer};
use crate::keys::peerstore::{ReplicatorKey, ReplicatorRetryDocIDKey, ReplicatorRetryIDKey};
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
    pub async fn create_replicator(&self, peer_id: &str, data: &[u8]) -> Result<()> {
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
    pub async fn list_replicators(&self) -> Result<Vec<(String, Vec<u8>)>> {
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

    /// Persist a list of P2P collection subscriptions as JSON.
    pub async fn persist_collections(&self, collections: &[String]) -> Result<()> {
        let data = serde_json::to_vec(collections).map_err(|e| {
            crate::corekv::Error::Other(format!("failed to serialize P2P collections: {}", e))
        })?;
        self.set_p2p_collections(&data).await
    }

    /// Load the persisted P2P collection subscription list.
    pub async fn load_collections(&self) -> Result<Vec<String>> {
        match self.get_p2p_collections().await? {
            Some(data) => serde_json::from_slice(&data).map_err(|e| {
                crate::corekv::Error::Other(format!("failed to deserialize P2P collections: {}", e))
            }),
            None => Ok(Vec::new()),
        }
    }

    /// Persist a list of P2P document subscriptions as JSON.
    pub async fn persist_documents(&self, documents: &[String]) -> Result<()> {
        let data = serde_json::to_vec(documents).map_err(|e| {
            crate::corekv::Error::Other(format!("failed to serialize P2P documents: {}", e))
        })?;
        self.set_p2p_documents(&data).await
    }

    /// Load the persisted P2P document subscription list.
    pub async fn load_documents(&self) -> Result<Vec<String>> {
        match self.get_p2p_documents().await? {
            Some(data) => serde_json::from_slice(&data).map_err(|e| {
                crate::corekv::Error::Other(format!("failed to deserialize P2P documents: {}", e))
            }),
            None => Ok(Vec::new()),
        }
    }

    /// Record a push failure for a specific peer/doc pair.
    ///
    /// Writes the retry info at `/rep/retry/id/{peer}` and the collection_id
    /// at `/rep/retry/doc/{peer}/{doc}`.
    pub async fn record_push_failure(
        &self,
        peer_id: &str,
        doc_id: &str,
        collection_id: &str,
        retry_info_bytes: &[u8],
    ) -> Result<()> {
        let mut txn = self.store.new_txn(false).await?;
        let id_key = ReplicatorRetryIDKey::new(peer_id);
        // Only write retry info if not already present (preserve existing backoff state).
        if !txn.has(&id_key.bytes()).await? {
            txn.set(&id_key.bytes(), retry_info_bytes).await?;
        }
        let doc_key = ReplicatorRetryDocIDKey::new(peer_id, doc_id);
        txn.set(&doc_key.bytes(), collection_id.as_bytes()).await?;
        txn.commit().await
    }

    /// Get retry info bytes for a peer.
    pub async fn get_retry_info(&self, peer_id: &str) -> Result<Option<Vec<u8>>> {
        let txn = self.store.new_txn(true).await?;
        let key = ReplicatorRetryIDKey::new(peer_id);
        txn.get(&key.bytes()).await
    }

    /// Get all peers that have pending retries.
    ///
    /// Returns `(peer_id, retry_info_bytes)` pairs.
    pub async fn get_all_retry_peers(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let prefix = ReplicatorRetryIDKey::retry_prefix();
        let txn = self.store.new_txn(true).await?;
        let opts = IterOptions::new().with_prefix(prefix);
        let mut iter = txn.iterator(opts).await?;

        let mut results = Vec::new();
        while let Some(pair) = iter.next().await? {
            let key_str = String::from_utf8_lossy(&pair.key);
            if let Some(peer_id) = key_str.strip_prefix("/rep/retry/id/") {
                if !peer_id.is_empty() {
                    results.push((peer_id.to_string(), pair.value));
                }
            }
        }
        Ok(results)
    }

    /// Get all doc IDs pending retry for a specific peer.
    ///
    /// Returns `(doc_id, collection_id)` pairs.
    pub async fn get_retry_doc_ids(&self, peer_id: &str) -> Result<Vec<(String, String)>> {
        let prefix = ReplicatorRetryDocIDKey::peer_prefix(peer_id);
        let txn = self.store.new_txn(true).await?;
        let opts = IterOptions::new().with_prefix(prefix);
        let mut iter = txn.iterator(opts).await?;

        let expected_prefix = format!("/rep/retry/doc/{}/", peer_id);
        let mut results = Vec::new();
        while let Some(pair) = iter.next().await? {
            let key_str = String::from_utf8_lossy(&pair.key);
            if let Some(doc_id) = key_str.strip_prefix(&expected_prefix) {
                if !doc_id.is_empty() {
                    let collection_id = String::from_utf8_lossy(&pair.value).to_string();
                    results.push((doc_id.to_string(), collection_id));
                }
            }
        }
        Ok(results)
    }

    /// Remove a single doc retry entry for a peer.
    pub async fn remove_retry_doc(&self, peer_id: &str, doc_id: &str) -> Result<()> {
        let key = ReplicatorRetryDocIDKey::new(peer_id, doc_id);
        let mut txn = self.store.new_txn(false).await?;
        txn.delete(&key.bytes()).await?;
        txn.commit().await
    }

    /// Clear retry info for a peer (called when all docs succeed).
    pub async fn clear_retry_peer(&self, peer_id: &str) -> Result<()> {
        let key = ReplicatorRetryIDKey::new(peer_id);
        let mut txn = self.store.new_txn(false).await?;
        txn.delete(&key.bytes()).await?;
        txn.commit().await
    }

    /// Update the retry info for a peer (after a failed retry attempt).
    pub async fn update_retry_info(&self, peer_id: &str, bytes: &[u8]) -> Result<()> {
        let key = ReplicatorRetryIDKey::new(peer_id);
        let mut txn = self.store.new_txn(false).await?;
        txn.set(&key.bytes(), bytes).await?;
        txn.commit().await
    }
}

impl<S: Store> crate::corekv::private::Sealed for Peerstore<S> {}

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
        peerstore.create_replicator(peer_id, data).await.unwrap();

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
        peerstore.create_replicator(peer_id, data).await.unwrap();
        assert!(peerstore.has_replicator(peer_id).await.unwrap());

        // Delete
        peerstore.delete_replicator(peer_id).await.unwrap();
        assert!(!peerstore.has_replicator(peer_id).await.unwrap());

        // Get returns None
        let result = peerstore.get_replicator(peer_id).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_list_replicators() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);

        // Add multiple replicators
        peerstore
            .create_replicator("peer1", b"config1")
            .await
            .unwrap();
        peerstore
            .create_replicator("peer2", b"config2")
            .await
            .unwrap();
        peerstore
            .create_replicator("peer3", b"config3")
            .await
            .unwrap();

        // Get all
        let all = peerstore.list_replicators().await.unwrap();
        assert_eq!(all.len(), 3);

        // Check they're all present (order may vary)
        let peer_ids: Vec<&str> = all.iter().map(|(id, _)| id.as_str()).collect();
        assert!(peer_ids.contains(&"peer1"));
        assert!(peer_ids.contains(&"peer2"));
        assert!(peer_ids.contains(&"peer3"));
    }

    #[tokio::test]
    async fn test_list_replicators_empty() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);

        let all = peerstore.list_replicators().await.unwrap();
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn test_update_replicator() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);

        let peer_id = "QmTestPeer123";

        // Set initial
        peerstore
            .create_replicator(peer_id, b"config_v1")
            .await
            .unwrap();
        let result = peerstore.get_replicator(peer_id).await.unwrap();
        assert_eq!(result, Some(b"config_v1".to_vec()));

        // Update
        peerstore
            .create_replicator(peer_id, b"config_v2")
            .await
            .unwrap();
        let result = peerstore.get_replicator(peer_id).await.unwrap();
        assert_eq!(result, Some(b"config_v2".to_vec()));

        // Still only one replicator
        let all = peerstore.list_replicators().await.unwrap();
        assert_eq!(all.len(), 1);
    }
}
