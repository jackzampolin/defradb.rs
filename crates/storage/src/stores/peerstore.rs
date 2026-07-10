/// Peerstore - Peer and replication metadata
///
/// The Peerstore handles storage of replicator configuration, replication
/// retry tracking, and search engine retry tracking for P2P operations.
use crate::corekv::{IterOptions, Key, Reader, Result, Store, Txn, Writer};
use crate::keys::peerstore::{ReplicatorKey, ReplicatorRetryDocIDKey, ReplicatorRetryIDKey};
use crate::namespace::{Namespace, NamespacedStore};
use async_trait::async_trait;
use cid::Cid;
use std::cmp::Ordering;
use std::sync::Arc;
use tracing;

fn compare_push_versions(
    left_priority: u64,
    left_cid: &str,
    right_priority: u64,
    right_cid: &str,
) -> Ordering {
    left_priority.cmp(&right_priority).then_with(|| {
        match (Cid::try_from(left_cid), Cid::try_from(right_cid)) {
            (Ok(left), Ok(right)) => left.cmp(&right),
            // Legacy or corrupt values still need a deterministic order.
            _ => left_cid.as_bytes().cmp(right_cid.as_bytes()),
        }
    })
}

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
    /// Writes the peer marker at `/rep/retry/id/{peer}` and a versioned,
    /// independently scheduled record at `/rep/retry/doc/{peer}/{doc}`.
    pub async fn record_push_failure(
        &self,
        peer_id: &str,
        doc_id: &str,
        collection_id: &str,
        cid: &str,
        priority: u64,
        retry_info_bytes: &[u8],
    ) -> Result<()> {
        // Collection commits are CID-scoped live obligations. The document
        // retry loop cannot service an empty document ID, so persisting one
        // would create a permanent peer marker with no runnable work.
        if doc_id.is_empty() {
            return Ok(());
        }
        let mut txn = self.store.new_txn(false).await?;
        let id_key = ReplicatorRetryIDKey::new(peer_id);
        // Only write retry info if not already present (preserve existing backoff state).
        if !txn.has(&id_key.bytes()).await? {
            txn.set(&id_key.bytes(), retry_info_bytes).await?;
        }
        let doc_key = ReplicatorRetryDocIDKey::new(peer_id, doc_id);
        let existing = txn.get(&doc_key.bytes()).await?;
        let current = existing
            .as_deref()
            .and_then(|bytes| super::PersistedPushRetry::from_bytes(bytes).ok());
        let retry_key = format!("{peer_id}:{cid}");
        let retry = match current {
            Some(retry)
                if compare_push_versions(retry.priority, &retry.cid, priority, cid).is_gt() =>
            {
                retry
            }
            Some(retry) if retry.cid == cid && retry.pending => retry,
            Some(mut retry) if retry.cid == cid => {
                retry.activate(&retry_key);
                retry
            }
            _ => {
                let mut retry =
                    super::PersistedPushRetry::new_observed(doc_id, collection_id, cid, priority);
                retry.activate(&retry_key);
                retry
            }
        };
        let bytes = retry.to_bytes().map_err(crate::corekv::Error::Other)?;
        txn.set(&doc_key.bytes(), &bytes).await?;
        txn.commit().await
    }

    /// Replace an existing persisted retry with a dormant newer-head watermark
    /// without creating state for healthy `(document, peer)` pairs. A later
    /// failure of this exact head activates its independently jittered retry.
    pub async fn observe_push_head(
        &self,
        peer_id: &str,
        doc_id: &str,
        collection_id: &str,
        cid: &str,
        priority: u64,
    ) -> Result<()> {
        if doc_id.is_empty() {
            return Ok(());
        }
        let key = ReplicatorRetryDocIDKey::new(peer_id, doc_id);
        let mut txn = self.store.new_txn(false).await?;
        let Some(bytes) = txn.get(&key.bytes()).await? else {
            return txn.commit().await;
        };
        let current = super::PersistedPushRetry::from_bytes(&bytes).ok();
        if let Some(retry) = current.as_ref() {
            if compare_push_versions(retry.priority, &retry.cid, priority, cid).is_gt()
                || (retry.cid == cid && retry.pending)
            {
                return txn.commit().await;
            }
        }
        let retry = super::PersistedPushRetry::new_observed(doc_id, collection_id, cid, priority);
        let bytes = retry.to_bytes().map_err(crate::corekv::Error::Other)?;
        txn.set(&key.bytes(), &bytes).await?;
        txn.commit().await
    }

    /// Get each pending independently scheduled retry for a peer. Dormant
    /// newest-head watermarks are omitted. Legacy raw collection-ID values
    /// inherit the old peer-level schedule and are rewritten on their next
    /// failure/update.
    pub async fn get_retry_documents(
        &self,
        peer_id: &str,
    ) -> Result<Vec<super::PersistedPushRetry>> {
        let fallback = self
            .get_retry_info(peer_id)
            .await?
            .and_then(|bytes| super::RetryInfo::from_bytes(&bytes).ok())
            .unwrap_or_else(super::RetryInfo::new_initial);
        let prefix = ReplicatorRetryDocIDKey::peer_prefix(peer_id);
        let txn = self.store.new_txn(true).await?;
        let opts = IterOptions::new().with_prefix(prefix);
        let mut iter = txn.iterator(opts).await?;
        let expected_prefix = format!("/rep/retry/doc/{peer_id}/");
        let mut results = Vec::new();
        while let Some(pair) = iter.next().await? {
            let key = String::from_utf8_lossy(&pair.key);
            let Some(doc_id) = key.strip_prefix(&expected_prefix) else {
                continue;
            };
            if doc_id.is_empty() {
                continue;
            }
            let retry = super::PersistedPushRetry::from_bytes(&pair.value).unwrap_or_else(|_| {
                super::PersistedPushRetry {
                    doc_id: doc_id.to_string(),
                    collection_id: String::from_utf8_lossy(&pair.value).to_string(),
                    cid: String::new(),
                    priority: 0,
                    pending: true,
                    retry_info: fallback.clone(),
                }
            });
            if retry.pending {
                results.push(retry);
            }
        }
        Ok(results)
    }

    /// Promote dormant live-send watermarks after process startup. A dormant
    /// record means the previous process was betting on volatile in-memory
    /// work; after a restart that work no longer exists, so the exact newest
    /// head must become an immediately due durable retry.
    pub async fn activate_dormant_push_retries(&self) -> Result<usize> {
        let txn = self.store.new_txn(true).await?;
        let mut iter = txn
            .iterator(IterOptions::new().with_prefix(ReplicatorRetryDocIDKey::retry_doc_prefix()))
            .await?;
        let expected_prefix = "/rep/retry/doc/";
        let mut dormant = Vec::new();
        while let Some(pair) = iter.next().await? {
            let key = String::from_utf8_lossy(&pair.key);
            let Some((peer_id, _)) = key
                .strip_prefix(expected_prefix)
                .and_then(|suffix| suffix.split_once('/'))
            else {
                continue;
            };
            let Ok(retry) = super::PersistedPushRetry::from_bytes(&pair.value) else {
                continue;
            };
            if !retry.pending {
                dormant.push((peer_id.to_string(), retry));
            }
        }
        drop(iter);
        drop(txn);

        let mut activated = 0;
        for (peer_id, mut retry) in dormant {
            let key = ReplicatorRetryDocIDKey::new(&peer_id, &retry.doc_id);
            let mut txn = self.store.new_txn(false).await?;
            let current_bytes = txn.get(&key.bytes()).await?;
            let current = current_bytes
                .as_deref()
                .and_then(|bytes| super::PersistedPushRetry::from_bytes(bytes).ok());
            let is_same_dormant = current.as_ref().is_some_and(|current| {
                !current.pending && current.priority == retry.priority && current.cid == retry.cid
            });
            if is_same_dormant {
                retry.pending = true;
                retry.retry_info = super::RetryInfo::new_initial();
                let bytes = retry.to_bytes().map_err(crate::corekv::Error::Other)?;
                txn.set(&key.bytes(), &bytes).await?;
                let id_key = ReplicatorRetryIDKey::new(&peer_id);
                if !txn.has(&id_key.bytes()).await? {
                    let info = super::RetryInfo::new_initial()
                        .to_bytes()
                        .map_err(crate::corekv::Error::Other)?;
                    txn.set(&id_key.bytes(), &info).await?;
                }
                activated += 1;
            }
            txn.commit().await?;
        }
        Ok(activated)
    }

    pub async fn update_retry_document(
        &self,
        peer_id: &str,
        retry: &super::PersistedPushRetry,
    ) -> Result<()> {
        let key = ReplicatorRetryDocIDKey::new(peer_id, &retry.doc_id);
        let mut txn = self.store.new_txn(false).await?;
        let current = txn.get(&key.bytes()).await?;
        let current_retry = current
            .as_deref()
            .and_then(|bytes| super::PersistedPushRetry::from_bytes(bytes).ok());
        let is_current =
            current_retry.as_ref().is_some_and(|current| {
                current.pending && current.priority == retry.priority && current.cid == retry.cid
            }) || (current.is_some() && current_retry.is_none() && retry.cid.is_empty());
        if !is_current {
            return txn.commit().await;
        }
        let bytes = retry.to_bytes().map_err(crate::corekv::Error::Other)?;
        txn.set(&key.bytes(), &bytes).await?;
        txn.commit().await
    }

    /// Complete a retry only if the stored version still matches the attempt.
    /// A concurrent newer-head observation must survive an old attempt's ack.
    pub async fn complete_retry_document(
        &self,
        peer_id: &str,
        retry: &super::PersistedPushRetry,
    ) -> Result<()> {
        let key = ReplicatorRetryDocIDKey::new(peer_id, &retry.doc_id);
        let mut txn = self.store.new_txn(false).await?;
        let current = txn.get(&key.bytes()).await?;
        let current_retry = current
            .as_deref()
            .and_then(|bytes| super::PersistedPushRetry::from_bytes(bytes).ok());
        let is_current =
            current_retry.as_ref().is_some_and(|current| {
                current.pending && current.priority == retry.priority && current.cid == retry.cid
            }) || (current.is_some() && current_retry.is_none() && retry.cid.is_empty());
        if is_current {
            txn.delete(&key.bytes()).await?;
        }
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
        Ok(self
            .get_retry_documents(peer_id)
            .await?
            .into_iter()
            .map(|retry| (retry.doc_id, retry.collection_id))
            .collect())
    }

    /// Remove a single doc retry entry for a peer.
    pub async fn remove_retry_doc(&self, peer_id: &str, doc_id: &str) -> Result<()> {
        let key = ReplicatorRetryDocIDKey::new(peer_id, doc_id);
        let mut txn = self.store.new_txn(false).await?;
        txn.delete(&key.bytes()).await?;
        txn.commit().await
    }

    /// Stop sweeping a peer once no retry is pending. Dormant newest-head
    /// watermarks deliberately survive: they cover volatile live work and
    /// are promoted by `activate_dormant_push_retries` after a restart.
    pub async fn clear_retry_peer(&self, peer_id: &str) -> Result<()> {
        let mut txn = self.store.new_txn(false).await?;
        let prefix = ReplicatorRetryDocIDKey::peer_prefix(peer_id);
        let mut iter = txn.iterator(IterOptions::new().with_prefix(prefix)).await?;
        let expected_prefix = format!("/rep/retry/doc/{peer_id}/");
        let mut empty_doc_keys = Vec::new();
        let mut has_pending = false;
        while let Some(pair) = iter.next().await? {
            let key = String::from_utf8_lossy(&pair.key);
            if key
                .strip_prefix(&expected_prefix)
                .is_some_and(str::is_empty)
            {
                // Clean up the unserviceable shape produced by earlier
                // revisions without letting it wedge this peer forever.
                empty_doc_keys.push(pair.key);
                continue;
            }
            match super::PersistedPushRetry::from_bytes(&pair.value) {
                Ok(retry) if !retry.pending => {}
                // A pending or legacy record raced the caller's empty check;
                // preserve it and its peer marker for the retry loop.
                _ => {
                    has_pending = true;
                    break;
                }
            }
        }
        drop(iter);
        if has_pending {
            return txn.commit().await;
        }
        for doc_key in empty_doc_keys {
            txn.delete(&doc_key).await?;
        }
        txn.delete(&ReplicatorRetryIDKey::new(peer_id).bytes())
            .await?;
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

    #[test]
    fn push_version_tie_break_uses_cid_bytes_not_base32_text() {
        let cids: Vec<_> = (0_u8..=255)
            .map(|seed| {
                let digest = [seed; 32];
                let hash = cid::multihash::Multihash::<64>::wrap(0x12, &digest).unwrap();
                Cid::new_v1(0x55, hash)
            })
            .collect();
        let (left, right) = cids
            .iter()
            .flat_map(|left| cids.iter().map(move |right| (left, right)))
            .find(|(left, right)| left.cmp(right) != left.to_string().cmp(&right.to_string()))
            .expect("test corpus must contain a base32/CID ordering disagreement");

        assert_eq!(
            compare_push_versions(1, &left.to_string(), 1, &right.to_string()),
            left.cmp(right)
        );
    }

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

    #[tokio::test]
    async fn retry_record_keeps_only_newest_cid_and_its_own_backoff() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);
        let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

        peerstore
            .record_push_failure("peer", "doc", "collection", "cid-1", 1, &initial)
            .await
            .unwrap();
        let mut retry = peerstore
            .get_retry_documents("peer")
            .await
            .unwrap()
            .remove(0);
        retry.retry_info.bump();
        peerstore
            .update_retry_document("peer", &retry)
            .await
            .unwrap();

        peerstore
            .observe_push_head("peer", "doc", "collection", "cid-2", 2)
            .await
            .unwrap();
        assert!(peerstore
            .get_retry_documents("peer")
            .await
            .unwrap()
            .is_empty());
        peerstore
            .record_push_failure("peer", "doc", "collection", "cid-1", 1, &initial)
            .await
            .unwrap();
        assert!(peerstore
            .get_retry_documents("peer")
            .await
            .unwrap()
            .is_empty());
        peerstore
            .record_push_failure("peer", "doc", "collection", "cid-2", 2, &initial)
            .await
            .unwrap();

        let retries = peerstore.get_retry_documents("peer").await.unwrap();
        assert_eq!(retries.len(), 1);
        assert_eq!(retries[0].cid, "cid-2");
        assert_eq!(retries[0].priority, 2);
        assert_eq!(retries[0].retry_info.num_retries, 1);

        let stale_attempt = retries[0].clone();
        peerstore
            .observe_push_head("peer", "doc", "collection", "cid-3", 3)
            .await
            .unwrap();
        peerstore
            .complete_retry_document("peer", &stale_attempt)
            .await
            .unwrap();
        peerstore
            .update_retry_document("peer", &stale_attempt)
            .await
            .unwrap();
        peerstore
            .record_push_failure("peer", "doc", "collection", "cid-2", 2, &initial)
            .await
            .unwrap();
        assert!(peerstore
            .get_retry_documents("peer")
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn observing_an_equal_pending_head_does_not_deactivate_its_retry() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);
        let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

        peerstore
            .record_push_failure("peer", "doc", "collection", "cid", 1, &initial)
            .await
            .unwrap();
        peerstore
            .observe_push_head("peer", "doc", "collection", "cid", 1)
            .await
            .unwrap();

        let retries = peerstore.get_retry_documents("peer").await.unwrap();
        assert_eq!(retries.len(), 1);
        assert_eq!(retries[0].cid, "cid");
        assert!(retries[0].pending);
    }

    #[tokio::test]
    async fn sweep_clear_preserves_dormant_watermark_for_restart_promotion() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);
        let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

        peerstore
            .record_push_failure("peer", "doc", "collection", "old", 1, &initial)
            .await
            .unwrap();
        peerstore
            .observe_push_head("peer", "doc", "collection", "new", 2)
            .await
            .unwrap();
        assert!(peerstore
            .get_retry_documents("peer")
            .await
            .unwrap()
            .is_empty());

        // The live sweep stops revisiting this peer but must preserve the
        // dormant crash-recovery obligation.
        peerstore.clear_retry_peer("peer").await.unwrap();
        assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
        assert_eq!(peerstore.activate_dormant_push_retries().await.unwrap(), 1);
        let retries = peerstore.get_retry_documents("peer").await.unwrap();
        assert_eq!(retries.len(), 1);
        assert_eq!(retries[0].cid, "new");
        assert!(retries[0].retry_info.is_due());

        // A clear racing pending work must preserve it.
        peerstore.clear_retry_peer("peer").await.unwrap();
        assert_eq!(
            peerstore.get_retry_documents("peer").await.unwrap().len(),
            1
        );
        peerstore
            .complete_retry_document("peer", &retries[0])
            .await
            .unwrap();
        peerstore.clear_retry_peer("peer").await.unwrap();
        assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
        assert_eq!(peerstore.activate_dormant_push_retries().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn empty_document_push_failure_creates_no_retry_state() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);
        let initial = super::super::RetryInfo::new_initial().to_bytes().unwrap();

        peerstore
            .record_push_failure("peer", "", "collection", "collection-cid", 1, &initial)
            .await
            .unwrap();
        peerstore
            .observe_push_head("peer", "", "collection", "collection-cid", 1)
            .await
            .unwrap();

        assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
        assert!(peerstore
            .get_retry_documents("peer")
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn sweep_clear_removes_preexisting_empty_document_retry() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);
        let mut retry =
            super::super::PersistedPushRetry::new_observed("", "collection", "collection-cid", 1);
        retry.activate("peer:collection-cid");
        let mut txn = peerstore.store.new_txn(false).await.unwrap();
        txn.set(
            &ReplicatorRetryIDKey::new("peer").bytes(),
            &super::super::RetryInfo::new_initial().to_bytes().unwrap(),
        )
        .await
        .unwrap();
        txn.set(
            &ReplicatorRetryDocIDKey::new("peer", "").bytes(),
            &retry.to_bytes().unwrap(),
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();

        peerstore.clear_retry_peer("peer").await.unwrap();

        assert!(peerstore.get_all_retry_peers().await.unwrap().is_empty());
        let txn = peerstore.store.new_txn(true).await.unwrap();
        assert!(txn
            .get(&ReplicatorRetryDocIDKey::new("peer", "").bytes())
            .await
            .unwrap()
            .is_none());
    }
}
