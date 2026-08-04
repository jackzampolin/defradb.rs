/// Peerstore - Peer and replication metadata
///
/// The Peerstore handles storage of replicator configuration, replication
/// retry tracking, and search engine retry tracking for P2P operations.
use crate::corekv::{IterOptions, Key, Reader, Result, Store, Txn, Writer};
use crate::keys::peerstore::{
    ReplicatorKey, ReplicatorRetryCommitKey, ReplicatorRetryDocIDKey, ReplicatorRetryIDKey,
};
use crate::namespace::{Namespace, NamespacedStore};
use async_trait::async_trait;
use cid::Cid;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use tracing;

const PUSH_RETRY_TXN_MAX_ATTEMPTS: usize = 4;

type RetryPeerLock = tokio::sync::RwLock<()>;

fn retry_peer_lock(peer_id: &str) -> Arc<RetryPeerLock> {
    static LOCKS: OnceLock<Mutex<HashMap<String, Weak<RetryPeerLock>>>> = OnceLock::new();

    let mut locks = LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(lock) = locks.get(peer_id).and_then(Weak::upgrade) {
        return lock;
    }

    let lock = Arc::new(RetryPeerLock::new(()));
    locks.insert(peer_id.to_string(), Arc::downgrade(&lock));
    lock
}

/// Keeps a retry pass or failure-recording operation coordinated with forget.
pub struct ReplicatorRetryGuard {
    _guard: tokio::sync::OwnedRwLockReadGuard<()>,
}

async fn retry_push_txn_conflicts<T, F, Fut>(mut operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    for attempt in 1..=PUSH_RETRY_TXN_MAX_ATTEMPTS {
        match operation().await {
            Err(error) if error.is_retriable() && attempt < PUSH_RETRY_TXN_MAX_ATTEMPTS => {
                tracing::debug!(attempt, "retrying push-ledger transaction conflict");
            }
            result => return result,
        }
    }
    unreachable!("bounded transaction retry loop always returns")
}

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

    /// Acquire permission to process retry state while the replicator exists.
    ///
    /// The guard must be retained through transport replay and any resulting
    /// ledger update. Forget takes the corresponding write lock, so it cannot
    /// return while a selected retry can still be sent or persisted.
    pub async fn acquire_replicator_retry_guard(
        &self,
        peer_id: &str,
    ) -> Result<Option<ReplicatorRetryGuard>> {
        let guard = retry_peer_lock(peer_id).read_owned().await;
        if self.has_replicator(peer_id).await? {
            Ok(Some(ReplicatorRetryGuard { _guard: guard }))
        } else {
            Ok(None)
        }
    }

    /// Delete a replicator and all of its persisted push-retry state.
    pub async fn delete_replicator(&self, peer_id: &str) -> Result<()> {
        let _retry_guard = retry_peer_lock(peer_id).write_owned().await;
        retry_push_txn_conflicts(|| self.delete_replicator_once(peer_id)).await
    }

    async fn delete_replicator_once(&self, peer_id: &str) -> Result<()> {
        let mut txn = self.store.new_txn(false).await?;
        txn.delete(&ReplicatorKey::new(peer_id).bytes()).await?;
        Self::delete_retry_state(txn.as_mut(), peer_id).await?;
        txn.commit().await
    }

    async fn delete_retry_state(txn: &mut dyn Txn, peer_id: &str) -> Result<()> {
        let mut keys = Vec::new();
        for prefix in [
            ReplicatorRetryDocIDKey::peer_prefix(peer_id),
            ReplicatorRetryCommitKey::peer_prefix(peer_id),
        ] {
            let mut iter = txn.iterator(IterOptions::new().with_prefix(prefix)).await?;
            while let Some(pair) = iter.next().await? {
                keys.push(pair.key);
            }
        }

        for key in keys {
            txn.delete(&key).await?;
        }
        txn.delete(&ReplicatorRetryIDKey::new(peer_id).bytes())
            .await
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
        // Collection commits are doc-less, CID-scoped obligations. They are
        // recorded under their own keyspace so they can be replayed by CID;
        // dropping them here made a failed collection-commit push permanent,
        // leaving receivers with heads whose parents never arrive
        // (defradb#1113, source-inc/gents#696).
        if doc_id.is_empty() {
            if cid.is_empty() {
                // A versionless doc-less failure (SE artifact) has nothing to
                // replay: no document to re-resolve and no CID to re-send.
                return Ok(());
            }
            return retry_push_txn_conflicts(|| {
                self.record_commit_push_failure_once(
                    peer_id,
                    collection_id,
                    cid,
                    priority,
                    retry_info_bytes,
                )
            })
            .await;
        }
        retry_push_txn_conflicts(|| {
            self.record_push_failure_once(
                peer_id,
                doc_id,
                collection_id,
                cid,
                priority,
                retry_info_bytes,
            )
        })
        .await
    }

    /// Record a failed collection-commit push under `/rep/retry/commit/{peer}/
    /// {collection}/{cid}`.
    ///
    /// One record per CID: collection-commit DAGs chain, so a newer commit does
    /// not make an older undelivered one redundant. A record for this exact CID
    /// that is already pending keeps its backoff (repeat failures must not reset
    /// the ladder); a dormant one is activated.
    async fn record_commit_push_failure_once(
        &self,
        peer_id: &str,
        collection_id: &str,
        cid: &str,
        priority: u64,
        retry_info_bytes: &[u8],
    ) -> Result<()> {
        let mut txn = self.store.new_txn(false).await?;
        let id_key = ReplicatorRetryIDKey::new(peer_id);
        if !txn.has(&id_key.bytes()).await? {
            txn.set(&id_key.bytes(), retry_info_bytes).await?;
        }
        let commit_key = ReplicatorRetryCommitKey::new(peer_id, collection_id, cid);
        let existing = txn.get(&commit_key.bytes()).await?;
        let current = existing
            .as_deref()
            .and_then(|bytes| super::PersistedPushRetry::from_bytes(bytes).ok());
        let retry = match current {
            Some(retry) if retry.pending => retry,
            Some(mut retry) => {
                retry.activate(&format!("{peer_id}:{cid}"));
                retry
            }
            None => {
                let mut retry =
                    super::PersistedPushRetry::new_observed_commit(collection_id, cid, priority);
                retry.activate(&format!("{peer_id}:{cid}"));
                retry
            }
        };
        let bytes = retry.to_bytes().map_err(crate::corekv::Error::Other)?;
        txn.set(&commit_key.bytes(), &bytes).await?;
        txn.commit().await
    }

    async fn record_push_failure_once(
        &self,
        peer_id: &str,
        doc_id: &str,
        collection_id: &str,
        cid: &str,
        priority: u64,
        retry_info_bytes: &[u8],
    ) -> Result<()> {
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
        let retry = match current {
            // SE-artifact failures are versionless (`cid == ""`). They must
            // activate the current dormant head instead of comparing as an
            // older priority-0 document failure and disappearing. The retry
            // pass re-reads current document heads before regenerating SE
            // artifacts, so retaining the watermark's version is correct.
            Some(mut retry) if cid.is_empty() => {
                if !retry.pending {
                    retry.activate(&format!("{peer_id}:{}", retry.cid));
                }
                retry
            }
            Some(retry)
                if compare_push_versions(retry.priority, &retry.cid, priority, cid).is_gt() =>
            {
                retry
            }
            Some(retry) if retry.cid == cid && retry.pending => retry,
            Some(mut retry) if retry.cid == cid => {
                retry.activate(&format!("{peer_id}:{cid}"));
                retry
            }
            _ => {
                let mut retry =
                    super::PersistedPushRetry::new_observed(doc_id, collection_id, cid, priority);
                retry.activate(&format!("{peer_id}:{cid}"));
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
            if cid.is_empty() {
                return Ok(());
            }
            return retry_push_txn_conflicts(|| {
                self.observe_commit_push_head_once(peer_id, collection_id, cid, priority)
            })
            .await;
        }
        retry_push_txn_conflicts(|| {
            self.observe_push_head_once(peer_id, doc_id, collection_id, cid, priority)
        })
        .await
    }

    /// Dormant watermark for an in-flight collection-commit push, so a restart
    /// can promote it if the send never completed.
    async fn observe_commit_push_head_once(
        &self,
        peer_id: &str,
        collection_id: &str,
        cid: &str,
        priority: u64,
    ) -> Result<()> {
        let key = ReplicatorRetryCommitKey::new(peer_id, collection_id, cid);
        let mut txn = self.store.new_txn(false).await?;
        if txn.has(&key.bytes()).await? {
            // An existing record (pending or dormant) already tracks this exact
            // CID; observing it again must not reset its backoff.
            return txn.commit().await;
        }
        let retry = super::PersistedPushRetry::new_observed_commit(collection_id, cid, priority);
        let bytes = retry.to_bytes().map_err(crate::corekv::Error::Other)?;
        txn.set(&key.bytes(), &bytes).await?;
        txn.commit().await
    }

    async fn observe_push_head_once(
        &self,
        peer_id: &str,
        doc_id: &str,
        collection_id: &str,
        cid: &str,
        priority: u64,
    ) -> Result<()> {
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

    /// The store key a retry record lives under, derived from its scope so
    /// document heads and collection commits round-trip to the right keyspace.
    fn retry_key_bytes(peer_id: &str, retry: &super::PersistedPushRetry) -> Vec<u8> {
        if retry.is_collection_commit() {
            ReplicatorRetryCommitKey::new(peer_id, &retry.collection_id, &retry.cid).bytes()
        } else {
            ReplicatorRetryDocIDKey::new(peer_id, &retry.doc_id).bytes()
        }
    }

    /// Get each pending independently scheduled retry for a peer. Dormant
    /// newest-head watermarks are omitted. Legacy raw collection-ID values
    /// inherit the old peer-level schedule and are rewritten on their next
    /// failure/update. Retries are returned in next-attempt order so bounded
    /// consumers cannot starve later store keys.
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
                    scope: super::RetryScope::Document,
                    retry_info: fallback.clone(),
                }
            });
            if retry.pending {
                results.push(retry);
            }
        }

        // Collection-commit obligations live in their own keyspace and are
        // replayed by CID (defradb#1113).
        let commit_prefix = ReplicatorRetryCommitKey::peer_prefix(peer_id);
        let mut commit_iter = txn
            .iterator(IterOptions::new().with_prefix(commit_prefix))
            .await?;
        while let Some(pair) = commit_iter.next().await? {
            let Ok(retry) = super::PersistedPushRetry::from_bytes(&pair.value) else {
                continue;
            };
            if retry.pending && retry.is_collection_commit() {
                results.push(retry);
            }
        }
        results.sort_by_key(|retry| retry.retry_info.next_retry_unix);
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
            let Some((peer_id, key_doc_id)) = key
                .strip_prefix(expected_prefix)
                .and_then(|suffix| suffix.split_once('/'))
            else {
                continue;
            };
            let Ok(retry) = super::PersistedPushRetry::from_bytes(&pair.value) else {
                continue;
            };
            if !retry.pending {
                dormant.push((peer_id.to_string(), key_doc_id.to_string(), retry));
            }
        }
        drop(iter);

        // Collection-commit watermarks live under /rep/retry/commit/{peer}/
        // {collection}/{cid} — a 3-segment key, so the doc-shaped `split_once`
        // above cannot parse them. Their peer is taken from the record's own
        // key prefix (defradb#1113).
        let mut commit_iter = txn
            .iterator(
                IterOptions::new().with_prefix(ReplicatorRetryCommitKey::retry_commit_prefix()),
            )
            .await?;
        while let Some(pair) = commit_iter.next().await? {
            let key = String::from_utf8_lossy(&pair.key);
            let Some(peer_id) = key
                .strip_prefix("/rep/retry/commit/")
                .and_then(|suffix| suffix.split_once('/'))
                .map(|(peer_id, _)| peer_id)
            else {
                continue;
            };
            let Ok(retry) = super::PersistedPushRetry::from_bytes(&pair.value) else {
                continue;
            };
            if !retry.pending && retry.is_collection_commit() {
                dormant.push((peer_id.to_string(), retry.doc_id.clone(), retry));
            }
        }
        drop(commit_iter);
        drop(txn);

        let mut activated = 0;
        for (peer_id, key_doc_id, retry) in dormant {
            let Some(_retry_guard) = self.acquire_replicator_retry_guard(&peer_id).await? else {
                continue;
            };
            match retry_push_txn_conflicts(|| {
                self.activate_dormant_push_retry_once(&peer_id, &key_doc_id, &retry)
            })
            .await
            {
                Ok(true) => activated += 1,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(
                        peer_id,
                        doc_id = key_doc_id,
                        %error,
                        "failed to activate dormant push retry; continuing startup scan"
                    );
                }
            }
        }
        Ok(activated)
    }

    async fn activate_dormant_push_retry_once(
        &self,
        peer_id: &str,
        key_doc_id: &str,
        retry: &super::PersistedPushRetry,
    ) -> Result<bool> {
        let key_bytes = if retry.is_collection_commit() {
            ReplicatorRetryCommitKey::new(peer_id, &retry.collection_id, &retry.cid).bytes()
        } else {
            ReplicatorRetryDocIDKey::new(peer_id, key_doc_id).bytes()
        };
        let mut txn = self.store.new_txn(false).await?;
        let current_bytes = txn.get(&key_bytes).await?;
        let current = current_bytes
            .as_deref()
            .and_then(|bytes| super::PersistedPushRetry::from_bytes(bytes).ok());
        let is_same_dormant = current.as_ref().is_some_and(|current| {
            !current.pending && current.priority == retry.priority && current.cid == retry.cid
        });
        if is_same_dormant {
            let mut activated_retry = retry.clone();
            activated_retry.pending = true;
            activated_retry.retry_info = super::RetryInfo::new_initial();
            let bytes = activated_retry
                .to_bytes()
                .map_err(crate::corekv::Error::Other)?;
            txn.set(&key_bytes, &bytes).await?;
            let id_key = ReplicatorRetryIDKey::new(peer_id);
            if !txn.has(&id_key.bytes()).await? {
                let info = super::RetryInfo::new_initial()
                    .to_bytes()
                    .map_err(crate::corekv::Error::Other)?;
                txn.set(&id_key.bytes(), &info).await?;
            }
        }
        txn.commit().await?;
        Ok(is_same_dormant)
    }

    pub async fn update_retry_document(
        &self,
        peer_id: &str,
        retry: &super::PersistedPushRetry,
    ) -> Result<()> {
        retry_push_txn_conflicts(|| self.update_retry_document_once(peer_id, retry)).await
    }

    async fn update_retry_document_once(
        &self,
        peer_id: &str,
        retry: &super::PersistedPushRetry,
    ) -> Result<()> {
        let key_bytes = Self::retry_key_bytes(peer_id, retry);
        let mut txn = self.store.new_txn(false).await?;
        let current = txn.get(&key_bytes).await?;
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
        txn.set(&key_bytes, &bytes).await?;
        txn.commit().await
    }

    /// Complete a retry only if the stored version still matches the attempt.
    /// A concurrent newer-head observation must survive an old attempt's ack.
    pub async fn complete_retry_document(
        &self,
        peer_id: &str,
        retry: &super::PersistedPushRetry,
    ) -> Result<()> {
        retry_push_txn_conflicts(|| self.complete_retry_document_once(peer_id, retry)).await
    }

    async fn complete_retry_document_once(
        &self,
        peer_id: &str,
        retry: &super::PersistedPushRetry,
    ) -> Result<()> {
        let key_bytes = Self::retry_key_bytes(peer_id, retry);
        let mut txn = self.store.new_txn(false).await?;
        let current = txn.get(&key_bytes).await?;
        let current_retry = current
            .as_deref()
            .and_then(|bytes| super::PersistedPushRetry::from_bytes(bytes).ok());
        let is_current =
            current_retry.as_ref().is_some_and(|current| {
                current.pending && current.priority == retry.priority && current.cid == retry.cid
            }) || (current.is_some() && current_retry.is_none() && retry.cid.is_empty());
        if is_current {
            txn.delete(&key_bytes).await?;
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
        self.get_retry_peers(false).await
    }

    /// Get retry peers that still have a persisted replicator.
    pub async fn get_replicator_retry_peers(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.get_retry_peers(true).await
    }

    async fn get_retry_peers(&self, require_replicator: bool) -> Result<Vec<(String, Vec<u8>)>> {
        let prefix = ReplicatorRetryIDKey::retry_prefix();
        let txn = self.store.new_txn(true).await?;
        let opts = IterOptions::new().with_prefix(prefix);
        let mut iter = txn.iterator(opts).await?;

        let mut results = Vec::new();
        while let Some(pair) = iter.next().await? {
            let key_str = String::from_utf8_lossy(&pair.key);
            if let Some(peer_id) = key_str.strip_prefix("/rep/retry/id/") {
                if !peer_id.is_empty()
                    && (!require_replicator
                        || txn.has(&ReplicatorKey::new(peer_id).bytes()).await?)
                {
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

    /// Stop sweeping a peer once no retry is pending. Dormant newest-head
    /// watermarks deliberately survive: they cover volatile live work and
    /// are promoted by `activate_dormant_push_retries` after a restart.
    pub async fn clear_retry_peer(&self, peer_id: &str) -> Result<()> {
        retry_push_txn_conflicts(|| self.clear_retry_peer_once(peer_id)).await
    }

    async fn clear_retry_peer_once(&self, peer_id: &str) -> Result<()> {
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

        // A pending collection-commit obligation keeps the peer swept, exactly
        // like a pending document head: dropping the marker here would strand
        // the commit and re-open defradb#1113 from the other end.
        if !has_pending {
            let commit_prefix = ReplicatorRetryCommitKey::peer_prefix(peer_id);
            let mut commit_iter = txn
                .iterator(IterOptions::new().with_prefix(commit_prefix))
                .await?;
            while let Some(pair) = commit_iter.next().await? {
                match super::PersistedPushRetry::from_bytes(&pair.value) {
                    Ok(retry) if !retry.pending => {}
                    _ => {
                        has_pending = true;
                        break;
                    }
                }
            }
            drop(commit_iter);
        }

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
#[path = "peerstore_tests.rs"]
mod tests;
