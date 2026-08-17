/// Peerstore - Peer and replication metadata
///
/// The Peerstore handles storage of replicator configuration, replication
/// retry tracking, and search engine retry tracking for P2P operations.
use crate::corekv::{IterOptions, Key, Reader, Result, Store, Txn, Writer};
use crate::keys::peerstore::{
    ReplicatorKey, ReplicatorRetryCollectionKey, ReplicatorRetryCommitKey, ReplicatorRetryDocIDKey,
    ReplicatorRetryIDKey,
};
use crate::namespace::{Namespace, NamespacedStore};
use async_lock::{RwLock, RwLockWriteGuardArc};
use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use tracing;

const PUSH_RETRY_TXN_MAX_ATTEMPTS: usize = 4;

type RetryPeerLock = RwLock<()>;

fn retry_peer_lock(peer_id: &str) -> Arc<RetryPeerLock> {
    static LOCKS: OnceLock<Mutex<HashMap<String, Weak<RetryPeerLock>>>> = OnceLock::new();

    let mut locks = LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    locks.retain(|_, lock| lock.upgrade().is_some());
    if let Some(lock) = locks.get(peer_id).and_then(Weak::upgrade) {
        return lock;
    }

    let lock = Arc::new(RetryPeerLock::new(()));
    locks.insert(peer_id.to_string(), Arc::downgrade(&lock));
    lock
}

/// Keeps a retry pass or failure-recording operation coordinated with forget.
pub struct ReplicatorRetryGuard {
    _guard: RwLockWriteGuardArc<()>,
}

async fn retry_push_txn_conflicts<T, F, Fut>(mut operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut retried = false;
    for attempt in 1..=PUSH_RETRY_TXN_MAX_ATTEMPTS {
        match operation().await {
            Err(error) if error.is_retriable() && attempt < PUSH_RETRY_TXN_MAX_ATTEMPTS => {
                telemetry::record_retry_attempt(telemetry::RetryLayer::PushMarker);
                retried = true;
                tracing::debug!(attempt, "retrying push-ledger transaction conflict");
            }
            Err(error) if error.is_retriable() => {
                telemetry::record_retry_exhaustion(telemetry::RetryLayer::PushMarker);
                return Err(error);
            }
            Ok(value) => {
                if retried {
                    telemetry::record_retry_success(telemetry::RetryLayer::PushMarker);
                }
                return Ok(value);
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("bounded transaction retry loop always returns")
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
    /// marker update. It serializes each peer's live registration, retry/ack,
    /// and forget transitions so an old ack cannot race a newer dirty marker.
    pub async fn acquire_replicator_retry_guard(
        &self,
        peer_id: &str,
    ) -> Result<Option<ReplicatorRetryGuard>> {
        let guard = retry_peer_lock(peer_id).write_arc().await;
        if self.has_replicator(peer_id).await? {
            Ok(Some(ReplicatorRetryGuard { _guard: guard }))
        } else {
            Ok(None)
        }
    }

    /// Delete a replicator and all of its persisted push-retry state.
    pub async fn delete_replicator(&self, peer_id: &str) -> Result<()> {
        let _retry_guard = retry_peer_lock(peer_id).write_arc().await;
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
            ReplicatorRetryCollectionKey::peer_prefix(peer_id),
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

    async fn register_scope_marker(
        &self,
        peer_id: &str,
        doc_id: &str,
        collection_id: &str,
        retry_info_bytes: &[u8],
    ) -> Result<()> {
        retry_push_txn_conflicts(|| async {
            let mut txn = self.store.new_txn(false).await?;
            let id_key = ReplicatorRetryIDKey::new(peer_id);
            if !txn.has(&id_key.bytes()).await? {
                let mut info = super::RetryInfo::from_bytes(retry_info_bytes)
                    .unwrap_or_else(|_| super::RetryInfo::new_initial());
                info.bump_for(peer_id);
                txn.set(
                    &id_key.bytes(),
                    &info.to_bytes().map_err(crate::corekv::Error::Other)?,
                )
                .await?;
            }
            let marker = if doc_id.is_empty() {
                if collection_id.is_empty() {
                    return txn.commit().await;
                }
                ReplicatorRetryCollectionKey::new(peer_id, collection_id).bytes()
            } else {
                ReplicatorRetryDocIDKey::new(peer_id, doc_id).bytes()
            };
            txn.set(&marker, &[]).await?;
            txn.commit().await
        })
        .await
    }

    /// One-way conversion of payload-valued document records and CID-scoped
    /// collection commits into presence-only scope markers.
    async fn migrate_push_retry_markers(&self, only_peer: Option<&str>) -> Result<usize> {
        retry_push_txn_conflicts(|| async {
            let mut txn = self.store.new_txn(false).await?;
            let mut migrated = 0usize;
            let doc_prefix = only_peer
                .map(ReplicatorRetryDocIDKey::peer_prefix)
                .unwrap_or_else(ReplicatorRetryDocIDKey::retry_doc_prefix);
            let mut docs = txn
                .iterator(IterOptions::new().with_prefix(doc_prefix))
                .await?;
            let mut doc_keys = Vec::new();
            while let Some(pair) = docs.next().await? {
                if !pair.value.is_empty() {
                    let key = String::from_utf8_lossy(&pair.key);
                    let peer = key
                        .strip_prefix("/rep/retry/doc/")
                        .and_then(|rest| rest.split('/').next())
                        .filter(|peer| !peer.is_empty())
                        .map(str::to_owned);
                    doc_keys.push((pair.key, peer));
                }
            }
            drop(docs);
            for (key, peer) in doc_keys {
                if key.ends_with(b"/") {
                    txn.delete(&key).await?;
                } else {
                    txn.set(&key, &[]).await?;
                    if let Some(peer) = peer {
                        let id_key = ReplicatorRetryIDKey::new(&peer);
                        if !txn.has(&id_key.bytes()).await? {
                            let info = super::RetryInfo::new_initial();
                            txn.set(
                                &id_key.bytes(),
                                &info.to_bytes().map_err(crate::corekv::Error::Other)?,
                            )
                            .await?;
                        }
                    }
                }
                migrated += 1;
            }

            let commit_prefix = only_peer
                .map(ReplicatorRetryCommitKey::peer_prefix)
                .unwrap_or_else(ReplicatorRetryCommitKey::retry_commit_prefix);
            let mut commits = txn
                .iterator(IterOptions::new().with_prefix(commit_prefix))
                .await?;
            let mut legacy = Vec::new();
            while let Some(pair) = commits.next().await? {
                let key = String::from_utf8_lossy(&pair.key);
                let Some(rest) = key.strip_prefix("/rep/retry/commit/") else {
                    continue;
                };
                let mut parts = rest.split('/');
                let (Some(peer), Some(collection)) = (parts.next(), parts.next()) else {
                    continue;
                };
                if !peer.is_empty() && !collection.is_empty() {
                    legacy.push((pair.key.clone(), peer.to_string(), collection.to_string()));
                }
            }
            drop(commits);
            for (old_key, peer, collection) in legacy {
                txn.set(
                    &ReplicatorRetryCollectionKey::new(&peer, &collection).bytes(),
                    &[],
                )
                .await?;
                let id_key = ReplicatorRetryIDKey::new(&peer);
                if !txn.has(&id_key.bytes()).await? {
                    let mut info = super::RetryInfo::new_initial();
                    info.bump_for(&peer);
                    txn.set(
                        &id_key.bytes(),
                        &info.to_bytes().map_err(crate::corekv::Error::Other)?,
                    )
                    .await?;
                }
                txn.delete(&old_key).await?;
                migrated += 1;
            }
            txn.commit().await?;
            Ok(migrated)
        })
        .await
    }

    /// Record a push failure for a specific peer/doc pair.
    ///
    /// Writes the peer schedule and a presence-only document or collection marker.
    pub async fn record_push_failure(
        &self,
        peer_id: &str,
        doc_id: &str,
        collection_id: &str,
        cid: &str,
        priority: u64,
        retry_info_bytes: &[u8],
    ) -> Result<()> {
        if doc_id.is_empty() && cid.is_empty() {
            return Ok(());
        }
        let _ = priority;
        self.register_scope_marker(peer_id, doc_id, collection_id, retry_info_bytes)
            .await
    }

    /// Register the dirty scope before a live head announcement.
    pub async fn observe_push_head(
        &self,
        peer_id: &str,
        doc_id: &str,
        collection_id: &str,
        cid: &str,
        priority: u64,
    ) -> Result<()> {
        if doc_id.is_empty() && cid.is_empty() {
            return Ok(());
        }
        let _ = priority;
        let initial = super::RetryInfo::new_initial()
            .to_bytes()
            .map_err(crate::corekv::Error::Other)?;
        self.register_scope_marker(peer_id, doc_id, collection_id, &initial)
            .await
    }

    async fn load_scope_markers(&self, peer_id: &str) -> Result<Vec<super::PersistedPushRetry>> {
        self.migrate_push_retry_markers(Some(peer_id)).await?;
        let retry_info = self
            .get_retry_info(peer_id)
            .await?
            .and_then(|bytes| super::RetryInfo::from_bytes(&bytes).ok())
            .unwrap_or_else(super::RetryInfo::new_initial);
        let txn = self.store.new_txn(true).await?;
        let mut result = Vec::new();

        let doc_prefix = ReplicatorRetryDocIDKey::peer_prefix(peer_id);
        let expected_doc = format!("/rep/retry/doc/{peer_id}/");
        let mut docs = txn
            .iterator(IterOptions::new().with_prefix(doc_prefix))
            .await?;
        while let Some(pair) = docs.next().await? {
            let key = String::from_utf8_lossy(&pair.key);
            let Some(doc_id) = key.strip_prefix(&expected_doc) else {
                continue;
            };
            if !doc_id.is_empty() {
                result.push(super::PersistedPushRetry {
                    doc_id: doc_id.to_string(),
                    collection_id: String::new(),
                    cid: String::new(),
                    priority: 0,
                    pending: true,
                    scope: super::RetryScope::Document,
                    retry_info: retry_info.clone(),
                });
            }
        }
        drop(docs);

        let col_prefix = ReplicatorRetryCollectionKey::peer_prefix(peer_id);
        let expected_col = format!("/rep/retry/col/{peer_id}/");
        let mut cols = txn
            .iterator(IterOptions::new().with_prefix(col_prefix))
            .await?;
        while let Some(pair) = cols.next().await? {
            let key = String::from_utf8_lossy(&pair.key);
            let Some(collection_id) = key.strip_prefix(&expected_col) else {
                continue;
            };
            if !collection_id.is_empty() {
                result.push(super::PersistedPushRetry {
                    doc_id: String::new(),
                    collection_id: collection_id.to_string(),
                    cid: String::new(),
                    priority: 0,
                    pending: true,
                    scope: super::RetryScope::CollectionCommit,
                    retry_info: retry_info.clone(),
                });
            }
        }
        result.sort_by(|a, b| {
            a.scope
                .cmp(&b.scope)
                .then_with(|| a.doc_id.cmp(&b.doc_id))
                .then_with(|| a.collection_id.cmp(&b.collection_id))
        });
        Ok(result)
    }

    /// Load presence-only document and collection scopes on the peer clock.
    pub async fn get_retry_documents(
        &self,
        peer_id: &str,
    ) -> Result<Vec<super::PersistedPushRetry>> {
        self.load_scope_markers(peer_id).await
    }

    /// Migrate legacy payload/CID records into scope markers after startup.
    pub async fn migrate_legacy_push_retries(&self) -> Result<usize> {
        self.migrate_push_retry_markers(None).await
    }

    /// Count durable scope markers and report the oldest shared peer schedule.
    pub async fn push_retry_marker_stats(&self) -> Result<super::PushRetryMarkerStats> {
        let txn = self.store.new_txn(true).await?;
        let mut stats = super::PushRetryMarkerStats::default();

        let mut docs = txn
            .iterator(IterOptions::new().with_prefix(ReplicatorRetryDocIDKey::retry_doc_prefix()))
            .await?;
        while let Some(pair) = docs.next().await? {
            if !pair.key.ends_with(b"/") {
                stats.document_markers += 1;
            }
        }
        drop(docs);

        let mut collections = txn
            .iterator(
                IterOptions::new()
                    .with_prefix(ReplicatorRetryCollectionKey::retry_collection_prefix()),
            )
            .await?;
        while let Some(pair) = collections.next().await? {
            if !pair.key.ends_with(b"/") {
                stats.collection_markers += 1;
            }
        }
        drop(collections);

        let mut schedules = txn
            .iterator(IterOptions::new().with_prefix(ReplicatorRetryIDKey::retry_prefix()))
            .await?;
        while let Some(pair) = schedules.next().await? {
            let Ok(info) = super::RetryInfo::from_bytes(&pair.value) else {
                continue;
            };
            stats.scheduled_peers += 1;
            stats.oldest_scheduled_retry_unix = Some(
                stats
                    .oldest_scheduled_retry_unix
                    .map_or(info.next_retry_unix, |oldest| {
                        oldest.min(info.next_retry_unix)
                    }),
            );
        }
        Ok(stats)
    }

    pub async fn update_retry_document(
        &self,
        peer_id: &str,
        retry: &super::PersistedPushRetry,
    ) -> Result<()> {
        let bytes = retry
            .retry_info
            .to_bytes()
            .map_err(crate::corekv::Error::Other)?;
        retry_push_txn_conflicts(|| async {
            let mut txn = self.store.new_txn(false).await?;
            txn.set(&ReplicatorRetryIDKey::new(peer_id).bytes(), &bytes)
                .await?;
            txn.commit().await
        })
        .await
    }

    /// Make an existing peer retry schedule immediately due without changing
    /// its failure-ladder rung.  A connection-established event is new
    /// delivery evidence: retaining an old connection-failure deadline after
    /// that event can leave otherwise actionable scope markers dormant.
    pub async fn activate_retry_peer(&self, peer_id: &str) -> Result<bool> {
        retry_push_txn_conflicts(|| async {
            let mut txn = self.store.new_txn(false).await?;
            let key = ReplicatorRetryIDKey::new(peer_id).bytes();
            let Some(bytes) = txn.get(&key).await? else {
                return Ok(false);
            };
            let mut info =
                super::RetryInfo::from_bytes(&bytes).map_err(crate::corekv::Error::Other)?;
            info.defer_for(std::time::Duration::ZERO);
            txn.set(&key, &info.to_bytes().map_err(crate::corekv::Error::Other)?)
                .await?;
            txn.commit().await?;
            Ok(true)
        })
        .await
    }

    /// Remove a scope marker after the caller has verified its rederived heads
    /// are still current. Runtime ack fences serialize this with live updates.
    pub async fn complete_retry_document(
        &self,
        peer_id: &str,
        retry: &super::PersistedPushRetry,
    ) -> Result<()> {
        self.complete_retry_scope(
            peer_id,
            &retry.doc_id,
            &retry.collection_id,
            retry.is_collection_commit(),
        )
        .await
    }

    /// Remove one presence-only retry marker after its current scope state has
    /// been checked while holding the peer's retry-transition guard.
    pub async fn complete_retry_scope(
        &self,
        peer_id: &str,
        doc_id: &str,
        collection_id: &str,
        is_collection: bool,
    ) -> Result<()> {
        let key = if is_collection {
            ReplicatorRetryCollectionKey::new(peer_id, collection_id).bytes()
        } else {
            ReplicatorRetryDocIDKey::new(peer_id, doc_id).bytes()
        };
        retry_push_txn_conflicts(|| async {
            let mut txn = self.store.new_txn(false).await?;
            txn.delete(&key).await?;
            txn.commit().await
        })
        .await
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

    /// Stop sweeping a peer once no document or collection marker remains.
    pub async fn clear_retry_peer(&self, peer_id: &str) -> Result<()> {
        if !self.load_scope_markers(peer_id).await?.is_empty() {
            return Ok(());
        }
        retry_push_txn_conflicts(|| async {
            let mut txn = self.store.new_txn(false).await?;
            txn.delete(&ReplicatorRetryIDKey::new(peer_id).bytes())
                .await?;
            txn.commit().await
        })
        .await
    }
}

impl<S: Store> crate::corekv::private::Sealed for Peerstore<S> {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store> Store for Peerstore<S> {
    #[cfg(not(target_arch = "wasm32"))]
    fn transaction_stats_handle(&self) -> Option<crate::backends::TransactionStatsHandle> {
        self.store.transaction_stats_handle()
    }

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
