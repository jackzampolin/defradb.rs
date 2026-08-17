//! Sync manager for coordinating P2P block synchronization.
//!
//! The SyncManager handles:
//! - Processing incoming PushLog messages
//! - Storing blocks in the blockstore with merge tracking
//! - Emitting events for database-layer CRDT merging
//! - Broadcasting local changes to the network
//!
//! # Architecture Note
//!
//! The P2P layer handles block storage and network coordination.
//! The actual CRDT merge is performed by the database layer.
//! This matches Go's architecture where p2p calls db.Merge().

mod bitswap;
mod pending_dag;
mod pushlog;

pub(crate) use bitswap::{BlockSyncCompletionTracker, RootedCarCompletionTracker};
pub(crate) use pending_dag::PendingDagLease;

use crate::QueryId;
use cid::Cid;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

use blockstore::Blockstore;

use crate::sync::PeerStateTracker;

use super::super::queue::ProcessQueue;
use super::config::SyncConfig;
use super::diagnostics::SyncDiagnostics;
use super::events::SyncEvent;
use super::pending::PendingDagRegistry;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct PersistedScopeKey {
    source_peer: String,
    collection_id: String,
    doc_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct PersistedHeadVersion {
    priority: u64,
    cid: Cid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PersistedScopeDecision {
    Independent,
    Current,
    CoveredByCurrent,
    Supersedes(Cid),
}

/// Manager for P2P block synchronization.
///
/// # Usage
///
/// ```ignore
/// use p2p::sync::{SyncManager, SyncConfig};
/// use blockstore::DefraBlockstore;
///
/// // Create blockstore in P2P mode (merge tracking enabled)
/// let blockstore = DefraBlockstore::new(store, true);
///
/// // Create peer state tracker and sync manager
/// let peer_state = Arc::new(PeerStateTracker::new());
/// let (manager, mut events) = SyncManager::new(blockstore, peer_state, SyncConfig::default());
///
/// // Handle incoming PushLog
/// manager.process_pushlog(pushlog).await?;
///
/// // Process events
/// while let Some(event) = events.recv().await {
///     match event {
///         SyncEvent::BlockReceived { cid, doc_id, .. } => {
///             // Do CRDT merge
///             db.merge(&cid).await?;
///         }
///         _ => {}
///     }
/// }
/// ```
pub struct SyncManager<B: Blockstore> {
    /// Blockstore for storing/retrieving blocks
    pub(super) blockstore: Arc<B>,

    /// Process queue for serializing concurrent syncs
    pub(super) process_queue: ProcessQueue,

    /// Channel for emitting sync events
    pub(super) event_tx: mpsc::Sender<SyncEvent>,

    /// Peer state tracker for finding providers
    pub(super) peer_state: Arc<PeerStateTracker>,

    /// Pending DAGs waiting for Bitswap to complete, indexed by root and each
    /// missing CID.
    pub(super) pending_dags: Arc<RwLock<PendingDagRegistry>>,

    /// Maps Bitswap QueryId → root CID for tracking completions.
    pub(super) query_to_root: Arc<RwLock<HashMap<QueryId, Cid>>>,

    /// Completion waiters for poll-owned exact-CID queries.
    pub(super) block_sync_completions: BlockSyncCompletionTracker,

    /// Completion waiters for libp2p's separately streamed rooted CAR replies.
    pub(super) rooted_car_completions: RootedCarCompletionTracker,

    /// Observability counters (see `SyncDiagnostics`).
    pub(crate) diagnostics: Arc<SyncDiagnostics>,

    /// Capacity of `pending_dags`; overflow is rejected with a backpressure nack.
    pub(super) max_pending_dags: usize,

    /// Durable backing for push-originated pending registrations (#1099).
    /// Empty until the embedding layer installs a store; process-local
    /// semantics apply while empty.
    pub(super) pending_store:
        std::sync::OnceLock<Arc<dyn crate::sync::pending_store::PendingDagStorage>>,

    /// Serializes same-scope durable replacement decisions across distinct
    /// root CIDs. The storage transaction remains bounded and atomic.
    pub(super) pending_registration_writer: tokio::sync::Mutex<()>,

    /// Roots with a durable registration. Superset guard so the merge path
    /// only pays a delete transaction for roots that actually have records,
    /// and admission can bound durable growth without hitting storage.
    pub(super) persisted_roots: Arc<RwLock<std::collections::HashSet<Cid>>>,

    /// Current durable root for each sender/document-or-collection scope.
    /// This survives pending TTL eviction and is hydrated before the first
    /// post-restart PushLog.
    pub(super) persisted_scope_heads: Arc<RwLock<HashMap<PersistedScopeKey, PersistedHeadVersion>>>,

    /// Single-flight guard for the durable resync sweep.
    pub(super) pending_resync_in_flight: std::sync::atomic::AtomicBool,

    /// Resync invocation counter; every Nth sweep skips the steady-state
    /// early-exit so store/set desyncs from rare races self-heal.
    pub(super) pending_resync_tick: std::sync::atomic::AtomicUsize,

    /// Gauge of currently quarantined pending-DAG roots (#1128). Hydrated
    /// from `load_quarantined().len()` when the store is installed and
    /// bumped on each `quarantine_pending_dag` call, rather than rescanning
    /// the store on every read — quarantine is rare (deterministic content
    /// rejection) but `quarantined_pending_count()` may be polled by status
    /// endpoints far more often than that.
    pub(super) quarantined_pending_count: std::sync::atomic::AtomicUsize,
}

/// Every Nth resync is a forced full sweep (see `pending_resync_tick`).
pub(super) const PENDING_RESYNC_FORCED_TICK: usize = 10;

/// Durable registrations may outlive their in-memory pending entries (TTL
/// eviction frees the map slot but not the recovery obligation), so the
/// durable set gets its own, larger cap. At the cap new registrations are
/// nacked — the hub refuses the obligation while the pusher still owns its
/// retry state, rather than accepting and later dropping it.
pub(super) const PERSISTED_PENDING_CAP_FACTOR: usize = 4;

impl<B: Blockstore + 'static> SyncManager<B> {
    pub(super) fn persisted_scope_key(
        source_peer: Option<&str>,
        collection_id: &str,
        doc_id: &str,
        head_priority: Option<u64>,
    ) -> Option<(PersistedScopeKey, u64)> {
        Some((
            PersistedScopeKey {
                source_peer: source_peer?.to_owned(),
                collection_id: collection_id.to_owned(),
                doc_id: doc_id.to_owned(),
            },
            head_priority?,
        ))
    }

    pub(super) fn persisted_scope_decision(
        &self,
        root_cid: Cid,
        source_peer: Option<&str>,
        collection_id: &str,
        doc_id: &str,
        head_priority: Option<u64>,
    ) -> PersistedScopeDecision {
        let Some((key, priority)) =
            Self::persisted_scope_key(source_peer, collection_id, doc_id, head_priority)
        else {
            return PersistedScopeDecision::Independent;
        };
        let version = PersistedHeadVersion {
            priority,
            cid: root_cid,
        };
        match self.persisted_scope_heads.read().get(&key) {
            None => PersistedScopeDecision::Current,
            Some(current) if current.cid == root_cid => PersistedScopeDecision::Current,
            Some(current) if version <= *current => PersistedScopeDecision::CoveredByCurrent,
            Some(current) => PersistedScopeDecision::Supersedes(current.cid),
        }
    }

    pub(super) fn scope_head_is_refresh_or_newer(
        &self,
        root_cid: Cid,
        source_peer: Option<&str>,
        collection_id: &str,
        doc_id: &str,
        head_priority: Option<u64>,
    ) -> bool {
        let Some((key, priority)) =
            Self::persisted_scope_key(source_peer, collection_id, doc_id, head_priority)
        else {
            return false;
        };
        let version = PersistedHeadVersion {
            priority,
            cid: root_cid,
        };
        self.persisted_scope_heads
            .read()
            .get(&key)
            .is_some_and(|current| current.cid == root_cid || version > *current)
            || self.pending_dags.read().scope_head_is_refresh_or_newer(
                root_cid,
                source_peer,
                collection_id,
                doc_id,
                head_priority,
            )
    }

    pub(super) fn remember_persisted_scope_head(
        &self,
        root_cid: Cid,
        source_peer: Option<&str>,
        collection_id: &str,
        doc_id: &str,
        head_priority: Option<u64>,
    ) {
        let Some((key, priority)) =
            Self::persisted_scope_key(source_peer, collection_id, doc_id, head_priority)
        else {
            return;
        };
        let version = PersistedHeadVersion {
            priority,
            cid: root_cid,
        };
        let mut heads = self.persisted_scope_heads.write();
        if heads.get(&key).is_none_or(|current| version >= *current) {
            heads.insert(key, version);
        }
    }

    pub(super) fn forget_persisted_scope_root(&self, root_cid: &Cid) {
        self.persisted_scope_heads
            .write()
            .retain(|_, version| version.cid != *root_cid);
    }

    /// Create a new SyncManager.
    ///
    /// # Arguments
    ///
    /// * `blockstore` - The blockstore for storing/retrieving blocks
    /// * `peer_state` - Peer state tracker for finding block providers
    /// * `config` - Configuration options
    ///
    /// Returns the manager and a receiver for sync events.
    pub fn new(
        blockstore: Arc<B>,
        peer_state: Arc<PeerStateTracker>,
        config: SyncConfig,
    ) -> (Self, mpsc::Receiver<SyncEvent>) {
        let (event_tx, event_rx) = mpsc::channel(config.event_buffer_size);

        let manager = Self {
            blockstore,
            process_queue: ProcessQueue::new(),
            event_tx,
            peer_state,
            pending_dags: Arc::new(RwLock::new(PendingDagRegistry::default())),
            query_to_root: Arc::new(RwLock::new(HashMap::new())),
            block_sync_completions: BlockSyncCompletionTracker::default(),
            rooted_car_completions: RootedCarCompletionTracker::default(),
            diagnostics: Arc::new(SyncDiagnostics::default()),
            // A zero cap would reject every missing-link push forever
            // (permanent admission outage); normalize to a 1-slot map.
            max_pending_dags: config.max_pending_dags.max(1),
            pending_store: std::sync::OnceLock::new(),
            pending_registration_writer: tokio::sync::Mutex::new(()),
            persisted_roots: Arc::new(RwLock::new(std::collections::HashSet::new())),
            persisted_scope_heads: Arc::new(RwLock::new(HashMap::new())),
            pending_resync_in_flight: std::sync::atomic::AtomicBool::new(false),
            pending_resync_tick: std::sync::atomic::AtomicUsize::new(0),
            quarantined_pending_count: std::sync::atomic::AtomicUsize::new(0),
        };

        (manager, event_rx)
    }

    /// Check if a block exists and is merged.
    pub async fn is_merged(&self, cid: &Cid) -> crate::error::Result<bool> {
        self.blockstore
            .is_merged(cid)
            .await
            .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))
    }

    /// Mark a block as merged (called by database layer after CRDT merge).
    ///
    /// A successful terminal merge is the point where a durable pending
    /// registration has discharged its recovery obligation (#1099): only
    /// here or by quarantine (#1128, `quarantine_pending_dag` — a
    /// deterministic content rejection that will never succeed on replay)
    /// — and never at DagReady emission, TTL eviction, or clear — is the
    /// persisted record deleted.
    pub async fn mark_as_merged(&self, cid: &Cid) -> crate::error::Result<()> {
        self.blockstore
            .mark_as_merged(cid)
            .await
            .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
        if self.persisted_roots.read().contains(cid) {
            self.remove_persisted_pending(cid).await;
            if !self.persisted_roots.read().contains(cid) {
                self.diagnostics.record_pending_dag_terminal_merged();
            }
        }
        Ok(())
    }

    /// Mark multiple blocks as merged in a single transaction.
    pub async fn mark_batch_as_merged(&self, cids: &[Cid]) -> crate::error::Result<()> {
        self.blockstore
            .mark_batch_as_merged(cids)
            .await
            .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
        let persisted: Vec<Cid> = {
            let roots = self.persisted_roots.read();
            cids.iter()
                .filter(|cid| roots.contains(cid))
                .copied()
                .collect()
        };
        for cid in &persisted {
            self.remove_persisted_pending(cid).await;
            if !self.persisted_roots.read().contains(cid) {
                self.diagnostics.record_pending_dag_terminal_merged();
            }
        }
        Ok(())
    }

    /// Get the process queue used to serialize work for the same CID.
    pub(crate) fn process_queue(&self) -> ProcessQueue {
        self.process_queue.clone()
    }

    /// Acquire the shared ingest/merge owners for a rooted block batch.
    ///
    /// The root is included even when an exact selective-CAR response only
    /// carries descendants. This keeps response storage ordered with the
    /// root's PushLog registration and eventual merge, matching Go's
    /// root-owned ingest path. Each contained CID is included as well because
    /// Rust's SSI store conflict-checks the mutable `ToMergeIndexKey`; shared
    /// child blocks from different roots must therefore also have one writer.
    pub(crate) async fn acquire_car_storage_owners(
        &self,
        root_cid: Cid,
        block_cids: Vec<Cid>,
    ) -> Vec<crate::sync::queue::ProcessGuard> {
        self.process_queue
            .acquire_all(std::iter::once(root_cid).chain(block_cids))
            .await
    }

    /// Get all unmerged block CIDs.
    pub async fn get_unmerged(&self) -> crate::error::Result<Vec<Cid>> {
        self.blockstore
            .get_unmerged()
            .await
            .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))
    }

    /// Get the blockstore reference.
    pub fn blockstore(&self) -> &Arc<B> {
        &self.blockstore
    }

    /// Get a clone of the sync event sender for spawning background tasks.
    pub fn event_sender(&self) -> mpsc::Sender<SyncEvent> {
        self.event_tx.clone()
    }

    /// Shared reference to the sync diagnostics counters.
    pub fn diagnostics(&self) -> Arc<SyncDiagnostics> {
        Arc::clone(&self.diagnostics)
    }

    /// Effective pending-DAG capacity.
    pub fn max_pending_dags(&self) -> usize {
        self.max_pending_dags
    }

    /// Number of durable pending-DAG registrations currently held.
    pub fn persisted_pending_count(&self) -> usize {
        self.persisted_roots.read().len()
    }

    /// Cap on durable pending-DAG registrations; admission nacks beyond it.
    pub fn persisted_pending_capacity(&self) -> usize {
        self.max_pending_dags
            .saturating_mul(PERSISTED_PENDING_CAP_FACTOR)
    }

    /// Whether the durable resync sweep is currently running.
    pub fn pending_resync_in_flight(&self) -> bool {
        self.pending_resync_in_flight
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Install the durable pending-DAG store. First-call-wins (OnceLock
    /// semantics); subsequent calls are silently discarded.
    ///
    /// Hydrates the persisted-roots set from the store before returning so
    /// the durable admission cap is hard from the first PushLog — without
    /// this, pushes arriving before the first resync sweep would be admitted
    /// against an empty set.
    pub async fn install_pending_dag_store(
        &self,
        store: Arc<dyn crate::sync::pending_store::PendingDagStorage>,
    ) {
        if self.pending_store.set(Arc::clone(&store)).is_err() {
            return;
        }
        match store.load_all().await {
            Ok(records) => {
                self.persisted_roots
                    .write()
                    .extend(records.iter().map(|(cid, _)| *cid));
                for (cid, record) in &records {
                    self.remember_persisted_scope_head(
                        *cid,
                        record.source_peer.as_deref(),
                        &record.collection_id,
                        &record.doc_id,
                        record.head_priority,
                    );
                }
                self.diagnostics
                    .observe_persisted_pending_dag_depth(self.persisted_roots.read().len());
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "Failed to hydrate persisted pending DAG roots; durable cap is soft until the first resync"
                );
            }
        }
        match store.load_quarantined().await {
            Ok(records) => {
                self.quarantined_pending_count
                    .store(records.len(), std::sync::atomic::Ordering::Relaxed);
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "Failed to hydrate quarantined pending DAG gauge; count is 0 until the next quarantine"
                );
            }
        }
    }

    /// Number of pending-DAG roots currently quarantined (#1128).
    pub fn quarantined_pending_count(&self) -> usize {
        self.quarantined_pending_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(super) fn pending_store(
        &self,
    ) -> Option<Arc<dyn crate::sync::pending_store::PendingDagStorage>> {
        self.pending_store.get().cloned()
    }

    pub(super) async fn remove_persisted_pending(&self, root_cid: &Cid) {
        if let Some(store) = self.pending_store() {
            match store.remove(root_cid).await {
                Ok(()) => {
                    self.persisted_roots.write().remove(root_cid);
                    self.forget_persisted_scope_root(root_cid);
                }
                Err(error) => {
                    tracing::warn!(
                        root_cid = %root_cid,
                        error = %error,
                        "Failed to delete persisted pending DAG record"
                    );
                }
            }
        }
    }
}
