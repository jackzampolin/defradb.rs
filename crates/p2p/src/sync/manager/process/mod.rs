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
use super::pending::PendingDag;

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

    /// Pending DAGs waiting for Bitswap to complete.
    /// Maps root CID → pending DAG metadata.
    pub(super) pending_dags: Arc<RwLock<HashMap<Cid, PendingDag>>>,

    /// Maps Bitswap QueryId → root CID for tracking completions.
    pub(super) query_to_root: Arc<RwLock<HashMap<QueryId, Cid>>>,

    /// Observability counters (see `SyncDiagnostics`).
    pub(crate) diagnostics: Arc<SyncDiagnostics>,

    /// Capacity of `pending_dags`; overflow is rejected with a backpressure nack.
    pub(super) max_pending_dags: usize,

    /// Durable backing for push-originated pending registrations (#1099).
    /// Empty until the embedding layer installs a store; process-local
    /// semantics apply while empty.
    pub(super) pending_store:
        std::sync::OnceLock<Arc<dyn crate::sync::pending_store::PendingDagStorage>>,
}

impl<B: Blockstore + 'static> SyncManager<B> {
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
            pending_dags: Arc::new(RwLock::new(HashMap::new())),
            query_to_root: Arc::new(RwLock::new(HashMap::new())),
            diagnostics: Arc::new(SyncDiagnostics::default()),
            // A zero cap would reject every missing-link push forever
            // (permanent admission outage); normalize to a 1-slot map.
            max_pending_dags: config.max_pending_dags.max(1),
            pending_store: std::sync::OnceLock::new(),
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
    pub async fn mark_as_merged(&self, cid: &Cid) -> crate::error::Result<()> {
        self.blockstore
            .mark_as_merged(cid)
            .await
            .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))
    }

    /// Mark multiple blocks as merged in a single transaction.
    pub async fn mark_batch_as_merged(&self, cids: &[Cid]) -> crate::error::Result<()> {
        self.blockstore
            .mark_batch_as_merged(cids)
            .await
            .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))
    }

    /// Get the process queue used to serialize work for the same CID.
    pub(crate) fn process_queue(&self) -> ProcessQueue {
        self.process_queue.clone()
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

    /// Install the durable pending-DAG store. First-call-wins (OnceLock
    /// semantics); subsequent calls are silently discarded.
    pub fn install_pending_dag_store(
        &self,
        store: Arc<dyn crate::sync::pending_store::PendingDagStorage>,
    ) {
        let _ = self.pending_store.set(store);
    }

    pub(super) fn pending_store(
        &self,
    ) -> Option<Arc<dyn crate::sync::pending_store::PendingDagStorage>> {
        self.pending_store.get().cloned()
    }

    /// Best-effort deletion of persisted registrations from sync call sites.
    /// A late (or lost) delete is safe: restore re-checks merge state and the
    /// TTL expiry path deletes stale records.
    pub(super) fn schedule_persisted_pending_removal(&self, roots: Vec<Cid>) {
        let Some(store) = self.pending_store() else {
            return;
        };
        if roots.is_empty() {
            return;
        }
        tokio::spawn(async move {
            for root_cid in roots {
                if let Err(error) = store.remove(&root_cid).await {
                    tracing::warn!(
                        root_cid = %root_cid,
                        error = %error,
                        "Failed to delete persisted pending DAG record"
                    );
                }
            }
        });
    }

    pub(super) async fn remove_persisted_pending(&self, root_cid: &Cid) {
        if let Some(store) = self.pending_store() {
            if let Err(error) = store.remove(root_cid).await {
                tracing::warn!(
                    root_cid = %root_cid,
                    error = %error,
                    "Failed to delete persisted pending DAG record"
                );
            }
        }
    }
}
