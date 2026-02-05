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

use crate::QueryId;
use cid::Cid;
use libp2p::PeerId;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;

use blockstore::Blockstore;

use crate::error::{Error, Result};
use crate::message::PushLogBroadcast;
use crate::sync::PeerStateTracker;

use super::config::SyncConfig;
use super::events::SyncEvent;
use super::links::{find_all_missing_links, find_missing_links};
use super::pending::PendingDag;
use super::super::queue::ProcessQueue;

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
    blockstore: Arc<B>,

    /// Process queue for serializing concurrent syncs
    process_queue: ProcessQueue,

    /// Channel for emitting sync events
    event_tx: mpsc::Sender<SyncEvent>,

    /// Peer state tracker for finding providers
    peer_state: Arc<PeerStateTracker>,

    /// Pending DAGs waiting for Bitswap to complete.
    /// Maps root CID → pending DAG metadata.
    pending_dags: Arc<RwLock<HashMap<Cid, PendingDag>>>,

    /// Maps Bitswap QueryId → root CID for tracking completions.
    query_to_root: Arc<RwLock<HashMap<QueryId, Cid>>>,
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
        };

        (manager, event_rx)
    }

    /// Process an incoming PushLog broadcast.
    ///
    /// This is the main entry point for handling sync messages from the network.
    ///
    /// # Flow
    ///
    /// 1. Parse CID from the message
    /// 2. Acquire process queue lock (serialize concurrent syncs for same CID)
    /// 3. Check if already merged
    /// 4. Store block in blockstore (marked as unmerged)
    /// 5. Emit BlockReceived event for database layer to merge
    ///
    /// # Go Compatibility
    ///
    /// This matches Go's `processPushlogRequest()` in `p2p.go:446-530`,
    /// except the actual CRDT merge is delegated to the database layer.
    pub async fn process_pushlog(&self, msg: &PushLogBroadcast) -> Result<()> {
        // Parse CID from message
        let cid = Cid::try_from(msg.cid.as_slice())
            .map_err(|e| Error::InvalidCid(format!("Failed to parse CID: {}", e)))?;
        eprintln!(
            "[SYNC-MGR] process_pushlog cid={} doc_id={} collection={} block_len={}",
            cid,
            msg.doc_id,
            msg.collection_id,
            msg.block.len()
        );

        // Try to acquire exclusive processing rights for this CID
        match self.process_queue.try_acquire(&cid).await {
            Ok(_guard) => {
                // We're the first - process the block
                self.process_block_inner(&cid, msg).await
            }
            Err(rx) => {
                // Another task is processing - wait for it
                if rx.await.is_err() {
                    tracing::debug!(
                        ?cid,
                        "First processor task was cancelled, will check merge status"
                    );
                }

                // Now check if block is already merged
                match self.blockstore.is_merged(&cid).await {
                    Ok(true) => {
                        // Already merged by the other task
                        if self
                            .event_tx
                            .send(SyncEvent::BlockAlreadyMerged { cid })
                            .await
                            .is_err()
                        {
                            tracing::warn!(
                                ?cid,
                                "Failed to send BlockAlreadyMerged event - receiver dropped"
                            );
                            return Err(Error::ChannelSend);
                        }
                        Ok(())
                    }
                    Ok(false) => {
                        // Not yet merged - we need to process it
                        // (This can happen if the first task failed)
                        self.process_block_inner(&cid, msg).await
                    }
                    Err(e) => {
                        if self
                            .event_tx
                            .send(SyncEvent::SyncError {
                                cid,
                                error: e.to_string(),
                            })
                            .await
                            .is_err()
                        {
                            tracing::warn!(
                                ?cid,
                                "Failed to send SyncError event - receiver dropped"
                            );
                            // Return channel error since we can't notify caller of the blockstore error
                            return Err(Error::ChannelSend);
                        }
                        Err(Error::BlockstoreError(e.to_string()))
                    }
                }
            }
        }
    }

    /// Inner block processing logic.
    async fn process_block_inner(&self, cid: &Cid, msg: &PushLogBroadcast) -> Result<()> {
        // Check if already merged
        match self.blockstore.is_merged(cid).await {
            Ok(true) => {
                eprintln!(
                    "[SYNC-MGR] Block already merged cid={} doc_id={}",
                    cid, msg.doc_id
                );
                tracing::debug!(?cid, "Block already merged, skipping");
                if self
                    .event_tx
                    .send(SyncEvent::BlockAlreadyMerged { cid: *cid })
                    .await
                    .is_err()
                {
                    tracing::warn!(
                        ?cid,
                        "Failed to send BlockAlreadyMerged event - receiver dropped"
                    );
                    return Err(Error::ChannelSend);
                }
                return Ok(());
            }
            Ok(false) => {
                // Not merged, continue processing
            }
            Err(e) => {
                if self
                    .event_tx
                    .send(SyncEvent::SyncError {
                        cid: *cid,
                        error: e.to_string(),
                    })
                    .await
                    .is_err()
                {
                    tracing::warn!(?cid, "Failed to send SyncError event - receiver dropped");
                    return Err(Error::ChannelSend);
                }
                return Err(Error::BlockstoreError(e.to_string()));
            }
        }

        // Store the block (marked as unmerged in P2P mode)
        if let Err(e) = self.blockstore.put(cid, &msg.block).await {
            if self
                .event_tx
                .send(SyncEvent::SyncError {
                    cid: *cid,
                    error: e.to_string(),
                })
                .await
                .is_err()
            {
                tracing::warn!(?cid, "Failed to send SyncError event - receiver dropped");
                return Err(Error::ChannelSend);
            }
            return Err(Error::BlockstoreError(e.to_string()));
        }

        tracing::debug!(
            ?cid,
            doc_id = %msg.doc_id,
            collection_id = %msg.collection_id,
            "Block stored, checking for missing links"
        );

        // Check for missing linked blocks
        let missing = match find_missing_links(self.blockstore.as_ref(), &msg.block).await {
            Ok(m) => m,
            Err(e) => {
                // Block parsing failed - emit error event and propagate error
                if self
                    .event_tx
                    .send(SyncEvent::SyncError {
                        cid: *cid,
                        error: e.to_string(),
                    })
                    .await
                    .is_err()
                {
                    tracing::warn!(?cid, "Failed to send SyncError event - receiver dropped");
                    return Err(Error::ChannelSend);
                }
                return Err(e);
            }
        };

        if missing.is_empty() {
            // DAG is complete - emit BlockReceived for merge
            eprintln!(
                "[SYNC-MGR] DAG complete cid={} doc_id={} — emitting BlockReceived",
                cid, msg.doc_id
            );
            tracing::info!(
                ?cid,
                doc_id = %msg.doc_id,
                "DAG complete, emitting BlockReceived event"
            );

            if self
                .event_tx
                .send(SyncEvent::BlockReceived {
                    cid: *cid,
                    doc_id: msg.doc_id.clone(),
                    collection_id: msg.collection_id.clone(),
                    creator: msg.creator.clone(),
                })
                .await
                .is_err()
            {
                tracing::error!(
                    ?cid,
                    doc_id = %msg.doc_id,
                    "CRITICAL: Failed to send BlockReceived event - block stored but will not be merged. \
                     Event receiver may have been dropped."
                );
                return Err(Error::ChannelSend);
            }
        } else {
            // DAG has missing blocks - track as pending and request Bitswap fetch
            eprintln!(
                "[SYNC-MGR] DAG incomplete cid={} doc_id={} missing={} — requesting Bitswap",
                cid,
                msg.doc_id,
                missing.len()
            );
            tracing::info!(
                ?cid,
                missing_count = missing.len(),
                doc_id = %msg.doc_id,
                "DAG has missing links, requesting Bitswap fetch"
            );

            // Track this DAG as pending
            {
                let mut pending = self.pending_dags.write();
                pending.insert(
                    *cid,
                    PendingDag {
                        doc_id: msg.doc_id.clone(),
                        collection_id: msg.collection_id.clone(),
                        creator: msg.creator.clone(),
                        missing: missing.iter().cloned().collect(),
                    },
                );
            }

            // Get providers for the missing blocks
            let providers = self.get_providers_for_cids(&missing);

            // Emit event to request Bitswap fetch
            if self
                .event_tx
                .send(SyncEvent::DagNeedsFetch {
                    root_cid: *cid,
                    missing: missing.clone(),
                    providers,
                    doc_id: msg.doc_id.clone(),
                    collection_id: msg.collection_id.clone(),
                    creator: msg.creator.clone(),
                })
                .await
                .is_err()
            {
                tracing::error!(
                    ?cid,
                    "Failed to send DagNeedsFetch event - receiver dropped"
                );
                // Clean up pending dag since we can't request fetch
                self.pending_dags.write().remove(cid);
                return Err(Error::ChannelSend);
            }
        }

        Ok(())
    }

    /// Get providers (peers that may have the blocks) for the given CIDs.
    fn get_providers_for_cids(&self, cids: &[Cid]) -> Vec<PeerId> {
        let mut providers = HashSet::new();

        // Add peers known to have any of the CIDs
        for cid in cids {
            for peer in self.peer_state.peers_with_cid(cid) {
                providers.insert(peer);
            }
        }

        // If no specific providers found, use all connected peers
        if providers.is_empty() {
            for peer in self.peer_state.connected_peers() {
                providers.insert(peer);
            }
        }

        providers.into_iter().collect()
    }

    /// Register a Bitswap query for tracking.
    ///
    /// This maps the QueryId to the root CID so we can identify
    /// which DAG a completion event belongs to.
    pub fn register_query(&self, query_id: QueryId, root_cid: Cid) {
        self.query_to_root.write().insert(query_id, root_cid);
    }

    /// Handle Bitswap query completion.
    ///
    /// Called when a Bitswap sync completes (success or failure).
    pub async fn handle_bitswap_complete(
        &self,
        query_id: QueryId,
        success: bool,
        error: Option<String>,
    ) -> Result<()> {
        // Find the root CID for this query
        let root_cid = match self.query_to_root.write().remove(&query_id) {
            Some(cid) => cid,
            None => {
                tracing::debug!(
                    query_id = ?query_id,
                    "Bitswap complete for unknown query, ignoring"
                );
                return Ok(());
            }
        };

        if success {
            // All blocks fetched - emit BlockReceived for the root
            let dag = self.pending_dags.write().remove(&root_cid);
            match dag {
                Some(dag) => {
                    tracing::info!(
                        cid = %root_cid,
                        doc_id = %dag.doc_id,
                        "Bitswap sync complete, emitting BlockReceived"
                    );

                    if self
                        .event_tx
                        .send(SyncEvent::BlockReceived {
                            cid: root_cid,
                            doc_id: dag.doc_id,
                            collection_id: dag.collection_id,
                            creator: dag.creator,
                        })
                        .await
                        .is_err()
                    {
                        tracing::error!(
                            cid = %root_cid,
                            "Failed to send BlockReceived after Bitswap complete - receiver dropped"
                        );
                        return Err(Error::ChannelSend);
                    }
                }
                None => {
                    // This can happen if the DAG was processed by another path,
                    // cleaned up, or if there's a race condition
                    tracing::warn!(
                        cid = %root_cid,
                        "Bitswap sync completed but no pending DAG found - \
                         DAG may have been processed by another path or cleaned up"
                    );
                }
            }
        } else {
            // Sync failed - emit error, clean up
            self.pending_dags.write().remove(&root_cid);

            let error_msg = error.unwrap_or_else(|| "Bitswap sync failed".to_string());
            tracing::warn!(
                cid = %root_cid,
                error = %error_msg,
                "Bitswap sync failed"
            );

            if self
                .event_tx
                .send(SyncEvent::SyncError {
                    cid: root_cid,
                    error: error_msg,
                })
                .await
                .is_err()
            {
                tracing::warn!(
                    cid = %root_cid,
                    "Failed to send SyncError event - receiver dropped"
                );
                return Err(Error::ChannelSend);
            }
        }

        Ok(())
    }

    /// Get the pending DAGs count (for testing/monitoring).
    pub fn pending_dag_count(&self) -> usize {
        self.pending_dags.read().len()
    }

    /// Get CIDs of all pending DAGs.
    pub fn pending_dag_cids(&self) -> Vec<Cid> {
        self.pending_dags.read().keys().copied().collect()
    }

    /// Get missing CIDs for a pending DAG.
    pub fn pending_dag_missing(&self, root_cid: &Cid) -> Vec<Cid> {
        self.pending_dags
            .read()
            .get(root_cid)
            .map(|dag| dag.missing.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Register a pending DAG for DocSync.
    ///
    /// This is called when a DocSyncReply contains head CIDs that need to be
    /// fetched via Bitswap. Unlike PushLog-initiated syncs, DocSync doesn't
    /// have collection_id or creator in the message, so we use empty strings.
    /// The merge handler will extract the actual metadata from the block data.
    ///
    /// # Arguments
    ///
    /// * `root_cid` - The head CID to fetch
    /// * `doc_id` - Document ID from the DocSyncItem
    pub fn register_docsync_dag(&self, root_cid: Cid, doc_id: String) {
        tracing::debug!(
            cid = %root_cid,
            doc_id = %doc_id,
            "Registering DocSync pending DAG"
        );

        let mut pending = self.pending_dags.write();
        pending.insert(
            root_cid,
            PendingDag {
                doc_id,
                // DocSync protocol doesn't include collection_id or creator.
                // The merge handler will extract these from the block data.
                collection_id: String::new(),
                creator: String::new(),
                missing: std::iter::once(root_cid).collect(),
            },
        );
    }

    /// Register a pending DAG for branchable collection sync.
    ///
    /// Unlike `register_docsync_dag` which stores the document ID,
    /// this stores the collection ID so the merge handler can look up
    /// the local collection for cross-schema-version merges.
    pub fn register_branchable_dag(&self, root_cid: Cid, collection_id: String) {
        tracing::debug!(
            cid = %root_cid,
            collection_id = %collection_id,
            "Registering branchable sync pending DAG"
        );

        let mut pending = self.pending_dags.write();
        pending.insert(
            root_cid,
            PendingDag {
                doc_id: String::new(),
                collection_id,
                creator: String::new(),
                missing: std::iter::once(root_cid).collect(),
            },
        );
    }

    /// Check if a block exists and is merged.
    pub async fn is_merged(&self, cid: &Cid) -> Result<bool> {
        self.blockstore
            .is_merged(cid)
            .await
            .map_err(|e| Error::BlockstoreError(e.to_string()))
    }

    /// Mark a block as merged (called by database layer after CRDT merge).
    pub async fn mark_as_merged(&self, cid: &Cid) -> Result<()> {
        self.blockstore
            .mark_as_merged(cid)
            .await
            .map_err(|e| Error::BlockstoreError(e.to_string()))
    }

    /// Get all unmerged block CIDs.
    pub async fn get_unmerged(&self) -> Result<Vec<Cid>> {
        self.blockstore
            .get_unmerged()
            .await
            .map_err(|e| Error::BlockstoreError(e.to_string()))
    }

    /// Get the blockstore reference.
    pub fn blockstore(&self) -> &Arc<B> {
        &self.blockstore
    }

    /// Store a block received via Bitswap and check if pending DAGs can now proceed.
    ///
    /// This is called when blocks are fetched via Bitswap during DAG synchronization.
    /// The block is stored in the blockstore, and we check if any pending DAGs are
    /// now complete and can be processed.
    ///
    /// Returns `true` if the block was stored (not a duplicate).
    pub async fn store_bitswap_block(&self, cid: &Cid, data: &[u8]) -> Result<bool> {
        // Check if we already have the block
        if self
            .blockstore
            .has(cid)
            .await
            .map_err(|e| Error::BlockstoreError(e.to_string()))?
        {
            tracing::debug!(
                cid = %cid,
                "Bitswap block already in blockstore (duplicate)"
            );
            return Ok(false);
        }

        // Store the block
        if let Err(e) = self.blockstore.put(cid, data).await {
            tracing::error!(
                cid = %cid,
                error = %e,
                "Failed to store Bitswap block"
            );
            return Err(Error::BlockstoreError(e.to_string()));
        }

        tracing::info!(
            cid = %cid,
            data_len = data.len(),
            "Stored Bitswap block in blockstore"
        );

        // Check if any pending DAGs can now proceed
        // This is done by checking which pending DAGs were waiting for this CID
        let pending = self.pending_dags.read().clone();
        for (root_cid, pending_info) in pending {
            if pending_info.missing.contains(cid) {
                tracing::debug!(
                    root_cid = %root_cid,
                    received_cid = %cid,
                    "Pending DAG received a missing block - will check completeness"
                );
            }
        }

        Ok(true)
    }

    /// Process a pending DAG after Bitswap blocks have been received.
    ///
    /// This is called when BitswapComplete is received, indicating all requested
    /// blocks have arrived. We re-check the DAG for any remaining missing links
    /// (recursively, at all depths) and process it if complete.
    pub async fn retry_pending_dag(&self, root_cid: &Cid) -> Result<bool> {
        // Get the pending DAG info
        let pending_info = {
            let pending = self.pending_dags.read();
            pending.get(root_cid).cloned()
        };

        let Some(info) = pending_info else {
            tracing::warn!(
                root_cid = %root_cid,
                "No pending DAG found for retry"
            );
            return Ok(false);
        };

        // Load the root block from blockstore
        let block_data = match self.blockstore.get(root_cid).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                tracing::error!(
                    root_cid = %root_cid,
                    "Root block not found in blockstore during retry"
                );
                return Err(Error::BlockstoreError("Root block not found".to_string()));
            }
            Err(e) => {
                tracing::error!(
                    root_cid = %root_cid,
                    error = %e,
                    "Failed to load root block from blockstore"
                );
                return Err(Error::BlockstoreError(e.to_string()));
            }
        };

        // Recursively check ALL missing links at every depth of the DAG.
        // This is critical for multi-level DAGs like Collection → Composite → LWW
        // where a single-level check would declare the DAG "ready" prematurely.
        let missing = match find_all_missing_links(self.blockstore.as_ref(), &block_data).await {
            Ok(missing) => missing,
            Err(e) => {
                tracing::error!(
                    root_cid = %root_cid,
                    error = %e,
                    "Failed to re-check missing links for pending DAG"
                );
                return Err(e);
            }
        };

        eprintln!(
            "[DAG-RETRY] root_cid={} doc_id={} missing_count={}",
            root_cid,
            info.doc_id,
            missing.len()
        );

        if !missing.is_empty() {
            eprintln!(
                "[DAG-RETRY] Still missing {} blocks: {:?}",
                missing.len(),
                missing.iter().map(|c| c.to_string()).collect::<Vec<_>>()
            );
            // Update the pending info with new missing CIDs
            self.pending_dags.write().insert(
                *root_cid,
                PendingDag {
                    missing: missing.into_iter().collect(),
                    ..info
                },
            );
            return Ok(false);
        }

        // DAG is complete at all depths - remove from pending and process
        self.pending_dags.write().remove(root_cid);
        eprintln!(
            "[DAG-RETRY] DAG complete! root_cid={} doc_id={} — emitting DagReady",
            root_cid, info.doc_id
        );

        // Emit event that DAG is ready for merge
        let _ = self
            .event_tx
            .send(SyncEvent::DagReady {
                root_cid: *root_cid,
                doc_id: info.doc_id.clone(),
                collection_id: info.collection_id.clone(),
                schema_version_id: info.creator.clone(),
            })
            .await;

        Ok(true)
    }
}
