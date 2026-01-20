// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Sync coordinator for DefraDB P2P synchronization.
//!
//! The coordinator ties together:
//! - P2P host for network communication
//! - SyncManager for block storage and merge tracking
//! - Broadcaster for publishing updates
//!
//! # Architecture
//!
//! ```text
//! Database Layer
//!       ↓
//! SyncCoordinator
//!       ├── Broadcaster (publish updates)
//!       ├── SyncManager (store blocks, emit events)
//!       └── Event loop (receive GossipSub messages)
//!       ↓
//! P2PHost (network)
//! ```
//!
//! # Usage
//!
//! ```ignore
//! // Create the coordinator
//! let (coordinator, mut events) = SyncCoordinator::new(
//!     host_handle.clone(),
//!     blockstore,
//!     SyncConfig::default(),
//! );
//!
//! // Subscribe to collections
//! coordinator.subscribe_collection("users").await?;
//!
//! // Process host events in a task
//! tokio::spawn(async move {
//!     while let Some(event) = host_events.recv().await {
//!         coordinator.handle_host_event(event).await;
//!     }
//! });
//!
//! // Handle sync events in the database layer
//! while let Some(event) = events.recv().await {
//!     match event {
//!         SyncEvent::BlockReceived { cid, .. } => {
//!             // Do CRDT merge
//!             db.merge(&cid).await?;
//!             coordinator.mark_as_merged(&cid).await?;
//!         }
//!         _ => {}
//!     }
//! }
//! ```

use std::sync::Arc;
use tokio::sync::mpsc;

use blockstore::Blockstore;
use cid::Cid;
use libp2p::PeerId;

use crate::bitswap::{AccessMode, ReplicatorRegistry};
use crate::error::{Error, Result};
use crate::host::{HostEvent, P2PHostHandle};
use crate::message::{PushLogBroadcast, PushLogReply};
use crate::replicator::ReplicatorInfo;

/// Result of setting a replicator with auto-subscribe.
#[derive(Debug, Clone)]
pub struct SetReplicatorResult {
    /// Collections that were successfully subscribed.
    pub subscribed: Vec<String>,
    /// Collections that failed to subscribe (with error messages).
    pub failed_subscriptions: Vec<(String, String)>,
}

impl SetReplicatorResult {
    /// Returns true if all subscriptions succeeded.
    pub fn all_subscribed(&self) -> bool {
        self.failed_subscriptions.is_empty()
    }

    /// Returns true if any subscription failed.
    pub fn has_failures(&self) -> bool {
        !self.failed_subscriptions.is_empty()
    }
}

/// Result of loading multiple replicators.
#[derive(Debug, Clone, Default)]
pub struct LoadReplicatorsResult {
    /// Number of replicators successfully loaded.
    pub loaded: usize,
    /// Peer IDs that were skipped due to invalid format.
    pub skipped_invalid_ids: Vec<String>,
    /// Peer IDs that failed to load with error messages.
    pub failed: Vec<(String, String)>,
    /// Collections that failed to subscribe (across all replicators).
    pub failed_subscriptions: Vec<(String, String)>,
}

use super::broadcaster::Broadcaster;
use super::manager::{SyncConfig, SyncEvent, SyncManager};
use super::peer_state::PeerStateTracker;

/// Coordinator for P2P synchronization.
///
/// This is the main integration point between the P2P layer and the database.
pub struct SyncCoordinator<B: Blockstore> {
    /// Host handle for sending responses
    host: P2PHostHandle,

    /// Broadcaster for publishing updates
    broadcaster: Broadcaster,

    /// Sync manager for block storage
    manager: SyncManager<B>,

    /// Peer state tracker
    peer_state: Arc<PeerStateTracker>,

    /// Local peer ID (for creator field in broadcasts)
    local_peer_id: String,

    /// Access control mode
    access_mode: AccessMode,

    /// Replicator registry for access control checks
    replicators: Arc<ReplicatorRegistry>,
}

impl<B: Blockstore + 'static> SyncCoordinator<B> {
    /// Create a new sync coordinator with default Open access mode.
    ///
    /// Returns the coordinator and a receiver for sync events.
    ///
    /// This constructor creates the coordinator without access control
    /// (AccessMode::Open). Use `with_access_control` for production deployments
    /// where access control is required.
    pub async fn new(
        host: P2PHostHandle,
        blockstore: Arc<B>,
        config: SyncConfig,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        Self::with_access_control(
            host,
            blockstore,
            config,
            AccessMode::Open,
            Arc::new(ReplicatorRegistry::new()),
        )
        .await
    }

    /// Create a new sync coordinator with access control.
    ///
    /// Returns the coordinator and a receiver for sync events.
    ///
    /// # Arguments
    ///
    /// * `host` - Handle to the P2P host
    /// * `blockstore` - Shared blockstore for storing blocks
    /// * `config` - Sync configuration
    /// * `access_mode` - Access control mode (Open or Controlled)
    /// * `replicators` - Registry of authorized replicator peers
    ///
    /// When `access_mode` is `AccessMode::Controlled`, incoming PushLog requests
    /// and GossipSub messages are checked against the replicator registry. Only
    /// peers registered as replicators for the collection can sync documents.
    pub async fn with_access_control(
        host: P2PHostHandle,
        blockstore: Arc<B>,
        config: SyncConfig,
        access_mode: AccessMode,
        replicators: Arc<ReplicatorRegistry>,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        let local_peer_id = host.local_peer_id().await?.to_string();
        let broadcaster = Broadcaster::new(host.clone());
        let peer_state = Arc::new(PeerStateTracker::new());
        let (manager, events) = SyncManager::new(blockstore, peer_state.clone(), config);

        Ok((
            Self {
                host,
                broadcaster,
                manager,
                peer_state,
                local_peer_id,
                access_mode,
                replicators,
            },
            events,
        ))
    }

    /// Check if a peer has access to sync a collection.
    ///
    /// Returns `Ok(())` if access is granted, or `Err(Error::AccessDenied)` if denied.
    ///
    /// Access rules:
    /// 1. If mode is Open → allow all
    /// 2. If peer is a replicator for the collection → allow
    /// 3. Otherwise → deny
    fn check_access(&self, peer_id: &PeerId, collection_id: &str) -> Result<()> {
        // Fast path: Open mode allows all access
        if self.access_mode.is_open() {
            return Ok(());
        }

        // Check if peer is a replicator for this collection
        if self.replicators.is_replicator(collection_id, peer_id) {
            return Ok(());
        }

        // Check if peer is a replicator for any collection (more permissive fallback)
        // This handles the case where the peer might be syncing across collections
        if self.replicators.is_any_replicator(peer_id) {
            tracing::debug!(
                peer_id = %peer_id,
                collection_id = %collection_id,
                "Peer is replicator for other collections, allowing access"
            );
            return Ok(());
        }

        // Access denied
        tracing::warn!(
            peer_id = %peer_id,
            collection_id = %collection_id,
            "Access denied: peer is not a replicator for this collection"
        );
        Err(Error::AccessDenied {
            peer_id: peer_id.to_string(),
            collection_id: collection_id.to_string(),
        })
    }

    /// Get the current access mode.
    pub fn access_mode(&self) -> AccessMode {
        self.access_mode
    }

    /// Get the replicator registry.
    pub fn replicators(&self) -> &Arc<ReplicatorRegistry> {
        &self.replicators
    }

    /// Handle an event from the P2P host.
    ///
    /// This should be called from the event loop that processes HostEvents.
    pub async fn handle_host_event(&self, event: HostEvent) -> Result<()> {
        match event {
            HostEvent::PeerConnected(peer_id) => {
                tracing::debug!(peer_id = %peer_id, "Peer connected");
                self.peer_state.peer_connected(peer_id);
            }
            HostEvent::PeerDisconnected(peer_id) => {
                tracing::debug!(peer_id = %peer_id, "Peer disconnected");
                self.peer_state.peer_disconnected(&peer_id);
            }
            HostEvent::PeerSubscribed { peer_id, topic } => {
                tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer subscribed to topic");
                self.peer_state.peer_subscribed(&peer_id, topic);
            }
            HostEvent::PeerUnsubscribed { peer_id, topic } => {
                tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer unsubscribed from topic");
                self.peer_state.peer_unsubscribed(&peer_id, &topic);
            }
            HostEvent::GossipMessage {
                propagation_source,
                message,
                topic,
                ..
            } => {
                tracing::debug!(
                    doc_id = %message.doc_id,
                    collection_id = %message.collection_id,
                    topic = %topic,
                    "Received GossipSub message"
                );

                // Access control check
                if let Err(e) = self.check_access(&propagation_source, &message.collection_id) {
                    tracing::warn!(
                        peer_id = %propagation_source,
                        collection_id = %message.collection_id,
                        doc_id = %message.doc_id,
                        "Dropping GossipSub message from unauthorized peer"
                    );
                    return Err(e);
                }

                // Parse CID - if invalid, return error early (don't call process_pushlog
                // which will also fail with the same invalid CID)
                match Cid::try_from(message.cid.as_slice()) {
                    Ok(cid) => {
                        self.peer_state.peer_has_cid(&propagation_source, cid);
                    }
                    Err(e) => {
                        tracing::warn!(
                            peer_id = %propagation_source,
                            cid_bytes_len = message.cid.len(),
                            error = %e,
                            "Failed to parse CID from gossip message - skipping message"
                        );
                        return Err(crate::error::Error::InvalidCid(format!(
                            "Failed to parse CID from gossip message: {}",
                            e
                        )));
                    }
                }

                self.manager.process_pushlog(&message).await?;
            }
            HostEvent::PushLogRequest {
                peer_id,
                request,
                channel,
            } => {
                tracing::debug!(
                    peer_id = %peer_id,
                    doc_id = %request.doc_id,
                    "Received PushLog request"
                );

                // Access control check
                if let Err(e) = self.check_access(&peer_id, &request.collection_id) {
                    tracing::warn!(
                        peer_id = %peer_id,
                        collection_id = %request.collection_id,
                        doc_id = %request.doc_id,
                        "Rejecting PushLog request from unauthorized peer"
                    );
                    let reply = PushLogReply::error(
                        &request.metadata.message_id,
                        &format!("access denied: not authorized for collection {}", request.collection_id),
                    );
                    if let Err(send_err) = self.host.send_pushlog_response(channel, reply).await {
                        tracing::warn!(
                            peer_id = %peer_id,
                            error = %send_err,
                            "Failed to send access denied response"
                        );
                    }
                    return Err(e);
                }

                // Parse CID - if invalid, send error response and return early
                let cid = match Cid::try_from(request.cid.as_slice()) {
                    Ok(cid) => {
                        self.peer_state.peer_has_cid(&peer_id, cid);
                        cid
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to parse CID: {}", e);
                        tracing::warn!(
                            peer_id = %peer_id,
                            cid_bytes_len = request.cid.len(),
                            error = %e,
                            "Failed to parse CID from PushLog request - sending error response"
                        );
                        let reply = PushLogReply::error(&request.metadata.message_id, &error_msg);
                        if let Err(send_err) = self.host.send_pushlog_response(channel, reply).await
                        {
                            tracing::warn!(
                                peer_id = %peer_id,
                                error = %send_err,
                                "Failed to send error response for invalid CID"
                            );
                        }
                        return Err(crate::error::Error::InvalidCid(error_msg));
                    }
                };

                // Log that we have a valid CID
                tracing::trace!(?cid, "Parsed valid CID from PushLog request");

                // Convert request to broadcast format and process
                let broadcast = PushLogBroadcast::from_request(&request);
                let process_result = self.manager.process_pushlog(&broadcast).await;

                // Send response based on processing result
                let reply = match &process_result {
                    Ok(()) => PushLogReply::success(&request.metadata.message_id),
                    Err(e) => PushLogReply::error(&request.metadata.message_id, &e.to_string()),
                };

                if let Err(e) = self.host.send_pushlog_response(channel, reply).await {
                    tracing::warn!(
                        peer_id = %peer_id,
                        doc_id = %request.doc_id,
                        error = %e,
                        "Failed to send PushLog response"
                    );
                } else {
                    tracing::trace!(
                        peer_id = %peer_id,
                        doc_id = %request.doc_id,
                        "Sent PushLog response"
                    );
                }

                // Propagate the processing error if there was one
                process_result?;
            }
            other => {
                // Other events (peer discovery, listening, etc.) don't need sync handling
                tracing::trace!(event = ?other, "Ignoring non-sync host event");
            }
        }
        Ok(())
    }

    /// Subscribe to a collection for sync.
    ///
    /// After subscribing, updates to any document in the collection will be
    /// received and processed.
    pub async fn subscribe_collection(&self, collection_id: &str) -> Result<bool> {
        self.broadcaster.subscribe_collection(collection_id).await
    }

    /// Subscribe to a specific document for sync.
    pub async fn subscribe_document(&self, doc_id: &str) -> Result<bool> {
        self.broadcaster.subscribe_document(doc_id).await
    }

    /// Unsubscribe from a collection.
    pub async fn unsubscribe_collection(&self, collection_id: &str) -> Result<bool> {
        self.broadcaster.unsubscribe_collection(collection_id).await
    }

    /// Unsubscribe from a document.
    pub async fn unsubscribe_document(&self, doc_id: &str) -> Result<bool> {
        self.broadcaster.unsubscribe_document(doc_id).await
    }

    /// Broadcast a local update to the network.
    ///
    /// Call this after successfully creating a local block to propagate it
    /// to other nodes.
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the block
    /// * `block` - The raw block data
    /// * `doc_id` - The document ID
    /// * `collection_id` - The collection ID
    ///
    /// # Returns
    ///
    /// Returns `Ok(BroadcastResult)` indicating success or partial success.
    /// Partial success means one topic received the message but not both.
    pub async fn broadcast_local_update(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) -> Result<super::BroadcastResult> {
        let broadcast =
            Broadcaster::create_broadcast(cid, block, doc_id, collection_id, &self.local_peer_id);
        self.broadcaster.broadcast_update(&broadcast).await
    }

    /// Mark a block as merged.
    ///
    /// Call this after successfully completing the CRDT merge for a block.
    pub async fn mark_as_merged(&self, cid: &Cid) -> Result<()> {
        self.manager.mark_as_merged(cid).await
    }

    /// Check if a block is merged.
    pub async fn is_merged(&self, cid: &Cid) -> Result<bool> {
        self.manager.is_merged(cid).await
    }

    /// Get all unmerged block CIDs.
    ///
    /// Useful for startup recovery - process any blocks that were stored
    /// but not yet merged.
    pub async fn get_unmerged(&self) -> Result<Vec<Cid>> {
        self.manager.get_unmerged().await
    }

    /// Get the blockstore reference.
    pub fn blockstore(&self) -> &Arc<B> {
        self.manager.blockstore()
    }

    /// Get the broadcaster reference.
    pub fn broadcaster(&self) -> &Broadcaster {
        &self.broadcaster
    }

    /// Get the local peer ID.
    pub fn local_peer_id(&self) -> &str {
        &self.local_peer_id
    }

    /// Get the peer state tracker reference.
    pub fn peer_state(&self) -> &PeerStateTracker {
        &self.peer_state
    }

    /// Get the host handle for direct peer communication.
    pub fn host(&self) -> &P2PHostHandle {
        &self.host
    }

    /// Get the sync manager reference.
    pub fn manager(&self) -> &SyncManager<B> {
        &self.manager
    }

    // Note: The request_block and request_block_from_any_peer methods were removed.
    // They didn't interoperate with Go DefraDB (which uses Bitswap).
    //
    // For block fetching, use the DagSync module with Bitswap:
    //   - DagSync::prepare_sync() to identify missing blocks
    //   - behaviour.bitswap_sync() to fetch via Bitswap protocol

    // === Replicator Management ===

    /// Set (add/update) a replicator for the specified collections.
    ///
    /// This adds the peer to the replicator registry and optionally auto-subscribes
    /// to the collection topics so we can sync with them.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - The peer ID of the replicator
    /// * `collections` - Collections this peer should replicate
    /// * `auto_subscribe` - Whether to auto-subscribe to the collection topics
    ///
    /// # Returns
    ///
    /// Returns `Ok(SetReplicatorResult)` with details about subscription status.
    /// The replicator is registered even if some subscriptions fail.
    pub async fn set_replicator(
        &self,
        peer_id: PeerId,
        collections: Vec<String>,
        auto_subscribe: bool,
    ) -> Result<SetReplicatorResult> {
        // Update the registry via host command
        self.host
            .set_replicator(peer_id, collections.clone())
            .await?;

        let mut result = SetReplicatorResult {
            subscribed: Vec::new(),
            failed_subscriptions: Vec::new(),
        };

        // Auto-subscribe to collection topics so we receive updates
        if auto_subscribe {
            for collection_id in &collections {
                match self.subscribe_collection(collection_id).await {
                    Ok(_) => {
                        result.subscribed.push(collection_id.clone());
                    }
                    Err(e) => {
                        tracing::warn!(
                            collection_id = %collection_id,
                            error = %e,
                            "Failed to auto-subscribe to collection for replicator"
                        );
                        result
                            .failed_subscriptions
                            .push((collection_id.clone(), e.to_string()));
                    }
                }
            }
        }

        if result.has_failures() {
            tracing::warn!(
                peer_id = %peer_id,
                subscribed = ?result.subscribed,
                failed = ?result.failed_subscriptions,
                "Set replicator with subscription failures"
            );
        } else {
            tracing::info!(
                peer_id = %peer_id,
                collections = ?collections,
                "Set replicator"
            );
        }

        Ok(result)
    }

    /// Delete a replicator.
    ///
    /// Removes the peer from the replicator registry.
    /// Does not unsubscribe from collections (other peers may still be replicating).
    pub async fn delete_replicator(&self, peer_id: PeerId) -> Result<()> {
        self.host.delete_replicator(peer_id).await?;
        tracing::info!(peer_id = %peer_id, "Deleted replicator");
        Ok(())
    }

    /// Get all registered replicators.
    pub async fn get_all_replicators(&self) -> Result<Vec<ReplicatorInfo>> {
        self.host.get_all_replicators().await
    }

    /// Get replicator info for a specific peer.
    ///
    /// Returns None if the peer is not a replicator.
    pub async fn get_replicator(&self, peer_id: PeerId) -> Result<Option<ReplicatorInfo>> {
        self.host.get_replicator(peer_id).await
    }

    /// Load replicators from stored ReplicatorInfo records.
    ///
    /// This is typically called during startup to restore replicator state
    /// from persistent storage.
    ///
    /// # Arguments
    ///
    /// * `infos` - ReplicatorInfo records loaded from storage
    /// * `auto_subscribe` - Whether to auto-subscribe to collection topics
    ///
    /// # Returns
    ///
    /// Returns a `LoadReplicatorsResult` with details about what was loaded
    /// and any failures that occurred. Unlike individual `set_replicator` calls,
    /// this method continues loading remaining replicators even if some fail.
    pub async fn load_replicators(
        &self,
        infos: &[ReplicatorInfo],
        auto_subscribe: bool,
    ) -> LoadReplicatorsResult {
        let mut result = LoadReplicatorsResult::default();

        for info in infos {
            if let Some(peer_id) = info.peer_id() {
                match self
                    .set_replicator(peer_id, info.collections.clone(), auto_subscribe)
                    .await
                {
                    Ok(set_result) => {
                        result.loaded += 1;
                        // Collect any subscription failures
                        result
                            .failed_subscriptions
                            .extend(set_result.failed_subscriptions);
                    }
                    Err(e) => {
                        tracing::error!(
                            peer_id = %peer_id,
                            error = %e,
                            "Failed to load replicator"
                        );
                        result.failed.push((peer_id.to_string(), e.to_string()));
                    }
                }
            } else {
                tracing::warn!(
                    peer_id_str = %info.peer_id_str(),
                    "Skipping replicator with invalid peer ID"
                );
                result
                    .skipped_invalid_ids
                    .push(info.peer_id_str().to_string());
            }
        }

        if result.failed.is_empty() && result.skipped_invalid_ids.is_empty() {
            tracing::info!(
                loaded = result.loaded,
                auto_subscribe = auto_subscribe,
                "Loaded replicators from storage"
            );
        } else {
            tracing::warn!(
                loaded = result.loaded,
                skipped = result.skipped_invalid_ids.len(),
                failed = result.failed.len(),
                failed_subscriptions = result.failed_subscriptions.len(),
                auto_subscribe = auto_subscribe,
                "Loaded replicators from storage with some failures"
            );
        }

        result
    }
}

#[cfg(test)]
mod tests {
    // Integration tests are in tests/integration.rs
    // Unit tests for individual components are in their respective modules
}
