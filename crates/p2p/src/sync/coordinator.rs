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

use crate::error::Result;
use crate::host::{HostEvent, P2PHostHandle};
use crate::message::{PushLogBroadcast, PushLogReply};

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
    peer_state: PeerStateTracker,

    /// Local peer ID (for creator field in broadcasts)
    local_peer_id: String,
}

impl<B: Blockstore + 'static> SyncCoordinator<B> {
    /// Create a new sync coordinator.
    ///
    /// Returns the coordinator and a receiver for sync events.
    pub async fn new(
        host: P2PHostHandle,
        blockstore: Arc<B>,
        config: SyncConfig,
    ) -> Result<(Self, mpsc::Receiver<SyncEvent>)> {
        let local_peer_id = host.local_peer_id().await?.to_string();
        let broadcaster = Broadcaster::new(host.clone());
        let (manager, events) = SyncManager::new(blockstore, config);
        let peer_state = PeerStateTracker::new();

        Ok((
            Self {
                host,
                broadcaster,
                manager,
                peer_state,
                local_peer_id,
            },
            events,
        ))
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

                // Parse CID and track that the peer has this block
                match Cid::try_from(message.cid.as_slice()) {
                    Ok(cid) => {
                        self.peer_state.peer_has_cid(&propagation_source, cid);
                    }
                    Err(e) => {
                        tracing::warn!(
                            peer_id = %propagation_source,
                            cid_bytes_len = message.cid.len(),
                            error = %e,
                            "Failed to parse CID from gossip message"
                        );
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

                // Parse CID and track that the peer has this block
                match Cid::try_from(request.cid.as_slice()) {
                    Ok(cid) => {
                        self.peer_state.peer_has_cid(&peer_id, cid);
                    }
                    Err(e) => {
                        tracing::warn!(
                            peer_id = %peer_id,
                            cid_bytes_len = request.cid.len(),
                            error = %e,
                            "Failed to parse CID from PushLog request"
                        );
                    }
                }

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

    // Note: The request_block and request_block_from_any_peer methods were removed.
    // They didn't interoperate with Go DefraDB (which uses Bitswap).
    //
    // For block fetching, use the DagSync module with Bitswap:
    //   - DagSync::prepare_sync() to identify missing blocks
    //   - behaviour.bitswap_sync() to fetch via Bitswap protocol
}

#[cfg(test)]
mod tests {
    // Integration tests are in tests/integration.rs
    // Unit tests for individual components are in their respective modules
}
