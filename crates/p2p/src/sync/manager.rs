// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

use cid::Cid;
use std::sync::Arc;
use tokio::sync::mpsc;

use blockstore::Blockstore;

use crate::error::{Error, Result};
use crate::message::PushLogBroadcast;

use super::queue::ProcessQueue;

/// Configuration for the SyncManager.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Size of the event channel buffer.
    pub event_buffer_size: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            event_buffer_size: 256,
        }
    }
}

/// Events emitted by the SyncManager for higher layers to process.
#[derive(Debug, Clone)]
pub enum SyncEvent {
    /// A new block was received and stored, needs CRDT merge.
    ///
    /// The database layer should process this by:
    /// 1. Loading the block from blockstore
    /// 2. Applying CRDT merge
    /// 3. Calling blockstore.mark_as_merged()
    BlockReceived {
        /// The CID of the received block
        cid: Cid,
        /// Document ID this block belongs to
        doc_id: String,
        /// Collection ID
        collection_id: String,
        /// Creator peer ID
        creator: String,
    },

    /// A block was already merged (received duplicate).
    BlockAlreadyMerged { cid: Cid },

    /// Failed to process a sync request.
    SyncError { cid: Cid, error: String },
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
/// // Create sync manager
/// let (manager, mut events) = SyncManager::new(blockstore, SyncConfig::default());
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
}

impl<B: Blockstore + 'static> SyncManager<B> {
    /// Create a new SyncManager.
    ///
    /// Returns the manager and a receiver for sync events.
    pub fn new(blockstore: Arc<B>, config: SyncConfig) -> (Self, mpsc::Receiver<SyncEvent>) {
        let (event_tx, event_rx) = mpsc::channel(config.event_buffer_size);

        let manager = Self {
            blockstore,
            process_queue: ProcessQueue::new(),
            event_tx,
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

        tracing::info!(
            ?cid,
            doc_id = %msg.doc_id,
            collection_id = %msg.collection_id,
            "Block stored, emitting BlockReceived event"
        );

        // Emit event for database layer to merge.
        // This is critical - if the event can't be sent, the block will be stored
        // but never merged, leading to data inconsistency.
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

        Ok(())
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockstore::DefraBlockstore;
    use std::str::FromStr;
    use storage::backends::MemoryStore;

    fn test_cid() -> Cid {
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
    }

    fn create_test_broadcast(cid: &Cid) -> PushLogBroadcast {
        PushLogBroadcast::new(
            "doc123".to_string(),
            cid.to_bytes(),
            "collection1".to_string(),
            "creator1".to_string(),
            b"block data".to_vec(),
        )
    }

    #[tokio::test]
    async fn test_process_pushlog_stores_block() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (manager, mut events) = SyncManager::new(blockstore.clone(), SyncConfig::default());

        let cid = test_cid();
        let msg = create_test_broadcast(&cid);

        // Process the pushlog
        manager.process_pushlog(&msg).await.unwrap();

        // Block should be stored
        assert!(blockstore.has(&cid).await.unwrap());

        // Should not be merged yet
        assert!(!blockstore.is_merged(&cid).await.unwrap());

        // Should receive BlockReceived event
        let event = events.try_recv().unwrap();
        match event {
            SyncEvent::BlockReceived {
                cid: event_cid,
                doc_id,
                ..
            } => {
                assert_eq!(event_cid, cid);
                assert_eq!(doc_id, "doc123");
            }
            _ => panic!("Expected BlockReceived event"),
        }
    }

    #[tokio::test]
    async fn test_process_pushlog_already_merged() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (manager, mut events) = SyncManager::new(blockstore.clone(), SyncConfig::default());

        let cid = test_cid();
        let msg = create_test_broadcast(&cid);

        // Pre-store and merge the block
        blockstore.put(&cid, &msg.block).await.unwrap();
        blockstore.mark_as_merged(&cid).await.unwrap();

        // Process the pushlog
        manager.process_pushlog(&msg).await.unwrap();

        // Should receive BlockAlreadyMerged event
        let event = events.try_recv().unwrap();
        match event {
            SyncEvent::BlockAlreadyMerged { cid: event_cid } => {
                assert_eq!(event_cid, cid);
            }
            _ => panic!("Expected BlockAlreadyMerged event"),
        }
    }

    #[tokio::test]
    async fn test_mark_as_merged() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (manager, _events) = SyncManager::new(blockstore.clone(), SyncConfig::default());

        let cid = test_cid();
        let msg = create_test_broadcast(&cid);

        // Process the pushlog
        manager.process_pushlog(&msg).await.unwrap();

        // Not merged initially
        assert!(!manager.is_merged(&cid).await.unwrap());

        // Mark as merged
        manager.mark_as_merged(&cid).await.unwrap();

        // Now merged
        assert!(manager.is_merged(&cid).await.unwrap());
    }

    #[tokio::test]
    async fn test_get_unmerged() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (manager, _events) = SyncManager::new(blockstore.clone(), SyncConfig::default());

        let cid = test_cid();
        let msg = create_test_broadcast(&cid);

        // Initially no unmerged
        let unmerged = manager.get_unmerged().await.unwrap();
        assert!(unmerged.is_empty());

        // Process pushlog
        manager.process_pushlog(&msg).await.unwrap();

        // Now one unmerged
        let unmerged = manager.get_unmerged().await.unwrap();
        assert_eq!(unmerged.len(), 1);
        assert!(unmerged.contains(&cid));

        // Mark as merged
        manager.mark_as_merged(&cid).await.unwrap();

        // Now none unmerged
        let unmerged = manager.get_unmerged().await.unwrap();
        assert!(unmerged.is_empty());
    }

    #[tokio::test]
    async fn test_process_pushlog_invalid_cid_returns_error() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (manager, _events) = SyncManager::new(blockstore, SyncConfig::default());

        // Create a broadcast with invalid CID bytes
        let msg = PushLogBroadcast::new(
            "doc123".to_string(),
            vec![0xFF, 0xFF, 0xFF], // Invalid CID bytes
            "collection1".to_string(),
            "creator1".to_string(),
            b"block data".to_vec(),
        );

        // Processing should fail with InvalidCid error
        let result = manager.process_pushlog(&msg).await;
        assert!(result.is_err());
        match result {
            Err(Error::InvalidCid(msg)) => {
                assert!(msg.contains("Failed to parse CID"));
            }
            other => panic!("Expected InvalidCid error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_concurrent_processing_second_waiter_processes_on_first_not_merged() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;

        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (manager, mut events) = SyncManager::new(blockstore.clone(), SyncConfig::default());
        let manager = Arc::new(manager);

        let cid = test_cid();
        let msg = create_test_broadcast(&cid);

        // Flag to track if first processor completed
        let first_done = Arc::new(AtomicBool::new(false));

        // First task: acquire lock, store block, but DON'T mark as merged
        let manager1 = manager.clone();
        let msg1 = msg.clone();
        let first_done1 = first_done.clone();
        let first_task = tokio::spawn(async move {
            manager1.process_pushlog(&msg1).await.unwrap();
            first_done1.store(true, Ordering::SeqCst);
        });

        // Give first task time to acquire the lock
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Second task: should wait for first, then also process (since not merged)
        let manager2 = manager.clone();
        let msg2 = msg.clone();
        let second_task = tokio::spawn(async move {
            manager2.process_pushlog(&msg2).await.unwrap();
        });

        // Wait for both tasks
        first_task.await.unwrap();
        second_task.await.unwrap();

        // Block should be stored
        assert!(blockstore.has(&cid).await.unwrap());

        // We should get at least one BlockReceived event
        // (could get two if second waiter also processes before checking merge status)
        let mut received_count = 0;
        while let Ok(event) = events.try_recv() {
            match event {
                SyncEvent::BlockReceived { .. } => received_count += 1,
                SyncEvent::BlockAlreadyMerged { .. } => {} // Also valid
                _ => {}
            }
        }
        assert!(
            received_count >= 1,
            "Should have at least one BlockReceived event"
        );
    }

    #[tokio::test]
    async fn test_process_pushlog_returns_error_when_receiver_dropped() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (manager, events) = SyncManager::new(blockstore.clone(), SyncConfig::default());

        // Drop the event receiver immediately
        drop(events);

        let cid = test_cid();
        let msg = create_test_broadcast(&cid);

        // Processing should fail with ChannelSend error since receiver is dropped
        let result = manager.process_pushlog(&msg).await;
        assert!(result.is_err());
        match result {
            Err(Error::ChannelSend) => {
                // Expected - channel send failed because receiver was dropped
            }
            other => panic!("Expected ChannelSend error, got {:?}", other),
        }

        // Block should still be stored (we store before sending event)
        assert!(blockstore.has(&cid).await.unwrap());
    }

    #[tokio::test]
    async fn test_already_merged_returns_error_when_receiver_dropped() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (manager, events) = SyncManager::new(blockstore.clone(), SyncConfig::default());

        let cid = test_cid();
        let msg = create_test_broadcast(&cid);

        // Pre-store and merge the block
        blockstore.put(&cid, &msg.block).await.unwrap();
        blockstore.mark_as_merged(&cid).await.unwrap();

        // Drop the event receiver
        drop(events);

        // Processing already-merged block should fail since we can't send event
        let result = manager.process_pushlog(&msg).await;
        assert!(result.is_err());
        match result {
            Err(Error::ChannelSend) => {
                // Expected - can't send BlockAlreadyMerged event
            }
            other => panic!("Expected ChannelSend error, got {:?}", other),
        }
    }
}
