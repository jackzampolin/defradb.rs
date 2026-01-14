// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Replication loop for processing sync events and executing CRDT merges.
//!
//! The replication loop is the bridge between the P2P layer and the database.
//! It consumes SyncEvents, loads blocks from the blockstore, delegates merge
//! operations to the database layer, and marks blocks as merged.
//!
//! # Architecture
//!
//! ```text
//! SyncManager emits SyncEvent::BlockReceived
//!         ↓
//! ReplicationLoop receives event
//!         ↓
//! Load block from blockstore
//!         ↓
//! MergeHandler::handle_block() [database layer]
//!         ↓
//! SyncCoordinator::mark_as_merged()
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use blockstore::Blockstore;
use cid::Cid;
use tokio::sync::mpsc;

use super::coordinator::SyncCoordinator;
use super::manager::SyncEvent;

/// Handler for merging incoming blocks.
///
/// Implement this trait in the database layer to handle CRDT merges.
/// The P2P layer calls this when a new block is received.
#[async_trait]
pub trait MergeHandler: Send + Sync {
    /// Error type for merge operations
    type Error: std::error::Error + Send + Sync + 'static;

    /// Handle an incoming block.
    ///
    /// This method should:
    /// 1. Decode the block data into a CRDT delta
    /// 2. Load/create the appropriate CRDT for the document
    /// 3. Execute the merge within a transaction
    /// 4. Return Ok(true) if merge was successful, Ok(false) if skipped
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the block
    /// * `block_data` - The raw block data
    /// * `doc_id` - The document this block belongs to
    /// * `collection_id` - The collection ID
    /// * `creator` - The peer that created this block
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Block was merged successfully
    /// * `Ok(false)` - Block was skipped (already applied, rejected by CRDT)
    /// * `Err(e)` - Merge failed
    async fn handle_block(
        &self,
        cid: &Cid,
        block_data: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator: &str,
    ) -> Result<bool, Self::Error>;
}

/// Result of a replication loop iteration.
#[derive(Debug, Clone)]
pub enum ReplicationResult {
    /// Block was merged successfully
    Merged { cid: Cid, doc_id: String },
    /// Block was skipped (already applied or rejected)
    Skipped { cid: Cid, reason: String },
    /// Merge failed
    Failed { cid: Cid, error: String },
    /// Event channel closed
    ChannelClosed,
}

/// Configuration for the replication loop.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Whether to continue on merge errors or stop the loop
    pub continue_on_error: bool,
    /// Whether to re-broadcast successfully merged blocks
    pub rebroadcast_on_merge: bool,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            continue_on_error: true,
            rebroadcast_on_merge: false,
        }
    }
}

/// Replication loop that processes sync events.
///
/// # Usage
///
/// ```ignore
/// // Create coordinator and get event receiver
/// let (coordinator, events) = SyncCoordinator::new(host, blockstore, config).await?;
///
/// // Create merge handler (database layer)
/// let handler = MyMergeHandler::new(db);
///
/// // Run the replication loop
/// let config = ReplicationConfig::default();
/// ReplicationLoop::run(coordinator, events, handler, config).await;
/// ```
pub struct ReplicationLoop;

impl ReplicationLoop {
    /// Run the replication loop.
    ///
    /// This method runs until the event channel is closed or a fatal error occurs.
    /// It processes SyncEvents, delegates merges to the handler, and marks blocks
    /// as merged.
    pub async fn run<B, H>(
        coordinator: Arc<SyncCoordinator<B>>,
        mut events: mpsc::Receiver<SyncEvent>,
        handler: Arc<H>,
        config: ReplicationConfig,
    ) where
        B: Blockstore + 'static,
        H: MergeHandler + 'static,
    {
        tracing::info!("Starting replication loop");

        loop {
            let result =
                Self::process_next(&coordinator, &mut events, handler.as_ref(), &config).await;

            match &result {
                ReplicationResult::Merged { cid, doc_id } => {
                    tracing::info!(cid = %cid, doc_id = %doc_id, "Block merged successfully");
                }
                ReplicationResult::Skipped { cid, reason } => {
                    tracing::debug!(cid = %cid, reason = %reason, "Block skipped");
                }
                ReplicationResult::Failed { cid, error } => {
                    tracing::error!(cid = %cid, error = %error, "Block merge failed");
                    if !config.continue_on_error {
                        tracing::error!("Stopping replication loop due to error");
                        break;
                    }
                }
                ReplicationResult::ChannelClosed => {
                    tracing::info!("Event channel closed, stopping replication loop");
                    break;
                }
            }
        }

        tracing::info!("Replication loop stopped");
    }

    /// Process the next sync event.
    async fn process_next<B, H>(
        coordinator: &SyncCoordinator<B>,
        events: &mut mpsc::Receiver<SyncEvent>,
        handler: &H,
        config: &ReplicationConfig,
    ) -> ReplicationResult
    where
        B: Blockstore + 'static,
        H: MergeHandler + ?Sized + 'static,
    {
        let event = match events.recv().await {
            Some(e) => e,
            None => return ReplicationResult::ChannelClosed,
        };

        match event {
            SyncEvent::BlockReceived {
                cid,
                doc_id,
                collection_id,
                creator,
            } => {
                Self::handle_block_received(
                    coordinator,
                    handler,
                    config,
                    cid,
                    &doc_id,
                    &collection_id,
                    &creator,
                )
                .await
            }
            SyncEvent::BlockAlreadyMerged { cid } => ReplicationResult::Skipped {
                cid,
                reason: "already merged".to_string(),
            },
            SyncEvent::SyncError { cid, error } => ReplicationResult::Failed { cid, error },
        }
    }

    /// Handle a BlockReceived event.
    async fn handle_block_received<B, H>(
        coordinator: &SyncCoordinator<B>,
        handler: &H,
        config: &ReplicationConfig,
        cid: Cid,
        doc_id: &str,
        collection_id: &str,
        creator: &str,
    ) -> ReplicationResult
    where
        B: Blockstore + 'static,
        H: MergeHandler + ?Sized + 'static,
    {
        // Load block from blockstore
        let block_data = match coordinator.blockstore().get(&cid).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                return ReplicationResult::Failed {
                    cid,
                    error: "Block not found in blockstore".to_string(),
                }
            }
            Err(e) => {
                return ReplicationResult::Failed {
                    cid,
                    error: format!("Failed to load block: {}", e),
                }
            }
        };

        // Delegate merge to handler
        match handler
            .handle_block(&cid, &block_data, doc_id, collection_id, creator)
            .await
        {
            Ok(true) => {
                // Merge successful - mark as merged
                if let Err(e) = coordinator.mark_as_merged(&cid).await {
                    tracing::warn!(
                        cid = %cid,
                        error = %e,
                        "Failed to mark block as merged"
                    );
                }

                // Optionally re-broadcast
                if config.rebroadcast_on_merge {
                    if let Err(e) = coordinator
                        .broadcast_local_update(&cid, &block_data, doc_id, collection_id)
                        .await
                    {
                        tracing::warn!(
                            cid = %cid,
                            error = %e,
                            "Failed to re-broadcast merged block"
                        );
                    }
                }

                ReplicationResult::Merged {
                    cid,
                    doc_id: doc_id.to_string(),
                }
            }
            Ok(false) => {
                // Merge skipped - still mark as merged to prevent reprocessing
                if let Err(e) = coordinator.mark_as_merged(&cid).await {
                    tracing::warn!(
                        cid = %cid,
                        error = %e,
                        "Failed to mark skipped block as merged"
                    );
                }

                ReplicationResult::Skipped {
                    cid,
                    reason: "merge rejected by CRDT".to_string(),
                }
            }
            Err(e) => ReplicationResult::Failed {
                cid,
                error: e.to_string(),
            },
        }
    }

    /// Process all pending events without blocking.
    ///
    /// Useful for draining events during shutdown or testing.
    pub async fn drain<B, H>(
        coordinator: Arc<SyncCoordinator<B>>,
        events: &mut mpsc::Receiver<SyncEvent>,
        handler: Arc<H>,
        config: ReplicationConfig,
    ) -> Vec<ReplicationResult>
    where
        B: Blockstore + 'static,
        H: MergeHandler + 'static,
    {
        let mut results = Vec::new();

        loop {
            match events.try_recv() {
                Ok(event) => {
                    let result = match event {
                        SyncEvent::BlockReceived {
                            cid,
                            doc_id,
                            collection_id,
                            creator,
                        } => {
                            Self::handle_block_received(
                                &coordinator,
                                handler.as_ref(),
                                &config,
                                cid,
                                &doc_id,
                                &collection_id,
                                &creator,
                            )
                            .await
                        }
                        SyncEvent::BlockAlreadyMerged { cid } => ReplicationResult::Skipped {
                            cid,
                            reason: "already merged".to_string(),
                        },
                        SyncEvent::SyncError { cid, error } => {
                            ReplicationResult::Failed { cid, error }
                        }
                    };
                    results.push(result);
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        results
    }

    /// Process unmerged blocks from startup recovery.
    ///
    /// Call this at startup to process any blocks that were stored
    /// but not yet merged (e.g., due to crash recovery).
    pub async fn recover_unmerged<B, H>(
        coordinator: Arc<SyncCoordinator<B>>,
        handler: Arc<H>,
    ) -> Vec<ReplicationResult>
    where
        B: Blockstore + 'static,
        H: MergeHandler + 'static,
    {
        let config = ReplicationConfig {
            continue_on_error: true,
            rebroadcast_on_merge: false,
        };

        let unmerged = match coordinator.get_unmerged().await {
            Ok(cids) => cids,
            Err(e) => {
                tracing::error!(error = %e, "Failed to get unmerged blocks");
                return vec![];
            }
        };

        tracing::info!(count = unmerged.len(), "Recovering unmerged blocks");

        let mut results = Vec::new();
        for cid in unmerged {
            // For recovery, we don't have the original metadata
            // Use empty strings as placeholders - the handler should be able to
            // determine doc_id and collection_id from the block itself
            let result = Self::handle_block_received(
                &coordinator,
                handler.as_ref(),
                &config,
                cid,
                "", // Unknown doc_id
                "", // Unknown collection_id
                "", // Unknown creator
            )
            .await;
            results.push(result);
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockstore::DefraBlockstore;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use storage::backends::MemoryStore;

    fn test_cid() -> Cid {
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
    }

    /// Test merge handler that tracks calls
    struct TestMergeHandler {
        call_count: AtomicUsize,
        should_succeed: bool,
        should_skip: bool,
    }

    impl TestMergeHandler {
        fn new(should_succeed: bool, should_skip: bool) -> Self {
            Self {
                call_count: AtomicUsize::new(0),
                should_succeed,
                should_skip,
            }
        }

        fn calls(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    #[derive(Debug)]
    struct TestError(String);

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestError: {}", self.0)
        }
    }

    impl std::error::Error for TestError {}

    #[async_trait]
    impl MergeHandler for TestMergeHandler {
        type Error = TestError;

        async fn handle_block(
            &self,
            _cid: &Cid,
            _block_data: &[u8],
            _doc_id: &str,
            _collection_id: &str,
            _creator: &str,
        ) -> Result<bool, Self::Error> {
            self.call_count.fetch_add(1, Ordering::SeqCst);

            if !self.should_succeed {
                return Err(TestError("merge failed".to_string()));
            }

            Ok(!self.should_skip)
        }
    }

    #[tokio::test]
    async fn test_process_block_received_success() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));

        // Store a block
        let cid = test_cid();
        blockstore.put(&cid, b"test data").await.unwrap();

        let handler = Arc::new(TestMergeHandler::new(true, false));

        // Create a simple event
        let (tx, _rx) = mpsc::channel(1);
        tx.send(SyncEvent::BlockReceived {
            cid,
            doc_id: "doc1".to_string(),
            collection_id: "col1".to_string(),
            creator: "peer1".to_string(),
        })
        .await
        .unwrap();
        drop(tx); // Close channel

        // We can't easily test the full loop without a coordinator
        // but we can verify the handler trait works
        let result = handler
            .handle_block(&cid, b"test data", "doc1", "col1", "peer1")
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap()); // Should return true (merged)
        assert_eq!(handler.calls(), 1);
    }

    #[tokio::test]
    async fn test_handler_skip() {
        let cid = test_cid();
        let handler = TestMergeHandler::new(true, true); // succeed but skip

        let result = handler
            .handle_block(&cid, b"test", "doc", "col", "peer")
            .await;
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Should return false (skipped)
    }

    #[tokio::test]
    async fn test_handler_error() {
        let cid = test_cid();
        let handler = TestMergeHandler::new(false, false); // fail

        let result = handler
            .handle_block(&cid, b"test", "doc", "col", "peer")
            .await;
        assert!(result.is_err());
    }
}
