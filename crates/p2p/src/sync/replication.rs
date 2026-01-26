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

use crate::QueryId;
use blockstore::Blockstore;
use cid::Cid;
use libp2p::PeerId;
use tokio::sync::mpsc;

use super::coordinator::SyncCoordinator;
use super::manager::SyncEvent;
use super::merge::{BlockMetadata, MergeHandler, MergeOutcome};

/// Result of a replication loop iteration.
#[derive(Debug, Clone)]
pub enum ReplicationResult {
    /// Block was merged successfully
    Merged { cid: Cid, doc_id: String },
    /// Block was merged but re-broadcast failed (replication to other nodes may be incomplete)
    MergedButBroadcastFailed {
        cid: Cid,
        doc_id: String,
        broadcast_error: String,
    },
    /// Block was skipped (already applied or rejected)
    Skipped { cid: Cid, reason: String },
    /// Merge failed
    Failed { cid: Cid, error: String },
    /// Merge succeeded but failed to mark as merged (will be reprocessed on restart)
    MergedButNotMarked { cid: Cid, error: String },
    /// Event channel closed
    ChannelClosed,
    /// Bitswap fetch started for missing blocks
    BitswapFetchStarted { root_cid: Cid, query_id: QueryId },
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
                ReplicationResult::MergedButBroadcastFailed {
                    cid,
                    doc_id,
                    broadcast_error,
                } => {
                    tracing::error!(
                        cid = %cid,
                        doc_id = %doc_id,
                        error = %broadcast_error,
                        "Block merged but re-broadcast failed - other nodes may not receive this update"
                    );
                    // Continue processing - the local merge succeeded
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
                ReplicationResult::MergedButNotMarked { cid, error } => {
                    tracing::error!(
                        cid = %cid,
                        error = %error,
                        "Block merged but failed to mark - will be reprocessed on restart"
                    );
                    // Continue processing - the merge succeeded, just the bookkeeping failed
                }
                ReplicationResult::ChannelClosed => {
                    tracing::info!("Event channel closed, stopping replication loop");
                    break;
                }
                ReplicationResult::BitswapFetchStarted { root_cid, query_id } => {
                    tracing::debug!(
                        cid = %root_cid,
                        query_id = ?query_id,
                        "Bitswap fetch started for missing blocks"
                    );
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
                    BlockMetadata::normal(&doc_id, &collection_id, &creator),
                )
                .await
            }
            SyncEvent::BlockAlreadyMerged { cid } => ReplicationResult::Skipped {
                cid,
                reason: "already merged".to_string(),
            },
            SyncEvent::SyncError { cid, error } => ReplicationResult::Failed { cid, error },
            SyncEvent::DagNeedsFetch {
                root_cid,
                missing,
                providers,
                ..
            } => {
                // Initiate Bitswap fetch for missing blocks
                Self::handle_dag_needs_fetch(coordinator, root_cid, missing, providers).await
            }
            SyncEvent::DagReady {
                root_cid,
                doc_id,
                collection_id,
                schema_version_id,
            } => {
                // DAG is complete after Bitswap fetch - process as block received
                tracing::info!(
                    cid = %root_cid,
                    doc_id = %doc_id,
                    "DAG ready for merge after Bitswap fetch"
                );
                Self::handle_block_received(
                    coordinator,
                    handler,
                    config,
                    root_cid,
                    BlockMetadata::normal(&doc_id, &collection_id, &schema_version_id),
                )
                .await
            }
        }
    }

    /// Handle a DagNeedsFetch event by initiating a Bitswap sync.
    async fn handle_dag_needs_fetch<B>(
        coordinator: &SyncCoordinator<B>,
        root_cid: Cid,
        missing: Vec<Cid>,
        providers: Vec<PeerId>,
    ) -> ReplicationResult
    where
        B: Blockstore + 'static,
    {
        tracing::debug!(
            cid = %root_cid,
            missing_count = missing.len(),
            provider_count = providers.len(),
            "Initiating Bitswap fetch for missing blocks"
        );

        // Start Bitswap sync via host
        match coordinator
            .host()
            .bitswap_sync(root_cid, providers, missing)
            .await
        {
            Ok(query_id) => {
                // Register the query so we can track completion
                coordinator.manager().register_query(query_id, root_cid);
                ReplicationResult::BitswapFetchStarted { root_cid, query_id }
            }
            Err(e) => {
                tracing::warn!(
                    cid = %root_cid,
                    error = %e,
                    "Failed to start Bitswap fetch"
                );
                ReplicationResult::Failed {
                    cid: root_cid,
                    error: format!("Failed to start Bitswap fetch: {}", e),
                }
            }
        }
    }

    /// Handle a BlockReceived event.
    async fn handle_block_received<B, H>(
        coordinator: &SyncCoordinator<B>,
        handler: &H,
        config: &ReplicationConfig,
        cid: Cid,
        metadata: BlockMetadata<'_>,
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

        // Extract doc_id for use in result (use empty string if recovery mode)
        let doc_id_for_result = metadata.doc_id.unwrap_or("").to_string();
        let collection_id_for_broadcast = metadata.collection_id.unwrap_or("");

        // Delegate merge to handler
        match handler.handle_block(&cid, &block_data, metadata).await {
            Ok(MergeOutcome::Merged) => {
                // Merge successful - mark as merged
                if let Err(e) = coordinator.mark_as_merged(&cid).await {
                    // Return a distinct result so callers know the merge succeeded
                    // but bookkeeping failed (block will be reprocessed on restart)
                    return ReplicationResult::MergedButNotMarked {
                        cid,
                        error: e.to_string(),
                    };
                }

                // Optionally re-broadcast (skip if metadata incomplete - can't broadcast without doc/collection IDs)
                if config.rebroadcast_on_merge && !collection_id_for_broadcast.is_empty() {
                    match coordinator
                        .broadcast_local_update(
                            &cid,
                            &block_data,
                            &doc_id_for_result,
                            collection_id_for_broadcast,
                        )
                        .await
                    {
                        Ok(super::BroadcastResult::Success) => {
                            // Both topics succeeded - nothing to report
                        }
                        Ok(super::BroadcastResult::PartialDocumentOnly { collection_error }) => {
                            // Partial success - return distinct result so callers know
                            return ReplicationResult::MergedButBroadcastFailed {
                                cid,
                                doc_id: doc_id_for_result,
                                broadcast_error: format!(
                                    "Partial: document topic succeeded but collection topic failed: {}",
                                    collection_error
                                ),
                            };
                        }
                        Ok(super::BroadcastResult::PartialCollectionOnly { document_error }) => {
                            // Partial success - return distinct result so callers know
                            return ReplicationResult::MergedButBroadcastFailed {
                                cid,
                                doc_id: doc_id_for_result,
                                broadcast_error: format!(
                                    "Partial: collection topic succeeded but document topic failed: {}",
                                    document_error
                                ),
                            };
                        }
                        Err(e) => {
                            // Total failure - return a distinct result
                            return ReplicationResult::MergedButBroadcastFailed {
                                cid,
                                doc_id: doc_id_for_result,
                                broadcast_error: e.to_string(),
                            };
                        }
                    }
                }

                ReplicationResult::Merged {
                    cid,
                    doc_id: doc_id_for_result,
                }
            }
            Ok(MergeOutcome::Skipped { reason }) => {
                // Merge skipped - still mark as merged to prevent reprocessing
                if let Err(e) = coordinator.mark_as_merged(&cid).await {
                    // For skipped blocks, marking failure is less critical since
                    // re-processing will just skip again, but still report it
                    return ReplicationResult::MergedButNotMarked {
                        cid,
                        error: format!("skipped but failed to mark: {}", e),
                    };
                }

                ReplicationResult::Skipped { cid, reason }
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
                                BlockMetadata::normal(&doc_id, &collection_id, &creator),
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
                        SyncEvent::DagNeedsFetch {
                            root_cid,
                            missing,
                            providers,
                            ..
                        } => {
                            Self::handle_dag_needs_fetch(&coordinator, root_cid, missing, providers)
                                .await
                        }
                        SyncEvent::DagReady {
                            root_cid,
                            doc_id,
                            collection_id,
                            schema_version_id,
                        } => {
                            Self::handle_block_received(
                                &coordinator,
                                handler.as_ref(),
                                &config,
                                root_cid,
                                BlockMetadata::normal(&doc_id, &collection_id, &schema_version_id),
                            )
                            .await
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
    ///
    /// # Recovery Mode
    ///
    /// During recovery, `BlockMetadata::recovery()` is passed to the handler with
    /// all metadata fields set to `None`. The `MergeHandler` implementation MUST:
    /// 1. Extract doc_id, collection_id, and creator from the block data itself
    /// 2. Return an error if extraction fails (do NOT silently use defaults)
    ///
    /// This ensures data integrity is maintained even after crashes.
    ///
    /// # Returns
    ///
    /// * `Ok(results)` - All blocks recovered successfully (or skipped)
    /// * `Err(RecoveryFailed)` - One or more blocks failed to recover
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The unmerged block list cannot be retrieved
    /// * One or more blocks failed to recover (returns `Error::RecoveryFailed`)
    pub async fn recover_unmerged<B, H>(
        coordinator: Arc<SyncCoordinator<B>>,
        handler: Arc<H>,
    ) -> Result<Vec<ReplicationResult>, crate::error::Error>
    where
        B: Blockstore + 'static,
        H: MergeHandler + 'static,
    {
        let config = ReplicationConfig {
            continue_on_error: true,
            rebroadcast_on_merge: false,
        };

        let unmerged = coordinator.get_unmerged().await?;
        let total = unmerged.len();

        if unmerged.is_empty() {
            tracing::info!("No unmerged blocks to recover");
            return Ok(Vec::new());
        }

        tracing::warn!(
            count = total,
            "Recovering unmerged blocks - metadata unavailable, handler must extract from block data"
        );

        let mut results = Vec::new();
        let mut success_count = 0;
        let mut failure_count = 0;

        for cid in unmerged {
            tracing::debug!(cid = %cid, "Recovering unmerged block in recovery mode");

            let result = Self::handle_block_received(
                &coordinator,
                handler.as_ref(),
                &config,
                cid,
                BlockMetadata::recovery(),
            )
            .await;

            match &result {
                ReplicationResult::Merged { .. } | ReplicationResult::Skipped { .. } => {
                    success_count += 1;
                }
                ReplicationResult::MergedButNotMarked { cid, error } => {
                    // Merge succeeded but bookkeeping failed - count as success
                    success_count += 1;
                    tracing::warn!(
                        cid = %cid,
                        error = %error,
                        "Block merged during recovery but marking failed - will be reprocessed next startup"
                    );
                }
                ReplicationResult::MergedButBroadcastFailed {
                    cid,
                    doc_id,
                    broadcast_error,
                } => {
                    // Merge succeeded - count as success (broadcast not expected during recovery)
                    success_count += 1;
                    tracing::debug!(
                        cid = %cid,
                        doc_id = %doc_id,
                        error = %broadcast_error,
                        "Block merged during recovery but broadcast failed (expected - recovery mode)"
                    );
                }
                ReplicationResult::Failed { cid, error } => {
                    failure_count += 1;
                    tracing::error!(
                        cid = %cid,
                        error = %error,
                        "Failed to recover block - manual intervention may be required"
                    );
                }
                ReplicationResult::BitswapFetchStarted { root_cid, .. } => {
                    // Unexpected during recovery - blocks should already be in blockstore
                    tracing::warn!(
                        cid = %root_cid,
                        "Unexpected BitswapFetchStarted during recovery - block may have missing links"
                    );
                }
                ReplicationResult::ChannelClosed => {
                    tracing::error!(
                        "Channel closed during recovery - some blocks may not be recovered"
                    );
                    break; // Exit recovery loop early
                }
            }

            results.push(result);
        }

        tracing::info!(
            success = success_count,
            failed = failure_count,
            "Recovery complete"
        );

        // Return error if any blocks failed to recover
        if failure_count > 0 {
            return Err(crate::error::Error::RecoveryFailed {
                success: success_count,
                failed: failure_count,
                total,
            });
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
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
            _metadata: BlockMetadata<'_>,
        ) -> Result<MergeOutcome, Self::Error> {
            self.call_count.fetch_add(1, Ordering::SeqCst);

            if !self.should_succeed {
                return Err(TestError("merge failed".to_string()));
            }

            if self.should_skip {
                Ok(MergeOutcome::skipped("test skip reason"))
            } else {
                Ok(MergeOutcome::Merged)
            }
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
            .handle_block(
                &cid,
                b"test data",
                BlockMetadata::normal("doc1", "col1", "peer1"),
            )
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_merged());
        assert_eq!(handler.calls(), 1);
    }

    #[tokio::test]
    async fn test_handler_skip() {
        let cid = test_cid();
        let handler = TestMergeHandler::new(true, true); // succeed but skip

        let result = handler
            .handle_block(&cid, b"test", BlockMetadata::normal("doc", "col", "peer"))
            .await;
        assert!(result.is_ok());
        let outcome = result.unwrap();
        assert!(outcome.is_skipped());
        match outcome {
            MergeOutcome::Skipped { reason } => {
                assert_eq!(reason, "test skip reason");
            }
            _ => panic!("Expected Skipped outcome"),
        }
    }

    #[tokio::test]
    async fn test_handler_error() {
        let cid = test_cid();
        let handler = TestMergeHandler::new(false, false); // fail

        let result = handler
            .handle_block(&cid, b"test", BlockMetadata::normal("doc", "col", "peer"))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handler_recovery_mode() {
        let cid = test_cid();
        let handler = TestMergeHandler::new(true, false);

        // Recovery mode - metadata is None
        let metadata = BlockMetadata::recovery();
        assert!(metadata.is_recovery);
        assert!(metadata.is_incomplete());
        assert!(metadata.doc_id.is_none());

        let result = handler.handle_block(&cid, b"test", metadata).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_replication_result_merged_but_not_marked() {
        // Test that MergedButNotMarked is a distinct result type
        let cid = test_cid();
        let result = ReplicationResult::MergedButNotMarked {
            cid,
            error: "mark_as_merged failed".to_string(),
        };

        // Verify the result contains the expected data
        match result {
            ReplicationResult::MergedButNotMarked { cid: c, error } => {
                assert_eq!(c, cid);
                assert!(error.contains("mark_as_merged"));
            }
            _ => panic!("Expected MergedButNotMarked"),
        }
    }

    #[tokio::test]
    async fn test_replication_result_merged_but_broadcast_failed() {
        // Test that MergedButBroadcastFailed is a distinct result type
        let cid = test_cid();
        let result = ReplicationResult::MergedButBroadcastFailed {
            cid,
            doc_id: "doc123".to_string(),
            broadcast_error: "no peers connected".to_string(),
        };

        // Verify the result contains the expected data
        match result {
            ReplicationResult::MergedButBroadcastFailed {
                cid: c,
                doc_id,
                broadcast_error,
            } => {
                assert_eq!(c, cid);
                assert_eq!(doc_id, "doc123");
                assert!(broadcast_error.contains("no peers"));
            }
            _ => panic!("Expected MergedButBroadcastFailed"),
        }
    }
}
