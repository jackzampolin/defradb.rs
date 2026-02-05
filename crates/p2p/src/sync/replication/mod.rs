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

mod config;
mod handlers;
mod loop_runner;
mod recovery;
mod result;

pub use config::ReplicationConfig;
pub use loop_runner::ReplicationLoop;
pub use recovery::recover_unmerged;
pub use result::ReplicationResult;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::manager::SyncEvent;
    use crate::sync::merge::{BlockMetadata, MergeHandler, MergeOutcome};
    use async_trait::async_trait;
    use blockstore::{Blockstore, DefraBlockstore};
    use cid::Cid;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use storage::backends::MemoryStore;
    use tokio::sync::mpsc;

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
            collection_id: "col1".to_string(),
            broadcast_error: "no peers connected".to_string(),
        };

        // Verify the result contains the expected data
        match result {
            ReplicationResult::MergedButBroadcastFailed {
                cid: c,
                doc_id,
                collection_id,
                broadcast_error,
            } => {
                assert_eq!(c, cid);
                assert_eq!(doc_id, "doc123");
                assert_eq!(collection_id, "col1");
                assert!(broadcast_error.contains("no peers"));
            }
            _ => panic!("Expected MergedButBroadcastFailed"),
        }
    }
}
