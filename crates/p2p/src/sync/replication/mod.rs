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
    use crate::sync::merge::{BlockMetadata, MergeBlock, MergeHandler, MergeOutcome};
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

    /// Generate distinct test CIDs by hashing different data.
    fn make_cid(data: &[u8]) -> Cid {
        use cid::multihash::{Code, MultihashDigest};
        let hash = Code::Sha2_256.digest(data);
        Cid::new_v1(0x55, hash) // 0x55 = raw codec
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

    /// Batch-aware merge handler that tracks per-block and batch calls separately.
    struct BatchTestHandler {
        per_block_calls: AtomicUsize,
        batch_calls: AtomicUsize,
        batch_block_count: AtomicUsize,
        fail_at_index: Option<usize>,
    }

    impl BatchTestHandler {
        fn new() -> Self {
            Self {
                per_block_calls: AtomicUsize::new(0),
                batch_calls: AtomicUsize::new(0),
                batch_block_count: AtomicUsize::new(0),
                fail_at_index: None,
            }
        }

        fn with_failure_at(index: usize) -> Self {
            Self {
                per_block_calls: AtomicUsize::new(0),
                batch_calls: AtomicUsize::new(0),
                batch_block_count: AtomicUsize::new(0),
                fail_at_index: Some(index),
            }
        }

        fn per_block_calls(&self) -> usize {
            self.per_block_calls.load(Ordering::SeqCst)
        }

        fn batch_calls(&self) -> usize {
            self.batch_calls.load(Ordering::SeqCst)
        }

        fn batch_block_count(&self) -> usize {
            self.batch_block_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl MergeHandler for BatchTestHandler {
        type Error = TestError;

        async fn handle_block(
            &self,
            _cid: &Cid,
            _block_data: &[u8],
            _metadata: BlockMetadata<'_>,
        ) -> Result<MergeOutcome, Self::Error> {
            self.per_block_calls.fetch_add(1, Ordering::SeqCst);
            Ok(MergeOutcome::Merged)
        }

        async fn handle_block_batch(
            &self,
            blocks: &[MergeBlock],
        ) -> Vec<Result<MergeOutcome, Self::Error>> {
            self.batch_calls.fetch_add(1, Ordering::SeqCst);
            self.batch_block_count
                .fetch_add(blocks.len(), Ordering::SeqCst);

            blocks
                .iter()
                .enumerate()
                .map(|(i, _block)| {
                    if self.fail_at_index == Some(i) {
                        Err(TestError("batch block failed".to_string()))
                    } else {
                        Ok(MergeOutcome::Merged)
                    }
                })
                .collect()
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

    // =========================================================================
    // Batch merge tests
    // =========================================================================

    #[tokio::test]
    async fn test_default_handle_block_batch_calls_per_block() {
        // The default handle_block_batch delegates to handle_block per block.
        let handler = TestMergeHandler::new(true, false);
        let blocks: Vec<MergeBlock> = (0..5)
            .map(|i| {
                let data = format!("block{}", i);
                MergeBlock {
                    cid: make_cid(data.as_bytes()),
                    block_data: data.into_bytes(),
                    doc_id: format!("doc{}", i),
                    collection_id: "col1".to_string(),
                    creator: "peer1".to_string(),
                }
            })
            .collect();

        let results = handler.handle_block_batch(&blocks).await;

        assert_eq!(results.len(), 5);
        assert!(results.iter().all(|r| r.as_ref().unwrap().is_merged()));
        assert_eq!(
            handler.calls(),
            5,
            "default impl should call handle_block per block"
        );
    }

    #[tokio::test]
    async fn test_batch_handler_receives_all_blocks_at_once() {
        // A custom handle_block_batch override gets all blocks in one call.
        let handler = BatchTestHandler::new();
        let blocks: Vec<MergeBlock> = (0..10)
            .map(|i| {
                let data = format!("block{}", i);
                MergeBlock {
                    cid: make_cid(data.as_bytes()),
                    block_data: data.into_bytes(),
                    doc_id: format!("doc{}", i),
                    collection_id: "col1".to_string(),
                    creator: "peer1".to_string(),
                }
            })
            .collect();

        let results = handler.handle_block_batch(&blocks).await;

        assert_eq!(results.len(), 10);
        assert!(results.iter().all(|r| r.as_ref().unwrap().is_merged()));
        assert_eq!(handler.batch_calls(), 1, "batch should be called once");
        assert_eq!(
            handler.batch_block_count(),
            10,
            "batch should receive all 10 blocks"
        );
        assert_eq!(
            handler.per_block_calls(),
            0,
            "handle_block should NOT be called"
        );
    }

    #[tokio::test]
    async fn test_batch_handler_partial_failure() {
        // When one block in a batch fails, the rest still get results.
        let handler = BatchTestHandler::with_failure_at(2);
        let blocks: Vec<MergeBlock> = (0..5)
            .map(|i| {
                let data = format!("block{}", i);
                MergeBlock {
                    cid: make_cid(data.as_bytes()),
                    block_data: data.into_bytes(),
                    doc_id: format!("doc{}", i),
                    collection_id: "col1".to_string(),
                    creator: "peer1".to_string(),
                }
            })
            .collect();

        let results = handler.handle_block_batch(&blocks).await;

        assert_eq!(results.len(), 5);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
        assert!(results[2].is_err());
        assert!(results[3].is_ok());
        assert!(results[4].is_ok());
    }

    #[tokio::test]
    async fn test_batch_handler_empty_batch() {
        let handler = BatchTestHandler::new();
        let results = handler.handle_block_batch(&[]).await;

        assert!(results.is_empty());
        assert_eq!(handler.batch_calls(), 1, "batch called even when empty");
        assert_eq!(handler.batch_block_count(), 0);
    }

    #[tokio::test]
    async fn test_batch_handler_single_block() {
        // Single-block batch should still go through handle_block_batch.
        let handler = BatchTestHandler::new();
        let blocks = vec![MergeBlock {
            cid: make_cid(b"single"),
            block_data: b"single".to_vec(),
            doc_id: "doc0".to_string(),
            collection_id: "col1".to_string(),
            creator: "peer1".to_string(),
        }];

        let results = handler.handle_block_batch(&blocks).await;

        assert_eq!(results.len(), 1);
        assert!(results[0].as_ref().unwrap().is_merged());
        assert_eq!(handler.batch_calls(), 1);
        assert_eq!(handler.batch_block_count(), 1);
    }

    #[tokio::test]
    async fn test_merge_block_metadata_preserved() {
        // Verify that MergeBlock fields are accessible to the batch handler.
        let handler = TestMergeHandler::new(true, false);
        let cid = make_cid(b"metadata-test");
        let blocks = vec![MergeBlock {
            cid,
            block_data: b"test data".to_vec(),
            doc_id: "my-doc".to_string(),
            collection_id: "my-collection".to_string(),
            creator: "my-peer".to_string(),
        }];

        // The default impl passes metadata through to handle_block.
        // We can't inspect the metadata inside TestMergeHandler, but we can
        // verify the call succeeds and the block round-trips correctly.
        let results = handler.handle_block_batch(&blocks).await;
        assert_eq!(results.len(), 1);
        assert!(results[0].as_ref().unwrap().is_merged());
        assert_eq!(blocks[0].doc_id, "my-doc");
        assert_eq!(blocks[0].collection_id, "my-collection");
        assert_eq!(blocks[0].creator, "my-peer");
    }

    #[tokio::test]
    async fn test_default_batch_propagates_errors() {
        // The default handle_block_batch should propagate per-block errors.
        let handler = TestMergeHandler::new(false, false); // all blocks fail
        let blocks: Vec<MergeBlock> = (0..3)
            .map(|i| {
                let data = format!("block{}", i);
                MergeBlock {
                    cid: make_cid(data.as_bytes()),
                    block_data: data.into_bytes(),
                    doc_id: format!("doc{}", i),
                    collection_id: "col1".to_string(),
                    creator: "peer1".to_string(),
                }
            })
            .collect();

        let results = handler.handle_block_batch(&blocks).await;

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.is_err()));
        assert_eq!(
            handler.calls(),
            3,
            "default impl calls handle_block for each"
        );
    }

    #[tokio::test]
    async fn test_default_batch_mixed_skip_and_merge() {
        // Test that skip outcomes are preserved through the batch.
        let handler = TestMergeHandler::new(true, true); // all blocks skip
        let blocks: Vec<MergeBlock> = (0..3)
            .map(|i| {
                let data = format!("block{}", i);
                MergeBlock {
                    cid: make_cid(data.as_bytes()),
                    block_data: data.into_bytes(),
                    doc_id: format!("doc{}", i),
                    collection_id: "col1".to_string(),
                    creator: "peer1".to_string(),
                }
            })
            .collect();

        let results = handler.handle_block_batch(&blocks).await;

        assert_eq!(results.len(), 3);
        for result in &results {
            assert!(result.as_ref().unwrap().is_skipped());
        }
    }
}
