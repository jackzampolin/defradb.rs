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
    use super::handlers::{handle_block_received, process_event};
    use super::*;
    use crate::bitswap::AccessMode;
    use crate::error::Result as P2PResult;
    use crate::message::{
        BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
        PushLogReply, PushLogRequest, PushSEArtifactsRequest,
    };
    use crate::sync::manager::SyncEvent;
    use crate::sync::merge::{BlockMetadata, MergeBlock, MergeHandler, MergeOutcome};
    use crate::topics::DefraTopic;
    use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId, TransportEvent};
    use crate::QueryId;
    use crate::ReplicatorInfo;
    use async_trait::async_trait;
    use blockstore::{Blockstore, DefraBlockstore};
    use cid::Cid;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use storage::backends::MemoryStore;
    use tokio::sync::{mpsc, Semaphore};

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

    #[derive(Clone)]
    struct NoopTransport {
        peer_id: PeerId,
        pubkey: Vec<u8>,
    }

    impl NoopTransport {
        fn new() -> Self {
            Self {
                peer_id: PeerId::new("local-peer".to_string()),
                pubkey: vec![1, 2, 3],
            }
        }
    }

    #[derive(Clone)]
    struct PollFetchTransport {
        peer_id: PeerId,
        pubkey: Vec<u8>,
        blockstore: Arc<DefraBlockstore<MemoryStore>>,
        child_cid: Cid,
        child_data: Vec<u8>,
        sync_blocks_calls: Arc<AtomicUsize>,
    }

    impl PollFetchTransport {
        fn new(
            blockstore: Arc<DefraBlockstore<MemoryStore>>,
            child_cid: Cid,
            child_data: Vec<u8>,
        ) -> Self {
            Self {
                peer_id: PeerId::new("local-peer".to_string()),
                pubkey: vec![1, 2, 3],
                blockstore,
                child_cid,
                child_data,
                sync_blocks_calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn sync_blocks_calls(&self) -> usize {
            self.sync_blocks_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl P2PTransport for PollFetchTransport {
        type ResponseToken = ();

        fn local_peer_id(&self) -> &PeerId {
            &self.peer_id
        }

        fn local_public_key_proto(&self) -> &[u8] {
            &self.pubkey
        }

        fn sign(&self, _data: &[u8]) -> P2PResult<Vec<u8>> {
            Ok(vec![0])
        }

        async fn dial(&self, _peer_id: &PeerId, _addrs: Vec<PeerAddr>) -> P2PResult<()> {
            Ok(())
        }

        async fn listen(&self, _addr: PeerAddr) -> P2PResult<()> {
            Ok(())
        }

        async fn connected_peers(&self) -> P2PResult<Vec<PeerId>> {
            Ok(Vec::new())
        }

        async fn listen_addresses(&self) -> P2PResult<Vec<PeerAddr>> {
            Ok(Vec::new())
        }

        async fn poll_until_connected(
            &self,
            _peer_id: &PeerId,
            _timeout: Duration,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn peer_addresses(&self) -> P2PResult<Vec<String>> {
            Ok(Vec::new())
        }

        async fn topic_peers(&self, _topic: DefraTopic) -> P2PResult<Vec<PeerId>> {
            Ok(Vec::new())
        }

        async fn subscribe(&self, _topic: DefraTopic) -> P2PResult<bool> {
            Ok(true)
        }

        async fn unsubscribe(&self, _topic: DefraTopic) -> P2PResult<bool> {
            Ok(true)
        }

        async fn publish(
            &self,
            _topic: DefraTopic,
            _msg: PushLogBroadcast,
        ) -> P2PResult<MessageId> {
            Ok(MessageId::new("noop".to_string()))
        }

        async fn send_pushlog_response(
            &self,
            _token: Self::ResponseToken,
            _reply: PushLogReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_two_stream_request(
            &self,
            _peer_id: &PeerId,
            _req: PushLogRequest,
        ) -> P2PResult<PushLogReply> {
            Ok(PushLogReply::success("noop"))
        }

        async fn send_two_stream_response(
            &self,
            _peer_id: &PeerId,
            _reply: PushLogReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_doc_sync_request(
            &self,
            _peer_id: &PeerId,
            _req: DocSyncRequest,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_doc_sync_response(
            &self,
            _peer_id: &PeerId,
            _reply: DocSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_branchable_sync_request(
            &self,
            _peer_id: &PeerId,
            _req: BranchableSyncRequest,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_branchable_sync_response(
            &self,
            _peer_id: &PeerId,
            _reply: BranchableSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_car_request(&self, _peer_id: &PeerId, _root_cid: Cid) -> P2PResult<()> {
            self.blockstore
                .put(&self.child_cid, &self.child_data)
                .await
                .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;
            Ok(())
        }

        async fn send_car_response(&self, _peer_id: &PeerId, _car_data: Vec<u8>) -> P2PResult<()> {
            Ok(())
        }

        async fn send_car_response_token(
            &self,
            _token: Self::ResponseToken,
            _car_data: Vec<u8>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_doc_sync_response_token(
            &self,
            _token: Self::ResponseToken,
            _reply: DocSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_branchable_sync_response_token(
            &self,
            _token: Self::ResponseToken,
            _reply: BranchableSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_se_artifacts(
            &self,
            _peer_id: &PeerId,
            _req: PushSEArtifactsRequest,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn sync_blocks(
            &self,
            _root: Cid,
            _providers: Vec<PeerId>,
            _missing: Vec<Cid>,
        ) -> P2PResult<QueryId> {
            self.sync_blocks_calls.fetch_add(1, Ordering::SeqCst);
            Ok(QueryId(1))
        }

        async fn cancel_sync(&self, _query_id: QueryId) -> P2PResult<bool> {
            Ok(true)
        }

        async fn create_replicator(
            &self,
            _peer_id: &PeerId,
            _collections: Vec<String>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn delete_replicator(&self, _peer_id: &PeerId) -> P2PResult<()> {
            Ok(())
        }

        async fn list_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>> {
            Ok(Vec::new())
        }

        async fn get_replicator(&self, _peer_id: &PeerId) -> P2PResult<Option<ReplicatorInfo>> {
            Ok(None)
        }

        async fn remove_replicator_collections(
            &self,
            _peer_id: &PeerId,
            _collections: Vec<String>,
        ) -> P2PResult<bool> {
            Ok(false)
        }

        async fn shutdown(&self) -> P2PResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl P2PTransport for NoopTransport {
        type ResponseToken = ();

        fn local_peer_id(&self) -> &PeerId {
            &self.peer_id
        }

        fn local_public_key_proto(&self) -> &[u8] {
            &self.pubkey
        }

        fn sign(&self, _data: &[u8]) -> P2PResult<Vec<u8>> {
            Ok(vec![0])
        }

        async fn dial(&self, _peer_id: &PeerId, _addrs: Vec<PeerAddr>) -> P2PResult<()> {
            Ok(())
        }

        async fn listen(&self, _addr: PeerAddr) -> P2PResult<()> {
            Ok(())
        }

        async fn connected_peers(&self) -> P2PResult<Vec<PeerId>> {
            Ok(Vec::new())
        }

        async fn listen_addresses(&self) -> P2PResult<Vec<PeerAddr>> {
            Ok(Vec::new())
        }

        async fn poll_until_connected(
            &self,
            _peer_id: &PeerId,
            _timeout: Duration,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn peer_addresses(&self) -> P2PResult<Vec<String>> {
            Ok(Vec::new())
        }

        async fn topic_peers(&self, _topic: DefraTopic) -> P2PResult<Vec<PeerId>> {
            Ok(Vec::new())
        }

        async fn subscribe(&self, _topic: DefraTopic) -> P2PResult<bool> {
            Ok(true)
        }

        async fn unsubscribe(&self, _topic: DefraTopic) -> P2PResult<bool> {
            Ok(true)
        }

        async fn publish(
            &self,
            _topic: DefraTopic,
            _msg: PushLogBroadcast,
        ) -> P2PResult<MessageId> {
            Ok(MessageId::new("noop".to_string()))
        }

        async fn send_pushlog_response(
            &self,
            _token: Self::ResponseToken,
            _reply: PushLogReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_two_stream_request(
            &self,
            _peer_id: &PeerId,
            _req: PushLogRequest,
        ) -> P2PResult<PushLogReply> {
            Ok(PushLogReply::success("noop"))
        }

        async fn send_two_stream_response(
            &self,
            _peer_id: &PeerId,
            _reply: PushLogReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_doc_sync_request(
            &self,
            _peer_id: &PeerId,
            _req: DocSyncRequest,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_doc_sync_response(
            &self,
            _peer_id: &PeerId,
            _reply: DocSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_branchable_sync_request(
            &self,
            _peer_id: &PeerId,
            _req: BranchableSyncRequest,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_branchable_sync_response(
            &self,
            _peer_id: &PeerId,
            _reply: BranchableSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_car_request(&self, _peer_id: &PeerId, _root_cid: Cid) -> P2PResult<()> {
            Ok(())
        }

        async fn send_car_response(&self, _peer_id: &PeerId, _car_data: Vec<u8>) -> P2PResult<()> {
            Ok(())
        }

        async fn send_car_response_token(
            &self,
            _token: Self::ResponseToken,
            _car_data: Vec<u8>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_doc_sync_response_token(
            &self,
            _token: Self::ResponseToken,
            _reply: DocSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_branchable_sync_response_token(
            &self,
            _token: Self::ResponseToken,
            _reply: BranchableSyncReply,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn send_se_artifacts(
            &self,
            _peer_id: &PeerId,
            _req: PushSEArtifactsRequest,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn sync_blocks(
            &self,
            _root: Cid,
            _providers: Vec<PeerId>,
            _missing: Vec<Cid>,
        ) -> P2PResult<QueryId> {
            Ok(QueryId(0))
        }

        async fn cancel_sync(&self, _query_id: QueryId) -> P2PResult<bool> {
            Ok(true)
        }

        async fn create_replicator(
            &self,
            _peer_id: &PeerId,
            _collections: Vec<String>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn delete_replicator(&self, _peer_id: &PeerId) -> P2PResult<()> {
            Ok(())
        }

        async fn list_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>> {
            Ok(Vec::new())
        }

        async fn get_replicator(&self, _peer_id: &PeerId) -> P2PResult<Option<ReplicatorInfo>> {
            Ok(None)
        }

        async fn remove_replicator_collections(
            &self,
            _peer_id: &PeerId,
            _collections: Vec<String>,
        ) -> P2PResult<bool> {
            Ok(false)
        }

        async fn shutdown(&self) -> P2PResult<()> {
            Ok(())
        }
    }

    struct RetryThenMergeHandler {
        call_count: AtomicUsize,
    }

    impl RetryThenMergeHandler {
        fn new() -> Self {
            Self {
                call_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl MergeHandler for RetryThenMergeHandler {
        type Error = TestError;

        async fn handle_block(
            &self,
            _cid: &Cid,
            _block_data: &[u8],
            _metadata: BlockMetadata<'_>,
        ) -> Result<MergeOutcome, Self::Error> {
            let attempt = self.call_count.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                Ok(MergeOutcome::retryable_skip("pending ACP"))
            } else {
                Ok(MergeOutcome::Merged)
            }
        }
    }

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
                Ok(MergeOutcome::terminal_skip("test skip reason"))
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
            sender_peer: None,
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
            acp_actor_relationships: None,
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
                BlockMetadata::normal("doc1", "col1", "peer1", None, false),
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
            .handle_block(
                &cid,
                b"test",
                BlockMetadata::normal("doc", "col", "peer", None, false),
            )
            .await;
        assert!(result.is_ok());
        let outcome = result.unwrap();
        assert!(outcome.is_skipped());
        match outcome {
            MergeOutcome::Skipped { reason, terminal } => {
                assert_eq!(reason, "test skip reason");
                assert!(terminal);
            }
            _ => panic!("Expected Skipped outcome"),
        }
    }

    #[tokio::test]
    async fn test_handler_error() {
        let cid = test_cid();
        let handler = TestMergeHandler::new(false, false); // fail

        let result = handler
            .handle_block(
                &cid,
                b"test",
                BlockMetadata::normal("doc", "col", "peer", None, false),
            )
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
    async fn test_retryable_skip_remains_unmerged_until_replayed() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let cid = test_cid();
        blockstore.put(&cid, b"test data").await.unwrap();

        let (coordinator, _events) =
            crate::sync::coordinator::SyncCoordinator::with_access_control(
                NoopTransport::new(),
                blockstore.clone(),
                crate::sync::SyncConfig::default(),
                AccessMode::Open,
                Arc::new(crate::ReplicatorRegistry::new()),
                Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
            )
            .await
            .unwrap();

        let config = ReplicationConfig {
            continue_on_error: true,
            rebroadcast_on_merge: false,
            batch_size: 1,
            max_workers: 1,
        };
        let handler = RetryThenMergeHandler::new();

        let first = handle_block_received(
            &coordinator,
            &handler,
            &config,
            cid,
            BlockMetadata::normal("doc1", "col1", "peer1", Some("sender1"), true),
            None,
        )
        .await;

        match first {
            ReplicationResult::Skipped {
                terminal: false,
                ref reason,
                ..
            } => assert_eq!(reason, "pending ACP"),
            other => panic!("expected retryable skip, got {:?}", other),
        }
        assert!(
            !blockstore.is_merged(&cid).await.unwrap(),
            "retryable skip must leave the CID unmerged"
        );

        let second = handle_block_received(
            &coordinator,
            &handler,
            &config,
            cid,
            BlockMetadata::normal("doc1", "col1", "peer1", Some("sender1"), true),
            None,
        )
        .await;

        match second {
            ReplicationResult::Merged { .. } => {}
            other => panic!("expected replay merge, got {:?}", other),
        }
        assert!(
            blockstore.is_merged(&cid).await.unwrap(),
            "successful replay should mark the CID as merged"
        );
    }

    #[tokio::test]
    async fn test_pushlog_dag_needs_fetch_uses_poll_fetcher_when_sender_known() {
        use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};

        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));

        let child_block = Block::new(
            CrdtDelta::Lww(LwwDeltaPayload {
                doc_id: b"doc1".to_vec(),
                field_name: "name".to_string(),
                priority: 1,
                schema_version_id: "schema1".to_string(),
                data: b"value".to_vec(),
            }),
            vec![],
            vec![],
        );
        let child_data = child_block.to_dag_cbor().unwrap();
        let child_cid = child_block.generate_cid().unwrap();

        let root_block = Block::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
                doc_id: b"doc1".to_vec(),
                schema_version_id: "schema1".to_string(),
                priority: 1,
                status: 1,
            }),
            vec![],
            vec![DAGLink::new("name", child_cid)],
        );
        let root_data = root_block.to_dag_cbor().unwrap();
        let root_cid = root_block.generate_cid().unwrap();

        blockstore.put(&root_cid, &root_data).await.unwrap();

        let transport = PollFetchTransport::new(blockstore.clone(), child_cid, child_data);
        let transport_handle = transport.clone();
        let (coordinator, mut events) =
            crate::sync::coordinator::SyncCoordinator::with_access_control(
                transport,
                blockstore.clone(),
                crate::sync::SyncConfig::default(),
                AccessMode::Open,
                Arc::new(crate::ReplicatorRegistry::new()),
                Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
            )
            .await
            .unwrap();

        coordinator
            .handle_transport_event(TransportEvent::TwoStreamRequest {
                peer_id: PeerId::new("source-peer".to_string()),
                request: PushLogRequest::new(
                    "doc1".to_string(),
                    bytes::Bytes::from(root_cid.to_bytes()),
                    "col1".to_string(),
                    "creator-1".to_string(),
                    bytes::Bytes::from(root_data),
                ),
                token: None,
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
            })
            .await
            .unwrap();

        let dag_needs_fetch = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("DagNeedsFetch should arrive")
            .expect("event should be present");

        assert_eq!(coordinator.pending_dag_count(), 1);

        let result = process_event(
            &coordinator,
            dag_needs_fetch,
            &TestMergeHandler::new(true, false),
            &ReplicationConfig::default(),
        )
        .await;

        assert!(matches!(
            result,
            ReplicationResult::DagFetchStarted { root_cid: cid } if cid == root_cid
        ));

        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("DagReady should arrive")
            .expect("event should be present");

        assert!(matches!(&event, SyncEvent::DagReady { root_cid: cid, .. } if *cid == root_cid));
        assert_eq!(transport_handle.sync_blocks_calls(), 0);
        assert!(blockstore.has(&child_cid).await.unwrap());

        let merge_result = process_event(
            &coordinator,
            event,
            &TestMergeHandler::new(true, false),
            &ReplicationConfig::default(),
        )
        .await;

        assert!(matches!(
            merge_result,
            ReplicationResult::Merged { cid, .. } if cid == root_cid
        ));
        assert_eq!(coordinator.pending_dag_count(), 0);
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

    #[tokio::test]
    async fn test_run_parallel_exits_cleanly_when_worker_semaphore_closed() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let (coordinator, _events) =
            crate::sync::coordinator::SyncCoordinator::with_access_control(
                NoopTransport::new(),
                blockstore,
                crate::sync::SyncConfig::default(),
                AccessMode::Open,
                Arc::new(crate::ReplicatorRegistry::new()),
                Arc::new(crate::sync::collection_store::NoOpCollectionStorage),
            )
            .await
            .unwrap();

        let (tx, rx) = mpsc::channel(1);
        tx.send(SyncEvent::BlockReceived {
            cid: test_cid(),
            doc_id: "doc1".to_string(),
            collection_id: "col1".to_string(),
            creator: "peer1".to_string(),
            sender_peer: None,
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
            acp_actor_relationships: None,
        })
        .await
        .unwrap();
        drop(tx);

        let semaphore = Arc::new(Semaphore::new(1));
        semaphore.close();

        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = callback_count.clone();
        let handler = Arc::new(TestMergeHandler::new(true, false));

        tokio::time::timeout(
            Duration::from_secs(1),
            ReplicationLoop::run_parallel_with_semaphore(
                Arc::new(coordinator),
                rx,
                handler,
                ReplicationConfig::default(),
                move |_| {
                    callback_count_clone.fetch_add(1, Ordering::SeqCst);
                },
                semaphore,
            ),
        )
        .await
        .expect("parallel replication loop should exit when semaphore is closed");

        assert_eq!(
            callback_count.load(Ordering::SeqCst),
            0,
            "closed semaphore should stop the loop before any worker starts"
        );
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
                    block_data: bytes::Bytes::from(data.into_bytes()),
                    doc_id: format!("doc{}", i),
                    collection_id: "col1".to_string(),
                    creator: "peer1".to_string(),
                    sender_peer: None,
                    is_explicit_replicator: false,
                    explicit_replay_authorization: None,
                    verified_creator: None,
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
                    block_data: bytes::Bytes::from(data.into_bytes()),
                    doc_id: format!("doc{}", i),
                    collection_id: "col1".to_string(),
                    creator: "peer1".to_string(),
                    sender_peer: None,
                    is_explicit_replicator: false,
                    explicit_replay_authorization: None,
                    verified_creator: None,
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
                    block_data: bytes::Bytes::from(data.into_bytes()),
                    doc_id: format!("doc{}", i),
                    collection_id: "col1".to_string(),
                    creator: "peer1".to_string(),
                    sender_peer: None,
                    is_explicit_replicator: false,
                    explicit_replay_authorization: None,
                    verified_creator: None,
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
            block_data: bytes::Bytes::from_static(b"single"),
            doc_id: "doc0".to_string(),
            collection_id: "col1".to_string(),
            creator: "peer1".to_string(),
            sender_peer: None,
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
            verified_creator: None,
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
            block_data: bytes::Bytes::from_static(b"test data"),
            doc_id: "my-doc".to_string(),
            collection_id: "my-collection".to_string(),
            creator: "my-peer".to_string(),
            sender_peer: None,
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
            verified_creator: None,
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
                    block_data: bytes::Bytes::from(data.into_bytes()),
                    doc_id: format!("doc{}", i),
                    collection_id: "col1".to_string(),
                    creator: "peer1".to_string(),
                    sender_peer: None,
                    is_explicit_replicator: false,
                    explicit_replay_authorization: None,
                    verified_creator: None,
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
                    block_data: bytes::Bytes::from(data.into_bytes()),
                    doc_id: format!("doc{}", i),
                    collection_id: "col1".to_string(),
                    creator: "peer1".to_string(),
                    sender_peer: None,
                    is_explicit_replicator: false,
                    explicit_replay_authorization: None,
                    verified_creator: None,
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
