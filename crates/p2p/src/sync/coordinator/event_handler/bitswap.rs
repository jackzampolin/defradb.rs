//! Bitswap block and completion event handling.

use cid::Cid;

use blockstore::Blockstore;

use super::super::SyncCoordinator;
use crate::error::Result;
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    async fn retry_pending_root_after_bitswap(
        &self,
        query_id: crate::QueryId,
        root_cid: Cid,
    ) -> Result<()> {
        match self.manager.retry_pending_dag(&root_cid).await {
            Ok(true) => {
                tracing::debug!(
                    query_id = query_id.0,
                    root_cid = %root_cid,
                    "Pending DAG completed after Bitswap activity"
                );
            }
            Ok(false) => {
                let missing = self.manager.pending_dag_missing(&root_cid);
                if !missing.is_empty() {
                    let mut providers: Vec<PeerId> = self
                        .access
                        .peer_state
                        .connected_peers()
                        .into_iter()
                        .map(PeerId::new)
                        .collect();
                    if let Some(source) = self.manager.pending_dag_source_peer(&root_cid) {
                        let source_transport_id = PeerId::new(source);
                        if !providers.contains(&source_transport_id) {
                            providers.push(source_transport_id);
                        }
                    }
                    match self
                        .runtime
                        .transport
                        .sync_blocks(root_cid, providers, missing)
                        .await
                    {
                        Ok(retry_query_id) => {
                            self.manager.register_query(retry_query_id, root_cid);
                            tracing::debug!(
                                query_id = query_id.0,
                                retry_query_id = retry_query_id.0,
                                root_cid = %root_cid,
                                "Started Bitswap fetch for remaining child blocks"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                query_id = query_id.0,
                                root_cid = %root_cid,
                                error = %e,
                                "Failed to start Bitswap fetch for remaining child blocks"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    query_id = query_id.0,
                    root_cid = %root_cid,
                    error = %e,
                    "Failed to retry pending DAG"
                );
            }
        }
        Ok(())
    }

    pub(super) async fn handle_bitswap_block_received(
        &self,
        query_id: crate::QueryId,
        cid: Cid,
        data: Vec<u8>,
    ) -> Result<()> {
        tracing::info!(
            query_id = query_id.0,
            cid = %cid,
            data_len = data.len(),
            "Storing Bitswap block in blockstore"
        );

        match self.manager.store_bitswap_block(&cid, &data).await {
            Ok(true) => {
                tracing::debug!(
                    query_id = query_id.0,
                    cid = %cid,
                    "Bitswap block stored successfully"
                );
            }
            Ok(false) => {
                tracing::debug!(
                    query_id = query_id.0,
                    cid = %cid,
                    "Bitswap block was already in blockstore"
                );
            }
            Err(e) => {
                tracing::error!(
                    query_id = query_id.0,
                    cid = %cid,
                    error = %e,
                    "Failed to store Bitswap block"
                );
                return Err(e);
            }
        }

        let completed_roots = self.manager.retry_pending_dags_waiting_on(&cid).await?;
        if !completed_roots.is_empty() {
            tracing::debug!(
                query_id = query_id.0,
                received_cid = %cid,
                completed_count = completed_roots.len(),
                completed_roots = ?completed_roots,
                "Bitswap block completed pending DAGs waiting on this CID"
            );
        }
        Ok(())
    }

    pub(super) async fn handle_bitswap_complete(
        &self,
        query_id: crate::QueryId,
        success: bool,
        error: Option<String>,
    ) -> Result<()> {
        let Some(root_cid) = self.manager.take_query_root(query_id) else {
            tracing::debug!(
                query_id = query_id.0,
                "Bitswap fetch completed for unknown query, ignoring"
            );
            return Ok(());
        };

        if success {
            tracing::debug!(
                query_id = query_id.0,
                root_cid = %root_cid,
                "Bitswap fetch completed"
            );
            self.manager.clear_pending_dag_fetch_failures(&root_cid);
        } else if let Some(ref err) = error {
            if let Some(snapshot) = self
                .manager
                .record_pending_dag_fetch_failure(&root_cid, err)
            {
                let sampled_warn =
                    snapshot.fetch_failures == 1 || snapshot.fetch_failures.is_power_of_two();
                if sampled_warn {
                    tracing::warn!(
                        query_id = query_id.0,
                        root_cid = %root_cid,
                        doc_id = %snapshot.doc_id,
                        collection_id = %snapshot.collection_id,
                        source_peer = ?snapshot.source_peer,
                        missing_count = snapshot.missing_count,
                        fetch_failures = snapshot.fetch_failures,
                        error = %err,
                        "Bitswap fetch failed after exhausting providers; pending DAG remains unresolved"
                    );
                } else {
                    tracing::debug!(
                        query_id = query_id.0,
                        root_cid = %root_cid,
                        doc_id = %snapshot.doc_id,
                        collection_id = %snapshot.collection_id,
                        source_peer = ?snapshot.source_peer,
                        missing_count = snapshot.missing_count,
                        fetch_failures = snapshot.fetch_failures,
                        error = %err,
                        "Bitswap fetch failed after exhausting providers; pending DAG remains unresolved"
                    );
                }
            } else {
                tracing::debug!(
                    query_id = query_id.0,
                    root_cid = %root_cid,
                    error = %err,
                    "Bitswap fetch failed after exhausting providers for unknown pending DAG"
                );
            }
        }

        self.retry_pending_root_after_bitswap(query_id, root_cid)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use blockstore::DefraBlockstore;
    use bytes::Bytes;
    use defra_core::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use storage::backends::MemoryStore;

    use crate::error::Result as P2PResult;
    use crate::message::{
        BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
        PushLogReply, PushLogRequest, PushSEArtifactsRequest,
    };
    use crate::sync::{SyncConfig, SyncCoordinator, SyncEvent};
    use crate::topics::DefraTopic;
    use crate::transport::{MessageId, P2PTransport, PeerAddr};
    use crate::{QueryId, ReplicatorInfo};

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SyncCall {
        root: Cid,
        providers: Vec<PeerId>,
        missing: Vec<Cid>,
    }

    #[derive(Clone)]
    struct TestTransport {
        peer_id: PeerId,
        pubkey: Vec<u8>,
        sync_calls: Arc<Mutex<Vec<SyncCall>>>,
    }

    impl TestTransport {
        fn new() -> Self {
            Self {
                peer_id: PeerId::new("local-peer".to_string()),
                pubkey: vec![1, 2, 3],
                sync_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn sync_calls(&self) -> Vec<SyncCall> {
            self.sync_calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl P2PTransport for TestTransport {
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

        async fn topic_peers(&self, _topic: DefraTopic) -> P2PResult<Vec<PeerId>> {
            Ok(Vec::new())
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
            root: Cid,
            providers: Vec<PeerId>,
            missing: Vec<Cid>,
        ) -> P2PResult<QueryId> {
            self.sync_calls.lock().unwrap().push(SyncCall {
                root,
                providers,
                missing,
            });
            Ok(QueryId(999))
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

    fn create_lww_block(field_name: &str) -> (Cid, Vec<u8>) {
        let block = Block::new(
            CrdtDelta::Lww(LwwDeltaPayload {
                doc_id: b"doc123".to_vec(),
                field_name: field_name.to_string(),
                priority: 1,
                schema_version_id: "schema1".to_string(),
                data: b"value".to_vec(),
            }),
            vec![],
            vec![],
        );
        let bytes = block.to_dag_cbor().expect("encode lww block");
        let cid = block.generate_cid().expect("generate lww cid");
        (cid, bytes)
    }

    fn create_composite_block(doc_id: &str, field_name: &str, field_cid: Cid) -> (Cid, Vec<u8>) {
        let block = Block::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
                doc_id: doc_id.as_bytes().to_vec(),
                schema_version_id: "schema1".to_string(),
                priority: 1,
                status: 1,
            }),
            vec![],
            vec![DAGLink::new(field_name, field_cid)],
        );
        let bytes = block.to_dag_cbor().expect("encode composite block");
        let cid = block.generate_cid().expect("generate composite cid");
        (cid, bytes)
    }

    fn make_broadcast(
        doc_id: &str,
        cid: Cid,
        block: Vec<u8>,
        collection_id: &str,
    ) -> PushLogBroadcast {
        PushLogBroadcast::new(
            doc_id.to_string(),
            Bytes::from(cid.to_bytes()),
            collection_id.to_string(),
            "creator1".to_string(),
            Bytes::from(block),
            None,
        )
    }

    #[tokio::test]
    async fn bitswap_block_received_retries_only_waiting_dags() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let transport = TestTransport::new();
        let (coordinator, mut events) =
            SyncCoordinator::new(transport, blockstore, SyncConfig::default())
                .await
                .expect("coordinator");

        let (field_cid, field_block) = create_lww_block("name");
        let (root_cid, root_block) = create_composite_block("doc123", "name", field_cid);

        coordinator
            .manager()
            .process_pushlog(
                &make_broadcast("doc123", root_cid, root_block, "collection1"),
                Some("peer-1"),
                false,
                None,
            )
            .await
            .expect("root pushlog");

        match events.try_recv().expect("DagNeedsFetch event") {
            SyncEvent::DagNeedsFetch {
                root_cid: event_root,
                ..
            } => {
                assert_eq!(event_root, root_cid);
            }
            other => panic!("expected DagNeedsFetch, got {:?}", other),
        }

        coordinator
            .handle_bitswap_block_received(QueryId(42), field_cid, field_block)
            .await
            .expect("bitswap block received");

        match tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("event")
            .expect("channel open")
        {
            SyncEvent::DagReady {
                root_cid: event_root,
                doc_id,
                ..
            } => {
                assert_eq!(event_root, root_cid);
                assert_eq!(doc_id, "doc123");
            }
            other => panic!("expected DagReady, got {:?}", other),
        }

        assert_eq!(coordinator.manager().pending_dag_count(), 0);
    }

    #[tokio::test]
    async fn bitswap_complete_retries_only_its_registered_root() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let transport = TestTransport::new();
        let transport_handle = transport.clone();
        let (coordinator, mut events) =
            SyncCoordinator::new(transport, blockstore, SyncConfig::default())
                .await
                .expect("coordinator");

        let (field1_cid, _field1_block) = create_lww_block("name");
        let (field2_cid, _field2_block) = create_lww_block("age");
        let (root1_cid, root1_block) = create_composite_block("doc1", "name", field1_cid);
        let (root2_cid, root2_block) = create_composite_block("doc2", "age", field2_cid);

        coordinator
            .manager()
            .process_pushlog(
                &make_broadcast("doc1", root1_cid, root1_block, "collection1"),
                Some("peer-1"),
                false,
                None,
            )
            .await
            .expect("first pending root");
        coordinator
            .manager()
            .process_pushlog(
                &make_broadcast("doc2", root2_cid, root2_block, "collection2"),
                Some("peer-2"),
                false,
                None,
            )
            .await
            .expect("second pending root");

        for _ in 0..2 {
            let _ = events.try_recv().expect("DagNeedsFetch event");
        }

        let query_id = QueryId(7);
        coordinator.manager().register_query(query_id, root1_cid);
        coordinator
            .handle_bitswap_complete(
                query_id,
                false,
                Some("selective CAR fetch failed".to_string()),
            )
            .await
            .expect("bitswap complete");

        let sync_calls = transport_handle.sync_calls();
        assert_eq!(sync_calls.len(), 1, "only one pending root should retry");
        assert_eq!(sync_calls[0].root, root1_cid);
        assert_eq!(sync_calls[0].missing, vec![field1_cid]);
        assert_eq!(
            sync_calls[0].providers,
            vec![PeerId::new("peer-1".to_string())]
        );
        assert_eq!(coordinator.manager().pending_dag_attempts(&root1_cid), 1);
        assert_eq!(coordinator.manager().pending_dag_attempts(&root2_cid), 0);

        coordinator
            .handle_bitswap_complete(
                QueryId(999),
                false,
                Some("remaining child fetch failed".to_string()),
            )
            .await
            .expect("registered retry bitswap complete");
        assert_eq!(
            transport_handle.sync_calls().len(),
            2,
            "retry query completion should trigger another retry"
        );
        assert_eq!(coordinator.manager().pending_dag_attempts(&root1_cid), 2);
        assert_eq!(coordinator.manager().pending_dag_attempts(&root2_cid), 0);

        coordinator
            .handle_bitswap_complete(QueryId(9999), false, Some("ignored".to_string()))
            .await
            .expect("unknown query is ignored");
        assert_eq!(
            transport_handle.sync_calls().len(),
            2,
            "unknown query should not trigger another retry"
        );
    }
}
