//! Coordinator-side public API for the pubsub_rpc DocSync /
//! BranchableSync services.
//!
//! - [`SyncCoordinator::start_pubsub_services`] — subscribe the base and
//!   response sub-topics, register them with the transport, and mark the
//!   coordinator as ready to serve inbound requests.
//! - [`SyncCoordinator::pubsub_sync_documents`] — caller-side DocSync
//!   publish; returns collected [`DocSyncReply`]s.
//! - [`SyncCoordinator::pubsub_sync_branchable_collection`] — caller-side
//!   BranchableSync publish.
//!
//! These are all no-ops on transports whose `local_peer_id()` isn't a
//! libp2p PeerId (iroh), so the existing two-stream paths remain the
//! functional ones for those transports.

use std::time::Duration;

use blockstore::Blockstore;
use tracing::{debug, warn};

use super::pubsub_services::{BRANCHABLE_SYNC_TOPIC, DOC_SYNC_TOPIC};
use super::SyncCoordinator;
use crate::error::{Error, Result};
use crate::message::pubsub as wire;
use crate::message::{
    BranchableSyncReply as TwoStreamBranchableSyncReply, DocSyncItem as TwoStreamDocSyncItem,
    DocSyncReply as TwoStreamDocSyncReply,
};
use crate::pubsub_rpc::{PublishOptions, PubsubResponse};
use crate::transport::{P2PTransport, PeerId};

/// Default wait for DocSync / BranchableSync responses before returning
/// whatever has arrived. Matches Go's `5*time.Second` fallback in
/// `sync_doc.go:125`.
const DEFAULT_PUBSUB_SYNC_TIMEOUT: Duration = Duration::from_secs(5);

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Subscribe to `doc-sync`, `sync-branchable`, and our per-peer
    /// `<base>/<self>/_response` sub-topics, then register all four with
    /// the transport so inbound messages arrive as
    /// [`crate::transport::TransportEvent::GossipRawMessage`].
    ///
    /// Idempotent: safe to call at startup and again on reconnect. Returns
    /// an error if any subscription or registration fails so callers never
    /// observe a partially wired pubsub_rpc service as ready.
    pub async fn start_pubsub_services(&self) -> Result<()> {
        let Some(services) = self.pubsub_services.as_ref() else {
            debug!("pubsub_rpc services disabled (local peer is not a libp2p PeerId)");
            return Ok(());
        };
        services.set_ready(false);

        let doc_self = services.doc_sync.self_response_topic().to_string();
        let branch_self = services.branchable_sync.self_response_topic().to_string();

        for topic in [
            DOC_SYNC_TOPIC.to_string(),
            BRANCHABLE_SYNC_TOPIC.to_string(),
            doc_self.clone(),
            branch_self.clone(),
        ] {
            self.runtime.transport.subscribe_raw(topic.clone()).await?;
            self.runtime
                .transport
                .register_pubsub_rpc_topic(topic)
                .await?;
        }

        services.set_ready(true);
        debug!(
            doc_sync_topic = DOC_SYNC_TOPIC,
            branchable_topic = BRANCHABLE_SYNC_TOPIC,
            "pubsub_rpc services started"
        );
        Ok(())
    }

    pub fn pubsub_services_ready(&self) -> bool {
        self.pubsub_services
            .as_ref()
            .is_some_and(|services| services.is_ready())
    }

    /// Publish a DocSync request over `doc-sync` and wait up to
    /// `timeout` (default 5s) for responses. Matches the behavior of Go's
    /// `SyncDocuments` in `sync_doc.go:61-88`.
    pub async fn pubsub_sync_documents(
        &self,
        doc_ids: Vec<String>,
        timeout: Option<Duration>,
        expected_responses: Option<usize>,
    ) -> Result<Vec<(String, wire::DocSyncReply)>> {
        let Some(services) = self.pubsub_services.as_ref() else {
            return Err(Error::Transport(
                "pubsub_rpc DocSync is not available on this transport".into(),
            ));
        };
        if !services.is_ready() {
            return Err(Error::Transport(
                "pubsub_rpc DocSync is not ready on this transport".into(),
            ));
        }

        let mut req_bytes = Vec::new();
        ciborium::into_writer(&wire::DocSyncRequest::new(doc_ids), &mut req_bytes)
            .map_err(|e| Error::CborSerialization(e.to_string()))?;

        let mut prep = services.doc_sync.prepare_publish(
            req_bytes,
            PublishOptions {
                multi_response: true,
                ..Default::default()
            },
        );

        if let Err(e) = self
            .runtime
            .transport
            .publish_raw(DOC_SYNC_TOPIC.to_string(), prep.data.clone())
            .await
        {
            warn!(error = %e, "doc-sync publish_raw failed");
            return Err(e);
        }

        let wait = timeout.unwrap_or(DEFAULT_PUBSUB_SYNC_TIMEOUT);
        let deadline = tokio::time::Instant::now() + wait;
        let mut out = Vec::new();
        loop {
            if expected_responses.is_some_and(|expected| out.len() >= expected) {
                break;
            }

            let Some(remaining) = deadline.checked_duration_since(tokio::time::Instant::now())
            else {
                break;
            };

            let Ok(Some(resp)) = tokio::time::timeout(remaining, prep.responses.recv()).await
            else {
                break;
            };

            if let Some(parsed) = parse_doc_sync_response(&resp) {
                let peer_str = resp.from.to_string();
                // Feed the reply into the coordinator's standard handler
                // so DAG fetches and merges trigger just like the
                // two-stream path. Converted to the two-stream struct
                // shape (which has a MetaData header) so we can reuse
                // the existing handle_doc_sync_reply logic.
                let converted = TwoStreamDocSyncReply {
                    version: crate::protocol::MESSAGE_VERSION.to_string(),
                    message_id: String::new(),
                    sender_id: parsed.sender.clone(),
                    pubkey: Vec::new(),
                    signature: None,
                    err_message: None,
                    results: parsed
                        .results
                        .iter()
                        .map(|item| TwoStreamDocSyncItem {
                            doc_id: item.doc_id.clone(),
                            heads: item.heads.clone(),
                        })
                        .collect(),
                };
                if let Err(e) = self
                    .handle_doc_sync_reply(PeerId::new(parsed.sender.clone()), converted)
                    .await
                {
                    warn!(
                        from = %peer_str,
                        sender = %parsed.sender,
                        error = %e,
                        "doc-sync: reply processing failed"
                    );
                }
                out.push((peer_str, parsed));
            }
        }
        Ok(out)
    }

    /// Publish a BranchableSync request for `collection_id` and wait up
    /// to `timeout` (default 5s) for peer replies. Matches Go's
    /// `SyncBranchableCollection`, which processes all replies that arrive
    /// before the wait context expires.
    pub async fn pubsub_sync_branchable_collection(
        &self,
        collection_id: String,
        timeout: Option<Duration>,
        expected_responses: Option<usize>,
    ) -> Result<Vec<(String, wire::BranchableSyncReply)>> {
        let Some(services) = self.pubsub_services.as_ref() else {
            return Err(Error::Transport(
                "pubsub_rpc BranchableSync is not available on this transport".into(),
            ));
        };
        if !services.is_ready() {
            return Err(Error::Transport(
                "pubsub_rpc BranchableSync is not ready on this transport".into(),
            ));
        }

        let mut req_bytes = Vec::new();
        ciborium::into_writer(
            &wire::BranchableSyncRequest::new(collection_id),
            &mut req_bytes,
        )
        .map_err(|e| Error::CborSerialization(e.to_string()))?;

        let mut prep = services.branchable_sync.prepare_publish(
            req_bytes,
            PublishOptions {
                multi_response: true,
                ..Default::default()
            },
        );

        if let Err(e) = self
            .runtime
            .transport
            .publish_raw(BRANCHABLE_SYNC_TOPIC.to_string(), prep.data.clone())
            .await
        {
            warn!(error = %e, "sync-branchable publish_raw failed");
            return Err(e);
        }

        let wait = timeout.unwrap_or(DEFAULT_PUBSUB_SYNC_TIMEOUT);
        let deadline = tokio::time::Instant::now() + wait;
        let mut out = Vec::new();
        loop {
            if expected_responses.is_some_and(|expected| out.len() >= expected) {
                break;
            }

            let Some(remaining) = deadline.checked_duration_since(tokio::time::Instant::now())
            else {
                break;
            };

            let Ok(Some(resp)) = tokio::time::timeout(remaining, prep.responses.recv()).await
            else {
                break;
            };

            let peer_str = resp.from.to_string();
            let Some(reply) = parse_branchable_sync_response(&resp) else {
                continue;
            };

            // Feed through the two-stream handler so DAG fetches
            // schedule the same way.
            let converted = TwoStreamBranchableSyncReply {
                version: crate::protocol::MESSAGE_VERSION.to_string(),
                message_id: String::new(),
                sender_id: reply.sender.clone(),
                pubkey: Vec::new(),
                signature: None,
                err_message: None,
                collection_id: reply.collection_id.clone(),
                heads: reply.heads.clone(),
            };
            if let Err(e) = self
                .handle_branchable_sync_reply(PeerId::new(reply.sender.clone()), converted)
                .await
            {
                warn!(
                    from = %peer_str,
                    sender = %reply.sender,
                    error = %e,
                    "sync-branchable: reply processing failed"
                );
            } else {
                out.push((peer_str, reply));
            }
        }

        Ok(out)
    }
}

fn parse_doc_sync_response(resp: &PubsubResponse) -> Option<wire::DocSyncReply> {
    if let Some(err) = &resp.err {
        warn!(from = %resp.from, error = %err, "doc-sync: peer returned error");
        return None;
    }
    match ciborium::from_reader::<wire::DocSyncReply, _>(resp.data.as_slice()) {
        Ok(r) => Some(r),
        Err(e) => {
            warn!(from = %resp.from, error = %e, "doc-sync: failed to decode reply");
            None
        }
    }
}

fn parse_branchable_sync_response(resp: &PubsubResponse) -> Option<wire::BranchableSyncReply> {
    if let Some(err) = &resp.err {
        warn!(from = %resp.from, error = %err, "sync-branchable: peer returned error");
        return None;
    }
    match ciborium::from_reader::<wire::BranchableSyncReply, _>(resp.data.as_slice()) {
        Ok(r) => Some(r),
        Err(e) => {
            warn!(from = %resp.from, error = %e, "sync-branchable: failed to decode reply");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use blockstore::DefraBlockstore;
    use parking_lot::Mutex;
    use storage::backends::MemoryStore;

    use super::*;
    use crate::message::{
        BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
        PushLogReply, PushLogRequest, PushSEArtifactsRequest,
    };
    use crate::sync::SyncConfig;
    use crate::topics::DefraTopic;
    use crate::transport::{MessageId, PeerAddr, PeerId};
    use crate::{QueryId, ReplicatorInfo};

    #[derive(Clone)]
    struct RawSubscribeFailTransport {
        local_peer_id: PeerId,
        subscribe_calls: Arc<AtomicUsize>,
        fail_on_call: usize,
        registered_topics: Arc<Mutex<Vec<String>>>,
    }

    impl RawSubscribeFailTransport {
        fn new(fail_on_call: usize) -> Self {
            let peer = libp2p::PeerId::from_public_key(
                &libp2p::identity::Keypair::generate_ed25519().public(),
            );
            Self {
                local_peer_id: PeerId::new(peer.to_string()),
                subscribe_calls: Arc::new(AtomicUsize::new(0)),
                fail_on_call,
                registered_topics: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl P2PTransport for RawSubscribeFailTransport {
        type ResponseToken = ();

        fn local_peer_id(&self) -> &PeerId {
            &self.local_peer_id
        }

        fn local_public_key_proto(&self) -> &[u8] {
            &[]
        }

        fn sign(&self, _data: &[u8]) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn dial(&self, _peer_id: &PeerId, _addrs: Vec<PeerAddr>) -> Result<()> {
            Ok(())
        }

        async fn disconnect(&self, _peer_id: &PeerId) -> Result<()> {
            Ok(())
        }

        async fn listen(&self, _addr: PeerAddr) -> Result<()> {
            Ok(())
        }

        async fn connected_peers(&self) -> Result<Vec<PeerId>> {
            Ok(Vec::new())
        }

        async fn listen_addresses(&self) -> Result<Vec<PeerAddr>> {
            Ok(Vec::new())
        }

        async fn poll_until_connected(&self, _peer_id: &PeerId, _timeout: Duration) -> Result<()> {
            Ok(())
        }

        async fn peer_addresses(&self) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn topic_peers(&self, _topic: DefraTopic) -> Result<Vec<PeerId>> {
            Ok(Vec::new())
        }

        async fn subscribe(&self, _topic: DefraTopic) -> Result<bool> {
            Ok(true)
        }

        async fn unsubscribe(&self, _topic: DefraTopic) -> Result<bool> {
            Ok(true)
        }

        async fn publish(&self, _topic: DefraTopic, _msg: PushLogBroadcast) -> Result<MessageId> {
            Ok(MessageId::new("noop".to_string()))
        }

        async fn publish_raw(&self, _topic: String, _data: Vec<u8>) -> Result<MessageId> {
            Ok(MessageId::new("noop".to_string()))
        }

        async fn subscribe_raw(&self, _topic: String) -> Result<bool> {
            let call = self.subscribe_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.fail_on_call {
                Err(Error::Transport("subscribe failed".to_string()))
            } else {
                Ok(true)
            }
        }

        async fn register_pubsub_rpc_topic(&self, topic: String) -> Result<()> {
            self.registered_topics.lock().push(topic);
            Ok(())
        }

        async fn send_pushlog_response(
            &self,
            _token: Self::ResponseToken,
            _reply: PushLogReply,
        ) -> Result<()> {
            Ok(())
        }

        async fn send_two_stream_request(
            &self,
            _peer_id: &PeerId,
            _req: PushLogRequest,
        ) -> Result<PushLogReply> {
            Err(Error::Transport("not implemented".to_string()))
        }

        async fn send_two_stream_response(
            &self,
            _peer_id: &PeerId,
            _reply: PushLogReply,
        ) -> Result<()> {
            Ok(())
        }

        async fn send_doc_sync_request(
            &self,
            _peer_id: &PeerId,
            _req: DocSyncRequest,
        ) -> Result<()> {
            Ok(())
        }

        async fn send_doc_sync_response(
            &self,
            _peer_id: &PeerId,
            _reply: DocSyncReply,
        ) -> Result<()> {
            Ok(())
        }

        async fn send_branchable_sync_request(
            &self,
            _peer_id: &PeerId,
            _req: BranchableSyncRequest,
        ) -> Result<()> {
            Ok(())
        }

        async fn send_branchable_sync_response(
            &self,
            _peer_id: &PeerId,
            _reply: BranchableSyncReply,
        ) -> Result<()> {
            Ok(())
        }

        async fn send_car_request(&self, _peer_id: &PeerId, _root_cid: cid::Cid) -> Result<()> {
            Ok(())
        }

        async fn send_car_response(&self, _peer_id: &PeerId, _car_data: Vec<u8>) -> Result<()> {
            Ok(())
        }

        async fn send_car_response_token(
            &self,
            _token: Self::ResponseToken,
            _car_data: Vec<u8>,
        ) -> Result<()> {
            Ok(())
        }

        async fn send_doc_sync_response_token(
            &self,
            _token: Self::ResponseToken,
            _reply: DocSyncReply,
        ) -> Result<()> {
            Ok(())
        }

        async fn send_branchable_sync_response_token(
            &self,
            _token: Self::ResponseToken,
            _reply: BranchableSyncReply,
        ) -> Result<()> {
            Ok(())
        }

        async fn send_se_artifacts(
            &self,
            _peer_id: &PeerId,
            _req: PushSEArtifactsRequest,
        ) -> Result<()> {
            Ok(())
        }

        async fn sync_blocks(
            &self,
            _root: cid::Cid,
            _providers: Vec<PeerId>,
            _missing: Vec<cid::Cid>,
        ) -> Result<QueryId> {
            Ok(QueryId(0))
        }

        async fn cancel_sync(&self, _query_id: QueryId) -> Result<bool> {
            Ok(true)
        }

        async fn create_replicator(
            &self,
            _peer_id: &PeerId,
            _collections: Vec<String>,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_replicator(&self, _peer_id: &PeerId) -> Result<()> {
            Ok(())
        }

        async fn list_replicators(&self) -> Result<Vec<ReplicatorInfo>> {
            Ok(Vec::new())
        }

        async fn get_replicator(&self, _peer_id: &PeerId) -> Result<Option<ReplicatorInfo>> {
            Ok(None)
        }

        async fn remove_replicator_collections(
            &self,
            _peer_id: &PeerId,
            _collections: Vec<String>,
        ) -> Result<bool> {
            Ok(false)
        }

        async fn shutdown(&self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn start_pubsub_services_returns_error_and_stays_unready_on_partial_subscribe() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let transport = RawSubscribeFailTransport::new(2);
        let registered_topics = transport.registered_topics.clone();
        let subscribe_calls = transport.subscribe_calls.clone();
        let (coordinator, _events) =
            SyncCoordinator::new(transport, blockstore, SyncConfig::default())
                .await
                .expect("coordinator should construct");

        let error = coordinator
            .start_pubsub_services()
            .await
            .expect_err("partial subscribe failure must be visible to caller");

        assert!(error.to_string().contains("subscribe failed"));
        assert!(!coordinator.pubsub_services_ready());
        assert_eq!(subscribe_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            registered_topics.lock().len(),
            1,
            "first topic registered before failure, but services must remain unready"
        );
    }
}
