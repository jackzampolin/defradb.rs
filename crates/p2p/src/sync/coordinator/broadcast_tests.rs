use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use multihash_codetable::{Code, MultihashDigest};

use crate::error::{Result as P2PResult, RATE_LIMITED_MESSAGE};
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
    PushLogReply, PushLogRequest, PushSEArtifactsRequest,
};
use crate::topics::DefraTopic;
use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId};
use crate::{QueryId, ReplicatorInfo};

use super::super::push_worker::send_head_hint_via_transport;
use super::*;

#[derive(Clone)]
pub(in crate::sync::coordinator) struct SentPush {
    pub(in crate::sync::coordinator) peer_id: String,
    pub(in crate::sync::coordinator) cid: Vec<u8>,
    pub(in crate::sync::coordinator) block_bytes: usize,
}

type SentLog = Vec<SentPush>;

#[derive(Clone)]
pub(in crate::sync::coordinator) struct TestTransport {
    peer_id: PeerId,
    pubkey: Vec<u8>,
    replies: Arc<Mutex<VecDeque<PushLogReply>>>,
    sent: Arc<Mutex<SentLog>>,
    stalled_peers: Arc<Mutex<std::collections::HashSet<String>>>,
    send_delay: Duration,
    signs: Arc<AtomicUsize>,
    sign_failures_remaining: Arc<AtomicUsize>,
}

impl TestTransport {
    pub(in crate::sync::coordinator) fn new(replies: Vec<PushLogReply>) -> Self {
        Self {
            peer_id: PeerId::new("local-peer".to_string()),
            pubkey: vec![1, 2, 3],
            replies: Arc::new(Mutex::new(VecDeque::from(replies))),
            sent: Arc::new(Mutex::new(Vec::new())),
            stalled_peers: Arc::new(Mutex::new(std::collections::HashSet::new())),
            send_delay: Duration::ZERO,
            signs: Arc::new(AtomicUsize::new(0)),
            sign_failures_remaining: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(in crate::sync::coordinator) fn with_send_delay(mut self, send_delay: Duration) -> Self {
        self.send_delay = send_delay;
        self
    }

    /// Sends to this peer never complete: a deterministic nonresponsive
    /// peer for worker fault-injection tests.
    pub(in crate::sync::coordinator) fn with_stalled_peer(self, peer: &str) -> Self {
        self.stalled_peers.lock().unwrap().insert(peer.to_string());
        self
    }

    pub(in crate::sync::coordinator) fn with_sign_failures(self, count: usize) -> Self {
        self.sign_failures_remaining.store(count, Ordering::Relaxed);
        self
    }

    fn sent_cids(&self) -> Vec<Vec<u8>> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .map(|push| push.cid.clone())
            .collect()
    }

    pub(in crate::sync::coordinator) fn sent(&self) -> SentLog {
        self.sent.lock().unwrap().clone()
    }

    pub(in crate::sync::coordinator) fn sign_count(&self) -> usize {
        self.signs.load(Ordering::Relaxed)
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
        self.signs.fetch_add(1, Ordering::Relaxed);
        if self
            .sign_failures_remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(crate::Error::SigningFailed("injected failure".to_string()));
        }
        Ok(vec![0])
    }

    async fn dial(&self, _peer_id: &PeerId, _addrs: Vec<PeerAddr>) -> P2PResult<()> {
        Ok(())
    }

    async fn disconnect(&self, _peer_id: &PeerId) -> P2PResult<()> {
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

    async fn poll_until_connected(&self, _peer_id: &PeerId, _timeout: Duration) -> P2PResult<()> {
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

    async fn publish(&self, _topic: DefraTopic, _msg: PushLogBroadcast) -> P2PResult<MessageId> {
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
        peer_id: &PeerId,
        req: PushLogRequest,
    ) -> P2PResult<PushLogReply> {
        if self
            .stalled_peers
            .lock()
            .unwrap()
            .contains(&peer_id.to_string())
        {
            std::future::pending::<()>().await;
        }
        self.sent.lock().unwrap().push(SentPush {
            peer_id: peer_id.to_string(),
            cid: req.cid.to_vec(),
            block_bytes: req.block.len(),
        });
        if !self.send_delay.is_zero() {
            tokio::time::sleep(self.send_delay).await;
        }
        Ok(self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| PushLogReply::success("ok")))
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

#[tokio::test]
async fn rate_limited_head_hint_is_nacked_once_for_durable_redrive() {
    let transport = TestTransport::new(vec![
        PushLogReply::error("first", RATE_LIMITED_MESSAGE),
        PushLogReply::success("first"),
        PushLogReply::success("second"),
    ]);
    let peer_id = PeerId::new("remote-peer".to_string());
    let cid1 = Cid::new_v1(0x55, Code::Sha2_256.digest(b"cid-1"));
    let request = PushLogRequest::new(
        "doc-1".to_string(),
        Bytes::from(cid1.to_bytes()),
        "collection".to_string(),
        "creator".to_string(),
        Bytes::from_static(b"block-1"),
    );

    let outcome = send_head_hint_via_transport(
        &transport,
        &peer_id,
        (cid1, request),
        Duration::from_secs(1),
    )
    .await;

    assert!(outcome.failed);
    assert_eq!(transport.sent_cids(), vec![cid1.to_bytes()]);
}

#[tokio::test]
async fn head_hint_stops_immediately_on_capacity_nack_and_parks_the_peer() {
    // defradb#1112: a saturated receiver is a PEER-WIDE, structural condition —
    // it cannot accept any new root until it drains. Answering it with the
    // rate-limit pacing ladder meant one logical push became 11 resends in
    // ~3.3s, each costing the receiver a block write plus a full DAG
    // traversal, all guaranteed to fail. The sender must stop at the first
    // capacity nack, report it, and let the persisted retry sweep own the
    // replay.
    let capacity_nack = crate::error::Error::PendingDagCapacity { max: 1 }
        .backpressure_reply_message()
        .expect("capacity error maps to the capacity sentinel");
    let transport = TestTransport::new(vec![
        PushLogReply::error("first", capacity_nack),
        PushLogReply::success("first"),
        PushLogReply::success("second"),
    ]);
    let peer_id = PeerId::new("remote-peer".to_string());
    let cid1 = Cid::new_v1(0x55, Code::Sha2_256.digest(b"cid-1"));
    let request = PushLogRequest::new(
        "doc-1".to_string(),
        Bytes::from(cid1.to_bytes()),
        "collection".to_string(),
        "creator".to_string(),
        Bytes::from_static(b"block-1"),
    );

    let outcome = send_head_hint_via_transport(
        &transport,
        &peer_id,
        (cid1, request),
        Duration::from_secs(1),
    )
    .await;

    assert!(outcome.failed, "a capacity nack is a failed push");
    assert!(
        outcome.at_capacity,
        "the caller must learn the receiver is saturated so it can park the peer"
    );
    assert_eq!(
        transport.sent_cids(),
        vec![cid1.to_bytes()],
        "the sender must NOT resend the rejected block, and must not push the \
         rejected head"
    );
}

#[tokio::test]
async fn head_hint_timeout_is_terminal_for_the_live_attempt() {
    let transport = TestTransport::new(vec![PushLogReply::success("first")])
        .with_send_delay(Duration::from_millis(25));
    let peer_id = PeerId::new("remote-peer".to_string());
    let cid1 = Cid::new_v1(0x55, Code::Sha2_256.digest(b"cid-1"));
    let request = PushLogRequest::new(
        "doc-1".to_string(),
        Bytes::from(cid1.to_bytes()),
        "collection".to_string(),
        "creator".to_string(),
        Bytes::from_static(b"block-1"),
    );

    let any_failed = send_head_hint_via_transport(
        &transport,
        &peer_id,
        (cid1, request),
        Duration::from_millis(1),
    )
    .await;

    assert!(any_failed.failed);
    assert_eq!(transport.sent_cids(), vec![cid1.to_bytes()]);
}
