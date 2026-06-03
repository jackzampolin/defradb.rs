//! `P2PTransport` implementation backed by iroh's QUIC-native networking.
//!
//! `IrohTransport` is a thin facade that sends commands to the background
//! `IrohEndpoint` event loop via an mpsc channel.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cid::Cid;
use iroh::SecretKey;
use tokio::sync::{mpsc, oneshot};

use crate::error::{Error, Result};
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, ManageQueryReply,
    ManageQueryRequest, ManageReply, ManageRequest, PushLogBroadcast, PushLogReply, PushLogRequest,
    PushSEArtifactsRequest, QuerySEArtifactsReply, QuerySEArtifactsRequest,
};
use crate::replicator::ReplicatorInfo;
use crate::topics::DefraTopic;
use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId};
use crate::QueryId;

use super::command::IrohCommand;

/// Iroh-backed P2P transport.
///
/// Implements `P2PTransport` by sending commands to a background `IrohEndpoint`
/// task via an mpsc channel. The endpoint owns all iroh state (QUIC connections,
/// gossip subscriptions, etc.).
#[derive(Clone)]
pub struct IrohTransport {
    command_tx: mpsc::Sender<IrohCommand>,
    local_peer_id: PeerId,
    local_public_key_bytes: Vec<u8>,
    secret_key: Arc<SecretKey>,
}

impl IrohTransport {
    /// Create a new `IrohTransport` with the given command channel and identity.
    pub fn new(command_tx: mpsc::Sender<IrohCommand>, secret_key: SecretKey) -> Self {
        let node_id = secret_key.public();
        let local_peer_id = PeerId::new(node_id.to_string());
        let local_public_key_bytes = node_id.as_bytes().to_vec();

        Self {
            command_tx,
            local_peer_id,
            local_public_key_bytes,
            secret_key: Arc::new(secret_key),
        }
    }

    /// Send a command and await the oneshot reply.
    async fn send_command<T>(
        &self,
        make_cmd: impl FnOnce(oneshot::Sender<Result<T>>) -> IrohCommand,
    ) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(make_cmd(tx))
            .await
            .map_err(|_| Error::ChannelSend)?;
        rx.await.map_err(|_| Error::ChannelReceive)?
    }

    /// Notify iroh that the surrounding network may have changed.
    pub async fn network_change(&self) -> Result<()> {
        self.send_command(|reply| IrohCommand::NetworkChange { reply })
            .await
    }
}

#[async_trait]
impl P2PTransport for IrohTransport {
    type ResponseToken = iroh::endpoint::SendStream;

    fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    fn local_public_key_proto(&self) -> &[u8] {
        &self.local_public_key_bytes
    }

    fn sign(&self, data: &[u8]) -> Result<Vec<u8>> {
        let signature = self.secret_key.sign(data);
        Ok(signature.to_bytes().to_vec())
    }

    async fn dial(&self, peer_id: &PeerId, addrs: Vec<PeerAddr>) -> Result<()> {
        self.send_command(|reply| IrohCommand::Dial {
            peer_id: peer_id.clone(),
            addrs,
            reply,
        })
        .await
    }

    fn parse_dial_addr(&self, addr: &str) -> Result<(PeerId, Vec<PeerAddr>)> {
        super::addr::parse_public_peer_addr(addr)
    }

    async fn listen(&self, addr: PeerAddr) -> Result<()> {
        self.send_command(|reply| IrohCommand::Listen { addr, reply })
            .await
    }

    async fn connected_peers(&self) -> Result<Vec<PeerId>> {
        self.send_command(|reply| IrohCommand::ConnectedPeers { reply })
            .await
    }

    async fn listen_addresses(&self) -> Result<Vec<PeerAddr>> {
        self.send_command(|reply| IrohCommand::ListenAddresses { reply })
            .await
    }

    async fn poll_until_connected(&self, peer_id: &PeerId, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        loop {
            let peers = self.connected_peers().await?;
            if peers.iter().any(|p| p.as_str() == peer_id.as_str()) {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                return Err(Error::ConnectionTimeout(peer_id.to_string()));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn peer_addresses(&self) -> Result<Vec<String>> {
        self.send_command(|reply| IrohCommand::PeerAddresses { reply })
            .await
    }

    async fn topic_peers(&self, topic: DefraTopic) -> Result<Vec<PeerId>> {
        self.send_command(|reply| IrohCommand::TopicPeers { topic, reply })
            .await
    }

    async fn subscribe(&self, topic: DefraTopic) -> Result<bool> {
        self.send_command(|reply| IrohCommand::Subscribe { topic, reply })
            .await
    }

    async fn unsubscribe(&self, topic: DefraTopic) -> Result<bool> {
        self.send_command(|reply| IrohCommand::Unsubscribe { topic, reply })
            .await
    }

    async fn publish(&self, topic: DefraTopic, msg: PushLogBroadcast) -> Result<MessageId> {
        self.send_command(|reply| IrohCommand::Publish { topic, msg, reply })
            .await
    }

    async fn publish_raw(&self, topic: String, data: Vec<u8>) -> Result<MessageId> {
        self.send_command(|reply| IrohCommand::PublishRaw { topic, data, reply })
            .await
    }

    async fn register_pubsub_rpc_topic(&self, topic: String) -> Result<()> {
        self.send_command(|reply| IrohCommand::RegisterRawTopic { topic, reply })
            .await
    }

    async fn subscribe_raw(&self, topic: String) -> Result<bool> {
        // Actually JOIN the iroh-gossip mesh for this raw topic and spawn a
        // reader, in addition to marking it raw-routed. The KMS pubsub
        // transport subscribes to its private `encryption/<self>/_response`
        // sub-topic here to receive reply envelopes; `RegisterRawTopic` alone
        // only classifies routing and never joins, so replies published by a
        // responder on that topic would never arrive (#976).
        self.send_command(|reply| IrohCommand::SubscribeRaw { topic, reply })
            .await
    }

    async fn send_pushlog_response(
        &self,
        send_stream: Self::ResponseToken,
        reply: PushLogReply,
    ) -> Result<()> {
        self.send_command(|r| IrohCommand::SendPushLogResponse {
            send_stream,
            reply_msg: reply,
            reply: r,
        })
        .await
    }

    async fn send_two_stream_request(
        &self,
        peer_id: &PeerId,
        req: PushLogRequest,
    ) -> Result<PushLogReply> {
        self.send_command(|reply| IrohCommand::SendTwoStreamRequest {
            peer_id: peer_id.clone(),
            request: req,
            reply,
        })
        .await
    }

    async fn send_two_stream_response(&self, peer_id: &PeerId, reply: PushLogReply) -> Result<()> {
        self.send_command(|r| IrohCommand::SendTwoStreamResponse {
            peer_id: peer_id.clone(),
            reply_msg: reply,
            reply: r,
        })
        .await
    }

    async fn send_doc_sync_request(&self, peer_id: &PeerId, req: DocSyncRequest) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendDocSyncRequest {
            peer_id: peer_id.clone(),
            request: req,
            reply,
        })
        .await
    }

    async fn send_doc_sync_response(&self, peer_id: &PeerId, reply: DocSyncReply) -> Result<()> {
        self.send_command(|r| IrohCommand::SendDocSyncResponse {
            peer_id: peer_id.clone(),
            reply_msg: reply,
            reply: r,
        })
        .await
    }

    async fn send_branchable_sync_request(
        &self,
        peer_id: &PeerId,
        req: BranchableSyncRequest,
    ) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendBranchableSyncRequest {
            peer_id: peer_id.clone(),
            request: req,
            reply,
        })
        .await
    }

    async fn send_branchable_sync_response(
        &self,
        peer_id: &PeerId,
        reply: BranchableSyncReply,
    ) -> Result<()> {
        self.send_command(|r| IrohCommand::SendBranchableSyncResponse {
            peer_id: peer_id.clone(),
            reply_msg: reply,
            reply: r,
        })
        .await
    }

    async fn send_car_request(&self, peer_id: &PeerId, root_cid: Cid) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendCarRequest {
            peer_id: peer_id.clone(),
            root_cid,
            reply,
        })
        .await
    }

    async fn send_car_response(&self, peer_id: &PeerId, car_data: Vec<u8>) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendCarResponse {
            peer_id: peer_id.clone(),
            car_data,
            reply,
        })
        .await
    }

    async fn send_doc_sync_response_token(
        &self,
        send_stream: Self::ResponseToken,
        reply: DocSyncReply,
    ) -> Result<()> {
        self.send_command(|r| IrohCommand::SendDocSyncResponseToken {
            send_stream,
            reply_msg: reply,
            reply: r,
        })
        .await
    }

    async fn send_branchable_sync_response_token(
        &self,
        send_stream: Self::ResponseToken,
        reply: BranchableSyncReply,
    ) -> Result<()> {
        self.send_command(|r| IrohCommand::SendBranchableSyncResponseToken {
            send_stream,
            reply_msg: reply,
            reply: r,
        })
        .await
    }

    async fn send_car_response_token(
        &self,
        mut send_stream: Self::ResponseToken,
        car_data: Vec<u8>,
    ) -> Result<()> {
        send_stream
            .write_all(&car_data)
            .await
            .map_err(|e| Error::ResponseSend(format!("write CAR data: {}", e)))?;
        send_stream
            .finish()
            .map_err(|e| Error::ResponseSend(format!("finish CAR stream: {}", e)))?;
        Ok(())
    }

    async fn send_se_artifacts(
        &self,
        peer_id: &PeerId,
        mut req: PushSEArtifactsRequest,
    ) -> Result<()> {
        crate::signing::sign_with_transport(self, &mut req)?;
        self.send_command(|reply| IrohCommand::SendSEArtifacts {
            peer_id: peer_id.clone(),
            request: req,
            reply,
        })
        .await
    }

    async fn send_se_query_request(
        &self,
        peer_id: &PeerId,
        req: QuerySEArtifactsRequest,
    ) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendSEQueryRequest {
            peer_id: peer_id.clone(),
            request: req,
            reply,
        })
        .await
    }

    async fn send_se_query_response(
        &self,
        peer_id: &PeerId,
        reply_msg: QuerySEArtifactsReply,
    ) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendSEQueryResponse {
            peer_id: peer_id.clone(),
            reply_msg,
            reply,
        })
        .await
    }

    async fn send_manage_request(&self, peer_id: &PeerId, req: ManageRequest) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendManageRequest {
            peer_id: peer_id.clone(),
            request: req,
            reply,
        })
        .await
    }

    async fn send_manage_response(&self, peer_id: &PeerId, reply_msg: ManageReply) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendManageResponse {
            peer_id: peer_id.clone(),
            reply_msg,
            reply,
        })
        .await
    }

    async fn send_manage_query_request(
        &self,
        peer_id: &PeerId,
        req: ManageQueryRequest,
    ) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendManageQueryRequest {
            peer_id: peer_id.clone(),
            request: req,
            reply,
        })
        .await
    }

    async fn send_manage_query_response(
        &self,
        peer_id: &PeerId,
        reply_msg: ManageQueryReply,
    ) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendManageQueryResponse {
            peer_id: peer_id.clone(),
            reply_msg,
            reply,
        })
        .await
    }

    async fn sync_blocks(
        &self,
        root: Cid,
        providers: Vec<PeerId>,
        missing: Vec<Cid>,
    ) -> Result<QueryId> {
        self.send_command(|reply| IrohCommand::SyncBlocks {
            root,
            providers,
            missing,
            reply,
        })
        .await
    }

    async fn cancel_sync(&self, query_id: QueryId) -> Result<bool> {
        self.send_command(|reply| IrohCommand::CancelSync { query_id, reply })
            .await
    }

    async fn create_replicator(&self, peer_id: &PeerId, collections: Vec<String>) -> Result<()> {
        self.send_command(|reply| IrohCommand::CreateReplicator {
            peer_id: peer_id.clone(),
            collections,
            reply,
        })
        .await
    }

    async fn delete_replicator(&self, peer_id: &PeerId) -> Result<()> {
        self.send_command(|reply| IrohCommand::DeleteReplicator {
            peer_id: peer_id.clone(),
            reply,
        })
        .await
    }

    async fn list_replicators(&self) -> Result<Vec<ReplicatorInfo>> {
        self.send_command(|reply| IrohCommand::ListReplicators { reply })
            .await
    }

    async fn get_replicator(&self, peer_id: &PeerId) -> Result<Option<ReplicatorInfo>> {
        self.send_command(|reply| IrohCommand::GetReplicator {
            peer_id: peer_id.clone(),
            reply,
        })
        .await
    }

    async fn remove_replicator_collections(
        &self,
        peer_id: &PeerId,
        collections: Vec<String>,
    ) -> Result<bool> {
        self.send_command(|reply| IrohCommand::RemoveReplicatorCollections {
            peer_id: peer_id.clone(),
            collections,
            reply,
        })
        .await
    }

    async fn shutdown(&self) -> Result<()> {
        let send_started = std::time::Instant::now();
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(IrohCommand::Shutdown { reply: tx })
            .await
            .map_err(|_| Error::ChannelSend)?;
        let send_elapsed = send_started.elapsed();

        let reply_started = std::time::Instant::now();
        let result = rx.await.map_err(|_| Error::ChannelReceive)?;
        tracing::warn!(
            send_elapsed_ms = send_elapsed.as_millis(),
            reply_elapsed_ms = reply_started.elapsed().as_millis(),
            "Iroh transport shutdown command completed"
        );
        result
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    use tokio::time::timeout;

    use crate::iroh::{spawn_endpoint, IrohDiscoveryConfig, IrohEndpointConfig};
    use crate::message::{
        PushSEArtifactsRequest, QuerySEArtifactsReply, QuerySEArtifactsRequest, SEArtifact,
        SEFieldQuery,
    };
    use crate::signing::sign_with_transport;
    use crate::transport::{P2PTransport, TransportEvent};

    use super::*;

    fn test_config(secret_key: SecretKey) -> IrohEndpointConfig {
        IrohEndpointConfig {
            secret_key,
            relay_mode: crate::iroh::IrohRelayModeConfig::Disabled,
            discovery: IrohDiscoveryConfig::Disabled,
            bind_port: None,
            bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        }
    }

    #[tokio::test]
    async fn se_query_roundtrip_emits_request_and_reply_events() {
        let key0 = SecretKey::generate();
        let key1 = SecretKey::generate();
        let (command_tx0, mut events0, _replicators0, task0) =
            spawn_endpoint(test_config(key0.clone())).await.unwrap();
        let (command_tx1, mut events1, _replicators1, task1) =
            spawn_endpoint(test_config(key1.clone())).await.unwrap();
        let transport0 = IrohTransport::new(command_tx0, key0);
        let transport1 = IrohTransport::new(command_tx1, key1);

        transport0
            .dial(
                transport1.local_peer_id(),
                transport1.listen_addresses().await.unwrap(),
            )
            .await
            .unwrap();
        transport0
            .poll_until_connected(transport1.local_peer_id(), Duration::from_secs(5))
            .await
            .unwrap();
        transport1
            .poll_until_connected(transport0.local_peer_id(), Duration::from_secs(5))
            .await
            .unwrap();

        let mut request = QuerySEArtifactsRequest::new(
            "collection1",
            vec![SEFieldQuery::new("name", "name", vec![1, 2, 3])],
        );
        sign_with_transport(&transport0, &mut request).unwrap();
        let request_message_id = request.message_id.clone();

        transport0
            .send_se_query_request(transport1.local_peer_id(), request)
            .await
            .unwrap();

        let received_request = loop {
            let event = timeout(Duration::from_secs(5), events1.recv())
                .await
                .expect("timed out waiting for SE query request")
                .expect("iroh event channel closed");
            if let TransportEvent::SEQueryRequest { peer_id, request } = event {
                assert_eq!(peer_id, *transport0.local_peer_id());
                break request;
            }
        };
        assert_eq!(received_request.message_id, request_message_id);
        assert_eq!(
            received_request.sender_id,
            transport0.local_peer_id().to_string()
        );
        assert_eq!(received_request.collection_id, "collection1");
        assert_eq!(received_request.queries.len(), 1);
        assert_eq!(received_request.queries[0].search_tag, vec![1, 2, 3]);

        let mut reply =
            QuerySEArtifactsReply::success(&request_message_id, vec!["doc1".into(), "doc2".into()]);
        sign_with_transport(&transport1, &mut reply).unwrap();
        transport1
            .send_se_query_response(transport0.local_peer_id(), reply)
            .await
            .unwrap();

        loop {
            let event = timeout(Duration::from_secs(5), events0.recv())
                .await
                .expect("timed out waiting for SE query reply")
                .expect("iroh event channel closed");
            if let TransportEvent::SEQueryReply { peer_id, reply } = event {
                assert_eq!(peer_id, *transport1.local_peer_id());
                assert_eq!(reply.message_id, request_message_id);
                assert_eq!(reply.sender_id, transport1.local_peer_id().to_string());
                assert_eq!(reply.doc_ids, vec!["doc1".to_string(), "doc2".to_string()]);
                break;
            }
        }

        transport0.shutdown().await.unwrap();
        transport1.shutdown().await.unwrap();
        task0.await.unwrap();
        task1.await.unwrap();
    }

    #[tokio::test]
    async fn publish_raw_emits_gossip_raw_message_on_registered_topic() {
        use crate::topics::{DefraTopic, ENCRYPTION_TOPIC};

        let key0 = SecretKey::generate();
        let key1 = SecretKey::generate();
        let (command_tx0, mut events0, _replicators0, task0) =
            spawn_endpoint(test_config(key0.clone())).await.unwrap();
        let (command_tx1, mut events1, _replicators1, task1) =
            spawn_endpoint(test_config(key1.clone())).await.unwrap();
        let transport0 = IrohTransport::new(command_tx0, key0);
        let transport1 = IrohTransport::new(command_tx1, key1);

        transport0
            .dial(
                transport1.local_peer_id(),
                transport1.listen_addresses().await.unwrap(),
            )
            .await
            .unwrap();
        transport0
            .poll_until_connected(transport1.local_peer_id(), Duration::from_secs(5))
            .await
            .unwrap();
        transport1
            .poll_until_connected(transport0.local_peer_id(), Duration::from_secs(5))
            .await
            .unwrap();

        // Both subscribe to the encryption topic (spawns the gossip reader),
        // then the receiver registers it as raw-routed.
        transport0.subscribe(DefraTopic::Encryption).await.unwrap();
        transport1.subscribe(DefraTopic::Encryption).await.unwrap();
        transport1
            .register_pubsub_rpc_topic(ENCRYPTION_TOPIC.to_string())
            .await
            .unwrap();

        // Wait for the gossip mesh to form on BOTH sides so the broadcast is
        // deliverable from the sender to the receiver.
        async fn wait_peer_subscribed(
            events: &mut tokio::sync::mpsc::Receiver<TransportEvent<iroh::endpoint::SendStream>>,
        ) {
            loop {
                let event = timeout(Duration::from_secs(5), events.recv())
                    .await
                    .expect("timed out waiting for peer subscription")
                    .expect("iroh event channel closed");
                if let TransportEvent::PeerSubscribed { topic, .. } = &event {
                    if topic == ENCRYPTION_TOPIC {
                        break;
                    }
                }
            }
        }
        wait_peer_subscribed(&mut events0).await;
        wait_peer_subscribed(&mut events1).await;

        let payload = vec![0xCB, 0x0Au8, 0x01, 0x02, 0x03];
        transport0
            .publish_raw(ENCRYPTION_TOPIC.to_string(), payload.clone())
            .await
            .unwrap();

        loop {
            let event = timeout(Duration::from_secs(10), events1.recv())
                .await
                .expect("timed out waiting for raw gossip message")
                .expect("iroh event channel closed");
            match event {
                TransportEvent::GossipRawMessage {
                    propagation_source,
                    topic,
                    data,
                    ..
                } => {
                    assert_eq!(propagation_source, *transport0.local_peer_id());
                    assert_eq!(topic, ENCRYPTION_TOPIC);
                    assert_eq!(data, payload);
                    break;
                }
                TransportEvent::GossipMessage { .. } => {
                    panic!("raw-registered topic must not decode as PushLogBroadcast");
                }
                _ => continue,
            }
        }

        transport0.shutdown().await.unwrap();
        transport1.shutdown().await.unwrap();
        task0.await.unwrap();
        task1.await.unwrap();
    }

    /// Regression for #976: `subscribe_raw` must JOIN the gossip mesh for a raw
    /// string sub-topic (here a KMS `encryption/<peer>/_response`-style topic
    /// that no `DefraTopic::subscribe` covers) and spawn a reader, so a peer's
    /// `publish_raw` on the same string is actually received as a
    /// `GossipRawMessage`. Before the fix `subscribe_raw` only registered the
    /// topic for raw routing without joining, so the message never arrived and
    /// the KMS key fetch timed out.
    #[tokio::test]
    async fn subscribe_raw_joins_mesh_and_receives_publish_raw() {
        let key0 = SecretKey::generate();
        let key1 = SecretKey::generate();
        let (command_tx0, mut events0, _replicators0, task0) =
            spawn_endpoint(test_config(key0.clone())).await.unwrap();
        let (command_tx1, mut events1, _replicators1, task1) =
            spawn_endpoint(test_config(key1.clone())).await.unwrap();
        let transport0 = IrohTransport::new(command_tx0, key0);
        let transport1 = IrohTransport::new(command_tx1, key1);

        transport0
            .dial(
                transport1.local_peer_id(),
                transport1.listen_addresses().await.unwrap(),
            )
            .await
            .unwrap();
        transport0
            .poll_until_connected(transport1.local_peer_id(), Duration::from_secs(5))
            .await
            .unwrap();
        transport1
            .poll_until_connected(transport0.local_peer_id(), Duration::from_secs(5))
            .await
            .unwrap();

        // A `_response`-style sub-topic not covered by any DefraTopic. Both
        // peers join it via subscribe_raw so the mesh forms; publisher then
        // broadcasts raw bytes on the same string.
        let response_topic = format!(
            "encryption/{}/_response",
            transport1.local_peer_id().as_str()
        );
        transport0
            .subscribe_raw(response_topic.clone())
            .await
            .unwrap();
        transport1
            .subscribe_raw(response_topic.clone())
            .await
            .unwrap();

        async fn wait_peer_subscribed(
            events: &mut tokio::sync::mpsc::Receiver<TransportEvent<iroh::endpoint::SendStream>>,
            want_topic: &str,
        ) {
            loop {
                let event = timeout(Duration::from_secs(5), events.recv())
                    .await
                    .expect("timed out waiting for peer subscription")
                    .expect("iroh event channel closed");
                if let TransportEvent::PeerSubscribed { topic, .. } = &event {
                    if topic == want_topic {
                        break;
                    }
                }
            }
        }
        wait_peer_subscribed(&mut events0, &response_topic).await;
        wait_peer_subscribed(&mut events1, &response_topic).await;

        let payload = vec![0xA1u8, 0x02, 0x03, 0x04];
        transport0
            .publish_raw(response_topic.clone(), payload.clone())
            .await
            .unwrap();

        loop {
            let event = timeout(Duration::from_secs(10), events1.recv())
                .await
                .expect("timed out waiting for raw gossip reply on _response sub-topic")
                .expect("iroh event channel closed");
            match event {
                TransportEvent::GossipRawMessage { topic, data, .. } => {
                    assert_eq!(topic, response_topic);
                    assert_eq!(data, payload);
                    break;
                }
                TransportEvent::GossipMessage { .. } => {
                    panic!("raw-subscribed topic must not decode as PushLogBroadcast");
                }
                _ => continue,
            }
        }

        transport0.shutdown().await.unwrap();
        transport1.shutdown().await.unwrap();
        task0.await.unwrap();
        task1.await.unwrap();
    }

    #[tokio::test]
    async fn se_artifacts_roundtrip_signs_and_verifies_sender() {
        let key0 = SecretKey::generate();
        let key1 = SecretKey::generate();
        let (command_tx0, _events0, _replicators0, task0) =
            spawn_endpoint(test_config(key0.clone())).await.unwrap();
        let (command_tx1, mut events1, _replicators1, task1) =
            spawn_endpoint(test_config(key1.clone())).await.unwrap();
        let transport0 = IrohTransport::new(command_tx0, key0);
        let transport1 = IrohTransport::new(command_tx1, key1);

        transport0
            .dial(
                transport1.local_peer_id(),
                transport1.listen_addresses().await.unwrap(),
            )
            .await
            .unwrap();
        transport0
            .poll_until_connected(transport1.local_peer_id(), Duration::from_secs(5))
            .await
            .unwrap();
        transport1
            .poll_until_connected(transport0.local_peer_id(), Duration::from_secs(5))
            .await
            .unwrap();

        let request = PushSEArtifactsRequest::new(
            "collection1",
            vec![SEArtifact::new("doc1", "name", vec![4, 5, 6])],
        );
        transport0
            .send_se_artifacts(transport1.local_peer_id(), request)
            .await
            .unwrap();

        loop {
            let event = timeout(Duration::from_secs(5), events1.recv())
                .await
                .expect("timed out waiting for SE artifacts")
                .expect("iroh event channel closed");
            if let TransportEvent::SEArtifactsReceived { peer_id, data } = event {
                assert_eq!(peer_id, *transport0.local_peer_id());
                let received: PushSEArtifactsRequest =
                    serde_cbor::from_slice(&data).expect("SE artifacts should decode");
                assert_eq!(received.sender_id, transport0.local_peer_id().to_string());
                assert!(received.signature.is_some());
                assert_eq!(received.collection_id, "collection1");
                assert_eq!(received.artifacts.len(), 1);
                assert_eq!(received.artifacts[0].doc_id, "doc1");
                assert_eq!(received.artifacts[0].search_tag, vec![4, 5, 6]);
                break;
            }
        }

        transport0.shutdown().await.unwrap();
        transport1.shutdown().await.unwrap();
        task0.await.unwrap();
        task1.await.unwrap();
    }
}
