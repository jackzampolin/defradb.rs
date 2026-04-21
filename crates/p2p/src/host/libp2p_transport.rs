//! Libp2p implementation of the P2PTransport trait.
//!
//! Wraps `P2PHostHandle` to implement the transport-agnostic `P2PTransport` trait,
//! converting between libp2p-specific types and the transport newtypes at the boundary.

use std::time::Duration;

use async_trait::async_trait;
use cid::Cid;

use crate::error::{Error, Result};
use crate::host::{P2PHostHandle, ResponseChannel};
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
    PushLogReply, PushLogRequest, PushSEArtifactsRequest,
};
use crate::replicator::ReplicatorInfo;
use crate::topics::DefraTopic;
use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId, TransportEvent};
use crate::QueryId;

/// Libp2p-backed P2P transport.
///
/// Implements `P2PTransport` by delegating to `P2PHostHandle` and converting
/// between transport-agnostic types and libp2p-specific types at the boundary.
#[derive(Clone)]
pub struct Libp2pTransport {
    handle: P2PHostHandle,
    local_peer_id: PeerId,
    local_public_key_proto: Vec<u8>,
}

impl Libp2pTransport {
    pub fn new(handle: P2PHostHandle) -> Self {
        let local_peer_id = PeerId::from(handle.local_peer_id_cached());
        let local_public_key_proto = handle.local_public_key_proto().to_vec();
        Self {
            handle,
            local_peer_id,
            local_public_key_proto,
        }
    }

    /// Get the underlying `P2PHostHandle` for direct access.
    pub fn handle(&self) -> &P2PHostHandle {
        &self.handle
    }
}

/// Parse a transport `PeerId` into a libp2p `PeerId`.
fn parse_libp2p_peer_id(peer_id: &PeerId) -> Result<libp2p::PeerId> {
    peer_id
        .as_str()
        .parse::<libp2p::PeerId>()
        .map_err(|e| Error::InvalidPeerId(e.to_string()))
}

/// Parse a transport `PeerAddr` into a libp2p `Multiaddr`.
fn parse_multiaddr(addr: &PeerAddr) -> Result<libp2p::Multiaddr> {
    addr.as_str()
        .parse::<libp2p::Multiaddr>()
        .map_err(|e| Error::InvalidMultiaddr(e.to_string()))
}

#[async_trait]
impl P2PTransport for Libp2pTransport {
    type ResponseToken = ResponseChannel;

    fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    fn local_public_key_proto(&self) -> &[u8] {
        &self.local_public_key_proto
    }

    fn sign(&self, data: &[u8]) -> Result<Vec<u8>> {
        self.handle
            .keypair()
            .sign(data)
            .map_err(|e| Error::SigningFailed(e.to_string()))
    }

    async fn dial(&self, peer_id: &PeerId, addrs: Vec<PeerAddr>) -> Result<()> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        let multiaddrs: Vec<libp2p::Multiaddr> = addrs
            .iter()
            .map(parse_multiaddr)
            .collect::<Result<Vec<_>>>()?;
        self.handle.dial(pid, multiaddrs).await
    }

    async fn listen(&self, addr: PeerAddr) -> Result<()> {
        let multiaddr = parse_multiaddr(&addr)?;
        self.handle.listen(multiaddr).await
    }

    async fn connected_peers(&self) -> Result<Vec<PeerId>> {
        let peers = self.handle.connected_peers().await?;
        Ok(peers.into_iter().map(PeerId::from).collect())
    }

    async fn listen_addresses(&self) -> Result<Vec<PeerAddr>> {
        let addrs = self.handle.listen_addresses().await?;
        Ok(addrs.into_iter().map(PeerAddr::from).collect())
    }

    async fn poll_until_connected(&self, peer_id: &PeerId, timeout: Duration) -> Result<()> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle.poll_until_connected(pid, timeout).await
    }

    async fn peer_addresses(&self) -> Result<Vec<String>> {
        self.handle.peer_addresses().await
    }

    async fn subscribe(&self, topic: DefraTopic) -> Result<bool> {
        self.handle.subscribe(topic).await
    }

    async fn unsubscribe(&self, topic: DefraTopic) -> Result<bool> {
        self.handle.unsubscribe(topic).await
    }

    async fn publish(&self, topic: DefraTopic, msg: PushLogBroadcast) -> Result<MessageId> {
        let gossip_id = self.handle.publish(topic, msg).await?;
        Ok(MessageId::from(gossip_id))
    }

    async fn publish_raw(&self, topic: String, data: Vec<u8>) -> Result<MessageId> {
        let gossip_id = self.handle.publish_raw(topic, data).await?;
        Ok(MessageId::from(gossip_id))
    }

    async fn subscribe_raw(&self, topic: String) -> Result<bool> {
        self.handle.subscribe_raw(topic).await
    }

    async fn register_pubsub_rpc_topic(&self, topic: String) -> Result<()> {
        self.handle.register_pubsub_rpc_topic(topic).await
    }

    async fn topic_peers(&self, topic: DefraTopic) -> Result<Vec<PeerId>> {
        let libp2p_peers = self.handle.topic_peers(topic).await?;
        Ok(libp2p_peers
            .into_iter()
            .map(|pid| PeerId::new(pid.to_string()))
            .collect())
    }

    async fn send_pushlog_response(
        &self,
        token: Self::ResponseToken,
        reply: PushLogReply,
    ) -> Result<()> {
        self.handle.send_pushlog_response(token, reply).await
    }

    async fn send_two_stream_request(
        &self,
        peer_id: &PeerId,
        req: PushLogRequest,
    ) -> Result<PushLogReply> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle.send_two_stream_request(pid, req).await
    }

    async fn send_two_stream_response(&self, peer_id: &PeerId, reply: PushLogReply) -> Result<()> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle.send_two_stream_response(pid, reply).await
    }

    async fn send_doc_sync_request(&self, peer_id: &PeerId, req: DocSyncRequest) -> Result<()> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle.send_doc_sync_request(pid, req).await
    }

    async fn send_doc_sync_response(&self, peer_id: &PeerId, reply: DocSyncReply) -> Result<()> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle.send_doc_sync_response(pid, reply).await
    }

    async fn send_branchable_sync_request(
        &self,
        peer_id: &PeerId,
        req: BranchableSyncRequest,
    ) -> Result<()> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle.send_branchable_sync_request(pid, req).await
    }

    async fn send_branchable_sync_response(
        &self,
        peer_id: &PeerId,
        reply: BranchableSyncReply,
    ) -> Result<()> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle.send_branchable_sync_response(pid, reply).await
    }

    async fn send_car_request(&self, peer_id: &PeerId, root_cid: Cid) -> Result<()> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle.send_car_request(pid, root_cid).await
    }

    async fn send_car_response(&self, peer_id: &PeerId, car_data: Vec<u8>) -> Result<()> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle.send_car_response(pid, car_data).await
    }

    async fn send_car_response_token(
        &self,
        _token: Self::ResponseToken,
        _car_data: Vec<u8>,
    ) -> Result<()> {
        Err(Error::ResponseSend(
            "libp2p does not use response tokens for CAR".to_string(),
        ))
    }

    async fn send_doc_sync_response_token(
        &self,
        _token: Self::ResponseToken,
        _reply: DocSyncReply,
    ) -> Result<()> {
        Err(Error::ResponseSend(
            "libp2p does not use response tokens for DocSync".to_string(),
        ))
    }

    async fn send_branchable_sync_response_token(
        &self,
        _token: Self::ResponseToken,
        _reply: BranchableSyncReply,
    ) -> Result<()> {
        Err(Error::ResponseSend(
            "libp2p does not use response tokens for BranchableSync".to_string(),
        ))
    }

    async fn send_se_artifacts(&self, peer_id: &PeerId, req: PushSEArtifactsRequest) -> Result<()> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle.send_se_artifacts(pid, req).await
    }

    async fn sync_blocks(
        &self,
        root: Cid,
        providers: Vec<PeerId>,
        missing: Vec<Cid>,
    ) -> Result<QueryId> {
        let libp2p_providers: Vec<libp2p::PeerId> = providers
            .iter()
            .map(parse_libp2p_peer_id)
            .collect::<Result<Vec<_>>>()?;
        self.handle
            .bitswap_sync(root, libp2p_providers, missing)
            .await
    }

    async fn cancel_sync(&self, query_id: QueryId) -> Result<bool> {
        self.handle.bitswap_cancel(query_id).await
    }

    async fn create_replicator(&self, peer_id: &PeerId, collections: Vec<String>) -> Result<()> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle.create_replicator(pid, collections).await
    }

    async fn delete_replicator(&self, peer_id: &PeerId) -> Result<()> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle.delete_replicator(pid).await
    }

    async fn list_replicators(&self) -> Result<Vec<ReplicatorInfo>> {
        self.handle.list_replicators().await
    }

    async fn get_replicator(&self, peer_id: &PeerId) -> Result<Option<ReplicatorInfo>> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle.get_replicator(pid).await
    }

    async fn remove_replicator_collections(
        &self,
        peer_id: &PeerId,
        collections: Vec<String>,
    ) -> Result<bool> {
        let pid = parse_libp2p_peer_id(peer_id)?;
        self.handle
            .remove_replicator_collections(pid, collections)
            .await
    }

    async fn shutdown(&self) -> Result<()> {
        self.handle.shutdown().await
    }
}

/// Convert a `HostEvent` into a `TransportEvent`.
pub fn convert_host_event(event: crate::host::HostEvent) -> TransportEvent<ResponseChannel> {
    use crate::host::HostEvent;

    match event {
        HostEvent::PeerConnected(pid) => TransportEvent::PeerConnected(PeerId::from(pid)),
        HostEvent::PeerDisconnected(pid) => TransportEvent::PeerDisconnected(PeerId::from(pid)),
        HostEvent::PushLogRequest {
            peer_id,
            request,
            channel,
        } => TransportEvent::PushLogRequest {
            peer_id: PeerId::from(peer_id),
            request,
            token: channel,
        },
        HostEvent::Listening(addr) => TransportEvent::Listening(PeerAddr::from(addr)),
        HostEvent::GossipMessage {
            propagation_source,
            message_id,
            topic,
            message,
        } => TransportEvent::GossipMessage {
            propagation_source: PeerId::from(propagation_source),
            message_id: MessageId::from(message_id),
            topic,
            message,
        },
        HostEvent::GossipRawMessage {
            propagation_source,
            message_id,
            topic,
            data,
        } => TransportEvent::GossipRawMessage {
            propagation_source: PeerId::from(propagation_source),
            message_id: MessageId::from(message_id),
            topic,
            data,
        },
        HostEvent::PeerSubscribed { peer_id, topic } => TransportEvent::PeerSubscribed {
            peer_id: PeerId::from(peer_id),
            topic,
        },
        HostEvent::PeerUnsubscribed { peer_id, topic } => TransportEvent::PeerUnsubscribed {
            peer_id: PeerId::from(peer_id),
            topic,
        },
        HostEvent::BitswapProgress {
            query_id,
            missing_count,
        } => TransportEvent::BitswapProgress {
            query_id,
            missing_count,
        },
        HostEvent::BitswapComplete {
            query_id,
            success,
            error,
        } => TransportEvent::BitswapComplete {
            query_id,
            success,
            error,
        },
        HostEvent::BitswapBlockReceived {
            query_id,
            cid,
            data,
        } => TransportEvent::BitswapBlockReceived {
            query_id,
            cid,
            data,
        },
        HostEvent::TwoStreamRequest {
            peer_id,
            request,
            is_explicit_replicator,
            explicit_replay_authorization,
        } => TransportEvent::TwoStreamRequest {
            peer_id: PeerId::from(peer_id),
            request,
            token: None,
            is_explicit_replicator,
            explicit_replay_authorization,
        },
        HostEvent::DocSyncRequest { peer_id, request } => TransportEvent::DocSyncRequest {
            peer_id: PeerId::from(peer_id),
            request,
            token: None,
        },
        HostEvent::DocSyncReply { peer_id, reply } => TransportEvent::DocSyncReply {
            peer_id: PeerId::from(peer_id),
            reply,
        },
        HostEvent::BranchableSyncRequest { peer_id, request } => {
            TransportEvent::BranchableSyncRequest {
                peer_id: PeerId::from(peer_id),
                request,
                token: None,
            }
        }
        HostEvent::BranchableSyncReply { peer_id, reply } => TransportEvent::BranchableSyncReply {
            peer_id: PeerId::from(peer_id),
            reply,
        },
        HostEvent::CarFetchRequest { peer_id, root_cid } => TransportEvent::CarFetchRequest {
            peer_id: PeerId::from(peer_id),
            request: crate::message::CarFetchRequest::full_dag(root_cid),
            token: None,
        },
        HostEvent::CarFetchResponse {
            peer_id,
            root_cid,
            car_data,
        } => TransportEvent::CarFetchResponse {
            peer_id: PeerId::from(peer_id),
            root_cid,
            car_data,
        },
        HostEvent::SEArtifactsReceived { peer_id, data } => TransportEvent::SEArtifactsReceived {
            peer_id: PeerId::from(peer_id),
            data,
        },
    }
}
