//! Transport-agnostic types and trait for P2P networking.
//!
//! This module defines the `P2PTransport` trait that abstracts the sync engine
//! from the concrete libp2p transport, enabling alternative implementations
//! (e.g., iroh) without modifying the coordinator.

use std::any::Any;
use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use cid::Cid;

use crate::error::Result;
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
    PushLogReply, PushLogRequest, PushSEArtifactsRequest,
};
use crate::replicator::ReplicatorInfo;
use crate::topics::DefraTopic;
use crate::QueryId;

/// Transport-agnostic peer identifier.
///
/// Wraps a string representation of a peer ID, allowing conversion from
/// both `libp2p::PeerId` and iroh node IDs.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PeerId(String);

impl PeerId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<libp2p::PeerId> for PeerId {
    fn from(peer_id: libp2p::PeerId) -> Self {
        Self(peer_id.to_string())
    }
}

impl From<&libp2p::PeerId> for PeerId {
    fn from(peer_id: &libp2p::PeerId) -> Self {
        Self(peer_id.to_string())
    }
}

#[cfg(feature = "iroh-transport")]
impl From<iroh::EndpointId> for PeerId {
    fn from(id: iroh::EndpointId) -> Self {
        Self(id.to_string())
    }
}

/// Transport-agnostic peer address.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PeerAddr(String);

impl PeerAddr {
    pub fn new(addr: String) -> Self {
        Self(addr)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PeerAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<libp2p::Multiaddr> for PeerAddr {
    fn from(addr: libp2p::Multiaddr) -> Self {
        Self(addr.to_string())
    }
}

/// Transport-agnostic message identifier.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct MessageId(String);

impl MessageId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<libp2p::gossipsub::MessageId> for MessageId {
    fn from(id: libp2p::gossipsub::MessageId) -> Self {
        Self(id.to_string())
    }
}

/// Opaque token for sending a response to a request.
///
/// For libp2p, this wraps a `ResponseChannel`. Other transports may
/// wrap their own response correlation types.
pub struct ResponseToken(Box<dyn Any + Send>);

impl ResponseToken {
    pub fn new<T: Any + Send + 'static>(inner: T) -> Self {
        Self(Box::new(inner))
    }

    pub fn downcast<T: Any + Send + 'static>(self) -> Option<T> {
        self.0.downcast::<T>().ok().map(|b| *b)
    }
}

impl fmt::Debug for ResponseToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseToken").finish()
    }
}

/// Events from the transport layer, consumed by the sync coordinator.
///
/// This mirrors `HostEvent` but uses transport-agnostic types.
#[derive(Debug)]
pub enum TransportEvent {
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
    PushLogRequest {
        peer_id: PeerId,
        request: PushLogRequest,
        token: ResponseToken,
    },
    GossipMessage {
        propagation_source: PeerId,
        message_id: MessageId,
        topic: String,
        message: PushLogBroadcast,
    },
    PeerSubscribed {
        peer_id: PeerId,
        topic: String,
    },
    PeerUnsubscribed {
        peer_id: PeerId,
        topic: String,
    },
    BitswapProgress {
        query_id: QueryId,
        missing_count: usize,
    },
    BitswapComplete {
        query_id: QueryId,
        success: bool,
        error: Option<String>,
    },
    BitswapBlockReceived {
        query_id: QueryId,
        cid: Cid,
        data: Vec<u8>,
    },
    TwoStreamRequest {
        peer_id: PeerId,
        request: PushLogRequest,
        token: Option<ResponseToken>,
    },
    DocSyncRequest {
        peer_id: PeerId,
        request: DocSyncRequest,
    },
    DocSyncReply {
        peer_id: PeerId,
        reply: DocSyncReply,
    },
    BranchableSyncRequest {
        peer_id: PeerId,
        request: BranchableSyncRequest,
    },
    BranchableSyncReply {
        peer_id: PeerId,
        reply: BranchableSyncReply,
    },
    CarFetchRequest {
        peer_id: PeerId,
        root_cid: Cid,
        token: Option<ResponseToken>,
    },
    CarFetchResponse {
        peer_id: PeerId,
        root_cid: Cid,
        car_data: Vec<u8>,
    },
    Listening(PeerAddr),
}

/// Trait abstracting the P2P transport layer.
///
/// The sync coordinator is generic over this trait, allowing different transport
/// implementations (libp2p, iroh) without modifying coordinator logic.
#[async_trait]
pub trait P2PTransport: Clone + Send + Sync + 'static {
    // ---- Identity ----

    fn local_peer_id(&self) -> &PeerId;

    fn local_public_key_proto(&self) -> &[u8];

    fn sign(&self, data: &[u8]) -> Result<Vec<u8>>;

    // ---- Connection ----

    async fn dial(&self, peer_id: &PeerId, addrs: Vec<PeerAddr>) -> Result<()>;

    async fn listen(&self, addr: PeerAddr) -> Result<()>;

    async fn connected_peers(&self) -> Result<Vec<PeerId>>;

    async fn listen_addresses(&self) -> Result<Vec<PeerAddr>>;

    async fn poll_until_connected(&self, peer_id: &PeerId, timeout: Duration) -> Result<()>;

    async fn peer_addresses(&self) -> Result<Vec<String>>;

    // ---- PubSub ----

    async fn subscribe(&self, topic: DefraTopic) -> Result<bool>;

    async fn unsubscribe(&self, topic: DefraTopic) -> Result<bool>;

    async fn publish(&self, topic: DefraTopic, msg: PushLogBroadcast) -> Result<MessageId>;

    // ---- Messaging ----

    async fn send_pushlog_response(&self, token: ResponseToken, reply: PushLogReply) -> Result<()>;

    async fn send_two_stream_request(
        &self,
        peer_id: &PeerId,
        req: PushLogRequest,
    ) -> Result<PushLogReply>;

    async fn send_two_stream_response(&self, peer_id: &PeerId, reply: PushLogReply) -> Result<()>;

    async fn send_doc_sync_request(&self, peer_id: &PeerId, req: DocSyncRequest) -> Result<()>;

    async fn send_doc_sync_response(&self, peer_id: &PeerId, reply: DocSyncReply) -> Result<()>;

    async fn send_branchable_sync_request(
        &self,
        peer_id: &PeerId,
        req: BranchableSyncRequest,
    ) -> Result<()>;

    async fn send_branchable_sync_response(
        &self,
        peer_id: &PeerId,
        reply: BranchableSyncReply,
    ) -> Result<()>;

    async fn send_car_request(&self, peer_id: &PeerId, root_cid: Cid) -> Result<()>;

    async fn send_car_response(&self, peer_id: &PeerId, car_data: Vec<u8>) -> Result<()>;

    async fn send_car_response_token(&self, token: ResponseToken, car_data: Vec<u8>) -> Result<()>;

    async fn send_se_artifacts(&self, peer_id: &PeerId, req: PushSEArtifactsRequest) -> Result<()>;

    // ---- Block sync ----

    async fn sync_blocks(
        &self,
        root: Cid,
        providers: Vec<PeerId>,
        missing: Vec<Cid>,
    ) -> Result<QueryId>;

    async fn cancel_sync(&self, query_id: QueryId) -> Result<bool>;

    // ---- Replicators ----

    async fn create_replicator(&self, peer_id: &PeerId, collections: Vec<String>) -> Result<()>;

    async fn delete_replicator(&self, peer_id: &PeerId) -> Result<()>;

    async fn list_replicators(&self) -> Result<Vec<ReplicatorInfo>>;

    async fn get_replicator(&self, peer_id: &PeerId) -> Result<Option<ReplicatorInfo>>;

    async fn remove_replicator_collections(
        &self,
        peer_id: &PeerId,
        collections: Vec<String>,
    ) -> Result<bool>;

    // ---- Lifecycle ----

    async fn shutdown(&self) -> Result<()>;
}
