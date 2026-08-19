//! Transport-agnostic types and trait for P2P networking.
//!
//! This module defines the `P2PTransport` trait that abstracts the sync engine
//! from the concrete libp2p transport, enabling alternative implementations
//! (e.g., iroh) without modifying the coordinator.

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use cid::Cid;

use crate::error::Result;
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, CarFetchRequest, DocSyncReply, DocSyncRequest,
    ManageQueryReply, ManageQueryRequest, ManageReply, ManageRequest, PushLogBroadcast,
    PushLogReply, PushLogRequest, PushSEArtifactsRequest, QuerySEArtifactsReply,
    QuerySEArtifactsRequest,
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

#[cfg(feature = "libp2p-transport")]
impl From<libp2p::PeerId> for PeerId {
    fn from(peer_id: libp2p::PeerId) -> Self {
        Self(peer_id.to_string())
    }
}

#[cfg(feature = "libp2p-transport")]
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

#[cfg(feature = "libp2p-transport")]
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

#[cfg(feature = "libp2p-transport")]
impl From<libp2p::gossipsub::MessageId> for MessageId {
    fn from(id: libp2p::gossipsub::MessageId) -> Self {
        Self(id.to_string())
    }
}

/// Events from the transport layer, consumed by the sync coordinator.
///
/// This mirrors `HostEvent` but uses transport-agnostic types.
#[derive(Debug)]
#[non_exhaustive]
pub enum TransportEvent<ResponseToken> {
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
    /// Raw gossipsub message on a topic registered as `pubsub_rpc`-owned
    /// (#828). The payload is an opaque CBOR-encoded DocSync/Branchable
    /// request or an `InternalResponse` envelope; the coordinator feeds
    /// it into a `pubsub_rpc::TopicHandler` for decoding.
    GossipRawMessage {
        propagation_source: PeerId,
        message_id: MessageId,
        topic: String,
        data: Vec<u8>,
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
        is_explicit_replicator: bool,
        explicit_replay_authorization: Option<crate::ExplicitReplayAuthorization>,
    },
    DocSyncRequest {
        peer_id: PeerId,
        request: DocSyncRequest,
        token: Option<ResponseToken>,
    },
    DocSyncReply {
        peer_id: PeerId,
        reply: DocSyncReply,
    },
    BranchableSyncRequest {
        peer_id: PeerId,
        request: BranchableSyncRequest,
        token: Option<ResponseToken>,
    },
    BranchableSyncReply {
        peer_id: PeerId,
        reply: BranchableSyncReply,
    },
    CarFetchRequest {
        peer_id: PeerId,
        request: CarFetchRequest,
        token: Option<ResponseToken>,
    },
    CarFetchResponse {
        /// Query correlation for transport-owned selective CAR recovery.
        /// Direct and legacy rooted CAR responses have no query ID.
        query_id: Option<QueryId>,
        peer_id: PeerId,
        root_cid: Cid,
        car_data: Vec<u8>,
    },
    SEArtifactsReceived {
        peer_id: PeerId,
        data: Vec<u8>,
    },
    SEQueryRequest {
        peer_id: PeerId,
        request: QuerySEArtifactsRequest,
    },
    SEQueryReply {
        peer_id: PeerId,
        reply: QuerySEArtifactsReply,
    },
    ManageRequest {
        peer_id: PeerId,
        request: ManageRequest,
    },
    ManageReply {
        peer_id: PeerId,
        reply: ManageReply,
    },
    ManageQueryRequest {
        peer_id: PeerId,
        request: ManageQueryRequest,
    },
    ManageQueryReply {
        peer_id: PeerId,
        reply: ManageQueryReply,
    },
    Listening(PeerAddr),
}

impl<ResponseToken> TransportEvent<ResponseToken> {
    /// Stable operation label for transport-event diagnostics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::PeerConnected(_) => "peer_connected",
            Self::PeerDisconnected(_) => "peer_disconnected",
            Self::PushLogRequest { .. } => "pushlog_request",
            Self::GossipMessage { .. } => "gossip_message",
            Self::GossipRawMessage { .. } => "gossip_raw_message",
            Self::PeerSubscribed { .. } => "peer_subscribed",
            Self::PeerUnsubscribed { .. } => "peer_unsubscribed",
            Self::BitswapProgress { .. } => "bitswap_progress",
            Self::BitswapComplete { .. } => "bitswap_complete",
            Self::BitswapBlockReceived { .. } => "bitswap_block_received",
            Self::TwoStreamRequest { .. } => "two_stream_request",
            Self::DocSyncRequest { .. } => "doc_sync_request",
            Self::DocSyncReply { .. } => "doc_sync_reply",
            Self::BranchableSyncRequest { .. } => "branchable_sync_request",
            Self::BranchableSyncReply { .. } => "branchable_sync_reply",
            Self::CarFetchRequest { .. } => "car_fetch_request",
            Self::CarFetchResponse { .. } => "car_fetch_response",
            Self::SEArtifactsReceived { .. } => "se_artifacts_received",
            Self::SEQueryRequest { .. } => "se_query_request",
            Self::SEQueryReply { .. } => "se_query_reply",
            Self::ManageRequest { .. } => "manage_request",
            Self::ManageReply { .. } => "manage_reply",
            Self::ManageQueryRequest { .. } => "manage_query_request",
            Self::ManageQueryReply { .. } => "manage_query_reply",
            Self::Listening(_) => "listening",
        }
    }

    /// Assign every transport event to the shared bounded scheduler.
    pub fn dispatch_class(&self) -> crate::sync::DispatchClass {
        use crate::sync::DispatchClass;

        match self {
            // Recovery serving retains an independent bounded reserve. Slow
            // ownership registration must not prevent a provider from serving
            // a receiver-owned CAR obligation.
            Self::CarFetchRequest { .. } => DispatchClass::Recovery,
            Self::GossipRawMessage { .. }
            | Self::BitswapComplete { .. }
            | Self::BitswapBlockReceived { .. }
            | Self::DocSyncReply { .. }
            | Self::BranchableSyncReply { .. }
            | Self::CarFetchResponse { .. } => DispatchClass::Completion,
            Self::PushLogRequest { .. }
            | Self::GossipMessage { .. }
            | Self::TwoStreamRequest { .. }
            | Self::DocSyncRequest { .. }
            | Self::BranchableSyncRequest { .. } => DispatchClass::Admission,
            _ => DispatchClass::Inline,
        }
    }
}

/// Trait abstracting the P2P transport layer.
///
/// The sync coordinator is generic over this trait, allowing different transport
/// implementations (libp2p, iroh) without modifying coordinator logic.
#[async_trait]
pub trait P2PTransport: Clone + Send + Sync + 'static {
    type ResponseToken: Send + 'static;

    // ---- Identity ----

    fn local_peer_id(&self) -> &PeerId;

    fn local_public_key_proto(&self) -> &[u8];

    fn sign(&self, data: &[u8]) -> Result<Vec<u8>>;

    // ---- Connection ----

    async fn dial(&self, peer_id: &PeerId, addrs: Vec<PeerAddr>) -> Result<()>;

    /// Disconnect from a peer, hanging up any live connection.
    ///
    /// Disconnect-only: this hangs up the current connection but does not
    /// prevent the peer from reconnecting (no persistent allow/deny list).
    /// Idempotent: disconnecting an already-absent peer returns `Ok(())`.
    async fn disconnect(&self, peer_id: &PeerId) -> Result<()>;

    /// Parse a peer address string into a transport-agnostic peer id and dial
    /// hints. The accepted string format is transport-specific (libp2p
    /// multiaddrs vs iroh tickets/endpoint ids), which is why this lives on the
    /// transport rather than as a free function. Used by callers that need to
    /// dial a peer given only its shareable address string (e.g. the outbound
    /// management requester).
    ///
    /// The default implementation returns an error; concrete transports
    /// override it.
    fn parse_dial_addr(&self, _addr: &str) -> Result<(PeerId, Vec<PeerAddr>)> {
        Err(crate::error::Error::Transport(
            "parse_dial_addr is not supported on this transport".to_string(),
        ))
    }

    async fn listen(&self, addr: PeerAddr) -> Result<()>;

    async fn connected_peers(&self) -> Result<Vec<PeerId>>;

    async fn listen_addresses(&self) -> Result<Vec<PeerAddr>>;

    async fn poll_until_connected(&self, peer_id: &PeerId, timeout: Duration) -> Result<()>;

    async fn peer_addresses(&self) -> Result<Vec<String>>;

    // ---- PubSub ----

    async fn subscribe(&self, topic: DefraTopic) -> Result<bool>;

    async fn unsubscribe(&self, topic: DefraTopic) -> Result<bool>;

    async fn publish(&self, topic: DefraTopic, msg: PushLogBroadcast) -> Result<MessageId>;

    /// Publish raw pre-encoded bytes on `topic`. Used by the pubsub_rpc layer
    /// (#828) for DocSync/BranchableSync requests and for
    /// `<base>/<peer>/_response` reply envelopes, whose payloads are not
    /// `PushLogBroadcast`-shaped and whose topic names are not
    /// [`DefraTopic`] variants.
    ///
    /// Default implementation returns `Error::Transport("not supported")` —
    /// transports that don't implement gossipsub (iroh, mocks) can rely on
    /// it; libp2p overrides.
    async fn publish_raw(&self, _topic: String, _data: Vec<u8>) -> Result<MessageId> {
        Err(crate::error::Error::Transport(
            "publish_raw is not supported on this transport".to_string(),
        ))
    }

    /// Subscribe to an arbitrary topic string without the [`DefraTopic`]
    /// wrapper. Used for dynamic pubsub_rpc response sub-topics.
    ///
    /// Default implementation returns `Error::Transport("not supported")`.
    async fn subscribe_raw(&self, _topic: String) -> Result<bool> {
        Err(crate::error::Error::Transport(
            "subscribe_raw is not supported on this transport".to_string(),
        ))
    }

    /// Register `topic` as owned by the pubsub_rpc layer. Future inbound
    /// gossipsub messages on `topic` (or any sub-topic matching
    /// `<topic>/<peer>/_response`) arrive as
    /// [`TransportEvent::GossipRawMessage`] rather than being decoded as
    /// PushLog broadcasts. Idempotent.
    ///
    /// Default implementation is a no-op so callers can register
    /// unconditionally on transports without a gossipsub dispatcher.
    async fn register_pubsub_rpc_topic(&self, _topic: String) -> Result<()> {
        Ok(())
    }

    /// Get all peers known to GossipSub for a topic (mesh + non-mesh).
    async fn topic_peers(&self, topic: DefraTopic) -> Result<Vec<PeerId>>;

    // ---- Messaging ----

    async fn send_pushlog_response(
        &self,
        token: Self::ResponseToken,
        reply: PushLogReply,
    ) -> Result<()>;

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

    async fn send_car_response_token(
        &self,
        token: Self::ResponseToken,
        car_data: Vec<u8>,
    ) -> Result<()>;

    async fn send_doc_sync_response_token(
        &self,
        token: Self::ResponseToken,
        reply: DocSyncReply,
    ) -> Result<()>;

    async fn send_branchable_sync_response_token(
        &self,
        token: Self::ResponseToken,
        reply: BranchableSyncReply,
    ) -> Result<()>;

    async fn send_se_artifacts(&self, peer_id: &PeerId, req: PushSEArtifactsRequest) -> Result<()>;

    async fn send_se_query_request(
        &self,
        _peer_id: &PeerId,
        _req: QuerySEArtifactsRequest,
    ) -> Result<()> {
        Err(crate::error::Error::Transport(
            "send_se_query_request is not supported on this transport".to_string(),
        ))
    }

    async fn send_se_query_response(
        &self,
        _peer_id: &PeerId,
        _reply: QuerySEArtifactsReply,
    ) -> Result<()> {
        Err(crate::error::Error::Transport(
            "send_se_query_response is not supported on this transport".to_string(),
        ))
    }

    async fn send_manage_request(&self, _peer_id: &PeerId, _req: ManageRequest) -> Result<()> {
        Err(crate::error::Error::Transport(
            "send_manage_request is not supported on this transport".to_string(),
        ))
    }

    async fn send_manage_response(&self, _peer_id: &PeerId, _reply: ManageReply) -> Result<()> {
        Err(crate::error::Error::Transport(
            "send_manage_response is not supported on this transport".to_string(),
        ))
    }

    async fn send_manage_query_request(
        &self,
        _peer_id: &PeerId,
        _req: ManageQueryRequest,
    ) -> Result<()> {
        Err(crate::error::Error::Transport(
            "send_manage_query_request is not supported on this transport".to_string(),
        ))
    }

    async fn send_manage_query_response(
        &self,
        _peer_id: &PeerId,
        _reply: ManageQueryReply,
    ) -> Result<()> {
        Err(crate::error::Error::Transport(
            "send_manage_query_response is not supported on this transport".to_string(),
        ))
    }

    // ---- Block sync ----

    async fn sync_blocks(
        &self,
        root: Cid,
        providers: Vec<PeerId>,
        missing: Vec<Cid>,
    ) -> Result<QueryId>;

    /// Whether `sync_blocks(root, providers, [])` is a cancellable recursive
    /// CAR request.  The iroh transport provides that contract; libp2p's
    /// implementation is exact-CID Bitswap, where an empty set is a no-op.
    fn supports_cancellable_rooted_sync(&self) -> bool {
        false
    }

    async fn cancel_sync(&self, query_id: QueryId) -> Result<bool>;

    // ---- Replicators ----

    async fn create_replicator(&self, peer_id: &PeerId, collections: Vec<String>) -> Result<()>;

    async fn create_replicator_info(&self, peer_id: &PeerId, info: ReplicatorInfo) -> Result<()> {
        self.create_replicator(peer_id, info.collections).await
    }

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
