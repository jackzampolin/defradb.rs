//! Host commands for controlling the P2P host.

use cid::Cid;
use libp2p::{gossipsub, Multiaddr, PeerId};
use tokio::sync::oneshot;

use crate::error::Result;
use crate::message::PushLogBroadcast;
use crate::replicator::ReplicatorInfo;
use crate::topics::DefraTopic;
use crate::QueryId;

use super::ResponseChannel;

/// Commands that can be sent to the P2P host.
#[derive(Debug)]
#[non_exhaustive]
pub enum HostCommand {
    /// Start listening on an address.
    Listen {
        addr: Multiaddr,
        response: oneshot::Sender<Result<()>>,
    },

    /// Dial a peer at the given addresses.
    Dial {
        peer_id: PeerId,
        addrs: Vec<Multiaddr>,
        response: oneshot::Sender<Result<()>>,
    },

    /// Disconnect from a peer, hanging up any live connection.
    Disconnect {
        peer_id: PeerId,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a PushLog request to a peer.
    SendPushLog {
        peer_id: PeerId,
        request: crate::message::PushLogRequest,
        response: oneshot::Sender<Result<crate::message::PushLogReply>>,
    },

    /// Send a PushLog response through a response channel.
    SendPushLogResponse {
        channel: ResponseChannel,
        reply: crate::message::PushLogReply,
        response: oneshot::Sender<Result<()>>,
    },

    /// Get the local peer ID.
    LocalPeerId { response: oneshot::Sender<PeerId> },

    /// Get listening addresses.
    ListenAddresses {
        response: oneshot::Sender<Vec<Multiaddr>>,
    },

    /// Get connected peers.
    ConnectedPeers {
        response: oneshot::Sender<Vec<PeerId>>,
    },

    /// Subscribe to a GossipSub topic.
    Subscribe {
        topic: DefraTopic,
        response: oneshot::Sender<Result<bool>>,
    },

    /// Unsubscribe from a GossipSub topic.
    Unsubscribe {
        topic: DefraTopic,
        response: oneshot::Sender<Result<bool>>,
    },

    /// Subscribe to an arbitrary topic string without the [`DefraTopic`]
    /// wrapper. Used for dynamic pubsub_rpc response sub-topics whose names
    /// embed a runtime peer ID.
    SubscribeRaw {
        topic: String,
        response: oneshot::Sender<Result<bool>>,
    },

    /// Publish a message to a GossipSub topic.
    Publish {
        topic: DefraTopic,
        message: PushLogBroadcast,
        response: oneshot::Sender<Result<gossipsub::MessageId>>,
    },

    /// Publish raw bytes to a GossipSub topic.
    ///
    /// Used by the `pubsub_rpc` layer for DocSync / BranchableSync where
    /// the payload is an opaque CBOR-encoded request or an IPLD
    /// `InternalResponse` envelope that must land on the wire verbatim.
    /// Accepts an arbitrary topic string (not the limited `DefraTopic`
    /// enum) because per-peer response sub-topics are dynamically named
    /// `<base>/<peer>/_response`.
    PublishRaw {
        topic: String,
        data: Vec<u8>,
        response: oneshot::Sender<Result<gossipsub::MessageId>>,
    },

    /// Register a topic as owned by the `pubsub_rpc` layer.
    ///
    /// Incoming gossipsub messages on registered topics skip the default
    /// PushLog-broadcast decoder and are forwarded verbatim via
    /// [`super::event::HostEvent::GossipRawMessage`]. Idempotent: registering
    /// the same topic twice is a no-op. The coordinator is expected to
    /// register both the base topic (e.g. `"doc-sync"`) and its own
    /// `<base>/<self>/_response` sub-topic so replies correlate back.
    RegisterPubsubRpcTopic {
        topic: String,
        response: oneshot::Sender<()>,
    },

    /// Get subscribed topics.
    SubscribedTopics {
        response: oneshot::Sender<Vec<String>>,
    },

    /// Shutdown the host.
    Shutdown,

    /// Start a Bitswap sync operation to fetch missing blocks.
    BitswapSync {
        cid: Cid,
        providers: Vec<PeerId>,
        missing: Vec<Cid>,
        response: oneshot::Sender<Result<QueryId>>,
    },

    /// Cancel a Bitswap query.
    BitswapCancel {
        query_id: QueryId,
        response: oneshot::Sender<bool>,
    },

    /// Set (add/update) a replicator.
    ///
    /// Adds the peer as a replicator for the specified collections.
    /// If the peer is already a replicator, updates their collections.
    CreateReplicator {
        peer_id: PeerId,
        info: ReplicatorInfo,
        response: oneshot::Sender<Result<()>>,
    },

    /// Delete a replicator.
    ///
    /// Removes the peer from all collections they were replicating.
    DeleteReplicator {
        peer_id: PeerId,
        response: oneshot::Sender<Result<()>>,
    },

    /// Remove specific collections from a replicator.
    ///
    /// If the replicator has no collections left after removal, they are deleted.
    /// This matches Go DefraDB's partial removal behavior.
    RemoveReplicatorCollections {
        peer_id: PeerId,
        collections: Vec<String>,
        response: oneshot::Sender<Result<bool>>, // true if replicator was fully deleted
    },

    /// Get all registered replicators.
    ListReplicators {
        response: oneshot::Sender<Vec<ReplicatorInfo>>,
    },

    /// Get replicator info for a specific peer.
    GetReplicator {
        peer_id: PeerId,
        response: oneshot::Sender<Option<ReplicatorInfo>>,
    },

    /// Send a PushLog response via two-stream protocol (Go compatibility).
    SendTwoStreamResponse {
        peer_id: PeerId,
        reply: crate::message::PushLogReply,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a PushLog request via two-stream protocol (Go compatibility).
    SendTwoStreamRequest {
        peer_id: PeerId,
        request: crate::message::PushLogRequest,
        response: oneshot::Sender<Result<crate::message::PushLogReply>>,
    },

    /// Send a DocSync response via two-stream protocol.
    SendDocSyncResponse {
        peer_id: PeerId,
        reply: crate::message::DocSyncReply,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a DocSync request via two-stream protocol.
    SendDocSyncRequest {
        peer_id: PeerId,
        request: crate::message::DocSyncRequest,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a BranchableSync response via two-stream protocol.
    SendBranchableSyncResponse {
        peer_id: PeerId,
        reply: crate::message::BranchableSyncReply,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a BranchableSync request via two-stream protocol.
    SendBranchableSyncRequest {
        peer_id: PeerId,
        request: crate::message::BranchableSyncRequest,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send SE artifacts to a peer via SE two-stream protocol.
    SendSEArtifacts {
        peer_id: PeerId,
        request: crate::message::PushSEArtifactsRequest,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a PushSEArtifacts reply on the SE response protocol (ack a push).
    SendSEArtifactsResponse {
        peer_id: PeerId,
        reply: crate::message::PushSEArtifactsReply,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send an SE query request to a peer via SE query two-stream protocol.
    SendSEQueryRequest {
        peer_id: PeerId,
        request: crate::message::QuerySEArtifactsRequest,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send an SE query response to a peer via SE query two-stream protocol.
    SendSEQueryResponse {
        peer_id: PeerId,
        reply: crate::message::QuerySEArtifactsReply,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a management mutate request to a peer via the manage two-stream protocol.
    SendManageRequest {
        peer_id: PeerId,
        request: crate::message::ManageRequest,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a management mutate response to a peer via the manage two-stream protocol.
    SendManageResponse {
        peer_id: PeerId,
        reply: crate::message::ManageReply,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a management query request to a peer via the manage query two-stream protocol.
    SendManageQueryRequest {
        peer_id: PeerId,
        request: crate::message::ManageQueryRequest,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a management query response to a peer via the manage query two-stream protocol.
    SendManageQueryResponse {
        peer_id: PeerId,
        reply: crate::message::ManageQueryReply,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a CAR request to a peer (request DAG as CARv1).
    SendCarRequest {
        peer_id: PeerId,
        root_cid: Cid,
        response: oneshot::Sender<Result<()>>,
    },

    /// Send a CAR response to a peer (CARv1 bytes).
    SendCarResponse {
        peer_id: PeerId,
        car_data: Vec<u8>,
        response: oneshot::Sender<Result<()>>,
    },

    /// Resolve a peer's DEFRA identity through the Go-compatible identity protocol.
    GetPeerIdentity {
        peer_id: PeerId,
        response: oneshot::Sender<Result<Option<identity::Did>>>,
    },

    /// Get connected peers with their full multiaddrs (Go-compatible ActivePeers).
    PeerAddresses {
        response: oneshot::Sender<Vec<String>>,
    },

    /// Get all peers known to GossipSub for a topic (mesh + non-mesh).
    TopicPeers {
        topic: DefraTopic,
        response: oneshot::Sender<Vec<PeerId>>,
    },
}
