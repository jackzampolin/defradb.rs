//! Host events emitted by the P2P host.

use cid::Cid;
use libp2p::{gossipsub, Multiaddr, PeerId};

use crate::message::PushLogBroadcast;
use crate::QueryId;

use super::ResponseChannel;

/// Events emitted by the P2P host.
#[derive(Debug)]
#[non_exhaustive]
pub enum HostEvent {
    /// A peer connection was established.
    PeerConnected(PeerId),

    /// A peer disconnected.
    PeerDisconnected(PeerId),

    /// Received a PushLog request.
    PushLogRequest {
        peer_id: PeerId,
        request: crate::message::PushLogRequest,
        channel: ResponseChannel,
    },

    /// Started listening on an address.
    Listening(Multiaddr),

    /// Received a GossipSub message.
    GossipMessage {
        /// The peer that propagated the message.
        propagation_source: PeerId,
        /// The message ID.
        message_id: gossipsub::MessageId,
        /// The topic the message was received on.
        topic: String,
        /// The message payload.
        message: PushLogBroadcast,
    },

    /// Received a gossipsub message on a topic registered as `pubsub_rpc`
    /// (via [`super::command::HostCommand::RegisterPubsubRpcTopic`]).
    ///
    /// The host does not attempt to decode the payload; the consumer (the
    /// coordinator, see `sync/coordinator/event_handler/doc_sync.rs` and
    /// `branchable_sync.rs`) runs it through its own
    /// `pubsub_rpc::TopicHandler`. This channel is how DocSync and
    /// BranchableSync receive Go-compatible traffic (#828).
    GossipRawMessage {
        propagation_source: PeerId,
        message_id: gossipsub::MessageId,
        topic: String,
        data: Vec<u8>,
    },

    /// A peer subscribed to a topic we're also subscribed to.
    PeerSubscribed { peer_id: PeerId, topic: String },

    /// A peer unsubscribed from a topic.
    PeerUnsubscribed { peer_id: PeerId, topic: String },

    /// Bitswap sync progress update.
    BitswapProgress {
        query_id: QueryId,
        missing_count: usize,
    },

    /// Bitswap sync completed (success or failure).
    BitswapComplete {
        query_id: QueryId,
        success: bool,
        error: Option<String>,
    },

    /// Block received via Bitswap - needs to be stored and processed.
    BitswapBlockReceived {
        query_id: QueryId,
        cid: Cid,
        data: Vec<u8>,
    },

    /// Received a PushLog request via two-stream protocol (Go compatibility).
    ///
    /// Invariant (#838): `is_explicit_replicator` must only be set to
    /// `true` by the two-stream transport after verifying the remote peer via
    /// the signed explicit-replicator handshake. The sync coordinator treats
    /// this flag as an authenticated claim and skips the `ReplicatorRegistry`
    /// membership check when it is `true`.
    TwoStreamRequest {
        peer_id: PeerId,
        request: crate::message::PushLogRequest,
        is_explicit_replicator: bool,
        explicit_replay_authorization: Option<crate::ExplicitReplayAuthorization>,
    },

    /// Received a DocSync request via two-stream protocol.
    DocSyncRequest {
        peer_id: PeerId,
        request: crate::message::DocSyncRequest,
    },

    /// Received a DocSync reply via two-stream protocol.
    DocSyncReply {
        peer_id: PeerId,
        reply: crate::message::DocSyncReply,
    },

    /// Received a BranchableSync request via two-stream protocol.
    BranchableSyncRequest {
        peer_id: PeerId,
        request: crate::message::BranchableSyncRequest,
    },

    /// Received a BranchableSync reply via two-stream protocol.
    BranchableSyncReply {
        peer_id: PeerId,
        reply: crate::message::BranchableSyncReply,
    },

    /// Received a CAR fetch request (peer wants a DAG packaged as CARv1).
    CarFetchRequest { peer_id: PeerId, root_cid: Cid },

    /// Received a CAR fetch response (CARv1 bytes containing a DAG).
    CarFetchResponse {
        peer_id: PeerId,
        root_cid: Cid,
        car_data: Vec<u8>,
    },

    /// Received SE artifacts from a peer.
    SEArtifactsReceived {
        peer_id: PeerId,
        /// Raw CBOR bytes of the PushSEArtifactsRequest for the db layer to process.
        data: Vec<u8>,
    },

    /// Received an SE query request from a peer.
    SEQueryRequest {
        peer_id: PeerId,
        request: crate::message::QuerySEArtifactsRequest,
    },

    /// Received an SE query reply from a peer.
    SEQueryReply {
        peer_id: PeerId,
        reply: crate::message::QuerySEArtifactsReply,
    },
}
