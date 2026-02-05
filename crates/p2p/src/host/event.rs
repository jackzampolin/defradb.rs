//! Host events emitted by the P2P host.

use cid::Cid;
use libp2p::{gossipsub, Multiaddr, PeerId};

use crate::message::PushLogBroadcast;
use crate::QueryId;

use super::ResponseChannel;

/// Events emitted by the P2P host.
#[derive(Debug)]
pub enum HostEvent {
    /// A new peer was discovered.
    PeerDiscovered(PeerId),

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
    TwoStreamRequest {
        peer_id: PeerId,
        request: crate::message::PushLogRequest,
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
}
