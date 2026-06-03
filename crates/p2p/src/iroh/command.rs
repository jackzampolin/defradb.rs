//! Commands sent from `IrohTransport` to the background `IrohEndpoint` event loop.

use cid::Cid;
use iroh::endpoint::SendStream;
use tokio::sync::oneshot;

use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, ManageQueryReply,
    ManageQueryRequest, ManageReply, ManageRequest, PushLogBroadcast, PushLogReply, PushLogRequest,
    PushSEArtifactsRequest, QuerySEArtifactsReply, QuerySEArtifactsRequest,
};
use crate::replicator::ReplicatorInfo;
use crate::transport::{MessageId, PeerAddr, PeerId};
use crate::QueryId;

/// Commands from the transport facade to the background endpoint.
#[non_exhaustive]
pub enum IrohCommand {
    Dial {
        peer_id: PeerId,
        addrs: Vec<PeerAddr>,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    Listen {
        addr: PeerAddr,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    ConnectedPeers {
        reply: oneshot::Sender<crate::error::Result<Vec<PeerId>>>,
    },
    ListenAddresses {
        reply: oneshot::Sender<crate::error::Result<Vec<PeerAddr>>>,
    },
    PeerAddresses {
        reply: oneshot::Sender<crate::error::Result<Vec<String>>>,
    },
    NetworkChange {
        reply: oneshot::Sender<crate::error::Result<()>>,
    },

    // PubSub
    Subscribe {
        topic: crate::topics::DefraTopic,
        reply: oneshot::Sender<crate::error::Result<bool>>,
    },
    Unsubscribe {
        topic: crate::topics::DefraTopic,
        reply: oneshot::Sender<crate::error::Result<bool>>,
    },
    Publish {
        topic: crate::topics::DefraTopic,
        msg: PushLogBroadcast,
        reply: oneshot::Sender<crate::error::Result<MessageId>>,
    },
    TopicPeers {
        topic: crate::topics::DefraTopic,
        reply: oneshot::Sender<crate::error::Result<Vec<PeerId>>>,
    },
    /// Publish raw bytes on a gossip topic (no PushLogBroadcast encoding).
    /// Used by the KMS pubsub transport. `topic` is the raw topic string.
    PublishRaw {
        topic: String,
        data: Vec<u8>,
        reply: oneshot::Sender<crate::error::Result<MessageId>>,
    },
    /// Mark a topic as raw-routed: inbound gossip on it is delivered as
    /// `TransportEvent::GossipRawMessage` instead of decoded as PushLogBroadcast.
    RegisterRawTopic {
        topic: String,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    /// Join the gossip mesh for a raw string topic (e.g. the KMS
    /// `encryption/<peer>/_response` sub-topic) AND mark it raw-routed, so
    /// inbound messages are actually received and surfaced as
    /// `TransportEvent::GossipRawMessage`. Unlike `RegisterRawTopic` (which only
    /// classifies routing), this spawns a gossip reader — without it a topic is
    /// classified but never subscribed, so replies never arrive (#976).
    SubscribeRaw {
        topic: String,
        reply: oneshot::Sender<crate::error::Result<bool>>,
    },

    // Messaging
    SendPushLogResponse {
        send_stream: SendStream,
        reply_msg: PushLogReply,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendTwoStreamRequest {
        peer_id: PeerId,
        request: PushLogRequest,
        reply: oneshot::Sender<crate::error::Result<PushLogReply>>,
    },
    SendTwoStreamResponse {
        peer_id: PeerId,
        reply_msg: PushLogReply,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendDocSyncRequest {
        peer_id: PeerId,
        request: DocSyncRequest,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendDocSyncResponse {
        peer_id: PeerId,
        reply_msg: DocSyncReply,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendDocSyncResponseToken {
        send_stream: SendStream,
        reply_msg: DocSyncReply,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendBranchableSyncRequest {
        peer_id: PeerId,
        request: BranchableSyncRequest,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendBranchableSyncResponse {
        peer_id: PeerId,
        reply_msg: BranchableSyncReply,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendBranchableSyncResponseToken {
        send_stream: SendStream,
        reply_msg: BranchableSyncReply,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendCarRequest {
        peer_id: PeerId,
        root_cid: Cid,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendCarResponse {
        peer_id: PeerId,
        car_data: Vec<u8>,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendSEArtifacts {
        peer_id: PeerId,
        request: PushSEArtifactsRequest,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendSEQueryRequest {
        peer_id: PeerId,
        request: QuerySEArtifactsRequest,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendSEQueryResponse {
        peer_id: PeerId,
        reply_msg: QuerySEArtifactsReply,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendManageRequest {
        peer_id: PeerId,
        request: ManageRequest,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendManageResponse {
        peer_id: PeerId,
        reply_msg: ManageReply,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendManageQueryRequest {
        peer_id: PeerId,
        request: ManageQueryRequest,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    SendManageQueryResponse {
        peer_id: PeerId,
        reply_msg: ManageQueryReply,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },

    // Block sync
    SyncBlocks {
        root: Cid,
        providers: Vec<PeerId>,
        missing: Vec<Cid>,
        reply: oneshot::Sender<crate::error::Result<QueryId>>,
    },
    CancelSync {
        query_id: QueryId,
        reply: oneshot::Sender<crate::error::Result<bool>>,
    },

    // Replicators
    CreateReplicator {
        peer_id: PeerId,
        collections: Vec<String>,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    DeleteReplicator {
        peer_id: PeerId,
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
    ListReplicators {
        reply: oneshot::Sender<crate::error::Result<Vec<ReplicatorInfo>>>,
    },
    GetReplicator {
        peer_id: PeerId,
        reply: oneshot::Sender<crate::error::Result<Option<ReplicatorInfo>>>,
    },
    RemoveReplicatorCollections {
        peer_id: PeerId,
        collections: Vec<String>,
        reply: oneshot::Sender<crate::error::Result<bool>>,
    },

    // Lifecycle
    Shutdown {
        reply: oneshot::Sender<crate::error::Result<()>>,
    },
}
