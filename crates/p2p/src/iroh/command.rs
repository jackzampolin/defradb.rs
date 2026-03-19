//! Commands sent from `IrohTransport` to the background `IrohEndpoint` event loop.

use cid::Cid;
use iroh::endpoint::SendStream;
use tokio::sync::oneshot;

use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
    PushLogReply, PushLogRequest, PushSEArtifactsRequest,
};
use crate::replicator::ReplicatorInfo;
use crate::transport::{MessageId, PeerAddr, PeerId};
use crate::QueryId;

/// Commands from the transport facade to the background endpoint.
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
