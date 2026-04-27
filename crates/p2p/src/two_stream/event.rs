//! Two-stream protocol events.

use cid::Cid;
use libp2p::PeerId;

use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, IdentityRequest,
    IdentityResponse, PushLogRequest, PushSEArtifactsRequest, QuerySEArtifactsReply,
    QuerySEArtifactsRequest,
};

/// Event emitted by the two-stream handler.
#[derive(Debug)]
#[non_exhaustive]
pub enum TwoStreamEvent {
    /// Received a PushLog request from a peer.
    InboundRequest {
        peer_id: PeerId,
        request: PushLogRequest,
    },
    /// Received a DocSync request from a peer.
    DocSyncRequest {
        peer_id: PeerId,
        request: DocSyncRequest,
    },
    /// Received a DocSync reply from a peer.
    DocSyncReply {
        peer_id: PeerId,
        reply: DocSyncReply,
    },
    /// Received a BranchableSync request from a peer.
    BranchableSyncRequest {
        peer_id: PeerId,
        request: BranchableSyncRequest,
    },
    /// Received a BranchableSync reply from a peer.
    BranchableSyncReply {
        peer_id: PeerId,
        reply: BranchableSyncReply,
    },
    /// Received an identity request from a peer.
    IdentityRequest {
        peer_id: PeerId,
        request: IdentityRequest,
    },
    /// Received an identity reply from a peer.
    IdentityReply {
        peer_id: PeerId,
        reply: IdentityResponse,
    },
    /// Received a CAR fetch request (peer wants a DAG packaged as CARv1).
    CarFetchRequest { peer_id: PeerId, root_cid: Cid },
    /// Received a CAR fetch response (CARv1 bytes containing a DAG).
    CarFetchResponse {
        peer_id: PeerId,
        root_cid: Cid,
        car_data: Vec<u8>,
    },
    /// Received SE artifacts from a peer (push request).
    SEArtifactsReceived {
        peer_id: PeerId,
        request: PushSEArtifactsRequest,
    },
    /// Received an SE query request from a peer.
    SEQueryRequest {
        peer_id: PeerId,
        request: QuerySEArtifactsRequest,
    },
    /// Received an SE query reply from a peer.
    SEQueryReply {
        peer_id: PeerId,
        reply: QuerySEArtifactsReply,
    },
    /// Failed to decode an incoming message.
    DecodeError { peer_id: PeerId, error: String },
}
