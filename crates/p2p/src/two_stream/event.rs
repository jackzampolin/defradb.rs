//! Two-stream protocol events.

use cid::Cid;
use libp2p::PeerId;

use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogRequest,
};

/// Event emitted by the two-stream handler.
#[derive(Debug)]
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
    /// Received a CAR fetch request (peer wants a DAG packaged as CARv1).
    CarFetchRequest { peer_id: PeerId, root_cid: Cid },
    /// Received a CAR fetch response (CARv1 bytes containing a DAG).
    CarFetchResponse {
        peer_id: PeerId,
        root_cid: Cid,
        car_data: Vec<u8>,
    },
    /// Failed to decode an incoming message.
    DecodeError { peer_id: PeerId, error: String },
}
