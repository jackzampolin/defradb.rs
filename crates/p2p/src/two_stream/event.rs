//! Two-stream protocol events.

use cid::Cid;
use libp2p::PeerId;

use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, IdentityRequest,
    IdentityResponse, ManageQueryReply, ManageQueryRequest, ManageReply, ManageRequest,
    PushLogRequest, PushSEArtifactsRequest, QuerySEArtifactsReply, QuerySEArtifactsRequest,
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
    /// Received a management request from a peer (mutating operation).
    ManageRequest {
        peer_id: PeerId,
        request: ManageRequest,
    },
    /// Received a management reply from a peer (mutating operation ack/error).
    ManageReply {
        peer_id: PeerId,
        reply: ManageReply,
    },
    /// Received a management query request from a peer (read-only operation).
    ManageQueryRequest {
        peer_id: PeerId,
        request: ManageQueryRequest,
    },
    /// Received a management query reply from a peer (read-only operation result).
    ManageQueryReply {
        peer_id: PeerId,
        reply: ManageQueryReply,
    },
    /// Failed to decode an incoming message.
    DecodeError { peer_id: PeerId, error: String },
}

#[cfg(test)]
mod tests {
    use super::TwoStreamEvent;

    #[test]
    fn manage_variants_exist() {
        fn _a(e: TwoStreamEvent) -> bool {
            matches!(
                e,
                TwoStreamEvent::ManageRequest { .. }
                    | TwoStreamEvent::ManageReply { .. }
                    | TwoStreamEvent::ManageQueryRequest { .. }
                    | TwoStreamEvent::ManageQueryReply { .. }
            )
        }
        let _ = _a;
    }
}
