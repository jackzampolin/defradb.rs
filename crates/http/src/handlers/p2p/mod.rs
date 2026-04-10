//! P2P endpoint handlers.
//!
//! These handlers provide HTTP access to P2P networking functionality:
//! - Node info (peer ID, addresses)
//! - Peer management (list, connect)
//! - Replicator management (list, add, remove)
//! - P2P collection management (list, add, remove)
//! - P2P document replication (list, add, remove, sync)
//!
//! All endpoints enforce NAC permissions when NAC is enabled.

use crate::error::HttpError;

mod collections;
mod documents;
mod peers;
mod replicators;

fn map_p2p_bad_request(error: String) -> HttpError {
    HttpError::BadRequest(error)
}

fn map_p2p_internal(error: String) -> HttpError {
    HttpError::Internal(error)
}

pub use collections::{
    add_collections, list_collections, remove_collections, sync_branchable, sync_versions,
};
pub use documents::{add_documents, list_documents, remove_documents, sync_documents};
pub use peers::{
    active_peers, connect, connect_peer, get_info, list_peers, ConnectPeerRequest, P2pInfoResponse,
    PeerInfo,
};
pub use replicators::{
    add_replicator, list_replicators, remove_replicator, ReplicatorDeleteRequest,
    ReplicatorInfoResponse, ReplicatorRequest,
};
