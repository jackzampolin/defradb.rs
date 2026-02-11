//! P2P FFI functions for DefraDB.
//!
//! This module provides FFI functions for P2P networking operations:
//! - Peer info and connection
//! - Replicator management
//! - P2P collection management

mod collections;
mod documents;
mod node;
mod peer;
mod push;
mod replicator;
mod sync;
mod version_sync;

pub use collections::{p2p_create_collections, p2p_delete_collections, p2p_list_collections};
pub use documents::{p2p_create_documents, p2p_delete_documents, p2p_list_documents};
pub use node::new_node_with_p2p;
pub use peer::{p2p_active_peers, p2p_connect, p2p_peer_info};
pub use push::p2p_retry_replicators;
pub use replicator::{p2p_create_replicator, p2p_delete_replicator, p2p_list_replicators};
pub use sync::{p2p_sync_branchable_collection, p2p_sync_documents};
pub use version_sync::p2p_sync_collection_versions;

use storage::stores::Peerstore;

/// Parsed multiaddr containing peer ID and transport address.
pub(crate) struct ParsedMultiaddr {
    /// The peer ID extracted from the multiaddr.
    pub(crate) peer_id: libp2p::PeerId,
    /// The transport address (multiaddr without the /p2p component).
    pub(crate) transport_addr: libp2p::Multiaddr,
}

/// Parse a full multiaddr string that includes a peer ID.
///
/// Expects format like: `/ip4/127.0.0.1/tcp/9171/p2p/12D3KooW...`
///
/// Returns the peer ID and the transport address (without /p2p component).
pub(crate) fn parse_multiaddr_with_peer_id(addr_str: &str) -> Result<ParsedMultiaddr, String> {
    let full_addr: libp2p::Multiaddr = addr_str
        .parse()
        .map_err(|e| format!("invalid multiaddr '{}': {}", addr_str, e))?;

    let peer_id = full_addr
        .iter()
        .find_map(|p| {
            if let libp2p::multiaddr::Protocol::P2p(peer_id) = p {
                Some(peer_id)
            } else {
                None
            }
        })
        .ok_or_else(|| format!("multiaddr '{}' does not contain peer ID", addr_str))?;

    let transport_addr: libp2p::Multiaddr = full_addr
        .iter()
        .filter(|p| !matches!(p, libp2p::multiaddr::Protocol::P2p(_)))
        .collect();

    Ok(ParsedMultiaddr {
        peer_id,
        transport_addr,
    })
}

/// Parse a JSON array of collection names.
///
/// Expects format like: `["collection1", "collection2"]`
/// Also handles JSON `null` (treated as empty array).
pub(crate) fn parse_collections_json(json_str: &str) -> Result<Vec<String>, String> {
    let opt: Option<Vec<String>> =
        serde_json::from_str(json_str).map_err(|e| format!("invalid collections JSON: {}", e))?;
    Ok(opt.unwrap_or_default())
}

pub(crate) fn parse_doc_ids_json(json_str: &str) -> Result<Vec<String>, String> {
    let opt: Option<Vec<String>> =
        serde_json::from_str(json_str).map_err(|e| format!("invalid doc_ids JSON: {}", e))?;
    Ok(opt.unwrap_or_default())
}

/// Persist the current P2P collection subscription list to the Peerstore.
pub(crate) async fn persist_p2p_collections(
    db: &crate::state::FfiDatabase,
    collections: &[String],
) {
    let data = match serde_json::to_vec(collections) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("[PERSIST-P2P] Failed to serialize collections: {}", e);
            return;
        }
    };
    let peerstore = Peerstore::new(db.store().clone());
    if let Err(e) = peerstore.set_p2p_collections(&data).await {
        eprintln!("[PERSIST-P2P] Failed to persist collections: {}", e);
    }
}

/// Persist the current P2P document subscription list to the Peerstore.
pub(crate) async fn persist_p2p_documents(db: &crate::state::FfiDatabase, documents: &[String]) {
    let data = match serde_json::to_vec(documents) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("[PERSIST-P2P] Failed to serialize documents: {}", e);
            return;
        }
    };
    let peerstore = Peerstore::new(db.store().clone());
    if let Err(e) = peerstore.set_p2p_documents(&data).await {
        eprintln!("[PERSIST-P2P] Failed to persist documents: {}", e);
    }
}
