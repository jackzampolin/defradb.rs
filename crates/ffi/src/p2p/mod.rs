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

pub use collections::{p2p_add_collections, p2p_delete_collections, p2p_list_collections};
pub use documents::{p2p_add_documents, p2p_delete_documents, p2p_list_documents};
pub use node::new_node_with_p2p;
pub use peer::{p2p_active_peers, p2p_connect, p2p_peer_info};
pub use push::p2p_retry_replicators;
pub use replicator::{p2p_add_replicator, p2p_delete_replicator, p2p_list_replicators};
pub use sync::{p2p_sync_branchable_collection, p2p_sync_documents};
pub use version_sync::p2p_sync_collection_versions;

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
