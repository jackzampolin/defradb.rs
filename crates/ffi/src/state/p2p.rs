use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::RwLock;

use blockstore::DefraBlockstore;
use p2p::P2PHostHandle;

use super::FfiStore;

/// P2P state for FFI nodes.
///
/// This wraps the P2P host handle and tracks P2P-specific state like
/// subscribed collections and background task handles.
pub struct P2PState {
    /// Handle to communicate with the P2P host.
    pub handle: P2PHostHandle,
    /// Blockstore for accessing IPLD blocks.
    pub blockstore: Arc<DefraBlockstore<FfiStore>>,
    /// Merge handler for processing incoming blocks.
    pub merge_handler: Arc<db::DbMergeHandler<FfiStore, DefraBlockstore<FfiStore>>>,
    /// Collections subscribed for P2P replication (insertion-ordered).
    pub collections: RwLock<Vec<String>>,
    /// Documents subscribed for P2P replication (by doc ID).
    pub documents: RwLock<HashSet<String>>,
    /// Known peer addresses: peer_id_string -> full multiaddr with /p2p/ component.
    /// Populated when peers connect via p2p_connect or p2p_create_replicator.
    pub peer_addresses: RwLock<HashMap<String, String>>,
    /// Abort handle for the host event loop task.
    pub host_event_handle: Option<tokio::task::AbortHandle>,
    /// Abort handle for the replication loop task.
    pub replication_handle: Option<tokio::task::AbortHandle>,
    /// Abort handle for the failure recorder task.
    pub failure_recorder_handle: Option<tokio::task::AbortHandle>,
    /// Abort handle for the retry loop task.
    pub retry_loop_handle: Option<tokio::task::AbortHandle>,
}

impl P2PState {
    /// Create new P2P state with sync pipeline components and abort handles.
    pub fn new(
        handle: P2PHostHandle,
        blockstore: Arc<DefraBlockstore<FfiStore>>,
        merge_handler: Arc<db::DbMergeHandler<FfiStore, DefraBlockstore<FfiStore>>>,
        host_event_handle: tokio::task::AbortHandle,
        replication_handle: tokio::task::AbortHandle,
    ) -> Self {
        Self {
            handle,
            blockstore,
            merge_handler,
            collections: RwLock::new(Vec::new()),
            documents: RwLock::new(HashSet::new()),
            peer_addresses: RwLock::new(HashMap::new()),
            host_event_handle: Some(host_event_handle),
            replication_handle: Some(replication_handle),
            failure_recorder_handle: None,
            retry_loop_handle: None,
        }
    }

    /// Abort all background tasks.
    pub fn abort_all_tasks(&self) {
        if let Some(ref h) = self.host_event_handle {
            h.abort();
        }
        if let Some(ref h) = self.replication_handle {
            h.abort();
        }
        if let Some(ref h) = self.failure_recorder_handle {
            h.abort();
        }
        if let Some(ref h) = self.retry_loop_handle {
            h.abort();
        }
    }

    /// Add a collection to P2P (preserves insertion order).
    pub fn add_collection(&self, name: &str) {
        let mut cols = self.collections.write();
        if !cols.contains(&name.to_string()) {
            cols.push(name.to_string());
        }
    }

    /// Remove a collection from P2P.
    pub fn remove_collection(&self, name: &str) -> bool {
        let mut cols = self.collections.write();
        if let Some(pos) = cols.iter().position(|c| c == name) {
            cols.remove(pos);
            true
        } else {
            false
        }
    }

    /// Get all P2P collections (in insertion order).
    pub fn get_collections(&self) -> Vec<String> {
        self.collections.read().clone()
    }

    /// Add a document to P2P.
    pub fn add_document(&self, doc_id: &str) {
        self.documents.write().insert(doc_id.to_string());
    }

    /// Remove a document from P2P.
    pub fn remove_document(&self, doc_id: &str) -> bool {
        self.documents.write().remove(doc_id)
    }

    /// Get all P2P documents.
    pub fn get_documents(&self) -> Vec<String> {
        self.documents.read().iter().cloned().collect()
    }

    /// Store a peer's full multiaddr (called on connect/set_replicator).
    pub fn set_peer_address(&self, peer_id: &str, full_multiaddr: &str) {
        self.peer_addresses
            .write()
            .insert(peer_id.to_string(), full_multiaddr.to_string());
    }

    /// Get the stored multiaddr for a peer.
    pub fn get_peer_address(&self, peer_id: &str) -> Option<String> {
        self.peer_addresses.read().get(peer_id).cloned()
    }
}
