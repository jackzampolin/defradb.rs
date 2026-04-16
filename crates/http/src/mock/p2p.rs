//! Mock P2P operations for testing P2P handlers.

use async_trait::async_trait;
use std::sync::{Arc, RwLock};

use crate::router::{
    P2PError, P2POperations, P2PResult, P2pDocumentInfo, P2pDocumentRequest, ReplicatorInfo,
};

/// Mock P2P operations for testing P2P handlers.
#[derive(Debug)]
pub struct MockP2POperations {
    peer_id: String,
    addresses: Vec<String>,
    peers: Arc<RwLock<Vec<String>>>,
    replicators: Arc<RwLock<Vec<ReplicatorInfo>>>,
    collections: Arc<RwLock<Vec<String>>>,
}

impl Clone for MockP2POperations {
    fn clone(&self) -> Self {
        Self {
            peer_id: self.peer_id.clone(),
            addresses: self.addresses.clone(),
            peers: Arc::clone(&self.peers),
            replicators: Arc::clone(&self.replicators),
            collections: Arc::clone(&self.collections),
        }
    }
}

impl Default for MockP2POperations {
    fn default() -> Self {
        Self::new()
    }
}

impl MockP2POperations {
    /// Create a new mock P2P operations instance.
    pub fn new() -> Self {
        Self {
            peer_id: "12D3KooWMockPeerId123456789".to_string(),
            addresses: vec!["/ip4/127.0.0.1/tcp/9000".to_string()],
            peers: Arc::new(RwLock::new(vec![])),
            replicators: Arc::new(RwLock::new(vec![])),
            collections: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Create with a connected peer.
    pub fn with_peer(self, peer_id: &str) -> Self {
        self.peers.write().unwrap().push(peer_id.to_string());
        self
    }

    /// Create with a replicator.
    pub fn with_replicator(self, collections: Vec<String>, address: Option<String>) -> Self {
        self.replicators.write().unwrap().push(ReplicatorInfo {
            id: Some("12D3KooWReplicator".to_string()),
            collections,
            address,
        });
        self
    }

    /// Create with P2P collections.
    pub fn with_collections(self, collections: Vec<String>) -> Self {
        *self.collections.write().unwrap() = collections;
        self
    }
}

#[async_trait]
impl P2POperations for MockP2POperations {
    async fn local_peer_id(&self) -> P2PResult<String> {
        Ok(self.peer_id.clone())
    }

    async fn listen_addresses(&self) -> P2PResult<Vec<String>> {
        Ok(self.addresses.clone())
    }

    async fn connected_peers(&self) -> P2PResult<Vec<String>> {
        Ok(self.peers.read().unwrap().clone())
    }

    async fn connect_peer(&self, addr: &str) -> P2PResult<()> {
        // Extract a mock peer ID from the address
        let peer_id = if addr.contains("/p2p/") {
            addr.split("/p2p/").last().unwrap_or("unknown").to_string()
        } else {
            format!("peer-{}", addr.len())
        };
        self.peers.write().unwrap().push(peer_id);
        Ok(())
    }

    async fn notify_network_change(&self) -> P2PResult<()> {
        Ok(())
    }

    async fn get_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>> {
        Ok(self.replicators.read().unwrap().clone())
    }

    async fn add_replicator(
        &self,
        collections: Vec<String>,
        addr: Option<&str>,
        _explicit_replay_capabilities: Vec<crate::router::ExplicitReplayCapabilityInput>,
        _expected_authorizer_did: Option<&str>,
    ) -> P2PResult<()> {
        self.replicators.write().unwrap().push(ReplicatorInfo {
            id: Some("12D3KooWNewReplicator".to_string()),
            collections,
            address: addr.map(|s| s.to_string()),
        });
        Ok(())
    }

    async fn remove_replicator(
        &self,
        collections: Vec<String>,
        _addr: Option<&str>,
    ) -> P2PResult<()> {
        let mut replicators = self.replicators.write().unwrap();
        replicators.retain(|r| !collections.iter().all(|c| r.collections.contains(c)));
        Ok(())
    }

    async fn get_collections(&self) -> P2PResult<Vec<String>> {
        Ok(self.collections.read().unwrap().clone())
    }

    async fn add_collections(&self, collections: Vec<String>) -> P2PResult<()> {
        let mut existing = self.collections.write().unwrap();
        for col in collections {
            if !existing.contains(&col) {
                existing.push(col);
            }
        }
        Ok(())
    }

    async fn remove_collections(&self, collections: Vec<String>) -> P2PResult<()> {
        let mut existing = self.collections.write().unwrap();
        existing.retain(|c| !collections.contains(c));
        Ok(())
    }

    async fn get_documents(&self) -> P2PResult<Vec<P2pDocumentInfo>> {
        Ok(vec![])
    }

    async fn add_documents(&self, _docs: Vec<P2pDocumentRequest>) -> P2PResult<()> {
        Ok(())
    }

    async fn remove_documents(&self, _docs: Vec<P2pDocumentRequest>) -> P2PResult<()> {
        Ok(())
    }

    async fn republish_document(&self, _collection_name: &str, _doc_id: &str) -> P2PResult<()> {
        Ok(())
    }

    async fn sync_documents(&self, _collection_name: &str, _doc_ids: Vec<String>) -> P2PResult<()> {
        Ok(())
    }

    async fn sync_branchable_collection(&self, _collection_id: &str) -> P2PResult<()> {
        Ok(())
    }

    async fn sync_collection_versions(&self, _version_ids: Vec<String>) -> P2PResult<()> {
        Ok(())
    }
}

/// Mock P2P operations that always fails with a configurable error.
#[derive(Debug, Clone)]
pub struct FailingMockP2POperations {
    error: String,
}

impl FailingMockP2POperations {
    /// Create a new failing mock with the given error message.
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[async_trait]
impl P2POperations for FailingMockP2POperations {
    async fn local_peer_id(&self) -> P2PResult<String> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn listen_addresses(&self) -> P2PResult<Vec<String>> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn connected_peers(&self) -> P2PResult<Vec<String>> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn connect_peer(&self, _addr: &str) -> P2PResult<()> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn notify_network_change(&self) -> P2PResult<()> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn get_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn add_replicator(
        &self,
        _collections: Vec<String>,
        _addr: Option<&str>,
        _explicit_replay_capabilities: Vec<crate::router::ExplicitReplayCapabilityInput>,
        _expected_authorizer_did: Option<&str>,
    ) -> P2PResult<()> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn remove_replicator(
        &self,
        _collections: Vec<String>,
        _addr: Option<&str>,
    ) -> P2PResult<()> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn get_collections(&self) -> P2PResult<Vec<String>> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn add_collections(&self, _collections: Vec<String>) -> P2PResult<()> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn remove_collections(&self, _collections: Vec<String>) -> P2PResult<()> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn get_documents(&self) -> P2PResult<Vec<P2pDocumentInfo>> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn add_documents(&self, _docs: Vec<P2pDocumentRequest>) -> P2PResult<()> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn remove_documents(&self, _docs: Vec<P2pDocumentRequest>) -> P2PResult<()> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn republish_document(&self, _collection_name: &str, _doc_id: &str) -> P2PResult<()> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn sync_documents(&self, _collection_name: &str, _doc_ids: Vec<String>) -> P2PResult<()> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn sync_branchable_collection(&self, _collection_id: &str) -> P2PResult<()> {
        Err(P2PError::Internal(self.error.clone()))
    }

    async fn sync_collection_versions(&self, _version_ids: Vec<String>) -> P2PResult<()> {
        Err(P2PError::Internal(self.error.clone()))
    }
}
