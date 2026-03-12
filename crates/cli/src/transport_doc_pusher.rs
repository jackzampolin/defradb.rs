//! Transport-generic DocPusher for iroh (and future transports).
//!
//! Mirrors `DocPusher` but uses `P2PTransport` instead of `P2PHostHandle`.
//! The transport is embedded in the struct (not passed per-call) because
//! `P2PTransport` is not object-safe (`Clone` bound).

use std::sync::Arc;

use async_trait::async_trait;
use p2p::transport::PeerId;
use p2p::P2PTransport;

/// Type-erased interface for transport-generic push operations.
#[async_trait]
pub trait TransportDocPusher: Send + Sync {
    async fn push_existing_docs(
        &self,
        peer_id: &PeerId,
        collections: &[String],
        se_key: Option<&[u8]>,
    ) -> Result<(), String>;

    async fn retry_doc(
        &self,
        peer_id: &PeerId,
        doc_id: &str,
        collection_id: &str,
    ) -> Result<(), String>;

    fn get_collection_id(&self, name: &str) -> Option<String>;

    fn list_collections(&self) -> Result<Vec<String>, String>;

    async fn persist_replicator(&self, peer_id: &str, collections: &[String])
        -> Result<(), String>;

    async fn delete_persisted_replicator(&self, peer_id: &str) -> Result<(), String>;

    async fn persist_p2p_documents(&self, doc_ids: &[String]) -> Result<(), String>;

    async fn load_p2p_documents(&self) -> Result<Vec<String>, String>;

    async fn persist_p2p_collections(&self, collections: &[String]) -> Result<(), String>;

    fn validate_collection_exists(&self, name: &str) -> Result<(), String>;

    fn validate_branchable_collection(&self, collection_id: &str) -> Result<(), String>;
}

/// Database-backed `TransportDocPusher` with an embedded transport.
pub struct DbTransportDocPusher<S: storage::corekv::Store, T: P2PTransport> {
    db: Arc<db::DB<S>>,
    transport: T,
    document_acp: std::sync::OnceLock<Arc<dyn acp::DocumentACP>>,
}

impl<S: storage::corekv::Store + 'static, T: P2PTransport> DbTransportDocPusher<S, T> {
    pub fn new(db: Arc<db::DB<S>>, transport: T) -> Self {
        Self {
            db,
            transport,
            document_acp: std::sync::OnceLock::new(),
        }
    }

    pub fn new_arc(db: Arc<db::DB<S>>, transport: T) -> Arc<dyn TransportDocPusher> {
        Arc::new(Self::new(db, transport))
    }

    pub fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>) {
        let _ = self.document_acp.set(acp);
    }
}

#[async_trait]
impl<S: storage::corekv::Store + 'static, T: P2PTransport> TransportDocPusher
    for DbTransportDocPusher<S, T>
{
    async fn push_existing_docs(
        &self,
        peer_id: &PeerId,
        collections: &[String],
        se_key: Option<&[u8]>,
    ) -> Result<(), String> {
        db::push_existing_docs_via_transport(
            &self.transport,
            &self.db,
            self.document_acp.get().map(|acp| acp.as_ref()),
            peer_id,
            collections,
            se_key,
        )
        .await
    }

    async fn retry_doc(
        &self,
        peer_id: &PeerId,
        doc_id: &str,
        collection_id: &str,
    ) -> Result<(), String> {
        db::retry_doc_via_transport(
            &self.transport,
            &self.db,
            self.document_acp.get().map(|acp| acp.as_ref()),
            peer_id,
            doc_id,
            collection_id,
        )
        .await
    }

    fn get_collection_id(&self, name: &str) -> Option<String> {
        match self.db.get_collection(name) {
            Ok(Some(collection)) => Some(collection.collection_id().to_string()),
            Ok(None) => {
                tracing::debug!(collection_name = %name, "Collection not found for P2P lookup");
                None
            }
            Err(e) => {
                tracing::warn!(
                    collection_name = %name,
                    error = %e,
                    "Error looking up collection for P2P"
                );
                None
            }
        }
    }

    fn list_collections(&self) -> Result<Vec<String>, String> {
        self.db
            .list_collections()
            .map_err(|e| format!("failed to list collections: {}", e))
    }

    async fn persist_replicator(
        &self,
        peer_id: &str,
        collections: &[String],
    ) -> Result<(), String> {
        let info =
            p2p::ReplicatorInfo::from_raw(peer_id.to_string(), collections.to_vec(), Vec::new());
        let bytes = info
            .to_bytes()
            .map_err(|e| format!("failed to serialize replicator info: {}", e))?;
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .create_replicator(peer_id, &bytes)
            .await
            .map_err(|e| format!("failed to persist replicator: {}", e))
    }

    async fn delete_persisted_replicator(&self, peer_id: &str) -> Result<(), String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .delete_replicator(peer_id)
            .await
            .map_err(|e| format!("failed to delete persisted replicator: {}", e))
    }

    async fn persist_p2p_documents(&self, doc_ids: &[String]) -> Result<(), String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .persist_documents(doc_ids)
            .await
            .map_err(|e| format!("failed to persist P2P documents: {}", e))
    }

    async fn load_p2p_documents(&self) -> Result<Vec<String>, String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .load_documents()
            .await
            .map_err(|e| format!("failed to load P2P documents: {}", e))
    }

    async fn persist_p2p_collections(&self, collections: &[String]) -> Result<(), String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .persist_collections(collections)
            .await
            .map_err(|e| format!("failed to persist P2P collections: {}", e))
    }

    fn validate_collection_exists(&self, name: &str) -> Result<(), String> {
        self.db
            .require_collection(name)
            .map(|_| ())
            .map_err(|e| format!("{}", e))
    }

    fn validate_branchable_collection(&self, collection_id: &str) -> Result<(), String> {
        match self.db.find_collection_by_id(collection_id) {
            Ok(Some(collection)) => {
                if !collection.schema().is_branchable {
                    Err("collection is not branchable".to_string())
                } else {
                    Ok(())
                }
            }
            Ok(None) => Err(format!("collection with ID '{}' not found", collection_id)),
            Err(e) => Err(format!("failed to find collection: {}", e)),
        }
    }
}
