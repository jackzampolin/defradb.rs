//! Database-backed DocPusher implementation for P2P replication.

use std::sync::Arc;

use async_trait::async_trait;

use p2p::P2PHostHandle;

use crate::p2p_adapter::DocPusher;

/// Database-backed `DocPusher` implementation.
///
/// Wraps `db::DB<S>` and delegates to `db_merge::push_existing_docs` for push
/// operations and `db::DB::get_collection` / `list_collections` for lookups.
pub struct DbDocPusher<S: storage::corekv::Store> {
    db: Arc<db::DB<S>>,
    document_acp: std::sync::OnceLock<Arc<dyn acp::DocumentACP>>,
}

impl<S: storage::corekv::Store + 'static> DbDocPusher<S> {
    pub fn new(db: Arc<db::DB<S>>) -> Self {
        Self {
            db,
            document_acp: std::sync::OnceLock::new(),
        }
    }

    pub fn new_arc(db: Arc<db::DB<S>>) -> Arc<dyn DocPusher> {
        Arc::new(Self::new(db))
    }

    pub fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>) {
        let _ = self.document_acp.set(acp);
    }
}

#[async_trait]
impl<S: storage::corekv::Store + 'static> DocPusher for DbDocPusher<S> {
    async fn push_existing_docs(
        &self,
        handle: &P2PHostHandle,
        peer_id: libp2p::PeerId,
        collections: &[String],
        se_key: Option<&[u8]>,
        se_identity_pubkey: Option<&[u8]>,
    ) -> Result<(), String> {
        db_merge::push_existing_docs(
            handle,
            &self.db,
            self.document_acp.get().map(|acp| acp.as_ref()),
            peer_id,
            collections,
            se_key,
            se_identity_pubkey,
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
        let pid: libp2p::PeerId = peer_id
            .parse()
            .map_err(|e| format!("invalid peer ID: {}", e))?;
        let info = p2p::ReplicatorInfo::new(pid, collections.to_vec());
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

    async fn retry_doc(
        &self,
        handle: &P2PHostHandle,
        peer_id: libp2p::PeerId,
        doc_id: &str,
        collection_id: &str,
    ) -> Result<(), String> {
        db_merge::retry_doc(
            handle,
            &self.db,
            self.document_acp.get().map(|acp| acp.as_ref()),
            peer_id,
            doc_id,
            collection_id,
        )
        .await
    }
}

/// Also implement `CollectionLookup` so `DbDocPusher` can be used anywhere
/// the older trait is expected.
impl<S: storage::corekv::Store + 'static> crate::p2p_adapter::CollectionLookup for DbDocPusher<S> {
    fn get_collection_id(&self, name: &str) -> Option<String> {
        DocPusher::get_collection_id(self, name)
    }
}
