use std::sync::Arc;

use crate::libp2p_adapter::CollectionLookup;
use async_trait::async_trait;
use cid::Cid;
use p2p::P2PHostHandle;

/// Type-erased interface for libp2p-backed document push operations.
#[async_trait]
pub trait DocPusher: Send + Sync {
    async fn push_existing_docs(
        &self,
        handle: &P2PHostHandle,
        peer_id: libp2p::PeerId,
        collections: &[String],
        se_key: Option<&[u8]>,
        se_identity_pubkey: Option<&[u8]>,
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

    async fn retry_doc(
        &self,
        handle: &P2PHostHandle,
        peer_id: libp2p::PeerId,
        doc_id: &str,
        collection_id: &str,
    ) -> Result<(), String>;

    async fn load_document_head_blocks(&self, doc_id: &str) -> Result<Vec<(Cid, Vec<u8>)>, String>;
}

/// Database-backed `DocPusher` implementation.
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
        db::push_existing_docs(
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
                tracing::debug!(collection_name = %name, "collection not found for P2P lookup");
                None
            }
            Err(error) => {
                tracing::warn!(
                    collection_name = %name,
                    error = %error,
                    "error looking up collection for P2P"
                );
                None
            }
        }
    }

    fn list_collections(&self) -> Result<Vec<String>, String> {
        self.db
            .list_collections()
            .map_err(|error| format!("failed to list collections: {error}"))
    }

    async fn persist_replicator(
        &self,
        peer_id: &str,
        collections: &[String],
    ) -> Result<(), String> {
        let parsed_peer_id: libp2p::PeerId = peer_id
            .parse()
            .map_err(|error| format!("invalid peer ID: {error}"))?;
        let info = p2p::ReplicatorInfo::new(parsed_peer_id, collections.to_vec());
        let bytes = info
            .to_bytes()
            .map_err(|error| format!("failed to serialize replicator info: {error}"))?;
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .create_replicator(peer_id, &bytes)
            .await
            .map_err(|error| format!("failed to persist replicator: {error}"))
    }

    async fn delete_persisted_replicator(&self, peer_id: &str) -> Result<(), String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .delete_replicator(peer_id)
            .await
            .map_err(|error| format!("failed to delete persisted replicator: {error}"))
    }

    async fn persist_p2p_documents(&self, doc_ids: &[String]) -> Result<(), String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .persist_documents(doc_ids)
            .await
            .map_err(|error| format!("failed to persist P2P documents: {error}"))
    }

    async fn load_p2p_documents(&self) -> Result<Vec<String>, String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .load_documents()
            .await
            .map_err(|error| format!("failed to load P2P documents: {error}"))
    }

    async fn persist_p2p_collections(&self, collections: &[String]) -> Result<(), String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .persist_collections(collections)
            .await
            .map_err(|error| format!("failed to persist P2P collections: {error}"))
    }

    fn validate_collection_exists(&self, name: &str) -> Result<(), String> {
        self.db
            .require_collection(name)
            .map(|_| ())
            .map_err(|error| error.to_string())
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
            Ok(None) => Err(format!("collection with ID '{collection_id}' not found")),
            Err(error) => Err(format!("failed to find collection: {error}")),
        }
    }

    async fn retry_doc(
        &self,
        handle: &P2PHostHandle,
        peer_id: libp2p::PeerId,
        doc_id: &str,
        collection_id: &str,
    ) -> Result<(), String> {
        db::retry_doc(
            handle,
            &self.db,
            self.document_acp.get().map(|acp| acp.as_ref()),
            peer_id,
            doc_id,
            collection_id,
        )
        .await
    }

    async fn load_document_head_blocks(&self, doc_id: &str) -> Result<Vec<(Cid, Vec<u8>)>, String> {
        let provider = db::DbHeadProvider::new(self.db.clone());
        let heads = <db::DbHeadProvider<S> as p2p::sync::DocumentHeadProvider>::get_document_heads(
            &provider, doc_id,
        )
        .await
        .map_err(|error| format!("failed to load document heads: {error}"))?;

        let txn = self
            .db
            .new_txn(true)
            .await
            .map_err(|error| format!("failed to create read transaction: {error}"))?;
        let blockstore = txn
            .blockstore()
            .map_err(|error| format!("failed to get blockstore: {error}"))?;

        let mut blocks = Vec::with_capacity(heads.len());
        for cid in heads {
            let bytes = blockstore
                .get(&cid.to_bytes())
                .await
                .map_err(|error| format!("failed to read head block {cid}: {error}"))?
                .ok_or_else(|| format!("head block {cid} not found"))?;
            blocks.push((cid, bytes));
        }
        Ok(blocks)
    }
}

impl<S: storage::corekv::Store + 'static> CollectionLookup for DbDocPusher<S> {
    fn get_collection_id(&self, name: &str) -> Option<String> {
        DocPusher::get_collection_id(self, name)
    }
}
