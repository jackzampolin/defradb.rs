use std::sync::Arc;

use crate::libp2p::CollectionLookup;
use crate::{P2PError, P2PErrorExt as _, P2PResult};
use acp::ReplicatedDocActorRelationships;
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
    ) -> P2PResult<()>;

    fn get_collection_id(&self, name: &str) -> Option<String>;

    fn list_collections(&self) -> P2PResult<Vec<String>>;

    async fn persist_replicator(&self, peer_id: &str, collections: &[String]) -> P2PResult<()>;

    async fn delete_persisted_replicator(&self, peer_id: &str) -> P2PResult<()>;

    async fn persist_p2p_documents(&self, doc_ids: &[String]) -> P2PResult<()>;

    async fn load_p2p_documents(&self) -> P2PResult<Vec<String>>;

    async fn persist_p2p_collections(&self, collections: &[String]) -> P2PResult<()>;

    fn validate_collection_exists(&self, name: &str) -> P2PResult<()>;

    fn validate_branchable_collection(&self, collection_id: &str) -> P2PResult<()>;

    async fn retry_doc(
        &self,
        handle: &P2PHostHandle,
        peer_id: libp2p::PeerId,
        doc_id: &str,
        collection_id: &str,
    ) -> P2PResult<()>;

    async fn load_document_head_blocks(&self, doc_id: &str) -> P2PResult<Vec<(Cid, Vec<u8>)>>;

    async fn load_doc_actor_relationships(
        &self,
        collection_name: &str,
        doc_id: &str,
    ) -> P2PResult<Option<ReplicatedDocActorRelationships>>;
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
    ) -> P2PResult<()> {
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
        .map_err(P2PError::from)
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

    fn list_collections(&self) -> P2PResult<Vec<String>> {
        self.db
            .list_collections()
            .map_err(|error| P2PError::internal(format!("failed to list collections: {error}")))
    }

    async fn persist_replicator(&self, peer_id: &str, collections: &[String]) -> P2PResult<()> {
        let parsed_peer_id: libp2p::PeerId = peer_id
            .parse()
            .map_err(|error| P2PError::invalid_input(format!("invalid peer ID: {error}")))?;
        let info = p2p::ReplicatorInfo::new(parsed_peer_id, collections.to_vec());
        let bytes = info.to_bytes().map_err(|error| {
            P2PError::internal(format!("failed to serialize replicator info: {error}"))
        })?;
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .create_replicator(peer_id, &bytes)
            .await
            .map_err(|error| {
                P2PError::persistence(format!("failed to persist replicator: {error}"))
            })
    }

    async fn delete_persisted_replicator(&self, peer_id: &str) -> P2PResult<()> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore.delete_replicator(peer_id).await.map_err(|error| {
            P2PError::persistence(format!("failed to delete persisted replicator: {error}"))
        })
    }

    async fn persist_p2p_documents(&self, doc_ids: &[String]) -> P2PResult<()> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore.persist_documents(doc_ids).await.map_err(|error| {
            P2PError::persistence(format!("failed to persist P2P documents: {error}"))
        })
    }

    async fn load_p2p_documents(&self) -> P2PResult<Vec<String>> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore.load_documents().await.map_err(|error| {
            P2PError::persistence(format!("failed to load P2P documents: {error}"))
        })
    }

    async fn persist_p2p_collections(&self, collections: &[String]) -> P2PResult<()> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        peerstore
            .persist_collections(collections)
            .await
            .map_err(|error| {
                P2PError::persistence(format!("failed to persist P2P collections: {error}"))
            })
    }

    fn validate_collection_exists(&self, name: &str) -> P2PResult<()> {
        self.db
            .require_collection(name)
            .map(|_| ())
            .map_err(|error| P2PError::not_found(error.to_string()))
    }

    fn validate_branchable_collection(&self, collection_id: &str) -> P2PResult<()> {
        match self.db.find_collection_by_id(collection_id) {
            Ok(Some(collection)) => {
                if !collection.schema().is_branchable {
                    Err(P2PError::invalid_input("collection is not branchable"))
                } else {
                    Ok(())
                }
            }
            Ok(None) => Err(P2PError::not_found(format!(
                "collection with ID '{collection_id}' not found"
            ))),
            Err(error) => Err(P2PError::internal(format!(
                "failed to find collection: {error}"
            ))),
        }
    }

    async fn retry_doc(
        &self,
        handle: &P2PHostHandle,
        peer_id: libp2p::PeerId,
        doc_id: &str,
        collection_id: &str,
    ) -> P2PResult<()> {
        db_merge::retry_doc(
            handle,
            &self.db,
            self.document_acp.get().map(|acp| acp.as_ref()),
            peer_id,
            doc_id,
            collection_id,
        )
        .await
        .map_err(P2PError::from)
    }

    async fn load_document_head_blocks(&self, doc_id: &str) -> P2PResult<Vec<(Cid, Vec<u8>)>> {
        db_merge::load_document_head_blocks(&self.db, doc_id)
            .await
            .map_err(P2PError::internal)
    }

    async fn load_doc_actor_relationships(
        &self,
        collection_name: &str,
        doc_id: &str,
    ) -> P2PResult<Option<ReplicatedDocActorRelationships>> {
        let Some(acp) = self.document_acp.get() else {
            return Ok(None);
        };
        let collection = match self.db.get_collection(collection_name) {
            Ok(Some(collection)) => collection,
            Ok(None) => return Ok(None),
            Err(error) => {
                return Err(P2PError::internal(format!(
                    "failed to load collection for ACP relationships: {error}"
                )));
            }
        };
        let Some(policy) = collection.schema().policy.as_ref() else {
            return Ok(None);
        };

        let relationships = acp
            .export_actor_relationships(&policy.id, &policy.resource_name, doc_id)
            .await
            .map_err(|error| {
                P2PError::internal(format!("failed to export ACP relationships: {error}"))
            })?;

        Ok(Some(ReplicatedDocActorRelationships {
            policy_id: policy.id.clone(),
            resource_name: policy.resource_name.clone(),
            relationships,
        }))
    }
}

impl<S: storage::corekv::Store + 'static> CollectionLookup for DbDocPusher<S> {
    fn get_collection_id(&self, name: &str) -> Option<String> {
        DocPusher::get_collection_id(self, name)
    }
}
