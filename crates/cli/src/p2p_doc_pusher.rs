//! Database-backed DocPusher implementation for P2P replication.

use std::sync::Arc;

use acp::ReplicatedDocActorRelationships;
use async_trait::async_trait;
use cid::Cid;

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
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        let mut info = match peerstore
            .get_replicator(peer_id)
            .await
            .map_err(|e| format!("failed to load persisted replicator: {}", e))?
        {
            Some(bytes) => match p2p::ReplicatorInfo::from_bytes(&bytes) {
                Ok(info) => info,
                Err(e) => {
                    tracing::warn!(
                        peer_id = %peer_id,
                        error = %e,
                        "Replacing invalid persisted replicator"
                    );
                    p2p::ReplicatorInfo::new(pid, collections.to_vec())
                        .map_err(|e| format!("invalid replicator info: {}", e))?
                }
            },
            None => p2p::ReplicatorInfo::new(pid, collections.to_vec())
                .map_err(|e| format!("invalid replicator info: {}", e))?,
        };
        info.id = peer_id.to_string();
        info.collections = collections.to_vec();
        let bytes = info
            .to_bytes()
            .map_err(|e| format!("failed to serialize replicator info: {}", e))?;
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

    async fn load_persisted_replicators(&self) -> Result<Option<Vec<p2p::ReplicatorInfo>>, String> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        let entries = peerstore
            .list_replicators()
            .await
            .map_err(|e| format!("failed to list persisted replicators: {}", e))?;

        let mut replicators = Vec::with_capacity(entries.len());
        for (peer_id, bytes) in entries {
            match p2p::ReplicatorInfo::from_bytes(&bytes) {
                Ok(info) => replicators.push(info),
                Err(e) => {
                    tracing::warn!(
                        peer_id = %peer_id,
                        error = %e,
                        "Skipping invalid persisted replicator"
                    );
                }
            }
        }
        Ok(Some(replicators))
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

    async fn load_document_head_blocks(&self, doc_id: &str) -> Result<Vec<(Cid, Vec<u8>)>, String> {
        let provider = db_merge::DbHeadProvider::new(self.db.clone());
        let heads =
            <db_merge::DbHeadProvider<S> as p2p::sync::DocumentHeadProvider>::get_document_heads(
                &provider, doc_id,
            )
            .await
            .map_err(|e| format!("failed to load document heads: {}", e))?;

        let txn = self
            .db
            .new_txn(true)
            .await
            .map_err(|e| format!("failed to create read transaction: {}", e))?;
        let blockstore = txn
            .blockstore()
            .map_err(|e| format!("failed to get blockstore: {}", e))?;

        let mut blocks = Vec::with_capacity(heads.len());
        for cid in heads {
            let bytes = blockstore
                .get(&cid.to_bytes())
                .await
                .map_err(|e| format!("failed to read head block {cid}: {e}"))?
                .ok_or_else(|| format!("head block {cid} not found"))?;
            blocks.push((cid, bytes));
        }
        Ok(blocks)
    }

    async fn load_doc_actor_relationships(
        &self,
        collection_name: &str,
        doc_id: &str,
    ) -> Result<Option<ReplicatedDocActorRelationships>, String> {
        let Some(acp) = self.document_acp.get() else {
            return Ok(None);
        };
        let collection = match self.db.get_collection(collection_name) {
            Ok(Some(collection)) => collection,
            Ok(None) => return Ok(None),
            Err(e) => {
                return Err(format!(
                    "failed to load collection for ACP relationships: {}",
                    e
                ));
            }
        };
        let Some(policy) = collection.schema().policy.as_ref() else {
            return Ok(None);
        };

        let relationships = acp
            .export_actor_relationships(&policy.id, &policy.resource_name, doc_id)
            .await
            .map_err(|e| format!("failed to export ACP relationships: {}", e))?;

        Ok(Some(ReplicatedDocActorRelationships {
            policy_id: policy.id.clone(),
            resource_name: policy.resource_name.clone(),
            relationships,
        }))
    }

    async fn load_doc_creator_did(
        &self,
        collection_name: &str,
        doc_id: &str,
    ) -> Result<Option<String>, String> {
        let Some(acp) = self.document_acp.get() else {
            return Ok(None);
        };
        let collection = match self.db.get_collection(collection_name) {
            Ok(Some(collection)) => collection,
            Ok(None) => return Ok(None),
            Err(error) => {
                return Err(format!(
                    "failed to load collection for ACP creator resolution: {error}"
                ));
            }
        };
        let Some(policy) = collection.schema().policy.as_ref() else {
            return Ok(None);
        };

        let owner = acp
            .get_doc_owner(&policy.id, &policy.resource_name, doc_id)
            .await
            .map_err(|error| format!("failed to resolve ACP owner DID: {error}"))?;

        Ok(owner.map(|did| did.to_string()))
    }
}

/// Also implement `CollectionLookup` so `DbDocPusher` can be used anywhere
/// the older trait is expected.
impl<S: storage::corekv::Store + 'static> crate::p2p_adapter::CollectionLookup for DbDocPusher<S> {
    fn get_collection_id(&self, name: &str) -> Option<String> {
        DocPusher::get_collection_id(self, name)
    }
}
