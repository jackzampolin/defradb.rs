//! Collection lookup trait and fallback DocPusher for P2P.

use std::sync::Arc;

use async_trait::async_trait;

use p2p::P2PHostHandle;

use crate::p2p_adapter::{CollectionLookup, DocPusher};

/// Adapter that wraps a `CollectionLookup` as a `DocPusher` for backward
/// compatibility. Push operations return an error since no DB is available.
pub(crate) struct LookupOnlyDocPusher(pub(crate) Arc<dyn CollectionLookup>);

#[async_trait]
impl DocPusher for LookupOnlyDocPusher {
    async fn push_existing_docs(
        &self,
        _handle: &P2PHostHandle,
        _peer_id: libp2p::PeerId,
        _collections: &[String],
        _se_key: Option<&[u8]>,
        _se_identity_pubkey: Option<&[u8]>,
    ) -> Result<(), String> {
        Err("push_existing_docs not available (no database context)".to_string())
    }

    fn get_collection_id(&self, name: &str) -> Option<String> {
        self.0.get_collection_id(name)
    }

    fn list_collections(&self) -> Result<Vec<String>, String> {
        Err("list_collections not available (no database context)".to_string())
    }

    async fn persist_replicator(
        &self,
        _peer_id: &str,
        _collections: &[String],
    ) -> Result<(), String> {
        Err("persist_replicator not available (no database context)".to_string())
    }

    async fn delete_persisted_replicator(&self, _peer_id: &str) -> Result<(), String> {
        Err("delete_persisted_replicator not available (no database context)".to_string())
    }

    async fn persist_p2p_documents(&self, _doc_ids: &[String]) -> Result<(), String> {
        Err("persist_p2p_documents not available (no database context)".to_string())
    }

    async fn load_p2p_documents(&self) -> Result<Vec<String>, String> {
        Err("load_p2p_documents not available (no database context)".to_string())
    }

    async fn persist_p2p_collections(&self, _collections: &[String]) -> Result<(), String> {
        Err("persist_p2p_collections not available (no database context)".to_string())
    }

    fn validate_collection_exists(&self, _name: &str) -> Result<(), String> {
        Err("validate_collection_exists not available (no database context)".to_string())
    }

    fn validate_branchable_collection(&self, _collection_id: &str) -> Result<(), String> {
        Err("validate_branchable_collection not available (no database context)".to_string())
    }

    async fn retry_doc(
        &self,
        _handle: &P2PHostHandle,
        _peer_id: libp2p::PeerId,
        _doc_id: &str,
        _collection_id: &str,
    ) -> Result<(), String> {
        Err("retry_doc not available (no database context)".to_string())
    }
}

/// Implementation of CollectionLookup for the database.
///
/// Retained for backward compatibility. Prefer `DbDocPusher` for new code.
pub struct DbCollectionLookup<S: storage::corekv::Store> {
    db: Arc<db::DB<S>>,
}

impl<S: storage::corekv::Store + 'static> DbCollectionLookup<S> {
    pub fn new(db: Arc<db::DB<S>>) -> Self {
        Self { db }
    }

    pub fn new_arc(db: Arc<db::DB<S>>) -> Arc<dyn CollectionLookup> {
        Arc::new(Self::new(db))
    }
}

impl<S: storage::corekv::Store + 'static> CollectionLookup for DbCollectionLookup<S> {
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
}
