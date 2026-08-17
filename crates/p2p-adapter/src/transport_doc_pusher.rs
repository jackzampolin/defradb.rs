use std::sync::Arc;

use crate::{P2PError, P2PErrorExt as _, P2PResult};
use async_trait::async_trait;
use cid::Cid;
use p2p::transport::PeerId;
use p2p::P2PTransport;

/// Type-erased interface for transport-generic push operations.
#[async_trait]
pub trait TransportDocPusher: Send + Sync {
    async fn push_retry_marker_stats(&self) -> P2PResult<storage::stores::PushRetryMarkerStats> {
        Ok(storage::stores::PushRetryMarkerStats::default())
    }

    async fn push_existing_docs(
        &self,
        peer_id: &PeerId,
        collections: &[String],
        filters: &p2p::ReplicationFilters,
        se_key: Option<&[u8]>,
    ) -> P2PResult<()>;

    async fn retry_doc(&self, peer_id: &PeerId, doc_id: &str, collection_id: &str)
        -> P2PResult<()>;

    /// Replay a failed collection scope by rederiving current collection heads.
    ///
    /// Collection commits are doc-less, so they cannot be replayed through
    /// `retry_doc`, which resolves work from a document's composite heads.
    async fn retry_collection_commit(&self, peer_id: &PeerId, collection_id: &str)
        -> P2PResult<()>;

    async fn load_document_head_blocks(&self, doc_id: &str) -> P2PResult<Vec<(Cid, Vec<u8>)>>;

    async fn load_doc_creator_did(
        &self,
        collection_name: &str,
        doc_id: &str,
    ) -> P2PResult<Option<String>>;

    fn get_collection_id(&self, name: &str) -> Option<String>;

    fn list_collections(&self) -> P2PResult<Vec<String>>;

    fn validate_replication_filters(&self, _filters: &p2p::ReplicationFilters) -> P2PResult<()> {
        Err(P2PError::invalid_input(
            "no database context to validate replication filters",
        ))
    }

    async fn persist_replicator(&self, peer_id: &str, collections: &[String]) -> P2PResult<()>;

    async fn persist_replicator_info(&self, info: &p2p::ReplicatorInfo) -> P2PResult<()> {
        self.persist_replicator(info.peer_id_str(), &info.collections)
            .await
    }

    async fn delete_persisted_replicator(&self, peer_id: &str) -> P2PResult<()>;

    async fn load_persisted_replicators(&self) -> P2PResult<Option<Vec<p2p::ReplicatorInfo>>> {
        Ok(None)
    }

    async fn persist_p2p_documents(&self, doc_ids: &[String]) -> P2PResult<()>;

    async fn load_p2p_documents(&self) -> P2PResult<Vec<String>>;

    async fn persist_p2p_collections(&self, collections: &[String]) -> P2PResult<()>;

    fn validate_collection_exists(&self, name: &str) -> P2PResult<()>;

    fn validate_branchable_collection(&self, collection_id: &str) -> P2PResult<()>;
}

/// Database-backed `TransportDocPusher` with an embedded transport.
pub struct DbTransportDocPusher<S: storage::corekv::Store, T: P2PTransport> {
    db: Arc<db::DB<S>>,
    transport: T,
    car_authority: p2p::sync::HeadHintCarAuthority,
    document_acp: std::sync::OnceLock<Arc<dyn acp::DocumentACP>>,
}

impl<S: storage::corekv::Store + 'static, T: P2PTransport> DbTransportDocPusher<S, T> {
    pub fn new(
        db: Arc<db::DB<S>>,
        transport: T,
        car_authority: p2p::sync::HeadHintCarAuthority,
    ) -> Self {
        Self {
            db,
            transport,
            car_authority,
            document_acp: std::sync::OnceLock::new(),
        }
    }

    pub fn new_arc(
        db: Arc<db::DB<S>>,
        transport: T,
        car_authority: p2p::sync::HeadHintCarAuthority,
    ) -> Arc<dyn TransportDocPusher> {
        Arc::new(Self::new(db, transport, car_authority))
    }

    pub fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>) {
        let _ = self.document_acp.set(acp);
    }
}

#[async_trait]
impl<S: storage::corekv::Store + 'static, T: P2PTransport> TransportDocPusher
    for DbTransportDocPusher<S, T>
{
    async fn push_retry_marker_stats(&self) -> P2PResult<storage::stores::PushRetryMarkerStats> {
        storage::stores::Peerstore::new(self.db.store().clone())
            .push_retry_marker_stats()
            .await
            .map_err(|error| P2PError::internal(error.to_string()))
    }

    async fn push_existing_docs(
        &self,
        peer_id: &PeerId,
        collections: &[String],
        filters: &p2p::ReplicationFilters,
        se_key: Option<&[u8]>,
    ) -> P2PResult<()> {
        db_merge::push_existing_docs_via_transport(
            &self.transport,
            &self.db,
            self.document_acp.get().map(|acp| acp.as_ref()),
            peer_id,
            collections,
            filters,
            se_key,
            &replication_filter::QueryReplicationFilterMatcher::new(),
            &self.car_authority,
        )
        .await
        .map_err(P2PError::from)
    }

    async fn retry_doc(
        &self,
        peer_id: &PeerId,
        doc_id: &str,
        collection_id: &str,
    ) -> P2PResult<()> {
        let peer_id_str = peer_id.to_string();
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        let filters = match peerstore.get_replicator(&peer_id_str).await {
            Ok(Some(bytes)) => p2p::ReplicatorInfo::from_bytes(&bytes)
                .map(|info| info.filters)
                .unwrap_or_default(),
            _ => p2p::ReplicationFilters::new(),
        };
        db_merge::retry_doc_via_transport(
            &self.transport,
            &self.db,
            self.document_acp.get().map(|acp| acp.as_ref()),
            peer_id,
            doc_id,
            collection_id,
            &filters,
            &replication_filter::QueryReplicationFilterMatcher::new(),
            &self.car_authority,
        )
        .await
        .map_err(P2PError::from)
    }

    async fn retry_collection_commit(
        &self,
        peer_id: &PeerId,
        collection_id: &str,
    ) -> P2PResult<()> {
        db_merge::retry_collection_commit_via_transport(
            &self.transport,
            &self.db,
            peer_id,
            collection_id,
            &self.car_authority,
        )
        .await
        .map_err(P2PError::from)
    }

    async fn load_document_head_blocks(&self, doc_id: &str) -> P2PResult<Vec<(Cid, Vec<u8>)>> {
        db_merge::load_document_head_blocks(&self.db, doc_id)
            .await
            .map_err(P2PError::internal)
    }

    async fn load_doc_creator_did(
        &self,
        collection_name: &str,
        doc_id: &str,
    ) -> P2PResult<Option<String>> {
        let Some(acp) = self.document_acp.get() else {
            return Ok(None);
        };
        let collection = match self.db.get_collection(collection_name) {
            Ok(Some(collection)) => collection,
            Ok(None) => return Ok(None),
            Err(error) => {
                return Err(P2PError::internal(format!(
                    "failed to load collection for ACP creator resolution: {error}"
                )));
            }
        };
        let Some(policy) = collection.schema().policy.as_ref() else {
            return Ok(None);
        };

        let owner = acp
            .get_doc_owner(&policy.id, &policy.resource_name, doc_id)
            .await
            .map_err(|error| {
                P2PError::internal(format!("failed to resolve ACP owner DID: {error}"))
            })?;

        Ok(owner.map(|did| did.to_string()))
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

    fn validate_replication_filters(&self, filters: &p2p::ReplicationFilters) -> P2PResult<()> {
        for (collection_id, filter) in filters {
            let collection = self
                .db
                .find_collection_by_id(collection_id)
                .map_err(|error| {
                    P2PError::internal(format!(
                        "failed to load collection '{collection_id}': {error}"
                    ))
                })?
                .ok_or_else(|| {
                    P2PError::not_found(format!("collection '{collection_id}' not found"))
                })?;
            replication_filter::validate_replication_filter(
                &collection.schema().fields,
                collection_id,
                filter,
            )
            .map_err(P2PError::invalid_input)?;
        }
        Ok(())
    }

    async fn persist_replicator(&self, peer_id: &str, collections: &[String]) -> P2PResult<()> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        let mut info = match peerstore.get_replicator(peer_id).await.map_err(|error| {
            P2PError::persistence(format!("failed to load persisted replicator: {error}"))
        })? {
            Some(bytes) => p2p::ReplicatorInfo::from_bytes(&bytes).unwrap_or_else(|_| {
                p2p::ReplicatorInfo::from_raw(peer_id.to_string(), collections.to_vec(), Vec::new())
            }),
            None => {
                p2p::ReplicatorInfo::from_raw(peer_id.to_string(), collections.to_vec(), Vec::new())
            }
        };
        info.id = peer_id.to_string();
        info.collections = collections.to_vec();
        let bytes = info.to_bytes().map_err(|error| {
            P2PError::internal(format!("failed to serialize replicator info: {error}"))
        })?;
        peerstore
            .create_replicator(peer_id, &bytes)
            .await
            .map_err(|error| {
                P2PError::persistence(format!("failed to persist replicator: {error}"))
            })
    }

    async fn persist_replicator_info(&self, info: &p2p::ReplicatorInfo) -> P2PResult<()> {
        let peer_id = info.peer_id_str();
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        let mut info = info.clone();
        info.id = peer_id.to_string();
        let bytes = info.to_bytes().map_err(|error| {
            P2PError::internal(format!("failed to serialize replicator info: {error}"))
        })?;
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

    async fn load_persisted_replicators(&self) -> P2PResult<Option<Vec<p2p::ReplicatorInfo>>> {
        let peerstore = storage::stores::Peerstore::new(self.db.store().clone());
        crate::load_persisted_replicators(&peerstore)
            .await
            .map(Some)
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
}
