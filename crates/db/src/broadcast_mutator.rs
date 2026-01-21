//! Document mutator with P2P broadcast support.
//!
//! Wraps the standard mutator and broadcasts document changes
//! to the P2P network after successful commits.

use async_trait::async_trait;
use blockstore::Blockstore;
use document::{DocID, Document};
use p2p::sync::SyncCoordinator;
use query::mutator::{CreateResult, DeleteResult, DocMutator, UpdateResult};
use std::sync::Arc;
use storage::corekv::Store;

use crate::auto_commit_mutator::AutoCommitMutator;
use crate::block_builder::build_block_from_document;
use crate::database::DB;

/// Document mutator that broadcasts changes to P2P network.
///
/// Wraps `AutoCommitMutator` and adds P2P broadcast after successful mutations.
/// Use this mutator when P2P is enabled to propagate changes to peers.
pub struct BroadcastMutator<S: Store, B: Blockstore> {
    inner: AutoCommitMutator<S>,
    sync: Arc<SyncCoordinator<B>>,
    db: Arc<DB<S>>,
}

impl<S: Store, B: Blockstore + 'static> BroadcastMutator<S, B> {
    /// Create a new broadcast-enabled mutator.
    pub fn new(db: Arc<DB<S>>, sync: Arc<SyncCoordinator<B>>) -> Self {
        Self {
            inner: AutoCommitMutator::new(db.clone()),
            sync,
            db,
        }
    }
}

#[async_trait]
impl<S: Store + 'static, B: Blockstore + 'static> DocMutator for BroadcastMutator<S, B> {
    async fn create(
        &self,
        collection_name: &str,
        doc: Document,
    ) -> query::error::Result<CreateResult> {
        // Get collection ID for broadcast
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;
        let collection_id = collection.collection_id().to_string();

        // Execute the create mutation
        let result = self.inner.create(collection_name, doc).await?;

        // Build block from created document
        let block_result = build_block_from_document(&result.document)
            .map_err(|e| query::error::QueryError::execution(e))?;

        // Broadcast to network (fire-and-forget, log errors)
        if let Err(e) = self
            .sync
            .broadcast_local_update(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                &collection_id,
            )
            .await
        {
            tracing::warn!(
                doc_id = %block_result.doc_id,
                collection = %collection_name,
                error = %e,
                "Failed to broadcast document create to P2P network"
            );
        } else {
            tracing::debug!(
                doc_id = %block_result.doc_id,
                cid = %block_result.cid,
                collection = %collection_name,
                "Broadcast document create to P2P network"
            );
        }

        Ok(result)
    }

    async fn update(
        &self,
        collection_name: &str,
        doc: Document,
    ) -> query::error::Result<UpdateResult> {
        // Get collection ID for broadcast
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;
        let collection_id = collection.collection_id().to_string();

        // Execute the update mutation
        let result = self.inner.update(collection_name, doc).await?;

        // Build block from updated document
        let block_result = build_block_from_document(&result.document)
            .map_err(|e| query::error::QueryError::execution(e))?;

        // Broadcast to network
        if let Err(e) = self
            .sync
            .broadcast_local_update(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                &collection_id,
            )
            .await
        {
            tracing::warn!(
                doc_id = %block_result.doc_id,
                collection = %collection_name,
                error = %e,
                "Failed to broadcast document update to P2P network"
            );
        } else {
            tracing::debug!(
                doc_id = %block_result.doc_id,
                cid = %block_result.cid,
                collection = %collection_name,
                "Broadcast document update to P2P network"
            );
        }

        Ok(result)
    }

    async fn delete(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        // Execute the delete mutation
        let result = self.inner.delete(collection_name, doc_id).await?;

        // Get collection ID for logging
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;
        let _collection_id = collection.collection_id().to_string();

        // For delete, we would need to create a tombstone block
        // This matches Go behavior where deletes are also broadcast
        // TODO: Implement tombstone block creation and broadcast
        tracing::debug!(
            doc_id = %doc_id,
            collection = %collection_name,
            "Document deleted (P2P delete broadcast not yet implemented)"
        );

        Ok(result)
    }

    async fn exists(&self, collection_name: &str, doc_id: &DocID) -> query::error::Result<bool> {
        self.inner.exists(collection_name, doc_id).await
    }

    async fn get_for_update(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<Option<Document>> {
        self.inner.get_for_update(collection_name, doc_id).await
    }
}
