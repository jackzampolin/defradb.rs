//! Document mutator with P2P broadcast support.
//!
//! Wraps the standard mutator and broadcasts document changes
//! to the P2P network after successful commits.
//!
//! # Broadcast Status
//!
//! All mutation results include a `broadcast_status` field that indicates
//! whether the P2P broadcast succeeded, failed, or was not attempted.
//! Callers should check this status to determine if data synchronization
//! may be incomplete.

use async_trait::async_trait;
use blockstore::Blockstore;
use document::{DocID, Document};
use p2p::sync::{BroadcastResult, SyncCoordinator};
use query::mutator::{BroadcastStatus, CreateResult, DeleteResult, DocMutator, UpdateResult};
use std::sync::Arc;
use storage::corekv::Store;

use crate::auto_commit_mutator::AutoCommitMutator;
use crate::block_builder::build_blocks_from_document;
use crate::database::DB;

/// Document mutator that broadcasts changes to P2P network.
///
/// Wraps `AutoCommitMutator` and adds P2P broadcast after successful mutations.
/// Use this mutator when P2P is enabled to propagate changes to peers.
///
/// # Broadcast Behavior
///
/// - **Create/Update**: Builds proper Block structures and broadcasts to network
/// - **Delete**: Currently not broadcast (tombstone support pending)
///
/// # Error Handling
///
/// Local mutations are atomic with the transaction. Broadcast failures do NOT
/// roll back the local mutation - instead, the `broadcast_status` field in the
/// result indicates whether broadcast succeeded or failed.
///
/// Callers should check `result.broadcast_status.is_failed()` and handle
/// appropriately (e.g., queue for retry, alert user, etc.).
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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
        let version_id = collection.version_id().to_string();
        let collection_id = collection.collection_id().to_string();

        // Execute the create mutation
        let result = self.inner.create(collection_name, doc).await?;

        // Build proper Block structures for P2P sync
        // This creates LWW field blocks + Composite block matching Go's format
        let block_result = match build_blocks_from_document(
            &result.document,
            &version_id, // Go uses VersionID() for collectionVersionID
            self.sync.blockstore(),
        )
        .await
        {
            Ok(br) => br,
            Err(e) => {
                tracing::error!(
                    doc_id = %result.doc_id,
                    collection = %collection_name,
                    error = %e,
                    "Failed to build blocks for P2P broadcast - local mutation succeeded but broadcast aborted"
                );
                return Ok(CreateResult::with_broadcast(
                    result.doc_id,
                    result.document,
                    BroadcastStatus::Failed(format!("Block build failed: {}", e)),
                ));
            }
        };

        // Broadcast to network and capture result
        let broadcast_status = match self
            .sync
            .broadcast_local_update(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                &collection_id,
            )
            .await
        {
            Ok(BroadcastResult::Success) => {
                tracing::debug!(
                    doc_id = %block_result.doc_id,
                    cid = %block_result.cid,
                    collection = %collection_name,
                    field_blocks = block_result.field_cids.len(),
                    "Broadcast document create to P2P network"
                );
                BroadcastStatus::Success
            }
            Ok(BroadcastResult::PartialDocumentOnly { collection_error }) => {
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name,
                    error = %collection_error,
                    "Partial broadcast: document topic succeeded, collection topic failed"
                );
                BroadcastStatus::Failed(format!(
                    "Partial: collection topic failed: {}",
                    collection_error
                ))
            }
            Ok(BroadcastResult::PartialCollectionOnly { document_error }) => {
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name,
                    error = %document_error,
                    "Partial broadcast: collection topic succeeded, document topic failed"
                );
                BroadcastStatus::Failed(format!(
                    "Partial: document topic failed: {}",
                    document_error
                ))
            }
            Err(e) => {
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name,
                    error = %e,
                    "Failed to broadcast document create to P2P network - local mutation succeeded"
                );
                BroadcastStatus::Failed(e.to_string())
            }
        };

        Ok(CreateResult::with_broadcast(
            result.doc_id,
            result.document,
            broadcast_status,
        ))
    }

    async fn update(
        &self,
        collection_name: &str,
        doc: Document,
        modified_fields: std::collections::HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        // Get collection ID for broadcast
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;
        let version_id = collection.version_id().to_string();
        let collection_id = collection.collection_id().to_string();

        // Execute the update mutation
        let result = self
            .inner
            .update(collection_name, doc, modified_fields)
            .await?;

        // Build proper Block structures for P2P sync
        let block_result = match build_blocks_from_document(
            &result.document,
            &version_id,
            self.sync.blockstore(),
        )
        .await
        {
            Ok(br) => br,
            Err(e) => {
                let doc_id = result
                    .document
                    .id()
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "<unknown>".to_string());
                tracing::error!(
                    doc_id = %doc_id,
                    collection = %collection_name,
                    error = %e,
                    "Failed to build blocks for P2P broadcast - local mutation succeeded but broadcast aborted"
                );
                return Ok(UpdateResult::with_broadcast(
                    result.document,
                    result.fields_modified,
                    BroadcastStatus::Failed(format!("Block build failed: {}", e)),
                ));
            }
        };

        // Broadcast to network and capture result
        let broadcast_status = match self
            .sync
            .broadcast_local_update(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                &collection_id,
            )
            .await
        {
            Ok(BroadcastResult::Success) => {
                tracing::debug!(
                    doc_id = %block_result.doc_id,
                    cid = %block_result.cid,
                    collection = %collection_name,
                    field_blocks = block_result.field_cids.len(),
                    "Broadcast document update to P2P network"
                );
                BroadcastStatus::Success
            }
            Ok(BroadcastResult::PartialDocumentOnly { collection_error }) => {
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name,
                    error = %collection_error,
                    "Partial broadcast: document topic succeeded, collection topic failed"
                );
                BroadcastStatus::Failed(format!(
                    "Partial: collection topic failed: {}",
                    collection_error
                ))
            }
            Ok(BroadcastResult::PartialCollectionOnly { document_error }) => {
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name,
                    error = %document_error,
                    "Partial broadcast: collection topic succeeded, document topic failed"
                );
                BroadcastStatus::Failed(format!(
                    "Partial: document topic failed: {}",
                    document_error
                ))
            }
            Err(e) => {
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name,
                    error = %e,
                    "Failed to broadcast document update to P2P network - local mutation succeeded"
                );
                BroadcastStatus::Failed(e.to_string())
            }
        };

        Ok(UpdateResult::with_broadcast(
            result.document,
            result.fields_modified,
            broadcast_status,
        ))
    }

    async fn delete(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        // Execute the delete mutation
        let result = self.inner.delete(collection_name, doc_id).await?;

        // Get collection ID for logging
        let _collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Delete broadcast requires tombstone block creation, which is not yet implemented.
        // Return NotAttempted status so callers know delete was not broadcast.
        tracing::debug!(
            doc_id = %doc_id,
            collection = %collection_name,
            "Document deleted locally (P2P delete broadcast not yet implemented)"
        );

        Ok(DeleteResult::with_broadcast(
            result.doc_id,
            result.existed,
            BroadcastStatus::NotAttempted,
        ))
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
