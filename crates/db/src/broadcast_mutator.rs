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
use p2p::transport::P2PTransport;
use query::mutator::{BroadcastStatus, CreateResult, DeleteResult, DocMutator, UpdateResult};
use std::sync::Arc;
use storage::corekv::Store;

use crate::auto_commit_mutator::AutoCommitMutator;
use crate::block_builder::{build_blocks_from_document, read_latest_composite_block};
use crate::database::DB;

/// Document mutator that broadcasts changes to P2P network.
///
/// Wraps `AutoCommitMutator` and adds P2P broadcast after successful mutations.
/// Use this mutator when P2P is enabled to propagate changes to peers.
///
/// # Broadcast Behavior
///
/// - **Create**: Builds proper Block structures and broadcasts to network
/// - **Update/Delete**: Reads the committed composite block and broadcasts it
///
/// # Error Handling
///
/// Local mutations are atomic with the transaction. Broadcast failures do NOT
/// roll back the local mutation - instead, the `broadcast_status` field in the
/// result indicates whether broadcast succeeded or failed.
///
/// Callers should check `result.broadcast_status.is_failed()` and handle
/// appropriately (e.g., queue for retry, alert user, etc.).
pub struct BroadcastMutator<S: Store, B: Blockstore, T: P2PTransport = p2p::Libp2pTransport> {
    inner: AutoCommitMutator<S>,
    sync: Arc<SyncCoordinator<B, T>>,
    db: Arc<DB<S>>,
}

impl<S: Store, B: Blockstore + 'static, T: P2PTransport> BroadcastMutator<S, B, T> {
    /// Create a new broadcast-enabled mutator.
    pub fn new(db: Arc<DB<S>>, sync: Arc<SyncCoordinator<B, T>>) -> Self {
        Self {
            inner: AutoCommitMutator::new(db.clone()),
            sync,
            db,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static, B: Blockstore + 'static, T: P2PTransport> DocMutator
    for BroadcastMutator<S, B, T>
{
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

        // Always broadcast the composite block first (ensures composite + field blocks
        // are available on the receiver before any collection block processing).
        let (cid, block, doc_id_str) = if let (Some(cid), Some(block)) =
            (result.commit_cid, result.commit_block.as_ref())
        {
            (cid, block.clone(), result.doc_id.to_string())
        } else {
            // Fallback: build blocks if commit data not available
            match build_blocks_from_document(&result.document, &version_id, self.sync.blockstore())
                .await
            {
                Ok(br) => (br.cid, br.block, br.doc_id),
                Err(e) => {
                    tracing::error!(
                        doc_id = %result.doc_id,
                        collection = %collection_name,
                        error = %e,
                        "Failed to build blocks for P2P broadcast"
                    );
                    return Ok(CreateResult::with_broadcast(
                        result.doc_id,
                        result.document,
                        BroadcastStatus::Failed(format!("Block build failed: {}", e)),
                    ));
                }
            }
        };

        let block_result = BlockResult {
            cid,
            block,
            doc_id: doc_id_str,
            field_cids: vec![],
        };

        // Read broadcast creator DID: when the mutation was initiated with an
        // identity (ACP owner), PushLog messages carry the owner DID so the
        // receiving node can register the document in its local ACP.
        let creator_did = defra_core::signing::get_broadcast_creator_did();
        let creator_ref = creator_did.as_deref();

        // Push the full DAG (field blocks + composite) to replicators.
        // Field blocks are sent first so the receiver has them when the composite arrives,
        // avoiding Bitswap fetches (which can fail after node restarts).
        self.sync
            .push_dag_to_replicators_with_creator(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                &collection_id,
                creator_ref,
            )
            .await;

        // Broadcast composite via GossipSub with retry for InsufficientPeers
        let broadcast_status = broadcast_with_retry_with_creator(
            &self.sync,
            &block_result,
            &collection_id,
            collection_name,
            creator_ref,
        )
        .await;

        // For branchable collections, also broadcast the collection block so receivers
        // get the sender's exact collection CID (critical for CID consistency across nodes).
        // The composite broadcast above ensures the linked blocks are available first.
        if let (Some(col_cid), Some(col_block)) =
            (result.broadcast_cid, result.broadcast_block.as_ref())
        {
            let col_block_result = BlockResult {
                cid: col_cid,
                block: col_block.clone(),
                doc_id: block_result.doc_id.clone(),
                field_cids: vec![],
            };
            self.sync
                .push_to_replicators_with_creator(
                    &col_block_result.cid,
                    &col_block_result.block,
                    &col_block_result.doc_id,
                    &collection_id,
                    creator_ref,
                )
                .await;
            let _ = broadcast_with_retry_with_creator(
                &self.sync,
                &col_block_result,
                &collection_id,
                collection_name,
                creator_ref,
            )
            .await;
        }

        Ok(CreateResult::with_commit_and_broadcast(
            result.doc_id,
            result.document,
            block_result.cid,
            block_result.block,
            broadcast_status,
        ))
    }

    async fn create_many(
        &self,
        collection_name: &str,
        docs: Vec<Document>,
    ) -> query::error::Result<Vec<query::mutator::CreateResult>> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;
        let version_id = collection.version_id().to_string();
        let collection_id = collection.collection_id().to_string();

        // Delegate to inner (single transaction for all docs)
        let results = self.inner.create_many(collection_name, docs).await?;

        // Read broadcast creator DID for P2P ACP propagation
        let creator_did = defra_core::signing::get_broadcast_creator_did();
        let creator_ref = creator_did.as_deref();

        // Broadcast each result
        let mut broadcast_results = Vec::with_capacity(results.len());
        for result in results {
            let (cid, block, doc_id_str) = if let (Some(cid), Some(block)) =
                (result.commit_cid, result.commit_block.as_ref())
            {
                (cid, block.clone(), result.doc_id.to_string())
            } else {
                match build_blocks_from_document(
                    &result.document,
                    &version_id,
                    self.sync.blockstore(),
                )
                .await
                {
                    Ok(br) => (br.cid, br.block, br.doc_id),
                    Err(e) => {
                        tracing::error!(
                            doc_id = %result.doc_id,
                            collection = %collection_name,
                            error = %e,
                            "Failed to build blocks for P2P broadcast"
                        );
                        broadcast_results.push(CreateResult::with_broadcast(
                            result.doc_id,
                            result.document,
                            BroadcastStatus::Failed(format!("Block build failed: {}", e)),
                        ));
                        continue;
                    }
                }
            };

            let block_result = BlockResult {
                cid,
                block,
                doc_id: doc_id_str,
                field_cids: vec![],
            };

            self.sync
                .push_dag_to_replicators_with_creator(
                    &block_result.cid,
                    &block_result.block,
                    &block_result.doc_id,
                    &collection_id,
                    creator_ref,
                )
                .await;

            let broadcast_status = broadcast_with_retry_with_creator(
                &self.sync,
                &block_result,
                &collection_id,
                collection_name,
                creator_ref,
            )
            .await;

            if let (Some(col_cid), Some(col_block)) =
                (result.broadcast_cid, result.broadcast_block.as_ref())
            {
                let col_block_result = BlockResult {
                    cid: col_cid,
                    block: col_block.clone(),
                    doc_id: block_result.doc_id.clone(),
                    field_cids: vec![],
                };
                self.sync
                    .push_to_replicators_with_creator(
                        &col_block_result.cid,
                        &col_block_result.block,
                        &col_block_result.doc_id,
                        &collection_id,
                        creator_ref,
                    )
                    .await;
                let _ = broadcast_with_retry_with_creator(
                    &self.sync,
                    &col_block_result,
                    &collection_id,
                    collection_name,
                    creator_ref,
                )
                .await;
            }

            broadcast_results.push(CreateResult::with_commit_and_broadcast(
                result.doc_id,
                result.document,
                block_result.cid,
                block_result.block,
                broadcast_status,
            ));
        }

        Ok(broadcast_results)
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
        let collection_id = collection.collection_id().to_string();

        // Execute the update mutation
        let result = self
            .inner
            .update(collection_name, doc, modified_fields)
            .await?;

        // Use the committed block directly when available (from ffi/query),
        // falling back to reading from storage.
        let doc_id_for_broadcast = result
            .document
            .id()
            .map(|id| id.to_string())
            .unwrap_or_default();
        let (cid, block, doc_id_str) =
            if let (Some(cid), Some(block)) = (result.commit_cid, result.commit_block.as_ref()) {
                (cid, block.clone(), doc_id_for_broadcast)
            } else {
                // Fallback: read committed composite block from storage
                match read_latest_composite_block(&self.db, &doc_id_for_broadcast).await {
                    Ok(br) => (br.cid, br.block, br.doc_id),
                    Err(e) => {
                        tracing::error!(
                            doc_id = %doc_id_for_broadcast,
                            collection = %collection_name,
                            error = %e,
                            "Failed to read composite block for P2P broadcast"
                        );
                        return Ok(UpdateResult::with_broadcast(
                            result.document,
                            result.fields_modified,
                            BroadcastStatus::Failed(format!("Block read failed: {}", e)),
                        ));
                    }
                }
            };

        let block_result = BlockResult {
            cid,
            block,
            doc_id: doc_id_str,
            field_cids: vec![],
        };

        // Read broadcast creator DID for P2P ACP propagation
        let creator_did = defra_core::signing::get_broadcast_creator_did();
        let creator_ref = creator_did.as_deref();

        // Push the full DAG (field blocks + composite) to replicators.
        self.sync
            .push_dag_to_replicators_with_creator(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                &collection_id,
                creator_ref,
            )
            .await;

        // Broadcast composite via GossipSub with retry for InsufficientPeers
        let broadcast_status = broadcast_with_retry_with_creator(
            &self.sync,
            &block_result,
            &collection_id,
            collection_name,
            creator_ref,
        )
        .await;

        // For branchable collections, also broadcast the collection block so receivers
        // get the sender's exact collection CID (critical for CID consistency across nodes).
        if let (Some(col_cid), Some(col_block)) =
            (result.broadcast_cid, result.broadcast_block.as_ref())
        {
            let col_block_result = BlockResult {
                cid: col_cid,
                block: col_block.clone(),
                doc_id: block_result.doc_id.clone(),
                field_cids: vec![],
            };
            self.sync
                .push_to_replicators_with_creator(
                    &col_block_result.cid,
                    &col_block_result.block,
                    &col_block_result.doc_id,
                    &collection_id,
                    creator_ref,
                )
                .await;
            let _ = broadcast_with_retry_with_creator(
                &self.sync,
                &col_block_result,
                &collection_id,
                collection_name,
                creator_ref,
            )
            .await;
        }

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
        // Get collection ID for broadcast
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;
        let collection_id = collection.collection_id().to_string();

        // Execute the delete mutation
        let result = self.inner.delete(collection_name, doc_id).await?;

        // Read the delete composite block that was written during the mutation.
        let doc_id_str = doc_id.to_string();
        let block_result = match read_latest_composite_block(&self.db, &doc_id_str).await {
            Ok(br) => br,
            Err(e) => {
                tracing::error!(
                    doc_id = %doc_id,
                    collection = %collection_name,
                    error = %e,
                    "Failed to read delete composite block for P2P broadcast"
                );
                return Ok(DeleteResult::with_broadcast(
                    result.doc_id,
                    result.existed,
                    BroadcastStatus::Failed(format!("Block read failed: {}", e)),
                ));
            }
        };

        // Read broadcast creator DID for P2P ACP propagation
        let creator_did = defra_core::signing::get_broadcast_creator_did();
        let creator_ref = creator_did.as_deref();

        // Push to replicators and broadcast via GossipSub
        self.sync
            .push_to_replicators_with_creator(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                &collection_id,
                creator_ref,
            )
            .await;

        let broadcast_status = broadcast_with_retry_with_creator(
            &self.sync,
            &block_result,
            &collection_id,
            collection_name,
            creator_ref,
        )
        .await;

        Ok(DeleteResult::with_broadcast(
            result.doc_id,
            result.existed,
            broadcast_status,
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

use crate::block_builder::BlockResult;

/// Broadcast via GossipSub with retry, optionally overriding the creator DID.
async fn broadcast_with_retry_with_creator<B: Blockstore + 'static, T: P2PTransport>(
    sync: &SyncCoordinator<B, T>,
    block_result: &BlockResult,
    collection_id: &str,
    collection_name: &str,
    creator_override: Option<&str>,
) -> BroadcastStatus {
    const MAX_RETRIES: u32 = 10;
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        match sync
            .broadcast_local_update_with_creator(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                collection_id,
                creator_override,
            )
            .await
        {
            Ok(BroadcastResult::Success) => {
                tracing::debug!(
                    doc_id = %block_result.doc_id,
                    cid = %block_result.cid,
                    collection = %collection_name,
                    attempts = attempt,
                    "Broadcast document to P2P network"
                );
                return BroadcastStatus::Success;
            }
            Ok(BroadcastResult::PartialDocumentOnly { collection_error }) => {
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name,
                    error = %collection_error,
                    "Partial broadcast: document topic succeeded, collection topic failed"
                );
                return BroadcastStatus::Failed(format!(
                    "Partial: collection topic failed: {}",
                    collection_error
                ));
            }
            Ok(BroadcastResult::PartialCollectionOnly { document_error }) => {
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name,
                    error = %document_error,
                    "Partial broadcast: collection topic succeeded, document topic failed"
                );
                return BroadcastStatus::Failed(format!(
                    "Partial: document topic failed: {}",
                    document_error
                ));
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("InsufficientPeers") && attempt <= MAX_RETRIES {
                    let delay_ms = 100 * (1u64 << attempt.min(5));
                    tracing::trace!(
                        doc_id = %block_result.doc_id,
                        attempt = attempt,
                        delay_ms = delay_ms,
                        "Retrying broadcast after InsufficientPeers"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }
                tracing::warn!(
                    doc_id = %block_result.doc_id,
                    collection = %collection_name,
                    error = %e,
                    attempts = attempt,
                    "Failed to broadcast document to P2P network - local mutation succeeded"
                );
                return BroadcastStatus::Failed(e.to_string());
            }
        }
    }
}
