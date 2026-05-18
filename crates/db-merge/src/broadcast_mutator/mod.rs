//! Document mutator with P2P broadcast support.
//!
//! Wraps the standard mutator and broadcasts document changes
//! to the P2P network after successful commits.
//!
//! # Broadcast Status
//!
//! Broadcast is fire-and-forget: mutation results return `BroadcastStatus::Pending`
//! immediately after the local commit. Broadcast failures are logged at `error`
//! level but do not affect the mutation result.

mod batch;
pub(crate) mod broadcast;

use async_trait::async_trait;
use blockstore::Blockstore;
use document::{DocID, Document};
use identity::Did;
use p2p::sync::SyncCoordinator;
use p2p::transport::P2PTransport;
use query::mutator::{
    BroadcastStatus, CreateResult, DeleteResult, DocMutator, MutationBatch,
    MutationBatchController, UpdateResult,
};
use std::sync::Arc;
use storage::corekv::Store;

use self::batch::BroadcastBatchMutator;
use self::broadcast::{broadcast_with_retry_with_creator, log_broadcast_failure};
use db::auto_commit_mutator::AutoCommitMutator;
use db::block_reader::read_latest_composite_block;
use db::database::DB;
use db_blocks::{build_blocks_from_document, BlockResult};

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
/// Local mutations are atomic with the transaction. Broadcast is fire-and-forget
/// via `tokio::spawn` -- the mutation returns `BroadcastStatus::Pending` immediately
/// after the local commit. Broadcast failures are logged at `error` level but do
/// not affect the mutation result. Peers will eventually receive the data via the
/// next replicator sync or DAG fetch.
pub struct BroadcastMutator<S: Store, B: Blockstore, T: P2PTransport = p2p::Libp2pTransport> {
    inner: AutoCommitMutator<S>,
    sync: Arc<SyncCoordinator<B, T>>,
    db: Arc<DB<S>>,
    document_acp: std::sync::OnceLock<Arc<dyn acp::DocumentACP>>,
}

impl<S: Store, B: Blockstore + 'static, T: P2PTransport> BroadcastMutator<S, B, T> {
    /// Create a new broadcast-enabled mutator.
    pub fn new(db: Arc<DB<S>>, sync: Arc<SyncCoordinator<B, T>>) -> Self {
        Self {
            inner: AutoCommitMutator::new(db.clone()),
            sync,
            db,
            document_acp: std::sync::OnceLock::new(),
        }
    }

    pub fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>) {
        let _ = self.document_acp.set(acp);
    }

    async fn register_created_doc_with_acp_if_needed(
        &self,
        collection: &db::Collection,
        doc_id: &str,
        creator_did: Option<&str>,
    ) -> query::error::Result<()> {
        let Some(policy) = collection.schema().policy.as_ref() else {
            return Ok(());
        };
        let Some(creator_did) = creator_did else {
            return Ok(());
        };
        let Some(acp) = self.document_acp.get() else {
            return Ok(());
        };
        let acp: &dyn acp::DocumentACP = acp.as_ref();

        let creator = Did::new(creator_did).map_err(|error| {
            query::error::QueryError::execution(format!("invalid broadcast creator DID: {error}"))
        })?;

        let is_registered = acp
            .is_doc_registered(&policy.id, &policy.resource_name, doc_id)
            .await
            .map_err(|error| {
                query::error::QueryError::execution(format!(
                    "failed to check ACP registration before broadcast: {error}"
                ))
            })?;

        if !is_registered {
            acp.register_doc_object(&creator, &policy.id, &policy.resource_name, doc_id)
                .await
                .map_err(|error| {
                    query::error::QueryError::execution(format!(
                        "failed to register document with ACP before broadcast: {error}"
                    ))
                })?;
        }

        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static, B: Blockstore + 'static, T: P2PTransport> DocMutator
    for BroadcastMutator<S, B, T>
{
    async fn begin_batch(&self) -> query::error::Result<Option<MutationBatch>> {
        let (inner_batch, fetcher) = self.inner.new_batch_components().await?;
        let inner_controller: Arc<dyn MutationBatchController> = inner_batch.clone();
        let broadcast_batch = Arc::new(BroadcastBatchMutator::new(
            inner_batch,
            inner_controller,
            self.sync.clone(),
            self.db.clone(),
        ));
        let mutator: Arc<dyn DocMutator> = broadcast_batch.clone();
        let controller: Arc<dyn MutationBatchController> = broadcast_batch;
        Ok(Some(MutationBatch::new(mutator, fetcher, controller)))
    }

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

        // Build the block result for broadcast
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

        // Read broadcast creator DID before spawning (reads thread-local state).
        let creator_did = defra_core::signing::get_broadcast_creator_did();

        // For ACP-protected collections, ensure newly created docs are registered
        // before any detached P2P broadcast can expose them to other peers.
        self.register_created_doc_with_acp_if_needed(
            &collection,
            &block_result.doc_id,
            creator_did.as_deref(),
        )
        .await?;

        // Capture branchable collection broadcast data before spawning.
        let branchable_data = if let (Some(col_cid), Some(col_block)) =
            (result.broadcast_cid, result.broadcast_block.as_ref())
        {
            Some(BlockResult {
                cid: col_cid,
                block: col_block.clone(),
                doc_id: block_result.doc_id.clone(),
                field_cids: vec![],
            })
        } else {
            None
        };

        // Capture everything for the spawned task by value.
        let sync = self.sync.clone();
        let collection_name_owned = collection_name.to_string();
        let return_cid = block_result.cid;
        let return_block = block_result.block.clone();

        // Spawn broadcast work as a detached task -- the local transaction
        // is already committed, so we return immediately.
        tokio::spawn(async move {
            let creator_ref = creator_did.as_deref();

            // Match Go DefraDB's live replicator model: push the new head block
            // and let the receiver resolve any missing links via DAG sync.
            sync.push_to_replicators_with_creator(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                &collection_id,
                creator_ref,
            )
            .await;

            // Broadcast composite via GossipSub with retry for InsufficientPeers
            log_broadcast_failure(
                &broadcast_with_retry_with_creator(
                    &sync,
                    &block_result,
                    &collection_id,
                    &collection_name_owned,
                    creator_ref,
                )
                .await,
            );

            // For branchable collections, also broadcast the collection block.
            if let Some(col_block_result) = branchable_data {
                sync.push_to_replicators_with_creator(
                    &col_block_result.cid,
                    &col_block_result.block,
                    &col_block_result.doc_id,
                    &collection_id,
                    creator_ref,
                )
                .await;
                log_broadcast_failure(
                    &broadcast_with_retry_with_creator(
                        &sync,
                        &col_block_result,
                        &collection_id,
                        &collection_name_owned,
                        creator_ref,
                    )
                    .await,
                );
            }
        });

        Ok(CreateResult::with_commit_and_broadcast(
            result.doc_id,
            result.document,
            return_cid,
            return_block,
            BroadcastStatus::Pending,
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

        // Read broadcast creator DID before spawning (reads thread-local state).
        let creator_did = defra_core::signing::get_broadcast_creator_did();

        // Build block results and collect broadcast work items.
        // Block building failures return Failed status immediately (not spawned).
        let mut broadcast_results = Vec::with_capacity(results.len());
        let mut broadcast_work: Vec<(BlockResult, Option<BlockResult>)> =
            Vec::with_capacity(results.len());

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

            let branchable_data = if let (Some(col_cid), Some(col_block)) =
                (result.broadcast_cid, result.broadcast_block.as_ref())
            {
                Some(BlockResult {
                    cid: col_cid,
                    block: col_block.clone(),
                    doc_id: block_result.doc_id.clone(),
                    field_cids: vec![],
                })
            } else {
                None
            };

            self.register_created_doc_with_acp_if_needed(
                &collection,
                &block_result.doc_id,
                creator_did.as_deref(),
            )
            .await?;

            broadcast_results.push(CreateResult::with_commit_and_broadcast(
                result.doc_id,
                result.document,
                block_result.cid,
                block_result.block.clone(),
                BroadcastStatus::Pending,
            ));

            broadcast_work.push((block_result, branchable_data));
        }

        // Spawn a single detached task that processes all broadcast work items.
        if !broadcast_work.is_empty() {
            let sync = self.sync.clone();
            let collection_name_owned = collection_name.to_string();

            tokio::spawn(async move {
                let creator_ref = creator_did.as_deref();

                for (block_result, branchable_data) in &broadcast_work {
                    sync.push_to_replicators_with_creator(
                        &block_result.cid,
                        &block_result.block,
                        &block_result.doc_id,
                        &collection_id,
                        creator_ref,
                    )
                    .await;

                    log_broadcast_failure(
                        &broadcast_with_retry_with_creator(
                            &sync,
                            block_result,
                            &collection_id,
                            &collection_name_owned,
                            creator_ref,
                        )
                        .await,
                    );

                    if let Some(col_block_result) = branchable_data {
                        sync.push_to_replicators_with_creator(
                            &col_block_result.cid,
                            &col_block_result.block,
                            &col_block_result.doc_id,
                            &collection_id,
                            creator_ref,
                        )
                        .await;
                        log_broadcast_failure(
                            &broadcast_with_retry_with_creator(
                                &sync,
                                col_block_result,
                                &collection_id,
                                &collection_name_owned,
                                creator_ref,
                            )
                            .await,
                        );
                    }
                }
            });
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

        // Read broadcast creator DID before spawning (reads thread-local state).
        let creator_did = defra_core::signing::get_broadcast_creator_did();

        // Capture branchable collection broadcast data before spawning.
        let branchable_data = if let (Some(col_cid), Some(col_block)) =
            (result.broadcast_cid, result.broadcast_block.as_ref())
        {
            Some(BlockResult {
                cid: col_cid,
                block: col_block.clone(),
                doc_id: block_result.doc_id.clone(),
                field_cids: vec![],
            })
        } else {
            None
        };

        // Capture everything for the spawned task by value.
        let sync = self.sync.clone();
        let collection_name_owned = collection_name.to_string();

        // Spawn broadcast work as a detached task -- the local transaction
        // is already committed, so we return immediately.
        tokio::spawn(async move {
            let creator_ref = creator_did.as_deref();

            // Match Go DefraDB's live replicator model: push the new head block
            // and let the receiver resolve any missing links via DAG sync.
            sync.push_to_replicators_with_creator(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                &collection_id,
                creator_ref,
            )
            .await;

            // Broadcast composite via GossipSub with retry for InsufficientPeers
            log_broadcast_failure(
                &broadcast_with_retry_with_creator(
                    &sync,
                    &block_result,
                    &collection_id,
                    &collection_name_owned,
                    creator_ref,
                )
                .await,
            );

            // For branchable collections, also broadcast the collection block.
            if let Some(col_block_result) = branchable_data {
                sync.push_to_replicators_with_creator(
                    &col_block_result.cid,
                    &col_block_result.block,
                    &col_block_result.doc_id,
                    &collection_id,
                    creator_ref,
                )
                .await;
                log_broadcast_failure(
                    &broadcast_with_retry_with_creator(
                        &sync,
                        &col_block_result,
                        &collection_id,
                        &collection_name_owned,
                        creator_ref,
                    )
                    .await,
                );
            }
        });

        Ok(UpdateResult::with_broadcast(
            result.document,
            result.fields_modified,
            BroadcastStatus::Pending,
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

        // No-op delete (missing doc): the inner mutator wrote nothing, so
        // there's no tombstone block to read or broadcast. Returning here
        // also avoids re-broadcasting the previous (stale) head.
        if !result.existed {
            return Ok(result);
        }

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

        // Read broadcast creator DID before spawning (reads thread-local state).
        let creator_did = defra_core::signing::get_broadcast_creator_did();

        // Capture everything for the spawned task by value.
        let sync = self.sync.clone();
        let collection_name_owned = collection_name.to_string();

        // Spawn broadcast work as a detached task -- the local transaction
        // is already committed, so we return immediately.
        tokio::spawn(async move {
            let creator_ref = creator_did.as_deref();

            // Push to replicators (single block for delete, not full DAG).
            sync.push_to_replicators_with_creator(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                &collection_id,
                creator_ref,
            )
            .await;

            // Broadcast via GossipSub with retry for InsufficientPeers
            log_broadcast_failure(
                &broadcast_with_retry_with_creator(
                    &sync,
                    &block_result,
                    &collection_id,
                    &collection_name_owned,
                    creator_ref,
                )
                .await,
            );
        });

        Ok(DeleteResult::with_broadcast(
            result.doc_id,
            result.existed,
            BroadcastStatus::Pending,
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
