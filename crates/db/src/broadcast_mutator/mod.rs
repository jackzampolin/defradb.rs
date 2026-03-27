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

use async_trait::async_trait;
use blockstore::Blockstore;
use document::{DocID, Document};
use p2p::sync::{BroadcastResult, SyncCoordinator};
use p2p::transport::P2PTransport;
use query::mutator::{
    BroadcastStatus, CreateResult, DeleteResult, DocMutator, MutationBatch,
    MutationBatchController, UpdateResult,
};
use std::sync::Arc;
use storage::corekv::Store;

use self::batch::BroadcastBatchMutator;
use crate::auto_commit_mutator::AutoCommitMutator;
use crate::block_builder::{build_blocks_from_document, read_latest_composite_block, BlockResult};
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
/// Local mutations are atomic with the transaction. Broadcast is fire-and-forget
/// via `tokio::spawn` — the mutation returns `BroadcastStatus::Pending` immediately
/// after the local commit. Broadcast failures are logged at `error` level but do
/// not affect the mutation result. Peers will eventually receive the data via the
/// next replicator sync or DAG fetch.
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

        // Spawn broadcast work as a detached task — the local transaction
        // is already committed, so we return immediately.
        tokio::spawn(async move {
            let creator_ref = creator_did.as_deref();

            // Push the full DAG (field blocks + composite) to replicators.
            sync.push_dag_to_replicators_with_creator(
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
                    sync.push_dag_to_replicators_with_creator(
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

        // Spawn broadcast work as a detached task — the local transaction
        // is already committed, so we return immediately.
        tokio::spawn(async move {
            let creator_ref = creator_did.as_deref();

            // Push the full DAG (field blocks + composite) to replicators.
            sync.push_dag_to_replicators_with_creator(
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

        // Spawn broadcast work as a detached task — the local transaction
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

const BROADCAST_MAX_RETRIES: u32 = 10;

fn broadcast_retry_delay_ms(err_str: &str, connected_peers: usize, attempt: u32) -> Option<u64> {
    if !err_str.contains("InsufficientPeers") {
        return None;
    }
    if connected_peers == 0 || attempt > BROADCAST_MAX_RETRIES {
        return None;
    }
    Some(100 * (1u64 << attempt.min(5)))
}

/// Log broadcast failures at error level for observability in fire-and-forget paths.
fn log_broadcast_failure(status: &BroadcastStatus) {
    if let BroadcastStatus::Failed(err) = status {
        tracing::error!(
            error = %err,
            "Fire-and-forget broadcast failed — document committed locally but NOT replicated"
        );
    }
}

/// Broadcast via GossipSub with retry, optionally overriding the creator DID.
async fn broadcast_with_retry_with_creator<B: Blockstore + 'static, T: P2PTransport>(
    sync: &SyncCoordinator<B, T>,
    block_result: &BlockResult,
    collection_id: &str,
    collection_name: &str,
    creator_override: Option<&str>,
) -> BroadcastStatus {
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
                let connected_peers = sync.peer_state().stats().connected_peers();
                if let Some(delay_ms) = broadcast_retry_delay_ms(&err_str, connected_peers, attempt)
                {
                    tracing::trace!(
                        doc_id = %block_result.doc_id,
                        attempt = attempt,
                        connected_peers = connected_peers,
                        delay_ms = delay_ms,
                        "Retrying broadcast after InsufficientPeers"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }
                if err_str.contains("InsufficientPeers") && connected_peers == 0 {
                    tracing::debug!(
                        doc_id = %block_result.doc_id,
                        collection = %collection_name,
                        attempts = attempt,
                        "Skipping GossipSub retries because no P2P peers are connected"
                    );
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
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::broadcast_retry_delay_ms;

    #[test]
    fn insufficient_peers_without_connections_does_not_retry() {
        let delay = broadcast_retry_delay_ms("gossipsub publish error: InsufficientPeers", 0, 1);
        assert_eq!(delay, None);
    }

    #[test]
    fn insufficient_peers_with_connections_retries_with_backoff() {
        let delay = broadcast_retry_delay_ms("gossipsub publish error: InsufficientPeers", 2, 3);
        assert_eq!(delay, Some(800));
    }

    #[test]
    fn non_retryable_broadcast_errors_fail_fast() {
        let delay = broadcast_retry_delay_ms("gossipsub publish error: MessageTooLarge", 2, 1);
        assert_eq!(delay, None);
    }
}
