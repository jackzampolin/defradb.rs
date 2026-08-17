//! Document mutator with P2P broadcast support.
//!
//! Wraps the standard mutator and broadcasts document changes
//! to the P2P network after successful commits.
//!
//! # Broadcast Status
//!
//! Broadcast runs asynchronously in coordinator-owned tasks: mutation results return
//! `BroadcastStatus::Pending` immediately after the local commit. Broadcast failures
//! are logged at `error` level but do not affect the mutation result.

mod batch;
pub(crate) mod broadcast;

use async_trait::async_trait;
use blockstore::Blockstore;
use document::{DocID, Document};
use p2p::message::SEArtifact;
use p2p::sync::SyncCoordinator;
use p2p::transport::P2PTransport;
use query::mutator::{
    BroadcastStatus, CreateResult, DeleteResult, DocMutator, MutationBatch,
    MutationBatchController, UpdateResult,
};
use schema::CollectionVersion;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use storage::corekv::Store;
use zeroize::Zeroizing;

use self::batch::BroadcastBatchMutator;
use self::broadcast::{broadcast_with_retry_with_creator, log_broadcast_failure};
use db::auto_commit_mutator::AutoCommitMutator;
use db::block_reader::read_latest_composite_block;
use db::database::DB;
use db_blocks::{build_blocks_from_document, BlockResult};

#[derive(Debug, Clone, Default)]
pub struct BroadcastSeOptions {
    pub encryption_key: Option<Zeroizing<Vec<u8>>>,
    pub identity_pubkey: Option<Vec<u8>>,
}

/// Regenerates and re-pushes searchable-encryption artifacts for a document to
/// the collection's replicators.
///
/// Mirrors Go's `Coordinator.retrySEArtifacts` (`internal/se/coordinator_retry.go`):
/// the producer never stores SE artifacts locally, so on reconnect it regenerates
/// them from the document's current field values and re-pushes. Object-safe so the
/// embedded retry loop and the `p2p_retry_replicators` FFI op can drive SE
/// re-push without naming the transport type.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait SeArtifactRepusher: Send + Sync {
    /// Regenerate SE artifacts for `doc_id` in `collection_id` and push them to
    /// the collection's replicators. A no-op when the collection has no encrypted
    /// indexes, no SE key is provisioned, or the document is absent.
    async fn regenerate_and_push_se_artifacts(&self, collection_id: &str, doc_id: &str);
}

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
/// Local mutations are atomic with the transaction. Broadcast runs in a
/// coordinator-owned background task -- the mutation returns
/// `BroadcastStatus::Pending` immediately after the local commit. Broadcast
/// failures are logged at `error` level but do not affect the mutation result.
/// Peers will eventually receive the data via the next replicator sync or DAG
/// fetch.
pub struct BroadcastMutator<S: Store, B: Blockstore, T: P2PTransport> {
    inner: AutoCommitMutator<S>,
    sync: Arc<SyncCoordinator<B, T>>,
    db: Arc<DB<S>>,
    se_options: Arc<RwLock<BroadcastSeOptions>>,
}

impl<S: Store, B: Blockstore + 'static, T: P2PTransport> BroadcastMutator<S, B, T> {
    /// Create a new broadcast-enabled mutator.
    pub fn new(db: Arc<DB<S>>, sync: Arc<SyncCoordinator<B, T>>) -> Self {
        Self {
            inner: AutoCommitMutator::new(db.clone()),
            sync,
            db,
            se_options: Arc::new(RwLock::new(BroadcastSeOptions::default())),
        }
    }

    pub fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>) {
        self.inner.set_document_acp(acp);
    }

    pub fn set_se_options(&self, options: BroadcastSeOptions) -> Result<(), String> {
        let mut guard = self
            .se_options
            .write()
            .map_err(|_| "broadcast SE options lock poisoned".to_string())?;
        *guard = options;
        Ok(())
    }

    fn load_se_options(&self) -> BroadcastSeOptions {
        self.se_options
            .read()
            .map(|options| options.clone())
            .unwrap_or_default()
    }

    fn generate_se_artifacts(
        &self,
        collection: &CollectionVersion,
        doc_id: &str,
        doc: &Document,
        field_names: &[String],
    ) -> Vec<SEArtifact> {
        if collection.encrypted_indexes.is_empty() {
            return Vec::new();
        }

        let se_options = self.load_se_options();
        let Some(se_key) = se_options.encryption_key else {
            return Vec::new();
        };

        let coordinator = match se_options.identity_pubkey {
            Some(pubkey) => {
                crate::se::SECoordinator::with_key_and_identity(se_key.to_vec(), pubkey)
            }
            None => crate::se::SECoordinator::with_key(se_key.to_vec()),
        };
        let field_values: HashMap<String, document::NormalValue> = doc
            .values()
            .iter()
            .map(|(key, value)| (key.clone(), value.value().clone()))
            .collect();

        match coordinator.generate_artifacts(
            &collection.collection_id,
            doc_id,
            &collection.encrypted_indexes,
            field_names,
            &field_values,
        ) {
            Ok(artifacts) => artifacts
                .into_iter()
                .map(|artifact| {
                    SEArtifact::new(artifact.doc_id, artifact.index_id, artifact.search_tag)
                })
                .collect(),
            Err(error) => {
                tracing::warn!(
                    collection_id = %collection.collection_id,
                    doc_id,
                    error = %error,
                    "failed to generate live SE artifacts"
                );
                Vec::new()
            }
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static, B: Blockstore + 'static, T: P2PTransport> SeArtifactRepusher
    for BroadcastMutator<S, B, T>
{
    async fn regenerate_and_push_se_artifacts(&self, collection_id: &str, doc_id: &str) {
        let collection = match self.db.find_collection_by_id(collection_id) {
            Ok(Some(collection)) => collection,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(collection_id, error = %error, "SE retry: failed to load collection");
                return;
            }
        };
        if collection.schema().encrypted_indexes.is_empty() {
            return;
        }

        let document =
            match db::block_reader::read_document_for_se(&self.db, collection_id, doc_id).await {
                Ok(Some(document)) => document,
                Ok(None) => return,
                Err(error) => {
                    tracing::warn!(doc_id, error = %error, "SE retry: failed to read document");
                    return;
                }
            };

        let artifacts = self.generate_se_artifacts(collection.schema(), doc_id, &document, &[]);
        if artifacts.is_empty() {
            return;
        }
        let document_json =
            serde_json::Value::Object(document.to_map().unwrap_or_default().into_iter().collect());
        self.sync
            .push_se_artifacts_to_replicators_for_document(collection_id, artifacts, &document_json)
            .await;
    }
}

impl<S: Store + 'static, B: Blockstore + 'static, T: P2PTransport> BroadcastMutator<S, B, T> {
    async fn broadcast_update_result(
        &self,
        collection_name: &str,
        se_fields: Vec<String>,
        result: UpdateResult,
    ) -> query::error::Result<UpdateResult> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;
        let collection_id = collection.collection_id().to_string();

        let doc_id_for_broadcast = result
            .document
            .id()
            .map(|id| id.to_string())
            .unwrap_or_default();
        let (cid, block, doc_id_str) =
            if let (Some(cid), Some(block)) = (result.commit_cid, result.commit_block.as_ref()) {
                (cid, block.clone(), doc_id_for_broadcast)
            } else {
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
            encryption_cids: vec![],
        };
        let creator_did = defra_core::signing::get_broadcast_creator_did();
        let branchable_data = if let (Some(col_cid), Some(col_block)) =
            (result.broadcast_cid, result.broadcast_block.as_ref())
        {
            Some(BlockResult {
                cid: col_cid,
                block: col_block.clone(),
                doc_id: String::new(),
                field_cids: vec![],
                encryption_cids: vec![],
            })
        } else {
            None
        };
        let se_artifacts = self.generate_se_artifacts(
            collection.schema(),
            &block_result.doc_id,
            &result.document,
            &se_fields,
        );
        let document_json = serde_json::Value::Object(
            result
                .document
                .to_map()
                .unwrap_or_default()
                .into_iter()
                .collect(),
        );
        let sync = self.sync.clone();
        let collection_name_owned = collection_name.to_string();

        // Transfer durable head-hint ownership before returning the committed
        // mutation. The remaining gossip/artifact work may be asynchronous,
        // but a crash must not land between commit and scope-marker creation.
        let creator_ref = creator_did.as_deref();
        self.sync
            .push_document_to_replicators_with_creator(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                &collection_id,
                &document_json,
                creator_ref,
            )
            .await;
        if let Some(col_block_result) = branchable_data.as_ref() {
            self.sync
                .push_to_replicators_with_creator(
                    &col_block_result.cid,
                    &col_block_result.block,
                    &col_block_result.doc_id,
                    &collection_id,
                    creator_ref,
                )
                .await;
        }

        self.sync
            .spawn_non_authoritative_broadcast_task("broadcast_document_update", async move {
                let creator_ref = creator_did.as_deref();
                sync.push_se_artifacts_to_replicators_for_document(
                    &collection_id,
                    se_artifacts,
                    &document_json,
                )
                .await;
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

                if let Some(col_block_result) = branchable_data {
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
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static, B: Blockstore + 'static, T: P2PTransport> DocMutator
    for BroadcastMutator<S, B, T>
{
    fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>) {
        BroadcastMutator::set_document_acp(self, acp);
    }

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
            encryption_cids: vec![],
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
                doc_id: String::new(),
                field_cids: vec![],
                encryption_cids: vec![],
            })
        } else {
            None
        };
        let se_artifacts = self.generate_se_artifacts(
            collection.schema(),
            &block_result.doc_id,
            &result.document,
            &[],
        );
        let document_json = serde_json::Value::Object(
            result
                .document
                .to_map()
                .unwrap_or_default()
                .into_iter()
                .collect(),
        );

        // Capture everything for the spawned task by value.
        let sync = self.sync.clone();
        let collection_name_owned = collection_name.to_string();
        let return_cid = block_result.cid;
        let return_block = block_result.block.clone();

        // Register the document/collection scope markers before the committed
        // mutation is returned. Network transmission remains queue-owned.
        let creator_ref = creator_did.as_deref();
        self.sync
            .push_document_to_replicators_with_creator(
                &block_result.cid,
                &block_result.block,
                &block_result.doc_id,
                &collection_id,
                &document_json,
                creator_ref,
            )
            .await;
        if let Some(col_block_result) = branchable_data.as_ref() {
            self.sync
                .push_to_replicators_with_creator(
                    &col_block_result.cid,
                    &col_block_result.block,
                    &col_block_result.doc_id,
                    &collection_id,
                    creator_ref,
                )
                .await;
        }

        // The local transaction is already committed. Do not wait for gossip
        // delivery; sender durability is already held by the scope marker.
        self.sync
            .spawn_non_authoritative_broadcast_task("broadcast_document_create", async move {
                let creator_ref = creator_did.as_deref();
                sync.push_se_artifacts_to_replicators_for_document(
                    &collection_id,
                    se_artifacts,
                    &document_json,
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
        let mut broadcast_work: Vec<(
            BlockResult,
            Option<BlockResult>,
            Vec<SEArtifact>,
            serde_json::Value,
        )> = Vec::with_capacity(results.len());

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
                encryption_cids: vec![],
            };

            let branchable_data = if let (Some(col_cid), Some(col_block)) =
                (result.broadcast_cid, result.broadcast_block.as_ref())
            {
                Some(BlockResult {
                    cid: col_cid,
                    block: col_block.clone(),
                    doc_id: String::new(),
                    field_cids: vec![],
                    encryption_cids: vec![],
                })
            } else {
                None
            };

            let se_artifacts = self.generate_se_artifacts(
                collection.schema(),
                &block_result.doc_id,
                &result.document,
                &[],
            );
            let document_json = serde_json::Value::Object(
                result
                    .document
                    .to_map()
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
            );

            broadcast_results.push(CreateResult::with_commit_and_broadcast(
                result.doc_id,
                result.document,
                block_result.cid,
                block_result.block.clone(),
                BroadcastStatus::Pending,
            ));

            broadcast_work.push((block_result, branchable_data, se_artifacts, document_json));
        }

        // Process all broadcast work items in one coordinator-owned task.
        if !broadcast_work.is_empty() {
            let sync = self.sync.clone();
            let collection_name_owned = collection_name.to_string();

            // Batch commit is already durable. Install every scope marker
            // before handing only the non-authoritative gossip/artifact work
            // to the background task.
            let creator_ref = creator_did.as_deref();
            for (block_result, branchable_data, _, document_json) in &broadcast_work {
                self.sync
                    .push_document_to_replicators_with_creator(
                        &block_result.cid,
                        &block_result.block,
                        &block_result.doc_id,
                        &collection_id,
                        document_json,
                        creator_ref,
                    )
                    .await;
                if let Some(col_block_result) = branchable_data {
                    self.sync
                        .push_to_replicators_with_creator(
                            &col_block_result.cid,
                            &col_block_result.block,
                            &col_block_result.doc_id,
                            &collection_id,
                            creator_ref,
                        )
                        .await;
                }
            }

            self.sync.spawn_non_authoritative_broadcast_task(
                "broadcast_document_create_many",
                async move {
                    let creator_ref = creator_did.as_deref();

                    for (block_result, branchable_data, se_artifacts, document_json) in
                        &broadcast_work
                    {
                        sync.push_se_artifacts_to_replicators_for_document(
                            &collection_id,
                            se_artifacts.clone(),
                            document_json,
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
                },
            );
        }

        Ok(broadcast_results)
    }

    async fn update(
        &self,
        collection_name: &str,
        doc: Document,
        modified_fields: std::collections::HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        let se_fields: Vec<String> = modified_fields.iter().cloned().collect();
        let result = self
            .inner
            .update(collection_name, doc, modified_fields)
            .await?;
        self.broadcast_update_result(collection_name, se_fields, result)
            .await
    }

    async fn update_if_unchanged(
        &self,
        collection_name: &str,
        expected: Document,
        doc: Document,
        modified_fields: std::collections::HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        let se_fields: Vec<String> = modified_fields.iter().cloned().collect();
        let result = self
            .inner
            .update_if_unchanged(collection_name, expected, doc, modified_fields)
            .await?;
        self.broadcast_update_result(collection_name, se_fields, result)
            .await
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
        let pre_delete_document_json = match db::block_reader::read_document_for_se(
            &self.db,
            &collection_id,
            &doc_id.to_string(),
        )
        .await
        {
            Ok(Some(document)) => Some(serde_json::Value::Object(
                document.to_map().unwrap_or_default().into_iter().collect(),
            )),
            _ => None,
        };

        // Execute the delete mutation
        let result = self.inner.delete(collection_name, doc_id).await?;

        // No-op delete (missing doc): the inner mutator wrote nothing, so
        // there's no tombstone block to read or broadcast. Returning here
        // also avoids re-broadcasting the previous (stale) head.
        if !result.existed {
            return Ok(result);
        }

        // Prefer the delete block the inner mutator just wrote — re-reading
        // the "latest" composite head via storage would race with concurrent
        // writes on the same doc and broadcast the wrong block. Fall back to
        // reading from storage only when commit artifacts are missing.
        let doc_id_str = doc_id.to_string();
        let block_result =
            if let (Some(cid), Some(block)) = (result.commit_cid, result.commit_block.as_ref()) {
                BlockResult {
                    cid,
                    block: block.clone(),
                    doc_id: doc_id_str,
                    field_cids: vec![],
                    encryption_cids: vec![],
                }
            } else {
                match read_latest_composite_block(&self.db, &doc_id_str).await {
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
                }
            };

        // Read broadcast creator DID before spawning (reads thread-local state).
        let creator_did = defra_core::signing::get_broadcast_creator_did();

        // For branchable collections, capture the collection-level head block
        // so we can broadcast it alongside the composite delete (Go emits two
        // updates for branchable mutations; see internal/db/collection.go:789).
        let branchable_data = if let (Some(col_cid), Some(col_block)) =
            (result.broadcast_cid, result.broadcast_block.as_ref())
        {
            Some(BlockResult {
                cid: col_cid,
                block: col_block.clone(),
                doc_id: String::new(),
                field_cids: vec![],
                encryption_cids: vec![],
            })
        } else {
            None
        };

        // Capture everything for the spawned task by value.
        let sync = self.sync.clone();
        let collection_name_owned = collection_name.to_string();

        // Preserve the committed delete as a durable scope obligation before
        // returning. Deletes and branchable collection commits use the same
        // head-hint queue as creates and updates.
        let creator_ref = creator_did.as_deref();
        if let Some(document_json) = pre_delete_document_json.as_ref() {
            self.sync
                .push_document_to_replicators_with_creator(
                    &block_result.cid,
                    &block_result.block,
                    &block_result.doc_id,
                    &collection_id,
                    document_json,
                    creator_ref,
                )
                .await;
        } else {
            self.sync
                .push_to_replicators_with_creator(
                    &block_result.cid,
                    &block_result.block,
                    &block_result.doc_id,
                    &collection_id,
                    creator_ref,
                )
                .await;
        }
        if let Some(col_block_result) = branchable_data.as_ref() {
            self.sync
                .push_to_replicators_with_creator(
                    &col_block_result.cid,
                    &col_block_result.block,
                    &col_block_result.doc_id,
                    &collection_id,
                    creator_ref,
                )
                .await;
        }

        // The local transaction is already committed; gossip remains
        // fire-and-forget after durable marker registration above.
        self.sync
            .spawn_non_authoritative_broadcast_task("broadcast_document_delete", async move {
                let creator_ref = creator_did.as_deref();

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

                // For branchable collections, also broadcast the collection block.
                if let Some(col_block_result) = branchable_data {
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
