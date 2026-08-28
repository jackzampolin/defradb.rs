use bytes::Bytes;
use async_lock::{Mutex as TokioMutex, MutexGuardArc};
use async_trait::async_trait;
use cid::Cid;
use document::{DocID, Document};
use parking_lot::Mutex as PlMutex;
use query::mutator::{
    CreateResult, DeleteResult, DocMutator, MutationBatchController, UpdateResult,
};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use storage::corekv::Store;

use super::helpers::{
    ensure_collection_is_active, register_block_doc_id_mappings, register_created_doc,
    write_branchable_collection_block, write_local_create, write_local_update,
};
use crate::block::builder::DocStorageIdentity;
use crate::block::builder::{write_delete_block, write_document_blocks};
use crate::collection::loader::{get_collection_with_index_manager, get_collection_with_lazy_load};
use crate::database::DB;
use crate::event::emission::register_update_event_callback;
use crate::txn::DbTxn;
use defra_core::encryption::get_encryption_config;
use defra_core::signing::get_signing_config;

pub struct BatchMutator<S: Store> {
    db: Arc<DB<S>>,
    txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
    /// Per-doc write guards held across the WHOLE batch txn so a local counter
    /// read-modify-write and a P2P merge on the same document never interleave
    /// (#1021). The merge handler shares the same `DocWriteQueue`. Released only
    /// after the batch commits/rolls back, so a concurrent merge observes the
    /// committed counter state, never a partial. `batch_gate` keeps this
    /// incremental multi-doc acquirer deadlock-free (see `ensure_doc_guard`).
    doc_guards: PlMutex<BTreeMap<String, MutexGuardArc<()>>>,
    batch_gate: PlMutex<Option<MutexGuardArc<()>>>,
}

impl<S: Store> BatchMutator<S> {
    pub fn new(db: Arc<DB<S>>, txn: Arc<TokioMutex<Option<DbTxn<S>>>>) -> Self {
        Self {
            db,
            txn,
            doc_guards: PlMutex::new(BTreeMap::new()),
            batch_gate: PlMutex::new(None),
        }
    }

    /// Serialize this batch's writes to `doc_id` against concurrent merges/writes
    /// on the same document, holding the guard until the batch commits/rolls back
    /// (#1021). Idempotent per doc within the batch. The first guard taken also
    /// acquires the shared batch gate, held for the whole batch: because this
    /// acquirer discovers its documents incrementally (one mutation at a time) it
    /// cannot pre-sort, so the gate — taken by every other multi-doc acquirer
    /// (batch merges, `create_many`) while they acquire — is what prevents a
    /// lock-ordering deadlock.
    async fn ensure_doc_guard(&self, doc_id: &str) {
        if self.doc_guards.lock().contains_key(doc_id) {
            return;
        }
        if self.batch_gate.lock().is_none() {
            let gate = self.db.doc_write_queue().acquire_batch_gate().await;
            let mut slot = self.batch_gate.lock();
            if slot.is_none() {
                *slot = Some(gate);
            }
        }
        let guard = self.db.doc_write_queue().acquire(doc_id).await;
        self.doc_guards.lock().insert(doc_id.to_string(), guard);
    }

    /// Release the per-doc guards and the batch gate. Called only AFTER the batch
    /// txn is durably committed or discarded.
    fn release_doc_guards(&self) {
        self.doc_guards.lock().clear();
        *self.batch_gate.lock() = None;
    }

    async fn block_and_head_stores(
        &self,
    ) -> query::error::Result<(datastore::NamespaceView, datastore::NamespaceView)> {
        let txn = self.txn.lock().await;
        let txn = txn.as_ref().ok_or_else(|| {
            query::error::QueryError::execution("mutation batch transaction is no longer active")
        })?;
        let blockstore = txn.blockstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get blockstore: {}", e))
        })?;
        let headstore = txn.headstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get headstore: {}", e))
        })?;
        Ok((blockstore, headstore))
    }

    async fn acquire_collection_read_lock(
        &self,
        collection: &crate::collection::Collection,
    ) -> query::error::Result<()> {
        self.db
            .acquire_collection_read_lock(&self.txn, collection.collection_id())
            .await
            .map_err(|error| query::error::QueryError::execution(error.to_string()))
    }

    pub async fn take_txn(&self) -> query::error::Result<DbTxn<S>> {
        self.txn.lock().await.take().ok_or_else(|| {
            query::error::QueryError::execution("mutation batch transaction is no longer active")
        })
    }
}

impl<S: Store + 'static> BatchMutator<S> {
    #[allow(clippy::too_many_arguments)]
    async fn register_update_callback(
        &self,
        collection_name: String,
        collection_id: String,
        doc_id: String,
        doc_cid: Cid,
        doc_block: Bytes,
        collection_block: Option<(Cid, Bytes)>,
    ) -> query::error::Result<()> {
        let mut txn_guard = self.txn.lock().await;
        let txn = txn_guard.as_mut().ok_or_else(|| {
            query::error::QueryError::execution("mutation batch transaction is no longer active")
        })?;
        register_update_event_callback(
            txn,
            self.db.event_bus(),
            // BatchMutator is the auto-commit-batch path; broadcast is handled
            // by the BroadcastMutator wrapper at the per-mutation layer.
            None,
            collection_name,
            collection_id,
            doc_id,
            doc_cid,
            doc_block,
            None,
            collection_block,
            None,
        )
        .map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to register tx update callback: {}",
                e
            ))
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> MutationBatchController for BatchMutator<S> {
    async fn commit(&self) -> query::error::Result<()> {
        let txn = self.take_txn().await?;
        let result = txn.commit().await.map_err(crate::error::commit_query_error);
        // Release per-doc guards only after the durable commit (#1021).
        self.release_doc_guards();
        result
    }

    async fn rollback(&self) -> query::error::Result<()> {
        let txn = self.take_txn().await?;
        let result = txn
            .discard()
            .map_err(|e| query::error::QueryError::execution(format!("discard error: {}", e)));
        self.release_doc_guards();
        result
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocMutator for BatchMutator<S> {
    async fn create(
        &self,
        collection_name: &str,
        mut doc: Document,
    ) -> query::error::Result<CreateResult> {
        self.db
            .check_node_access(None, acp::nac::NodePermission::DocumentUpdate)
            .await
            .map_err(|e| query::error::QueryError::permission_denied(e.to_string()))?;

        let (collection, datastore, systemstore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;
        self.acquire_collection_read_lock(&collection).await?;
        ensure_collection_is_active(&self.db, collection_name, &collection)?;
        let embedding_config = self.db.options().embedding_config();

        crate::search::set_embedding(
            &collection.schema().vector_embeddings,
            &mut doc,
            true,
            None,
            &embedding_config,
        )
        .await
        .map_err(|e| query::error::QueryError::execution(format!("embedding error: {}", e)))?;

        // No per-doc write guard for creates: the DocID is derived from the
        // genesis block inside the txn; the mapping duplicate check is the gate.

        self.db
            .validate_downsample_write(&datastore, &systemstore, collection.schema(), &doc, None)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        let short_id = collection.resolved_root_id();
        let schema_version_id = collection.version_id();
        let enc_config = get_encryption_config();
        let sign_config = get_signing_config();

        let doc_short_id = self
            .db
            .next_doc_short_id()
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
        let identity = DocStorageIdentity::new(short_id, doc_short_id);

        let (doc_id, doc_cid, doc_block, col_block_data) = {
            let (blockstore, headstore) = self.block_and_head_stores().await?;

            let block_result = write_document_blocks(
                &blockstore,
                &headstore,
                &doc,
                schema_version_id,
                identity,
                None,
                enc_config.as_ref(),
                sign_config.as_ref(),
                None,
            )
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to write document blocks for create on collection {}: {}",
                    collection_name, e
                ))
            })?;

            let doc_id = register_created_doc(
                &systemstore,
                &datastore,
                &collection,
                doc_short_id,
                &block_result,
            )
            .await?;
            doc.set_id(doc_id.clone());

            write_local_create(&datastore, &collection, &doc, doc_short_id, &index_manager).await?;

            let col_block_data = write_branchable_collection_block(
                collection_name,
                &collection,
                &blockstore,
                &headstore,
                block_result.cid,
                sign_config.as_ref(),
            )
            .await?;

            (doc_id, block_result.cid, block_result.block, col_block_data)
        };

        self.register_update_callback(
            collection_name.to_string(),
            collection.collection_id().to_string(),
            doc_id.to_string(),
            doc_cid,
            doc_block.clone(),
            col_block_data.clone(),
        )
        .await?;

        let mut result = CreateResult::with_commit(doc_id, doc, doc_cid, doc_block);
        if let Some((col_cid, col_bytes)) = col_block_data {
            result.broadcast_cid = Some(col_cid);
            result.broadcast_block = Some(col_bytes);
        }
        Ok(result)
    }

    async fn update(
        &self,
        collection_name: &str,
        mut doc: Document,
        mut modified_fields: HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        self.db
            .check_node_access(None, acp::nac::NodePermission::DocumentUpdate)
            .await
            .map_err(|e| query::error::QueryError::permission_denied(e.to_string()))?;

        let (collection, datastore, systemstore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;
        self.acquire_collection_read_lock(&collection).await?;
        ensure_collection_is_active(&self.db, collection_name, &collection)?;
        let embedding_config = self.db.options().embedding_config();

        let generated = crate::search::set_embedding(
            &collection.schema().vector_embeddings,
            &mut doc,
            false,
            Some(&modified_fields),
            &embedding_config,
        )
        .await
        .map_err(|e| query::error::QueryError::execution(format!("embedding error: {}", e)))?;

        for field in generated {
            modified_fields.insert(field);
        }

        let doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| query::error::QueryError::execution("update requires a document ID"))?;
        let (doc_short_id, canonical_doc_id) = collection
            .require_doc_identity(&systemstore, &doc_id)
            .await
            .map_err(|e| match e {
                crate::error::Error::DocumentNotFound(id) => {
                    query::error::QueryError::document_not_found(id)
                }
                other => query::error::QueryError::execution(other.to_string()),
            })?;
        doc.set_id(canonical_doc_id.clone());

        // Serialize this update's counter read-modify-write against concurrent
        // merges/writes on the canonical document, held until the batch commits.
        self.ensure_doc_guard(&canonical_doc_id.to_string()).await;

        self.db
            .validate_downsample_write(
                &datastore,
                &systemstore,
                collection.schema(),
                &doc,
                Some(&modified_fields),
            )
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        write_local_update(
            &datastore,
            &collection,
            &mut doc,
            doc_short_id,
            &index_manager,
        )
        .await?;

        let short_id = collection.resolved_root_id();
        let schema_version_id = collection.version_id();
        let enc_config = get_encryption_config();
        let sign_config = get_signing_config();

        let (doc_cid, doc_block, col_block_data) = {
            let (blockstore, headstore) = self.block_and_head_stores().await?;

            let block_result = write_document_blocks(
                &blockstore,
                &headstore,
                &doc,
                schema_version_id,
                DocStorageIdentity::new(short_id, doc_short_id),
                Some(&modified_fields),
                enc_config.as_ref(),
                sign_config.as_ref(),
                None,
            )
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to write document blocks for update on collection {}: {}",
                    collection_name, e
                ))
            })?;

            register_block_doc_id_mappings(
                &systemstore,
                &block_result,
                &canonical_doc_id.to_string(),
            )
            .await?;

            let col_block_data = write_branchable_collection_block(
                collection_name,
                &collection,
                &blockstore,
                &headstore,
                block_result.cid,
                sign_config.as_ref(),
            )
            .await?;

            (block_result.cid, block_result.block, col_block_data)
        };

        if let Some(doc_id) = doc.id() {
            self.register_update_callback(
                collection_name.to_string(),
                collection.collection_id().to_string(),
                doc_id.to_string(),
                doc_cid,
                doc_block.clone(),
                col_block_data.clone(),
            )
            .await?;
        }

        let fields_modified = doc.values().len();
        let mut result = UpdateResult::with_commit(doc, fields_modified, doc_cid, doc_block);
        if let Some((col_cid, col_bytes)) = col_block_data {
            result.broadcast_cid = Some(col_cid);
            result.broadcast_block = Some(col_bytes);
        }
        Ok(result)
    }

    async fn delete(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        self.db
            .check_node_access(None, acp::nac::NodePermission::DocumentDelete)
            .await
            .map_err(|e| query::error::QueryError::permission_denied(e.to_string()))?;

        let (collection, datastore, systemstore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;
        self.acquire_collection_read_lock(&collection).await?;
        ensure_collection_is_active(&self.db, collection_name, &collection)?;

        let Some((doc_short_id, canonical_doc_id)) = collection
            .resolve_doc_identity(&systemstore, doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?
        else {
            return Ok(DeleteResult::new(doc_id.clone(), false));
        };

        let existed = collection
            .delete_with_indexes(&datastore, &canonical_doc_id, doc_short_id, &index_manager)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("delete error: {}", e)))?;

        if !existed {
            return Ok(DeleteResult::new(canonical_doc_id, existed));
        }

        let schema_version_id = collection.version_id();
        let sign_config = get_signing_config();

        let (doc_cid, doc_block, col_block_data) = {
            let (blockstore, headstore) = self.block_and_head_stores().await?;

            let block_result = write_delete_block(
                &blockstore,
                &headstore,
                &canonical_doc_id.to_string(),
                doc_short_id,
                schema_version_id,
                sign_config.as_ref(),
            )
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to write delete block for collection {}: {}",
                    collection_name, e
                ))
            })?;

            let composite_cid = block_result.cid;

            crate::docid::map::set_block_doc_id_mapping(
                &systemstore,
                &composite_cid.to_string(),
                &canonical_doc_id.to_string(),
            )
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

            let col_block_data = write_branchable_collection_block(
                collection_name,
                &collection,
                &blockstore,
                &headstore,
                composite_cid,
                sign_config.as_ref(),
            )
            .await?;

            (composite_cid, block_result.block, col_block_data)
        };

        self.register_update_callback(
            collection_name.to_string(),
            collection.collection_id().to_string(),
            canonical_doc_id.to_string(),
            doc_cid,
            doc_block.clone(),
            col_block_data.clone(),
        )
        .await?;

        let mut result = DeleteResult::with_commit(canonical_doc_id, existed, doc_cid, doc_block);
        if let Some((col_cid, col_bytes)) = col_block_data {
            result.broadcast_cid = Some(col_cid);
            result.broadcast_block = Some(col_bytes);
        }
        Ok(result)
    }

    async fn exists(&self, collection_name: &str, doc_id: &DocID) -> query::error::Result<bool> {
        let (collection, datastore, systemstore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        collection
            .exists_by_doc_id(&datastore, &systemstore, doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("exists error: {}", e)))
    }

    async fn get_for_update(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<Option<Document>> {
        let (collection, datastore, systemstore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        collection
            .get_by_doc_id(&datastore, &systemstore, doc_id)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("get_for_update error: {}", e))
            })
    }
}
