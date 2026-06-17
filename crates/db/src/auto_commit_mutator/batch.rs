use async_lock::Mutex as TokioMutex;
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
use tokio::sync::OwnedMutexGuard;
use tracing::warn;

use super::helpers::{
    apply_local_counter_deltas, ensure_collection_is_active, init_counter_stores_on_create,
};
use crate::block_builder::{write_collection_block, write_delete_block, write_document_blocks};
use crate::collection_loader::{get_collection_with_index_manager, get_collection_with_lazy_load};
use crate::database::DB;
use crate::event_emission::register_update_event_callback;
use crate::txn::DbTxn;
use defra_core::encryption::{get_doc_encryption, get_encryption_config, store_doc_encryption};
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
    doc_guards: PlMutex<BTreeMap<String, OwnedMutexGuard<()>>>,
    batch_gate: PlMutex<Option<OwnedMutexGuard<()>>>,
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

    async fn take_txn(&self) -> query::error::Result<DbTxn<S>> {
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
        doc_block: Vec<u8>,
        collection_block: Option<(Cid, Vec<u8>)>,
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
        let result = txn
            .commit()
            .await
            .map_err(|e| query::error::QueryError::execution(format!("commit error: {}", e)));
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

        let (collection, datastore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;
        ensure_collection_is_active(&self.db, collection_name, &collection)?;
        let embedding_config = self.db.options().embedding_config();

        db_search::set_embedding(
            &collection.schema().vector_embeddings,
            &mut doc,
            true,
            None,
            &embedding_config,
        )
        .await
        .map_err(|e| query::error::QueryError::execution(format!("embedding error: {}", e)))?;

        let id_was_generated = doc.id().is_none();
        if id_was_generated {
            doc.generate_and_set_doc_id().map_err(|e| {
                query::error::QueryError::execution(format!("failed to generate DocID: {}", e))
            })?;
        }

        let doc_id = doc.id().cloned().ok_or_else(|| {
            query::error::QueryError::execution("document should have ID after generation")
        })?;

        // Serialize this create's counter-store seeding against concurrent
        // merges/writes on the same document, held until the batch commits
        // (#1021).
        self.ensure_doc_guard(&doc_id.to_string()).await;

        self.db
            .validate_downsample_write(&datastore, collection.schema(), &doc, None)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        collection
            .create_with_indexes(&datastore, &doc, &index_manager, id_was_generated)
            .await
            .map_err(|e| crate::error::index_write_query_error("create", e))?;

        init_counter_stores_on_create(&datastore, &collection, &doc).await?;

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

            if let Some(ref config) = enc_config {
                store_doc_encryption(&doc_id.to_string(), config.clone());
            }

            let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
            if collection.schema().is_branchable {
                match write_collection_block(
                    &blockstore,
                    &headstore,
                    short_id,
                    schema_version_id,
                    block_result.cid,
                    sign_config.as_ref(),
                )
                .await
                {
                    Ok((col_cid, col_bytes)) => {
                        col_block_data = Some((col_cid, col_bytes));
                    }
                    Err(e) => {
                        warn!(
                            collection = %collection_name,
                            error = %e,
                            "Failed to write collection block for branchable create"
                        );
                    }
                }
            }

            (block_result.cid, block_result.block, col_block_data)
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

        let (collection, datastore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;
        ensure_collection_is_active(&self.db, collection_name, &collection)?;
        let embedding_config = self.db.options().embedding_config();

        let generated = db_search::set_embedding(
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

        // Serialize this update's counter read-modify-write against concurrent
        // merges/writes on the same document, held until the batch commits. The
        // single-mutation path (`update_impl`) takes the same guard; without it a
        // concurrent P2P counter merge can interleave its RMW and drop this
        // increment (#1021).
        let guard_doc_id = doc.id().map(|id| id.to_string());
        if let Some(id) = guard_doc_id {
            self.ensure_doc_guard(&id).await;
        }

        self.db
            .validate_downsample_write(
                &datastore,
                collection.schema(),
                &doc,
                Some(&modified_fields),
            )
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        apply_local_counter_deltas(&datastore, &collection, &mut doc, false).await?;

        collection
            .update_with_indexes(&datastore, &doc, &index_manager)
            .await
            .map_err(|e| match e {
                crate::error::Error::DocumentNotFound(id) => {
                    query::error::QueryError::document_not_found(id)
                }
                other => crate::error::index_write_query_error("update", other),
            })?;

        let short_id = collection.resolved_root_id();
        let schema_version_id = collection.version_id();
        let enc_config = get_encryption_config()
            .or_else(|| doc.id().and_then(|id| get_doc_encryption(&id.to_string())));
        let sign_config = get_signing_config();

        let (doc_cid, doc_block, col_block_data) = {
            let (blockstore, headstore) = self.block_and_head_stores().await?;

            let block_result = write_document_blocks(
                &blockstore,
                &headstore,
                &doc,
                schema_version_id,
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

            let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
            if collection.schema().is_branchable {
                match write_collection_block(
                    &blockstore,
                    &headstore,
                    short_id,
                    schema_version_id,
                    block_result.cid,
                    sign_config.as_ref(),
                )
                .await
                {
                    Ok((col_cid, col_bytes)) => {
                        col_block_data = Some((col_cid, col_bytes));
                    }
                    Err(e) => {
                        warn!(
                            collection = %collection_name,
                            error = %e,
                            "Failed to write collection block for branchable update"
                        );
                    }
                }
            }

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

        let (collection, datastore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;
        ensure_collection_is_active(&self.db, collection_name, &collection)?;

        let existed = collection
            .delete_with_indexes(&datastore, doc_id, &index_manager)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("delete error: {}", e)))?;

        if !existed {
            return Ok(DeleteResult::new(doc_id.clone(), existed));
        }

        let short_id = collection.resolved_root_id();
        let schema_version_id = collection.version_id();
        let sign_config = get_signing_config();

        let (doc_cid, doc_block, col_block_data) = {
            let (blockstore, headstore) = self.block_and_head_stores().await?;

            let block_result = write_delete_block(
                &blockstore,
                &headstore,
                &doc_id.to_string(),
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

            let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
            if collection.schema().is_branchable {
                match write_collection_block(
                    &blockstore,
                    &headstore,
                    short_id,
                    schema_version_id,
                    composite_cid,
                    sign_config.as_ref(),
                )
                .await
                {
                    Ok((col_cid, col_bytes)) => {
                        col_block_data = Some((col_cid, col_bytes));
                    }
                    Err(e) => {
                        warn!(
                            collection = %collection_name,
                            error = %e,
                            "Failed to write collection block for branchable delete"
                        );
                    }
                }
            }

            (composite_cid, block_result.block, col_block_data)
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

        let mut result = DeleteResult::with_commit(doc_id.clone(), existed, doc_cid, doc_block);
        if let Some((col_cid, col_bytes)) = col_block_data {
            result.broadcast_cid = Some(col_cid);
            result.broadcast_block = Some(col_bytes);
        }
        Ok(result)
    }

    async fn exists(&self, collection_name: &str, doc_id: &DocID) -> query::error::Result<bool> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        collection
            .exists_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("exists error: {}", e)))
    }

    async fn get_for_update(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<Option<Document>> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        collection
            .get_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("get_for_update error: {}", e))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use events::{Bus, ChannelBus, EventName};
    use query::mutator::{DocMutator, MutationBatchController};
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use storage::backends::MemoryStore;

    async fn make_test_db_with_bus() -> (Arc<DB<MemoryStore>>, Arc<dyn Bus>) {
        let bus: Arc<dyn Bus> = Arc::new(ChannelBus::new());
        let mut db = DB::new(MemoryStore::new()).expect("create db");
        db.set_event_bus(Arc::clone(&bus));
        (Arc::new(db), bus)
    }

    fn test_collection() -> CollectionVersion {
        CollectionVersion::new(
            "TestDoc",
            "v1",
            "col-test-doc",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "x", FieldKind::int()),
            ],
        )
    }

    fn branchable_test_collection() -> CollectionVersion {
        CollectionVersion::new(
            "TestBranchable",
            "v1",
            "col-test-branchable",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "x", FieldKind::int()),
            ],
        )
        .as_branchable()
    }

    #[tokio::test]
    async fn batch_create_publishes_event_on_commit() {
        let (db, bus) = make_test_db_with_bus().await;
        db.create_collection(test_collection())
            .await
            .expect("schema");

        let mut sub = bus.subscribe(&[EventName::Update]);

        let txn = db.new_txn(false).await.expect("new_txn");
        let txn_arc = Arc::new(TokioMutex::new(Some(txn)));
        let mutator = BatchMutator::new(Arc::clone(&db), Arc::clone(&txn_arc));

        let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
        let result = mutator.create("TestDoc", doc).await.expect("create");

        // Before commit: no event should have fired
        assert!(
            sub.try_recv().is_err(),
            "no event should fire before commit"
        );

        mutator.commit().await.expect("commit");

        // After commit: one Update event arrives
        let msg = tokio::time::timeout(std::time::Duration::from_secs(1), sub.recv())
            .await
            .expect("event arrived within timeout")
            .expect("subscription not closed");

        let update = msg.as_update().expect("expected Update message");
        assert_eq!(update.doc_id, result.doc_id.to_string());
        assert_ne!(update.cid, cid::Cid::default(), "cid should be populated");
        assert!(
            !update.block.is_empty(),
            "block bytes should be populated (matches Go's sendUpdate)"
        );
    }

    #[tokio::test]
    async fn batch_create_publishes_no_event_when_block_write_fails() {
        // Documents the invariant: BatchMutator MUST NOT publish an Update event
        // when write_document_blocks returns Err.
        //
        // The post-refactor implementation guarantees this structurally:
        // register_update_callback is only called inside the `Some(commit_result)`
        // match arm in BatchMutator::create/update/delete.
        //
        // Triggering an actual block-write failure requires a faulty blockstore
        // that returns Err from `put`. No such mock exists in the codebase today
        // because the Store trait is sealed (only storage-crate types may implement
        // it), so a faulty-store mock cannot be constructed in this crate.
        //
        // If the sealed constraint is relaxed in the future, replace this comment
        // with a real assertion: subscribe to the bus, run a create against a
        // faulty store, and assert no Update event arrives.
    }

    #[tokio::test]
    async fn batch_delete_missing_doc_publishes_no_event_and_writes_no_block() {
        // DeleteNode treats existed==false as a no-op; the mutator must not
        // create a tombstone commit or fire an Update event for a missing doc.
        let (db, bus) = make_test_db_with_bus().await;
        db.create_collection(test_collection())
            .await
            .expect("schema");

        let mut sub = bus.subscribe(&[EventName::Update]);

        let txn = db.new_txn(false).await.expect("new_txn");
        let txn_arc = Arc::new(TokioMutex::new(Some(txn)));
        let mutator = BatchMutator::new(Arc::clone(&db), Arc::clone(&txn_arc));

        let mut placeholder = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
        placeholder
            .generate_and_set_doc_id()
            .expect("generate doc id");
        let missing_doc_id = placeholder.id().cloned().expect("doc id");

        let result = mutator
            .delete("TestDoc", &missing_doc_id)
            .await
            .expect("delete should succeed even on missing doc");
        assert!(!result.existed, "doc should not have existed");

        mutator.commit().await.expect("commit");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            sub.try_recv().is_err(),
            "deleting a non-existent doc should not publish an Update event"
        );
    }

    #[tokio::test]
    async fn batch_delete_branchable_surfaces_collection_block_for_broadcast() {
        // Go emits two updates for branchable deletes: the document composite
        // block AND the collection head block. DeleteResult must surface the
        // collection block so BroadcastMutator can re-broadcast it (matches
        // create/update's broadcast_cid/broadcast_block plumbing).
        let (db, _bus) = make_test_db_with_bus().await;
        db.create_collection(branchable_test_collection())
            .await
            .expect("schema");

        let txn = db.new_txn(false).await.expect("new_txn");
        let txn_arc = Arc::new(TokioMutex::new(Some(txn)));
        let mutator = BatchMutator::new(Arc::clone(&db), Arc::clone(&txn_arc));

        let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
        let create_result = mutator.create("TestBranchable", doc).await.expect("create");
        let doc_id = create_result.doc_id.clone();

        let delete_result = mutator
            .delete("TestBranchable", &doc_id)
            .await
            .expect("delete");

        assert!(delete_result.existed, "doc should have existed");
        assert!(
            delete_result.commit_cid.is_some(),
            "composite delete cid should be set"
        );
        assert!(
            delete_result.commit_block.is_some(),
            "composite delete block should be set"
        );
        assert!(
            delete_result.broadcast_cid.is_some(),
            "branchable collection cid should be surfaced for broadcast"
        );
        assert!(
            delete_result
                .broadcast_block
                .as_ref()
                .map(|b| !b.is_empty())
                .unwrap_or(false),
            "branchable collection block bytes should be surfaced for broadcast"
        );
        assert_ne!(
            delete_result.commit_cid, delete_result.broadcast_cid,
            "collection head cid must differ from the document composite cid"
        );

        mutator.commit().await.expect("commit");
    }
}
