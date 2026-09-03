//! Document mutator for transaction-scoped mutations.

use async_lock::Mutex as TokioMutex;
use async_trait::async_trait;
use bytes::Bytes;
use cid::Cid;
use document::{DocID, Document};
use query::mutator::{CreateResult, DeleteResult, DocMutator, UpdateResult};
use std::sync::Arc;
use storage::corekv::Store;

use crate::block::builder::DocStorageIdentity;
use crate::block::builder::{write_delete_block, write_document_blocks};
use crate::collection::loader::{get_collection_with_index_manager, get_collection_with_lazy_load};
use crate::collection::Collection;
use crate::database::DB;
use crate::event::emission::register_update_event_callback;
use crate::txn::DbTxn;
use crate::write::autocommit::helpers::write_branchable_collection_block;
use defra_core::encryption::get_encryption_config;
use defra_core::signing::get_signing_config;

fn document_json_value(doc: &Document) -> Option<serde_json::Value> {
    Some(serde_json::Value::Object(
        doc.to_map().ok()?.into_iter().collect(),
    ))
}

/// Document mutator that uses a database transaction.
///
/// This mutator holds a reference to an active transaction and uses the
/// transaction's collection cache with lazy loading.
///
/// # Ownership Model
///
/// The transaction is wrapped in `Arc<TokioMutex<Option<...>>>` because:
/// - `Arc`: Enables the mutator to be cloned and shared across multiple mutation
///   operations within the same transaction
/// - `TokioMutex`: Async-safe interior mutability for concurrent access
/// - `Option`: Enables `take_txn()` to extract the transaction for commit/rollback
///
/// The mutator can share its transaction with `DbDocFetcher` when created via
/// `from_shared_txn()`, allowing both read and write operations within the same
/// transaction context.
///
/// After `take_txn()` is called, all mutator operations will return an error
/// indicating the transaction was consumed. Use `is_consumed()` to check state.
///
/// # Collection Access
///
/// Collections are loaded lazily from the SystemStore on first access within
/// the transaction. Once loaded, the collection metadata is cached for the
/// duration of the transaction. Note: This provides transaction-level caching,
/// not true snapshot isolation - if collections are accessed at different times,
/// they reflect the store state at the time of first access.
pub struct DbDocMutator<S: Store> {
    db: Arc<DB<S>>,
    txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
    broadcaster: Option<Arc<dyn crate::event::emission::TxnBroadcaster>>,
}

impl<S: Store> DbDocMutator<S> {
    /// Create a new transaction-scoped document mutator.
    ///
    /// Collections will be loaded lazily from the transaction's cache.
    pub fn new(db: Arc<DB<S>>, txn: DbTxn<S>) -> Self {
        Self {
            db,
            txn: Arc::new(TokioMutex::new(Some(txn))),
            broadcaster: None,
        }
    }

    /// Create a mutator that shares a transaction and (optionally) forwards
    /// committed writes to a `TxnBroadcaster`. When `broadcaster` is `Some`,
    /// each per-mutation `on_success_async` callback both publishes to the
    /// local event bus and asks the broadcaster to push to P2P peers.
    pub fn from_shared_txn_with_broadcaster(
        db: Arc<DB<S>>,
        txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
        broadcaster: Option<Arc<dyn crate::event::emission::TxnBroadcaster>>,
    ) -> Self {
        Self {
            db,
            txn,
            broadcaster,
        }
    }

    /// Take the transaction out of the mutator (for commit/rollback).
    ///
    /// After calling this, `is_consumed()` will return `true` and all
    /// mutator operations will return an error.
    pub async fn take_txn(&self) -> Option<DbTxn<S>> {
        self.txn.lock().await.take()
    }

    /// Check if the transaction has been consumed (via `take_txn()`).
    ///
    /// Returns `true` if `take_txn()` was called and the transaction is
    /// no longer available for mutations.
    pub async fn is_consumed(&self) -> bool {
        self.txn.lock().await.is_none()
    }

    async fn ensure_collection_can_write(
        &self,
        collection_name: &str,
        collection: &Collection,
    ) -> query::error::Result<()> {
        self.db
            .acquire_collection_read_lock(&self.txn, collection.collection_id())
            .await
            .map_err(|error| query::error::QueryError::execution(error.to_string()))?;

        let was_created_in_txn = {
            let txn_guard = self.txn.lock().await;
            let txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction is no longer active")
            })?;
            txn.was_collection_created(collection.collection_id())
        };

        if was_created_in_txn {
            return Ok(());
        }

        let is_active = self
            .db
            .find_collection_by_id(collection.collection_id())
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .is_some();

        if is_active {
            Ok(())
        } else {
            Err(query::error::QueryError::collection_not_found(
                collection_name,
            ))
        }
    }
}

impl<S: Store + 'static> DbDocMutator<S> {
    #[allow(clippy::too_many_arguments)]
    async fn register_update_callback(
        &self,
        collection_name: String,
        collection_id: String,
        doc_id: String,
        doc_cid: Cid,
        doc_block: Bytes,
        document_json: Option<serde_json::Value>,
        collection_block: Option<(Cid, Bytes)>,
    ) -> query::error::Result<()> {
        let mut txn_guard = self.txn.lock().await;
        let txn = txn_guard.as_mut().ok_or_else(|| {
            query::error::QueryError::execution("transaction is no longer active")
        })?;
        let creator_did = defra_core::signing::get_broadcast_creator_did();
        register_update_event_callback(
            txn,
            self.db.event_bus(),
            self.broadcaster.as_ref(),
            collection_name,
            collection_id,
            doc_id,
            doc_cid,
            doc_block,
            document_json,
            collection_block,
            creator_did,
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
impl<S: Store + 'static> DocMutator for DbDocMutator<S> {
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
        self.ensure_collection_can_write(collection_name, &collection)
            .await?;

        self.db
            .validate_downsample_write(&datastore, &systemstore, collection.schema(), &doc, None)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        let doc_short_id = self
            .db
            .next_doc_short_id()
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
        let identity = DocStorageIdentity::new(collection.resolved_root_id(), doc_short_id);

        // Blocks first: the public DocID is derived from the genesis composite
        // block CID (Go #4838).
        let (doc_id, doc_cid, doc_block, col_block_data) = {
            let txn_guard = self.txn.lock().await;
            let txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction is no longer active")
            })?;

            let blockstore = txn.blockstore().map_err(|e| {
                query::error::QueryError::execution(format!("failed to get blockstore: {}", e))
            })?;
            let headstore = txn.headstore().map_err(|e| {
                query::error::QueryError::execution(format!("failed to get headstore: {}", e))
            })?;

            let schema_version_id = collection.version_id();
            let enc_config = get_encryption_config();
            let sign_config = get_signing_config();
            let kms = self.db.kms();

            let block_result = write_document_blocks(
                &blockstore,
                &headstore,
                &doc,
                schema_version_id,
                identity,
                None,
                enc_config.as_ref(),
                sign_config.as_ref(),
                kms.as_ref(),
            )
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to write document blocks for transaction create on collection {}: {}",
                    collection_name, e
                ))
            })?;

            // Two creates with the same field value encode to the same
            // byte-identical delta and so to the same content-addressed key,
            // which the engine may treat as a non-conflicting blind write.
            // `has_for_update` records the read even though this txn just
            // wrote the block, so the second commit is validated against it
            // and aborts (#1599).
            //
            // Interactive creates only: autocommit, batch and merge keep the
            // blind write, and interactive updates rely on that to let
            // distinct documents share a delta block (#1194). Encryption
            // blocks stay out: the KMS commits the DEK in its own txn, so
            // reading it back aborts every encrypted create.
            for cid in block_result.field_cids.iter().chain([&block_result.cid]) {
                blockstore
                    .has_for_update(&cid.to_bytes())
                    .await
                    .map_err(|e| {
                        query::error::QueryError::execution(format!(
                            "failed to record block read for {}: {}",
                            cid, e
                        ))
                    })?;
            }

            let doc_id = crate::write::autocommit::helpers::register_created_doc(
                &systemstore,
                &datastore,
                &collection,
                doc_short_id,
                &block_result,
            )
            .await?;
            doc.set_id(doc_id.clone());

            let col_block_data = write_branchable_collection_block(
                &self.db,
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

        // #1044 record-then-finalize: write the doc blob + indexes WITHOUT seeding
        // the counter store and WITHOUT taking any per-doc guard/batch gate. The
        // counter-store seeding (and its per-doc guard) is deferred to the
        // commit-time finalize so an interactive txn holds no gate over its
        // user-controlled lifetime. See `InteractiveTxnCounter.tla`.
        crate::write::autocommit::helpers::write_local_create_deferred(
            &datastore,
            &collection,
            &doc,
            doc_short_id,
            &index_manager,
        )
        .await?;

        // Record a seed op for each counter field present so the finalize seeds
        // the authoritative accumulation store (the created value is absolute).
        {
            let mut counter_ops = Vec::new();
            for field in &collection.schema().fields {
                if !field.crdt_type.is_counter() {
                    continue;
                }
                let Some(value) = doc.get(&field.name) else {
                    continue;
                };
                counter_ops.push(crate::txn::PendingCounterOp {
                    collection_name: collection_name.to_string(),
                    schema_version_id: collection.version_id().to_string(),
                    doc_id: doc_id.to_string(),
                    field: field.name.clone(),
                    delta: value.clone(),
                    base: None,
                    is_create: true,
                });
            }
            if !counter_ops.is_empty() {
                let mut txn_guard = self.txn.lock().await;
                let txn = txn_guard.as_mut().ok_or_else(|| {
                    query::error::QueryError::execution("transaction is no longer active")
                })?;
                for op in counter_ops {
                    txn.record_counter_op(op);
                }
            }
        }

        self.register_update_callback(
            collection_name.to_string(),
            collection.collection_id().to_string(),
            doc_id.to_string(),
            doc_cid,
            doc_block,
            document_json_value(&doc),
            col_block_data,
        )
        .await?;

        Ok(CreateResult::new(doc_id, doc))
    }

    async fn update(
        &self,
        collection_name: &str,
        mut doc: Document,
        modified_fields: std::collections::HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        self.db
            .check_node_access(None, acp::nac::NodePermission::DocumentUpdate)
            .await
            .map_err(|e| query::error::QueryError::permission_denied(e.to_string()))?;

        let (collection, datastore, systemstore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;
        self.ensure_collection_can_write(collection_name, &collection)
            .await?;

        let update_doc_id = doc
            .id()
            .cloned()
            .ok_or_else(|| query::error::QueryError::execution("update requires a document ID"))?;
        let (doc_short_id, canonical_doc_id) = collection
            .require_doc_identity(&systemstore, &update_doc_id)
            .await
            .map_err(|e| match e {
                crate::error::Error::DocumentNotFound(id) => {
                    query::error::QueryError::document_not_found(id)
                }
                other => query::error::QueryError::execution(other.to_string()),
            })?;
        doc.set_id(canonical_doc_id.clone());

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

        // #1044 record-then-finalize: write the doc blob + indexes with the
        // query-plan's provisional counter value, but do NOT do the counter RMW
        // here and take NO per-doc guard/batch gate. Each counter field carrying a
        // delta is RECORDED on the txn; the RMW (under a per-doc guard) and the
        // blob correction happen at the commit-time finalize. This keeps an
        // interactive txn from holding the process-wide gate over its
        // user-controlled lifetime. See `InteractiveTxnCounter.tla`.
        let mut counter_ops = Vec::new();
        // Read the PRE-WRITE committed doc ONCE (before the provisional blob write
        // below) so each counter op records its pre-update committed value as the
        // reconcile base — replicating the inline `apply_local_counter_deltas`
        // semantics. Re-reading at finalize would observe the already-overwritten
        // provisional blob and double-apply the delta for a PCounter (#1044).
        let committed_pre_write = if collection
            .schema()
            .fields
            .iter()
            .any(|f| f.crdt_type.is_counter() && doc.get_counter_delta(&f.name).is_some())
        {
            collection
                .get_with_datastore(&datastore, doc_short_id, &canonical_doc_id)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?
        } else {
            None
        };
        for field in &collection.schema().fields {
            if !field.crdt_type.is_counter() {
                continue;
            }
            let Some(delta) = doc.get_counter_delta(&field.name) else {
                continue;
            };
            if let Some(id) = doc.id().map(|id| id.to_string()) {
                let base = committed_pre_write
                    .as_ref()
                    .and_then(|d| d.get(&field.name))
                    .cloned();
                counter_ops.push(crate::txn::PendingCounterOp {
                    collection_name: collection_name.to_string(),
                    schema_version_id: collection.version_id().to_string(),
                    doc_id: id,
                    field: field.name.clone(),
                    delta: delta.clone(),
                    base,
                    is_create: false,
                });
            }
        }

        crate::write::autocommit::helpers::write_local_update_deferred(
            &datastore,
            &collection,
            &doc,
            doc_short_id,
            &index_manager,
        )
        .await?;

        if !counter_ops.is_empty() {
            let mut txn_guard = self.txn.lock().await;
            let txn = txn_guard.as_mut().ok_or_else(|| {
                query::error::QueryError::execution("transaction is no longer active")
            })?;
            for op in counter_ops {
                txn.record_counter_op(op);
            }
        }

        let (doc_cid, doc_block, col_block_data) = {
            let txn_guard = self.txn.lock().await;
            let txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction is no longer active")
            })?;

            let blockstore = txn.blockstore().map_err(|e| {
                query::error::QueryError::execution(format!("failed to get blockstore: {}", e))
            })?;
            let headstore = txn.headstore().map_err(|e| {
                query::error::QueryError::execution(format!("failed to get headstore: {}", e))
            })?;

            let schema_version_id = collection.version_id();
            let enc_config = get_encryption_config();
            let sign_config = get_signing_config();
            let kms = self.db.kms();

            let block_result = write_document_blocks(
                &blockstore,
                &headstore,
                &doc,
                schema_version_id,
                DocStorageIdentity::new(collection.resolved_root_id(), doc_short_id),
                Some(&modified_fields),
                enc_config.as_ref(),
                sign_config.as_ref(),
                kms.as_ref(),
            )
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to write document blocks for transaction update on collection {}: {}",
                    collection_name, e
                ))
            })?;

            crate::write::autocommit::helpers::register_block_doc_id_mappings(
                &systemstore,
                &block_result,
                &canonical_doc_id.to_string(),
            )
            .await?;

            let col_block_data = write_branchable_collection_block(
                &self.db,
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

        if let Some(doc_id) = doc.id().cloned() {
            self.register_update_callback(
                collection_name.to_string(),
                collection.collection_id().to_string(),
                doc_id.to_string(),
                doc_cid,
                doc_block,
                document_json_value(&doc),
                col_block_data,
            )
            .await?;
        }

        let fields_modified = modified_fields.len();
        Ok(UpdateResult::new(doc, fields_modified))
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
        self.ensure_collection_can_write(collection_name, &collection)
            .await?;

        let Some((doc_short_id, canonical_doc_id)) = collection
            .resolve_doc_identity(&systemstore, doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?
        else {
            return Ok(DeleteResult::new(doc_id.clone(), false));
        };

        let pre_delete_document_json = collection
            .get_with_datastore(&datastore, doc_short_id, &canonical_doc_id)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("pre-delete document read failed: {e}"))
            })?
            .and_then(|doc| document_json_value(&doc));

        let existed = collection
            .delete_with_indexes(&datastore, &canonical_doc_id, doc_short_id, &index_manager)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("delete error: {}", e)))?;

        if !existed {
            return Ok(DeleteResult::new(canonical_doc_id, existed));
        }

        let (doc_cid, doc_block, col_block_data) = {
            let txn_guard = self.txn.lock().await;
            let txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction is no longer active")
            })?;

            let blockstore = txn.blockstore().map_err(|e| {
                query::error::QueryError::execution(format!("failed to get blockstore: {}", e))
            })?;
            let headstore = txn.headstore().map_err(|e| {
                query::error::QueryError::execution(format!("failed to get headstore: {}", e))
            })?;

            let schema_version_id = collection.version_id();
            let sign_config = get_signing_config();

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
                    "failed to write delete block for transaction delete on collection {}: {}",
                    collection_name, e
                ))
            })?;

            crate::docid::map::set_block_doc_id_mapping(
                &systemstore,
                &block_result.cid.to_string(),
                &canonical_doc_id.to_string(),
            )
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

            let col_block_data = write_branchable_collection_block(
                &self.db,
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

        self.register_update_callback(
            collection_name.to_string(),
            collection.collection_id().to_string(),
            canonical_doc_id.to_string(),
            doc_cid,
            doc_block,
            pre_delete_document_json,
            col_block_data,
        )
        .await?;

        Ok(DeleteResult::new(canonical_doc_id, existed))
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
