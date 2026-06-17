//! Document mutator for transaction-scoped mutations.

use async_lock::Mutex as TokioMutex;
use async_trait::async_trait;
use cid::Cid;
use document::{DocID, Document};
use query::mutator::{CreateResult, DeleteResult, DocMutator, UpdateResult};
use std::sync::Arc;
use storage::corekv::Store;
use tracing::warn;

use crate::block_builder::{write_collection_block, write_delete_block, write_document_blocks};
use crate::collection::Collection;
use crate::collection_loader::{get_collection_with_index_manager, get_collection_with_lazy_load};
use crate::database::DB;
use crate::event_emission::register_update_event_callback;
use crate::txn::DbTxn;
use defra_core::encryption::{get_doc_encryption, get_encryption_config, store_doc_encryption};
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
    broadcaster: Option<Arc<dyn crate::event_emission::TxnBroadcaster>>,
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
    pub(crate) fn from_shared_txn_with_broadcaster(
        db: Arc<DB<S>>,
        txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
        broadcaster: Option<Arc<dyn crate::event_emission::TxnBroadcaster>>,
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
        doc_block: Vec<u8>,
        document_json: Option<serde_json::Value>,
        collection_block: Option<(Cid, Vec<u8>)>,
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

        let (collection, datastore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;
        self.ensure_collection_can_write(collection_name, &collection)
            .await?;

        // Generate document ID if not present.
        // Track whether ID was just generated for blind create optimization.
        let id_was_generated = doc.id().is_none();
        if id_was_generated {
            doc.generate_and_set_doc_id().map_err(|e| {
                query::error::QueryError::execution(format!("failed to generate DocID: {}", e))
            })?;
        }

        let doc_id = doc.id().cloned().ok_or_else(|| {
            query::error::QueryError::execution("document should have ID after generation")
        })?;

        self.db
            .validate_downsample_write(&datastore, collection.schema(), &doc, None)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        // #1044 record-then-finalize: write the doc blob + indexes WITHOUT seeding
        // the counter store and WITHOUT taking any per-doc guard/batch gate. The
        // counter-store seeding (and its per-doc guard) is deferred to the
        // commit-time finalize so an interactive txn holds no gate over its
        // user-controlled lifetime. See `InteractiveTxnCounter.tla`.
        crate::auto_commit_mutator::helpers::write_local_create_deferred(
            &datastore,
            &collection,
            &doc,
            &index_manager,
            id_was_generated,
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

            if let Some(ref config) = enc_config {
                store_doc_encryption(&doc_id.to_string(), config.clone());
            }

            let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
            if collection.schema().is_branchable {
                let short_id = collection.resolved_root_id();
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
                    Err(error) => {
                        warn!(
                            collection = %collection_name,
                            error = %error,
                            "Failed to write collection block for transaction create"
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
        doc: Document,
        modified_fields: std::collections::HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        self.db
            .check_node_access(None, acp::nac::NodePermission::DocumentUpdate)
            .await
            .map_err(|e| query::error::QueryError::permission_denied(e.to_string()))?;

        let (collection, datastore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;
        self.ensure_collection_can_write(collection_name, &collection)
            .await?;

        self.db
            .validate_downsample_write(
                &datastore,
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
            match doc.id() {
                Some(id) => collection
                    .get_with_datastore(&datastore, id)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
                None => None,
            }
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

        crate::auto_commit_mutator::helpers::write_local_update_deferred(
            &datastore,
            &collection,
            &doc,
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
            let enc_config = get_encryption_config()
                .or_else(|| doc.id().and_then(|id| get_doc_encryption(&id.to_string())));
            let sign_config = get_signing_config();
            let kms = self.db.kms();

            let block_result = write_document_blocks(
                &blockstore,
                &headstore,
                &doc,
                schema_version_id,
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

            let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
            if collection.schema().is_branchable {
                let short_id = collection.resolved_root_id();
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
                    Err(error) => {
                        warn!(
                            collection = %collection_name,
                            error = %error,
                            "Failed to write collection block for transaction update"
                        );
                    }
                }
            }

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

        let (collection, datastore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;
        self.ensure_collection_can_write(collection_name, &collection)
            .await?;
        let pre_delete_document_json = collection
            .get_with_datastore(&datastore, doc_id)
            .await
            .ok()
            .flatten()
            .and_then(|doc| document_json_value(&doc));

        let existed = collection
            .delete_with_indexes(&datastore, doc_id, &index_manager)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("delete error: {}", e)))?;

        if !existed {
            return Ok(DeleteResult::new(doc_id.clone(), existed));
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
                &doc_id.to_string(),
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

            let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
            if collection.schema().is_branchable {
                let short_id = collection.resolved_root_id();
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
                    Err(error) => {
                        warn!(
                            collection = %collection_name,
                            error = %error,
                            "Failed to write collection block for transaction delete"
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
            doc_block,
            pre_delete_document_json,
            col_block_data,
        )
        .await?;

        Ok(DeleteResult::new(doc_id.clone(), existed))
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
    use query::mutator::DocMutator;
    use schema::{CType, CollectionVersion, FieldDescription, FieldKind};
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

    fn counter_collection() -> CollectionVersion {
        CollectionVersion::new(
            "Counters",
            "cv1",
            "col-counters",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "count", FieldKind::int())
                    .with_crdt_type(CType::PnCounter),
            ],
        )
    }

    /// Read the PNCounter accumulation store value for a doc/field from the
    /// committed store (a fresh read txn), proving the authoritative store — not
    /// just the materialized blob — advanced.
    async fn read_counter_store(
        db: &Arc<DB<MemoryStore>>,
        schema_version_id: &str,
        doc_id: &str,
        field: &str,
    ) -> i64 {
        use crdt::traits::ValueReader;
        use crdt::{Counter, NumericKind};

        let txn = db.new_txn(true).await.expect("read txn");
        let datastore = txn.datastore().expect("datastore");
        let counter = Counter::new(
            schema_version_id.to_string(),
            doc_id.as_bytes(),
            field.to_string(),
            true,
            NumericKind::Int64,
        )
        .expect("counter");
        let bytes = ValueReader::value(&counter, &datastore)
            .await
            .expect("counter value");
        assert_eq!(bytes.len(), 8, "int64 counter store value is 8 bytes");
        i64::from_be_bytes(bytes.try_into().unwrap())
    }

    #[tokio::test]
    async fn explicit_txn_counter_increment_advances_accumulation_store() {
        // Regression for #1021 (now #1044 record-then-finalize): an
        // explicit-transaction counter increment must read-modify-write the
        // authoritative CRDT accumulation store (not only the materialized blob).
        // The RMW is now deferred to the registry commit-time finalize, so the
        // test drives commit through the registry (not a bare force_commit, which
        // would skip the finalize) — keeping the original intent: after commit the
        // authoritative store reflects the increment.
        use crate::txn_registry::DbTransactionRegistry;
        use query::txn::TransactionRegistry;

        let (db, _bus) = make_test_db_with_bus().await;
        db.create_collection(counter_collection())
            .await
            .expect("schema");
        let registry = DbTransactionRegistry::new(Arc::clone(&db));

        // Create the doc (count = 5) in an explicit txn, commit via the registry.
        let handle = registry.begin(false).await.expect("begin");
        let ctx = registry.get(&handle).into_result().unwrap().unwrap();
        let mutator = ctx.doc_mutator().expect("mutator");
        let create_doc = Document::from_json_str(r#"{"count": 5}"#).expect("doc");
        let created = mutator
            .create("Counters", create_doc)
            .await
            .expect("create");
        let doc_id = created.doc_id.to_string();
        drop(mutator);
        drop(ctx);
        registry.commit(&handle).await.expect("commit");

        assert_eq!(
            read_counter_store(&db, "cv1", &doc_id, "count").await,
            5,
            "create must seed the accumulation store at finalize"
        );

        // Increment by 3 in a fresh explicit txn, commit via the registry.
        let handle = registry.begin(false).await.expect("begin");
        let ctx = registry.get(&handle).into_result().unwrap().unwrap();
        let mutator = ctx.doc_mutator().expect("mutator");
        let mut update_doc = Document::from_json_str(r#"{"count": 8}"#).expect("doc");
        update_doc.set_id(document::DocID::from_string(&doc_id).expect("doc id"));
        update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(3));
        let mut modified = std::collections::HashSet::new();
        modified.insert("count".to_string());
        mutator
            .update("Counters", update_doc, modified)
            .await
            .expect("update");
        drop(mutator);
        drop(ctx);
        registry.commit(&handle).await.expect("commit");

        assert_eq!(
            read_counter_store(&db, "cv1", &doc_id, "count").await,
            8,
            "explicit-txn increment must advance the accumulation store at finalize (#1044)"
        );
    }

    /// PCounter (increment-only) collection: reconcile MIGRATES a present store
    /// upward via max, so the finalize must NOT re-read the provisional blob as the
    /// reconcile base (that would double-apply the delta). See #1044 BUG 1.
    fn pcounter_collection() -> CollectionVersion {
        CollectionVersion::new(
            "PCounters",
            "pcv1",
            "col-pcounters",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "count", FieldKind::int())
                    .with_crdt_type(CType::PCounter),
            ],
        )
    }

    /// Regression for #1044 BUG 1 (PCounter explicit-txn double-apply): create a
    /// PCounter doc at 5, then increment +3 in a separate explicit txn. The
    /// authoritative store must end at 8, NOT 11. Before the fix the finalize
    /// re-read the provisional blob (8) as the reconcile base, migrated the present
    /// store (5) UPWARD to 8 via PCounter max, then applied +3 → 11. Capturing the
    /// pre-write committed value (5) as the reconcile base fixes it.
    #[tokio::test]
    async fn explicit_txn_pcounter_increment_no_double_apply() {
        use crate::txn_registry::DbTransactionRegistry;
        use query::txn::TransactionRegistry;

        let (db, _bus) = make_test_db_with_bus().await;
        db.create_collection(pcounter_collection())
            .await
            .expect("schema");
        let registry = DbTransactionRegistry::new(Arc::clone(&db));

        // Create the doc (count = 5) in an explicit txn, commit via the registry.
        let handle = registry.begin(false).await.expect("begin");
        let ctx = registry.get(&handle).into_result().unwrap().unwrap();
        let mutator = ctx.doc_mutator().expect("mutator");
        let create_doc = Document::from_json_str(r#"{"count": 5}"#).expect("doc");
        let created = mutator
            .create("PCounters", create_doc)
            .await
            .expect("create");
        let doc_id = created.doc_id.to_string();
        drop(mutator);
        drop(ctx);
        registry.commit(&handle).await.expect("commit");

        assert_eq!(
            read_counter_store(&db, "pcv1", &doc_id, "count").await,
            5,
            "create must seed the PCounter accumulation store at 5"
        );

        // Increment by 3 in a fresh explicit txn (provisional blob = 8).
        let handle = registry.begin(false).await.expect("begin");
        let ctx = registry.get(&handle).into_result().unwrap().unwrap();
        let mutator = ctx.doc_mutator().expect("mutator");
        let mut update_doc = Document::from_json_str(r#"{"count": 8}"#).expect("doc");
        update_doc.set_id(document::DocID::from_string(&doc_id).expect("doc id"));
        update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(3));
        let mut modified = std::collections::HashSet::new();
        modified.insert("count".to_string());
        mutator
            .update("PCounters", update_doc, modified)
            .await
            .expect("update");
        drop(mutator);
        drop(ctx);
        registry.commit(&handle).await.expect("commit");

        assert_eq!(
            read_counter_store(&db, "pcv1", &doc_id, "count").await,
            8,
            "PCounter increment must NOT double-apply: store == 8 (not 11) (#1044)"
        );
    }

    /// Multi-doc finalize: an explicit txn that increments counters on TWO
    /// different docs must, after commit, leave BOTH accumulation stores
    /// advanced — exercising the sorted multi-doc acquire in the finalize driver.
    #[tokio::test]
    async fn explicit_txn_multi_doc_counter_finalize_advances_both_stores() {
        use crate::txn_registry::DbTransactionRegistry;
        use query::txn::TransactionRegistry;

        let (db, _bus) = make_test_db_with_bus().await;
        db.create_collection(counter_collection())
            .await
            .expect("schema");
        let registry = DbTransactionRegistry::new(Arc::clone(&db));

        // Seed two docs (count = 10 and count = 20) in one explicit txn.
        let handle = registry.begin(false).await.expect("begin");
        let ctx = registry.get(&handle).into_result().unwrap().unwrap();
        let mutator = ctx.doc_mutator().expect("mutator");
        let doc_a = mutator
            .create(
                "Counters",
                Document::from_json_str(r#"{"count": 10}"#).unwrap(),
            )
            .await
            .expect("create a")
            .doc_id
            .to_string();
        let doc_b = mutator
            .create(
                "Counters",
                Document::from_json_str(r#"{"count": 20}"#).unwrap(),
            )
            .await
            .expect("create b")
            .doc_id
            .to_string();
        drop(mutator);
        drop(ctx);
        registry.commit(&handle).await.expect("commit creates");

        // Increment BOTH docs in a single explicit txn (multi-doc finalize).
        let handle = registry.begin(false).await.expect("begin");
        let ctx = registry.get(&handle).into_result().unwrap().unwrap();
        let mutator = ctx.doc_mutator().expect("mutator");

        let mut up_a = Document::from_json_str(r#"{"count": 11}"#).unwrap();
        up_a.set_id(document::DocID::from_string(&doc_a).unwrap());
        up_a.set_counter_delta("count".to_string(), document::NormalValue::Int(1));
        let mut mod_a = std::collections::HashSet::new();
        mod_a.insert("count".to_string());
        mutator
            .update("Counters", up_a, mod_a)
            .await
            .expect("update a");

        let mut up_b = Document::from_json_str(r#"{"count": 25}"#).unwrap();
        up_b.set_id(document::DocID::from_string(&doc_b).unwrap());
        up_b.set_counter_delta("count".to_string(), document::NormalValue::Int(5));
        let mut mod_b = std::collections::HashSet::new();
        mod_b.insert("count".to_string());
        mutator
            .update("Counters", up_b, mod_b)
            .await
            .expect("update b");

        drop(mutator);
        drop(ctx);
        registry.commit(&handle).await.expect("commit updates");

        assert_eq!(
            read_counter_store(&db, "cv1", &doc_a, "count").await,
            11,
            "doc A store must reflect +1"
        );
        assert_eq!(
            read_counter_store(&db, "cv1", &doc_b, "count").await,
            25,
            "doc B store must reflect +5"
        );
    }

    /// Read the counter accumulation store value, returning `None` when the store
    /// key is absent (the counter was never durably finalized). Used by the
    /// discard test to prove a rolled-back interactive txn ran no RMW.
    async fn read_counter_store_opt(
        db: &Arc<DB<MemoryStore>>,
        schema_version_id: &str,
        doc_id: &str,
        field: &str,
    ) -> Option<i64> {
        use crdt::traits::ValueReader;
        use crdt::{Counter, NumericKind};

        let txn = db.new_txn(true).await.expect("read txn");
        let datastore = txn.datastore().expect("datastore");
        let counter = Counter::new(
            schema_version_id.to_string(),
            doc_id.as_bytes(),
            field.to_string(),
            true,
            NumericKind::Int64,
        )
        .expect("counter");
        match ValueReader::value(&counter, &datastore).await {
            Ok(bytes) => {
                assert_eq!(bytes.len(), 8, "int64 counter store value is 8 bytes");
                Some(i64::from_be_bytes(bytes.try_into().unwrap()))
            }
            Err(_) => None,
        }
    }

    /// Read a doc from the committed store via a fresh read txn; `None` if absent.
    async fn read_committed_doc(
        db: &Arc<DB<MemoryStore>>,
        collection_name: &str,
        doc_id: &str,
    ) -> Option<Document> {
        let collection = db
            .get_collection(collection_name)
            .expect("get collection")
            .expect("collection exists");
        let txn = db.new_txn(true).await.expect("read txn");
        let datastore = txn.datastore().expect("datastore");
        let doc_id_typed = document::DocID::from_string(doc_id).expect("doc id");
        collection
            .get_with_datastore(&datastore, &doc_id_typed)
            .await
            .expect("get doc")
    }

    /// PCounter create-then-update in ONE registry txn: create at 5 then +3 in the
    /// SAME txn, commit. The update's `committed_pre_write` read sees the
    /// same-txn-staged create (5), so the recorded base is 5 and the finalize ends
    /// at exactly 8 (NOT 11 from a double-apply, NOT 3 from a missing base seed).
    #[tokio::test]
    async fn explicit_txn_pcounter_create_then_update_same_txn() {
        use crate::txn_registry::DbTransactionRegistry;
        use query::txn::TransactionRegistry;

        let (db, _bus) = make_test_db_with_bus().await;
        db.create_collection(pcounter_collection())
            .await
            .expect("schema");
        let registry = DbTransactionRegistry::new(Arc::clone(&db));

        let handle = registry.begin(false).await.expect("begin");
        let ctx = registry.get(&handle).into_result().unwrap().unwrap();
        let mutator = ctx.doc_mutator().expect("mutator");

        let created = mutator
            .create(
                "PCounters",
                Document::from_json_str(r#"{"count": 5}"#).unwrap(),
            )
            .await
            .expect("create");
        let doc_id = created.doc_id.to_string();

        let mut update_doc = Document::from_json_str(r#"{"count": 8}"#).unwrap();
        update_doc.set_id(document::DocID::from_string(&doc_id).unwrap());
        update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(3));
        let mut modified = std::collections::HashSet::new();
        modified.insert("count".to_string());
        mutator
            .update("PCounters", update_doc, modified)
            .await
            .expect("update");

        drop(mutator);
        drop(ctx);
        registry.commit(&handle).await.expect("commit");

        assert_eq!(
            read_counter_store(&db, "pcv1", &doc_id, "count").await,
            8,
            "PCounter create(5)+update(+3) in one txn must finalize to exactly 8"
        );
    }

    /// PNCounter create-then-update in ONE registry txn: +3 then -5 in one txn
    /// (create at 3, then decrement by 5). The signed accumulation store result is
    /// -2 (3 + (-5)), proving the same-txn decrement path stages and finalizes the
    /// signed delta against the same-txn-staged base.
    #[tokio::test]
    async fn explicit_txn_pncounter_create_then_decrement_same_txn() {
        use crate::txn_registry::DbTransactionRegistry;
        use query::txn::TransactionRegistry;

        let (db, _bus) = make_test_db_with_bus().await;
        db.create_collection(counter_collection())
            .await
            .expect("schema");
        let registry = DbTransactionRegistry::new(Arc::clone(&db));

        let handle = registry.begin(false).await.expect("begin");
        let ctx = registry.get(&handle).into_result().unwrap().unwrap();
        let mutator = ctx.doc_mutator().expect("mutator");

        let created = mutator
            .create(
                "Counters",
                Document::from_json_str(r#"{"count": 3}"#).unwrap(),
            )
            .await
            .expect("create");
        let doc_id = created.doc_id.to_string();

        let mut update_doc = Document::from_json_str(r#"{"count": -2}"#).unwrap();
        update_doc.set_id(document::DocID::from_string(&doc_id).unwrap());
        update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(-5));
        let mut modified = std::collections::HashSet::new();
        modified.insert("count".to_string());
        mutator
            .update("Counters", update_doc, modified)
            .await
            .expect("update");

        drop(mutator);
        drop(ctx);
        registry.commit(&handle).await.expect("commit");

        assert_eq!(
            read_counter_store(&db, "cv1", &doc_id, "count").await,
            -2,
            "PNCounter create(+3)+decrement(-5) in one txn must finalize to exactly -2"
        );
    }

    /// Discard with pending counter ops: create a counter doc and increment it in
    /// one registry txn, then roll back. The accumulation store must have NO value
    /// and the doc must be absent — proving discard drops the pending ops and never
    /// ran the finalize RMW.
    #[tokio::test]
    async fn explicit_txn_discard_drops_pending_counter_ops() {
        use crate::txn_registry::DbTransactionRegistry;
        use query::txn::TransactionRegistry;

        let (db, _bus) = make_test_db_with_bus().await;
        db.create_collection(counter_collection())
            .await
            .expect("schema");
        let registry = DbTransactionRegistry::new(Arc::clone(&db));

        let handle = registry.begin(false).await.expect("begin");
        let ctx = registry.get(&handle).into_result().unwrap().unwrap();
        let mutator = ctx.doc_mutator().expect("mutator");

        let created = mutator
            .create(
                "Counters",
                Document::from_json_str(r#"{"count": 7}"#).unwrap(),
            )
            .await
            .expect("create");
        let doc_id = created.doc_id.to_string();

        let mut update_doc = Document::from_json_str(r#"{"count": 9}"#).unwrap();
        update_doc.set_id(document::DocID::from_string(&doc_id).unwrap());
        update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(2));
        let mut modified = std::collections::HashSet::new();
        modified.insert("count".to_string());
        mutator
            .update("Counters", update_doc, modified)
            .await
            .expect("update");

        drop(mutator);
        drop(ctx);
        registry.rollback(&handle).await.expect("rollback");

        assert_eq!(
            read_counter_store_opt(&db, "cv1", &doc_id, "count").await,
            None,
            "discard must leave NO accumulation store value (finalize RMW never ran)"
        );
        assert!(
            read_committed_doc(&db, "Counters", &doc_id).await.is_none(),
            "discard must leave the doc absent in the committed store"
        );
    }

    /// Multiple updates to the SAME counter field in ONE registry txn: create at 0,
    /// then +3 then +2 in the same txn. The two recorded delta ops both finalize
    /// against the SAME doc/field, summing to exactly 5 (each delta applied once).
    #[tokio::test]
    async fn explicit_txn_multiple_updates_same_field_sum_once() {
        use crate::txn_registry::DbTransactionRegistry;
        use query::txn::TransactionRegistry;

        let (db, _bus) = make_test_db_with_bus().await;
        db.create_collection(counter_collection())
            .await
            .expect("schema");
        let registry = DbTransactionRegistry::new(Arc::clone(&db));

        // Seed a doc at 0 in its own committed txn.
        let handle = registry.begin(false).await.expect("begin");
        let ctx = registry.get(&handle).into_result().unwrap().unwrap();
        let mutator = ctx.doc_mutator().expect("mutator");
        let created = mutator
            .create(
                "Counters",
                Document::from_json_str(r#"{"count": 0}"#).unwrap(),
            )
            .await
            .expect("create");
        let doc_id = created.doc_id.to_string();
        drop(mutator);
        drop(ctx);
        registry.commit(&handle).await.expect("commit create");

        // Two updates to the same field in ONE txn: +3 then +2.
        let handle = registry.begin(false).await.expect("begin");
        let ctx = registry.get(&handle).into_result().unwrap().unwrap();
        let mutator = ctx.doc_mutator().expect("mutator");

        let mut up1 = Document::from_json_str(r#"{"count": 3}"#).unwrap();
        up1.set_id(document::DocID::from_string(&doc_id).unwrap());
        up1.set_counter_delta("count".to_string(), document::NormalValue::Int(3));
        let mut m1 = std::collections::HashSet::new();
        m1.insert("count".to_string());
        mutator.update("Counters", up1, m1).await.expect("update 1");

        let mut up2 = Document::from_json_str(r#"{"count": 5}"#).unwrap();
        up2.set_id(document::DocID::from_string(&doc_id).unwrap());
        up2.set_counter_delta("count".to_string(), document::NormalValue::Int(2));
        let mut m2 = std::collections::HashSet::new();
        m2.insert("count".to_string());
        mutator.update("Counters", up2, m2).await.expect("update 2");

        drop(mutator);
        drop(ctx);
        registry.commit(&handle).await.expect("commit updates");

        assert_eq!(
            read_counter_store(&db, "cv1", &doc_id, "count").await,
            5,
            "two same-field updates (+3,+2) in one txn must sum to exactly 5"
        );
    }

    /// Counter collection with an @index on the counter field, exercising the
    /// finalize blob-correction's `update_with_indexes` index maintenance.
    fn indexed_counter_collection() -> CollectionVersion {
        let mut col = CollectionVersion::new(
            "IdxCounters",
            "icv1",
            "col-idx-counters",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "count", FieldKind::int())
                    .with_crdt_type(CType::PnCounter),
            ],
        );
        col.indexes = vec![schema::IndexDescription::new("idx_count").with_field("count", false)];
        col
    }

    /// Indexed counter: increment in an interactive txn, commit, then assert the
    /// index entry materialized at the AUTHORITATIVE post-RMW value (8), proving
    /// the finalize blob-correction maintained the index. The unit-test layer has
    /// no GraphQL filter-query executor, so this asserts the index entry directly
    /// (the value a filter query would resolve against).
    #[tokio::test]
    async fn explicit_txn_indexed_counter_index_reflects_post_rmw_value() {
        use crate::index_manager::IndexManager;
        use crate::txn_registry::DbTransactionRegistry;
        use query::txn::TransactionRegistry;
        use storage::index::IndexIterator;

        let (db, _bus) = make_test_db_with_bus().await;
        let col_version = indexed_counter_collection();
        db.create_collection(col_version).await.expect("schema");
        let registry = DbTransactionRegistry::new(Arc::clone(&db));

        // Create at 5, commit.
        let handle = registry.begin(false).await.expect("begin");
        let ctx = registry.get(&handle).into_result().unwrap().unwrap();
        let mutator = ctx.doc_mutator().expect("mutator");
        let created = mutator
            .create(
                "IdxCounters",
                Document::from_json_str(r#"{"count": 5}"#).unwrap(),
            )
            .await
            .expect("create");
        let doc_id = created.doc_id.to_string();
        drop(mutator);
        drop(ctx);
        registry.commit(&handle).await.expect("commit create");

        // Increment +3 in an interactive txn, commit.
        let handle = registry.begin(false).await.expect("begin");
        let ctx = registry.get(&handle).into_result().unwrap().unwrap();
        let mutator = ctx.doc_mutator().expect("mutator");
        let mut update_doc = Document::from_json_str(r#"{"count": 8}"#).unwrap();
        update_doc.set_id(document::DocID::from_string(&doc_id).unwrap());
        update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(3));
        let mut modified = std::collections::HashSet::new();
        modified.insert("count".to_string());
        mutator
            .update("IdxCounters", update_doc, modified)
            .await
            .expect("update");
        drop(mutator);
        drop(ctx);
        registry.commit(&handle).await.expect("commit update");

        // Authoritative store advanced to 8.
        assert_eq!(
            read_counter_store(&db, "icv1", &doc_id, "count").await,
            8,
            "indexed counter store must reflect +3 → 8"
        );

        // The index entry must exist at the post-RMW value 8 (what a filter query
        // `count: {_eq: 8}` would resolve), and must NOT exist at the stale 5.
        let collection = db
            .get_collection("IdxCounters")
            .expect("get collection")
            .expect("collection exists");
        let manager =
            IndexManager::from_collection(collection.resolved_root_id(), collection.schema())
                .expect("index manager");
        let index = manager.get_index("idx_count").expect("idx_count present");

        let txn = db.new_txn(true).await.expect("read txn");
        let datastore = txn.datastore().expect("datastore");

        let mut iter_8 = index
            .get(&datastore, &[document::NormalValue::Int(8)])
            .await
            .expect("index get 8");
        let entries_8 = iter_8.collect_all().await.expect("collect 8");
        assert_eq!(
            entries_8.len(),
            1,
            "index must have exactly one entry at the post-RMW value 8"
        );

        let mut iter_5 = index
            .get(&datastore, &[document::NormalValue::Int(5)])
            .await
            .expect("index get 5");
        let entries_5 = iter_5.collect_all().await.expect("collect 5");
        assert!(
            entries_5.is_empty(),
            "index must NOT have a stale entry at the pre-update value 5"
        );
    }

    // finalize-error-rollback and concurrent-finalize-vs-merge are intentionally
    // NOT tested at this unit layer: there is no fault-injection seam to force a
    // finalize error mid-RMW and no deterministic interleave seam for a concurrent
    // merge. The error path is covered by the whole-txn discard semantics
    // (`explicit_txn_discard_drops_pending_counter_ops` proves a non-committed txn
    // applies no RMW), and the concurrent-finalize-vs-merge guard lifecycle is
    // covered by `proofs/tla/InteractiveTxnCounter.tla`.

    #[tokio::test]
    async fn create_in_tx_publishes_event_on_commit() {
        let (db, bus) = make_test_db_with_bus().await;
        db.create_collection(test_collection())
            .await
            .expect("schema");

        let mut sub = bus.subscribe(&[EventName::Update]);

        let txn = db.new_txn(false).await.expect("new_txn");
        let mutator = DbDocMutator::new(Arc::clone(&db), txn);

        let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
        let result = mutator.create("TestDoc", doc).await.expect("create");

        // Before commit: no event should have fired
        assert!(
            sub.try_recv().is_err(),
            "no event should fire before commit"
        );

        let txn = mutator.take_txn().await.expect("take txn");
        txn.commit().await.expect("commit");

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
    async fn create_in_tx_publishes_no_event_on_discard() {
        let (db, bus) = make_test_db_with_bus().await;
        db.create_collection(test_collection())
            .await
            .expect("schema");

        let mut sub = bus.subscribe(&[EventName::Update]);

        let txn = db.new_txn(false).await.expect("new_txn");
        let mutator = DbDocMutator::new(Arc::clone(&db), txn);

        let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
        mutator.create("TestDoc", doc).await.expect("create");

        let txn = mutator.take_txn().await.expect("take txn");
        txn.discard().expect("discard");

        // Allow a brief window for any (unexpected) async delivery
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(sub.try_recv().is_err(), "discard should not publish events");
    }

    #[tokio::test]
    async fn delete_missing_doc_publishes_no_event_and_writes_no_block() {
        // DeleteNode treats existed==false as a no-op; the mutator must not
        // create a tombstone commit or fire an Update event for a missing doc.
        let (db, bus) = make_test_db_with_bus().await;
        db.create_collection(test_collection())
            .await
            .expect("schema");

        let mut sub = bus.subscribe(&[EventName::Update]);

        let txn = db.new_txn(false).await.expect("new_txn");
        let mutator = DbDocMutator::new(Arc::clone(&db), txn);

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

        let txn = mutator.take_txn().await.expect("take txn");
        txn.commit().await.expect("commit");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            sub.try_recv().is_err(),
            "deleting a non-existent doc should not publish an Update event"
        );
    }

    /// `TxnBroadcaster` test double: captures every event it's asked to
    /// broadcast for inspection.
    struct CapturingBroadcaster {
        events: Arc<std::sync::Mutex<Vec<crate::event_emission::TxnBroadcastEvent>>>,
    }

    #[async_trait::async_trait]
    impl crate::event_emission::TxnBroadcaster for CapturingBroadcaster {
        async fn broadcast_update(&self, event: crate::event_emission::TxnBroadcastEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    #[tokio::test]
    async fn create_in_tx_forwards_to_broadcaster_on_commit() {
        // F1 regression: a tx with a TxnBroadcaster wired in must invoke
        // broadcast_update for each committed mutation so P2P peers see
        // transactional writes (Go: db.sendUpdate → p2p.SendUpdate).
        let (db, _bus) = make_test_db_with_bus().await;
        db.create_collection(test_collection())
            .await
            .expect("schema");

        let captured: Arc<std::sync::Mutex<Vec<crate::event_emission::TxnBroadcastEvent>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let broadcaster: Arc<dyn crate::event_emission::TxnBroadcaster> =
            Arc::new(CapturingBroadcaster {
                events: Arc::clone(&captured),
            });

        let txn = db.new_txn(false).await.expect("new_txn");
        let txn_arc = Arc::new(TokioMutex::new(Some(txn)));
        let mutator = DbDocMutator::from_shared_txn_with_broadcaster(
            Arc::clone(&db),
            txn_arc,
            Some(broadcaster),
        );

        let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
        let result = mutator.create("TestDoc", doc).await.expect("create");

        // Broadcaster must NOT see anything before commit
        assert!(
            captured.lock().unwrap().is_empty(),
            "no broadcast before commit"
        );

        let txn = mutator.take_txn().await.expect("take txn");
        txn.commit().await.expect("commit");

        // Wait briefly for the on_success_async callback to fire
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let events = captured.lock().unwrap().clone();
        assert_eq!(events.len(), 1, "exactly one broadcast after commit");
        let event = &events[0];
        assert_eq!(event.doc_id, result.doc_id.to_string());
        assert_eq!(event.collection_name, "TestDoc");
        assert_ne!(event.doc_cid, cid::Cid::default(), "doc_cid populated");
        assert!(!event.doc_block.is_empty(), "doc_block populated");
        assert_eq!(
            event.document_json.as_ref().and_then(|json| json.get("x")),
            Some(&serde_json::json!(1)),
            "document_json populated for filtered transaction replication"
        );
    }

    #[tokio::test]
    async fn create_in_tx_does_not_broadcast_on_discard() {
        let (db, _bus) = make_test_db_with_bus().await;
        db.create_collection(test_collection())
            .await
            .expect("schema");

        let captured: Arc<std::sync::Mutex<Vec<crate::event_emission::TxnBroadcastEvent>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let broadcaster: Arc<dyn crate::event_emission::TxnBroadcaster> =
            Arc::new(CapturingBroadcaster {
                events: Arc::clone(&captured),
            });

        let txn = db.new_txn(false).await.expect("new_txn");
        let txn_arc = Arc::new(TokioMutex::new(Some(txn)));
        let mutator = DbDocMutator::from_shared_txn_with_broadcaster(
            Arc::clone(&db),
            txn_arc,
            Some(broadcaster),
        );

        let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
        mutator.create("TestDoc", doc).await.expect("create");

        let txn = mutator.take_txn().await.expect("take txn");
        txn.discard().expect("discard");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            captured.lock().unwrap().is_empty(),
            "discard should not trigger broadcast"
        );
    }
}
