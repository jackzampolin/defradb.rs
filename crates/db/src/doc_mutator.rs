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

    /// Serialize this explicit-txn write to `doc_id` against concurrent
    /// merges/writes on the same document, holding the guard until the txn
    /// commits/rolls back (#1021).
    ///
    /// The guards live on the `DbTxn` (not on this per-call mutator, which is
    /// recreated and dropped before commit), so they release only after the
    /// durable commit. Like `BatchMutator`, an explicit txn discovers its
    /// documents incrementally, so the first guard also takes the shared batch
    /// gate (held for the whole txn) to stay deadlock-free against other
    /// multi-doc acquirers (batch merges, `create_many`).
    ///
    /// The gate and per-doc guards are acquired WITHOUT holding the txn lock so
    /// acquisition (which may block on other holders) cannot deadlock against a
    /// merge that needs the txn's stores; only the brief insert into the txn is
    /// done under the lock.
    async fn ensure_doc_guard(&self, doc_id: &str) -> query::error::Result<()> {
        let (already_held, need_gate) = {
            let txn_guard = self.txn.lock().await;
            let txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction is no longer active")
            })?;
            (txn.holds_doc_guard(doc_id), !txn.holds_batch_gate())
        };
        if already_held {
            return Ok(());
        }

        if need_gate {
            let gate = self.db.doc_write_queue().acquire_batch_gate().await;
            let mut txn_guard = self.txn.lock().await;
            let txn = txn_guard.as_mut().ok_or_else(|| {
                query::error::QueryError::execution("transaction is no longer active")
            })?;
            txn.set_batch_gate(gate);
        }

        let guard = self.db.doc_write_queue().acquire(doc_id).await;
        let mut txn_guard = self.txn.lock().await;
        let txn = txn_guard.as_mut().ok_or_else(|| {
            query::error::QueryError::execution("transaction is no longer active")
        })?;
        txn.insert_doc_guard(doc_id.to_string(), guard);
        Ok(())
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

        // Serialize this create's counter-store seeding against concurrent
        // merges/writes on the same document, held until the txn commits (#1021).
        // Only the counter CRDT needs this RMW serialization, so skip the guard —
        // and thus the process-wide batch gate — for collections with no counter
        // field. Otherwise an ordinary interactive transaction would hold the gate
        // for its (user-controlled) lifetime and stall all inbound batch merges.
        if collection
            .schema()
            .fields
            .iter()
            .any(|f| f.crdt_type.is_counter())
        {
            self.ensure_doc_guard(&doc_id.to_string()).await?;
        }

        self.db
            .validate_downsample_write(&datastore, collection.schema(), &doc, None)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        // Bundle the doc blob + index write with the counter-store seeding so the
        // authoritative CRDT accumulation store is always seeded on create (#1021
        // single-store invariant). Blind create skips the existence check for
        // content-addressed (generated) IDs. Mirrors the auto-commit/batch paths.
        crate::auto_commit_mutator::helpers::write_local_create(
            &datastore,
            &collection,
            &doc,
            &index_manager,
            id_was_generated,
        )
        .await?;

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
        mut doc: Document,
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

        // Serialize this update's counter read-modify-write against concurrent
        // merges/writes on the same document, held until the txn commits. Without
        // it a concurrent P2P counter merge can interleave its RMW and drop this
        // increment (#1021). Take the guard — and thus the process-wide batch gate
        // — ONLY when this update actually carries a counter increment, so a
        // non-counter interactive transaction never holds the gate for its
        // (user-controlled) lifetime and stalls inbound batch merges.
        let touches_counter = collection
            .schema()
            .fields
            .iter()
            .any(|f| f.crdt_type.is_counter() && doc.get_counter_delta(&f.name).is_some());
        if touches_counter {
            if let Some(id) = doc.id().map(|id| id.to_string()) {
                self.ensure_doc_guard(&id).await?;
            }
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

        // Bundle the counter RMW (#1021) with the doc blob + index write so the
        // authoritative CRDT accumulation store always advances before the blob is
        // persisted — enforced by construction in `write_local_update`.
        crate::auto_commit_mutator::helpers::write_local_update(
            &datastore,
            &collection,
            &mut doc,
            &index_manager,
        )
        .await?;

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
        // Regression for #1021: an explicit-transaction counter increment must
        // read-modify-write the authoritative CRDT accumulation store (not only
        // the materialized blob). If the store stayed stale, a later merge would
        // re-materialize from it and silently drop the increment.
        let (db, _bus) = make_test_db_with_bus().await;
        db.create_collection(counter_collection())
            .await
            .expect("schema");

        // Create the doc (count = 5) in an explicit txn.
        let txn = db.new_txn(false).await.expect("new_txn");
        let mutator = DbDocMutator::new(Arc::clone(&db), txn);
        let create_doc = Document::from_json_str(r#"{"count": 5}"#).expect("doc");
        let created = mutator
            .create("Counters", create_doc)
            .await
            .expect("create");
        let doc_id = created.doc_id.to_string();
        let txn = mutator.take_txn().await.expect("take txn");
        txn.force_commit().await.expect("commit");

        assert_eq!(
            read_counter_store(&db, "cv1", &doc_id, "count").await,
            5,
            "create must seed the accumulation store"
        );

        // Increment by 3 in a fresh explicit txn.
        let txn = db.new_txn(false).await.expect("new_txn");
        let mutator = DbDocMutator::new(Arc::clone(&db), txn);
        let mut update_doc = Document::from_json_str(r#"{"count": 8}"#).expect("doc");
        update_doc.set_id(document::DocID::from_string(&doc_id).expect("doc id"));
        update_doc.set_counter_delta("count".to_string(), document::NormalValue::Int(3));
        let mut modified = std::collections::HashSet::new();
        modified.insert("count".to_string());
        mutator
            .update("Counters", update_doc, modified)
            .await
            .expect("update");
        let txn = mutator.take_txn().await.expect("take txn");
        txn.force_commit().await.expect("commit");

        assert_eq!(
            read_counter_store(&db, "cv1", &doc_id, "count").await,
            8,
            "explicit-txn increment must advance the accumulation store (#1021)"
        );
    }

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
