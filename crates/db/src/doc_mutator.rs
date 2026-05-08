//! Document mutator for transaction-scoped mutations.

use async_lock::Mutex as TokioMutex;
use async_trait::async_trait;
use document::{DocID, Document};
use query::mutator::{CreateResult, DeleteResult, DocMutator, UpdateResult};
use std::sync::Arc;
use storage::corekv::Store;
use tracing::warn;

use crate::block_builder::{write_collection_block, write_document_blocks};
use crate::collection::Collection;
use crate::collection_loader::{get_collection_with_index_manager, get_collection_with_lazy_load};
use crate::database::DB;
use crate::txn::DbTxn;
use defra_core::encryption::{get_encryption_config, store_doc_encryption};
use defra_core::signing::get_signing_config;

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
}

impl<S: Store> DbDocMutator<S> {
    /// Create a new transaction-scoped document mutator.
    ///
    /// Collections will be loaded lazily from the transaction's cache.
    pub fn new(db: Arc<DB<S>>, txn: DbTxn<S>) -> Self {
        Self {
            db,
            txn: Arc::new(TokioMutex::new(Some(txn))),
        }
    }

    /// Create a mutator that shares a transaction with an existing component.
    ///
    /// This is used by `DbTransactionContext` to create a mutator that shares
    /// the same transaction as the `DbDocFetcher`.
    pub(crate) fn from_shared_txn(db: Arc<DB<S>>, txn: Arc<TokioMutex<Option<DbTxn<S>>>>) -> Self {
        Self { db, txn }
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocMutator for DbDocMutator<S> {
    async fn create(
        &self,
        collection_name: &str,
        mut doc: Document,
    ) -> query::error::Result<CreateResult> {
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

        // Use create_with_indexes to enforce unique constraints and maintain indexes.
        // Blind create skips existence check for content-addressed (generated) IDs.
        collection
            .create_with_indexes(&datastore, &doc, &index_manager, id_was_generated)
            .await
            .map_err(|e| crate::error::index_write_query_error("create", e))?;

        {
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

            match write_document_blocks(
                &blockstore,
                &headstore,
                &doc,
                schema_version_id,
                None,
                enc_config.as_ref(),
                sign_config.as_ref(),
            )
            .await
            {
                Ok(block_result) => {
                    if let Some(ref config) = enc_config {
                        store_doc_encryption(&doc_id.to_string(), config.clone());
                    }

                    if collection.schema().is_branchable {
                        let short_id = collection.resolved_root_id();
                        if let Err(error) = write_collection_block(
                            &blockstore,
                            &headstore,
                            short_id,
                            schema_version_id,
                            block_result.cid,
                            sign_config.as_ref(),
                        )
                        .await
                        {
                            warn!(
                                collection = %collection_name,
                                error = %error,
                                "Failed to write collection block for transaction create"
                            );
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        collection = %collection_name,
                        error = %error,
                        "Failed to write document blocks for transaction create"
                    );
                }
            }
        }

        Ok(CreateResult::new(doc_id, doc))
    }

    async fn update(
        &self,
        collection_name: &str,
        doc: Document,
        modified_fields: std::collections::HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
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

        // Use update_with_indexes to maintain index consistency
        collection
            .update_with_indexes(&datastore, &doc, &index_manager)
            .await
            .map_err(|e| match e {
                crate::error::Error::DocumentNotFound(id) => {
                    query::error::QueryError::document_not_found(id)
                }
                other => crate::error::index_write_query_error("update", other),
            })?;

        // Return count of actually modified fields
        let fields_modified = modified_fields.len();

        Ok(UpdateResult::new(doc, fields_modified))
    }

    async fn delete(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        let (collection, datastore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;
        self.ensure_collection_can_write(collection_name, &collection)
            .await?;

        // Use delete_with_indexes to maintain index consistency
        let existed = collection
            .delete_with_indexes(&datastore, doc_id, &index_manager)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("delete error: {}", e)))?;

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
