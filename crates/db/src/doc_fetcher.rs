//! Document fetcher for transaction-scoped queries.

use async_trait::async_trait;
use document::Document;
use query::runner::{DocFetcher, FetchByIdsResult};
use std::collections::HashMap;
use std::sync::Arc;
use storage::corekv::Store;
use tokio::sync::Mutex as TokioMutex;
use tracing::warn;

use crate::collection::Collection;
use crate::txn::DbTxn;

/// Document fetcher that uses a database transaction.
///
/// This fetcher holds a reference to an active transaction and collection
/// definitions, allowing it to fetch documents within the transaction context.
///
/// # Ownership Model
///
/// The transaction is wrapped in `Arc<TokioMutex<Option<...>>>` because:
/// - `Arc`: Enables the fetcher to be cloned and shared across multiple query
///   executions within the same transaction (e.g., for parallel reads)
/// - `TokioMutex`: Async-safe interior mutability for concurrent access
/// - `Option`: Enables `take_txn()` to extract the transaction for commit/rollback
///
/// After `take_txn()` is called, all fetcher operations will return an error
/// indicating the transaction was consumed. Use `is_consumed()` to check state.
pub struct DbDocFetcher<S: Store> {
    txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
    collections: Arc<HashMap<String, Collection>>,
}

impl<S: Store> DbDocFetcher<S> {
    /// Create a new transaction-scoped document fetcher.
    pub(crate) fn new(txn: DbTxn<S>, collections: Arc<HashMap<String, Collection>>) -> Self {
        Self {
            txn: Arc::new(TokioMutex::new(Some(txn))),
            collections,
        }
    }

    /// Take the transaction out of the fetcher (for commit/rollback).
    ///
    /// After calling this, `is_consumed()` will return `true` and all
    /// fetcher operations will return an error.
    pub(crate) async fn take_txn(&self) -> Option<DbTxn<S>> {
        self.txn.lock().await.take()
    }

    /// Check if the transaction has been consumed (via `take_txn()`).
    ///
    /// Returns `true` if `take_txn()` was called and the transaction is
    /// no longer available for queries.
    pub async fn is_consumed(&self) -> bool {
        self.txn.lock().await.is_none()
    }
}

#[async_trait]
impl<S: Store + 'static> DocFetcher for DbDocFetcher<S> {
    async fn get_all(&self, collection_name: &str) -> query::error::Result<Vec<Document>> {
        let collection = self
            .collections
            .get(collection_name)
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Extract the datastore while holding the lock, then release the lock
        // before awaiting. The datastore is Send + Sync so this is safe.
        let datastore = {
            let txn_guard = self.txn.lock().await;
            let db_txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction already consumed")
            })?;
            db_txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?
        };

        collection
            .get_all_with_datastore(&datastore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))
    }

    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> query::error::Result<FetchByIdsResult> {
        let collection = self
            .collections
            .get(collection_name)
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Extract the datastore while holding the lock, then release the lock
        // before awaiting. The datastore is Send + Sync so this is safe.
        let datastore = {
            let txn_guard = self.txn.lock().await;
            let db_txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction already consumed")
            })?;
            db_txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?
        };

        let mut docs = Vec::new();
        let mut missing_ids = Vec::new();

        for id_str in doc_ids {
            let doc_id = document::DocID::from_string(id_str).map_err(|e| {
                query::error::QueryError::execution(format!("invalid doc ID '{}': {}", id_str, e))
            })?;

            match collection
                .get_with_datastore(&datastore, &doc_id)
                .await
                .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?
            {
                Some(doc) => docs.push(doc),
                None => {
                    missing_ids.push(id_str.clone());
                }
            }
        }

        if !missing_ids.is_empty() {
            warn!(
                collection = %collection_name,
                requested_count = doc_ids.len(),
                found_count = docs.len(),
                missing_count = missing_ids.len(),
                missing_ids = ?missing_ids,
                "Some explicitly requested documents were not found"
            );
        }

        Ok(FetchByIdsResult::partial(docs, missing_ids))
    }
}
