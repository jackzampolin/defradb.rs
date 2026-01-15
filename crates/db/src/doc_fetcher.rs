//! Document fetcher for transaction-scoped queries.

use async_trait::async_trait;
use datastore::NamespaceView;
use document::Document;
use query::runner::{DocFetcher, FetchByIdsResult};
use schema::CollectionVersion;
use std::sync::Arc;
use storage::corekv::{Key, Store};
use storage::keys::systemstore::CollectionNameKey;
use tokio::sync::Mutex as TokioMutex;
use tracing::warn;

use crate::collection::Collection;
use crate::txn::DbTxn;

/// Load a collection from the systemstore by name.
///
/// This is a standalone async function that doesn't hold any locks,
/// allowing it to be called outside the mutex lock scope.
async fn load_collection_from_systemstore(
    systemstore: &NamespaceView,
    name: &str,
) -> query::error::Result<Option<Collection>> {
    let key = CollectionNameKey::new(name);

    match systemstore
        .get(&key.bytes())
        .await
        .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?
    {
        Some(data) => {
            let schema: CollectionVersion = serde_json::from_slice(&data).map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to deserialize schema for collection '{}': {}",
                    name, e
                ))
            })?;
            Ok(Some(Collection::new(schema)))
        }
        None => Ok(None),
    }
}

/// Document fetcher that uses a database transaction.
///
/// This fetcher holds a reference to an active transaction and uses the
/// transaction's collection cache with lazy loading for snapshot isolation.
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
///
/// # Collection Access
///
/// Collections are loaded lazily from the SystemStore on first access within
/// the transaction. This provides snapshot isolation - the transaction sees
/// collections as they existed when first accessed, not at transaction start.
pub struct DbDocFetcher<S: Store> {
    txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
}

impl<S: Store> DbDocFetcher<S> {
    /// Create a new transaction-scoped document fetcher.
    ///
    /// Collections will be loaded lazily from the transaction's cache.
    pub(crate) fn new(txn: DbTxn<S>) -> Self {
        Self {
            txn: Arc::new(TokioMutex::new(Some(txn))),
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

    /// Get the shared transaction reference for use by other components.
    ///
    /// This allows DbDocMutator to share the same transaction.
    pub(crate) fn shared_txn(&self) -> Arc<TokioMutex<Option<DbTxn<S>>>> {
        self.txn.clone()
    }
}

#[async_trait]
impl<S: Store + 'static> DocFetcher for DbDocFetcher<S> {
    async fn get_all(&self, collection_name: &str) -> query::error::Result<Vec<Document>> {
        // Step 1: Check if collection is in cache (sync, no await while holding lock)
        let (collection_opt, systemstore, datastore) = {
            let txn_guard = self.txn.lock().await;
            let db_txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction already consumed")
            })?;
            let collection_opt = db_txn.collection_cache().get(collection_name).cloned();
            let systemstore = db_txn.systemstore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get systemstore for collection '{}': {}",
                    collection_name, e
                ))
            })?;
            let datastore = db_txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?;
            (collection_opt, systemstore, datastore)
        }; // Lock released here

        // Step 2: If not in cache, load from store (no lock held during await)
        let collection = if let Some(col) = collection_opt {
            col
        } else {
            let loaded = load_collection_from_systemstore(&systemstore, collection_name).await?;
            let collection = loaded
                .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

            // Add to cache
            {
                let mut txn_guard = self.txn.lock().await;
                if let Some(db_txn) = txn_guard.as_mut() {
                    db_txn.cache_collection(collection_name.to_string(), collection.clone());
                }
            }

            collection
        };

        // Step 3: Fetch documents (no lock held)
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
        // Step 1: Check if collection is in cache (sync, no await while holding lock)
        let (collection_opt, systemstore, datastore) = {
            let txn_guard = self.txn.lock().await;
            let db_txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction already consumed")
            })?;
            let collection_opt = db_txn.collection_cache().get(collection_name).cloned();
            let systemstore = db_txn.systemstore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get systemstore for collection '{}': {}",
                    collection_name, e
                ))
            })?;
            let datastore = db_txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?;
            (collection_opt, systemstore, datastore)
        }; // Lock released here

        // Step 2: If not in cache, load from store (no lock held during await)
        let collection = if let Some(col) = collection_opt {
            col
        } else {
            let loaded = load_collection_from_systemstore(&systemstore, collection_name).await?;
            let collection = loaded
                .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

            // Add to cache
            {
                let mut txn_guard = self.txn.lock().await;
                if let Some(db_txn) = txn_guard.as_mut() {
                    db_txn.cache_collection(collection_name.to_string(), collection.clone());
                }
            }

            collection
        };

        // Step 3: Fetch documents (no lock held)
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
