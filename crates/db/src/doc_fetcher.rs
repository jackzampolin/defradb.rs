//! Document fetcher for transaction-scoped queries.

use async_lock::Mutex as TokioMutex;
use async_trait::async_trait;
use document::Document;
use query::fetcher::CommitsQueryOptions;
use query::planner::index_selection::{IndexScanParams, IndexScanType};
use query::runner::{DocFetcher, FetchByIdsResult};
use std::sync::Arc;
use storage::corekv::Store;
use tracing::warn;

use crate::collection_loader::{get_collection_with_index_manager, get_collection_with_lazy_load};
use crate::commits_fetcher::{CommitsFetcher, CommitsQueryOptions as DbCommitsOptions};
use crate::txn::DbTxn;
use crate::versioned_fetcher::VersionedFetcher;

/// Document fetcher that uses a database transaction.
///
/// This fetcher holds a reference to an active transaction and uses the
/// transaction's collection cache with lazy loading.
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
/// the transaction. Once loaded, the collection metadata is cached for the
/// duration of the transaction. Note: This provides transaction-level caching,
/// not true snapshot isolation - if collections are accessed at different times,
/// they reflect the store state at the time of first access.
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocFetcher for DbDocFetcher<S> {
    async fn get_all(&self, collection_name: &str) -> query::error::Result<Vec<Document>> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        collection
            .get_all_with_datastore(&datastore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))
    }

    async fn get_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> query::error::Result<Vec<(Document, bool)>> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        collection
            .get_all_with_datastore_include_deleted(&datastore, show_deleted)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))
    }

    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> query::error::Result<FetchByIdsResult> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        let mut docs = Vec::new();
        let mut missing_ids = Vec::new();

        for id_str in doc_ids {
            // Go DefraDB treats invalid doc IDs as "not found" rather than errors.
            // This matches behavior where querying for a non-existent ID returns empty results.
            let doc_id = match document::DocID::from_string(id_str) {
                Ok(id) => id,
                Err(_) => {
                    // Invalid doc ID format - treat as not found
                    missing_ids.push(id_str.clone());
                    continue;
                }
            };

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

    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> query::error::Result<Vec<Document>> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        // Get all documents and filter by field value.
        // This is a fallback implementation - index-based lookup can be added later.
        let all_docs = collection
            .get_all_with_datastore(&datastore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        let matching_docs: Vec<Document> = all_docs
            .into_iter()
            .filter(|doc| {
                doc.get(field_name)
                    .and_then(|v| v.as_str())
                    .map(|v| v == value)
                    .unwrap_or(false)
            })
            .collect();

        Ok(matching_docs)
    }

    async fn get_commits(
        &self,
        options: &CommitsQueryOptions,
    ) -> query::error::Result<Vec<Document>> {
        // Convert query options to db options
        let db_options = DbCommitsOptions {
            doc_id: options.doc_id.clone(),
            cid: options.cid.clone(),
            depth: options.depth,
            field_name: options.field_name.clone(),
        };

        let commits_fetcher = CommitsFetcher::new(self.txn.clone());
        commits_fetcher
            .fetch_commits(&db_options)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("commits fetch error: {}", e)))
    }

    async fn get_by_index_scan(
        &self,
        collection_name: &str,
        params: &IndexScanParams,
    ) -> query::error::Result<Vec<String>> {
        use storage::index::IndexIterator;

        let (_collection, datastore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;

        // Get the index
        let index = index_manager.get_index(&params.index_name).ok_or_else(|| {
            query::error::QueryError::execution(format!(
                "index '{}' not found on collection '{}'",
                params.index_name, collection_name
            ))
        })?;

        // Execute the appropriate scan based on scan type
        let doc_ids = match &params.scan_type {
            IndexScanType::ExactMatch { values } => {
                let mut iter = index.get(&datastore, values).await.map_err(|e| {
                    query::error::QueryError::execution(format!("index error: {}", e))
                })?;
                let entries = iter.collect_all().await.map_err(|e| {
                    query::error::QueryError::execution(format!("index iteration error: {}", e))
                })?;
                entries.into_iter().map(|e| e.doc_id).collect()
            }
            IndexScanType::InScan { values } => {
                // For IN operator, we need to collect results for each value
                let mut all_doc_ids = Vec::new();
                for value in values {
                    let mut iter = index.get(&datastore, &[value.clone()]).await.map_err(|e| {
                        query::error::QueryError::execution(format!("index error: {}", e))
                    })?;
                    let entries = iter.collect_all().await.map_err(|e| {
                        query::error::QueryError::execution(format!("index iteration error: {}", e))
                    })?;
                    all_doc_ids.extend(entries.into_iter().map(|e| e.doc_id));
                }
                all_doc_ids
            }
            IndexScanType::PrefixScan {
                prefix_values,
                reverse,
            } => {
                let mut iter = index
                    .scan_prefix(&datastore, prefix_values, *reverse)
                    .await
                    .map_err(|e| {
                        query::error::QueryError::execution(format!("index error: {}", e))
                    })?;
                let entries = iter.collect_all().await.map_err(|e| {
                    query::error::QueryError::execution(format!("index iteration error: {}", e))
                })?;
                entries.into_iter().map(|e| e.doc_id).collect()
            }
            IndexScanType::RangeScan {
                prefix_values,
                lower,
                upper,
                reverse,
            } => {
                let mut iter = index
                    .scan_range(
                        &datastore,
                        prefix_values,
                        lower.clone(),
                        upper.clone(),
                        *reverse,
                    )
                    .await
                    .map_err(|e| {
                        query::error::QueryError::execution(format!("index error: {}", e))
                    })?;
                let entries = iter.collect_all().await.map_err(|e| {
                    query::error::QueryError::execution(format!("index iteration error: {}", e))
                })?;
                entries.into_iter().map(|e| e.doc_id).collect()
            }
        };

        Ok(doc_ids)
    }

    fn supports_index_queries(&self) -> bool {
        true
    }

    async fn get_document_at_cid(
        &self,
        cid: &str,
        expected_doc_id: Option<&str>,
    ) -> query::error::Result<Document> {
        let versioned_fetcher = VersionedFetcher::new(self.txn.clone());
        versioned_fetcher
            .get_document_at_cid(cid, expected_doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))
    }
}
