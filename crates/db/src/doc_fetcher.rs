//! Document fetcher for transaction-scoped queries.

use async_lock::Mutex as TokioMutex;
use async_trait::async_trait;
use document::Document;
use query::fetcher::CommitsQueryOptions;
use query::planner::index_selection::{IndexScanParams, IndexScanType};
use query::planner::index_selection::CursorSeek;
use query::runner::{DocFetcher, FetchByIdsResult};
use std::sync::Arc;
use storage::corekv::Store;
use storage::index::RangeIterator;
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
    #[allow(dead_code)]
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
            height_start: options.height_start,
            height_end: options.height_end,
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
    ) -> query::error::Result<query::fetcher::IndexScanResult> {
        use std::collections::HashSet;
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

        // Extract limit/offset for early termination optimization
        let limit = params.limit;
        let offset = params.offset;

        // Helper: apply cursor seek to a RangeIterator when cursor_seek is set.
        async fn apply_seek(
            iter: &mut RangeIterator,
            cursor_seek: &Option<CursorSeek>,
        ) -> Result<(), query::error::QueryError> {
            if let Some(ref seek) = cursor_seek {
                iter.apply_cursor_seek(seek.seek_key.clone(), seek.inclusive)
                    .await
                    .map_err(|e| {
                        query::error::QueryError::execution(format!("cursor seek error: {}", e))
                    })?;
            }
            Ok(())
        }

        // Helper to collect entries with optional early termination and value filtering.
        // Returns (doc_ids, total_iterated) where total_iterated counts ALL entries
        // including those filtered out (for indexFetches metrics).
        async fn collect_with_limit<I: IndexIterator>(
            iter: &mut I,
            limit: Option<u64>,
            offset: u64,
            value_filter: Option<&query::planner::index_selection::ScanValueFilter>,
        ) -> Result<(Vec<String>, u64), query::error::QueryError> {
            let mut entries = Vec::new();
            let mut skipped = 0u64;
            let mut total_iterated = 0u64;

            while let Some(entry) = iter.next().await.map_err(|e| {
                query::error::QueryError::execution(format!("index iteration error: {}", e))
            })? {
                total_iterated += 1;

                // Apply scan-level value filter (matches Go's indexLikeMatcher)
                if let Some(filter) = value_filter {
                    if let Some(first_value) = entry.values.first() {
                        if !filter.matches_value(first_value) {
                            continue;
                        }
                    }
                }

                // Skip offset entries
                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                entries.push(entry.doc_id);

                // Early termination when limit reached
                if let Some(lim) = limit {
                    if entries.len() >= lim as usize {
                        break;
                    }
                }
            }

            Ok((entries, total_iterated))
        }

        // Execute the appropriate scan based on scan type.
        // Returns (doc_ids, raw_fetches) where raw_fetches counts ALL entries iterated
        // including those filtered out by value_filter (for indexFetches metrics).
        let vf = params.value_filter.as_ref();
        let (raw_doc_ids, raw_fetches): (Vec<String>, u64) = match &params.scan_type {
            IndexScanType::ExactMatch { values } => {
                let mut iter = index.get(&datastore, values).await.map_err(|e| {
                    query::error::QueryError::execution(format!("index error: {}", e))
                })?;
                collect_with_limit(&mut iter, limit, offset, vf).await?
            }
            IndexScanType::InScan {
                values,
                suffix_values,
            } => {
                // For IN operator, we need to collect results for each value.
                // For composite indexes with suffix_values (subsequent Eq conditions),
                // use exact match (get) with combined values for efficiency.
                // For composite indexes without suffix_values, use scan_prefix.
                let is_composite = index.description().fields.len() > 1;
                let has_full_key = !suffix_values.is_empty()
                    && suffix_values.len() == index.description().fields.len() - 1;
                let mut all_doc_ids = Vec::new();
                for value in values {
                    if has_full_key {
                        // All index fields covered: In value + suffix Eq values = exact match
                        let mut key_values = vec![value.clone()];
                        key_values.extend(suffix_values.iter().cloned());
                        let mut iter = index.get(&datastore, &key_values).await.map_err(|e| {
                            query::error::QueryError::execution(format!("index error: {}", e))
                        })?;
                        let entries = iter.collect_all().await.map_err(|e| {
                            query::error::QueryError::execution(format!(
                                "index iteration error: {}",
                                e
                            ))
                        })?;
                        all_doc_ids.extend(entries.into_iter().map(|e| e.doc_id));
                    } else if is_composite {
                        let mut iter = index
                            .scan_prefix(&datastore, std::slice::from_ref(value), false)
                            .await
                            .map_err(|e| {
                                query::error::QueryError::execution(format!("index error: {}", e))
                            })?;
                        let entries = iter.collect_all().await.map_err(|e| {
                            query::error::QueryError::execution(format!(
                                "index iteration error: {}",
                                e
                            ))
                        })?;
                        all_doc_ids.extend(entries.into_iter().map(|e| e.doc_id));
                    } else {
                        let mut iter = index
                            .get(&datastore, std::slice::from_ref(value))
                            .await
                            .map_err(|e| {
                                query::error::QueryError::execution(format!("index error: {}", e))
                            })?;
                        let entries = iter.collect_all().await.map_err(|e| {
                            query::error::QueryError::execution(format!(
                                "index iteration error: {}",
                                e
                            ))
                        })?;
                        all_doc_ids.extend(entries.into_iter().map(|e| e.doc_id));
                    }
                }
                let count = all_doc_ids.len() as u64;
                (all_doc_ids, count)
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
                apply_seek(&mut iter, &params.cursor_seek).await?;
                collect_with_limit(&mut iter, limit, offset, vf).await?
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
                apply_seek(&mut iter, &params.cursor_seek).await?;
                collect_with_limit(&mut iter, limit, offset, vf).await?
            }
            IndexScanType::OrScan { branches } => {
                let mut all_doc_ids = Vec::new();
                let mut total_raw_fetches = 0u64;
                for branch in branches {
                    let branch_params = IndexScanParams {
                        index_name: params.index_name.clone(),
                        scan_type: branch.clone(),
                        limit: None,
                        offset: 0,
                        value_filter: None,
                        cursor_seek: None,
                    };
                    let branch_result = self
                        .get_by_index_scan(collection_name, &branch_params)
                        .await?;
                    total_raw_fetches += branch_result.raw_fetches();
                    all_doc_ids.extend(branch_result.doc_ids().iter().cloned());
                }
                (all_doc_ids, total_raw_fetches)
            }
            _ => (Vec::new(), 0),
        };

        // Deduplicate doc_ids while preserving order.
        // Array indexes can return the same document multiple times (once per array element).
        let mut seen = HashSet::new();
        let doc_ids: Vec<String> = raw_doc_ids
            .into_iter()
            .filter(|id| seen.insert(id.clone()))
            .collect();

        Ok(query::fetcher::IndexScanResult::with_raw_count(
            doc_ids,
            raw_fetches,
        ))
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

    async fn get_documents_at_cid(
        &self,
        cid: &str,
        expected_doc_id: Option<&str>,
    ) -> query::error::Result<Vec<Document>> {
        let versioned_fetcher = VersionedFetcher::new(self.txn.clone());
        versioned_fetcher
            .get_documents_at_cid(cid, expected_doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))
    }

    async fn search_fulltext_scored(
        &self,
        collection_name: &str,
        field_name: &str,
        query: &str,
    ) -> query::error::Result<std::collections::HashMap<String, f64>> {
        let (_collection, datastore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;

        let idx_name = crate::index_manager::fulltext_index_name(field_name);
        let ft_index = index_manager
            .get_index(&idx_name)
            .and_then(|idx| idx.as_fulltext())
            .ok_or_else(|| {
                query::error::QueryError::execution(format!(
                    "fulltext index for field '{}' not found on collection '{}'",
                    field_name, collection_name
                ))
            })?;

        ft_index
            .search_scored(&datastore, query)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("fulltext search error: {}", e))
            })
    }

    async fn get_view_cache_items(&self, collection_id: u32) -> query::error::Result<Vec<Vec<u8>>> {
        use storage::corekv::IterOptions;
        use storage::keys::datastore::ViewCacheKey;

        let guard = self.txn.lock().await;
        let txn = guard.as_ref().ok_or_else(|| {
            query::error::QueryError::execution("transaction was already consumed")
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {}", e))
        })?;

        let prefix = ViewCacheKey::collection_prefix(collection_id);
        let opts = IterOptions::new().with_prefix(prefix);
        let mut iter = datastore.iterator(opts).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to iterate view cache: {}", e))
        })?;

        let mut items = Vec::new();
        while let Some(pair) = iter.next().await.map_err(|e| {
            query::error::QueryError::execution(format!("view cache iteration error: {}", e))
        })? {
            items.push(pair.value);
        }

        iter.close().await.map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to close view cache iterator: {}",
                e
            ))
        })?;

        Ok(items)
    }
}
