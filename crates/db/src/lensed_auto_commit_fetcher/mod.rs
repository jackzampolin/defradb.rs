//! Lensed auto-committing document fetcher.
//!
//! This fetcher combines auto-commit transaction management with lens migrations.
//! Documents are automatically migrated during fetch when migrations are registered.

mod fetcher;
mod index_scan;
pub(crate) mod migration;
mod stream;
mod vector_search;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use document::Document;
use lens::TargetedHistoryLink;
use query::fetcher::CommitsQueryOptions;
use query::planner::index_selection::IndexScanParams;
use query::runner::{DocFetcher, FetchByIdsResult};
use storage::corekv::Store;

use crate::database::DB;

/// Cached migration context for a collection.
type MigrationContext = (bool, Option<HashMap<String, TargetedHistoryLink>>);

#[derive(Default)]
struct MigrationCache {
    generation: u64,
    contexts: HashMap<String, MigrationContext>,
}

/// Document fetcher that auto-commits and applies lens migrations.
///
/// Combines the auto-commit behavior of AutoCommitFetcher with lens
/// migration support from LensedDocFetcher.
pub struct LensedAutoCommitFetcher<S: Store> {
    db: Arc<DB<S>>,
    write_back_migrations: bool,
    /// Cache of migration contexts keyed by `"{collection_id}:{version_id}"`.
    /// The full cache is invalidated whenever the committed migration graph's
    /// generation changes.
    migration_cache: Mutex<MigrationCache>,
}

impl<S: Store> LensedAutoCommitFetcher<S> {
    /// Create a new lensed auto-committing fetcher.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self {
            db,
            write_back_migrations: true,
            migration_cache: Mutex::new(MigrationCache::default()),
        }
    }

    /// Create a lensed fetcher that returns transformed values without lazy
    /// persistence.
    ///
    /// Used by mutation rebasing after the caller already holds the document's
    /// write guard. The mutation itself persists the active-schema document,
    /// and attempting lazy write-back here would recursively acquire that guard.
    pub(crate) fn new_without_write_back(db: Arc<DB<S>>) -> Self {
        Self {
            db,
            write_back_migrations: false,
            migration_cache: Mutex::new(MigrationCache::default()),
        }
    }

    /// Build a lightweight fetcher for use by a `DocStream` that outlives a
    /// single `&self` call. `migration_cache` starts fresh rather than
    /// shared: it is pure memoization (`load_migration_context` recomputes on
    /// a miss), so a cold cache costs nothing correctness-wise.
    pub(super) fn clone_for_stream(&self) -> Self {
        Self {
            db: self.db.clone(),
            write_back_migrations: self.write_back_migrations,
            migration_cache: Mutex::new(MigrationCache::default()),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocFetcher for LensedAutoCommitFetcher<S> {
    async fn get_all(&self, collection_name: &str) -> query::error::Result<Vec<Document>> {
        self.get_all_impl(collection_name).await
    }

    async fn get_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> query::error::Result<Vec<(Document, bool)>> {
        self.get_all_with_deleted_impl(collection_name, show_deleted)
            .await
    }

    async fn stream_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> query::error::Result<Box<dyn query::doc_stream::DocStream>> {
        self.stream_all_with_deleted_impl(collection_name, show_deleted)
            .await
    }

    async fn stream_by_doc_short_ids(
        &self,
        collection_name: &str,
        doc_short_ids: &[u64],
        show_deleted: bool,
    ) -> query::error::Result<Box<dyn query::doc_stream::DocStream>> {
        self.stream_by_doc_short_ids_impl(collection_name, doc_short_ids, show_deleted)
            .await
    }

    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> query::error::Result<FetchByIdsResult> {
        self.get_by_ids_impl(collection_name, doc_ids).await
    }

    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> query::error::Result<Vec<Document>> {
        self.get_by_field_value_impl(collection_name, field_name, value)
            .await
    }

    async fn get_commits(
        &self,
        options: &CommitsQueryOptions,
    ) -> query::error::Result<Vec<Document>> {
        self.get_commits_impl(options).await
    }

    async fn get_by_index_scan(
        &self,
        collection_name: &str,
        params: &IndexScanParams,
    ) -> query::error::Result<query::fetcher::IndexScanResult> {
        self.get_by_index_scan_impl(collection_name, params).await
    }

    fn supports_index_queries(&self) -> bool {
        true
    }

    async fn vector_search(
        &self,
        collection_name: &str,
        index_id: u32,
        query_vector: &[f64],
        k: usize,
        effort: Option<usize>,
    ) -> query::error::Result<Vec<u64>> {
        self.vector_search_impl(collection_name, index_id, query_vector, k, effort)
            .await
    }

    fn supports_vector_search(&self) -> bool {
        true
    }

    async fn get_document_at_cid(
        &self,
        cid: &str,
        expected_doc_id: Option<&str>,
        caller_identity: Option<&identity::Did>,
    ) -> query::error::Result<Document> {
        self.get_document_at_cid_impl(cid, expected_doc_id, caller_identity)
            .await
    }

    async fn get_documents_at_cid(
        &self,
        cid: &str,
        expected_doc_id: Option<&str>,
        caller_identity: Option<&identity::Did>,
    ) -> query::error::Result<Vec<Document>> {
        self.get_documents_at_cid_impl(cid, expected_doc_id, caller_identity)
            .await
    }

    async fn search_fulltext_scored(
        &self,
        collection_name: &str,
        field_name: &str,
        query: &str,
    ) -> query::error::Result<std::collections::HashMap<String, f64>> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {}", e))
        })?;

        let short_id = collection.resolved_root_id();
        let index_manager = crate::index_manager::IndexManager::from_indexes(
            short_id,
            collection.schema(),
            collection.write_indexes(),
        )
        .map_err(|e| {
            query::error::QueryError::execution(format!("failed to create index manager: {}", e))
        })?;

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

        let result = match ft_index.search_scored(&datastore, query).await {
            Ok(scores) => match txn.systemstore() {
                Ok(systemstore) => crate::doc_id_map::resolve_doc_id_scores(&systemstore, scores)
                    .await
                    .map_err(|e| {
                        query::error::QueryError::execution(format!(
                            "doc ID resolution error: {}",
                            e
                        ))
                    }),
                Err(e) => Err(query::error::QueryError::execution(format!(
                    "failed to get systemstore: {}",
                    e
                ))),
            },
            Err(e) => Err(query::error::QueryError::execution(format!(
                "fulltext search error: {}",
                e
            ))),
        };

        let _ = txn.discard();
        result
    }

    async fn get_view_cache_items(&self, collection_id: u32) -> query::error::Result<Vec<Vec<u8>>> {
        self.get_view_cache_items_impl(collection_id).await
    }
}
