//! Lensed auto-committing document fetcher.
//!
//! This fetcher combines auto-commit transaction management with lens migrations.
//! Documents are automatically migrated during fetch when migrations are registered.

use std::collections::HashMap;
use std::sync::Arc;

use async_lock::Mutex as TokioMutex;
use async_trait::async_trait;
use document::Document;
use lens::{
    build_targeted_history, CollectionHistoryLink, Lens, LensDoc, TargetedHistoryLink, DOC_ID_FIELD,
};
use query::fetcher::CommitsQueryOptions;
use query::planner::index_selection::{IndexScanParams, IndexScanType};
use query::runner::{DocFetcher, FetchByIdsResult};
use storage::corekv::Store;
use storage::index::IndexIterator;
use tracing::{debug, trace};

use crate::collection::{collection_short_id, Collection};
use crate::commits_fetcher::{CommitsFetcher, CommitsQueryOptions as DbCommitsOptions};
use crate::database::DB;
use crate::index_manager::IndexManager;
use crate::schema_loader::get_collections_by_collection_id;
use crate::txn::DbTxn;
use crate::versioned_fetcher::VersionedFetcher;

/// Document fetcher that auto-commits and applies lens migrations.
///
/// Combines the auto-commit behavior of AutoCommitFetcher with lens
/// migration support from LensedDocFetcher.
pub struct LensedAutoCommitFetcher<S: Store> {
    db: Arc<DB<S>>,
}

impl<S: Store> LensedAutoCommitFetcher<S> {
    /// Create a new lensed auto-committing fetcher.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self { db }
    }

    /// Load migration context for a collection: checks full version history for transforms.
    ///
    /// Returns (has_migrations, optional pre-loaded history). This matches Go's
    /// HasMigrations() which loads ALL versions via GetTargetedCollectionHistory()
    /// and checks each one for transforms.
    async fn load_migration_context(
        &self,
        collection: &Collection,
    ) -> query::error::Result<(bool, Option<HashMap<String, TargetedHistoryLink>>)> {
        let history = self.load_collection_history(collection).await.ok();
        let has_migrations = history
            .as_ref()
            .is_some_and(|h| h.values().any(|link| link.transform.is_some()));
        Ok((has_migrations, if has_migrations { history } else { None }))
    }

    /// Check if a document needs migration.
    fn doc_needs_migration(doc: &Document, target_version_id: &str, has_migrations: bool) -> bool {
        if !has_migrations {
            return false;
        }
        doc.needs_migration(target_version_id)
    }

    /// Convert a Document to a LensDoc.
    fn doc_to_lens_doc(doc: &Document) -> Option<LensDoc> {
        let map = doc.to_map().ok()?;
        let mut lens_doc = LensDoc::new();
        for (key, value) in map {
            lens_doc.insert(key, value);
        }
        Some(lens_doc)
    }

    /// Convert a LensDoc back to a Document.
    fn lens_doc_to_doc(lens_doc: LensDoc, original_doc: &Document) -> Document {
        let mut doc = Document::new();
        if let Some(id) = original_doc.id() {
            doc.set_id(id.clone());
        }
        for (field_name, value) in lens_doc {
            if field_name != DOC_ID_FIELD {
                doc.set(&field_name, value);
            }
        }
        doc
    }

    /// Build collection history from versions.
    fn build_collection_history(
        versions: &[schema::CollectionVersion],
        target_version_id: &str,
    ) -> Option<HashMap<String, TargetedHistoryLink>> {
        if versions.is_empty() {
            return None;
        }

        let mut full_history: HashMap<String, CollectionHistoryLink> = HashMap::new();
        for version in versions {
            let mut link = CollectionHistoryLink::new(&version.version_id, &version.collection_id);
            if let Some(ref prev) = version.previous_version {
                link = link.with_previous(&prev.source_collection_id);
                if let Some(ref transform_id) = prev.transform {
                    link = link.with_transform(transform_id);
                }
            }
            full_history.insert(version.version_id.clone(), link);
        }

        // Build `next` links by reverse-indexing `previous` links.
        // Each version's `previous` points to its parent; the parent's `next` should point back.
        let reverse_links: Vec<(String, String)> = full_history
            .values()
            .flat_map(|link| {
                link.previous
                    .iter()
                    .map(|prev_id| (prev_id.clone(), link.version_id.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();

        for (parent_id, child_id) in reverse_links {
            if let Some(parent_link) = full_history.get_mut(&parent_id) {
                if !parent_link.next.contains(&child_id) {
                    parent_link.next.push(child_id);
                }
            }
        }

        build_targeted_history(&full_history, target_version_id)
    }

    /// Load collection history from database.
    async fn load_collection_history(
        &self,
        collection: &Collection,
    ) -> query::error::Result<HashMap<String, TargetedHistoryLink>> {
        let collection_id = &collection.schema().collection_id;
        let target_version_id = &collection.schema().version_id;

        // Create a read-only transaction to load versions
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to create transaction for history lookup: {}",
                e
            ))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        let versions = get_collections_by_collection_id(&systemstore, collection_id)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to load collection versions: {}",
                    e
                ))
            })?;

        let _ = txn.discard(); // Ignore discard errors for read-only txn

        Self::build_collection_history(&versions, target_version_id).ok_or_else(|| {
            query::error::QueryError::execution(format!(
                "failed to build migration history for collection {}",
                collection_id
            ))
        })
    }

    /// Process a document, applying migration if needed.
    ///
    /// Uses pre-loaded history when available to avoid redundant database lookups.
    async fn process_document(
        &self,
        doc: Document,
        collection: &Collection,
        has_migrations: bool,
        preloaded_history: &Option<HashMap<String, TargetedHistoryLink>>,
    ) -> query::error::Result<Document> {
        let target_version_id = &collection.schema().version_id;

        if !Self::doc_needs_migration(&doc, target_version_id, has_migrations) {
            return Ok(doc);
        }

        let doc_version = doc.schema_version_id().unwrap_or("unknown").to_string();
        let doc_id_str = doc.id().map(|id| id.to_string()).unwrap_or_default();
        debug!(
            doc_id = ?doc.id(),
            from_version = %doc_version,
            to_version = %target_version_id,
            "Document needs migration"
        );

        // Use pre-loaded history or load on demand
        let history = match preloaded_history {
            Some(h) => h.clone(),
            None => self.load_collection_history(collection).await?,
        };

        // Check if we have a migration path
        if !history.contains_key(&doc_version) {
            return Err(query::error::QueryError::execution(format!(
                "no migration path found for document {} from version {} to {}",
                doc_id_str, doc_version, target_version_id
            )));
        }

        // Convert to LensDoc
        let original_lens_doc = Self::doc_to_lens_doc(&doc).ok_or_else(|| {
            query::error::QueryError::execution(format!(
                "failed to convert document {} to LensDoc for migration",
                doc_id_str
            ))
        })?;

        // Create and run lens pipeline
        let lens_store = self.db.lens_store().clone();
        let mut lens = Lens::new(lens_store, target_version_id, history);

        lens.put(&doc_version, original_lens_doc.clone())
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to put document {} into lens pipeline: {}",
                    doc_id_str, e
                ))
            })?;

        // Get migrated document
        let migrated_lens_doc = match lens.next().await {
            Some(Ok(migrated)) => migrated,
            Some(Err(e)) => {
                return Err(query::error::QueryError::execution(format!(
                    "lens migration failed for document {}: {}",
                    doc_id_str, e
                )));
            }
            None => {
                return Err(query::error::QueryError::execution(format!(
                    "lens pipeline produced no output for document {}",
                    doc_id_str
                )));
            }
        };

        debug!(
            doc_id = ?doc.id(),
            from_version = %doc_version,
            to_version = %target_version_id,
            "Document migration completed"
        );

        // Convert back to Document
        let mut migrated_doc = Self::lens_doc_to_doc(migrated_lens_doc, &doc);
        migrated_doc.set_schema_version_id(target_version_id);

        Ok(migrated_doc)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocFetcher for LensedAutoCommitFetcher<S> {
    async fn get_all(&self, collection_name: &str) -> query::error::Result<Vec<Document>> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Load migration context once for the whole collection
        let (has_migrations, preloaded_history) = self.load_migration_context(&collection).await?;

        let target_version_id = &collection.schema().version_id;

        if has_migrations {
            debug!(
                collection = %collection_name,
                version_id = %target_version_id,
                "Collection has migrations registered"
            );
        }

        // Create read-only transaction
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {}", e))
        })?;

        let docs = collection
            .get_all_with_datastore(&datastore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        let _ = txn.discard();

        // Count docs needing migration
        let needs_migration_count = docs
            .iter()
            .filter(|doc| Self::doc_needs_migration(doc, target_version_id, has_migrations))
            .count();

        trace!(
            collection = %collection_name,
            doc_count = docs.len(),
            needs_migration = needs_migration_count,
            "Fetched documents"
        );

        // Process each document with pre-loaded history
        let mut processed_docs = Vec::with_capacity(docs.len());
        for doc in docs {
            let processed = self
                .process_document(doc, &collection, has_migrations, &preloaded_history)
                .await?;
            processed_docs.push(processed);
        }

        if needs_migration_count > 0 {
            debug!(
                collection = %collection_name,
                migrated = needs_migration_count,
                "Documents migrated"
            );
        }

        Ok(processed_docs)
    }

    async fn get_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> query::error::Result<Vec<(Document, bool)>> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Load migration context once for the whole collection
        let (has_migrations, preloaded_history) = self.load_migration_context(&collection).await?;

        // Create read-only transaction
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {}", e))
        })?;

        let docs_with_status = collection
            .get_all_with_datastore_include_deleted(&datastore, show_deleted)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        let _ = txn.discard();

        // Process each document (apply migrations if needed)
        let mut processed_docs = Vec::with_capacity(docs_with_status.len());
        for (doc, is_deleted) in docs_with_status {
            let processed = self
                .process_document(doc, &collection, has_migrations, &preloaded_history)
                .await?;
            processed_docs.push((processed, is_deleted));
        }

        Ok(processed_docs)
    }

    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> query::error::Result<FetchByIdsResult> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Load migration context once for the whole collection
        let (has_migrations, preloaded_history) = self.load_migration_context(&collection).await?;

        // Create read-only transaction
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {}", e))
        })?;

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
                None => missing_ids.push(id_str.clone()),
            }
        }

        let _ = txn.discard();

        // Process documents with pre-loaded history
        let mut processed_docs = Vec::with_capacity(docs.len());
        for doc in docs {
            let processed = self
                .process_document(doc, &collection, has_migrations, &preloaded_history)
                .await?;
            processed_docs.push(processed);
        }

        Ok(FetchByIdsResult::partial(processed_docs, missing_ids))
    }

    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> query::error::Result<Vec<Document>> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Load migration context once for the whole collection
        let (has_migrations, preloaded_history) = self.load_migration_context(&collection).await?;

        // Create read-only transaction
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {}", e))
        })?;

        let all_docs = collection
            .get_all_with_datastore(&datastore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        let _ = txn.discard();

        let matching_docs: Vec<Document> = all_docs
            .into_iter()
            .filter(|doc| {
                doc.get(field_name)
                    .and_then(|v| v.as_str())
                    .map(|v| v == value)
                    .unwrap_or(false)
            })
            .collect();

        // Process documents with pre-loaded history
        let mut processed_docs = Vec::with_capacity(matching_docs.len());
        for doc in matching_docs {
            let processed = self
                .process_document(doc, &collection, has_migrations, &preloaded_history)
                .await?;
            processed_docs.push(processed);
        }

        Ok(processed_docs)
    }

    async fn get_commits(
        &self,
        options: &CommitsQueryOptions,
    ) -> query::error::Result<Vec<Document>> {
        // Create a read-only transaction for the commits fetcher
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Wrap in Arc<Mutex<Option>> for CommitsFetcher
        let txn_holder: std::sync::Arc<TokioMutex<Option<DbTxn<S>>>> =
            std::sync::Arc::new(TokioMutex::new(Some(txn)));

        // Convert query options to db options
        let db_options = DbCommitsOptions {
            doc_id: options.doc_id.clone(),
            cid: options.cid.clone(),
            depth: options.depth,
            field_name: options.field_name.clone(),
        };

        let commits_fetcher = CommitsFetcher::new(txn_holder.clone());
        let result = commits_fetcher
            .fetch_commits(&db_options)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("commits fetch error: {}", e))
            });

        // Clean up transaction
        if let Some(txn) = txn_holder.lock().await.take() {
            let _ = txn.discard();
        }

        result
    }

    async fn get_by_index_scan(
        &self,
        collection_name: &str,
        params: &IndexScanParams,
    ) -> query::error::Result<Vec<String>> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create read-only transaction
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {}", e))
        })?;

        // Create an IndexManager from the collection schema
        let short_id = collection_short_id(collection.collection_id());
        let index_manager =
            IndexManager::from_collection(short_id, collection.schema()).map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to create index manager for collection '{}': {}",
                    collection_name, e
                ))
            })?;

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

        let _ = txn.discard();

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
        // Create a read-only transaction for the versioned fetcher
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Wrap in Arc<Mutex<Option>> for VersionedFetcher
        let txn_holder: std::sync::Arc<TokioMutex<Option<DbTxn<S>>>> =
            std::sync::Arc::new(TokioMutex::new(Some(txn)));

        let versioned_fetcher = VersionedFetcher::new(txn_holder.clone());
        let result = versioned_fetcher
            .get_document_at_cid(cid, expected_doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()));

        // Clean up transaction
        if let Some(txn) = txn_holder.lock().await.take() {
            let _ = txn.discard();
        }

        result
    }

    async fn get_documents_at_cid(
        &self,
        cid: &str,
        expected_doc_id: Option<&str>,
    ) -> query::error::Result<Vec<Document>> {
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let txn_holder: std::sync::Arc<TokioMutex<Option<DbTxn<S>>>> =
            std::sync::Arc::new(TokioMutex::new(Some(txn)));

        let versioned_fetcher = VersionedFetcher::new(txn_holder.clone());
        let result = versioned_fetcher
            .get_documents_at_cid(cid, expected_doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()));

        if let Some(txn) = txn_holder.lock().await.take() {
            let _ = txn.discard();
        }

        result
    }
}
