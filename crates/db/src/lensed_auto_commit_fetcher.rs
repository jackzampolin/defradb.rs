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
use tracing::{debug, info, trace, warn};

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
        let collection_id = &collection.schema().collection_id;
        let target_version_id = &collection.schema().version_id;

        info!(
            collection_name = %collection.name(),
            collection_id = %collection_id,
            target_version_id = %target_version_id,
            "Loading migration context"
        );

        let history = self.load_collection_history(collection).await.ok();

        // Log detailed history information
        if let Some(ref h) = history {
            info!(
                collection_name = %collection.name(),
                version_count = h.len(),
                "Loaded version history"
            );
            for (version_id, link) in h.iter() {
                info!(
                    version_id = %version_id,
                    transform = ?link.transform,
                    previous = ?link.previous,
                    next = ?link.next,
                    "Version history link"
                );
            }
        } else {
            warn!(
                collection_name = %collection.name(),
                "Failed to load collection history"
            );
        }

        let has_migrations = history
            .as_ref()
            .is_some_and(|h| h.values().any(|link| link.transform.is_some()));

        info!(
            collection_name = %collection.name(),
            has_migrations = has_migrations,
            "Migration context loaded"
        );

        Ok((has_migrations, if has_migrations { history } else { None }))
    }

    /// Check if a document needs migration.
    fn doc_needs_migration(doc: &Document, target_version_id: &str, has_migrations: bool) -> bool {
        let doc_version = doc.schema_version_id();
        let needs = if !has_migrations {
            false
        } else {
            doc.needs_migration(target_version_id)
        };

        debug!(
            doc_id = ?doc.id(),
            doc_version = ?doc_version,
            target_version = %target_version_id,
            has_migrations = has_migrations,
            needs_migration = needs,
            "Checking if document needs migration"
        );

        needs
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
    ///
    /// LensDoc values are serde_json::Value. We convert primitive JSON types
    /// to native NormalValues so they match the schema's expected types
    /// (e.g., Bool instead of Json(Bool)).
    fn lens_doc_to_doc(lens_doc: LensDoc, original_doc: &Document) -> Document {
        use document::NormalValue;

        let mut doc = Document::new();
        if let Some(id) = original_doc.id() {
            doc.set_id(id.clone());
        }
        for (field_name, value) in lens_doc {
            if field_name != DOC_ID_FIELD {
                let normal = match value {
                    serde_json::Value::Null => NormalValue::Null,
                    serde_json::Value::Bool(b) => NormalValue::Bool(b),
                    serde_json::Value::Number(ref n) => {
                        if let Some(i) = n.as_i64() {
                            NormalValue::Int(i)
                        } else if let Some(f) = n.as_f64() {
                            NormalValue::Float64(f)
                        } else {
                            NormalValue::Json(value)
                        }
                    }
                    serde_json::Value::String(s) => NormalValue::String(s),
                    other => NormalValue::Json(other),
                };
                doc.set(&field_name, normal);
            }
        }
        doc
    }

    /// Build collection history from versions.
    fn build_collection_history(
        versions: &[schema::CollectionVersion],
        target_version_id: &str,
    ) -> Option<HashMap<String, TargetedHistoryLink>> {
        info!(
            version_count = versions.len(),
            target_version_id = %target_version_id,
            "Building collection history"
        );

        if versions.is_empty() {
            warn!("No versions provided, cannot build history");
            return None;
        }

        let mut full_history: HashMap<String, CollectionHistoryLink> = HashMap::new();
        for version in versions {
            let mut link = CollectionHistoryLink::new(&version.version_id, &version.collection_id);
            if let Some(ref prev) = version.previous_version {
                info!(
                    version_id = %version.version_id,
                    previous_source_collection_id = %prev.source_collection_id,
                    transform = ?prev.transform,
                    "Version has previous_version"
                );
                link = link.with_previous(&prev.source_collection_id);
                if let Some(ref transform_id) = prev.transform {
                    link = link.with_transform(transform_id);
                }
            } else {
                debug!(
                    version_id = %version.version_id,
                    "Version has no previous_version (root version)"
                );
            }
            full_history.insert(version.version_id.clone(), link);
        }

        info!(
            full_history_size = full_history.len(),
            "Built initial history graph"
        );

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

        info!(
            reverse_links_count = reverse_links.len(),
            "Building reverse links"
        );

        for (parent_id, child_id) in &reverse_links {
            debug!(
                parent_id = %parent_id,
                child_id = %child_id,
                "Adding next link"
            );
            if let Some(parent_link) = full_history.get_mut(parent_id) {
                if !parent_link.next.contains(child_id) {
                    parent_link.next.push(child_id.clone());
                }
            } else {
                warn!(
                    parent_id = %parent_id,
                    "Parent version not found in history when adding next link"
                );
            }
        }

        // Log the final full history before targeting
        for (vid, link) in &full_history {
            info!(
                version_id = %vid,
                transform = ?link.transform,
                previous = ?link.previous,
                next = ?link.next,
                "Full history link"
            );
        }

        let result = build_targeted_history(&full_history, target_version_id);

        if result.is_none() {
            warn!(
                target_version_id = %target_version_id,
                "build_targeted_history returned None"
            );
        } else {
            info!(
                target_version_id = %target_version_id,
                targeted_history_size = result.as_ref().map_or(0, |h| h.len()),
                "Successfully built targeted history"
            );
        }

        result
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
            trace!(
                doc_id = ?doc.id(),
                doc_version = ?doc.schema_version_id(),
                target_version = %target_version_id,
                "Document does not need migration, returning as-is"
            );
            return Ok(doc);
        }

        let doc_version = doc.schema_version_id().unwrap_or("unknown").to_string();
        let doc_id_str = doc.id().map(|id| id.to_string()).unwrap_or_default();
        info!(
            doc_id = %doc_id_str,
            from_version = %doc_version,
            to_version = %target_version_id,
            "Document needs migration - starting lens pipeline"
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
    ) -> query::error::Result<query::fetcher::IndexScanResult> {
        use std::collections::HashSet;

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

        // Extract limit/offset for early termination optimization
        let limit = params.limit;
        let offset = params.offset;

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
                collect_with_limit(&mut iter, limit, offset, vf).await?
            }
            IndexScanType::OrScan { branches } => {
                // Discard this txn early; each recursive call creates its own
                let _ = txn.discard();
                let mut all_doc_ids = Vec::new();
                let mut total_raw_fetches = 0u64;
                for branch in branches {
                    let branch_params = IndexScanParams {
                        index_name: params.index_name.clone(),
                        scan_type: branch.clone(),
                        limit: None,
                        offset: 0,
                        value_filter: None,
                    };
                    let branch_result = self
                        .get_by_index_scan(collection_name, &branch_params)
                        .await?;
                    total_raw_fetches += branch_result.raw_fetches();
                    all_doc_ids.extend(branch_result.doc_ids().iter().cloned());
                }
                // Skip the txn.discard() below since we already discarded
                let mut seen = HashSet::new();
                let doc_ids: Vec<String> = all_doc_ids
                    .into_iter()
                    .filter(|id| seen.insert(id.clone()))
                    .collect();
                return Ok(query::fetcher::IndexScanResult::with_raw_count(
                    doc_ids,
                    total_raw_fetches,
                ));
            }
        };

        let _ = txn.discard();

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

    async fn get_view_cache_items(&self, collection_id: u32) -> query::error::Result<Vec<Vec<u8>>> {
        use storage::corekv::IterOptions;
        use storage::keys::datastore::ViewCacheKey;

        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
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

        // Clean up transaction
        let _ = txn.discard();

        Ok(items)
    }
}
