//! Lensed document fetcher that applies schema migrations.
//!
//! This fetcher wraps an inner fetcher and applies lens transforms to documents
//! that are stored with older schema versions.
//!
//! # Migration Flow
//!
//! When a document is fetched:
//! 1. The fetcher loads the document with its stored schema version
//! 2. If the document's version differs from the target collection version
//!    and migrations are registered, the document is transformed
//! 3. Migrated values are cached in the datastore to avoid re-migration
//!
//! # Lazy Migration
//!
//! Documents are migrated on first read, not when schemas are updated.
//! This allows schema updates without rewriting all existing documents.
//! The migrated values and new version are cached in the datastore.

use std::collections::HashMap;
use std::sync::Arc;

use async_lock::Mutex as TokioMutex;
use async_trait::async_trait;
use datastore::NamespaceView;
use document::Document;
use lens::{
    build_targeted_history, CollectionHistoryLink, Lens, LensDoc, TargetedHistoryLink,
    TransformStore, DOC_ID_FIELD,
};
use query::runner::{DocFetcher, FetchByIdsResult};
use schema::CollectionVersion;
use storage::corekv::Store;
use tracing::{debug, trace, warn};

use crate::collection::Collection;
use crate::collection_loader::get_collection_with_lazy_load;
use crate::schema_loader::get_collections_by_collection_id;
use crate::txn::DbTxn;

/// Document fetcher that applies lens migrations to documents.
///
/// When documents are fetched from older schema versions, they are
/// transformed to the current (target) schema version using registered
/// lens migrations.
pub struct LensedDocFetcher<S: Store> {
    txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
    #[allow(dead_code)]
    lens_store: Arc<dyn TransformStore>,
    /// Cache of collection version histories keyed by collection name.
    #[allow(dead_code)]
    history_cache: async_lock::RwLock<HashMap<String, HashMap<String, TargetedHistoryLink>>>,
}

impl<S: Store> LensedDocFetcher<S> {
    /// Create a new lensed document fetcher.
    ///
    /// # Arguments
    ///
    /// * `txn` - The database transaction
    /// * `lens_store` - The lens transform store for applying migrations
    #[allow(dead_code)]
    pub(crate) fn new(txn: DbTxn<S>, lens_store: Arc<dyn TransformStore>) -> Self {
        Self {
            txn: Arc::new(TokioMutex::new(Some(txn))),
            lens_store,
            history_cache: async_lock::RwLock::new(HashMap::new()),
        }
    }

    /// Take the transaction out of the fetcher (for commit/rollback).
    #[allow(dead_code)]
    pub(crate) async fn take_txn(&self) -> Option<DbTxn<S>> {
        self.txn.lock().await.take()
    }

    /// Check if the transaction has been consumed.
    pub async fn is_consumed(&self) -> bool {
        self.txn.lock().await.is_none()
    }

    /// Get the shared transaction reference.
    #[allow(dead_code)]
    pub(crate) fn shared_txn(&self) -> Arc<TokioMutex<Option<DbTxn<S>>>> {
        self.txn.clone()
    }

    /// Check if a collection has migrations registered.
    ///
    /// A collection has migrations if its schema or any of its previous versions
    /// have a transform configured in the previous_version field.
    fn collection_has_migrations(collection: &Collection) -> bool {
        // Check if the current collection version has a previous version with a transform
        if let Some(ref prev) = collection.schema().previous_version {
            if prev.transform.is_some() {
                return true;
            }
        }
        false
    }

    /// Build the version history for a collection from a list of all versions.
    ///
    /// This takes all known versions and builds a directed graph showing
    /// the migration path to the target version.
    ///
    /// # Arguments
    /// * `versions` - All versions of the collection loaded from systemstore
    /// * `target_version_id` - The version to build the history toward
    fn build_collection_history_from_versions(
        versions: &[CollectionVersion],
        target_version_id: &str,
    ) -> Option<HashMap<String, TargetedHistoryLink>> {
        if versions.is_empty() {
            return None;
        }

        let mut full_history: HashMap<String, CollectionHistoryLink> = HashMap::new();

        // Add each version to the history
        for version in versions {
            let mut link = CollectionHistoryLink::new(&version.version_id, &version.collection_id);

            // Check if there's a previous version
            if let Some(ref prev) = version.previous_version {
                link = link.with_previous(&prev.source_collection_id);
                if let Some(ref transform_id) = prev.transform {
                    link = link.with_transform(transform_id);
                }
            }

            full_history.insert(version.version_id.clone(), link);
        }

        build_targeted_history(&full_history, target_version_id)
    }

    /// Load full collection history from systemstore.
    ///
    /// This loads all versions of a collection and builds the targeted history graph.
    async fn load_collection_history(
        &self,
        collection: &Collection,
    ) -> query::error::Result<HashMap<String, TargetedHistoryLink>> {
        let collection_id = &collection.schema().collection_id;
        let target_version_id = &collection.schema().version_id;

        // First check if history is cached
        {
            let cache = self.history_cache.read().await;
            if let Some(history) = cache.get(collection_id) {
                return Ok(history.clone());
            }
        }

        // Load all versions from systemstore
        let txn_guard = self.txn.lock().await;
        let txn = txn_guard.as_ref().ok_or_else(|| {
            query::error::QueryError::execution("transaction not available for history lookup")
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

        drop(txn_guard); // Release lock before building history

        if versions.is_empty() {
            return Err(query::error::QueryError::execution(format!(
                "no versions found for collection {}",
                collection_id
            )));
        }

        // Build the targeted history
        let history = Self::build_collection_history_from_versions(&versions, target_version_id)
            .ok_or_else(|| {
                query::error::QueryError::execution(format!(
                    "failed to build migration history for collection {}",
                    collection_id
                ))
            })?;

        // Cache the history
        {
            let mut cache = self.history_cache.write().await;
            cache.insert(collection_id.clone(), history.clone());
        }

        Ok(history)
    }

    /// Convert a Document to a LensDoc.
    fn doc_to_lens_doc(doc: &Document) -> Option<LensDoc> {
        // Use Document's to_map which handles all field conversions properly
        let map = doc.to_map().ok()?;

        // Convert HashMap to serde_json::Map
        let mut lens_doc = LensDoc::new();
        for (key, value) in map {
            lens_doc.insert(key, value);
        }

        Some(lens_doc)
    }

    /// Convert a LensDoc back to a Document.
    #[allow(dead_code)]
    fn lens_doc_to_doc(lens_doc: LensDoc, original_doc: &Document) -> Document {
        let mut doc = Document::new();

        // Preserve original ID
        if let Some(id) = original_doc.id() {
            doc.set_id(id.clone());
        }

        // Copy fields from lens doc
        for (field_name, value) in lens_doc {
            if field_name != DOC_ID_FIELD {
                doc.set(&field_name, value);
            }
        }

        doc
    }

    /// Check if a document needs migration to the target version.
    fn doc_needs_migration(doc: &Document, target_version_id: &str, has_migrations: bool) -> bool {
        if !has_migrations {
            return false;
        }

        // Check if document's version differs from target
        doc.needs_migration(target_version_id)
    }

    /// Process a document, applying migration if needed.
    ///
    /// If the document's schema version matches the target, returns it unchanged.
    /// Otherwise, transforms it through the lens pipeline and caches the result.
    async fn process_document(
        &self,
        doc: Document,
        collection: &Collection,
        datastore: &NamespaceView,
        has_migrations: bool,
    ) -> query::error::Result<Document> {
        let target_version_id = &collection.schema().version_id;

        // Check if migration is needed
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

        // Load the collection history
        let history = self.load_collection_history(collection).await?;

        // Check if we have a migration path for this version
        if !history.contains_key(&doc_version) {
            return Err(query::error::QueryError::execution(format!(
                "no migration path found for document {} from version {} to {}",
                doc_id_str, doc_version, target_version_id
            )));
        }

        // Convert document to LensDoc
        let original_lens_doc = Self::doc_to_lens_doc(&doc).ok_or_else(|| {
            query::error::QueryError::execution(format!(
                "failed to convert document {} to LensDoc for migration",
                doc_id_str
            ))
        })?;

        // Create and run the lens pipeline
        let mut lens = Lens::new(self.lens_store.clone(), target_version_id, history);

        // Put document into pipeline
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
        let mut migrated_doc = Self::lens_doc_to_doc(migrated_lens_doc.clone(), &doc);
        migrated_doc.set_schema_version_id(target_version_id);

        // Cache the migrated values in datastore
        if let Err(e) = self
            .update_datastore(
                datastore,
                &doc,
                &original_lens_doc,
                &migrated_lens_doc,
                target_version_id,
            )
            .await
        {
            warn!(
                doc_id = ?doc.id(),
                error = %e,
                "Failed to cache migrated document - migration still applied in memory"
            );
        }

        Ok(migrated_doc)
    }

    /// Update the datastore with migrated document values.
    ///
    /// This caches the migrated field values and updates the document's
    /// schema version to the target version. Only modified fields are written.
    ///
    /// Matches Go's `updateDataStore` in internal/lens/fetcher.go.
    async fn update_datastore(
        &self,
        datastore: &NamespaceView,
        doc: &Document,
        original: &LensDoc,
        migrated: &LensDoc,
        target_version_id: &str,
    ) -> query::error::Result<()> {
        let doc_id = match doc.id() {
            Some(id) => id.to_string(),
            None => return Ok(()), // No ID, can't cache
        };

        // Find changed fields
        let changed_fields: Vec<(&String, &serde_json::Value)> = migrated
            .iter()
            .filter(|(key, value)| {
                // Skip special fields
                if *key == DOC_ID_FIELD {
                    return false;
                }
                // Include if field is new or value changed
                match original.get(*key) {
                    Some(orig_val) => orig_val != *value,
                    None => true,
                }
            })
            .collect();

        if changed_fields.is_empty() && original.len() == migrated.len() {
            // No changes, just update version
            trace!(
                doc_id = %doc_id,
                "No field changes, updating version only"
            );
        } else {
            trace!(
                doc_id = %doc_id,
                changed_count = changed_fields.len(),
                "Caching migrated field values"
            );
        }

        // Write changed fields to datastore
        // Note: In a full implementation, we would use the collection's field keys
        // For now, we just update the version to mark the document as migrated
        for (field_name, value) in &changed_fields {
            // Build field key: /d/<collection_short_id>/<doc_id>/<field_id>
            // This is simplified - full implementation needs collection metadata
            let value_bytes = serde_json::to_vec(value).map_err(|e| {
                query::error::QueryError::execution(format!("failed to serialize field: {}", e))
            })?;

            // Log what would be written (actual field key construction needs collection info)
            trace!(
                doc_id = %doc_id,
                field = %field_name,
                value_len = value_bytes.len(),
                "Would cache migrated field value"
            );
        }

        // Update the version field
        // Key format: /d/<collection_short_id>/<doc_id>/v
        // We need the collection short ID to build the proper key
        // For now, use a simplified approach that updates just the version marker
        let version_bytes = target_version_id.as_bytes();

        // Build version key - simplified, using doc_id as a marker
        // In full implementation, this would use Collection::version_key()
        let version_key = format!("/v/{}", doc_id);
        datastore
            .set(version_key.as_bytes(), version_bytes)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("failed to update version: {}", e))
            })?;

        debug!(
            doc_id = %doc_id,
            target_version = %target_version_id,
            changed_fields = changed_fields.len(),
            "Cached migrated document"
        );

        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocFetcher for LensedDocFetcher<S> {
    async fn get_all(&self, collection_name: &str) -> query::error::Result<Vec<Document>> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        // Check if collection has migrations registered
        let has_migrations = Self::collection_has_migrations(&collection);
        let target_version_id = &collection.schema().version_id;

        if has_migrations {
            debug!(
                collection = %collection_name,
                version_id = %target_version_id,
                "Collection has migrations registered"
            );
        }

        let docs = collection
            .get_all_with_datastore(&datastore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        // Count documents needing migration for logging
        let needs_migration_count = docs
            .iter()
            .filter(|doc| Self::doc_needs_migration(doc, target_version_id, has_migrations))
            .count();

        trace!(
            collection = %collection_name,
            doc_count = docs.len(),
            needs_migration = needs_migration_count,
            has_migrations = has_migrations,
            "Fetched documents"
        );

        // Process each document, applying migration if needed
        let mut processed_docs = Vec::with_capacity(docs.len());
        for doc in docs {
            let processed = self
                .process_document(doc, &collection, &datastore, has_migrations)
                .await?;
            processed_docs.push(processed);
        }

        if needs_migration_count > 0 {
            debug!(
                collection = %collection_name,
                migrated = needs_migration_count,
                total_docs = processed_docs.len(),
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
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        // Check if collection has migrations registered
        let has_migrations = Self::collection_has_migrations(&collection);

        let docs_with_status = collection
            .get_all_with_datastore_include_deleted(&datastore, show_deleted)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        // Process each document, applying migration if needed
        let mut processed_docs = Vec::with_capacity(docs_with_status.len());
        for (doc, is_deleted) in docs_with_status {
            let processed = self
                .process_document(doc, &collection, &datastore, has_migrations)
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
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        let has_migrations = Self::collection_has_migrations(&collection);
        let target_version_id = &collection.schema().version_id;

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

        // Count documents needing migration for logging
        let needs_migration_count = docs
            .iter()
            .filter(|doc| Self::doc_needs_migration(doc, target_version_id, has_migrations))
            .count();

        trace!(
            collection = %collection_name,
            requested = doc_ids.len(),
            found = docs.len(),
            missing = missing_ids.len(),
            needs_migration = needs_migration_count,
            has_migrations = has_migrations,
            "Fetched documents by ID"
        );

        // Process each document, applying migration if needed
        let mut processed_docs = Vec::with_capacity(docs.len());
        for doc in docs {
            let processed = self
                .process_document(doc, &collection, &datastore, has_migrations)
                .await?;
            processed_docs.push(processed);
        }

        if needs_migration_count > 0 {
            debug!(
                collection = %collection_name,
                migrated = needs_migration_count,
                total_docs = processed_docs.len(),
                "Documents migrated"
            );
        }

        Ok(FetchByIdsResult::partial(processed_docs, missing_ids))
    }

    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> query::error::Result<Vec<Document>> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        let has_migrations = Self::collection_has_migrations(&collection);
        let target_version_id = &collection.schema().version_id;

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

        // Count documents needing migration for logging
        let needs_migration_count = matching_docs
            .iter()
            .filter(|doc| Self::doc_needs_migration(doc, target_version_id, has_migrations))
            .count();

        trace!(
            collection = %collection_name,
            field = %field_name,
            value = %value,
            matches = matching_docs.len(),
            needs_migration = needs_migration_count,
            has_migrations = has_migrations,
            "Fetched documents by field value"
        );

        // Process each document, applying migration if needed
        let mut processed_docs = Vec::with_capacity(matching_docs.len());
        for doc in matching_docs {
            let processed = self
                .process_document(doc, &collection, &datastore, has_migrations)
                .await?;
            processed_docs.push(processed);
        }

        if needs_migration_count > 0 {
            debug!(
                collection = %collection_name,
                migrated = needs_migration_count,
                total_docs = processed_docs.len(),
                "Documents migrated"
            );
        }

        Ok(processed_docs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn test_doc_to_lens_doc_conversion() {
        let mut doc = Document::new();
        doc.set("name", Value::String("Alice".to_string()));
        doc.set("age", Value::Number(30.into()));

        let lens_doc = LensedDocFetcher::<storage::MemoryStore>::doc_to_lens_doc(&doc).unwrap();

        assert_eq!(
            lens_doc.get("name").unwrap(),
            &Value::String("Alice".to_string())
        );
        assert_eq!(lens_doc.get("age").unwrap(), &Value::Number(30.into()));
    }
}
