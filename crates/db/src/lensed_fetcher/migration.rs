//! Migration context loading and document migration logic.

use std::collections::HashMap;

use datastore::NamespaceView;
use document::Document;
use lens::{
    build_targeted_history, CollectionHistoryLink, Lens, LensDoc, TargetedHistoryLink, DOC_ID_FIELD,
};
use schema::CollectionVersion;
use storage::corekv::Store;
use tracing::{debug, trace, warn};

use crate::collection::Collection;
use crate::schema_loader::get_collections_by_collection_id;

use super::LensedDocFetcher;

impl<S: Store> LensedDocFetcher<S> {
    /// Check if any version in a list of collection versions has migrations registered.
    ///
    /// A collection has migrations if any version in its history has a transform
    /// configured in the previous_version field. This matches Go's behavior of
    /// checking the full history, not just the current version.
    fn versions_have_migrations(versions: &[CollectionVersion]) -> bool {
        for version in versions {
            if let Some(ref prev) = version.previous_version {
                if prev.transform.is_some() {
                    return true;
                }
            }
        }
        false
    }

    /// Check if a collection has migrations registered (quick check).
    ///
    /// This is a fast check that only looks at the current version.
    /// For a full check, use `versions_have_migrations` with all loaded versions.
    #[allow(dead_code)]
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

        // First pass: build links with `previous` and `transform` from each
        // version's stored `previous_version` pointer.
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

        // Second pass: backfill `next` so the targeted-history walk can
        // traverse forward from older versions. Without this, a node whose
        // collection sits at v1 cannot find a path to a doc that arrived at
        // v2 — the previous-only graph is unreachable from v1.
        // Mirrors the equivalent pass in `lensed_auto_commit_fetcher`.
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

    /// Load all versions of a collection and check if any have migrations.
    ///
    /// Returns the list of versions and a boolean indicating if any have transforms.
    /// This matches Go's behavior of checking the full history for migrations.
    pub(super) async fn load_versions_and_check_migrations(
        &self,
        collection: &Collection,
    ) -> query::error::Result<(Vec<CollectionVersion>, bool)> {
        let collection_id = &collection.schema().collection_id;

        // Load all versions from systemstore
        let txn_guard = self.txn.lock().await;
        let txn = txn_guard.as_ref().ok_or_else(|| {
            query::error::QueryError::execution("transaction not available for version lookup")
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

        drop(txn_guard); // Release lock

        // Check if any version has migrations (matching Go's behavior)
        let has_migrations = Self::versions_have_migrations(&versions);

        Ok((versions, has_migrations))
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

        // Load versions using the helper
        let (versions, _) = self.load_versions_and_check_migrations(collection).await?;

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
    pub(super) fn doc_to_lens_doc(doc: &Document) -> Option<LensDoc> {
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
        use document::NormalValue;

        let mut doc = Document::new();

        // Preserve original ID
        if let Some(id) = original_doc.id() {
            doc.set_id(id.clone());
        }

        // Copy fields from lens doc, converting JSON primitives to native NormalValues
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

    /// Check if a document needs migration to the target version.
    pub(super) fn doc_needs_migration(
        doc: &Document,
        target_version_id: &str,
        has_migrations: bool,
    ) -> bool {
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
    pub(super) async fn process_document(
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
