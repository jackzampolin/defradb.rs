//! Migration context loading and document migration logic.

use std::collections::HashMap;

use datastore::NamespaceView;
use document::Document;
use lens::{build_targeted_history, CollectionHistoryLink, Lens, LensDoc, TargetedHistoryLink};
use storage::corekv::Store;
use tracing::{debug, trace, warn};

use crate::collection::Collection;
use crate::migration::helpers::{cache_migrated_document, lens_doc_to_document};
use crate::schema_loader::get_collections_by_collection_id;

use super::LensedAutoCommitFetcher;

impl<S: Store> LensedAutoCommitFetcher<S> {
    /// Load migration context for a collection, using cache when available.
    ///
    /// Positive results are cached per collection and target version. Negative
    /// results are reloaded because a transform can be registered in place.
    pub(super) async fn load_migration_context(
        &self,
        collection: &Collection,
    ) -> query::error::Result<(bool, Option<HashMap<String, TargetedHistoryLink>>)> {
        let collection_id = collection.schema().collection_id.clone();
        let target_version_id = &collection.schema().version_id;

        // Check if cached entry is still valid: the cache key includes the target
        // version ID so that when the active collection version changes (via
        // set_active_collection_version or patch_collection), the stale entry is
        // bypassed and a fresh migration context is computed.
        let cache_key = format!("{}:{}", collection_id, target_version_id);

        if let Ok(cache) = self.migration_cache.lock() {
            if let Some(cached) = cache.get(&cache_key) {
                if cached.0 {
                    return Ok(cached.clone());
                }
            }
        }

        let history = self.load_collection_history(collection).await.ok();

        let has_migrations = history
            .as_ref()
            .is_some_and(|h| h.values().any(|link| link.transform.is_some()));

        let result = (has_migrations, if has_migrations { history } else { None });

        // A transform can be registered without changing the active version ID, so a negative
        // result must not be cached. Positive entries remain version-scoped.
        if has_migrations {
            if let Ok(mut cache) = self.migration_cache.lock() {
                cache.insert(cache_key, result.clone());
            }
        }

        Ok(result)
    }

    /// Check if a document needs migration.
    pub(super) fn doc_needs_migration(
        doc: &Document,
        target_version_id: &str,
        has_migrations: bool,
    ) -> bool {
        if !has_migrations {
            return false;
        }
        doc.needs_migration(target_version_id)
    }

    /// Convert a Document to a LensDoc.
    pub(super) fn doc_to_lens_doc(doc: &Document) -> Option<LensDoc> {
        let map = doc.to_map().ok()?;
        let mut lens_doc = LensDoc::new();
        for (key, value) in map {
            lens_doc.insert(key, value);
        }
        Some(lens_doc)
    }

    /// Build collection history from versions.
    pub(super) fn build_collection_history(
        versions: &[schema::CollectionVersion],
        target_version_id: &str,
    ) -> Option<HashMap<String, TargetedHistoryLink>> {
        debug!(
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
                debug!(
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

        debug!(
            full_history_size = full_history.len(),
            "Built initial history graph"
        );

        let reverse_links: Vec<(String, String)> = full_history
            .values()
            .flat_map(|link| {
                link.previous
                    .iter()
                    .map(|prev_id| (prev_id.clone(), link.version_id.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();

        debug!(
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

        for (vid, link) in &full_history {
            debug!(
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
            debug!(
                target_version_id = %target_version_id,
                targeted_history_size = result.as_ref().map_or(0, |h| h.len()),
                "Successfully built targeted history"
            );
        }

        result
    }

    /// Load collection history from database.
    pub(super) async fn load_collection_history(
        &self,
        collection: &Collection,
    ) -> query::error::Result<HashMap<String, TargetedHistoryLink>> {
        let collection_id = &collection.schema().collection_id;
        let target_version_id = &collection.schema().version_id;

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

        let _ = txn.discard();

        Self::build_collection_history(&versions, target_version_id).ok_or_else(|| {
            query::error::QueryError::execution(format!(
                "failed to build migration history for collection {}",
                collection_id
            ))
        })
    }

    /// Process a document, applying migration if needed.
    pub(super) async fn process_document(
        &self,
        doc: Document,
        collection: &Collection,
        datastore: &NamespaceView,
        systemstore: &NamespaceView,
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
        debug!(
            doc_id = %doc_id_str,
            from_version = %doc_version,
            to_version = %target_version_id,
            "Document needs migration - starting lens pipeline"
        );

        let history = match preloaded_history {
            Some(h) => h.clone(),
            None => self.load_collection_history(collection).await?,
        };

        if !history.contains_key(&doc_version) {
            warn!(
                doc_id = %doc_id_str,
                doc_version = %doc_version,
                target_version = %target_version_id,
                "Document version is unknown locally; returning it unchanged"
            );
            return Ok(doc);
        }

        let original_lens_doc = Self::doc_to_lens_doc(&doc).ok_or_else(|| {
            query::error::QueryError::execution(format!(
                "failed to convert document {} to LensDoc for migration",
                doc_id_str
            ))
        })?;

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

        let migrated_doc = lens_doc_to_document(migrated_lens_doc, &doc, collection);
        cache_migrated_document(datastore, systemstore, collection, &migrated_doc)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to cache migrated document: {}",
                    e
                ))
            })?;

        Ok(migrated_doc)
    }
}
