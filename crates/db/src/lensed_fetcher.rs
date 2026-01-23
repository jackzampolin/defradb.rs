//! Lensed document fetcher that applies schema migrations.
//!
//! This fetcher wraps an inner fetcher and applies lens transforms to documents
//! that are stored with older schema versions.
//!
//! # Migration Flow
//!
//! When a document is fetched:
//! 1. The fetcher checks if the collection has any registered migrations
//! 2. If migrations exist and the document's schema version differs from
//!    the target version, the document is transformed through the lens pipeline
//! 3. Migrated values are cached in the datastore to avoid re-migration
//!
//! # Current Limitations
//!
//! Per-document schema version tracking is not yet implemented. Documents
//! are currently assumed to be at the current collection version. Full
//! migration support requires storing schema version with each document.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use document::Document;
use lens::{
    build_targeted_history, CollectionHistoryLink, LensDoc, TargetedHistoryLink, TransformStore,
    DOC_ID_FIELD,
};
use query::runner::{DocFetcher, FetchByIdsResult};
use schema::CollectionVersion;
use storage::corekv::Store;
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, trace};

use crate::collection::Collection;
use crate::collection_loader::get_collection_with_lazy_load;
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
    history_cache: tokio::sync::RwLock<HashMap<String, HashMap<String, TargetedHistoryLink>>>,
}

impl<S: Store> LensedDocFetcher<S> {
    /// Create a new lensed document fetcher.
    ///
    /// # Arguments
    ///
    /// * `txn` - The database transaction
    /// * `lens_store` - The lens transform store for applying migrations
    pub(crate) fn new(txn: DbTxn<S>, lens_store: Arc<dyn TransformStore>) -> Self {
        Self {
            txn: Arc::new(TokioMutex::new(Some(txn))),
            lens_store,
            history_cache: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Take the transaction out of the fetcher (for commit/rollback).
    pub(crate) async fn take_txn(&self) -> Option<DbTxn<S>> {
        self.txn.lock().await.take()
    }

    /// Check if the transaction has been consumed.
    pub async fn is_consumed(&self) -> bool {
        self.txn.lock().await.is_none()
    }

    /// Get the shared transaction reference.
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

    /// Build the version history for a collection.
    ///
    /// This traverses the previous_version links to build a map of all known
    /// schema versions for the collection. Returns a targeted history that
    /// links each version to the path toward the target version.
    ///
    /// Note: Currently only builds history from the current version backwards.
    /// Full history building requires loading all versions from the systemstore.
    #[allow(dead_code)]
    fn build_collection_history(
        collection: &CollectionVersion,
    ) -> Option<HashMap<String, TargetedHistoryLink>> {
        let mut full_history: HashMap<String, CollectionHistoryLink> = HashMap::new();

        // Add the current version
        let mut current_link =
            CollectionHistoryLink::new(&collection.version_id, &collection.collection_id);

        // Check if there's a previous version and add transform if present
        if let Some(ref prev) = collection.previous_version {
            current_link = current_link.with_previous(&prev.source_collection_id);
            if let Some(ref transform_id) = prev.transform {
                current_link = current_link.with_transform(transform_id);
            }
        }

        full_history.insert(collection.version_id.clone(), current_link);

        // Note: This is a simplified version that only knows about the current version.
        // Full support requires loading all collection versions from systemstore
        // (GetCollectionsByCollectionID in Go) and building the complete history graph.

        build_targeted_history(&full_history, &collection.version_id)
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
}

#[async_trait]
impl<S: Store + 'static> DocFetcher for LensedDocFetcher<S> {
    async fn get_all(&self, collection_name: &str) -> query::error::Result<Vec<Document>> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        // Check if collection has migrations registered
        let has_migrations = Self::collection_has_migrations(&collection);
        if has_migrations {
            debug!(
                collection = %collection_name,
                version_id = %collection.schema().version_id,
                "Collection has migrations registered"
            );
        }

        let docs = collection
            .get_all_with_datastore(&datastore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        // Log document count for tracing
        trace!(
            collection = %collection_name,
            doc_count = docs.len(),
            has_migrations = has_migrations,
            "Fetched documents"
        );

        // Currently returns docs without transformation.
        // Full migration support requires per-document schema version tracking
        // to determine which documents need migration.
        //
        // When per-document versions are available, the flow will be:
        // 1. For each doc, check doc.schema_version_id vs collection.version_id
        // 2. If different and migrations registered, transform through lens pipeline
        // 3. Cache migrated values in datastore
        Ok(docs)
    }

    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> query::error::Result<FetchByIdsResult> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        let has_migrations = Self::collection_has_migrations(&collection);

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

        trace!(
            collection = %collection_name,
            requested = doc_ids.len(),
            found = docs.len(),
            missing = missing_ids.len(),
            has_migrations = has_migrations,
            "Fetched documents by ID"
        );

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

        let has_migrations = Self::collection_has_migrations(&collection);

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

        trace!(
            collection = %collection_name,
            field = %field_name,
            value = %value,
            matches = matching_docs.len(),
            has_migrations = has_migrations,
            "Fetched documents by field value"
        );

        Ok(matching_docs)
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
