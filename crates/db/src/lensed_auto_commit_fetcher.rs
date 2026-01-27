//! Lensed auto-committing document fetcher.
//!
//! This fetcher combines auto-commit transaction management with lens migrations.
//! Documents are automatically migrated during fetch when migrations are registered.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use document::Document;
use lens::{
    build_targeted_history, CollectionHistoryLink, Lens, LensDoc, TargetedHistoryLink, DOC_ID_FIELD,
};
use query::fetcher::CommitsQueryOptions;
use query::runner::{DocFetcher, FetchByIdsResult};
use storage::corekv::Store;
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, trace};

use crate::collection::Collection;
use crate::commits_fetcher::{CommitsFetcher, CommitsQueryOptions as DbCommitsOptions};
use crate::database::DB;
use crate::schema_loader::get_collections_by_collection_id;
use crate::txn::DbTxn;

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

    /// Check if a collection has migrations registered.
    fn collection_has_migrations(collection: &Collection) -> bool {
        if let Some(ref prev) = collection.schema().previous_version {
            if prev.transform.is_some() {
                return true;
            }
        }
        false
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
    async fn process_document(
        &self,
        doc: Document,
        collection: &Collection,
        has_migrations: bool,
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

        // Load collection history
        let history = self.load_collection_history(collection).await?;

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

#[async_trait]
impl<S: Store + 'static> DocFetcher for LensedAutoCommitFetcher<S> {
    async fn get_all(&self, collection_name: &str) -> query::error::Result<Vec<Document>> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        let has_migrations = Self::collection_has_migrations(&collection);
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

        // Process each document
        let mut processed_docs = Vec::with_capacity(docs.len());
        for doc in docs {
            let processed = self
                .process_document(doc, &collection, has_migrations)
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

        let has_migrations = Self::collection_has_migrations(&collection);

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
            let doc_id = document::DocID::from_string(id_str).map_err(|e| {
                query::error::QueryError::execution(format!("invalid doc ID '{}': {}", id_str, e))
            })?;

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

        // Process documents
        let mut processed_docs = Vec::with_capacity(docs.len());
        for doc in docs {
            let processed = self
                .process_document(doc, &collection, has_migrations)
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

        let has_migrations = Self::collection_has_migrations(&collection);

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

        // Process documents
        let mut processed_docs = Vec::with_capacity(matching_docs.len());
        for doc in matching_docs {
            let processed = self
                .process_document(doc, &collection, has_migrations)
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
        let result = commits_fetcher.fetch_commits(&db_options).await.map_err(|e| {
            query::error::QueryError::execution(format!("commits fetch error: {}", e))
        });

        // Clean up transaction
        if let Some(txn) = txn_holder.lock().await.take() {
            let _ = txn.discard();
        }

        result
    }
}
