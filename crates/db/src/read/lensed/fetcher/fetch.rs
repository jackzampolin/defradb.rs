//! DocFetcher trait method implementations.

use bytes::Bytes;
use document::Document;
use query::runner::FetchByIdsResult;
use storage::corekv::Store;
use tracing::{debug, trace};

use crate::collection::loader::get_collection_with_lazy_load;

use super::LensedDocFetcher;

impl<S: Store + 'static> LensedDocFetcher<S> {
    pub(super) async fn get_all_impl(
        &self,
        collection_name: &str,
    ) -> query::error::Result<Vec<Document>> {
        let (collection, datastore, systemstore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        // Check if collection has migrations by loading full version history (matching Go)
        let (_, has_migrations) = self.load_versions_and_check_migrations(&collection).await?;
        let target_version_id = &collection.schema().version_id;

        if has_migrations {
            debug!(
                collection = %collection_name,
                version_id = %target_version_id,
                "Collection has migrations registered (full history check)"
            );
        }

        let docs = collection
            .get_all_with_datastore(&datastore, &systemstore)
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

        if needs_migration_count > 0 {
            self.defer_full_scan_write_back(collection_name, false)
                .await;
        }

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

    pub(super) async fn get_all_with_deleted_impl(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> query::error::Result<Vec<(Document, bool)>> {
        let (collection, datastore, systemstore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        // Check if collection has migrations by loading full version history (matching Go)
        let (_, has_migrations) = self.load_versions_and_check_migrations(&collection).await?;
        let target_version_id = &collection.schema().version_id;

        let docs_with_status = collection
            .get_all_with_datastore_include_deleted(&datastore, &systemstore, show_deleted)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        let needs_migration_count = docs_with_status
            .iter()
            .filter(|(doc, _)| Self::doc_needs_migration(doc, target_version_id, has_migrations))
            .count();
        if needs_migration_count > 0 {
            self.defer_full_scan_write_back(collection_name, show_deleted)
                .await;
        }

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

    pub(super) async fn get_by_ids_impl(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> query::error::Result<FetchByIdsResult> {
        let (collection, datastore, systemstore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        // Check if collection has migrations by loading full version history (matching Go)
        let (_, has_migrations) = self.load_versions_and_check_migrations(&collection).await?;
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
                .get_by_doc_id(&datastore, &systemstore, &doc_id)
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

    pub(super) async fn get_by_field_value_impl(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> query::error::Result<Vec<Document>> {
        let (collection, datastore, systemstore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        // Check if collection has migrations by loading full version history (matching Go)
        let (_, has_migrations) = self.load_versions_and_check_migrations(&collection).await?;
        let target_version_id = &collection.schema().version_id;

        let all_docs = collection
            .get_all_with_datastore(&datastore, &systemstore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        // Count documents needing migration for logging
        let needs_migration_count = all_docs
            .iter()
            .filter(|doc| Self::doc_needs_migration(doc, target_version_id, has_migrations))
            .count();

        trace!(
            collection = %collection_name,
            field = %field_name,
            value = %value,
            candidates = all_docs.len(),
            needs_migration = needs_migration_count,
            has_migrations = has_migrations,
            "Fetched documents by field value"
        );

        if needs_migration_count > 0 {
            self.defer_full_scan_write_back(collection_name, false)
                .await;
        }

        // Migrations may create or change the filtered field, so apply them
        // before evaluating the field value.
        let mut processed_docs = Vec::new();
        for doc in all_docs {
            let processed = self
                .process_document(doc, &collection, &datastore, has_migrations)
                .await?;
            let is_match = processed
                .get(field_name)
                .and_then(|v| v.as_str())
                .is_some_and(|field_value| field_value == value);
            if is_match {
                processed_docs.push(processed);
            }
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

    pub(super) async fn get_view_cache_items_impl(
        &self,
        collection_id: u32,
    ) -> query::error::Result<Vec<Bytes>> {
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
