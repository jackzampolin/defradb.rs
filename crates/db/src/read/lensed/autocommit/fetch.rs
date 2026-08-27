//! DocFetcher trait method implementations (non-index-scan).

use bytes::Bytes;
use std::collections::HashMap;

use async_lock::Mutex as TokioMutex;
use document::Document;
use lens::TargetedHistoryLink;
use query::fetcher::CommitsQueryOptions;
use query::runner::FetchByIdsResult;
use storage::corekv::Store;
use tracing::{debug, trace};

use crate::collection::Collection;
use crate::read::commits::{CommitsFetcher, CommitsQueryOptions as DbCommitsOptions};
use crate::read::versioned::VersionedFetcher;
use crate::txn::DbTxn;

use super::migration::MigrationWriteBack;
use super::LensedAutoCommitFetcher;

impl<S: Store + 'static> LensedAutoCommitFetcher<S> {
    pub(super) async fn process_document_with_bounded_write_back(
        &self,
        doc: Document,
        collection: &Collection,
        migration_generation: u64,
        has_migrations: bool,
        preloaded_history: &Option<HashMap<String, TargetedHistoryLink>>,
        write_backs: &mut Vec<MigrationWriteBack>,
    ) -> query::error::Result<Document> {
        let outcome = self
            .process_document(doc, collection, has_migrations, preloaded_history)
            .await?;

        if self.write_back_migrations {
            if let Some(source_document) = outcome.source_document {
                write_backs.push(MigrationWriteBack {
                    source_document,
                    migrated_document: outcome.document.clone(),
                    migration_generation,
                });
                if write_backs.len() >= self.db.options().migration_write_back_batch_size() {
                    self.persist_migrated_documents(collection, std::mem::take(write_backs))
                        .await?;
                }
            }
        }

        Ok(outcome.document)
    }

    pub(super) async fn get_all_impl(
        &self,
        collection_name: &str,
    ) -> query::error::Result<Vec<Document>> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        let (migration_generation, has_migrations, preloaded_history) =
            self.load_migration_context(&collection).await?;

        let target_version_id = &collection.schema().version_id;

        if has_migrations {
            debug!(
                collection = %collection_name,
                version_id = %target_version_id,
                "Collection has migrations registered"
            );
        }

        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {}", e))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        let read_result = collection
            .get_all_with_datastore(&datastore, &systemstore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)));

        drop(datastore);
        drop(systemstore);
        txn.discard().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to discard lensed read transaction: {}",
                e
            ))
        })?;
        let docs = read_result?;
        trace!(
            collection = %collection_name,
            doc_count = docs.len(),
            "Fetched documents"
        );

        let mut processed_docs = Vec::with_capacity(docs.len());
        let mut write_backs =
            Vec::with_capacity(self.db.options().migration_write_back_batch_size());
        for doc in docs {
            let processed = self
                .process_document_with_bounded_write_back(
                    doc,
                    &collection,
                    migration_generation,
                    has_migrations,
                    &preloaded_history,
                    &mut write_backs,
                )
                .await?;
            processed_docs.push(processed);
        }
        self.persist_migrated_documents(&collection, write_backs)
            .await?;

        if !processed_docs.is_empty() {
            debug!(
                collection = %collection_name,
                returned = processed_docs.len(),
                "Lensed read completed"
            );
        }

        Ok(processed_docs)
    }

    pub(super) async fn get_all_with_deleted_impl(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> query::error::Result<Vec<(Document, bool)>> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        let (migration_generation, has_migrations, preloaded_history) =
            self.load_migration_context(&collection).await?;

        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {}", e))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        let read_result = collection
            .get_all_with_datastore_include_deleted(&datastore, &systemstore, show_deleted)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)));

        drop(datastore);
        drop(systemstore);
        txn.discard().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to discard lensed read transaction: {}",
                e
            ))
        })?;
        let docs_with_status = read_result?;
        let mut processed_docs = Vec::with_capacity(docs_with_status.len());
        let mut write_backs =
            Vec::with_capacity(self.db.options().migration_write_back_batch_size());
        for (doc, is_deleted) in docs_with_status {
            let processed = self
                .process_document_with_bounded_write_back(
                    doc,
                    &collection,
                    migration_generation,
                    has_migrations,
                    &preloaded_history,
                    &mut write_backs,
                )
                .await?;
            processed_docs.push((processed, is_deleted));
        }
        self.persist_migrated_documents(&collection, write_backs)
            .await?;

        Ok(processed_docs)
    }

    pub(super) async fn get_by_ids_impl(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> query::error::Result<FetchByIdsResult> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        let (migration_generation, has_migrations, preloaded_history) =
            self.load_migration_context(&collection).await?;

        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {}", e))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        let read_result: query::error::Result<(Vec<Document>, Vec<String>)> = async {
            let mut docs = Vec::new();
            let mut missing_ids = Vec::new();

            for id_str in doc_ids {
                let doc_id = match document::DocID::from_string(id_str) {
                    Ok(id) => id,
                    Err(_) => {
                        missing_ids.push(id_str.clone());
                        continue;
                    }
                };

                match collection
                    .get_by_doc_id(&datastore, &systemstore, &doc_id)
                    .await
                    .map_err(|e| {
                        query::error::QueryError::execution(format!("storage error: {}", e))
                    })? {
                    Some(doc) => docs.push(doc),
                    None => missing_ids.push(id_str.clone()),
                }
            }

            Ok((docs, missing_ids))
        }
        .await;

        drop(datastore);
        drop(systemstore);
        txn.discard().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to discard lensed read transaction: {}",
                e
            ))
        })?;
        let (docs, missing_ids) = read_result?;
        let mut processed_docs = Vec::with_capacity(docs.len());
        let mut write_backs =
            Vec::with_capacity(self.db.options().migration_write_back_batch_size());
        for doc in docs {
            let processed = self
                .process_document_with_bounded_write_back(
                    doc,
                    &collection,
                    migration_generation,
                    has_migrations,
                    &preloaded_history,
                    &mut write_backs,
                )
                .await?;
            processed_docs.push(processed);
        }
        self.persist_migrated_documents(&collection, write_backs)
            .await?;

        Ok(FetchByIdsResult::partial(processed_docs, missing_ids))
    }

    pub(super) async fn get_by_field_value_impl(
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

        let (migration_generation, has_migrations, preloaded_history) =
            self.load_migration_context(&collection).await?;

        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {}", e))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        let read_result = collection
            .get_all_with_datastore(&datastore, &systemstore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)));

        drop(datastore);
        drop(systemstore);
        txn.discard().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to discard lensed read transaction: {}",
                e
            ))
        })?;
        let all_docs = read_result?;
        let mut processed_docs = Vec::new();
        let mut write_backs =
            Vec::with_capacity(self.db.options().migration_write_back_batch_size());
        for doc in all_docs {
            let processed = self
                .process_document_with_bounded_write_back(
                    doc,
                    &collection,
                    migration_generation,
                    has_migrations,
                    &preloaded_history,
                    &mut write_backs,
                )
                .await?;
            let is_match = processed
                .get(field_name)
                .and_then(|v| v.as_str())
                .is_some_and(|field_value| field_value == value);
            if is_match {
                processed_docs.push(processed);
            }
        }
        self.persist_migrated_documents(&collection, write_backs)
            .await?;

        Ok(processed_docs)
    }

    pub(super) async fn get_commits_impl(
        &self,
        options: &CommitsQueryOptions,
    ) -> query::error::Result<Vec<Document>> {
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let txn_holder: std::sync::Arc<TokioMutex<Option<DbTxn<S>>>> =
            std::sync::Arc::new(TokioMutex::new(Some(txn)));

        let db_options = DbCommitsOptions {
            doc_id: options.doc_id.clone(),
            cid: options.cid.clone(),
            depth: options.depth,
            height_start: options.height_start,
            height_end: options.height_end,
            field_name: options.field_name.clone(),
        };

        let commits_fetcher = CommitsFetcher::new(txn_holder.clone());
        let result = commits_fetcher
            .fetch_commits(&db_options)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("commits fetch error: {}", e))
            });

        if let Some(txn) = txn_holder.lock().await.take() {
            let _ = txn.discard();
        }

        result
    }

    pub(super) async fn get_document_at_cid_impl(
        &self,
        cid: &str,
        expected_doc_id: Option<&str>,
        caller_identity: Option<&identity::Did>,
    ) -> query::error::Result<Document> {
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let txn_holder: std::sync::Arc<TokioMutex<Option<DbTxn<S>>>> =
            std::sync::Arc::new(TokioMutex::new(Some(txn)));

        let versioned_fetcher =
            VersionedFetcher::with_kms(txn_holder.clone(), self.db.kms(), caller_identity.cloned());
        let result = versioned_fetcher
            .get_document_at_cid(cid, expected_doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()));

        if let Some(txn) = txn_holder.lock().await.take() {
            let _ = txn.discard();
        }

        result
    }

    pub(super) async fn get_documents_at_cid_impl(
        &self,
        collection_name: &str,
        cid: &str,
        expected_doc_id: Option<&str>,
        caller_identity: Option<&identity::Did>,
    ) -> query::error::Result<Vec<Document>> {
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let txn_holder: std::sync::Arc<TokioMutex<Option<DbTxn<S>>>> =
            std::sync::Arc::new(TokioMutex::new(Some(txn)));

        let collection =
            crate::collection::loader::get_collection_with_lazy_load(&txn_holder, collection_name)
                .await
                .map(|(collection, _, _)| collection);

        let result = match collection {
            Ok(collection) => {
                let versioned_fetcher = VersionedFetcher::with_kms(
                    txn_holder.clone(),
                    self.db.kms(),
                    caller_identity.cloned(),
                );
                versioned_fetcher
                    .get_documents_at_cid(cid, expected_doc_id, Some(collection.resolved_root_id()))
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))
            }
            Err(e) => Err(e),
        };

        if let Some(txn) = txn_holder.lock().await.take() {
            let _ = txn.discard();
        }

        result
    }

    pub(super) async fn get_view_cache_items_impl(
        &self,
        collection_id: u32,
    ) -> query::error::Result<Vec<Bytes>> {
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

        let _ = txn.discard();

        Ok(items)
    }
}
