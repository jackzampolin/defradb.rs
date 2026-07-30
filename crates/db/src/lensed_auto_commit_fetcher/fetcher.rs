//! DocFetcher trait method implementations (non-index-scan).

use async_lock::Mutex as TokioMutex;
use document::Document;
use query::fetcher::CommitsQueryOptions;
use query::runner::FetchByIdsResult;
use storage::corekv::Store;
use tracing::{debug, trace};

use crate::commits_fetcher::{CommitsFetcher, CommitsQueryOptions as DbCommitsOptions};
use crate::txn::DbTxn;
use crate::versioned_fetcher::VersionedFetcher;

use super::migration::MigrationWriteBack;
use super::LensedAutoCommitFetcher;

type ProcessedDocs = (Vec<Document>, Vec<MigrationWriteBack>);
type ProcessedDocsWithStatus = (Vec<(Document, bool)>, Vec<MigrationWriteBack>);
type ProcessedDocsById = (Vec<Document>, Vec<String>, Vec<MigrationWriteBack>);

impl<S: Store + 'static> LensedAutoCommitFetcher<S> {
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

        let read_result: query::error::Result<ProcessedDocs> = async {
            let docs = collection
                .get_all_with_datastore(&datastore, &systemstore)
                .await
                .map_err(|e| {
                    query::error::QueryError::execution(format!("storage error: {}", e))
                })?;

            trace!(
                collection = %collection_name,
                doc_count = docs.len(),
                "Fetched documents"
            );

            let mut processed_docs = Vec::with_capacity(docs.len());
            let mut write_backs = Vec::new();
            for doc in docs {
                let outcome = self
                    .process_document(doc, &collection, has_migrations, &preloaded_history)
                    .await?;
                if let Some(source_document) = outcome.source_document {
                    write_backs.push(MigrationWriteBack {
                        source_document,
                        migrated_document: outcome.document.clone(),
                        migration_generation,
                    });
                }
                processed_docs.push(outcome.document);
            }
            Ok((processed_docs, write_backs))
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
        let (processed_docs, write_backs) = read_result?;
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

        let read_result: query::error::Result<ProcessedDocsWithStatus> = async {
            let docs_with_status = collection
                .get_all_with_datastore_include_deleted(&datastore, &systemstore, show_deleted)
                .await
                .map_err(|e| {
                    query::error::QueryError::execution(format!("storage error: {}", e))
                })?;

            let mut processed_docs = Vec::with_capacity(docs_with_status.len());
            let mut write_backs = Vec::new();
            for (doc, is_deleted) in docs_with_status {
                let outcome = self
                    .process_document(doc, &collection, has_migrations, &preloaded_history)
                    .await?;
                if let Some(source_document) = outcome.source_document {
                    write_backs.push(MigrationWriteBack {
                        source_document,
                        migrated_document: outcome.document.clone(),
                        migration_generation,
                    });
                }
                processed_docs.push((outcome.document, is_deleted));
            }
            Ok((processed_docs, write_backs))
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
        let (processed_docs, write_backs) = read_result?;
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

        let read_result: query::error::Result<ProcessedDocsById> = async {
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

            let mut processed_docs = Vec::with_capacity(docs.len());
            let mut write_backs = Vec::new();
            for doc in docs {
                let outcome = self
                    .process_document(doc, &collection, has_migrations, &preloaded_history)
                    .await?;
                if let Some(source_document) = outcome.source_document {
                    write_backs.push(MigrationWriteBack {
                        source_document,
                        migrated_document: outcome.document.clone(),
                        migration_generation,
                    });
                }
                processed_docs.push(outcome.document);
            }
            Ok((processed_docs, missing_ids, write_backs))
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
        let (processed_docs, missing_ids, write_backs) = read_result?;
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

        let read_result: query::error::Result<ProcessedDocs> = async {
            let all_docs = collection
                .get_all_with_datastore(&datastore, &systemstore)
                .await
                .map_err(|e| {
                    query::error::QueryError::execution(format!("storage error: {}", e))
                })?;

            let mut processed_docs = Vec::new();
            let mut write_backs = Vec::new();
            for doc in all_docs {
                let outcome = self
                    .process_document(doc, &collection, has_migrations, &preloaded_history)
                    .await?;
                if let Some(source_document) = outcome.source_document {
                    write_backs.push(MigrationWriteBack {
                        source_document,
                        migrated_document: outcome.document.clone(),
                        migration_generation,
                    });
                }
                let is_match = outcome
                    .document
                    .get(field_name)
                    .and_then(|v| v.as_str())
                    .is_some_and(|field_value| field_value == value);
                if is_match {
                    processed_docs.push(outcome.document);
                }
            }
            Ok((processed_docs, write_backs))
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
        let (processed_docs, write_backs) = read_result?;
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
        cid: &str,
        expected_doc_id: Option<&str>,
        caller_identity: Option<&identity::Did>,
    ) -> query::error::Result<Vec<Document>> {
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let txn_holder: std::sync::Arc<TokioMutex<Option<DbTxn<S>>>> =
            std::sync::Arc::new(TokioMutex::new(Some(txn)));

        let versioned_fetcher =
            VersionedFetcher::with_kms(txn_holder.clone(), self.db.kms(), caller_identity.cloned());
        let result = versioned_fetcher
            .get_documents_at_cid(cid, expected_doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()));

        if let Some(txn) = txn_holder.lock().await.take() {
            let _ = txn.discard();
        }

        result
    }

    pub(super) async fn get_view_cache_items_impl(
        &self,
        collection_id: u32,
    ) -> query::error::Result<Vec<Vec<u8>>> {
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
