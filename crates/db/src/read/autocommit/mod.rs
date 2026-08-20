//! Auto-committing document fetcher for non-transactional queries.
//!
//! This fetcher wraps a database and automatically creates and commits
//! a read-only transaction for each query operation. This enables queries
//! without explicit transaction management while still providing proper
//! transactional semantics.

use async_lock::Mutex as TokioMutex;
use async_trait::async_trait;
use document::Document;
use query::doc_stream::DocStream;
use query::runner::{DocFetcher, FetchByIdsResult};
use std::sync::Arc;
use storage::corekv::{IterOptions, Store};
use tracing::warn;

use crate::collection::stream::CollectionDocStream;
use crate::database::DB;
use crate::read::versioned::VersionedFetcher;
use crate::txn::DbTxn;

/// Document fetcher that auto-commits transactions for each operation.
///
/// This is useful for queries that don't need explicit transaction control.
/// Each operation creates a new read-only transaction, performs the query,
/// and commits (or discards on error).
pub struct AutoCommitFetcher<S: Store> {
    db: Arc<DB<S>>,
}

impl<S: Store> AutoCommitFetcher<S> {
    /// Create a new auto-committing fetcher wrapping the given database.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self { db }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocFetcher for AutoCommitFetcher<S> {
    async fn get_all(&self, collection_name: &str) -> query::error::Result<Vec<Document>> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a read-only transaction
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Get the datastore
        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        // Execute the query
        let result = collection
            .get_all_with_datastore(&datastore, &systemstore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)));

        // Discard the read-only transaction (no changes to commit)
        if let Err(e) = txn.discard() {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to discard read-only transaction after get_all"
            );
        }

        result
    }

    async fn get_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> query::error::Result<Vec<(Document, bool)>> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a read-only transaction
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Get the datastore
        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        // Execute the query with deletion status
        let result = collection
            .get_all_with_datastore_include_deleted(&datastore, &systemstore, show_deleted)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)));

        // Discard the read-only transaction (no changes to commit)
        if let Err(e) = txn.discard() {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to discard read-only transaction after get_all_with_deleted"
            );
        }

        result
    }

    async fn stream_by_doc_short_ids(
        &self,
        collection_name: &str,
        doc_short_ids: &[u64],
        show_deleted: bool,
    ) -> query::error::Result<Box<dyn DocStream>> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // The read transaction must outlive the stream, so it is handed over
        // rather than dropped here, exactly as the full-scan stream does.
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;
        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        Ok(Box::new(AutoCommitDocStream {
            inner: Some(Box::new(crate::collection::stream::ShortIdDocStream::new(
                collection,
                datastore,
                systemstore,
                doc_short_ids.to_vec(),
                show_deleted,
            ))),
            txn: std::sync::Mutex::new(Some(txn)),
        }))
    }

    async fn stream_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> query::error::Result<Box<dyn DocStream>> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a read-only transaction. Unlike the other methods here, this
        // one must stay open for the stream's lifetime, so it is handed to
        // AutoCommitDocStream instead of being discarded before returning.
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        let prefix = collection.collection_key_prefix();
        let prefix_len = prefix.len();
        let opts = IterOptions::new().with_prefix(prefix);
        let iter = datastore
            .iterator(opts)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        let inner = CollectionDocStream::new(
            collection,
            datastore,
            systemstore,
            iter,
            prefix_len,
            show_deleted,
        );

        Ok(Box::new(AutoCommitDocStream {
            inner: Some(Box::new(inner)),
            txn: std::sync::Mutex::new(Some(txn)),
        }))
    }

    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> query::error::Result<FetchByIdsResult> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a read-only transaction
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Get the datastore
        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        // Fetch documents
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
                None => missing_ids.push(id_str.clone()),
            }
        }

        // Discard the read-only transaction
        if let Err(e) = txn.discard() {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to discard read-only transaction after get_by_ids"
            );
        }

        if !missing_ids.is_empty() {
            warn!(
                collection = %collection_name,
                requested_count = doc_ids.len(),
                found_count = docs.len(),
                missing_count = missing_ids.len(),
                missing_ids = ?missing_ids,
                "Some explicitly requested documents were not found"
            );
        }

        Ok(FetchByIdsResult::partial(docs, missing_ids))
    }

    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> query::error::Result<Vec<Document>> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a read-only transaction
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Get the datastore
        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
        })?;

        // Get all documents and filter by field value.
        // This is a fallback implementation - index-based lookup can be added later.
        let all_docs = collection
            .get_all_with_datastore(&datastore, &systemstore)
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

        // Discard the read-only transaction
        if let Err(e) = txn.discard() {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to discard read-only transaction after get_by_field_value"
            );
        }

        Ok(matching_docs)
    }

    async fn get_document_at_cid(
        &self,
        cid: &str,
        expected_doc_id: Option<&str>,
        caller_identity: Option<&identity::Did>,
    ) -> query::error::Result<Document> {
        // Create a read-only transaction for the versioned fetcher
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Wrap in Arc<Mutex<Option>> for VersionedFetcher
        let txn_holder: Arc<TokioMutex<Option<DbTxn<S>>>> = Arc::new(TokioMutex::new(Some(txn)));

        let versioned_fetcher =
            VersionedFetcher::with_kms(txn_holder.clone(), self.db.kms(), caller_identity.cloned());
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
        caller_identity: Option<&identity::Did>,
    ) -> query::error::Result<Vec<Document>> {
        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let txn_holder: Arc<TokioMutex<Option<DbTxn<S>>>> = Arc::new(TokioMutex::new(Some(txn)));

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

/// Pulls documents from storage, owning the read-only transaction opened for
/// it (auto-commit fetchers open one per operation instead of reusing a
/// caller-managed one).
///
/// `release_read_txn` drops the inner stream first so its `NamespaceView`s
/// give up their `Arc<SharedTxn>` clones, then discards the transaction -
/// `BasicTxn::discard` requires sole ownership of that Arc. `txn` is wrapped
/// in a `std::sync::Mutex` purely so `DbTxn`'s non-`Sync` callback storage
/// doesn't stop this struct satisfying `DocStream`'s `MaybeSendSync` bound;
/// every access goes through `&mut self` via `get_mut`, so it never actually
/// locks.
struct AutoCommitDocStream<S: Store + 'static> {
    inner: Option<Box<dyn DocStream>>,
    txn: std::sync::Mutex<Option<DbTxn<S>>>,
}

impl<S: Store + 'static> AutoCommitDocStream<S> {
    fn release_read_txn(&mut self) {
        self.inner = None;
        let slot = self
            .txn
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(txn) = slot.take() {
            if let Err(e) = txn.discard() {
                warn!(error = %e, "failed to discard auto-commit stream transaction");
            }
        }
    }

    /// Close the inner stream, then release the transaction whether or not the
    /// close succeeded. Go propagates iterator-close errors from its
    /// exhaustion path (`fetcher/prefix.go`, `fetcher/multi.go`); the release
    /// is unconditional because a leaked read transaction is worse than a
    /// swallowed cleanup error.
    async fn close_inner_then_release(&mut self) -> query::error::Result<()> {
        let closed = match self.inner.take() {
            Some(mut inner) => inner.close().await,
            None => Ok(()),
        };
        self.release_read_txn();
        closed
    }
}

impl<S: Store + 'static> Drop for AutoCommitDocStream<S> {
    fn drop(&mut self) {
        self.release_read_txn();
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocStream for AutoCommitDocStream<S> {
    async fn next(&mut self) -> query::error::Result<Option<(Document, bool)>> {
        let pulled = match self.inner.as_mut() {
            Some(inner) => inner.next().await?,
            None => None,
        };
        if pulled.is_none() {
            self.close_inner_then_release().await?;
        }
        Ok(pulled)
    }

    async fn close(&mut self) -> query::error::Result<()> {
        self.close_inner_then_release().await
    }
}

#[cfg(test)]
mod tests;
