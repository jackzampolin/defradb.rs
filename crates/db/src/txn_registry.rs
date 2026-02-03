//! Transaction registry for query execution.
//!
//! This module provides the `DbTransactionRegistry` which implements the query crate's
//! `TransactionRegistry` trait, enabling transaction-aware query execution.
//!
//! # Architecture
//!
//! ```text
//! query crate                          db crate
//! ───────────                          ────────
//! TransactionRegistry (trait)    <--   DbTransactionRegistry (impl)
//! TransactionContext (trait)     <--   DbTransactionContext (impl)
//! DocFetcher (trait)             <--   DbDocFetcher (impl)
//! ```

use async_trait::async_trait;
use document::Document;
use query::error::TransactionError;
use query::txn::{
    GetTransactionResult, TransactionContext, TransactionHandle, TransactionRegistry,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use storage::corekv::Store;
use tracing::{error, warn};

use crate::collection::Collection;
use crate::database::DB;
use crate::error::{Error, Result};
use crate::lensed_fetcher::LensedDocFetcher;
use crate::txn_context::DbTransactionContext;

/// Result of a stale transaction cleanup operation.
///
/// Provides visibility into both successful cleanups and failures,
/// allowing callers to monitor for resource leaks.
#[derive(Debug, Clone, Default)]
pub struct CleanupResult {
    /// Number of transactions successfully cleaned up.
    pub cleaned: usize,
    /// Transactions that failed to clean up: (transaction_id, error_message).
    pub failed: Vec<(String, String)>,
}

impl CleanupResult {
    /// Returns true if all cleanup operations succeeded.
    pub fn is_complete(&self) -> bool {
        self.failed.is_empty()
    }

    /// Total number of transactions that were attempted to be cleaned.
    pub fn attempted(&self) -> usize {
        self.cleaned + self.failed.len()
    }
}

/// Transaction registry that manages database transactions for query execution.
///
/// Implements `query::TransactionRegistry` to provide transaction lifecycle
/// management to the query executor.
///
/// # Thread Safety
///
/// Uses `std::sync::RwLock` for the transaction map to allow synchronous
/// lookups (required by the trait), and `tokio::sync::Mutex` for the
/// underlying transaction to support async document fetching operations.
///
/// # Error Handling
///
/// If the internal lock becomes poisoned (due to a panic in another thread),
/// all operations will fail-fast: `get()` returns `LockPoisoned`, `get_ctx()`
/// returns an error, and `begin()`, `commit()`, and `rollback()` return errors.
/// A poisoned lock indicates a panic and potential data corruption - continuing
/// operation would be unsafe.
pub struct DbTransactionRegistry<S: Store> {
    db: Arc<DB<S>>,
    transactions: RwLock<HashMap<String, Arc<DbTransactionContext<S>>>>,
    id_counter: AtomicU64,
}

impl<S: Store + 'static> DbTransactionRegistry<S> {
    /// Create a new transaction registry.
    ///
    /// Collections are sourced from the DB's collection cache.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self {
            db,
            transactions: RwLock::new(HashMap::new()),
            id_counter: AtomicU64::new(0),
        }
    }

    /// Get the database instance.
    pub fn db(&self) -> &Arc<DB<S>> {
        &self.db
    }

    /// Get all collection names from the DB.
    ///
    /// Uses the process-wide cache. For transaction-scoped access,
    /// use the transaction's collection cache directly.
    pub fn collection_names(&self) -> Result<Vec<String>> {
        self.db.list_collections()
    }

    /// Get a collection by name from the DB.
    ///
    /// Uses the process-wide cache. For transaction-scoped access,
    /// use the transaction's collection cache directly.
    pub fn collection(&self, name: &str) -> Result<Option<Collection>> {
        self.db.get_collection(name)
    }

    /// Get an existing transaction by ID (for internal use).
    ///
    /// Returns `Ok(None)` if the transaction doesn't exist.
    /// Returns `Err(LockPoisoned)` if the lock is poisoned (indicates a panic elsewhere).
    pub fn get_ctx(&self, txn_id: &str) -> Result<Option<Arc<DbTransactionContext<S>>>> {
        match self.transactions.read() {
            Ok(guard) => Ok(guard.get(txn_id).cloned()),
            Err(poisoned) => {
                error!(
                    txn_id = %txn_id,
                    error = ?poisoned,
                    "Transaction registry lock poisoned - system may be in corrupted state"
                );
                Err(Error::LockPoisoned(format!(
                    "failed to acquire read lock for transaction '{}': a panic occurred elsewhere",
                    txn_id
                )))
            }
        }
    }

    /// Get all documents from a collection within a transaction.
    pub async fn get_all_docs(&self, txn_id: &str, collection_name: &str) -> Result<Vec<Document>> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;

        ctx.doc_fetcher()
            .get_all(collection_name)
            .await
            .map_err(Error::Query)
    }

    /// Get documents by IDs from a collection within a transaction.
    ///
    /// Note: This convenience method returns only the found documents.
    /// For information about missing IDs, use the DocFetcher's get_by_ids directly.
    pub async fn get_docs_by_ids(
        &self,
        txn_id: &str,
        collection_name: &str,
        doc_ids: &[String],
    ) -> Result<Vec<Document>> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;

        ctx.doc_fetcher()
            .get_by_ids(collection_name, doc_ids)
            .await
            .map(|result| result.into_docs())
            .map_err(Error::Query)
    }

    /// Cleanup transactions older than the given duration.
    ///
    /// This method finds all transactions that were created more than `max_age` ago
    /// and rolls them back, freeing resources. This should be called periodically
    /// by a background task to prevent resource leaks from dropped `TransactionGuard`s.
    ///
    /// Returns a `CleanupResult` containing both successfully cleaned transactions
    /// and any failures. Check `result.is_complete()` to verify all cleanups succeeded.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned, indicating a panic elsewhere.
    pub async fn cleanup_stale_transactions(&self, max_age: Duration) -> Result<CleanupResult> {
        let now = std::time::Instant::now();

        // Collect stale transaction IDs while holding the read lock briefly
        let stale_ids: Vec<String> = {
            let guard = self.transactions.read().map_err(|_| {
                Error::LockPoisoned("failed to acquire read lock during cleanup".to_string())
            })?;

            guard
                .iter()
                .filter(|(_, ctx)| now.duration_since(ctx.created_at()) > max_age)
                .map(|(id, _)| id.clone())
                .collect()
        };

        let mut result = CleanupResult::default();
        for txn_id in stale_ids {
            // Remove and rollback each stale transaction
            let ctx = {
                let mut guard = self.transactions.write().map_err(|_| {
                    Error::LockPoisoned("failed to acquire write lock during cleanup".to_string())
                })?;
                guard.remove(&txn_id)
            };

            if let Some(ctx) = ctx {
                warn!(
                    txn_id = %txn_id,
                    age_secs = ?now.duration_since(ctx.created_at()).as_secs(),
                    "Cleaning up stale transaction (leaked TransactionGuard?)"
                );

                // Try to take and discard the transaction
                if let Some(txn) = ctx.take_txn().await {
                    if let Err(e) = txn.force_discard() {
                        error!(
                            txn_id = %txn_id,
                            error = %e,
                            "Failed to discard stale transaction during cleanup"
                        );
                        result.failed.push((txn_id.clone(), e.to_string()));
                    } else {
                        result.cleaned += 1;
                    }
                } else {
                    // Transaction was already consumed (committed/rolled back)
                    result.cleaned += 1;
                }
            }
        }

        Ok(result)
    }

    /// Get the number of active transactions in the registry.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned.
    pub fn active_transaction_count(&self) -> Result<usize> {
        self.transactions
            .read()
            .map(|guard| guard.len())
            .map_err(|_| Error::LockPoisoned("failed to acquire read lock for count".to_string()))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> TransactionRegistry for DbTransactionRegistry<S> {
    async fn begin(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        let txn_id = format!("txn-{}", self.id_counter.fetch_add(1, Ordering::SeqCst));

        let db_txn = self
            .db
            .new_txn(readonly)
            .await
            .map_err(|e| TransactionError::execution(format!("storage error: {}", e)))?;

        // Transaction-scoped collection caching: collections are loaded lazily
        // from the SystemStore on first access within the transaction. Once loaded,
        // the collection metadata is cached for the transaction's duration.
        // Use LensedDocFetcher to support lens migrations within transactions.
        let lens_store = self.db.lens_store().clone();
        let fetcher = Arc::new(LensedDocFetcher::new(db_txn, lens_store));
        let ctx = Arc::new(DbTransactionContext::new(txn_id.clone(), readonly, fetcher));

        self.transactions
            .write()
            .map_err(|_| TransactionError::lock_poisoned("failed to acquire write lock for begin"))?
            .insert(txn_id.clone(), ctx);

        Ok(TransactionHandle::new(txn_id))
    }

    fn get(&self, handle: &TransactionHandle) -> GetTransactionResult {
        match self.transactions.read() {
            Ok(guard) => match guard.get(handle.as_str()).cloned() {
                Some(ctx) => GetTransactionResult::Found(ctx as Arc<dyn TransactionContext>),
                None => GetTransactionResult::NotFound,
            },
            Err(poisoned) => {
                error!(
                    txn_id = %handle,
                    error = ?poisoned,
                    "Transaction registry lock poisoned - system may be in corrupted state"
                );
                GetTransactionResult::LockPoisoned
            }
        }
    }

    async fn commit(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        let ctx = self
            .transactions
            .write()
            .map_err(|_| {
                TransactionError::lock_poisoned(format!(
                    "failed to acquire write lock during commit of '{}'",
                    handle
                ))
            })?
            .remove(handle.as_str())
            .ok_or_else(|| {
                TransactionError::not_found(format!("transaction '{}' not found", handle))
            })?;

        let txn = ctx.take_txn().await.ok_or_else(|| {
            TransactionError::already_finalized(format!(
                "transaction '{}' was already consumed (double commit/rollback?)",
                handle
            ))
        })?;

        txn.force_commit().await.map_err(|e| {
            TransactionError::execution(format!("commit error for transaction '{}': {}", handle, e))
        })
    }

    async fn rollback(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        let ctx = self
            .transactions
            .write()
            .map_err(|_| {
                TransactionError::lock_poisoned(format!(
                    "failed to acquire write lock during rollback of '{}'",
                    handle
                ))
            })?
            .remove(handle.as_str())
            .ok_or_else(|| {
                TransactionError::not_found(format!("transaction '{}' not found", handle))
            })?;

        let txn = ctx.take_txn().await.ok_or_else(|| {
            TransactionError::already_finalized(format!(
                "transaction '{}' was already consumed (double commit/rollback?)",
                handle
            ))
        })?;

        txn.force_discard().map_err(|e| {
            TransactionError::execution(format!(
                "rollback error for transaction '{}': {}",
                handle, e
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use document::NormalValue;
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use storage::backends::MemoryStore;

    fn test_schema() -> Vec<CollectionVersion> {
        vec![CollectionVersion::new(
            "Users",
            "v1",
            "col-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        )]
    }

    /// Create a test DB with collections pre-registered.
    async fn test_db_with_collections() -> Arc<DB<MemoryStore>> {
        let db = Arc::new(DB::new(MemoryStore::new()).unwrap());
        for schema in test_schema() {
            db.create_collection(schema).await.unwrap();
        }
        db
    }

    #[tokio::test]
    async fn test_begin_transaction() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(false).await.unwrap();
        assert!(txn_id.starts_with("txn-"));
    }

    #[tokio::test]
    async fn test_begin_readonly_transaction() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
        assert!(ctx.is_readonly());
    }

    #[tokio::test]
    async fn test_begin_readwrite_transaction() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(false).await.unwrap();
        let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
        assert!(!ctx.is_readonly());
    }

    #[tokio::test]
    async fn test_transaction_id_matches() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(false).await.unwrap();
        let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
        assert_eq!(ctx.id(), txn_id.as_str());
    }

    #[tokio::test]
    async fn test_begin_and_commit() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(false).await.unwrap();
        let result = registry.commit(&txn_id).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_begin_and_rollback() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(false).await.unwrap();
        let result = registry.rollback(&txn_id).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_commit_nonexistent_returns_error() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let nonexistent: TransactionHandle = "nonexistent".parse().unwrap();
        let result = registry.commit(&nonexistent).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_rollback_nonexistent_returns_error() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let nonexistent: TransactionHandle = "nonexistent".parse().unwrap();
        let result = registry.rollback(&nonexistent).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_get_nonexistent_returns_not_found() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let nonexistent: TransactionHandle = "nonexistent".parse().unwrap();
        assert!(matches!(
            registry.get(&nonexistent),
            GetTransactionResult::NotFound
        ));
    }

    #[tokio::test]
    async fn test_double_commit_returns_error() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(false).await.unwrap();
        registry.commit(&txn_id).await.unwrap();

        let result = registry.commit(&txn_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_double_rollback_returns_error() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(false).await.unwrap();
        registry.rollback(&txn_id).await.unwrap();

        let result = registry.rollback(&txn_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_commit_after_rollback_returns_error() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(false).await.unwrap();
        registry.rollback(&txn_id).await.unwrap();

        let result = registry.commit(&txn_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rollback_after_commit_returns_error() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(false).await.unwrap();
        registry.commit(&txn_id).await.unwrap();

        let result = registry.rollback(&txn_id).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_get_returns_not_found_after_commit() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(false).await.unwrap();
        assert!(registry.get(&txn_id).is_found());

        registry.commit(&txn_id).await.unwrap();
        assert!(matches!(
            registry.get(&txn_id),
            GetTransactionResult::NotFound
        ));
    }

    #[tokio::test]
    async fn test_get_returns_not_found_after_rollback() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(false).await.unwrap();
        assert!(registry.get(&txn_id).is_found());

        registry.rollback(&txn_id).await.unwrap();
        assert!(matches!(
            registry.get(&txn_id),
            GetTransactionResult::NotFound
        ));
    }

    #[tokio::test]
    async fn test_doc_fetcher_get_all_empty_collection() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
        let fetcher = ctx.doc_fetcher();

        let docs = fetcher.get_all("Users").await.unwrap();
        assert!(docs.is_empty());

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_doc_fetcher_get_all_unknown_collection() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
        let fetcher = ctx.doc_fetcher();

        let result = fetcher.get_all("NonExistent").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NonExistent"));

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_doc_fetcher_get_by_ids_empty() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
        let fetcher = ctx.doc_fetcher();

        let result = fetcher.get_by_ids("Users", &[]).await.unwrap();
        assert!(result.docs().is_empty());
        assert!(result.missing_ids().is_empty());

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_doc_fetcher_get_by_ids_invalid_id_treated_as_not_found() {
        // Go DefraDB treats invalid doc IDs as "not found" rather than errors.
        // This matches behavior where querying for a non-existent ID returns empty results.
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
        let fetcher = ctx.doc_fetcher();

        let result = fetcher
            .get_by_ids("Users", &["not-a-valid-docid".to_string()])
            .await
            .unwrap();
        // Invalid doc ID is treated as not found, not an error
        assert!(result.docs().is_empty());
        assert_eq!(result.missing_ids(), &["not-a-valid-docid".to_string()]);

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_is_consumed_returns_false_before_take() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get_ctx(&txn_id).unwrap().unwrap();

        assert!(
            !ctx.is_consumed().await,
            "Transaction should not be consumed before take_txn"
        );

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_is_consumed_returns_true_after_take() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get_ctx(&txn_id).unwrap().unwrap();

        // Take the transaction
        let _txn = ctx.take_txn().await;

        assert!(
            ctx.is_consumed().await,
            "Transaction should be consumed after take_txn"
        );
    }

    #[tokio::test]
    async fn test_doc_fetcher_after_txn_consumed_returns_error() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get_ctx(&txn_id).unwrap().unwrap();
        let fetcher = ctx.doc_fetcher();

        // Manually take the transaction to simulate commit/rollback having consumed it
        let _txn = ctx.take_txn().await;

        // Now try to use the fetcher - should fail with "transaction already consumed"
        let result = fetcher.get_all("Users").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("transaction already consumed"));
    }

    #[tokio::test]
    async fn test_transaction_sees_committed_data() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db.clone());
        let collection = db.get_collection("Users").unwrap().unwrap();

        // Write data in a separate transaction
        let write_txn = db.new_txn(false).await.unwrap();
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));
        doc.generate_and_set_doc_id().unwrap();
        collection.create(&write_txn, &doc).await.unwrap();
        write_txn.commit().await.unwrap();

        // Read via registry
        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
        let fetcher = ctx.doc_fetcher();

        let docs = fetcher.get_all("Users").await.unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("name").unwrap().as_str(), Some("Alice"));

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_get_by_ids_returns_matching_docs() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db.clone());
        let collection = db.get_collection("Users").unwrap().unwrap();

        // Create two documents
        let write_txn = db.new_txn(false).await.unwrap();

        let mut doc1 = Document::new();
        doc1.set("name", NormalValue::String("Alice".to_string()));
        doc1.generate_and_set_doc_id().unwrap();
        let doc1_id = doc1.id().unwrap().to_string();
        collection.create(&write_txn, &doc1).await.unwrap();

        let mut doc2 = Document::new();
        doc2.set("name", NormalValue::String("Bob".to_string()));
        doc2.generate_and_set_doc_id().unwrap();
        collection.create(&write_txn, &doc2).await.unwrap();

        write_txn.commit().await.unwrap();

        // Query for just one document
        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
        let fetcher = ctx.doc_fetcher();

        let result = fetcher.get_by_ids("Users", &[doc1_id]).await.unwrap();
        assert_eq!(result.docs().len(), 1);
        assert!(result.missing_ids().is_empty());
        assert_eq!(
            result.docs()[0].get("name").unwrap().as_str(),
            Some("Alice")
        );

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_concurrent_transactions() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn1 = registry.begin(true).await.unwrap();
        let txn2 = registry.begin(true).await.unwrap();
        let txn3 = registry.begin(false).await.unwrap();

        assert!(registry.get(&txn1).is_found());
        assert!(registry.get(&txn2).is_found());
        assert!(registry.get(&txn3).is_found());

        assert_ne!(txn1, txn2);
        assert_ne!(txn2, txn3);

        registry.rollback(&txn1).await.unwrap();
        registry.rollback(&txn2).await.unwrap();
        registry.rollback(&txn3).await.unwrap();
    }

    #[tokio::test]
    async fn test_transaction_ids_are_unique() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let mut ids = Vec::new();
        for _ in 0..10 {
            let txn_id = registry.begin(true).await.unwrap();
            assert!(!ids.contains(&txn_id), "Duplicate ID: {}", txn_id);
            ids.push(txn_id);
        }

        for id in ids {
            registry.rollback(&id).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_rollback_discards_uncommitted_writes() {
        let db = test_db_with_collections().await;
        let collection = db.get_collection("Users").unwrap().unwrap();

        // Write data in a transaction but rollback instead of commit
        let write_txn = db.new_txn(false).await.unwrap();
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("RollbackMe".to_string()));
        doc.set("age", NormalValue::Int(99));
        doc.generate_and_set_doc_id().unwrap();
        collection.create(&write_txn, &doc).await.unwrap();

        write_txn.force_discard().unwrap();

        // Verify data was NOT persisted
        let read_txn = db.new_txn(true).await.unwrap();
        let all_docs = collection.get_all(&read_txn).await.unwrap();
        assert!(
            all_docs.is_empty(),
            "Rolled-back data should not be visible, found {} docs",
            all_docs.len()
        );
        read_txn.force_discard().unwrap();
    }

    #[tokio::test]
    async fn test_transaction_does_not_see_uncommitted_writes() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db.clone());
        let collection = db.get_collection("Users").unwrap().unwrap();

        // Start a reader transaction FIRST
        let reader_txn_id = registry.begin(true).await.unwrap();
        let reader_ctx = registry.get(&reader_txn_id).into_result().unwrap().unwrap();
        let reader_fetcher = reader_ctx.doc_fetcher();

        // Start a writer transaction and write WITHOUT committing
        let write_txn = db.new_txn(false).await.unwrap();
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Uncommitted".to_string()));
        doc.generate_and_set_doc_id().unwrap();
        collection.create(&write_txn, &doc).await.unwrap();

        // Reader should NOT see the uncommitted write
        let docs = reader_fetcher.get_all("Users").await.unwrap();
        assert!(
            docs.is_empty(),
            "Reader should not see uncommitted writes (dirty read protection)"
        );

        write_txn.force_discard().unwrap();
        registry.rollback(&reader_txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_parallel_transaction_operations() {
        let db = test_db_with_collections().await;
        let registry = Arc::new(DbTransactionRegistry::new(db));

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let reg = registry.clone();
                tokio::spawn(async move {
                    let txn_id = reg.begin(true).await.unwrap();
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;

                    assert!(
                        reg.get(&txn_id).is_found(),
                        "Task {} should find its transaction",
                        i
                    );

                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    reg.rollback(&txn_id).await.unwrap();

                    assert!(
                        !reg.get(&txn_id).is_found(),
                        "Task {} transaction should be gone after rollback",
                        i
                    );
                })
            })
            .collect();

        for handle in handles {
            handle.await.expect("Task should complete without panic");
        }
    }

    #[tokio::test]
    async fn test_doc_fetcher_get_by_ids_unknown_collection() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
        let fetcher = ctx.doc_fetcher();

        let result = fetcher
            .get_by_ids("NonExistent", &["some-id".to_string()])
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NonExistent"));

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_get_by_ids_with_nonexistent_valid_id() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db.clone());
        let collection = db.get_collection("Users").unwrap().unwrap();

        // Create one document
        let write_txn = db.new_txn(false).await.unwrap();
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Exists".to_string()));
        doc.generate_and_set_doc_id().unwrap();
        let existing_id = doc.id().unwrap().to_string();
        collection.create(&write_txn, &doc).await.unwrap();
        write_txn.commit().await.unwrap();

        // Create a valid-format ID that doesn't exist
        let mut nonexistent_doc = Document::new();
        nonexistent_doc.set("name", NormalValue::String("Ghost".to_string()));
        nonexistent_doc.generate_and_set_doc_id().unwrap();
        let nonexistent_id = nonexistent_doc.id().unwrap().to_string();

        // Query for both
        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
        let fetcher = ctx.doc_fetcher();

        let result = fetcher
            .get_by_ids("Users", &[existing_id, nonexistent_id.clone()])
            .await
            .unwrap();

        assert_eq!(
            result.docs().len(),
            1,
            "Should only return existing document"
        );
        assert_eq!(
            result.docs()[0].get("name").unwrap().as_str(),
            Some("Exists")
        );

        // Verify missing IDs are reported
        assert_eq!(
            result.missing_ids().len(),
            1,
            "Should report one missing ID"
        );
        assert_eq!(result.missing_ids()[0], nonexistent_id);
        assert!(!result.is_complete(), "Result should not be complete");

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_commit_same_transaction() {
        let db = test_db_with_collections().await;
        let registry = Arc::new(DbTransactionRegistry::new(db));

        let txn_id = registry.begin(false).await.unwrap();

        // Spawn two tasks trying to commit the same transaction
        let reg1 = registry.clone();
        let txn1 = txn_id.clone();
        let handle1 = tokio::spawn(async move { reg1.commit(&txn1).await });

        let reg2 = registry.clone();
        let txn2 = txn_id.clone();
        let handle2 = tokio::spawn(async move { reg2.commit(&txn2).await });

        let (r1, r2) = tokio::join!(handle1, handle2);
        let results = [r1.unwrap(), r2.unwrap()];

        // Exactly one should succeed, one should fail
        let successes = results.iter().filter(|r| r.is_ok()).count();
        let failures = results.iter().filter(|r| r.is_err()).count();
        assert_eq!(successes, 1, "Exactly one commit should succeed");
        assert_eq!(failures, 1, "Exactly one commit should fail");
    }

    #[tokio::test]
    async fn test_concurrent_rollback_same_transaction() {
        let db = test_db_with_collections().await;
        let registry = Arc::new(DbTransactionRegistry::new(db));

        let txn_id = registry.begin(false).await.unwrap();

        // Spawn two tasks trying to rollback the same transaction
        let reg1 = registry.clone();
        let txn1 = txn_id.clone();
        let handle1 = tokio::spawn(async move { reg1.rollback(&txn1).await });

        let reg2 = registry.clone();
        let txn2 = txn_id.clone();
        let handle2 = tokio::spawn(async move { reg2.rollback(&txn2).await });

        let (r1, r2) = tokio::join!(handle1, handle2);
        let results = [r1.unwrap(), r2.unwrap()];

        // Exactly one should succeed, one should fail
        let successes = results.iter().filter(|r| r.is_ok()).count();
        let failures = results.iter().filter(|r| r.is_err()).count();
        assert_eq!(successes, 1, "Exactly one rollback should succeed");
        assert_eq!(failures, 1, "Exactly one rollback should fail");
    }

    #[tokio::test]
    async fn test_concurrent_commit_and_rollback_same_transaction() {
        let db = test_db_with_collections().await;
        let registry = Arc::new(DbTransactionRegistry::new(db));

        let txn_id = registry.begin(false).await.unwrap();

        // Spawn one task trying to commit, another trying to rollback
        let reg1 = registry.clone();
        let txn1 = txn_id.clone();
        let handle1 = tokio::spawn(async move { reg1.commit(&txn1).await });

        let reg2 = registry.clone();
        let txn2 = txn_id.clone();
        let handle2 = tokio::spawn(async move { reg2.rollback(&txn2).await });

        let (r1, r2) = tokio::join!(handle1, handle2);
        let results = [r1.unwrap(), r2.unwrap()];

        // Exactly one should succeed, one should fail
        let successes = results.iter().filter(|r| r.is_ok()).count();
        let failures = results.iter().filter(|r| r.is_err()).count();
        assert_eq!(successes, 1, "Exactly one operation should succeed");
        assert_eq!(failures, 1, "Exactly one operation should fail");
    }

    #[tokio::test]
    async fn test_cleanup_stale_transactions() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        // Begin some transactions
        let _txn1 = registry.begin(true).await.unwrap();
        let _txn2 = registry.begin(false).await.unwrap();

        assert_eq!(registry.active_transaction_count().unwrap(), 2);

        // Wait a bit so transactions become "stale"
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Cleanup with a very short max_age (0 means everything is stale)
        let result = registry
            .cleanup_stale_transactions(std::time::Duration::from_millis(0))
            .await
            .unwrap();

        assert_eq!(result.cleaned, 2, "Should have cleaned up 2 transactions");
        assert!(result.is_complete(), "All cleanups should succeed");
        assert_eq!(
            registry.active_transaction_count().unwrap(),
            0,
            "No transactions should remain"
        );
    }

    #[tokio::test]
    async fn test_cleanup_only_old_transactions() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        // Begin an "old" transaction
        let _old_txn = registry.begin(true).await.unwrap();

        // Wait a bit
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Begin a "new" transaction
        let new_txn = registry.begin(true).await.unwrap();

        assert_eq!(registry.active_transaction_count().unwrap(), 2);

        // Cleanup with max_age that only catches the old transaction
        let result = registry
            .cleanup_stale_transactions(std::time::Duration::from_millis(40))
            .await
            .unwrap();

        assert_eq!(
            result.cleaned, 1,
            "Should have cleaned up 1 old transaction"
        );
        assert!(result.is_complete(), "All cleanups should succeed");
        assert_eq!(
            registry.active_transaction_count().unwrap(),
            1,
            "One new transaction should remain"
        );

        // The new transaction should still be usable
        assert!(registry.get(&new_txn).is_found());
        registry.rollback(&new_txn).await.unwrap();
    }

    #[tokio::test]
    async fn test_active_transaction_count() {
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db);

        assert_eq!(registry.active_transaction_count().unwrap(), 0);

        let txn1 = registry.begin(true).await.unwrap();
        assert_eq!(registry.active_transaction_count().unwrap(), 1);

        let txn2 = registry.begin(false).await.unwrap();
        assert_eq!(registry.active_transaction_count().unwrap(), 2);

        registry.commit(&txn1).await.unwrap();
        assert_eq!(registry.active_transaction_count().unwrap(), 1);

        registry.rollback(&txn2).await.unwrap();
        assert_eq!(registry.active_transaction_count().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_snapshot_isolation_after_external_commit() {
        // This test verifies snapshot isolation: a transaction started before
        // another transaction commits should NOT see the committed data.
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db.clone());
        let collection = db.get_collection("Users").unwrap().unwrap();

        // Step 1: Start reader transaction A FIRST (gets snapshot at this point)
        let reader_txn_id = registry.begin(true).await.unwrap();
        let reader_ctx = registry.get(&reader_txn_id).into_result().unwrap().unwrap();
        let reader_fetcher = reader_ctx.doc_fetcher();

        // Verify initially empty
        let initial_docs = reader_fetcher.get_all("Users").await.unwrap();
        assert!(
            initial_docs.is_empty(),
            "Reader should see empty collection initially"
        );

        // Step 2: In a separate transaction, write and COMMIT data
        let write_txn = db.new_txn(false).await.unwrap();
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("CommittedData".to_string()));
        doc.set("age", NormalValue::Int(42));
        doc.generate_and_set_doc_id().unwrap();
        collection.create(&write_txn, &doc).await.unwrap();
        write_txn.commit().await.unwrap();

        // Step 3: Reader transaction A should STILL see empty (snapshot isolation)
        // because its snapshot was taken before the write committed
        let after_commit_docs = reader_fetcher.get_all("Users").await.unwrap();
        assert!(
            after_commit_docs.is_empty(),
            "Reader should NOT see committed data due to snapshot isolation (found {} docs)",
            after_commit_docs.len()
        );

        registry.rollback(&reader_txn_id).await.unwrap();

        // Step 4: A NEW transaction started after commit SHOULD see the data
        let new_reader_txn_id = registry.begin(true).await.unwrap();
        let new_reader_ctx = registry
            .get(&new_reader_txn_id)
            .into_result()
            .unwrap()
            .unwrap();
        let new_reader_fetcher = new_reader_ctx.doc_fetcher();

        let new_docs = new_reader_fetcher.get_all("Users").await.unwrap();
        assert_eq!(
            new_docs.len(),
            1,
            "New reader should see the committed data"
        );
        assert_eq!(
            new_docs[0].get("name").unwrap().as_str(),
            Some("CommittedData")
        );

        registry.rollback(&new_reader_txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_new_transaction_sees_recently_created_collection() {
        // Test that a transaction started AFTER a collection is created can see that collection
        let db = Arc::new(DB::new(MemoryStore::new()).unwrap());
        let registry = DbTransactionRegistry::new(db.clone());

        // Create collection after registry is created
        db.create_collection(CollectionVersion::new(
            "NewCollection",
            "v1",
            "col-new",
            vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
        ))
        .await
        .unwrap();

        // New transaction should see the collection
        let txn_id = registry.begin(true).await.unwrap();
        let collection_names = registry.collection_names().unwrap();
        assert!(
            collection_names.contains(&"NewCollection".to_string()),
            "New transaction should see recently created collection"
        );

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_collection_snapshot_isolation_during_deletion() {
        // Test snapshot isolation: a transaction that started before a collection
        // is deleted should still be able to query that collection
        let db = test_db_with_collections().await;
        let registry = DbTransactionRegistry::new(db.clone());
        let collection = db.get_collection("Users").unwrap().unwrap();

        // Add some data to the collection first
        {
            let write_txn = db.new_txn(false).await.unwrap();
            let mut doc = Document::new();
            doc.set("name", NormalValue::String("Alice".to_string()));
            doc.set("age", NormalValue::Int(30));
            doc.generate_and_set_doc_id().unwrap();
            collection.create(&write_txn, &doc).await.unwrap();
            write_txn.commit().await.unwrap();
        }

        // Start a transaction BEFORE deletion
        let reader_txn_id = registry.begin(true).await.unwrap();
        let reader_ctx = registry.get(&reader_txn_id).into_result().unwrap().unwrap();
        let reader_fetcher = reader_ctx.doc_fetcher();

        // Verify reader can see the collection with data
        let docs_before = reader_fetcher.get_all("Users").await.unwrap();
        assert_eq!(
            docs_before.len(),
            1,
            "Should see 1 document before deletion"
        );

        // Now delete the collection from the DB
        db.delete_collection("Users").await.unwrap();

        // The reader transaction should STILL be able to query the collection
        // because it has a snapshot from before the deletion
        let docs_after = reader_fetcher.get_all("Users").await.unwrap();
        assert_eq!(
            docs_after.len(),
            1,
            "Reader should still see document after deletion due to snapshot isolation"
        );
        assert_eq!(
            docs_after[0].get("name").unwrap().as_str(),
            Some("Alice"),
            "Should see the same document content"
        );

        // However, the DB should report the collection as gone
        assert!(
            !db.has_collection("Users").unwrap(),
            "DB should report collection as deleted"
        );

        registry.rollback(&reader_txn_id).await.unwrap();

        // A NEW transaction should NOT see the deleted collection
        let new_txn_id = registry.begin(true).await.unwrap();
        let new_ctx = registry.get(&new_txn_id).into_result().unwrap().unwrap();
        let new_fetcher = new_ctx.doc_fetcher();

        let result = new_fetcher.get_all("Users").await;
        assert!(
            result.is_err(),
            "New transaction should not see deleted collection"
        );
        assert!(
            result.unwrap_err().to_string().contains("Users"),
            "Error should mention the collection name"
        );

        registry.rollback(&new_txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_collections_snapshot_is_isolated_from_modifications() {
        // Test that modifying a snapshot does not affect the original cache
        let db = test_db_with_collections().await;

        let snapshot = db.collections_snapshot().unwrap();
        assert!(snapshot.contains("Users"));

        // The snapshot should be an independent copy
        // (This is implicitly tested by the fact that CollectionSnapshot
        // wraps an Arc and doesn't expose mutable methods)

        // Adding a new collection should not affect existing snapshots
        db.create_collection(CollectionVersion::new(
            "Posts",
            "v1",
            "col-posts",
            vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
        ))
        .await
        .unwrap();

        // Original snapshot should still only have Users
        assert_eq!(snapshot.len(), 1);
        assert!(snapshot.contains("Users"));
        assert!(!snapshot.contains("Posts"));

        // New snapshot should have both
        let new_snapshot = db.collections_snapshot().unwrap();
        assert_eq!(new_snapshot.len(), 2);
        assert!(new_snapshot.contains("Users"));
        assert!(new_snapshot.contains("Posts"));
    }
}
