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
use query::txn::{TransactionContext, TransactionHandle, TransactionRegistry};
use schema::CollectionVersion;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use storage::corekv::Store;
use tracing::error;

use crate::collection::Collection;
use crate::database::DB;
use crate::doc_fetcher::DbDocFetcher;
use crate::error::{Error, Result};
use crate::txn_context::DbTransactionContext;

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
/// all operations will fail-fast: `get()` and `get_ctx()` return `None`,
/// while `begin()`, `commit()`, and `rollback()` return errors. A poisoned
/// lock indicates a panic and potential data corruption - continuing operation
/// would be unsafe.
pub struct DbTransactionRegistry<S: Store> {
    db: Arc<DB<S>>,
    collections: Arc<HashMap<String, Collection>>,
    transactions: RwLock<HashMap<String, Arc<DbTransactionContext<S>>>>,
    id_counter: AtomicU64,
}

impl<S: Store + 'static> DbTransactionRegistry<S> {
    /// Create a new transaction registry.
    pub fn new(db: Arc<DB<S>>, schema: Vec<CollectionVersion>) -> Self {
        let collections: HashMap<String, Collection> = schema
            .into_iter()
            .map(|cv| (cv.name.clone(), Collection::new(cv)))
            .collect();

        Self {
            db,
            collections: Arc::new(collections),
            transactions: RwLock::new(HashMap::new()),
            id_counter: AtomicU64::new(0),
        }
    }

    /// Get the database instance.
    pub fn db(&self) -> &Arc<DB<S>> {
        &self.db
    }

    /// Get the collection definitions.
    pub fn collections(&self) -> &Arc<HashMap<String, Collection>> {
        &self.collections
    }

    /// Get a collection by name.
    pub fn collection(&self, name: &str) -> Option<&Collection> {
        self.collections.get(name)
    }

    /// Get an existing transaction by ID (for internal use).
    ///
    /// Returns `None` if the transaction doesn't exist or if the lock is poisoned.
    pub fn get_ctx(&self, txn_id: &str) -> Option<Arc<DbTransactionContext<S>>> {
        match self.transactions.read() {
            Ok(guard) => guard.get(txn_id).cloned(),
            Err(poisoned) => {
                error!(
                    txn_id = %txn_id,
                    error = ?poisoned,
                    "Transaction registry lock poisoned - returning None for safety"
                );
                None
            }
        }
    }

    /// Get all documents from a collection within a transaction.
    pub async fn get_all_docs(&self, txn_id: &str, collection_name: &str) -> Result<Vec<Document>> {
        let ctx = self
            .get_ctx(txn_id)
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;

        ctx.doc_fetcher()
            .get_all(collection_name)
            .await
            .map_err(Error::Query)
    }

    /// Get documents by IDs from a collection within a transaction.
    pub async fn get_docs_by_ids(
        &self,
        txn_id: &str,
        collection_name: &str,
        doc_ids: &[String],
    ) -> Result<Vec<Document>> {
        let ctx = self
            .get_ctx(txn_id)
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;

        ctx.doc_fetcher()
            .get_by_ids(collection_name, doc_ids)
            .await
            .map_err(Error::Query)
    }
}

#[async_trait]
impl<S: Store + 'static> TransactionRegistry for DbTransactionRegistry<S> {
    async fn begin(&self, readonly: bool) -> query::error::Result<TransactionHandle> {
        let txn_id = format!("txn-{}", self.id_counter.fetch_add(1, Ordering::SeqCst));

        let db_txn =
            self.db.new_txn(readonly).await.map_err(|e| {
                query::error::QueryError::execution(format!("storage error: {}", e))
            })?;

        let fetcher = Arc::new(DbDocFetcher::new(db_txn, self.collections.clone()));
        let ctx = Arc::new(DbTransactionContext::new(txn_id.clone(), readonly, fetcher));

        self.transactions
            .write()
            .map_err(|_| query::error::QueryError::execution("lock poisoned"))?
            .insert(txn_id.clone(), ctx);

        Ok(TransactionHandle::new(txn_id))
    }

    fn get(&self, handle: &TransactionHandle) -> Option<Arc<dyn TransactionContext>> {
        match self.transactions.read() {
            Ok(guard) => guard
                .get(handle.as_str())
                .cloned()
                .map(|ctx| ctx as Arc<dyn TransactionContext>),
            Err(poisoned) => {
                error!(
                    txn_id = %handle,
                    error = ?poisoned,
                    "Transaction registry lock poisoned - returning None for safety"
                );
                None
            }
        }
    }

    async fn commit(&self, handle: &TransactionHandle) -> query::error::Result<()> {
        let ctx = self
            .transactions
            .write()
            .map_err(|_| {
                query::error::QueryError::execution(format!(
                    "transaction registry lock poisoned during commit of '{}'",
                    handle
                ))
            })?
            .remove(handle.as_str())
            .ok_or_else(|| {
                query::error::QueryError::execution(format!("transaction '{}' not found", handle))
            })?;

        let txn = ctx.take_txn().await.ok_or_else(|| {
            query::error::QueryError::execution(format!(
                "transaction '{}' was already consumed (double commit/rollback?)",
                handle
            ))
        })?;

        txn.force_commit().await.map_err(|e| {
            query::error::QueryError::execution(format!(
                "commit error for transaction '{}': {}",
                handle, e
            ))
        })
    }

    async fn rollback(&self, handle: &TransactionHandle) -> query::error::Result<()> {
        let ctx = self
            .transactions
            .write()
            .map_err(|_| {
                query::error::QueryError::execution(format!(
                    "transaction registry lock poisoned during rollback of '{}'",
                    handle
                ))
            })?
            .remove(handle.as_str())
            .ok_or_else(|| {
                query::error::QueryError::execution(format!("transaction '{}' not found", handle))
            })?;

        let txn = ctx.take_txn().await.ok_or_else(|| {
            query::error::QueryError::execution(format!(
                "transaction '{}' was already consumed (double commit/rollback?)",
                handle
            ))
        })?;

        txn.force_discard().map_err(|e| {
            query::error::QueryError::execution(format!(
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

    #[tokio::test]
    async fn test_begin_transaction() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(false).await.unwrap();
        assert!(txn_id.starts_with("txn-"));
    }

    #[tokio::test]
    async fn test_begin_readonly_transaction() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).unwrap();
        assert!(ctx.is_readonly());
    }

    #[tokio::test]
    async fn test_begin_readwrite_transaction() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(false).await.unwrap();
        let ctx = registry.get(&txn_id).unwrap();
        assert!(!ctx.is_readonly());
    }

    #[tokio::test]
    async fn test_transaction_id_matches() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(false).await.unwrap();
        let ctx = registry.get(&txn_id).unwrap();
        assert_eq!(ctx.id(), txn_id.as_str());
    }

    #[tokio::test]
    async fn test_begin_and_commit() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(false).await.unwrap();
        let result = registry.commit(&txn_id).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_begin_and_rollback() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(false).await.unwrap();
        let result = registry.rollback(&txn_id).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_commit_nonexistent_returns_error() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let nonexistent: TransactionHandle = "nonexistent".parse().unwrap();
        let result = registry.commit(&nonexistent).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_rollback_nonexistent_returns_error() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let nonexistent: TransactionHandle = "nonexistent".parse().unwrap();
        let result = registry.rollback(&nonexistent).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_get_nonexistent_returns_none() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let nonexistent: TransactionHandle = "nonexistent".parse().unwrap();
        assert!(registry.get(&nonexistent).is_none());
    }

    #[tokio::test]
    async fn test_double_commit_returns_error() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(false).await.unwrap();
        registry.commit(&txn_id).await.unwrap();

        let result = registry.commit(&txn_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_double_rollback_returns_error() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(false).await.unwrap();
        registry.rollback(&txn_id).await.unwrap();

        let result = registry.rollback(&txn_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_commit_after_rollback_returns_error() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(false).await.unwrap();
        registry.rollback(&txn_id).await.unwrap();

        let result = registry.commit(&txn_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rollback_after_commit_returns_error() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(false).await.unwrap();
        registry.commit(&txn_id).await.unwrap();

        let result = registry.rollback(&txn_id).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_get_returns_none_after_commit() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(false).await.unwrap();
        assert!(registry.get(&txn_id).is_some());

        registry.commit(&txn_id).await.unwrap();
        assert!(registry.get(&txn_id).is_none());
    }

    #[tokio::test]
    async fn test_get_returns_none_after_rollback() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(false).await.unwrap();
        assert!(registry.get(&txn_id).is_some());

        registry.rollback(&txn_id).await.unwrap();
        assert!(registry.get(&txn_id).is_none());
    }

    #[tokio::test]
    async fn test_doc_fetcher_get_all_empty_collection() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).unwrap();
        let fetcher = ctx.doc_fetcher();

        let docs = fetcher.get_all("Users").await.unwrap();
        assert!(docs.is_empty());

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_doc_fetcher_get_all_unknown_collection() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).unwrap();
        let fetcher = ctx.doc_fetcher();

        let result = fetcher.get_all("NonExistent").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NonExistent"));

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_doc_fetcher_get_by_ids_empty() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).unwrap();
        let fetcher = ctx.doc_fetcher();

        let docs = fetcher.get_by_ids("Users", &[]).await.unwrap();
        assert!(docs.is_empty());

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_doc_fetcher_get_by_ids_invalid_id_returns_error() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).unwrap();
        let fetcher = ctx.doc_fetcher();

        let result = fetcher
            .get_by_ids("Users", &["not-a-valid-docid".to_string()])
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid doc ID"));

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_is_consumed_returns_false_before_take() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get_ctx(&txn_id).unwrap();

        assert!(
            !ctx.is_consumed().await,
            "Transaction should not be consumed before take_txn"
        );

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_is_consumed_returns_true_after_take() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get_ctx(&txn_id).unwrap();

        // Take the transaction
        let _txn = ctx.take_txn().await;

        assert!(
            ctx.is_consumed().await,
            "Transaction should be consumed after take_txn"
        );
    }

    #[tokio::test]
    async fn test_doc_fetcher_after_txn_consumed_returns_error() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get_ctx(&txn_id).unwrap();
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
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db.clone(), test_schema());
        let collection = Collection::new(test_schema().pop().unwrap());

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
        let ctx = registry.get(&txn_id).unwrap();
        let fetcher = ctx.doc_fetcher();

        let docs = fetcher.get_all("Users").await.unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("name").unwrap().as_str(), Some("Alice"));

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_get_by_ids_returns_matching_docs() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db.clone(), test_schema());
        let collection = Collection::new(test_schema().pop().unwrap());

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
        let ctx = registry.get(&txn_id).unwrap();
        let fetcher = ctx.doc_fetcher();

        let docs = fetcher.get_by_ids("Users", &[doc1_id]).await.unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("name").unwrap().as_str(), Some("Alice"));

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_concurrent_transactions() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn1 = registry.begin(true).await.unwrap();
        let txn2 = registry.begin(true).await.unwrap();
        let txn3 = registry.begin(false).await.unwrap();

        assert!(registry.get(&txn1).is_some());
        assert!(registry.get(&txn2).is_some());
        assert!(registry.get(&txn3).is_some());

        assert_ne!(txn1, txn2);
        assert_ne!(txn2, txn3);

        registry.rollback(&txn1).await.unwrap();
        registry.rollback(&txn2).await.unwrap();
        registry.rollback(&txn3).await.unwrap();
    }

    #[tokio::test]
    async fn test_transaction_ids_are_unique() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

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
        let db = Arc::new(DB::new(MemoryStore::new()));
        let collection = Collection::new(test_schema().pop().unwrap());

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
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db.clone(), test_schema());
        let collection = Collection::new(test_schema().pop().unwrap());

        // Start a reader transaction FIRST
        let reader_txn_id = registry.begin(true).await.unwrap();
        let reader_ctx = registry.get(&reader_txn_id).unwrap();
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
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = Arc::new(DbTransactionRegistry::new(db, test_schema()));

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let reg = registry.clone();
                tokio::spawn(async move {
                    let txn_id = reg.begin(true).await.unwrap();
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;

                    let ctx = reg.get(&txn_id);
                    assert!(ctx.is_some(), "Task {} should find its transaction", i);

                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    reg.rollback(&txn_id).await.unwrap();

                    assert!(
                        reg.get(&txn_id).is_none(),
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
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(true).await.unwrap();
        let ctx = registry.get(&txn_id).unwrap();
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
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db.clone(), test_schema());
        let collection = Collection::new(test_schema().pop().unwrap());

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
        let ctx = registry.get(&txn_id).unwrap();
        let fetcher = ctx.doc_fetcher();

        let docs = fetcher
            .get_by_ids("Users", &[existing_id, nonexistent_id])
            .await
            .unwrap();

        assert_eq!(docs.len(), 1, "Should only return existing document");
        assert_eq!(docs[0].get("name").unwrap().as_str(), Some("Exists"));

        registry.rollback(&txn_id).await.unwrap();
    }
}
