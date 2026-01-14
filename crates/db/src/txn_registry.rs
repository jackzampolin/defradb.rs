//! Transaction registry for query execution.
//!
//! This module provides the `DbTransactionRegistry` which implements the query crate's
//! `TransactionRegistry` trait, enabling transaction-aware query execution.

use document::Document;
use schema::CollectionVersion;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use storage::corekv::Store;
use tokio::sync::RwLock;

use crate::collection::Collection;
use crate::database::DB;
use crate::error::{Error, Result};
use crate::txn::DbTxn;

/// Document fetcher that uses a database transaction.
///
/// This fetcher holds a reference to an active transaction and collection
/// definitions, allowing it to fetch documents within the transaction context.
pub struct TxnDocFetcher<S: Store> {
    /// The database transaction
    txn: Arc<RwLock<Option<DbTxn<S>>>>,
    /// Collection definitions by name (used by registry methods)
    #[allow(dead_code)]
    collections: Arc<HashMap<String, Collection>>,
}

impl<S: Store> TxnDocFetcher<S> {
    /// Create a new transaction-scoped document fetcher.
    fn new(txn: DbTxn<S>, collections: Arc<HashMap<String, Collection>>) -> Self {
        Self {
            txn: Arc::new(RwLock::new(Some(txn))),
            collections,
        }
    }

    /// Take the transaction out of the fetcher (for commit/rollback).
    async fn take_txn(&self) -> Option<DbTxn<S>> {
        self.txn.write().await.take()
    }
}

/// Error type for query operations.
#[derive(Debug, Clone)]
pub struct QueryError(String);

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for QueryError {}

impl From<Error> for QueryError {
    fn from(e: Error) -> Self {
        QueryError(e.to_string())
    }
}

/// Transaction context for query execution.
pub struct DbTransactionContext<S: Store> {
    /// Transaction ID
    id: String,
    /// Whether this is a read-only transaction
    readonly: bool,
    /// The document fetcher for this transaction
    fetcher: Arc<TxnDocFetcher<S>>,
}

impl<S: Store + 'static> DbTransactionContext<S> {
    /// Get the transaction ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Check if this is a read-only transaction.
    pub fn is_readonly(&self) -> bool {
        self.readonly
    }

    /// Get the document fetcher.
    pub fn fetcher(&self) -> Arc<TxnDocFetcher<S>> {
        self.fetcher.clone()
    }

    /// Take the underlying transaction (for commit/rollback).
    pub async fn take_txn(&self) -> Option<DbTxn<S>> {
        self.fetcher.take_txn().await
    }
}

/// Transaction registry that manages database transactions for query execution.
///
/// This registry creates and tracks transactions, providing transaction-scoped
/// document fetchers for query execution.
pub struct DbTransactionRegistry<S: Store> {
    /// The database instance
    db: Arc<DB<S>>,
    /// Collection definitions by name
    collections: Arc<HashMap<String, Collection>>,
    /// Active transactions by ID
    transactions: RwLock<HashMap<String, Arc<DbTransactionContext<S>>>>,
    /// Counter for generating unique transaction IDs
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

    /// Begin a new transaction.
    pub async fn begin(&self, readonly: bool) -> Result<String> {
        let txn_id = format!("txn-{}", self.id_counter.fetch_add(1, Ordering::SeqCst));

        let db_txn = self.db.new_txn(readonly).await?;
        let fetcher = Arc::new(TxnDocFetcher::new(db_txn, self.collections.clone()));

        let ctx = Arc::new(DbTransactionContext {
            id: txn_id.clone(),
            readonly,
            fetcher,
        });

        self.transactions.write().await.insert(txn_id.clone(), ctx);

        Ok(txn_id)
    }

    /// Get an existing transaction by ID.
    pub async fn get(&self, txn_id: &str) -> Option<Arc<DbTransactionContext<S>>> {
        self.transactions.read().await.get(txn_id).cloned()
    }

    /// Commit a transaction.
    pub async fn commit(&self, txn_id: &str) -> Result<()> {
        let ctx = self
            .transactions
            .write()
            .await
            .remove(txn_id)
            .ok_or_else(|| Error::Other(format!("transaction '{}' not found", txn_id)))?;

        let txn = ctx
            .take_txn()
            .await
            .ok_or_else(|| Error::Other("transaction already consumed".into()))?;

        txn.force_commit().await
    }

    /// Rollback a transaction.
    pub async fn rollback(&self, txn_id: &str) -> Result<()> {
        let ctx = self
            .transactions
            .write()
            .await
            .remove(txn_id)
            .ok_or_else(|| Error::Other(format!("transaction '{}' not found", txn_id)))?;

        let txn = ctx
            .take_txn()
            .await
            .ok_or_else(|| Error::Other("transaction already consumed".into()))?;

        txn.force_discard()
    }

    /// Get all documents from a collection within a transaction.
    pub async fn get_all_docs(&self, txn_id: &str, collection_name: &str) -> Result<Vec<Document>> {
        let ctx = self
            .get(txn_id)
            .await
            .ok_or_else(|| Error::Other(format!("transaction '{}' not found", txn_id)))?;

        let collection = self
            .collections
            .get(collection_name)
            .ok_or_else(|| Error::Other(format!("collection '{}' not found", collection_name)))?;

        let txn_guard = ctx.fetcher.txn.read().await;

        let txn = txn_guard
            .as_ref()
            .ok_or_else(|| Error::Other("transaction already consumed".into()))?;

        collection.get_all(txn).await
    }

    /// Get documents by IDs from a collection within a transaction.
    pub async fn get_docs_by_ids(
        &self,
        txn_id: &str,
        collection_name: &str,
        doc_ids: &[String],
    ) -> Result<Vec<Document>> {
        let ctx = self
            .get(txn_id)
            .await
            .ok_or_else(|| Error::Other(format!("transaction '{}' not found", txn_id)))?;

        let collection = self
            .collections
            .get(collection_name)
            .ok_or_else(|| Error::Other(format!("collection '{}' not found", collection_name)))?;

        let txn_guard = ctx.fetcher.txn.read().await;

        let txn = txn_guard
            .as_ref()
            .ok_or_else(|| Error::Other("transaction already consumed".into()))?;

        let mut docs = Vec::new();
        for id_str in doc_ids {
            let doc_id = document::DocID::from_string(id_str)
                .map_err(|e| Error::Other(format!("invalid doc ID '{}': {}", id_str, e)))?;

            if let Some(doc) = collection.get(txn, &doc_id).await? {
                docs.push(doc);
            }
        }

        Ok(docs)
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

        let result = registry.commit("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_returns_none_after_commit() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(false).await.unwrap();
        assert!(registry.get(&txn_id).await.is_some());

        registry.commit(&txn_id).await.unwrap();
        assert!(registry.get(&txn_id).await.is_none());
    }

    #[tokio::test]
    async fn test_get_all_docs_empty_collection() {
        let db = Arc::new(DB::new(MemoryStore::new()));
        let registry = DbTransactionRegistry::new(db, test_schema());

        let txn_id = registry.begin(true).await.unwrap();
        let docs = registry.get_all_docs(&txn_id, "Users").await.unwrap();
        assert!(docs.is_empty());

        registry.rollback(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_transaction_data_isolation() {
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
        let docs = registry.get_all_docs(&txn_id, "Users").await.unwrap();
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

        assert!(registry.get(&txn1).await.is_some());
        assert!(registry.get(&txn2).await.is_some());
        assert!(registry.get(&txn3).await.is_some());

        // Different IDs
        assert_ne!(txn1, txn2);
        assert_ne!(txn2, txn3);

        registry.rollback(&txn1).await.unwrap();
        registry.rollback(&txn2).await.unwrap();
        registry.rollback(&txn3).await.unwrap();
    }
}
