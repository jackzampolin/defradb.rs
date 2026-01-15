//! Document mutator for transaction-scoped mutations.

use async_trait::async_trait;
use document::{DocID, Document};
use query::mutator::{CreateResult, DeleteResult, DocMutator, UpdateResult};
use std::sync::Arc;
use storage::corekv::Store;
use tokio::sync::Mutex as TokioMutex;

use crate::collection_snapshot::CollectionSnapshot;
use crate::txn::DbTxn;

/// Document mutator that uses a database transaction.
///
/// This mutator holds a reference to an active transaction and collection
/// snapshot, allowing it to perform mutations within the transaction context.
///
/// # Ownership Model
///
/// The transaction is wrapped in `Arc<TokioMutex<Option<...>>>` because:
/// - `Arc`: Enables the mutator to be cloned and shared across multiple mutation
///   operations within the same transaction
/// - `TokioMutex`: Async-safe interior mutability for concurrent access
/// - `Option`: Enables `take_txn()` to extract the transaction for commit/rollback
///
/// The mutator can share its transaction with `DbDocFetcher` when created via
/// `from_shared_txn()`, allowing both read and write operations within the same
/// transaction context.
///
/// After `take_txn()` is called, all mutator operations will return an error
/// indicating the transaction was consumed. Use `is_consumed()` to check state.
pub struct DbDocMutator<S: Store> {
    txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
    collections: CollectionSnapshot,
}

impl<S: Store> DbDocMutator<S> {
    /// Create a new transaction-scoped document mutator.
    pub fn new(txn: DbTxn<S>, collections: CollectionSnapshot) -> Self {
        Self {
            txn: Arc::new(TokioMutex::new(Some(txn))),
            collections,
        }
    }

    /// Create a mutator that shares a transaction with an existing component.
    ///
    /// This is used by `DbTransactionContext` to create a mutator that shares
    /// the same transaction as the `DbDocFetcher`.
    pub(crate) fn from_shared_txn(
        txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
        collections: CollectionSnapshot,
    ) -> Self {
        Self { txn, collections }
    }

    /// Take the transaction out of the mutator (for commit/rollback).
    ///
    /// After calling this, `is_consumed()` will return `true` and all
    /// mutator operations will return an error.
    pub async fn take_txn(&self) -> Option<DbTxn<S>> {
        self.txn.lock().await.take()
    }

    /// Check if the transaction has been consumed (via `take_txn()`).
    ///
    /// Returns `true` if `take_txn()` was called and the transaction is
    /// no longer available for mutations.
    pub async fn is_consumed(&self) -> bool {
        self.txn.lock().await.is_none()
    }
}

#[async_trait]
impl<S: Store + 'static> DocMutator for DbDocMutator<S> {
    async fn create(&self, collection_name: &str, mut doc: Document) -> query::error::Result<CreateResult> {
        let collection = self
            .collections
            .get(collection_name)
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Generate document ID if not present
        if doc.id().is_none() {
            doc.generate_and_set_doc_id().map_err(|e| {
                query::error::QueryError::execution(format!("failed to generate DocID: {}", e))
            })?;
        }

        let doc_id = doc.id().cloned().ok_or_else(|| {
            query::error::QueryError::execution("document should have ID after generation")
        })?;

        // Extract the datastore while holding the lock, then release the lock
        // before awaiting. The datastore is Send + Sync so this is safe.
        let datastore = {
            let txn_guard = self.txn.lock().await;
            let db_txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction already consumed")
            })?;
            db_txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?
        };

        collection
            .create_with_datastore(&datastore, &doc)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("create error: {}", e)))?;

        Ok(CreateResult::new(doc_id, doc))
    }

    async fn update(&self, collection_name: &str, doc: Document) -> query::error::Result<UpdateResult> {
        let collection = self
            .collections
            .get(collection_name)
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Extract the datastore while holding the lock
        let datastore = {
            let txn_guard = self.txn.lock().await;
            let db_txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction already consumed")
            })?;
            db_txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?
        };

        collection
            .update_with_datastore(&datastore, &doc)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("update error: {}", e)))?;

        // Count modified fields (for now, return the total field count)
        let fields_modified = doc.values().len();

        Ok(UpdateResult::new(doc, fields_modified))
    }

    async fn delete(&self, collection_name: &str, doc_id: &DocID) -> query::error::Result<DeleteResult> {
        let collection = self
            .collections
            .get(collection_name)
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Extract the datastore while holding the lock
        let datastore = {
            let txn_guard = self.txn.lock().await;
            let db_txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction already consumed")
            })?;
            db_txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?
        };

        let existed = collection
            .delete_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("delete error: {}", e)))?;

        Ok(DeleteResult::new(doc_id.clone(), existed))
    }

    async fn exists(&self, collection_name: &str, doc_id: &DocID) -> query::error::Result<bool> {
        let collection = self
            .collections
            .get(collection_name)
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Extract the datastore while holding the lock
        let datastore = {
            let txn_guard = self.txn.lock().await;
            let db_txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction already consumed")
            })?;
            db_txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?
        };

        collection
            .exists_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("exists error: {}", e)))
    }

    async fn get_for_update(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<Option<Document>> {
        let collection = self
            .collections
            .get(collection_name)
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Extract the datastore while holding the lock
        let datastore = {
            let txn_guard = self.txn.lock().await;
            let db_txn = txn_guard.as_ref().ok_or_else(|| {
                query::error::QueryError::execution("transaction already consumed")
            })?;
            db_txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?
        };

        collection
            .get_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("get_for_update error: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Collection;
    use crate::database::DB;
    use document::NormalValue;
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use std::collections::HashMap;
    use storage::backends::MemoryStore;

    fn test_collections() -> CollectionSnapshot {
        let fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ];
        let col = Collection::new(CollectionVersion::new("Users", "v1", "col-users", fields));

        let mut map = HashMap::new();
        map.insert("Users".to_string(), col);
        CollectionSnapshot::new(map)
    }

    #[tokio::test]
    async fn test_create_document() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let collections = test_collections();

        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn, collections.clone());

        // Create a document
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));

        let result = mutator.create("Users", doc).await.unwrap();
        assert!(!result.doc_id.to_string().is_empty());
        assert_eq!(
            result.document.get("name").and_then(|v| v.as_str()),
            Some("Alice")
        );

        // Commit the transaction
        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        // Verify the document was persisted
        let txn = db.new_txn(true).await.unwrap();
        let read_mutator = DbDocMutator::new(txn, collections);
        let exists = read_mutator.exists("Users", &result.doc_id).await.unwrap();
        assert!(exists);
    }

    #[tokio::test]
    async fn test_delete_document() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let collections = test_collections();

        // First create a document
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn, collections.clone());

        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Bob".to_string()));
        let result = mutator.create("Users", doc).await.unwrap();
        let doc_id = result.doc_id.clone();

        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        // Now delete it
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn, collections.clone());

        let delete_result = mutator.delete("Users", &doc_id).await.unwrap();
        assert!(delete_result.existed);

        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        // Verify it's gone
        let txn = db.new_txn(true).await.unwrap();
        let mutator = DbDocMutator::new(txn, collections);
        let exists = mutator.exists("Users", &doc_id).await.unwrap();
        assert!(!exists);
    }

    #[tokio::test]
    async fn test_update_document() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let collections = test_collections();

        // First create a document
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn, collections.clone());

        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Charlie".to_string()));
        doc.set("age", NormalValue::Int(25));
        let result = mutator.create("Users", doc).await.unwrap();
        let doc_id = result.doc_id.clone();

        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        // Now update it
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn, collections.clone());

        let mut updated_doc = Document::with_id(doc_id.clone());
        updated_doc.set("name", NormalValue::String("Charles".to_string()));
        updated_doc.set("age", NormalValue::Int(26));

        let update_result = mutator.update("Users", updated_doc).await.unwrap();
        assert!(update_result.fields_modified > 0);

        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        // Verify the update
        let txn = db.new_txn(true).await.unwrap();
        let mutator = DbDocMutator::new(txn, collections);
        let fetched = mutator.get_for_update("Users", &doc_id).await.unwrap();
        assert!(fetched.is_some());
        assert_eq!(
            fetched.unwrap().get("name").and_then(|v| v.as_str()),
            Some("Charles")
        );
    }

    #[tokio::test]
    async fn test_get_for_update() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let collections = test_collections();

        // First create a document
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn, collections.clone());

        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Diana".to_string()));
        let result = mutator.create("Users", doc).await.unwrap();
        let doc_id = result.doc_id.clone();

        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        // Get for update
        let txn = db.new_txn(true).await.unwrap();
        let mutator = DbDocMutator::new(txn, collections);

        let fetched = mutator.get_for_update("Users", &doc_id).await.unwrap();
        assert!(fetched.is_some());
        assert_eq!(
            fetched.unwrap().get("name").and_then(|v| v.as_str()),
            Some("Diana")
        );
    }

    #[tokio::test]
    async fn test_unknown_collection_returns_error() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let collections = test_collections();

        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn, collections);

        let doc = Document::new();
        let result = mutator.create("NonExistent", doc).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("collection not found"));
    }

    #[tokio::test]
    async fn test_consumed_transaction_returns_error() {
        let store = MemoryStore::new();
        let db = DB::new(store);
        let collections = test_collections();

        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn, collections);

        // Consume the transaction
        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        // Now try to use the mutator
        let doc = Document::new();
        let result = mutator.create("Users", doc).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("transaction already consumed"));
    }
}
