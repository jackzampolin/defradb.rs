//! Document mutator for transaction-scoped mutations.

use async_trait::async_trait;
use document::{DocID, Document};
use query::mutator::{CreateResult, DeleteResult, DocMutator, UpdateResult};
use std::sync::Arc;
use storage::corekv::Store;
use tokio::sync::Mutex as TokioMutex;

use crate::collection_loader::get_collection_with_lazy_load;
use crate::txn::DbTxn;

/// Document mutator that uses a database transaction.
///
/// This mutator holds a reference to an active transaction and uses the
/// transaction's collection cache with lazy loading.
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
///
/// # Collection Access
///
/// Collections are loaded lazily from the SystemStore on first access within
/// the transaction. Once loaded, the collection metadata is cached for the
/// duration of the transaction. Note: This provides transaction-level caching,
/// not true snapshot isolation - if collections are accessed at different times,
/// they reflect the store state at the time of first access.
pub struct DbDocMutator<S: Store> {
    txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
}

impl<S: Store> DbDocMutator<S> {
    /// Create a new transaction-scoped document mutator.
    ///
    /// Collections will be loaded lazily from the transaction's cache.
    pub fn new(txn: DbTxn<S>) -> Self {
        Self {
            txn: Arc::new(TokioMutex::new(Some(txn))),
        }
    }

    /// Create a mutator that shares a transaction with an existing component.
    ///
    /// This is used by `DbTransactionContext` to create a mutator that shares
    /// the same transaction as the `DbDocFetcher`.
    pub(crate) fn from_shared_txn(txn: Arc<TokioMutex<Option<DbTxn<S>>>>) -> Self {
        Self { txn }
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
    async fn create(
        &self,
        collection_name: &str,
        mut doc: Document,
    ) -> query::error::Result<CreateResult> {
        let (collection, datastore) = get_collection_with_lazy_load(&self.txn, collection_name).await?;

        // Generate document ID if not present
        if doc.id().is_none() {
            doc.generate_and_set_doc_id().map_err(|e| {
                query::error::QueryError::execution(format!("failed to generate DocID: {}", e))
            })?;
        }

        let doc_id = doc.id().cloned().ok_or_else(|| {
            query::error::QueryError::execution("document should have ID after generation")
        })?;

        collection
            .create_with_datastore(&datastore, &doc)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("create error: {}", e)))?;

        Ok(CreateResult::new(doc_id, doc))
    }

    async fn update(
        &self,
        collection_name: &str,
        doc: Document,
    ) -> query::error::Result<UpdateResult> {
        let (collection, datastore) = get_collection_with_lazy_load(&self.txn, collection_name).await?;

        collection
            .update_with_datastore(&datastore, &doc)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("update error: {}", e)))?;

        // Count modified fields (for now, return the total field count)
        let fields_modified = doc.values().len();

        Ok(UpdateResult::new(doc, fields_modified))
    }

    async fn delete(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        let (collection, datastore) = get_collection_with_lazy_load(&self.txn, collection_name).await?;

        let existed = collection
            .delete_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("delete error: {}", e)))?;

        Ok(DeleteResult::new(doc_id.clone(), existed))
    }

    async fn exists(&self, collection_name: &str, doc_id: &DocID) -> query::error::Result<bool> {
        let (collection, datastore) = get_collection_with_lazy_load(&self.txn, collection_name).await?;

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
        let (collection, datastore) = get_collection_with_lazy_load(&self.txn, collection_name).await?;

        collection
            .get_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("get_for_update error: {}", e))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::DB;
    use document::NormalValue;
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use storage::backends::MemoryStore;

    fn test_schema() -> CollectionVersion {
        let fields = vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ];
        CollectionVersion::new("Users", "v1", "col-users", fields)
    }

    async fn setup_db_with_collection() -> DB<MemoryStore> {
        let store = MemoryStore::new();
        let db = DB::new(store);
        db.create_collection(test_schema()).await.unwrap();
        db
    }

    #[tokio::test]
    async fn test_create_document() {
        let db = setup_db_with_collection().await;

        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn);

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
        let read_mutator = DbDocMutator::new(txn);
        let exists = read_mutator.exists("Users", &result.doc_id).await.unwrap();
        assert!(exists);
    }

    #[tokio::test]
    async fn test_delete_document() {
        let db = setup_db_with_collection().await;

        // First create a document
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn);

        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Bob".to_string()));
        let result = mutator.create("Users", doc).await.unwrap();
        let doc_id = result.doc_id.clone();

        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        // Now delete it
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn);

        let delete_result = mutator.delete("Users", &doc_id).await.unwrap();
        assert!(delete_result.existed);

        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        // Verify it's gone
        let txn = db.new_txn(true).await.unwrap();
        let mutator = DbDocMutator::new(txn);
        let exists = mutator.exists("Users", &doc_id).await.unwrap();
        assert!(!exists);
    }

    #[tokio::test]
    async fn test_update_document() {
        let db = setup_db_with_collection().await;

        // First create a document
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn);

        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Charlie".to_string()));
        doc.set("age", NormalValue::Int(25));
        let result = mutator.create("Users", doc).await.unwrap();
        let doc_id = result.doc_id.clone();

        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        // Now update it
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn);

        let mut updated_doc = Document::with_id(doc_id.clone());
        updated_doc.set("name", NormalValue::String("Charles".to_string()));
        updated_doc.set("age", NormalValue::Int(26));

        let update_result = mutator.update("Users", updated_doc).await.unwrap();
        assert!(update_result.fields_modified > 0);

        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        // Verify the update
        let txn = db.new_txn(true).await.unwrap();
        let mutator = DbDocMutator::new(txn);
        let fetched = mutator.get_for_update("Users", &doc_id).await.unwrap();
        assert!(fetched.is_some());
        assert_eq!(
            fetched.unwrap().get("name").and_then(|v| v.as_str()),
            Some("Charles")
        );
    }

    #[tokio::test]
    async fn test_get_for_update() {
        let db = setup_db_with_collection().await;

        // First create a document
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn);

        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Diana".to_string()));
        let result = mutator.create("Users", doc).await.unwrap();
        let doc_id = result.doc_id.clone();

        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        // Get for update
        let txn = db.new_txn(true).await.unwrap();
        let mutator = DbDocMutator::new(txn);

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
        // Don't create any collections

        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn);

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
        let db = setup_db_with_collection().await;

        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn);

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

    #[tokio::test]
    async fn test_rollback_reverts_mutations() {
        let db = setup_db_with_collection().await;

        // Create a document in a transaction
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn);

        let mut doc = Document::new();
        doc.set("name", NormalValue::String("RollbackTest".to_string()));
        let result = mutator.create("Users", doc).await.unwrap();
        let doc_id = result.doc_id.clone();

        // Verify document exists within the transaction
        let exists_in_txn = mutator.exists("Users", &doc_id).await.unwrap();
        assert!(exists_in_txn, "Document should exist within transaction");

        // Drop the transaction without committing (implicit rollback)
        let txn = mutator.take_txn().await.unwrap();
        drop(txn); // Transaction dropped without commit = rollback

        // Verify document does NOT exist after rollback
        let txn = db.new_txn(true).await.unwrap();
        let read_mutator = DbDocMutator::new(txn);
        let exists_after_rollback = read_mutator.exists("Users", &doc_id).await.unwrap();
        assert!(
            !exists_after_rollback,
            "Document should NOT exist after rollback"
        );
    }

    #[tokio::test]
    async fn test_partial_mutation_rollback() {
        let db = setup_db_with_collection().await;

        // Create first document successfully
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn);

        let mut doc1 = Document::new();
        doc1.set("name", NormalValue::String("Doc1".to_string()));
        let result1 = mutator.create("Users", doc1).await.unwrap();
        let doc1_id = result1.doc_id.clone();

        let mut doc2 = Document::new();
        doc2.set("name", NormalValue::String("Doc2".to_string()));
        let result2 = mutator.create("Users", doc2).await.unwrap();
        let doc2_id = result2.doc_id.clone();

        // Verify both documents exist within the transaction
        assert!(mutator.exists("Users", &doc1_id).await.unwrap());
        assert!(mutator.exists("Users", &doc2_id).await.unwrap());

        // Drop without committing (simulating failure scenario)
        drop(mutator);

        // Verify NEITHER document exists after rollback
        let txn = db.new_txn(true).await.unwrap();
        let read_mutator = DbDocMutator::new(txn);
        assert!(
            !read_mutator.exists("Users", &doc1_id).await.unwrap(),
            "Doc1 should not exist after rollback"
        );
        assert!(
            !read_mutator.exists("Users", &doc2_id).await.unwrap(),
            "Doc2 should not exist after rollback"
        );
    }

    #[tokio::test]
    async fn test_concurrent_mutations_are_serialized() {
        use std::sync::Arc;

        let db = setup_db_with_collection().await;

        let txn = db.new_txn(false).await.unwrap();
        let mutator = Arc::new(DbDocMutator::new(txn));

        // Spawn multiple concurrent create operations
        let m1 = mutator.clone();
        let m2 = mutator.clone();
        let m3 = mutator.clone();

        let (r1, r2, r3) = tokio::join!(
            async move {
                let mut doc = Document::new();
                doc.set("name", NormalValue::String("Concurrent1".to_string()));
                m1.create("Users", doc).await
            },
            async move {
                let mut doc = Document::new();
                doc.set("name", NormalValue::String("Concurrent2".to_string()));
                m2.create("Users", doc).await
            },
            async move {
                let mut doc = Document::new();
                doc.set("name", NormalValue::String("Concurrent3".to_string()));
                m3.create("Users", doc).await
            }
        );

        // All operations should succeed
        assert!(r1.is_ok(), "First concurrent create should succeed");
        assert!(r2.is_ok(), "Second concurrent create should succeed");
        assert!(r3.is_ok(), "Third concurrent create should succeed");

        // All documents should have unique IDs
        let doc1_id = r1.unwrap().doc_id;
        let doc2_id = r2.unwrap().doc_id;
        let doc3_id = r3.unwrap().doc_id;

        assert_ne!(doc1_id, doc2_id, "Doc IDs should be unique");
        assert_ne!(doc2_id, doc3_id, "Doc IDs should be unique");
        assert_ne!(doc1_id, doc3_id, "Doc IDs should be unique");

        // Commit and verify all documents exist
        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        let txn = db.new_txn(true).await.unwrap();
        let read_mutator = DbDocMutator::new(txn);
        assert!(read_mutator.exists("Users", &doc1_id).await.unwrap());
        assert!(read_mutator.exists("Users", &doc2_id).await.unwrap());
        assert!(read_mutator.exists("Users", &doc3_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_concurrent_read_write_operations() {
        use std::sync::Arc;

        let db = setup_db_with_collection().await;

        // First create a document
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(txn);

        let mut doc = Document::new();
        doc.set("name", NormalValue::String("ReadWriteTest".to_string()));
        let result = mutator.create("Users", doc).await.unwrap();
        let doc_id = result.doc_id.clone();

        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();

        // Now test concurrent read and write operations
        let txn = db.new_txn(false).await.unwrap();
        let mutator = Arc::new(DbDocMutator::new(txn));

        let m1 = mutator.clone();
        let m2 = mutator.clone();
        let doc_id_clone = doc_id.clone();

        let (read_result, update_result) = tokio::join!(
            async move { m1.get_for_update("Users", &doc_id_clone).await },
            async move {
                let mut updated_doc = Document::with_id(doc_id.clone());
                updated_doc.set("name", NormalValue::String("UpdatedName".to_string()));
                m2.update("Users", updated_doc).await
            }
        );

        // Both operations should succeed
        assert!(read_result.is_ok(), "Read should succeed");
        assert!(update_result.is_ok(), "Update should succeed");
    }
}
