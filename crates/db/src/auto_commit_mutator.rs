//! Auto-committing document mutator for non-transactional mutations.
//!
//! This mutator wraps a database and automatically creates and commits
//! a write transaction for each mutation operation. This enables mutations
//! without explicit transaction management while still providing proper
//! transactional semantics per operation.

use async_trait::async_trait;
use document::{DocID, Document};
use query::mutator::{CreateResult, DeleteResult, DocMutator, UpdateResult};
use std::sync::Arc;
use storage::corekv::Store;
use tracing::warn;

use crate::database::DB;

/// Document mutator that auto-commits transactions for each operation.
///
/// This is useful for mutations that don't need explicit transaction control.
/// Each operation creates a new write transaction, performs the mutation,
/// and commits (or discards on error).
///
/// # Transaction Semantics
///
/// Each mutation is atomic: it either succeeds entirely or fails without
/// partial changes. However, multiple mutations are NOT atomic with respect
/// to each other - if you need multiple operations to be atomic, use
/// `DbDocMutator` with explicit transaction management instead.
pub struct AutoCommitMutator<S: Store> {
    db: Arc<DB<S>>,
}

impl<S: Store> AutoCommitMutator<S> {
    /// Create a new auto-committing mutator wrapping the given database.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl<S: Store + 'static> DocMutator for AutoCommitMutator<S> {
    async fn create(
        &self,
        collection_name: &str,
        mut doc: Document,
    ) -> query::error::Result<CreateResult> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a write transaction
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Generate document ID if not present
        if doc.id().is_none() {
            doc.generate_and_set_doc_id().map_err(|e| {
                query::error::QueryError::execution(format!("failed to generate DocID: {}", e))
            })?;
        }

        let doc_id = doc.id().cloned().ok_or_else(|| {
            query::error::QueryError::execution("document should have ID after generation")
        })?;

        // Execute the mutation in a block to drop datastore before commit
        let result = {
            let datastore = txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?;

            collection
                .create_with_datastore(&datastore, &doc)
                .await
                .map_err(|e| query::error::QueryError::execution(format!("create error: {}", e)))
        };

        match result {
            Ok(_returned_doc_id) => {
                // Commit the transaction (datastore reference is now dropped)
                if let Err(e) = txn.commit().await {
                    warn!(
                        collection = %collection_name,
                        error = %e,
                        "Failed to commit transaction after create"
                    );
                    return Err(query::error::QueryError::execution(format!(
                        "commit error: {}",
                        e
                    )));
                }
                Ok(CreateResult::new(doc_id, doc))
            }
            Err(e) => {
                // Discard the transaction on error
                if let Err(discard_err) = txn.discard() {
                    warn!(
                        collection = %collection_name,
                        error = %discard_err,
                        "Failed to discard transaction after create error"
                    );
                }
                Err(e)
            }
        }
    }

    async fn update(
        &self,
        collection_name: &str,
        doc: Document,
    ) -> query::error::Result<UpdateResult> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a write transaction
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Execute the mutation in a block to drop datastore before commit
        let result = {
            let datastore = txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?;

            collection
                .update_with_datastore(&datastore, &doc)
                .await
                .map_err(|e| query::error::QueryError::execution(format!("update error: {}", e)))
        };

        match result {
            Ok(()) => {
                // Commit the transaction (datastore reference is now dropped)
                if let Err(e) = txn.commit().await {
                    warn!(
                        collection = %collection_name,
                        error = %e,
                        "Failed to commit transaction after update"
                    );
                    return Err(query::error::QueryError::execution(format!(
                        "commit error: {}",
                        e
                    )));
                }

                // Count modified fields
                let fields_modified = doc.values().len();
                Ok(UpdateResult::new(doc, fields_modified))
            }
            Err(e) => {
                // Discard the transaction on error
                if let Err(discard_err) = txn.discard() {
                    warn!(
                        collection = %collection_name,
                        error = %discard_err,
                        "Failed to discard transaction after update error"
                    );
                }
                Err(e)
            }
        }
    }

    async fn delete(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a write transaction
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        // Execute the mutation in a block to drop datastore before commit
        let result = {
            let datastore = txn.datastore().map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to get datastore for collection '{}': {}",
                    collection_name, e
                ))
            })?;

            collection
                .delete_with_datastore(&datastore, doc_id)
                .await
                .map_err(|e| query::error::QueryError::execution(format!("delete error: {}", e)))
        };

        match result {
            Ok(existed) => {
                // Commit the transaction (datastore reference is now dropped)
                if let Err(e) = txn.commit().await {
                    warn!(
                        collection = %collection_name,
                        error = %e,
                        "Failed to commit transaction after delete"
                    );
                    return Err(query::error::QueryError::execution(format!(
                        "commit error: {}",
                        e
                    )));
                }
                Ok(DeleteResult::new(doc_id.clone(), existed))
            }
            Err(e) => {
                // Discard the transaction on error
                if let Err(discard_err) = txn.discard() {
                    warn!(
                        collection = %collection_name,
                        error = %discard_err,
                        "Failed to discard transaction after delete error"
                    );
                }
                Err(e)
            }
        }
    }

    async fn exists(&self, collection_name: &str, doc_id: &DocID) -> query::error::Result<bool> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a read-only transaction (exists is read-only)
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

        // Execute the check
        let result = collection
            .exists_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("exists error: {}", e)));

        // Discard the read-only transaction
        if let Err(e) = txn.discard() {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to discard read-only transaction after exists"
            );
        }

        result
    }

    async fn get_for_update(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<Option<Document>> {
        // Get collection from DB cache
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Create a read-only transaction (get_for_update is read-only)
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

        // Execute the fetch
        let result = collection
            .get_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("get_for_update error: {}", e))
            });

        // Discard the read-only transaction
        if let Err(e) = txn.discard() {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to discard read-only transaction after get_for_update"
            );
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use document::NormalValue;
    use query::mutator::DocMutator;
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use storage::backends::MemoryStore;

    fn test_schema() -> CollectionVersion {
        CollectionVersion::new(
            "Users",
            "v1",
            "col-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        )
    }

    #[tokio::test]
    async fn test_create_document() {
        let store = MemoryStore::new();
        let db = Arc::new(DB::new(store));
        db.create_collection(test_schema()).await.unwrap();

        let mutator = AutoCommitMutator::new(db.clone());

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

        // Verify document persisted
        let exists = mutator.exists("Users", &result.doc_id).await.unwrap();
        assert!(exists);
    }

    #[tokio::test]
    async fn test_update_document() {
        let store = MemoryStore::new();
        let db = Arc::new(DB::new(store));
        db.create_collection(test_schema()).await.unwrap();

        let mutator = AutoCommitMutator::new(db);

        // First create a document
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Bob".to_string()));
        doc.set("age", NormalValue::Int(25));
        let result = mutator.create("Users", doc).await.unwrap();
        let doc_id = result.doc_id.clone();

        // Update the document
        let mut updated_doc = Document::with_id(doc_id.clone());
        updated_doc.set("name", NormalValue::String("Robert".to_string()));
        updated_doc.set("age", NormalValue::Int(26));

        let update_result = mutator.update("Users", updated_doc).await.unwrap();
        assert!(update_result.fields_modified > 0);

        // Verify update
        let fetched = mutator.get_for_update("Users", &doc_id).await.unwrap();
        assert_eq!(
            fetched.unwrap().get("name").and_then(|v| v.as_str()),
            Some("Robert")
        );
    }

    #[tokio::test]
    async fn test_delete_document() {
        let store = MemoryStore::new();
        let db = Arc::new(DB::new(store));
        db.create_collection(test_schema()).await.unwrap();

        let mutator = AutoCommitMutator::new(db);

        // First create a document
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Charlie".to_string()));
        let result = mutator.create("Users", doc).await.unwrap();
        let doc_id = result.doc_id.clone();

        // Verify it exists
        assert!(mutator.exists("Users", &doc_id).await.unwrap());

        // Delete it
        let delete_result = mutator.delete("Users", &doc_id).await.unwrap();
        assert!(delete_result.existed);

        // Verify it's gone
        assert!(!mutator.exists("Users", &doc_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_document() {
        let store = MemoryStore::new();
        let db = Arc::new(DB::new(store));
        db.create_collection(test_schema()).await.unwrap();

        let mutator = AutoCommitMutator::new(db);

        // Try to delete a document that doesn't exist
        let nonexistent_id =
            DocID::from_string("bae-c94acbfa-dd53-40d0-97f3-29ce16c333fc").unwrap();
        let delete_result = mutator.delete("Users", &nonexistent_id).await.unwrap();
        assert!(!delete_result.existed);
    }

    #[tokio::test]
    async fn test_get_for_update_nonexistent() {
        let store = MemoryStore::new();
        let db = Arc::new(DB::new(store));
        db.create_collection(test_schema()).await.unwrap();

        let mutator = AutoCommitMutator::new(db);

        let nonexistent_id =
            DocID::from_string("bae-c94acbfa-dd53-40d0-97f3-29ce16c333fc").unwrap();
        let result = mutator
            .get_for_update("Users", &nonexistent_id)
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_unknown_collection_returns_error() {
        let store = MemoryStore::new();
        let db = Arc::new(DB::new(store));

        let mutator = AutoCommitMutator::new(db);
        let doc = Document::new();
        let result = mutator.create("NonExistent", doc).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("collection not found"));
    }

    #[tokio::test]
    async fn test_each_mutation_is_independent() {
        let store = MemoryStore::new();
        let db = Arc::new(DB::new(store));
        db.create_collection(test_schema()).await.unwrap();

        let mutator = AutoCommitMutator::new(db);

        // Create first document
        let mut doc1 = Document::new();
        doc1.set("name", NormalValue::String("Doc1".to_string()));
        let result1 = mutator.create("Users", doc1).await.unwrap();

        // Create second document
        let mut doc2 = Document::new();
        doc2.set("name", NormalValue::String("Doc2".to_string()));
        let result2 = mutator.create("Users", doc2).await.unwrap();

        // Both should exist independently
        assert!(mutator.exists("Users", &result1.doc_id).await.unwrap());
        assert!(mutator.exists("Users", &result2.doc_id).await.unwrap());

        // Deleting one doesn't affect the other
        mutator.delete("Users", &result1.doc_id).await.unwrap();
        assert!(!mutator.exists("Users", &result1.doc_id).await.unwrap());
        assert!(mutator.exists("Users", &result2.doc_id).await.unwrap());
    }
}
