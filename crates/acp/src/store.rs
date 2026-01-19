//! ACP tuple storage trait.
//!
//! Defines the interface for storing and retrieving relation tuples.

use async_trait::async_trait;
use identity::Did;

use crate::error::Result;
use crate::relation::RelationTuple;

/// Trait for storing and querying relation tuples.
///
/// This abstraction allows different storage backends to be used
/// (in-memory for testing, RocksDB for production, etc.)
#[async_trait]
pub trait AcpStore: Send + Sync {
    /// Store a relation tuple.
    async fn put_tuple(&self, tuple: &RelationTuple) -> Result<()>;

    /// Delete a relation tuple.
    async fn delete_tuple(&self, tuple: &RelationTuple) -> Result<()>;

    /// Check if a tuple exists.
    async fn has_tuple(&self, tuple: &RelationTuple) -> Result<bool>;

    /// Get all tuples for a document.
    async fn get_doc_tuples(&self, collection_id: &str, doc_id: &str)
        -> Result<Vec<RelationTuple>>;

    /// Get all subjects with a specific relation to a document.
    async fn get_relation_subjects(
        &self,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<Vec<Did>>;

    /// Get all relations a subject has to a document.
    async fn get_subject_relations(
        &self,
        subject: &Did,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<Vec<String>>;

    /// Delete all tuples for a document.
    async fn delete_doc_tuples(&self, collection_id: &str, doc_id: &str) -> Result<()>;

    /// Check if a document has any tuples (i.e., is registered with ACP).
    async fn is_doc_registered(&self, collection_id: &str, doc_id: &str) -> Result<bool>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;
    use std::collections::HashMap;

    /// In-memory implementation for testing
    struct MemoryAcpStore {
        tuples: RwLock<HashMap<String, RelationTuple>>,
    }

    impl MemoryAcpStore {
        fn new() -> Self {
            Self {
                tuples: RwLock::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl AcpStore for MemoryAcpStore {
        async fn put_tuple(&self, tuple: &RelationTuple) -> Result<()> {
            self.tuples
                .write()
                .insert(tuple.storage_key(), tuple.clone());
            Ok(())
        }

        async fn delete_tuple(&self, tuple: &RelationTuple) -> Result<()> {
            self.tuples.write().remove(&tuple.storage_key());
            Ok(())
        }

        async fn has_tuple(&self, tuple: &RelationTuple) -> Result<bool> {
            Ok(self.tuples.read().contains_key(&tuple.storage_key()))
        }

        async fn get_doc_tuples(
            &self,
            collection_id: &str,
            doc_id: &str,
        ) -> Result<Vec<RelationTuple>> {
            let prefix = RelationTuple::doc_prefix(collection_id, doc_id);
            let tuples = self
                .tuples
                .read()
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, v)| v.clone())
                .collect();
            Ok(tuples)
        }

        async fn get_relation_subjects(
            &self,
            collection_id: &str,
            doc_id: &str,
            relation: &str,
        ) -> Result<Vec<Did>> {
            let prefix = RelationTuple::relation_prefix(collection_id, doc_id, relation);
            let subjects = self
                .tuples
                .read()
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, v)| v.subject().clone())
                .collect();
            Ok(subjects)
        }

        async fn get_subject_relations(
            &self,
            subject: &Did,
            collection_id: &str,
            doc_id: &str,
        ) -> Result<Vec<String>> {
            let prefix = RelationTuple::doc_prefix(collection_id, doc_id);
            let relations = self
                .tuples
                .read()
                .iter()
                .filter(|(k, v)| k.starts_with(&prefix) && v.subject() == subject)
                .map(|(_, v)| v.relation().to_string())
                .collect();
            Ok(relations)
        }

        async fn delete_doc_tuples(&self, collection_id: &str, doc_id: &str) -> Result<()> {
            let prefix = RelationTuple::doc_prefix(collection_id, doc_id);
            self.tuples.write().retain(|k, _| !k.starts_with(&prefix));
            Ok(())
        }

        async fn is_doc_registered(&self, collection_id: &str, doc_id: &str) -> Result<bool> {
            let prefix = RelationTuple::doc_prefix(collection_id, doc_id);
            Ok(self.tuples.read().keys().any(|k| k.starts_with(&prefix)))
        }
    }

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    fn test_did2() -> Did {
        Did::new("did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR").unwrap()
    }

    #[tokio::test]
    async fn test_memory_store_put_and_has() {
        let store = MemoryAcpStore::new();
        let tuple = RelationTuple::owner(test_did(), "users", "doc1");

        assert!(!store.has_tuple(&tuple).await.unwrap());
        store.put_tuple(&tuple).await.unwrap();
        assert!(store.has_tuple(&tuple).await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_store_delete() {
        let store = MemoryAcpStore::new();
        let tuple = RelationTuple::owner(test_did(), "users", "doc1");

        store.put_tuple(&tuple).await.unwrap();
        assert!(store.has_tuple(&tuple).await.unwrap());

        store.delete_tuple(&tuple).await.unwrap();
        assert!(!store.has_tuple(&tuple).await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_store_get_doc_tuples() {
        let store = MemoryAcpStore::new();
        let did1 = test_did();
        let did2 = test_did2();

        let tuple1 = RelationTuple::owner(did1.clone(), "users", "doc1");
        let tuple2 = RelationTuple::new(did2.clone(), "reader", "users", "doc1");
        let tuple3 = RelationTuple::owner(did1.clone(), "users", "doc2");

        store.put_tuple(&tuple1).await.unwrap();
        store.put_tuple(&tuple2).await.unwrap();
        store.put_tuple(&tuple3).await.unwrap();

        let doc1_tuples = store.get_doc_tuples("users", "doc1").await.unwrap();
        assert_eq!(doc1_tuples.len(), 2);

        let doc2_tuples = store.get_doc_tuples("users", "doc2").await.unwrap();
        assert_eq!(doc2_tuples.len(), 1);
    }

    #[tokio::test]
    async fn test_memory_store_is_doc_registered() {
        let store = MemoryAcpStore::new();
        let tuple = RelationTuple::owner(test_did(), "users", "doc1");

        assert!(!store.is_doc_registered("users", "doc1").await.unwrap());
        store.put_tuple(&tuple).await.unwrap();
        assert!(store.is_doc_registered("users", "doc1").await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_store_delete_doc_tuples() {
        let store = MemoryAcpStore::new();
        let did1 = test_did();
        let did2 = test_did2();

        let tuple1 = RelationTuple::owner(did1.clone(), "users", "doc1");
        let tuple2 = RelationTuple::new(did2.clone(), "reader", "users", "doc1");

        store.put_tuple(&tuple1).await.unwrap();
        store.put_tuple(&tuple2).await.unwrap();
        assert!(store.is_doc_registered("users", "doc1").await.unwrap());

        store.delete_doc_tuples("users", "doc1").await.unwrap();
        assert!(!store.is_doc_registered("users", "doc1").await.unwrap());
    }
}
