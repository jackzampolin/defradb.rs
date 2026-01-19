//! Persistent ACP store backed by RocksDB.
//!
//! This implementation stores relation tuples in a separate RocksDB instance,
//! following Go DefraDB's architecture where ACP data is stored independently
//! from document data at `<root>/local_document_acp/`.

use async_trait::async_trait;
use identity::Did;
use std::path::Path;
use std::sync::Arc;

use storage::corekv::{IterOptions, Reader, Store, Writer};
use storage::RocksDBStore;

use crate::error::{Error, Result};
use crate::relation::RelationTuple;
use crate::store::AcpStore;

/// Persistent ACP store backed by RocksDB.
///
/// Stores relation tuples in a separate RocksDB instance, providing:
/// - Persistence across node restarts
/// - ACID transactions for tuple operations
/// - Efficient prefix-based queries for document lookups
///
/// # Directory Structure
///
/// Following Go DefraDB's convention, this store should be opened at:
/// `<root>/local_document_acp/`
///
/// # Example
///
/// ```ignore
/// use acp::PersistentAcpStore;
/// use std::path::Path;
///
/// let store = PersistentAcpStore::open(Path::new("/data/local_document_acp"))?;
/// ```
pub struct PersistentAcpStore {
    store: Arc<RocksDBStore>,
}

impl PersistentAcpStore {
    /// Open a persistent ACP store at the given path.
    ///
    /// Creates the database if it doesn't exist.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let store = RocksDBStore::open(path).map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self {
            store: Arc::new(store),
        })
    }

    /// Create from an existing RocksDBStore.
    ///
    /// Useful when the store is managed externally.
    pub fn from_store(store: Arc<RocksDBStore>) -> Self {
        Self { store }
    }

    /// Close the store.
    pub async fn close(&self) -> Result<()> {
        self.store
            .close()
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }
}

#[async_trait]
impl AcpStore for PersistentAcpStore {
    async fn put_tuple(&self, tuple: &RelationTuple) -> Result<()> {
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let key = tuple.storage_key();
        // Store the tuple as JSON for debugging/inspection
        let value = serde_json::to_vec(tuple).map_err(|e| Error::Storage(e.to_string()))?;

        txn.set(key.as_bytes(), &value)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        txn.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn delete_tuple(&self, tuple: &RelationTuple) -> Result<()> {
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let key = tuple.storage_key();

        txn.delete(key.as_bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        txn.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn has_tuple(&self, tuple: &RelationTuple) -> Result<bool> {
        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let key = tuple.storage_key();

        txn.has(key.as_bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    async fn get_doc_tuples(
        &self,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<Vec<RelationTuple>> {
        // Validate inputs to prevent path traversal
        RelationTuple::validate_prefix(collection_id, doc_id)?;

        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);

        let iter_opts = IterOptions::new().with_prefix(prefix.into_bytes());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut tuples = Vec::new();

        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let tuple: RelationTuple =
                serde_json::from_slice(&kv.value).map_err(|e| Error::Storage(e.to_string()))?;
            tuples.push(tuple);
        }

        Ok(tuples)
    }

    async fn get_relation_subjects(
        &self,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<Vec<Did>> {
        // Validate inputs to prevent path traversal
        RelationTuple::validate_relation_prefix(collection_id, doc_id, relation)?;

        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let prefix = RelationTuple::relation_prefix(collection_id, doc_id, relation);

        let iter_opts = IterOptions::new().with_prefix(prefix.into_bytes());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut subjects = Vec::new();

        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let tuple: RelationTuple =
                serde_json::from_slice(&kv.value).map_err(|e| Error::Storage(e.to_string()))?;
            subjects.push(tuple.subject().clone());
        }

        Ok(subjects)
    }

    async fn get_subject_relations(
        &self,
        subject: &Did,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<Vec<String>> {
        // Validate inputs to prevent path traversal
        RelationTuple::validate_prefix(collection_id, doc_id)?;

        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);

        let iter_opts = IterOptions::new().with_prefix(prefix.into_bytes());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut relations = Vec::new();

        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let tuple: RelationTuple =
                serde_json::from_slice(&kv.value).map_err(|e| Error::Storage(e.to_string()))?;
            if tuple.subject() == subject {
                relations.push(tuple.relation().to_string());
            }
        }

        Ok(relations)
    }

    async fn delete_doc_tuples(&self, collection_id: &str, doc_id: &str) -> Result<()> {
        // Validate inputs to prevent path traversal
        RelationTuple::validate_prefix(collection_id, doc_id)?;

        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);

        let iter_opts = IterOptions::new().with_prefix(prefix.clone().into_bytes());

        // First collect all keys to delete
        let mut keys_to_delete = Vec::new();
        {
            let mut iter = txn
                .iterator(iter_opts)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;

            while let Some(kv) = iter
                .next()
                .await
                .map_err(|e| Error::Storage(e.to_string()))?
            {
                keys_to_delete.push(kv.key);
            }
        }

        // Now delete all collected keys
        for key in keys_to_delete {
            txn.delete(&key)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        }

        txn.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn is_doc_registered(&self, collection_id: &str, doc_id: &str) -> Result<bool> {
        // Validate inputs to prevent path traversal
        RelationTuple::validate_prefix(collection_id, doc_id)?;

        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);

        let iter_opts = IterOptions::new().with_prefix(prefix.into_bytes());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        // Document is registered if any tuple exists for it
        Ok(iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .is_some())
    }
}

// Tests extracted to crates/acp/tests/persistent_tests.rs
