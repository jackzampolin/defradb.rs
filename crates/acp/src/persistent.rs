//! Persistent ACP store backed by any Store implementation.
//!
//! This implementation stores relation tuples with namespace isolation,
//! allowing ACP data to be stored in the main database using the
//! `Namespace::Acpstore` prefix, or in a standalone database.

use async_trait::async_trait;
use identity::Did;
use std::sync::Arc;

use storage::corekv::{IterOptions, Reader, Store, Writer};
use storage::namespace::{Namespace, NamespacedStore};

use std::path::Path;
use storage::RegolithStore;

use crate::error::{Error, Result};
use crate::relation::RelationTuple;
use crate::store::AcpStore;

/// Persistent ACP store backed by any Store implementation.
///
/// Stores relation tuples with namespace isolation, providing:
/// - Persistence across node restarts
/// - ACID transactions for tuple operations
/// - Efficient prefix-based queries for document lookups
/// - Namespace isolation when sharing a database with other stores
///
/// # Usage Modes
///
/// ## Unified Mode (Recommended)
///
/// Share the main database with ACP namespace isolation:
///
/// ```ignore
/// use acp::PersistentAcpStore;
/// use storage::RegolithStore;
/// use std::sync::Arc;
///
/// let main_store = Arc::new(RegolithStore::open("/data")?);
/// let acp_store = PersistentAcpStore::from_store(main_store);
/// ```
///
/// ## Standalone Mode (Backward Compatible)
///
/// Use a separate database at `<root>/local_document_acp/`:
///
/// ```ignore
/// use acp::PersistentAcpStore;
///
/// let store = PersistentAcpStore::open("/data/local_document_acp")?;
/// ```
pub struct PersistentAcpStore<S: Store> {
    store: NamespacedStore<S>,
}

impl<S: Store> PersistentAcpStore<S> {
    /// Create from an existing Store with ACP namespace isolation.
    ///
    /// This is the recommended way to create a PersistentAcpStore when
    /// sharing a database with other stores. The ACP namespace prefix
    /// ensures complete isolation from other data.
    pub fn from_store(store: Arc<S>) -> Self {
        Self {
            store: NamespacedStore::new(store, Namespace::Acpstore),
        }
    }

    /// Get the underlying namespaced store.
    pub fn inner(&self) -> &NamespacedStore<S> {
        &self.store
    }
}

impl PersistentAcpStore<RegolithStore> {
    /// Open a persistent ACP store at the given directory path.
    ///
    /// Creates the directory and database file (`acp.regolith`) if they don't exist.
    /// The path should be a directory (e.g., `<root>/local_document_acp/`).
    ///
    /// This method provides backward compatibility for standalone ACP stores.
    /// For new deployments, prefer `from_store()` with a shared database.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The directory cannot be created (permission denied, disk full, etc.)
    /// - The path exists but is not a directory
    /// - The database file cannot be opened (corruption, permission denied, etc.)
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let dir_path = path.as_ref();

        if dir_path.exists() && !dir_path.is_dir() {
            return Err(Error::Storage(format!(
                "ACP path exists but is not a directory: {}",
                dir_path.display()
            )));
        }

        if !dir_path.exists() {
            std::fs::create_dir_all(dir_path).map_err(|e| {
                let kind = e.kind();
                Error::Storage(format!(
                    "failed to create ACP directory '{}': {} ({})",
                    dir_path.display(),
                    e,
                    match kind {
                        std::io::ErrorKind::PermissionDenied => "check directory permissions",
                        std::io::ErrorKind::NotFound => "parent directory does not exist",
                        _ => "check disk space and permissions",
                    }
                ))
            })?;
        }

        let db_path = dir_path.join("acp.regolith");
        if db_path.exists() {
            let metadata = std::fs::metadata(&db_path).map_err(|e| {
                Error::Storage(format!(
                    "cannot access ACP database '{}': {}",
                    db_path.display(),
                    e
                ))
            })?;

            if !metadata.is_file() {
                return Err(Error::Storage(format!(
                    "ACP database path is not a file: {}",
                    db_path.display()
                )));
            }
        }

        let store = RegolithStore::open(&db_path).map_err(|e| {
            Error::Storage(format!(
                "failed to open ACP database '{}': {}",
                db_path.display(),
                e
            ))
        })?;

        Ok(Self {
            store: NamespacedStore::new(Arc::new(store), Namespace::Acpstore),
        })
    }

    /// Close the store.
    pub async fn close(&self) -> Result<()> {
        self.store
            .close()
            .await
            .map_err(|e| Error::storage_txn("close", e))
    }
}

#[async_trait]
impl<S: Store + Send + Sync> AcpStore for PersistentAcpStore<S> {
    async fn put_tuple(&self, tuple: &RelationTuple) -> Result<()> {
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::storage_txn("put_tuple:begin", e))?;

        let key = tuple.storage_key();
        let value = serde_json::to_vec(tuple)?;

        txn.set(key.as_bytes(), &value)
            .await
            .map_err(|e| Error::storage_write("put_tuple:set", e))?;

        txn.commit()
            .await
            .map_err(|e| Error::storage_txn("put_tuple:commit", e))?;

        Ok(())
    }

    async fn delete_tuple(&self, tuple: &RelationTuple) -> Result<()> {
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::storage_txn("delete_tuple:begin", e))?;

        let key = tuple.storage_key();

        txn.delete(key.as_bytes())
            .await
            .map_err(|e| Error::storage_write("delete_tuple:delete", e))?;

        txn.commit()
            .await
            .map_err(|e| Error::storage_txn("delete_tuple:commit", e))?;

        Ok(())
    }

    async fn has_tuple(&self, tuple: &RelationTuple) -> Result<bool> {
        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::storage_txn("has_tuple:begin", e))?;

        let key = tuple.storage_key();

        txn.has(key.as_bytes())
            .await
            .map_err(|e| Error::storage_read("has_tuple:has", e))
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
            .map_err(|e| Error::storage_txn("get_doc_tuples:begin", e))?;

        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);

        let iter_opts = IterOptions::new().with_prefix(prefix.into_bytes());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::storage_iter("get_doc_tuples:iterator", e))?;

        let mut tuples = Vec::new();

        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| Error::storage_iter("get_doc_tuples:next", e))?
        {
            let tuple: RelationTuple = serde_json::from_slice(&kv.value)?;
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
            .map_err(|e| Error::storage_txn("get_relation_subjects:begin", e))?;

        let prefix = RelationTuple::relation_prefix(collection_id, doc_id, relation);

        let iter_opts = IterOptions::new().with_prefix(prefix.into_bytes());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::storage_iter("get_relation_subjects:iterator", e))?;

        let mut subjects = Vec::new();

        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| Error::storage_iter("get_relation_subjects:next", e))?
        {
            let tuple: RelationTuple = serde_json::from_slice(&kv.value)?;
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
            .map_err(|e| Error::storage_txn("get_subject_relations:begin", e))?;

        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);

        let iter_opts = IterOptions::new().with_prefix(prefix.into_bytes());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::storage_iter("get_subject_relations:iterator", e))?;

        let mut relations = Vec::new();

        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| Error::storage_iter("get_subject_relations:next", e))?
        {
            let tuple: RelationTuple = serde_json::from_slice(&kv.value)?;
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
            .map_err(|e| Error::storage_txn("delete_doc_tuples:begin", e))?;

        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);

        let iter_opts = IterOptions::new().with_prefix(prefix.clone().into_bytes());

        let mut keys_to_delete = Vec::new();
        {
            let mut iter = txn
                .iterator(iter_opts)
                .await
                .map_err(|e| Error::storage_iter("delete_doc_tuples:iterator", e))?;

            while let Some(kv) = iter
                .next()
                .await
                .map_err(|e| Error::storage_iter("delete_doc_tuples:next", e))?
            {
                keys_to_delete.push(kv.key);
            }
        }

        for key in keys_to_delete {
            txn.delete(&key)
                .await
                .map_err(|e| Error::storage_write("delete_doc_tuples:delete", e))?;
        }

        txn.commit()
            .await
            .map_err(|e| Error::storage_txn("delete_doc_tuples:commit", e))?;

        Ok(())
    }

    async fn is_doc_registered(&self, collection_id: &str, doc_id: &str) -> Result<bool> {
        // Validate inputs to prevent path traversal
        RelationTuple::validate_prefix(collection_id, doc_id)?;

        let txn = self
            .store
            .new_txn(true)
            .await
            .map_err(|e| Error::storage_txn("is_doc_registered:begin", e))?;

        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);

        let iter_opts = IterOptions::new().with_prefix(prefix.into_bytes());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::storage_iter("is_doc_registered:iterator", e))?;

        Ok(iter
            .next()
            .await
            .map_err(|e| Error::storage_iter("is_doc_registered:next", e))?
            .is_some())
    }

    async fn register_doc_atomic(
        &self,
        owner: &Did,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<bool> {
        // Validate inputs to prevent path traversal
        RelationTuple::validate_prefix(collection_id, doc_id)?;

        // Use a single write transaction for both check and write
        // RocksDB transactions provide atomicity and isolation
        let mut txn = self
            .store
            .new_txn(false)
            .await
            .map_err(|e| Error::storage_txn("register_doc_atomic:begin", e))?;

        let prefix = RelationTuple::doc_prefix(collection_id, doc_id);
        let iter_opts = IterOptions::new().with_prefix(prefix.into_bytes());

        let mut iter = txn
            .iterator(iter_opts)
            .await
            .map_err(|e| Error::storage_iter("register_doc_atomic:iterator", e))?;

        if iter
            .next()
            .await
            .map_err(|e| Error::storage_iter("register_doc_atomic:next", e))?
            .is_some()
        {
            return Ok(false);
        }

        let tuple = RelationTuple::owner(owner.clone(), collection_id, doc_id);
        let key = tuple.storage_key();
        let value = serde_json::to_vec(&tuple)?;

        txn.set(key.as_bytes(), &value)
            .await
            .map_err(|e| Error::storage_write("register_doc_atomic:set_tuple", e))?;

        // Write a sentinel key so concurrent registrations for the same doc conflict.
        // Without this, two owners writing different keys would both succeed under
        // snapshot isolation (the conflict tracker only detects write-write conflicts
        // on the same key).
        let sentinel = format!("/acp-reg/{}/{}", collection_id, doc_id);
        txn.set(sentinel.as_bytes(), &[1])
            .await
            .map_err(|e| Error::storage_write("register_doc_atomic:set_sentinel", e))?;

        match txn.commit().await {
            Ok(()) => Ok(true),
            Err(e) if e.is_txn_conflict() => Ok(false),
            Err(e) => Err(Error::storage_txn("register_doc_atomic:commit", e)),
        }
    }
}
