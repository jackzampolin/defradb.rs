/// Database struct for DefraDB matching Go's internal/db/db.go.
///
/// The DB struct is the main entry point for DefraDB operations.
/// It manages the root store, creates transactions, and provides
/// access to collections.
use crate::collection::Collection;
use crate::collection_name::CollectionName;
use crate::collection_snapshot::CollectionSnapshot;
use crate::error::{Error, Result};
use crate::txn::DbTxn;
use datastore::BasicTxn;
use schema::CollectionVersion;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::systemstore::CollectionNameKey;

/// Database options.
#[derive(Debug, Clone, Default)]
pub struct DbOptions {
    /// Maximum number of transaction retries.
    pub max_txn_retries: Option<u32>,
    /// Chunk size for large values in the blockstore.
    pub chunk_size: Option<usize>,
}

/// The main DefraDB database struct.
///
/// This matches Go's DB struct in internal/db/db.go.
pub struct DB<S: Store> {
    /// The underlying store.
    store: Arc<S>,
    /// Options for this database instance.
    options: DbOptions,
    /// Counter for generating unique transaction IDs.
    txn_id_counter: AtomicU64,
    /// In-memory collection cache (name -> Collection).
    collections: RwLock<HashMap<String, Collection>>,
}

impl<S: Store> DB<S> {
    /// Create a new database with the given store.
    ///
    /// This creates a DB with an empty collection cache. Use `open()` to
    /// load existing collections from the store.
    pub fn new(store: S) -> Self {
        Self::with_options(store, DbOptions::default())
    }

    /// Create a new database with the given store and options.
    ///
    /// This creates a DB with an empty collection cache. Use `open_with_options()`
    /// to load existing collections from the store.
    pub fn with_options(store: S, options: DbOptions) -> Self {
        Self {
            store: Arc::new(store),
            options,
            txn_id_counter: AtomicU64::new(0),
            collections: RwLock::new(HashMap::new()),
        }
    }

    /// Open a database and load existing collections from the store.
    pub async fn open(store: S) -> Result<Self> {
        Self::open_with_options(store, DbOptions::default()).await
    }

    /// Open a database with options and load existing collections from the store.
    pub async fn open_with_options(store: S, options: DbOptions) -> Result<Self> {
        let db = Self::with_options(store, options);
        db.load_collections().await?;
        Ok(db)
    }

    /// Get the next transaction ID.
    fn next_txn_id(&self) -> u64 {
        self.txn_id_counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Create a new transaction.
    ///
    /// If `readonly` is true, the transaction cannot perform writes.
    pub async fn new_txn(&self, readonly: bool) -> Result<DbTxn<S>> {
        let id = self.next_txn_id();
        let basic_txn = BasicTxn::new(&*self.store, id, readonly)
            .await
            .map_err(Error::Storage)?;
        Ok(DbTxn::new(basic_txn, self.store.clone()))
    }

    /// Execute a function within a transaction.
    ///
    /// If the function returns Ok, the transaction is committed.
    /// If the function returns Err, the transaction is discarded.
    pub async fn with_txn<F, T>(&self, readonly: bool, f: F) -> Result<T>
    where
        F: FnOnce(&DbTxn<S>) -> Result<T>,
    {
        let txn = self.new_txn(readonly).await?;
        let result = f(&txn);
        match result {
            Ok(value) => {
                txn.commit().await?;
                Ok(value)
            }
            Err(e) => {
                // Discard and log if it fails - return original error
                if let Err(discard_err) = txn.discard() {
                    tracing::warn!(
                        error = %discard_err,
                        original_error = %e,
                        "Transaction discard failed after operation error"
                    );
                }
                Err(e)
            }
        }
    }

    /// Execute an async function within a transaction.
    ///
    /// If the function returns Ok, the transaction is committed.
    /// If the function returns Err, the transaction is discarded.
    pub async fn with_txn_async<F, Fut, T>(&self, readonly: bool, f: F) -> Result<T>
    where
        F: FnOnce(DbTxn<S>) -> Fut,
        Fut: std::future::Future<Output = (DbTxn<S>, Result<T>)>,
    {
        let txn = self.new_txn(readonly).await?;
        let (txn, result) = f(txn).await;
        match result {
            Ok(value) => {
                txn.commit().await?;
                Ok(value)
            }
            Err(e) => {
                // Discard and log if it fails - return original error
                if let Err(discard_err) = txn.discard() {
                    tracing::warn!(
                        error = %discard_err,
                        original_error = %e,
                        "Transaction discard failed after async operation error"
                    );
                }
                Err(e)
            }
        }
    }

    /// Close the database.
    pub async fn close(&self) -> Result<()> {
        self.store.close().await.map_err(Error::Storage)
    }

    /// Get the database options.
    pub fn options(&self) -> &DbOptions {
        &self.options
    }

    /// Get the current transaction ID counter value.
    pub fn current_txn_id(&self) -> u64 {
        self.txn_id_counter.load(Ordering::SeqCst)
    }

    /// Load all collections from the SystemStore into the in-memory cache.
    pub async fn load_collections(&self) -> Result<()> {
        let txn = self.new_txn(true).await?;
        let prefix = CollectionNameKey::name_prefix();
        let mut collections = HashMap::new();

        // Block ensures systemstore reference is dropped before discard
        {
            let systemstore = txn.systemstore()?;
            let opts = IterOptions::new().with_prefix(prefix.clone());

            let mut iter = systemstore.iterator(opts).await.map_err(|e| {
                tracing::error!(error = ?e, "Failed to create iterator during collection load");
                Error::Storage(e)
            })?;

            while let Some(pair) = iter.next().await.map_err(|e| {
                tracing::error!(error = ?e, "Failed to iterate collections during database load");
                Error::Storage(e)
            })? {
                // Validate UTF-8 in key to catch data corruption early
                let key_str = String::from_utf8(pair.key.to_vec()).map_err(|e| {
                    tracing::error!(
                        error = ?e,
                        key_bytes = ?&pair.key[..pair.key.len().min(50)],
                        "Collection key contains invalid UTF-8"
                    );
                    Error::Serialization(format!("collection key contains invalid UTF-8: {}", e))
                })?;

                let prefix_str = String::from_utf8(prefix.clone()).map_err(|e| {
                    tracing::error!(
                        error = ?e,
                        prefix_bytes = ?&prefix[..prefix.len().min(50)],
                        "Internal error: collection key prefix contains invalid UTF-8"
                    );
                    Error::Other(format!("internal error: prefix is not valid UTF-8: {}", e))
                })?;

                let name = key_str
                    .strip_prefix(&prefix_str)
                    .ok_or_else(|| {
                        tracing::error!(
                            key = %key_str,
                            expected_prefix = %prefix_str,
                            "Collection key does not match expected prefix - possible data corruption"
                        );
                        Error::Other(format!(
                            "collection key '{}' does not match expected prefix '{}'",
                            key_str, prefix_str
                        ))
                    })?
                    .to_string();

                let schema: CollectionVersion =
                    serde_json::from_slice(&pair.value).map_err(|e| {
                        tracing::error!(
                            error = ?e,
                            collection_name = %name,
                            "Failed to deserialize schema for collection"
                        );
                        Error::Serialization(format!(
                            "failed to deserialize schema for collection '{}': {}",
                            name, e
                        ))
                    })?;

                collections.insert(name, Collection::new(schema));
            }

            iter.close().await.map_err(|e| {
                tracing::error!(error = ?e, "Failed to close iterator during collection load");
                Error::Storage(e)
            })?;
        }

        if let Err(discard_err) = txn.discard() {
            tracing::error!(
                error = %discard_err,
                "Transaction discard failed after loading collections"
            );
            return Err(Error::Other(format!(
                "failed to discard transaction: {}",
                discard_err
            )));
        }

        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(error = ?e, "Collection cache lock poisoned during load");
            Error::LockPoisoned("collection cache lock poisoned during load".into())
        })?;
        *cache = collections;

        Ok(())
    }

    /// Create a new collection with schema persistence.
    ///
    /// Uses store-level atomicity to prevent duplicates - checks existence within
    /// the transaction before writing.
    ///
    /// # Errors
    ///
    /// - `InvalidCollectionName` if the collection name is invalid
    /// - `CollectionAlreadyExists` if a collection with this name already exists
    /// - `CacheUpdateFailedAfterCommit` if the schema was persisted but cache update failed
    pub async fn create_collection(&self, schema: CollectionVersion) -> Result<()> {
        // Validate collection name
        let collection_name = CollectionName::new(&schema.name)?;
        let name = collection_name.as_str().to_string();

        let txn = self.new_txn(false).await?;
        let key = CollectionNameKey::new(&name);

        // Check if collection exists in store (must drop systemstore before discard)
        let exists = {
            let systemstore = txn.systemstore()?;
            systemstore
                .get(&key.bytes())
                .await
                .map_err(Error::Storage)?
                .is_some()
        };

        if exists {
            if let Err(discard_err) = txn.discard() {
                tracing::error!(
                    error = %discard_err,
                    collection_name = %name,
                    "Transaction discard failed while handling CollectionAlreadyExists"
                );
            }
            return Err(Error::CollectionAlreadyExists(name));
        }

        // Write schema to store
        {
            let systemstore = txn.systemstore()?;
            let data = serde_json::to_vec(&schema).map_err(|e| {
                Error::Serialization(format!(
                    "failed to serialize schema for collection '{}': {}",
                    name, e
                ))
            })?;
            systemstore
                .set(&key.bytes(), &data)
                .await
                .map_err(Error::Storage)?;
        }

        txn.commit().await?;

        // Update cache after successful commit.
        // If this fails, the collection IS persisted but not in cache.
        // A restart will recover by loading from store.
        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(
                error = ?e,
                collection_name = %name,
                "Collection cache lock poisoned during create - collection WAS persisted to store. Restart will recover."
            );
            Error::CacheUpdateFailedAfterCommit(name.clone())
        })?;
        cache.insert(name, Collection::new(schema));

        Ok(())
    }

    /// Delete a collection and all its documents.
    ///
    /// Checks store for existence within transaction for atomic delete.
    ///
    /// # Errors
    ///
    /// - `CollectionNotFound` if the collection does not exist
    /// - `CacheUpdateFailedAfterCommit` if the collection was deleted but cache update failed
    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        // Get collection_id from cache (read lock, released before async ops)
        let collection_id = {
            let cache = self.collections.read().map_err(|e| {
                tracing::error!(error = ?e, "Collection cache lock poisoned during delete");
                Error::LockPoisoned("collection cache lock poisoned during delete".into())
            })?;
            cache
                .get(name)
                .map(|c| c.collection_id().to_string())
                .ok_or_else(|| Error::CollectionNotFound(name.to_string()))?
        };

        let txn = self.new_txn(false).await?;
        let schema_key = CollectionNameKey::new(name);

        // Verify collection still exists in store (must drop systemstore before discard)
        let exists = {
            let systemstore = txn.systemstore()?;
            systemstore
                .get(&schema_key.bytes())
                .await
                .map_err(Error::Storage)?
                .is_some()
        };

        if !exists {
            if let Err(discard_err) = txn.discard() {
                tracing::error!(
                    error = %discard_err,
                    collection_name = %name,
                    "Transaction discard failed while handling CollectionNotFound"
                );
            }
            return Err(Error::CollectionNotFound(name.to_string()));
        }

        // Delete schema and documents
        {
            let systemstore = txn.systemstore()?;
            systemstore
                .delete(&schema_key.bytes())
                .await
                .map_err(Error::Storage)?;

            let datastore = txn.datastore()?;
            let doc_prefix = format!("/d/{}/", collection_id);
            let opts = IterOptions::new().with_prefix(doc_prefix.as_bytes().to_vec());

            let mut iter = datastore.iterator(opts).await.map_err(|e| {
                tracing::error!(
                    error = ?e,
                    collection_name = %name,
                    "Failed to create iterator for document deletion"
                );
                Error::Storage(e)
            })?;
            let mut keys_to_delete = Vec::new();

            while let Some(pair) = iter.next().await.map_err(|e| {
                tracing::error!(
                    error = ?e,
                    collection_name = %name,
                    documents_found = keys_to_delete.len(),
                    "Failed to iterate documents during collection deletion"
                );
                Error::Storage(e)
            })? {
                keys_to_delete.push(pair.key.clone());
            }
            iter.close().await.map_err(|e| {
                tracing::error!(
                    error = ?e,
                    collection_name = %name,
                    "Failed to close iterator during collection deletion"
                );
                Error::Storage(e)
            })?;

            tracing::debug!(
                collection_name = %name,
                documents_to_delete = keys_to_delete.len(),
                "Deleting documents for collection"
            );

            for (i, key) in keys_to_delete.iter().enumerate() {
                datastore.delete(key).await.map_err(|e| {
                    tracing::error!(
                        error = ?e,
                        collection_name = %name,
                        documents_deleted = i,
                        documents_total = keys_to_delete.len(),
                        "Failed to delete document during collection deletion"
                    );
                    Error::Storage(e)
                })?;
            }
        }

        txn.commit().await?;

        // Update cache after successful commit.
        // If this fails, the collection IS deleted but still in cache.
        // A restart will recover by loading from store.
        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(
                error = ?e,
                collection_name = %name,
                "Collection cache lock poisoned during delete - collection WAS deleted from store. Restart will recover."
            );
            Error::CacheUpdateFailedAfterCommit(name.to_string())
        })?;
        cache.remove(name);

        Ok(())
    }

    /// List all collection names.
    pub fn list_collections(&self) -> Result<Vec<String>> {
        let cache = self.collections.read().map_err(|e| {
            tracing::error!(error = ?e, "Collection cache lock poisoned during list");
            Error::LockPoisoned("collection cache lock poisoned during list".into())
        })?;
        Ok(cache.keys().cloned().collect())
    }

    /// Get a collection by name.
    pub fn get_collection(&self, name: &str) -> Result<Option<Collection>> {
        let cache = self
            .collections
            .read()
            .map_err(|e| {
                tracing::error!(error = ?e, collection_name = %name, "Collection cache lock poisoned during get");
                Error::LockPoisoned("collection cache lock poisoned during get".into())
            })?;
        Ok(cache.get(name).cloned())
    }

    /// Check if a collection exists.
    pub fn has_collection(&self, name: &str) -> Result<bool> {
        let cache = self
            .collections
            .read()
            .map_err(|e| {
                tracing::error!(error = ?e, collection_name = %name, "Collection cache lock poisoned during has_collection");
                Error::LockPoisoned("collection cache lock poisoned during has_collection".into())
            })?;
        Ok(cache.contains_key(name))
    }

    /// Get a snapshot of all collections (for use by DbTransactionRegistry).
    ///
    /// Returns an immutable snapshot that provides snapshot isolation for transactions.
    pub fn collections_snapshot(&self) -> Result<CollectionSnapshot> {
        let cache = self.collections.read().map_err(|e| {
            tracing::error!(error = ?e, "Collection cache lock poisoned during snapshot");
            Error::LockPoisoned("collection cache lock poisoned during snapshot".into())
        })?;
        Ok(CollectionSnapshot::new(cache.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::backends::MemoryStore;

    #[tokio::test]
    async fn test_db_new_txn() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        let txn = db.new_txn(false).await.unwrap();
        assert_eq!(txn.id().unwrap(), 1);

        let txn2 = db.new_txn(false).await.unwrap();
        assert_eq!(txn2.id().unwrap(), 2);
    }

    #[tokio::test]
    async fn test_db_txn_isolation() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        // Write in first transaction
        let txn1 = db.new_txn(false).await.unwrap();
        txn1.datastore()
            .unwrap()
            .set(b"key", b"value1")
            .await
            .unwrap();
        txn1.commit().await.unwrap();

        // Read in second transaction
        let txn2 = db.new_txn(true).await.unwrap();
        let value = txn2.datastore().unwrap().get(b"key").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_db_with_txn() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        // Execute with_txn that commits
        db.with_txn(false, |_txn| {
            // Sync closure - use with_txn_async for async operations
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_db_options() {
        let store = MemoryStore::new();
        let options = DbOptions {
            max_txn_retries: Some(5),
            chunk_size: Some(1024 * 1024),
        };
        let db = DB::with_options(store, options.clone());

        assert_eq!(db.options().max_txn_retries, Some(5));
        assert_eq!(db.options().chunk_size, Some(1024 * 1024));
    }

    #[tokio::test]
    async fn test_db_with_txn_async_commits_on_success() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        // Execute async operation that succeeds
        db.with_txn_async(false, |txn| async move {
            txn.datastore()
                .unwrap()
                .set(b"key", b"value")
                .await
                .unwrap();
            (txn, Ok(()))
        })
        .await
        .unwrap();

        // Verify data was committed
        let txn = db.new_txn(true).await.unwrap();
        let value = txn.datastore().unwrap().get(b"key").await.unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_db_with_txn_async_discards_on_error() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        // Execute async operation that fails
        let result: Result<()> = db
            .with_txn_async(false, |txn| async move {
                txn.datastore()
                    .unwrap()
                    .set(b"key", b"value")
                    .await
                    .unwrap();
                (txn, Err(Error::Other("test error".into())))
            })
            .await;

        assert!(result.is_err());

        // Verify data was NOT committed (discarded)
        let txn = db.new_txn(true).await.unwrap();
        let value = txn.datastore().unwrap().get(b"key").await.unwrap();
        assert_eq!(value, None);
    }

    use schema::{CollectionVersion, FieldDescription, FieldKind};

    fn test_users_schema() -> CollectionVersion {
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
    async fn test_create_collection_persists_schema() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        let schema = test_users_schema();
        db.create_collection(schema).await.unwrap();

        assert!(db.has_collection("Users").unwrap());
        let coll = db.get_collection("Users").unwrap().unwrap();
        assert_eq!(coll.name(), "Users");
    }

    #[tokio::test]
    async fn test_create_duplicate_collection_fails() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        let schema = test_users_schema();
        db.create_collection(schema.clone()).await.unwrap();

        // Second create with same name should fail
        let result = db.create_collection(schema).await;
        assert!(
            matches!(result, Err(Error::CollectionAlreadyExists(_))),
            "Expected CollectionAlreadyExists, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_create_collection_with_invalid_name_fails() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        // Empty name should fail
        let empty_schema = CollectionVersion::new(
            "",
            "v1",
            "col-empty",
            vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
        );
        let result = db.create_collection(empty_schema).await;
        assert!(
            matches!(result, Err(Error::InvalidCollectionName(_))),
            "Expected InvalidCollectionName for empty name, got: {:?}",
            result
        );

        // Name with slash should fail
        let slash_schema = CollectionVersion::new(
            "Users/Posts",
            "v1",
            "col-slash",
            vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
        );
        let result = db.create_collection(slash_schema).await;
        assert!(
            matches!(result, Err(Error::InvalidCollectionName(_))),
            "Expected InvalidCollectionName for name with slash, got: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_list_collections_empty() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        let collections = db.list_collections().unwrap();
        assert!(collections.is_empty());
    }

    #[tokio::test]
    async fn test_list_collections_multiple() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        db.create_collection(test_users_schema()).await.unwrap();
        db.create_collection(CollectionVersion::new(
            "Posts",
            "v1",
            "col-posts",
            vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
        ))
        .await
        .unwrap();

        let mut collections = db.list_collections().unwrap();
        collections.sort();
        assert_eq!(collections, vec!["Posts", "Users"]);
    }

    #[tokio::test]
    async fn test_delete_collection_removes_data() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        db.create_collection(test_users_schema()).await.unwrap();
        assert!(db.has_collection("Users").unwrap());

        db.delete_collection("Users").await.unwrap();
        assert!(!db.has_collection("Users").unwrap());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_collection_fails() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        let result = db.delete_collection("Nonexistent").await;
        assert!(matches!(result, Err(Error::CollectionNotFound(_))));
    }

    #[tokio::test]
    async fn test_has_collection() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        assert!(!db.has_collection("Users").unwrap());

        db.create_collection(test_users_schema()).await.unwrap();
        assert!(db.has_collection("Users").unwrap());
    }

    #[tokio::test]
    async fn test_get_collection() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        assert!(db.get_collection("Users").unwrap().is_none());

        db.create_collection(test_users_schema()).await.unwrap();
        let coll = db.get_collection("Users").unwrap().unwrap();
        assert_eq!(coll.collection_id(), "col-users");
    }

    #[tokio::test]
    async fn test_open_loads_existing_collections() {
        let store = MemoryStore::new();

        {
            let db = DB::new(store.clone());
            db.create_collection(test_users_schema()).await.unwrap();
        }

        let db = DB::open(store).await.unwrap();
        assert!(db.has_collection("Users").unwrap());
        let coll = db.get_collection("Users").unwrap().unwrap();
        assert_eq!(coll.name(), "Users");
    }

    #[tokio::test]
    async fn test_open_with_options_loads_existing_collections() {
        let store = MemoryStore::new();

        {
            let db = DB::new(store.clone());
            db.create_collection(test_users_schema()).await.unwrap();
        }

        // Use open_with_options with custom options
        let opts = DbOptions {
            max_txn_retries: Some(10),
            chunk_size: Some(1024),
        };
        let db = DB::open_with_options(store, opts).await.unwrap();

        // Verify collections loaded correctly
        assert!(db.has_collection("Users").unwrap());
        let coll = db.get_collection("Users").unwrap().unwrap();
        assert_eq!(coll.name(), "Users");

        // Verify options were applied
        assert_eq!(db.options().max_txn_retries, Some(10));
        assert_eq!(db.options().chunk_size, Some(1024));
    }

    #[tokio::test]
    async fn test_open_empty_store_returns_empty_collections() {
        let store = MemoryStore::new();
        let db = DB::open(store).await.unwrap();
        assert!(db.list_collections().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_collections_snapshot() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        db.create_collection(test_users_schema()).await.unwrap();

        let snapshot = db.collections_snapshot().unwrap();
        assert_eq!(snapshot.len(), 1);
        assert!(snapshot.contains("Users"));
    }

    #[tokio::test]
    async fn test_concurrent_create_same_collection() {
        let store = MemoryStore::new();
        let db = Arc::new(DB::new(store));

        let schema = test_users_schema();

        let db1 = db.clone();
        let schema1 = schema.clone();
        let handle1 = tokio::spawn(async move { db1.create_collection(schema1).await });

        let db2 = db.clone();
        let schema2 = schema.clone();
        let handle2 = tokio::spawn(async move { db2.create_collection(schema2).await });

        let (r1, r2) = tokio::join!(handle1, handle2);
        let results = [r1.unwrap(), r2.unwrap()];

        let successes = results.iter().filter(|r| r.is_ok()).count();
        let failures = results.iter().filter(|r| r.is_err()).count();

        assert_eq!(successes, 1, "Exactly one concurrent create should succeed");
        assert_eq!(failures, 1, "Exactly one concurrent create should fail");

        // Cache should have exactly one collection
        assert_eq!(db.list_collections().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_concurrent_delete_same_collection() {
        let store = MemoryStore::new();
        let db = Arc::new(DB::new(store));

        db.create_collection(test_users_schema()).await.unwrap();

        let db1 = db.clone();
        let handle1 = tokio::spawn(async move { db1.delete_collection("Users").await });

        let db2 = db.clone();
        let handle2 = tokio::spawn(async move { db2.delete_collection("Users").await });

        let (r1, r2) = tokio::join!(handle1, handle2);
        let results = [r1.unwrap(), r2.unwrap()];

        let successes = results.iter().filter(|r| r.is_ok()).count();
        let failures = results.iter().filter(|r| r.is_err()).count();

        assert_eq!(successes, 1, "Exactly one concurrent delete should succeed");
        assert_eq!(failures, 1, "Exactly one concurrent delete should fail");

        // Cache should be empty
        assert!(db.list_collections().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_load_collections_corrupted_json_returns_error() {
        use storage::corekv::Key;
        use storage::keys::systemstore::CollectionNameKey;

        let store = MemoryStore::new();

        // Write corrupted JSON directly to the store
        {
            let db = DB::new(store.clone());
            let txn = db.new_txn(false).await.unwrap();

            // Use block to ensure systemstore is dropped before commit
            {
                let systemstore = txn.systemstore().unwrap();
                let key = CollectionNameKey::new("CorruptedCollection");
                systemstore
                    .set(&key.bytes(), b"not valid json {{{")
                    .await
                    .unwrap();
            }

            txn.commit().await.unwrap();
        }

        // Try to open the database - should fail on load_collections
        let result = DB::open(store).await;
        assert!(result.is_err(), "Expected error loading corrupted JSON");
        match result {
            Err(Error::Serialization(msg)) => {
                assert!(
                    msg.contains("deserialize"),
                    "Error should mention deserialization: {}",
                    msg
                );
            }
            Err(e) => panic!("Expected Serialization error, got: {:?}", e),
            Ok(_) => panic!("Expected error but got Ok"),
        }
    }

    #[tokio::test]
    async fn test_delete_collection_removes_all_documents_from_store() {
        use document::{Document, NormalValue};

        let store = MemoryStore::new();
        let db = Arc::new(DB::new(store.clone()));

        // Create collection and add documents
        db.create_collection(test_users_schema()).await.unwrap();
        let collection = db.get_collection("Users").unwrap().unwrap();

        {
            let txn = db.new_txn(false).await.unwrap();
            for i in 0..5 {
                let mut doc = Document::new();
                doc.set("name", NormalValue::String(format!("User{}", i)));
                doc.set("age", NormalValue::Int(20 + i));
                doc.generate_and_set_doc_id().unwrap();
                collection.create(&txn, &doc).await.unwrap();
            }
            txn.commit().await.unwrap();
        }

        // Verify documents exist
        {
            let txn = db.new_txn(true).await.unwrap();
            let docs = collection.get_all(&txn).await.unwrap();
            assert_eq!(docs.len(), 5, "Should have 5 documents before delete");
            txn.discard().unwrap();
        }

        // Delete the collection
        db.delete_collection("Users").await.unwrap();

        // Verify documents are gone from the store by checking raw keys
        let count = {
            let txn = db.new_txn(true).await.unwrap();
            let doc_prefix = "/d/col-users/";
            let opts =
                storage::corekv::IterOptions::new().with_prefix(doc_prefix.as_bytes().to_vec());

            let count = {
                let datastore = txn.datastore().unwrap();
                let mut iter = datastore.iterator(opts).await.unwrap();

                let mut c = 0;
                while iter.next().await.unwrap().is_some() {
                    c += 1;
                }
                iter.close().await.unwrap();
                c
            };

            txn.discard().unwrap();
            count
        };

        assert_eq!(count, 0, "All documents should be deleted from store");
    }

    #[tokio::test]
    async fn test_schema_roundtrip_preserves_all_fields() {
        let store = MemoryStore::new();

        let original_schema = CollectionVersion::new(
            "TestCollection",
            "v1",
            "col-test-123",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
                FieldDescription::new("4", "active", FieldKind::bool()),
            ],
        );

        // Create collection and persist
        {
            let db = DB::new(store.clone());
            db.create_collection(original_schema.clone()).await.unwrap();
        }

        // Reopen and load from store
        let db = DB::open(store).await.unwrap();
        let loaded = db
            .get_collection("TestCollection")
            .unwrap()
            .expect("Collection should exist");
        let loaded_schema = loaded.schema();

        // Verify all fields are preserved
        assert_eq!(loaded_schema.name, original_schema.name);
        assert_eq!(loaded_schema.version_id, original_schema.version_id);
        assert_eq!(loaded_schema.collection_id, original_schema.collection_id);
        assert_eq!(
            loaded_schema.fields.len(),
            original_schema.fields.len(),
            "Field count should match"
        );

        for (loaded_field, original_field) in loaded_schema
            .fields
            .iter()
            .zip(original_schema.fields.iter())
        {
            assert_eq!(loaded_field.id, original_field.id, "Field ID mismatch");
            assert_eq!(
                loaded_field.name, original_field.name,
                "Field name mismatch"
            );
        }
    }

    #[tokio::test]
    async fn test_delete_collection_cache_store_inconsistency() {
        // Test behavior when cache and store diverge (collection in cache but not store)
        // This tests the safety check in delete_collection that verifies store existence
        use storage::corekv::Key;
        use storage::keys::systemstore::CollectionNameKey;

        let store = MemoryStore::new();
        let db = Arc::new(DB::new(store.clone()));

        // Create a collection normally
        db.create_collection(test_users_schema()).await.unwrap();
        assert!(db.has_collection("Users").unwrap());

        // Manually delete from store, bypassing cache (simulating inconsistency)
        {
            let txn = db.new_txn(false).await.unwrap();
            let key = CollectionNameKey::new("Users");
            {
                let systemstore = txn.systemstore().unwrap();
                systemstore.delete(&key.bytes()).await.unwrap();
            }
            txn.commit().await.unwrap();
        }

        // Cache still has it
        assert!(db.has_collection("Users").unwrap());

        // Now try to delete - should fail gracefully with CollectionNotFound
        // because the store check catches the inconsistency
        let result = db.delete_collection("Users").await;
        assert!(result.is_err());
        match result {
            Err(Error::CollectionNotFound(name)) => {
                assert_eq!(name, "Users");
            }
            Err(e) => panic!("Expected CollectionNotFound, got: {:?}", e),
            Ok(_) => panic!("Expected error but got Ok"),
        }
    }

    #[tokio::test]
    async fn test_concurrent_create_and_delete_same_collection() {
        // Test concurrent create + delete of the same collection
        let store = MemoryStore::new();
        let db = Arc::new(DB::new(store));

        // Create collection first
        db.create_collection(test_users_schema()).await.unwrap();

        // Now race create and delete
        let db1 = db.clone();
        let schema = test_users_schema();
        let handle1 = tokio::spawn(async move {
            // Delete then create
            db1.delete_collection("Users").await?;
            db1.create_collection(schema).await
        });

        let db2 = db.clone();
        let handle2 = tokio::spawn(async move {
            // Just delete
            db2.delete_collection("Users").await
        });

        let (r1, r2) = tokio::join!(handle1, handle2);

        // At least one should fail (either both tried to delete, or create raced with delete)
        let r1 = r1.unwrap();
        let r2 = r2.unwrap();

        // The important thing is no panics and the database is in a consistent state
        // Either collection exists or it doesn't
        let exists = db.has_collection("Users").unwrap();
        let list = db.list_collections().unwrap();

        // If collection exists, it should be in the list
        if exists {
            assert!(list.contains(&"Users".to_string()));
        } else {
            assert!(!list.contains(&"Users".to_string()));
        }

        // Log outcomes for debugging
        println!(
            "Concurrent create+delete results: r1={:?}, r2={:?}, exists={}",
            r1.is_ok(),
            r2.is_ok(),
            exists
        );
    }
}
