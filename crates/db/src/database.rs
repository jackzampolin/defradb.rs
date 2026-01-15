/// Database struct for DefraDB matching Go's internal/db/db.go.
///
/// The DB struct is the main entry point for DefraDB operations.
/// It manages the root store, creates transactions, and provides
/// access to collections.
use crate::collection::Collection;
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

    // =========================================================================
    // Collection Lifecycle Management
    // =========================================================================

    /// Load all collections from the SystemStore into the in-memory cache.
    pub async fn load_collections(&self) -> Result<()> {
        let txn = self.new_txn(true).await?;
        let prefix = CollectionNameKey::name_prefix();
        let mut collections = HashMap::new();

        // Use a block to ensure systemstore reference is dropped before discard
        {
            let systemstore = txn.systemstore()?;
            let opts = IterOptions::new().with_prefix(prefix.clone());

            let mut iter = systemstore.iterator(opts).await.map_err(Error::Storage)?;

            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                // Extract collection name from key: /collection/name/{name}
                let key_str = String::from_utf8_lossy(&pair.key);
                let prefix_str = String::from_utf8_lossy(&prefix);
                let name = key_str
                    .strip_prefix(&*prefix_str)
                    .unwrap_or(&key_str)
                    .to_string();

                // Deserialize CollectionVersion from JSON
                let schema: CollectionVersion = serde_json::from_slice(&pair.value)
                    .map_err(|e| Error::Serialization(format!("failed to deserialize schema: {}", e)))?;

                collections.insert(name, Collection::new(schema));
            }

            iter.close().await.map_err(Error::Storage)?;
        }

        txn.discard().map_err(|_| Error::TxnNotActive)?;

        // Update in-memory cache
        let mut cache = self
            .collections
            .write()
            .map_err(|_| Error::Other("collection cache lock poisoned".into()))?;
        *cache = collections;

        Ok(())
    }

    /// Create a new collection with schema persistence.
    pub async fn create_collection(&self, schema: CollectionVersion) -> Result<()> {
        let name = schema.name.clone();

        // Check if collection already exists
        if self.has_collection(&name) {
            return Err(Error::CollectionAlreadyExists(name));
        }

        // Persist schema to SystemStore
        let txn = self.new_txn(false).await?;

        let key = CollectionNameKey::new(&name);
        let data = serde_json::to_vec(&schema)
            .map_err(|e| Error::Serialization(format!("failed to serialize schema: {}", e)))?;

        // Use a block to ensure systemstore reference is dropped before commit
        {
            let systemstore = txn.systemstore()?;
            systemstore
                .set(&key.bytes(), &data)
                .await
                .map_err(Error::Storage)?;
        }

        txn.commit().await?;

        // Update in-memory cache
        let mut cache = self
            .collections
            .write()
            .map_err(|_| Error::Other("collection cache lock poisoned".into()))?;
        cache.insert(name, Collection::new(schema));

        Ok(())
    }

    /// Delete a collection and all its documents.
    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        // Check if collection exists
        let collection = self
            .get_collection(name)
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))?;

        let txn = self.new_txn(false).await?;

        // Use a block to ensure store references are dropped before commit
        {
            // Delete all documents in the collection
            let datastore = txn.datastore()?;
            let doc_prefix = format!("/d/{}/", collection.collection_id());
            let opts = IterOptions::new().with_prefix(doc_prefix.as_bytes().to_vec());

            let mut iter = datastore.iterator(opts).await.map_err(Error::Storage)?;
            let mut keys_to_delete = Vec::new();

            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                keys_to_delete.push(pair.key.clone());
            }
            iter.close().await.map_err(Error::Storage)?;

            for key in keys_to_delete {
                datastore.delete(&key).await.map_err(Error::Storage)?;
            }

            // Delete schema from SystemStore
            let systemstore = txn.systemstore()?;
            let key = CollectionNameKey::new(name);
            systemstore
                .delete(&key.bytes())
                .await
                .map_err(Error::Storage)?;
        }

        txn.commit().await?;

        // Remove from in-memory cache
        let mut cache = self
            .collections
            .write()
            .map_err(|_| Error::Other("collection cache lock poisoned".into()))?;
        cache.remove(name);

        Ok(())
    }

    /// List all collection names.
    pub fn list_collections(&self) -> Result<Vec<String>> {
        let cache = self
            .collections
            .read()
            .map_err(|_| Error::Other("collection cache lock poisoned".into()))?;
        Ok(cache.keys().cloned().collect())
    }

    /// Get a collection by name.
    pub fn get_collection(&self, name: &str) -> Option<Collection> {
        self.collections
            .read()
            .ok()
            .and_then(|cache| cache.get(name).cloned())
    }

    /// Check if a collection exists.
    pub fn has_collection(&self, name: &str) -> bool {
        self.collections
            .read()
            .ok()
            .map(|cache| cache.contains_key(name))
            .unwrap_or(false)
    }

    /// Get a snapshot of all collections (for use by DbTransactionRegistry).
    pub fn collections_snapshot(&self) -> HashMap<String, Collection> {
        self.collections
            .read()
            .ok()
            .map(|cache| cache.clone())
            .unwrap_or_default()
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
            // We need async operations inside, but this closure is sync
            // This is a limitation - we'll address in with_txn_async
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

    // =========================================================================
    // Collection Lifecycle Tests
    // =========================================================================

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

        assert!(db.has_collection("Users"));
        let coll = db.get_collection("Users").unwrap();
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
        assert!(matches!(result, Err(Error::CollectionAlreadyExists(_))));
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
        assert!(db.has_collection("Users"));

        db.delete_collection("Users").await.unwrap();
        assert!(!db.has_collection("Users"));
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

        assert!(!db.has_collection("Users"));

        db.create_collection(test_users_schema()).await.unwrap();
        assert!(db.has_collection("Users"));
    }

    #[tokio::test]
    async fn test_get_collection() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        assert!(db.get_collection("Users").is_none());

        db.create_collection(test_users_schema()).await.unwrap();
        let coll = db.get_collection("Users").unwrap();
        assert_eq!(coll.collection_id(), "col-users");
    }

    #[tokio::test]
    async fn test_open_loads_existing_collections() {
        // Create a store and populate it with a collection
        let store = MemoryStore::new();

        // First, create and populate DB
        {
            let db = DB::new(store.clone());
            db.create_collection(test_users_schema()).await.unwrap();
        }

        // Now open the same store with open() and verify collection loaded
        let db = DB::open(store).await.unwrap();
        assert!(db.has_collection("Users"));
        let coll = db.get_collection("Users").unwrap();
        assert_eq!(coll.name(), "Users");
    }

    #[tokio::test]
    async fn test_collections_snapshot() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        db.create_collection(test_users_schema()).await.unwrap();

        let snapshot = db.collections_snapshot();
        assert_eq!(snapshot.len(), 1);
        assert!(snapshot.contains_key("Users"));
    }
}
