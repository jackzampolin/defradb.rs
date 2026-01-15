/// Database transaction wrapper matching Go's internal/db/txn.go.
///
/// DbTxn wraps a BasicTxn and adds:
/// - Explicit/implicit transaction handling
/// - Transaction-scoped collection cache (lazy loading)
/// - Reference to the database for collection operations
use crate::collection::Collection;
use crate::collection_cache::CollectionCache;
use crate::error::{Error, Result};
use datastore::{BasicTxn, NamespaceView, RootView, TxnCallback};
use schema::CollectionVersion;
use std::sync::Arc;
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::systemstore::CollectionNameKey;

/// Database transaction wrapper.
///
/// This wraps a BasicTxn and provides:
/// - Explicit/implicit transaction handling
/// - Transaction-scoped collection cache with lazy loading
/// - Access to the underlying store for collection operations
///
/// Explicit transactions are created by the user and must be explicitly
/// committed or discarded. When a method receives an explicit transaction,
/// it should NOT commit or discard it.
///
/// Implicit transactions are created internally by database methods.
/// They are automatically committed on success and discarded on error.
///
/// Transaction liveness is tracked by the `txn` field:
/// - `Some(txn)` = transaction is active
/// - `None` = transaction has been committed or discarded
///
/// The collection cache is populated lazily from the SystemStore on first
/// access, matching the Go DefraDB pattern for transaction isolation.
pub struct DbTxn<S: Store> {
    /// The underlying BasicTxn. `None` after commit/discard.
    txn: Option<BasicTxn>,
    /// Whether this is an explicit transaction.
    explicit: bool,
    /// Transaction-scoped collection cache (lazy loading from SystemStore).
    collection_cache: CollectionCache,
    /// Phantom data for the store type.
    _marker: std::marker::PhantomData<S>,
}

impl<S: Store> DbTxn<S> {
    /// Create a new implicit DbTxn.
    pub fn new(txn: BasicTxn, _store: Arc<S>) -> Self {
        Self {
            txn: Some(txn),
            explicit: false,
            collection_cache: CollectionCache::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Create a new explicit DbTxn.
    pub fn new_explicit(txn: BasicTxn, _store: Arc<S>) -> Self {
        Self {
            txn: Some(txn),
            explicit: true,
            collection_cache: CollectionCache::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Mark this transaction as explicit.
    ///
    /// Explicit transactions are not automatically committed/discarded
    /// when passed to database methods.
    pub fn make_explicit(&mut self) {
        self.explicit = true;
    }

    /// Check if this is an explicit transaction.
    pub fn is_explicit(&self) -> bool {
        self.explicit
    }

    /// Get the transaction ID.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn id(&self) -> Result<u64> {
        self.txn.as_ref().map(|t| t.id()).ok_or(Error::TxnNotActive)
    }

    /// Check if this is a read-only transaction.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn is_readonly(&self) -> Result<bool> {
        self.txn
            .as_ref()
            .map(|t| t.is_readonly())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the blockstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn blockstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.blockstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the datastore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn datastore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.datastore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the encstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn encstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.encstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the headstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn headstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.headstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the peerstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn peerstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.peerstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the systemstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn systemstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.systemstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the rootstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn rootstore(&self) -> Result<RootView> {
        self.txn
            .as_ref()
            .map(|t| t.rootstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Register a callback for successful commit.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn on_success(&mut self, callback: TxnCallback) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_success(callback);
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    /// Register a callback for commit error.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn on_error(&mut self, callback: TxnCallback) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_error(callback);
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    /// Register a callback for discard.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn on_discard(&mut self, callback: TxnCallback) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_discard(callback);
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    // =========================================================================
    // Collection Cache Methods (Transaction-scoped caching)
    // =========================================================================

    /// Get a collection by name, loading from SystemStore if not in cache.
    ///
    /// This implements lazy loading - collections are loaded on first access.
    /// Returns `None` if the collection doesn't exist in the store.
    ///
    /// Note: This method is structured to avoid holding `&mut self` across awaits,
    /// which allows futures using this method to be `Send`.
    pub async fn get_collection(&mut self, name: &str) -> Result<Option<&Collection>> {
        // Check cache first
        if self.collection_cache.contains(name) {
            return Ok(self.collection_cache.get(name));
        }

        // Cache miss: extract systemstore synchronously, then do async operation
        let systemstore = self.systemstore()?;
        let key = CollectionNameKey::new(name);

        // Load from store (no &self held during this await)
        let maybe_data = systemstore
            .get(&key.bytes())
            .await
            .map_err(Error::Storage)?;

        // Process result and update cache
        if let Some(data) = maybe_data {
            let schema: CollectionVersion = serde_json::from_slice(&data).map_err(|e| {
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
            self.collection_cache.add(Collection::new(schema));
            return Ok(self.collection_cache.get(name));
        }

        Ok(None)
    }

    /// Load all collections from SystemStore into the cache.
    ///
    /// This is called when listing collections or when we need to iterate
    /// over all collections. After calling this, `is_fully_populated()` returns true.
    pub async fn load_all_collections(&mut self) -> Result<()> {
        if self.collection_cache.is_fully_populated() {
            return Ok(());
        }

        let prefix = CollectionNameKey::name_prefix();
        let mut collections = Vec::new();

        let systemstore = self.systemstore()?;
        let opts = IterOptions::new().with_prefix(prefix.clone());

        let mut iter = systemstore.iterator(opts).await.map_err(|e| {
            tracing::error!(error = ?e, "Failed to create iterator during collection load");
            Error::Storage(e)
        })?;

        while let Some(pair) = iter.next().await.map_err(|e| {
            tracing::error!(error = ?e, "Failed to iterate collections during load");
            Error::Storage(e)
        })? {
            // Validate UTF-8 in key
            let key_str = String::from_utf8(pair.key.to_vec()).map_err(|e| {
                tracing::error!(
                    error = ?e,
                    key_bytes = ?&pair.key[..pair.key.len().min(50)],
                    "Collection key contains invalid UTF-8"
                );
                Error::Serialization(format!("collection key contains invalid UTF-8: {}", e))
            })?;

            let prefix_str = String::from_utf8(prefix.clone()).map_err(|e| {
                Error::Other(format!("internal error: prefix is not valid UTF-8: {}", e))
            })?;

            let name = key_str
                .strip_prefix(&prefix_str)
                .ok_or_else(|| {
                    Error::Other(format!(
                        "collection key '{}' does not match expected prefix '{}'",
                        key_str, prefix_str
                    ))
                })?
                .to_string();

            let schema: CollectionVersion = serde_json::from_slice(&pair.value).map_err(|e| {
                tracing::error!(
                    error = ?e,
                    collection_name = %name,
                    "Failed to deserialize schema for collection '{}': {}",
                    name,
                    e
                );
                Error::Serialization(format!(
                    "failed to deserialize schema for collection '{}': {}",
                    name, e
                ))
            })?;

            collections.push(Collection::new(schema));
        }

        iter.close().await.map_err(|e| {
            tracing::error!(error = ?e, "Failed to close iterator during collection load");
            Error::Storage(e)
        })?;

        self.collection_cache.populate(collections);
        Ok(())
    }

    /// Add a collection to the transaction-scoped cache.
    ///
    /// The key is derived from the collection's name to prevent key-name mismatches.
    pub fn cache_collection(&mut self, collection: Collection) {
        self.collection_cache.add(collection);
    }

    /// Remove a collection from the transaction-scoped cache.
    ///
    /// Called by delete_collection to update the cache after writing to store.
    pub fn uncache_collection(&mut self, name: &str) {
        self.collection_cache.remove(name);
    }

    /// Get the collection cache.
    ///
    /// Use this for read-only access to iterate over cached collections.
    pub fn collection_cache(&self) -> &CollectionCache {
        &self.collection_cache
    }

    /// Get mutable access to the collection cache.
    ///
    /// Use this for advanced cache manipulation (e.g., populate from snapshot).
    pub fn collection_cache_mut(&mut self) -> &mut CollectionCache {
        &mut self.collection_cache
    }

    // =========================================================================
    // Transaction Lifecycle Methods
    // =========================================================================

    /// Commit the transaction.
    ///
    /// Returns an error for explicit transactions - use `force_commit()` instead.
    /// Returns an error if the transaction is not active.
    pub async fn commit(mut self) -> Result<()> {
        if self.explicit {
            return Err(Error::ExplicitTxnMustUseForce);
        }

        match self.txn.take() {
            Some(txn) => {
                txn.commit().await.map_err(Error::Datastore)?;
                Ok(())
            }
            None => Err(Error::TxnNotActive),
        }
    }

    /// Discard the transaction.
    ///
    /// Returns an error for explicit transactions - use `force_discard()` instead.
    /// Returns an error if the transaction is not active.
    pub fn discard(mut self) -> Result<()> {
        if self.explicit {
            return Err(Error::ExplicitTxnMustUseForce);
        }

        match self.txn.take() {
            Some(txn) => {
                txn.discard().map_err(Error::Datastore)?;
                Ok(())
            }
            None => Err(Error::TxnNotActive),
        }
    }

    /// Actually commit the transaction, even if explicit.
    ///
    /// This should only be called by the transaction creator.
    pub async fn force_commit(mut self) -> Result<()> {
        match self.txn.take() {
            Some(txn) => {
                txn.commit().await.map_err(Error::Datastore)?;
                Ok(())
            }
            None => Err(Error::TxnNotActive),
        }
    }

    /// Actually discard the transaction, even if explicit.
    ///
    /// This should only be called by the transaction creator.
    pub fn force_discard(mut self) -> Result<()> {
        match self.txn.take() {
            Some(txn) => {
                txn.discard().map_err(Error::Datastore)?;
                Ok(())
            }
            None => Err(Error::TxnNotActive),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastore::BasicTxn;
    use storage::backends::MemoryStore;

    #[tokio::test]
    async fn test_db_txn_basic() {
        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());

        assert_eq!(txn.id().unwrap(), 1);
        assert!(!txn.is_readonly().unwrap());
        assert!(!txn.is_explicit());
    }

    #[tokio::test]
    async fn test_db_txn_explicit() {
        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new_explicit(basic_txn, store.clone());

        assert!(txn.is_explicit());
    }

    #[tokio::test]
    async fn test_db_txn_make_explicit() {
        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let mut txn = DbTxn::new(basic_txn, store.clone());

        assert!(!txn.is_explicit());
        txn.make_explicit();
        assert!(txn.is_explicit());
    }

    #[tokio::test]
    async fn test_db_txn_write_and_commit() {
        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());

        // Write data
        txn.datastore()
            .unwrap()
            .set(b"key", b"value")
            .await
            .unwrap();

        // Commit
        txn.commit().await.unwrap();

        // Verify data persisted
        let basic_txn = BasicTxn::new(&*store, 2, true).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());
        let value = txn.datastore().unwrap().get(b"key").await.unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn test_db_txn_write_and_discard() {
        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());

        // Write data
        txn.datastore()
            .unwrap()
            .set(b"key", b"value")
            .await
            .unwrap();

        // Discard
        txn.discard().unwrap();

        // Verify data NOT persisted
        let basic_txn = BasicTxn::new(&*store, 2, true).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());
        let value = txn.datastore().unwrap().get(b"key").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_db_txn_force_commit() {
        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new_explicit(basic_txn, store.clone());

        // Write data
        txn.datastore()
            .unwrap()
            .set(b"key", b"value")
            .await
            .unwrap();

        // Force commit even though explicit
        txn.force_commit().await.unwrap();

        // Verify data persisted
        let basic_txn = BasicTxn::new(&*store, 2, true).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());
        let value = txn.datastore().unwrap().get(b"key").await.unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
    }

    // Negative tests for error conditions

    #[tokio::test]
    async fn test_db_txn_explicit_commit_returns_error() {
        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new_explicit(basic_txn, store.clone());

        // Commit on explicit transaction should return error
        let result = txn.commit().await;
        assert!(matches!(result, Err(Error::ExplicitTxnMustUseForce)));
    }

    #[tokio::test]
    async fn test_db_txn_explicit_discard_returns_error() {
        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new_explicit(basic_txn, store.clone());

        // Discard on explicit transaction should return error
        let result = txn.discard();
        assert!(matches!(result, Err(Error::ExplicitTxnMustUseForce)));
    }

    #[tokio::test]
    async fn test_db_txn_force_discard() {
        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new_explicit(basic_txn, store.clone());

        // Write data
        txn.datastore()
            .unwrap()
            .set(b"key", b"value")
            .await
            .unwrap();

        // Force discard even though explicit
        txn.force_discard().unwrap();

        // Verify data NOT persisted
        let basic_txn = BasicTxn::new(&*store, 2, true).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());
        let value = txn.datastore().unwrap().get(b"key").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_db_txn_accessor_returns_all_stores() {
        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());

        // All accessor methods should succeed on active transaction
        assert!(txn.blockstore().is_ok());
        assert!(txn.datastore().is_ok());
        assert!(txn.encstore().is_ok());
        assert!(txn.headstore().is_ok());
        assert!(txn.peerstore().is_ok());
        assert!(txn.systemstore().is_ok());
        assert!(txn.rootstore().is_ok());
    }

    // Transaction state and callback tests

    #[tokio::test]
    async fn test_db_txn_callbacks_executed_on_commit() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let mut txn = DbTxn::new(basic_txn, store.clone());

        let success_called = Arc::new(AtomicBool::new(false));
        let success_clone = success_called.clone();
        txn.on_success(Box::new(move || {
            success_clone.store(true, Ordering::SeqCst);
        }))
        .unwrap();

        txn.commit().await.unwrap();
        assert!(success_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_db_txn_callbacks_executed_on_discard() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let mut txn = DbTxn::new(basic_txn, store.clone());

        let discard_called = Arc::new(AtomicBool::new(false));
        let discard_clone = discard_called.clone();
        txn.on_discard(Box::new(move || {
            discard_clone.store(true, Ordering::SeqCst);
        }))
        .unwrap();

        txn.discard().unwrap();
        assert!(discard_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_db_txn_readonly_cannot_write() {
        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, true).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());

        assert!(txn.is_readonly().unwrap());

        // Attempting to write should fail
        let result = txn.datastore().unwrap().set(b"key", b"value").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_db_txn_id_increments() {
        let store = Arc::new(MemoryStore::new());

        let basic_txn1 = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn1 = DbTxn::new(basic_txn1, store.clone());
        assert_eq!(txn1.id().unwrap(), 1);

        let basic_txn2 = BasicTxn::new(&*store, 2, false).await.unwrap();
        let txn2 = DbTxn::new(basic_txn2, store.clone());
        assert_eq!(txn2.id().unwrap(), 2);

        let basic_txn3 = BasicTxn::new(&*store, 100, false).await.unwrap();
        let txn3 = DbTxn::new(basic_txn3, store.clone());
        assert_eq!(txn3.id().unwrap(), 100);
    }

    #[tokio::test]
    async fn test_db_txn_multiple_writes_single_commit() {
        let store = Arc::new(MemoryStore::new());
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());

        // Multiple writes in single transaction
        txn.datastore()
            .unwrap()
            .set(b"key1", b"value1")
            .await
            .unwrap();
        txn.datastore()
            .unwrap()
            .set(b"key2", b"value2")
            .await
            .unwrap();
        txn.datastore()
            .unwrap()
            .set(b"key3", b"value3")
            .await
            .unwrap();

        txn.commit().await.unwrap();

        // Verify all persisted
        let basic_txn = BasicTxn::new(&*store, 2, true).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());
        assert_eq!(
            txn.datastore().unwrap().get(b"key1").await.unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            txn.datastore().unwrap().get(b"key2").await.unwrap(),
            Some(b"value2".to_vec())
        );
        assert_eq!(
            txn.datastore().unwrap().get(b"key3").await.unwrap(),
            Some(b"value3".to_vec())
        );
    }

    #[tokio::test]
    async fn test_db_txn_overwrite_value() {
        let store = Arc::new(MemoryStore::new());

        // Write initial value
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());
        txn.datastore()
            .unwrap()
            .set(b"key", b"initial")
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Overwrite value
        let basic_txn = BasicTxn::new(&*store, 2, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());
        txn.datastore()
            .unwrap()
            .set(b"key", b"updated")
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Verify updated value
        let basic_txn = BasicTxn::new(&*store, 3, true).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());
        assert_eq!(
            txn.datastore().unwrap().get(b"key").await.unwrap(),
            Some(b"updated".to_vec())
        );
    }

    #[tokio::test]
    async fn test_db_txn_delete_value() {
        let store = Arc::new(MemoryStore::new());

        // Write initial value
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());
        txn.datastore()
            .unwrap()
            .set(b"key", b"value")
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Delete value
        let basic_txn = BasicTxn::new(&*store, 2, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());
        txn.datastore().unwrap().delete(b"key").await.unwrap();
        txn.commit().await.unwrap();

        // Verify deleted
        let basic_txn = BasicTxn::new(&*store, 3, true).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());
        assert_eq!(txn.datastore().unwrap().get(b"key").await.unwrap(), None);
    }
}
