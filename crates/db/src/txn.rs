/// Database transaction wrapper matching Go's internal/db/txn.go.
///
/// DbTxn wraps a BasicTxn and adds:
/// - Explicit/implicit transaction handling
/// - Reference to the database for collection operations
use crate::error::{Error, Result};
use datastore::{BasicTxn, NamespaceView, RootView, TxnCallback};
use std::sync::Arc;
use storage::corekv::Store;

/// Database transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnState {
    /// Transaction is active and can perform operations.
    Active,
    /// Transaction has been committed.
    Committed,
    /// Transaction has been discarded.
    Discarded,
}

/// Database transaction wrapper.
///
/// This wraps a BasicTxn and provides:
/// - Explicit/implicit transaction handling
/// - Access to the underlying store for collection operations
///
/// Explicit transactions are created by the user and must be explicitly
/// committed or discarded. When a method receives an explicit transaction,
/// it should NOT commit or discard it.
///
/// Implicit transactions are created internally by database methods.
/// They are automatically committed on success and discarded on error.
pub struct DbTxn<S: Store> {
    /// The underlying BasicTxn.
    txn: Option<BasicTxn>,
    /// Whether this is an explicit transaction.
    explicit: bool,
    /// Current transaction state.
    state: TxnState,
    /// Phantom data for the store type.
    _marker: std::marker::PhantomData<S>,
}

impl<S: Store> DbTxn<S> {
    /// Create a new implicit DbTxn.
    pub fn new(txn: BasicTxn, _store: Arc<S>) -> Self {
        Self {
            txn: Some(txn),
            explicit: false,
            state: TxnState::Active,
            _marker: std::marker::PhantomData,
        }
    }

    /// Create a new explicit DbTxn.
    pub fn new_explicit(txn: BasicTxn, _store: Arc<S>) -> Self {
        Self {
            txn: Some(txn),
            explicit: true,
            state: TxnState::Active,
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
        self.txn
            .as_ref()
            .map(|t| t.id())
            .ok_or(Error::TxnNotActive)
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

    /// Commit the transaction.
    ///
    /// Returns an error for explicit transactions - use `force_commit()` instead.
    /// Returns an error if the transaction is not active.
    pub async fn commit(mut self) -> Result<()> {
        if self.explicit {
            return Err(Error::ExplicitTxnMustUseForce);
        }

        if self.state != TxnState::Active {
            return Err(Error::TxnNotActive);
        }

        if let Some(txn) = self.txn.take() {
            txn.commit().await.map_err(Error::Datastore)?;
        }
        self.state = TxnState::Committed;
        Ok(())
    }

    /// Discard the transaction.
    ///
    /// Returns an error for explicit transactions - use `force_discard()` instead.
    /// Returns an error if the transaction is not active.
    pub fn discard(mut self) -> Result<()> {
        if self.explicit {
            return Err(Error::ExplicitTxnMustUseForce);
        }

        if self.state != TxnState::Active {
            return Err(Error::TxnNotActive);
        }

        if let Some(txn) = self.txn.take() {
            txn.discard().map_err(Error::Datastore)?;
        }
        self.state = TxnState::Discarded;
        Ok(())
    }

    /// Actually commit the transaction, even if explicit.
    ///
    /// This should only be called by the transaction creator.
    pub async fn force_commit(mut self) -> Result<()> {
        if self.state != TxnState::Active {
            return Err(Error::TxnNotActive);
        }

        if let Some(txn) = self.txn.take() {
            txn.commit().await.map_err(Error::Datastore)?;
        }
        self.state = TxnState::Committed;
        Ok(())
    }

    /// Actually discard the transaction, even if explicit.
    ///
    /// This should only be called by the transaction creator.
    pub fn force_discard(mut self) -> Result<()> {
        if self.state != TxnState::Active {
            return Err(Error::TxnNotActive);
        }

        if let Some(txn) = self.txn.take() {
            txn.discard().map_err(Error::Datastore)?;
        }
        self.state = TxnState::Discarded;
        Ok(())
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
