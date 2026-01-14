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
    /// Reference to the store (for collection operations).
    #[allow(dead_code)]
    store: Arc<S>,
    /// Whether this is an explicit transaction.
    explicit: bool,
    /// Current transaction state.
    state: TxnState,
}

impl<S: Store> DbTxn<S> {
    /// Create a new implicit DbTxn.
    pub fn new(txn: BasicTxn, store: Arc<S>) -> Self {
        Self {
            txn: Some(txn),
            store,
            explicit: false,
            state: TxnState::Active,
        }
    }

    /// Create a new explicit DbTxn.
    pub fn new_explicit(txn: BasicTxn, store: Arc<S>) -> Self {
        Self {
            txn: Some(txn),
            store,
            explicit: true,
            state: TxnState::Active,
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
    pub fn id(&self) -> u64 {
        self.txn.as_ref().map(|t| t.id()).unwrap_or(0)
    }

    /// Check if this is a read-only transaction.
    pub fn is_readonly(&self) -> bool {
        self.txn.as_ref().map(|t| t.is_readonly()).unwrap_or(true)
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
    pub fn on_success(&mut self, callback: TxnCallback) {
        if let Some(txn) = &mut self.txn {
            txn.on_success(callback);
        }
    }

    /// Register a callback for commit error.
    pub fn on_error(&mut self, callback: TxnCallback) {
        if let Some(txn) = &mut self.txn {
            txn.on_error(callback);
        }
    }

    /// Register a callback for discard.
    pub fn on_discard(&mut self, callback: TxnCallback) {
        if let Some(txn) = &mut self.txn {
            txn.on_discard(callback);
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
            txn.discard();
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
    pub fn force_discard(mut self) {
        if self.state != TxnState::Active {
            return;
        }

        if let Some(txn) = self.txn.take() {
            txn.discard();
        }
        self.state = TxnState::Discarded;
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

        assert_eq!(txn.id(), 1);
        assert!(!txn.is_readonly());
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
        txn.force_discard();

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
}
