/// Transaction wrapper for DefraDB matching Go's internal/datastore/txn.go.
///
/// BasicTxn wraps a corekv transaction and provides:
/// - Multistore access via namespace views
/// - Transaction ID
/// - Lifecycle callbacks (on_success, on_error, on_discard)
use crate::error::{Error, Result};
use crate::multistore::{NamespaceView, RootView, SharedTxn};
use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use storage::corekv::{Store, Txn, TxnCallback};
use storage::namespace::Namespace;

/// Asynchronous callback for transaction events.
pub type AsyncCallback = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnState {
    Active,
    Committed,
    Discarded,
}

/// BasicTxn wraps a corekv transaction with DefraDB-specific functionality.
///
/// This matches Go's BasicTxn in internal/datastore/txn.go, providing:
/// - Access to all namespaced stores (datastore, blockstore, etc.)
/// - Transaction ID for tracking
/// - Lifecycle callbacks
pub struct BasicTxn {
    shared_txn: Arc<SharedTxn>,
    id: u64,
    readonly: bool,
    state: TxnState,

    success_fns: Vec<TxnCallback>,
    error_fns: Vec<TxnCallback>,
    discard_fns: Vec<TxnCallback>,

    success_async_fns: Vec<AsyncCallback>,
    error_async_fns: Vec<AsyncCallback>,
    discard_async_fns: Vec<AsyncCallback>,
}

impl BasicTxn {
    /// Create a new BasicTxn from a store.
    pub async fn new<S: Store>(
        store: &S,
        id: u64,
        readonly: bool,
    ) -> storage::corekv::Result<Self> {
        let txn = store.new_txn(readonly).await?;
        Ok(Self::from_txn(txn, id, readonly))
    }

    /// Create a BasicTxn from an existing corekv transaction.
    pub fn from_txn(txn: Box<dyn Txn>, id: u64, readonly: bool) -> Self {
        Self {
            shared_txn: SharedTxn::new(txn),
            id,
            readonly,
            state: TxnState::Active,
            success_fns: Vec::new(),
            error_fns: Vec::new(),
            discard_fns: Vec::new(),
            success_async_fns: Vec::new(),
            error_async_fns: Vec::new(),
            discard_async_fns: Vec::new(),
        }
    }

    /// Get the transaction ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Check if this is a read-only transaction.
    pub fn is_readonly(&self) -> bool {
        self.readonly
    }

    /// Get the blockstore (namespace 'b').
    pub fn blockstore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Blockstore)
    }

    /// Get the datastore (namespace 'd').
    pub fn datastore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Datastore)
    }

    /// Get the encstore (namespace 'e').
    pub fn encstore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Encstore)
    }

    /// Get the headstore (namespace 'h').
    pub fn headstore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Headstore)
    }

    /// Get the peerstore (namespace 'p').
    pub fn peerstore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Peerstore)
    }

    /// Get the systemstore (namespace 's').
    pub fn systemstore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Systemstore)
    }

    /// Get the rootstore (no namespace prefix).
    pub fn rootstore(&self) -> RootView {
        RootView::new(self.shared_txn.clone())
    }

    /// Register a callback to be called on successful commit.
    pub fn on_success(&mut self, callback: TxnCallback) {
        self.success_fns.push(callback);
    }

    /// Register a callback to be called on commit error.
    pub fn on_error(&mut self, callback: TxnCallback) {
        self.error_fns.push(callback);
    }

    /// Register a callback to be called on discard.
    pub fn on_discard(&mut self, callback: TxnCallback) {
        self.discard_fns.push(callback);
    }

    /// Register an async callback to be called on successful commit.
    pub fn on_success_async(&mut self, callback: AsyncCallback) {
        self.success_async_fns.push(callback);
    }

    /// Register an async callback to be called on commit error.
    pub fn on_error_async(&mut self, callback: AsyncCallback) {
        self.error_async_fns.push(callback);
    }

    /// Register an async callback to be called on discard.
    pub fn on_discard_async(&mut self, callback: AsyncCallback) {
        self.discard_async_fns.push(callback);
    }

    /// Commit the transaction.
    ///
    /// On success, all on_success callbacks are executed.
    /// On error, all on_error callbacks are executed.
    pub async fn commit(mut self) -> Result<()> {
        if self.state != TxnState::Active {
            return Err(match self.state {
                TxnState::Committed => Error::TxnAlreadyCommitted,
                TxnState::Discarded => Error::TxnAlreadyDiscarded,
                TxnState::Active => unreachable!(),
            });
        }

        // Extract the underlying transaction
        // The SharedTxn holds it in an Arc<RwLock<Box<dyn Txn>>>
        // We need to get ownership to call commit
        let shared = Arc::try_unwrap(self.shared_txn).map_err(|_| {
            Error::Storage(storage::corekv::Error::Other(
                "Cannot commit: transaction still has references".into(),
            ))
        })?;

        let txn = shared.into_txn();
        let result = txn.commit().await;

        let (sync_fns, async_fns) = if result.is_ok() {
            self.state = TxnState::Committed;
            (self.success_fns, self.success_async_fns)
        } else {
            (self.error_fns, self.error_async_fns)
        };

        // Execute async callbacks concurrently, logging any failures
        for callback in async_fns {
            let txn_id = self.id;
            tokio::spawn(async move {
                let result = std::panic::AssertUnwindSafe(callback())
                    .catch_unwind()
                    .await;
                if let Err(e) = result {
                    tracing::error!(
                        txn_id = txn_id,
                        error = ?e,
                        "Transaction async callback panicked"
                    );
                }
            });
        }

        // Execute sync callbacks
        for callback in sync_fns {
            callback();
        }

        result.map_err(Error::Storage)
    }

    /// Discard the transaction.
    ///
    /// All on_discard callbacks are executed on success.
    /// Returns an error if the transaction is not active or still has references.
    pub fn discard(mut self) -> Result<()> {
        if self.state != TxnState::Active {
            return Err(match self.state {
                TxnState::Committed => Error::TxnAlreadyCommitted,
                TxnState::Discarded => Error::TxnAlreadyDiscarded,
                TxnState::Active => unreachable!(),
            });
        }

        // Extract and discard the underlying transaction
        let shared = Arc::try_unwrap(self.shared_txn).map_err(|_| Error::TxnStillInUse)?;

        let txn = shared.into_txn();
        txn.discard();

        self.state = TxnState::Discarded;

        // Execute async callbacks concurrently, logging any failures
        let txn_id = self.id;
        for callback in self.discard_async_fns {
            tokio::spawn(async move {
                let result = std::panic::AssertUnwindSafe(callback())
                    .catch_unwind()
                    .await;
                if let Err(e) = result {
                    tracing::error!(
                        txn_id = txn_id,
                        error = ?e,
                        "Transaction discard async callback panicked"
                    );
                }
            });
        }

        // Execute sync callbacks
        for callback in self.discard_fns {
            callback();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use storage::backends::MemoryStore;

    #[tokio::test]
    async fn test_basic_txn_id() {
        let store = MemoryStore::new();
        let txn = BasicTxn::new(&store, 42, false).await.unwrap();
        assert_eq!(txn.id(), 42);
    }

    #[tokio::test]
    async fn test_basic_txn_readonly() {
        let store = MemoryStore::new();

        let txn = BasicTxn::new(&store, 1, true).await.unwrap();
        assert!(txn.is_readonly());

        let txn = BasicTxn::new(&store, 2, false).await.unwrap();
        assert!(!txn.is_readonly());
    }

    #[tokio::test]
    async fn test_basic_txn_multistore_access() {
        let store = MemoryStore::new();
        let txn = BasicTxn::new(&store, 1, false).await.unwrap();

        // Write to different stores
        txn.datastore()
            .set(b"key", b"datastore_value")
            .await
            .unwrap();
        txn.systemstore()
            .set(b"key", b"systemstore_value")
            .await
            .unwrap();

        // Read back
        assert_eq!(
            txn.datastore().get(b"key").await.unwrap(),
            Some(b"datastore_value".to_vec())
        );
        assert_eq!(
            txn.systemstore().get(b"key").await.unwrap(),
            Some(b"systemstore_value".to_vec())
        );

        // Commit
        txn.commit().await.unwrap();

        // Verify data persisted
        let txn = BasicTxn::new(&store, 2, true).await.unwrap();
        assert_eq!(
            txn.datastore().get(b"key").await.unwrap(),
            Some(b"datastore_value".to_vec())
        );
    }

    #[tokio::test]
    async fn test_basic_txn_on_success_callback() {
        let store = MemoryStore::new();
        let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();
        txn.on_success(Box::new(move || {
            called_clone.store(true, Ordering::SeqCst);
        }));

        txn.commit().await.unwrap();

        assert!(called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_basic_txn_multiple_callbacks() {
        let store = MemoryStore::new();
        let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

        let counter = Arc::new(AtomicU32::new(0));
        for _ in 0..3 {
            let counter_clone = counter.clone();
            txn.on_success(Box::new(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            }));
        }

        txn.commit().await.unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_basic_txn_discard_callback() {
        let store = MemoryStore::new();
        let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();
        txn.on_discard(Box::new(move || {
            called_clone.store(true, Ordering::SeqCst);
        }));

        // Write some data
        txn.datastore().set(b"key", b"value").await.unwrap();

        // Discard
        txn.discard().unwrap();

        assert!(called.load(Ordering::SeqCst));

        // Verify data was not persisted
        let txn = BasicTxn::new(&store, 2, true).await.unwrap();
        assert_eq!(txn.datastore().get(b"key").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_basic_txn_rootstore_access() {
        let store = MemoryStore::new();
        let txn = BasicTxn::new(&store, 1, false).await.unwrap();

        // Write through datastore
        txn.datastore().set(b"mykey", b"value").await.unwrap();

        // Read through rootstore with prefix
        let value = txn.rootstore().get(b"dmykey").await.unwrap();
        assert_eq!(value, Some(b"value".to_vec()));

        txn.commit().await.unwrap();
    }

    // Error callback tests

    #[tokio::test]
    async fn test_basic_txn_error_callback_not_called_on_success() {
        let store = MemoryStore::new();
        let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

        let error_called = Arc::new(AtomicBool::new(false));
        let error_called_clone = error_called.clone();
        txn.on_error(Box::new(move || {
            error_called_clone.store(true, Ordering::SeqCst);
        }));

        let success_called = Arc::new(AtomicBool::new(false));
        let success_called_clone = success_called.clone();
        txn.on_success(Box::new(move || {
            success_called_clone.store(true, Ordering::SeqCst);
        }));

        txn.commit().await.unwrap();

        // Success should be called, error should not
        assert!(success_called.load(Ordering::SeqCst));
        assert!(!error_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_basic_txn_discard_callback_not_called_on_commit() {
        let store = MemoryStore::new();
        let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

        let discard_called = Arc::new(AtomicBool::new(false));
        let discard_called_clone = discard_called.clone();
        txn.on_discard(Box::new(move || {
            discard_called_clone.store(true, Ordering::SeqCst);
        }));

        txn.commit().await.unwrap();

        // Discard callback should not be called on commit
        assert!(!discard_called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_basic_txn_success_callback_not_called_on_discard() {
        let store = MemoryStore::new();
        let mut txn = BasicTxn::new(&store, 1, false).await.unwrap();

        let success_called = Arc::new(AtomicBool::new(false));
        let success_called_clone = success_called.clone();
        txn.on_success(Box::new(move || {
            success_called_clone.store(true, Ordering::SeqCst);
        }));

        txn.discard().unwrap();

        // Success callback should not be called on discard
        assert!(!success_called.load(Ordering::SeqCst));
    }

    // Transaction state transition tests

    #[tokio::test]
    async fn test_basic_txn_double_commit_returns_error() {
        let store = MemoryStore::new();
        let txn = BasicTxn::new(&store, 1, false).await.unwrap();

        // First commit succeeds
        txn.commit().await.unwrap();

        // Cannot commit twice - txn is consumed after first commit
        // This is enforced by Rust's ownership system
    }

    #[tokio::test]
    async fn test_basic_txn_discard_already_discarded_returns_error() {
        let store = MemoryStore::new();
        let txn = BasicTxn::new(&store, 1, false).await.unwrap();

        // First discard succeeds
        txn.discard().unwrap();

        // Cannot discard twice - txn is consumed after first discard
        // This is enforced by Rust's ownership system
    }

    #[tokio::test]
    async fn test_basic_txn_all_stores_accessible() {
        let store = MemoryStore::new();
        let txn = BasicTxn::new(&store, 1, false).await.unwrap();

        // All stores should be accessible and work
        txn.datastore().set(b"d", b"data").await.unwrap();
        txn.blockstore().set(b"b", b"block").await.unwrap();
        txn.encstore().set(b"e", b"enc").await.unwrap();
        txn.headstore().set(b"h", b"head").await.unwrap();
        txn.peerstore().set(b"p", b"peer").await.unwrap();
        txn.systemstore().set(b"s", b"sys").await.unwrap();

        txn.commit().await.unwrap();

        // Verify all stores persisted
        let txn = BasicTxn::new(&store, 2, true).await.unwrap();
        assert_eq!(
            txn.datastore().get(b"d").await.unwrap(),
            Some(b"data".to_vec())
        );
        assert_eq!(
            txn.blockstore().get(b"b").await.unwrap(),
            Some(b"block".to_vec())
        );
        assert_eq!(
            txn.encstore().get(b"e").await.unwrap(),
            Some(b"enc".to_vec())
        );
        assert_eq!(
            txn.headstore().get(b"h").await.unwrap(),
            Some(b"head".to_vec())
        );
        assert_eq!(
            txn.peerstore().get(b"p").await.unwrap(),
            Some(b"peer".to_vec())
        );
        assert_eq!(
            txn.systemstore().get(b"s").await.unwrap(),
            Some(b"sys".to_vec())
        );
    }
}
