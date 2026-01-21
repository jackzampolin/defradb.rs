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
    ///
    /// # Callback Panic Handling
    ///
    /// Callback panics are caught and logged but do not affect the return value.
    /// If a callback panics, remaining callbacks still execute, and `commit()`
    /// returns `Ok(())` if the database commit succeeded. Check error logs for
    /// callback panic details.
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

        // Execute async callbacks sequentially with panic protection (matches storage backend)
        for (i, callback) in async_fns.into_iter().enumerate() {
            let callback_result = std::panic::AssertUnwindSafe(callback())
                .catch_unwind()
                .await;
            if let Err(e) = callback_result {
                tracing::error!(
                    txn_id = self.id,
                    callback_index = i,
                    error = ?e,
                    "Transaction async callback panicked - continuing with remaining callbacks"
                );
            }
        }

        // Execute sync callbacks with panic protection
        for (i, callback) in sync_fns.into_iter().enumerate() {
            let callback_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback));
            if let Err(e) = callback_result {
                tracing::error!(
                    txn_id = self.id,
                    callback_index = i,
                    error = ?e,
                    "Transaction sync callback panicked - continuing with remaining callbacks"
                );
            }
        }

        result.map_err(Error::Storage)
    }

    /// Discard the transaction.
    ///
    /// All on_discard callbacks are executed on success.
    /// Returns an error if the transaction is not active or still has references.
    ///
    /// # Callback Panic Handling
    ///
    /// Callback panics are caught and logged but do not affect the return value.
    /// If a callback panics, remaining callbacks still execute.
    ///
    /// # Async Callback Warning
    ///
    /// Async discard callbacks are spawned as background tasks and may not complete
    /// if the process exits before they finish. For completion guarantees, use
    /// sync callbacks or prefer `commit()` when async cleanup is critical.
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

        // Execute async callbacks concurrently with panic protection (fire-and-forget for discard)
        let txn_id = self.id;
        if !self.discard_async_fns.is_empty() {
            let callback_count = self.discard_async_fns.len();
            tracing::debug!(
                txn_id = txn_id,
                count = callback_count,
                "Spawning async discard callbacks in background"
            );
            for (i, callback) in self.discard_async_fns.into_iter().enumerate() {
                tokio::spawn(async move {
                    let result = std::panic::AssertUnwindSafe(callback())
                        .catch_unwind()
                        .await;
                    if let Err(e) = result {
                        tracing::error!(
                            txn_id = txn_id,
                            callback_index = i,
                            error = ?e,
                            "Transaction discard async callback panicked"
                        );
                    }
                });
            }
        }

        // Execute sync callbacks with panic protection
        for (i, callback) in self.discard_fns.into_iter().enumerate() {
            let callback_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback));
            if let Err(e) = callback_result {
                tracing::error!(
                    txn_id = self.id,
                    callback_index = i,
                    error = ?e,
                    "Transaction discard sync callback panicked - continuing with remaining callbacks"
                );
            }
        }

        Ok(())
    }
}
