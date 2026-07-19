/// Transaction wrapper for DefraDB matching Go's internal/datastore/txn.go.
///
/// BasicTxn wraps a corekv transaction and provides:
/// - Multistore access via namespace views
/// - Transaction ID
/// - Lifecycle callbacks (on_success, on_error, on_discard)
use crate::error::{Error, Result};
use crate::multistore::{NamespaceView, RootView, SharedTxn};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use storage::corekv::{Store, Txn, TxnCallback};
use storage::namespace::Namespace;

/// Asynchronous callback for transaction events.
#[cfg(not(target_arch = "wasm32"))]
pub type AsyncCallback = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;
#[cfg(target_arch = "wasm32")]
pub type AsyncCallback = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()>>>>;

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnState {
    Active,
    Committed,
    Discarded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnCallbackPhase {
    Success,
    Error,
    Discard,
}

enum RegisteredCallback {
    Sync {
        phase: TxnCallbackPhase,
        callback: TxnCallback,
    },
    Async {
        phase: TxnCallbackPhase,
        callback: AsyncCallback,
    },
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
    callbacks: Vec<RegisteredCallback>,
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
            callbacks: Vec::new(),
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

    /// Get the access-control policy store (namespace 'a').
    pub fn acpstore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Acpstore)
    }

    /// Get the rootstore (no namespace prefix).
    pub fn rootstore(&self) -> RootView {
        RootView::new(self.shared_txn.clone())
    }

    /// Register a callback to be called on successful commit.
    pub fn on_success(&mut self, callback: TxnCallback) {
        self.callbacks.push(RegisteredCallback::Sync {
            phase: TxnCallbackPhase::Success,
            callback,
        });
    }

    /// Register a callback to be called on commit error.
    pub fn on_error(&mut self, callback: TxnCallback) {
        self.callbacks.push(RegisteredCallback::Sync {
            phase: TxnCallbackPhase::Error,
            callback,
        });
    }

    /// Register a callback to be called on discard.
    pub fn on_discard(&mut self, callback: TxnCallback) {
        self.callbacks.push(RegisteredCallback::Sync {
            phase: TxnCallbackPhase::Discard,
            callback,
        });
    }

    /// Register an async callback to be called on successful commit.
    pub fn on_success_async(&mut self, callback: AsyncCallback) {
        self.callbacks.push(RegisteredCallback::Async {
            phase: TxnCallbackPhase::Success,
            callback,
        });
    }

    /// Register an async callback to be called on commit error.
    pub fn on_error_async(&mut self, callback: AsyncCallback) {
        self.callbacks.push(RegisteredCallback::Async {
            phase: TxnCallbackPhase::Error,
            callback,
        });
    }

    /// Register an async callback to be called on discard.
    pub fn on_discard_async(&mut self, callback: AsyncCallback) {
        self.callbacks.push(RegisteredCallback::Async {
            phase: TxnCallbackPhase::Discard,
            callback,
        });
    }

    fn split_callbacks(
        callbacks: Vec<RegisteredCallback>,
        phase: TxnCallbackPhase,
    ) -> (
        Vec<TxnCallback>,
        Vec<AsyncCallback>,
        Vec<RegisteredCallback>,
    ) {
        let mut retained = Vec::with_capacity(callbacks.len());
        let mut sync_callbacks = Vec::new();
        let mut async_callbacks = Vec::new();

        for callback in callbacks {
            match callback {
                RegisteredCallback::Sync {
                    phase: callback_phase,
                    callback,
                } if callback_phase == phase => {
                    sync_callbacks.push(callback);
                }
                RegisteredCallback::Async {
                    phase: callback_phase,
                    callback,
                } if callback_phase == phase => {
                    async_callbacks.push(callback);
                }
                callback => retained.push(callback),
            }
        }

        (sync_callbacks, async_callbacks, retained)
    }

    /// Commit the transaction.
    ///
    /// On success, all on_success callbacks are executed.
    /// On error, all on_error callbacks are executed.
    ///
    /// # Callback Panic Handling
    ///
    /// Callback panics are not recovered here. In dev/test profiles the panic
    /// unwinds through `commit()`. In release builds this workspace uses
    /// `panic = "abort"`, so a panicking callback aborts the process.
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
        let callbacks = std::mem::take(&mut self.callbacks);
        let shared = Arc::try_unwrap(self.shared_txn).map_err(|_| {
            Error::Storage(storage::corekv::Error::Other(
                "Cannot commit: transaction still has references".into(),
            ))
        })?;

        let txn = shared.into_txn();
        let result = txn.commit().await;

        let (sync_fns, async_fns, retained_callbacks) = if result.is_ok() {
            self.state = TxnState::Committed;
            Self::split_callbacks(callbacks, TxnCallbackPhase::Success)
        } else {
            Self::split_callbacks(callbacks, TxnCallbackPhase::Error)
        };
        self.callbacks = retained_callbacks;

        // Callbacks run directly so the runtime's panic strategy stays explicit:
        // unwind propagates in dev/test, and release aborts the process.
        for callback in async_fns {
            callback().await;
        }

        for callback in sync_fns {
            callback();
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
    /// Callback panics are not recovered here. In dev/test profiles the panic
    /// unwinds through `discard()`. In release builds this workspace uses
    /// `panic = "abort"`, so a panicking callback aborts the process.
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
        let callbacks = std::mem::take(&mut self.callbacks);
        let shared = Arc::try_unwrap(self.shared_txn).map_err(|_| Error::TxnStillInUse)?;

        let txn = shared.into_txn();
        txn.discard();

        self.state = TxnState::Discarded;

        // Async discard callbacks are spawned and run directly. On native
        // targets a panic in the task follows the runtime panic strategy.
        let txn_id = self.id;
        let (discard_fns, discard_async_fns, retained_callbacks) =
            Self::split_callbacks(callbacks, TxnCallbackPhase::Discard);
        self.callbacks = retained_callbacks;
        if !discard_async_fns.is_empty() {
            let callback_count = discard_async_fns.len();
            tracing::debug!(
                txn_id = txn_id,
                count = callback_count,
                "Spawning async discard callbacks in background"
            );
            for callback in discard_async_fns {
                #[cfg(not(target_arch = "wasm32"))]
                tokio::spawn(async move {
                    callback().await;
                });
                // On wasm32, async callbacks cannot be spawned or awaited from
                // a sync context. Since discard callbacks are fire-and-forget,
                // we drop them. Use sync callbacks for critical cleanup on wasm32.
                #[cfg(target_arch = "wasm32")]
                {
                    let _ = callback;
                }
            }
        }

        for callback in discard_fns {
            callback();
        }

        Ok(())
    }
}
