/// Traits for the datastore layer matching Go's internal/datastore/txn.go.
use crate::multistore::{NamespaceView, RootView};
use storage::corekv::{MaybeSendSync, TxnCallback};

/// Transaction trait for DefraDB operations.
///
/// This matches Go's Txn interface in internal/datastore/txn.go.
/// It provides access to all namespaced stores and transaction lifecycle methods.
pub trait Txn: MaybeSendSync {
    /// Get the blockstore (namespace 'b').
    fn blockstore(&self) -> NamespaceView;

    /// Get the datastore (namespace 'd').
    fn datastore(&self) -> NamespaceView;

    /// Get the encstore (namespace 'e').
    fn encstore(&self) -> NamespaceView;

    /// Get the headstore (namespace 'h').
    fn headstore(&self) -> NamespaceView;

    /// Get the peerstore (namespace 'p').
    fn peerstore(&self) -> NamespaceView;

    /// Get the systemstore (namespace 's').
    fn systemstore(&self) -> NamespaceView;

    /// Get the rootstore (no namespace prefix).
    fn rootstore(&self) -> RootView;

    /// Get the unique transaction ID.
    fn id(&self) -> u64;

    /// Check if this is a read-only transaction.
    fn is_readonly(&self) -> bool;

    /// Register a callback to be called on successful commit.
    fn on_success(&mut self, callback: TxnCallback);

    /// Register a callback to be called on commit error.
    fn on_error(&mut self, callback: TxnCallback);

    /// Register a callback to be called on discard.
    fn on_discard(&mut self, callback: TxnCallback);
}
