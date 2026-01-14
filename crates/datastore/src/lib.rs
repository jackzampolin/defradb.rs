/// Datastore layer for DefraDB matching Go's internal/datastore/.
///
/// This crate provides the transaction wrapper layer that sits between
/// the storage layer (corekv) and the database layer (db). It provides:
///
/// - `BasicTxn`: Transaction wrapper with multistore access and callbacks
/// - `Txn` trait: Common interface for DefraDB transactions
/// - `NamespaceView`: View into a specific namespace of a transaction
/// - `Blockstore` trait: Block storage interface
///
/// # Architecture
///
/// ```text
/// db crate (Database, Collection, DbTxn)
///     ↓
/// datastore crate (BasicTxn, Txn trait, NamespaceView)
///     ↓
/// storage crate (corekv, Multistore, backends)
/// ```
///
/// # Example
///
/// ```ignore
/// use datastore::BasicTxn;
/// use storage::backends::MemoryStore;
///
/// let store = MemoryStore::new();
/// let txn = BasicTxn::new(&store, 1, false).await?;
///
/// // Access namespaced stores
/// txn.datastore().set(b"key", b"value").await?;
/// txn.systemstore().set(b"config", b"data").await?;
///
/// // Register callbacks
/// txn.on_success(Box::new(|| println!("Committed!")));
///
/// // Commit
/// txn.commit().await?;
/// ```
pub mod error;
pub mod multistore;
pub mod traits;
pub mod txn;

// Re-export commonly used types
pub use error::{Error, Result};
pub use multistore::{NamespaceView, RootView, SharedTxn};
pub use traits::{Blockstore, Txn};
pub use txn::{AsyncCallback, BasicTxn};

// Re-export storage types for convenience
pub use storage::corekv::{IterOptions, Store, TxnCallback};
pub use storage::namespace::Namespace;
