/// CoreKV abstraction layer for key-value storage.
///
/// This module provides a complete abstraction layer for key-value storage operations,
/// inspired by the Go corekv package. It defines a set of traits that allow for multiple
/// backend implementations (Redb, Memory, etc.) while providing a consistent API.
///
/// # Architecture
///
/// The CoreKV layer follows a hierarchical trait structure:
///
/// ```text
/// Store
///   ├── Reader (get, has, iterator)
///   ├── Writer (set, delete)
///   └── ReaderWriter (combines Reader + Writer)
///
/// Txn (extends ReaderWriter)
///   ├── commit/discard
///   └── lifecycle callbacks (on_success, on_error, on_discard)
///
/// TxnStore (extends Store)
///   └── new_txn(readonly) -> Txn
/// ```
///
/// # Key Features
///
/// - **Async-first**: All I/O operations are asynchronous using async/await
/// - **MVCC Transactions**: Support for serializable snapshot isolation
/// - **Callbacks**: Transaction lifecycle hooks for success, error, and discard events
/// - **Iteration**: Flexible iteration with prefix, range, and reverse support
/// - **Backend Agnostic**: Works with Redb, in-memory, or custom backends
///
/// # Example
///
/// ```ignore
/// use storage::corekv::{Store, Reader, Writer, IterOptions};
///
/// // Create a store (memory or redb)
/// let store = RegolithStore::in_memory().unwrap();
///
/// // Create a transaction
/// let mut txn = store.new_txn(false).await?;
///
/// // Write some data
/// txn.set(b"key1", b"value1").await?;
/// txn.set(b"key2", b"value2").await?;
///
/// // Register success callback
/// txn.on_success(Box::new(|| println!("Transaction committed!")));
///
/// // Commit the transaction
/// txn.commit().await?;
///
/// // Read back
/// let txn = store.new_txn(true).await?;
/// let value = txn.get(b"key1").await?;
/// assert_eq!(value, Some(Bytes::from_static(b"value1")));
///
/// // Iterate
/// let opts = IterOptions::new().with_prefix(b"key".to_vec());
/// let mut iter = txn.iterator(opts).await?;
/// while let Some(kv) = iter.next().await? {
///     println!("{}: {}", kv.key_str(), kv.value_str());
/// }
/// iter.close().await?;
/// ```
pub mod errors;
pub mod iterator;
pub mod traits;
pub mod types;

// Re-export commonly used types and traits for convenience
pub use errors::{Error, Result, UNIQUE_CONSTRAINT_VIOLATION_MESSAGE};
pub use iterator::{Iterator, KvPair};
pub use traits::private;
pub use traits::{
    make_async_callback, make_callback, AsyncTxnCallback, Dropable, MaybeSend, MaybeSendSync,
    MaybeSync, Reader, ReaderWriter, Store, Txn, TxnCallback, TxnStore, Writer,
};
pub use types::{IterOptions, Key};
