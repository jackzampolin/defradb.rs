pub mod config;
mod errors;
mod group_commit;
/// Redb backend implementation with snapshot isolation and ACID transactions.
///
/// This backend provides a pure Rust persistent key-value store using redb.
/// It uses read snapshots for isolation and buffered writes for read-your-writes
/// consistency within transactions.
///
/// # Features
///
/// - Pure Rust implementation (no C/C++ dependencies)
/// - ACID transactions with snapshot isolation
/// - Persistent storage with crash recovery
/// - Single-writer model (matches Go DefraDB's LevelDB semantics)
///
/// # Platform Support
///
/// **NOTE**: This backend is NOT WASM-compatible due to redb's use of memory-mapped
/// files and native filesystem operations. For WASM environments, use `MemoryStore`
/// instead, or implement a browser-specific backend using IndexedDB/OPFS.
///
/// # MVCC Behavior
///
/// When a transaction is created, it opens a redb `ReadTransaction` which provides
/// zero-copy MVCC snapshot isolation. All reads within the transaction see the
/// database state as it was at creation time, regardless of concurrent writes.
///
/// # Async Callback Lifecycle
///
/// Transaction callbacks follow fire-and-forget semantics (matching Go DefraDB):
///
/// - **Sync callbacks**: Executed inline during commit/discard, blocking until complete
/// - **Async callbacks on commit**: Awaited during commit, blocking return until complete
/// - **Async callbacks on discard**: Spawned as background tasks (fire-and-forget)
///
/// **Important**: Async discard callbacks may not complete if the process exits
/// before they finish. Callers requiring completion guarantees should use
/// `tokio::task::JoinSet`, `tokio_util::task::TaskTracker`, or similar
/// synchronization, or prefer `commit()` over `discard()` when async cleanup
/// is critical.
///
/// # Use Cases
///
/// - Production deployments on native platforms (Linux, macOS, Windows)
/// - Embedded applications with filesystem access
///
/// # Example
///
/// ```ignore
/// use storage::backends::redb::RedbStore;
/// use storage::corekv::{Store, Reader, Writer};
///
/// let store = RedbStore::open("/path/to/db")?;
/// let mut txn = store.new_txn(false).await?;
/// txn.set(b"key", b"value").await?;
/// txn.commit().await?;
/// ```
mod store;
mod transaction;

#[cfg(test)]
mod tests;

use std::ops::Bound;

use redb::TableDefinition;

pub use config::{DurabilityMode, RedbStoreOptions};
pub use store::{IntegrityReport, RedbStore};
#[cfg(test)]
pub(crate) use transaction::RedbTxn;

/// Table definition for the main key-value store.
const KV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

/// Convert a `Bound<Vec<u8>>` to `Bound<&[u8]>` for redb range queries.
fn bound_as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}
