/// In-memory backend implementation using BTreeMap.
///
/// This backend provides a simple, fast, in-memory key-value store suitable for
/// testing and development. It uses a BTreeMap for ordered storage and supports
/// full MVCC transactions with snapshot isolation.
///
/// # Features
///
/// - Ordered key-value storage with BTreeMap
/// - Full transaction support with snapshot isolation
/// - Concurrent read access with RwLock
/// - Zero persistence (data lost on process exit)
/// - No external dependencies beyond standard library
///
/// # Use Cases
///
/// - Unit testing
/// - Integration testing
/// - Development and prototyping
/// - Ephemeral caches
///
/// # Example
///
/// ```ignore
/// use storage::backends::memory::MemoryStore;
/// use storage::corekv::{Store, Reader, Writer};
///
/// let store = MemoryStore::new();
/// let mut txn = store.new_txn(false).await?;
/// txn.set(b"key", b"value").await?;
/// txn.commit().await?;
/// ```
mod store;
mod transaction;

#[cfg(test)]
mod tests;

pub use store::MemoryStore;
