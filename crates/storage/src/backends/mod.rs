/// Backend implementations for CoreKV storage.
///
/// This module provides concrete implementations of the CoreKV traits for
/// different storage backends. Each backend has different characteristics
/// and is suitable for different use cases.
///
/// # Available Backends
///
/// - **Memory**: Fast in-memory storage using BTreeMap. Suitable for testing,
///   development, and ephemeral caches. Data is lost when the process exits.
///
/// - **Redb** (default): Pure Rust persistent storage using redb. WASM-compatible,
///   ACID transactions with snapshot isolation. Suitable for production deployments.
///
/// # Choosing a Backend
///
/// | Feature | Memory | Redb |
/// |---------|--------|------|
/// | Persistence | No | Yes |
/// | WASM Support | Yes | Yes |
/// | Performance | Very Fast | Fast |
/// | Memory Usage | High (all in RAM) | High (snapshot per txn) |
/// | Crash Recovery | No | Yes |
/// | Concurrent Access | Yes | Yes |
/// | ACID Transactions | Yes | Yes |
/// | Snapshot Isolation | Yes (MVCC) | Yes |
/// | Use Case | Testing, Dev | Production (small DBs) |
///
/// # Example
///
/// ```ignore
/// use storage::backends::MemoryStore;
/// use storage::corekv::Store;
///
/// // For testing
/// let memory_store = MemoryStore::new();
///
/// #[cfg(feature = "redb")]
/// {
///     use storage::backends::RedbStore;
///     let redb_store = RedbStore::open("/path/to/db")?;
/// }
/// ```
pub mod memory;

#[cfg(feature = "redb")]
pub mod redb;

#[cfg(feature = "redb")]
pub mod redb_config;

#[cfg(test)]
pub mod test_suite;

pub use memory::MemoryStore;

#[cfg(feature = "redb")]
pub use redb::RedbStore;

#[cfg(feature = "redb")]
pub use redb_config::RedbStoreOptions;
