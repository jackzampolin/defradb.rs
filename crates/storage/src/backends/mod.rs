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
/// - **RocksDB**: Production-ready persistent storage using RocksDB (an LSM-tree
///   based key-value store). Suitable for production deployments requiring
///   persistence and high performance.
///
/// # Choosing a Backend
///
/// | Feature | Memory | RocksDB |
/// |---------|--------|---------|
/// | Persistence | No | Yes |
/// | Performance | Very Fast | Fast |
/// | Memory Usage | High (all in RAM) | Configurable |
/// | Crash Recovery | No | Yes (WAL) |
/// | Concurrent Access | Yes | Yes |
/// | ACID Transactions | Yes | Yes |
/// | Snapshot Isolation | Yes (MVCC) | Yes (RocksDB snapshots) |
/// | Use Case | Testing, Dev | Production |
///
/// # Example
///
/// ```ignore
/// use storage::backends::{MemoryStore, RocksDBStore};
/// use storage::corekv::Store;
///
/// // For testing
/// let memory_store = MemoryStore::new();
///
/// // For production
/// let rocksdb_store = RocksDBStore::open("/path/to/db")?;
/// ```
pub mod memory;
pub mod rocksdb;

#[cfg(test)]
pub mod test_suite;

pub use memory::MemoryStore;
pub use rocksdb::RocksDBStore;
