/// Backend implementations for CoreKV storage.
///
/// This module provides concrete implementations of the CoreKV traits for
/// different storage backends. Each backend has different characteristics
/// and is suitable for different use cases.
///
/// # Available Backends
///
/// - **Memory**: Fast in-memory storage using BTreeMap. Suitable for testing,
///   development, ephemeral caches, and WASM environments. Data is lost when
///   the process exits.
///
/// - **Redb** (default, native only): Pure Rust persistent storage using redb.
///   ACID transactions with snapshot isolation. **NOT WASM-compatible** due to
///   memory-mapped file usage. Suitable for native platform production deployments.
///
/// # Choosing a Backend
///
/// | Feature | Memory | Redb |
/// |---------|--------|------|
/// | Persistence | No | Yes |
/// | WASM Support | Yes | **No** |
/// | Performance | Very Fast | Fast |
/// | Memory Usage | High (all in RAM) | Low (MVCC snapshots) |
/// | Crash Recovery | No | Yes |
/// | Concurrent Access | Yes | Yes |
/// | ACID Transactions | Yes | Yes |
/// | Snapshot Isolation | Yes (MVCC) | Yes |
/// | Use Case | Testing, Dev, WASM | Production (native) |
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
// Memory backend uses tokio::sync::RwLock, only available on native platforms
// For WASM, use the simplified memory store in the wasm crate
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod shared;

#[cfg(not(target_arch = "wasm32"))]
pub mod memory;

// Redb is only available on native platforms (not WASM)
// because it requires memory-mapped files and native filesystem access
#[cfg(all(feature = "redb", not(target_arch = "wasm32")))]
pub mod redb;

// LevelDB backend - pure Rust, WASM only (rusty-leveldb uses Rc internally, not Send/Sync)
// On native platforms, use redb instead for full concurrency support.
#[cfg(all(target_arch = "wasm32", feature = "leveldb"))]
pub mod leveldb;

// OPFS environment for LevelDB browser persistence.
// Implements rusty-leveldb's synchronous Env trait using an in-memory filesystem
// that loads from and persists to the browser's Origin Private File System (OPFS).
#[cfg(all(target_arch = "wasm32", feature = "leveldb"))]
pub mod opfs_env;

#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod test_suite;

#[cfg(not(target_arch = "wasm32"))]
pub use memory::MemoryStore;

#[cfg(all(feature = "redb", not(target_arch = "wasm32")))]
pub use redb::{CallbackCounts, DurabilityMode, IntegrityReport, RedbStore, RedbStoreOptions};

#[cfg(all(target_arch = "wasm32", feature = "leveldb"))]
pub use leveldb::LevelDbStore;

#[cfg(all(target_arch = "wasm32", feature = "leveldb"))]
pub use opfs_env::OpfsEnv;
