/// Storage subsystem for DefraDB.rs
///
/// This crate provides the complete storage layer for DefraDB, including:
/// - CoreKV abstraction layer for key-value operations
/// - Multiple backend implementations (Memory, Redb)
/// - MVCC transactions with snapshot isolation
/// - Six specialized stores with namespace isolation (plus RootStore foundation)
/// - 33 key types for hierarchical organization across 6 stores
/// - Chunking support for large values (>1MB, up to 256MB)
/// - Merge tracking for CRDT operations
///
/// # Architecture
///
/// ```text
/// Application Layer
///     ↓
/// Multistore (6 specialized stores + RootStore)
///     ├── RootStore (foundation, no namespace)
///     ├── Datastore (documents)
///     ├── Blockstore (IPLD blocks)
///     ├── Headstore (document heads)
///     ├── Systemstore (metadata)
///     ├── Peerstore (P2P info)
///     └── Encstore (encrypted blocks)
///     ↓
/// CoreKV Abstraction Layer
///     ├── Reader, Writer, ReaderWriter
///     ├── Store, Txn, TxnStore
///     └── Iterator with filtering
///     ↓
/// Backend Implementation
///     ├── MemoryStore (testing, WASM)
///     └── RedbStore (production, native only)
/// ```
///
/// # Quick Start
///
/// ```ignore
/// use storage::backends::MemoryStore;
/// use storage::corekv::{Store, Reader, Writer};
///
/// // Create a store
/// let store = MemoryStore::new();
///
/// // Create a transaction
/// let mut txn = store.new_txn(false).await?;
///
/// // Write data
/// txn.set(b"key", b"value").await?;
///
/// // Commit
/// txn.commit().await?;
///
/// // Read back
/// let txn = store.new_txn(true).await?;
/// let value = txn.get(b"key").await?;
/// assert_eq!(value, Some(b"value".to_vec()));
/// ```
///
/// # Feature Status
///
/// ## Phase 1: CoreKV Foundation ✅ (Complete)
/// - CoreKV trait hierarchy
/// - Memory backend (WASM-compatible)
/// - Redb backend (native platforms only)
/// - Iterators with filtering
/// - Transaction callbacks
///
/// ## Phase 2: Key Hierarchy ✅ (Complete)
/// - 33 key types across 6 stores
/// - Key encoding/decoding with CockroachDB-style varint
/// - Hierarchical organization with prefixes
///
/// ## Phase 3: Store Implementations ⚠️ (Implementation Complete, Tests Pending)
/// - Namespace isolation ✅
/// - RootStore foundation ✅
/// - Datastore with automatic chunking (>1MB) ✅
/// - Blockstore with merge tracking for CRDTs ✅
/// - Headstore, Systemstore, Peerstore ✅
/// - Multistore coordinator ✅
/// - Transaction downcasting support ✅
/// - Note: Store-specific tests need updates for transaction wrapper pattern
///
/// ## Phase 4: Transaction System (Pending)
/// - DefraDB transaction wrapper
/// - Context propagation
/// - Explicit vs implicit transactions
///
/// ## Phase 5: Integration & Testing (Pending)
/// - Integration tests
/// - Performance benchmarks
/// - Compatibility tests
///
/// ## Phase 6: Documentation (Pending)
/// - Examples
/// - API documentation
/// - Architecture guides
pub mod backends;
pub mod corekv;
pub mod encoding;
pub mod encrypted_store;
pub mod field_value;
pub mod keys;
pub mod namespace;

pub mod index;
pub mod stores;

// See #19 for transaction wrapper module

// Re-export commonly used types for convenience
// MemoryStore is only available on native platforms (uses tokio::sync::RwLock)
#[cfg(not(target_arch = "wasm32"))]
pub use backends::MemoryStore;

#[cfg(all(feature = "redb", not(target_arch = "wasm32")))]
pub use backends::RedbStore;

#[cfg(all(feature = "fjall", not(target_arch = "wasm32")))]
pub use backends::{FjallStore, FjallStoreOptions};

#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
pub use backends::{RocksDbStore, RocksDbStoreOptions};

#[cfg(all(target_arch = "wasm32", feature = "leveldb"))]
pub use backends::LevelDbStore;

#[cfg(all(target_arch = "wasm32", feature = "leveldb"))]
pub use backends::OpfsEnv;

pub use corekv::{
    Error, IterOptions, Iterator, KvPair, Reader, ReaderWriter, Result, Store, Txn, Writer,
};
