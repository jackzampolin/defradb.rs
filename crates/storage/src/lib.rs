/// Storage subsystem for DefraDB.rs
///
/// This crate provides the complete storage layer for DefraDB, including:
/// - CoreKV abstraction layer for key-value operations
/// - Multiple backend implementations (Memory, RocksDB)
/// - MVCC transactions with snapshot isolation
/// - Seven specialized stores with namespace isolation
/// - 34 key types for hierarchical organization
/// - Chunking support for large values
/// - Merge tracking for CRDT operations
///
/// # Architecture
///
/// ```text
/// Application Layer
///     ↓
/// Multistore (7 specialized stores)
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
///     ├── MemoryStore (testing)
///     └── RocksDBStore (production)
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
/// - Memory backend
/// - RocksDB backend
/// - Iterators with filtering
/// - Transaction callbacks
///
/// ## Phase 2: Key Hierarchy (In Progress)
/// - 34 key types across 7 stores
/// - Key encoding/decoding
/// - Hierarchical organization
///
/// ## Phase 3: Store Implementations (Pending)
/// - Namespace isolation
/// - Seven specialized stores
/// - Multistore coordinator
/// - Chunking for large values
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

// Phase 2+ modules (to be implemented)
// pub mod keys;
// pub mod stores;
// pub mod transaction;
// pub mod namespace;

// Re-export commonly used types for convenience
pub use backends::{MemoryStore, RocksDBStore};
pub use corekv::{Error, IterOptions, Iterator, KvPair, Reader, Result, Store, Txn, Writer};
