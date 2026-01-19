/// Multistore - Coordinator for all specialized stores
///
/// The Multistore provides a unified interface to access all 7 specialized stores:
/// - RootStore: Foundation store without namespacing
/// - Datastore: Document and collection data with chunking
/// - Blockstore: IPLD blocks with merge tracking
/// - Headstore: Merkle tree heads and definitions
/// - Systemstore: Metadata and configuration
/// - Peerstore: Peer and replication metadata
/// - Encstore: Encrypted blocks (uses blockstore implementation)
use crate::backends::{MemoryStore, RocksDBStore};
use crate::corekv::{Result, Store};
use crate::stores::{
    blockstore::Blockstore, datastore::Datastore, headstore::Headstore, peerstore::Peerstore,
    rootstore::RootStore, systemstore::Systemstore,
};
use std::sync::Arc;

/// Multistore coordinates access to all specialized stores
pub struct Multistore<S: Store> {
    /// Root store (no namespace)
    pub root: RootStore<S>,
    /// Datastore (namespace 'd')
    pub datastore: Datastore<S>,
    /// Blockstore (namespace 'b')
    pub blockstore: Blockstore<S>,
    /// Headstore (namespace 'h')
    pub headstore: Headstore<S>,
    /// Systemstore (namespace 's')
    pub systemstore: Systemstore<S>,
    /// Peerstore (namespace 'p')
    pub peerstore: Peerstore<S>,
    /// Encstore (namespace 'e') - uses blockstore implementation
    pub encstore: Blockstore<S>,

    /// Underlying store reference
    store: Arc<S>,
}

impl<S: Store> Multistore<S> {
    /// Create a new Multistore
    pub fn new(store: Arc<S>) -> Self {
        Self {
            root: RootStore::new(store.clone()),
            datastore: Datastore::new(store.clone()),
            blockstore: Blockstore::new(store.clone(), false),
            headstore: Headstore::new(store.clone()),
            systemstore: Systemstore::new(store.clone()),
            peerstore: Peerstore::new(store.clone()),
            encstore: Blockstore::new_with_namespace(
                store.clone(),
                false,
                crate::namespace::Namespace::Encstore,
            ),
            store,
        }
    }

    /// Close all stores
    pub async fn close(&self) -> Result<()> {
        self.store.close().await
    }
}

/// Multistore specialized for MemoryStore
pub type MemoryMultistore = Multistore<MemoryStore>;

impl MemoryMultistore {
    /// Create a new in-memory Multistore
    pub fn new_memory() -> Self {
        Self::new(Arc::new(MemoryStore::new()))
    }
}

/// Multistore specialized for RocksDBStore
pub type RocksDBMultistore = Multistore<RocksDBStore>;

impl RocksDBMultistore {
    /// Create a new RocksDB-backed Multistore
    pub fn new_rocksdb(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let store = Arc::new(RocksDBStore::open(path)?);
        Ok(Self::new(store))
    }
}

// Tests extracted to crates/storage/tests/multistore_tests.rs
