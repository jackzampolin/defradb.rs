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
            encstore: Blockstore::new(store.clone(), false),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corekv::{Reader, Writer};
    use crate::keys::{
        blockstore::BlockstoreKey, datastore::DataStoreKey, headstore::HeadstoreDocKey,
        peerstore::ReplicatorKey, systemstore::CollectionKey, utils::InstanceType, Key,
    };
    use cid::Cid;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_multistore_creation() {
        let ms = MemoryMultistore::new_memory();
        assert!(ms.close().await.is_ok());
    }

    #[tokio::test]
    async fn test_multistore_all_stores_isolated() {
        let ms = MemoryMultistore::new_memory();

        // Write to each store with same logical key
        // Datastore
        let ds_key = DataStoreKey::new(1, InstanceType::Value, "doc1", "field");
        let mut txn = ms.datastore.new_txn(false).await.unwrap();
        txn.set(&ds_key.bytes(), b"datastore_value").await.unwrap();
        txn.commit().await.unwrap();

        // Blockstore
        let cid = Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
            .unwrap();
        let bs_key = BlockstoreKey::new(cid);
        let mut txn = ms.blockstore.new_txn(false).await.unwrap();
        txn.set(&bs_key.bytes(), b"blockstore_value").await.unwrap();
        txn.commit().await.unwrap();

        // Headstore
        let hs_key = HeadstoreDocKey::new("doc1", "field", cid);
        let mut txn = ms.headstore.new_txn(false).await.unwrap();
        txn.set(&hs_key.bytes(), b"headstore_value").await.unwrap();
        txn.commit().await.unwrap();

        // Systemstore
        let ss_key = CollectionKey::new("users");
        let mut txn = ms.systemstore.new_txn(false).await.unwrap();
        txn.set(&ss_key.bytes(), b"systemstore_value")
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Peerstore
        let ps_key = ReplicatorKey::new("rep1");
        let mut txn = ms.peerstore.new_txn(false).await.unwrap();
        txn.set(&ps_key.bytes(), b"peerstore_value").await.unwrap();
        txn.commit().await.unwrap();

        // Verify each store has its own value
        let txn = ms.datastore.new_txn(true).await.unwrap();
        let val = txn.get(&ds_key.bytes()).await.unwrap();
        assert_eq!(val, Some(b"datastore_value".to_vec()));

        let txn = ms.blockstore.new_txn(true).await.unwrap();
        let val = txn.get(&bs_key.bytes()).await.unwrap();
        assert_eq!(val, Some(b"blockstore_value".to_vec()));

        let txn = ms.headstore.new_txn(true).await.unwrap();
        let val = txn.get(&hs_key.bytes()).await.unwrap();
        assert_eq!(val, Some(b"headstore_value".to_vec()));

        let txn = ms.systemstore.new_txn(true).await.unwrap();
        let val = txn.get(&ss_key.bytes()).await.unwrap();
        assert_eq!(val, Some(b"systemstore_value".to_vec()));

        let txn = ms.peerstore.new_txn(true).await.unwrap();
        let val = txn.get(&ps_key.bytes()).await.unwrap();
        assert_eq!(val, Some(b"peerstore_value".to_vec()));
    }

    #[tokio::test]
    async fn test_multistore_rootstore_sees_all() {
        let ms = MemoryMultistore::new_memory();

        // Write to datastore (namespace 'd')
        let mut txn = ms.datastore.new_txn(false).await.unwrap();
        txn.set(b"key1", b"value1").await.unwrap();
        txn.commit().await.unwrap();

        // Read from rootstore with full prefixed key
        let txn = ms.root.new_txn(true).await.unwrap();
        let value = txn.get(b"dkey1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }
}
