/// Peerstore - Peer and replication metadata
///
/// The Peerstore handles storage of replicator configuration, replication
/// retry tracking, and search engine retry tracking for P2P operations.

use crate::corekv::{Result, Store, Txn};
use crate::namespace::{Namespace, NamespacedStore};
use async_trait::async_trait;
use std::sync::Arc;

/// Peerstore provides storage for peer and replication metadata
pub struct Peerstore<S: Store> {
    store: NamespacedStore<S>,
}

impl<S: Store> Peerstore<S> {
    /// Create a new Peerstore
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store: NamespacedStore::new(store, Namespace::Peerstore),
        }
    }
}

#[async_trait]
impl<S: Store> Store for Peerstore<S> {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        self.store.new_txn(readonly).await
    }

    async fn close(&self) -> Result<()> {
        self.store.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::MemoryStore;
    use crate::corekv::{Reader, Writer};
    use crate::keys::peerstore::ReplicatorKey;
    use crate::keys::Key;

    #[tokio::test]
    async fn test_peerstore_basic() {
        let store = Arc::new(MemoryStore::new());
        let peerstore = Peerstore::new(store);

        let key = ReplicatorKey::new("replicator_1");

        // Write
        let mut txn = peerstore.new_txn(false).await.unwrap();
        txn.set(&key.bytes(), b"replicator_config").await.unwrap();
        txn.commit().await.unwrap();

        // Read
        let txn = peerstore.new_txn(true).await.unwrap();
        let value = txn.get(&key.bytes()).await.unwrap();
        assert_eq!(value, Some(b"replicator_config".to_vec()));
    }
}
