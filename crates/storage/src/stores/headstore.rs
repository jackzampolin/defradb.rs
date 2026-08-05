/// Headstore - Merkle tree heads and definitions
///
/// The Headstore handles storage of document heads, collection heads,
/// field definitions, collection definitions, and collection set definitions.
use crate::corekv::{Result, Store, Txn};
use crate::namespace::{Namespace, NamespacedStore};
use async_trait::async_trait;
use std::sync::Arc;

/// Headstore provides storage for merkle tree heads and schema definitions
pub struct Headstore<S: Store> {
    store: NamespacedStore<S>,
}

impl<S: Store> Headstore<S> {
    /// Create a new Headstore
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store: NamespacedStore::new(store, Namespace::Headstore),
        }
    }
}

impl<S: Store> crate::corekv::private::Sealed for Headstore<S> {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store> Store for Headstore<S> {
    #[cfg(not(target_arch = "wasm32"))]
    fn transaction_stats_handle(&self) -> Option<crate::backends::TransactionStatsHandle> {
        self.store.transaction_stats_handle()
    }

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
    use crate::corekv::Key;
    use crate::keys::headstore::HeadstoreDocKey;
    use cid::Cid;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_headstore_basic() {
        let store = Arc::new(MemoryStore::new());
        let headstore = Headstore::new(store);

        let cid =
            Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();
        let key = HeadstoreDocKey::new(1, "field1", cid);

        // Write
        let mut txn = headstore.new_txn(false).await.unwrap();
        txn.set(&key.bytes(), b"head_data").await.unwrap();
        txn.commit().await.unwrap();

        // Read
        let txn = headstore.new_txn(true).await.unwrap();
        let value = txn.get(&key.bytes()).await.unwrap();
        assert_eq!(value, Some(b"head_data".to_vec()));
    }
}
