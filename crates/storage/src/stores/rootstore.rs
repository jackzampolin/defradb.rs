/// RootStore - Foundation store wrapper
///
/// The RootStore provides direct access to the underlying store without
/// namespace prefixing. It serves as the foundation for all other stores
/// and is used for operations that need to span multiple namespaces.
use crate::corekv::{Result, Store, Txn};
use async_trait::async_trait;
use std::sync::Arc;

/// RootStore wraps a backend store and provides the foundation for
/// all specialized stores
pub struct RootStore<S: Store> {
    store: Arc<S>,
}

impl<S: Store> RootStore<S> {
    /// Create a new RootStore
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }

    /// Get the underlying store
    pub fn inner(&self) -> &Arc<S> {
        &self.store
    }
}

impl<S: Store> crate::corekv::private::Sealed for RootStore<S> {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store> Store for RootStore<S> {
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

    #[tokio::test]
    async fn test_rootstore_basic() {
        let store = Arc::new(MemoryStore::new());
        let rootstore = RootStore::new(store);

        // Write data
        let mut txn = rootstore.new_txn(false).await.unwrap();
        txn.set(b"key1", b"value1").await.unwrap();
        txn.commit().await.unwrap();

        // Read back
        let txn = rootstore.new_txn(true).await.unwrap();
        let value = txn.get(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_rootstore_no_namespace_prefix() {
        let store = Arc::new(MemoryStore::new());
        let rootstore = RootStore::new(store);

        // Write with explicit key
        let mut txn = rootstore.new_txn(false).await.unwrap();
        txn.set(b"d/key1", b"value1").await.unwrap();
        txn.commit().await.unwrap();

        // Read back with same key - no automatic prefixing
        let txn = rootstore.new_txn(true).await.unwrap();
        let value = txn.get(b"d/key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Different prefix is different key
        let value = txn.get(b"b/key1").await.unwrap();
        assert_eq!(value, None);
    }
}
