use async_lock::RwLock;
/// Transaction-aware Multistore for the datastore layer.
///
/// Unlike the storage layer's Multistore which provides store-level access,
/// this provides namespaced access to a single underlying transaction.
/// This matches Go DefraDB's internal/datastore/multi.go pattern.
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use storage::corekv::{Error, IterOptions, Iterator, Reader, Result, Txn, Writer};
use storage::namespace::Namespace;

/// Shared transaction wrapper allowing multiple namespace views.
pub struct SharedTxn {
    txn: RwLock<Box<dyn Txn>>,
}

impl SharedTxn {
    /// Create a new shared transaction.
    // `Txn` drops its `Send + Sync` bounds on wasm32 (see `MaybeSendSync`), so
    // there the Arc wraps a non-Send value. That is sound in the
    // single-threaded browser runtime, and the return type has to stay `Arc`
    // for native callers that share it across threads.
    #[cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]
    pub fn new(txn: Box<dyn Txn>) -> Arc<Self> {
        Arc::new(Self {
            txn: RwLock::new(txn),
        })
    }

    /// Get a value with namespace prefix.
    pub async fn get(&self, namespace: Namespace, key: &[u8]) -> Result<Option<Bytes>> {
        let prefixed = prefix_key(namespace, key);
        let txn = self.txn.read().await;
        txn.get(&prefixed).await
    }

    /// Check if a key exists with namespace prefix.
    pub async fn has(&self, namespace: Namespace, key: &[u8]) -> Result<bool> {
        let prefixed = prefix_key(namespace, key);
        let txn = self.txn.read().await;
        txn.has(&prefixed).await
    }

    /// Existence check that enters the key into the transaction's read set.
    /// See [`storage::corekv::Reader::has_for_update`].
    pub async fn has_for_update(&self, namespace: Namespace, key: &[u8]) -> Result<bool> {
        let prefixed = prefix_key(namespace, key);
        let txn = self.txn.read().await;
        txn.has_for_update(&prefixed).await
    }

    /// Get value size with namespace prefix.
    pub async fn get_size(&self, namespace: Namespace, key: &[u8]) -> Result<Option<usize>> {
        let prefixed = prefix_key(namespace, key);
        let txn = self.txn.read().await;
        txn.get_size(&prefixed).await
    }

    /// Set a value with namespace prefix.
    pub async fn set(&self, namespace: Namespace, key: &[u8], value: &[u8]) -> Result<()> {
        let prefixed = prefix_key(namespace, key);
        let mut txn = self.txn.write().await;
        txn.set(&prefixed, value).await
    }

    /// Delete a key with namespace prefix.
    pub async fn delete(&self, namespace: Namespace, key: &[u8]) -> Result<()> {
        let prefixed = prefix_key(namespace, key);
        let mut txn = self.txn.write().await;
        txn.delete(&prefixed).await
    }

    /// Create an iterator with namespace prefix.
    pub async fn iterator(
        &self,
        namespace: Namespace,
        opts: IterOptions,
    ) -> Result<Box<dyn Iterator>> {
        let prefixed_opts = prefix_iter_options(namespace, opts);
        let txn = self.txn.read().await;
        let iter = txn.iterator(prefixed_opts).await?;
        Ok(Box::new(NamespacedIterator { iter, namespace }))
    }

    /// Get from rootstore (no namespace prefix).
    pub async fn root_get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.txn.read().await;
        txn.get(key).await
    }

    /// Set in rootstore (no namespace prefix).
    pub async fn root_set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut txn = self.txn.write().await;
        txn.set(key, value).await
    }

    /// Delete from rootstore (no namespace prefix).
    pub async fn root_delete(&self, key: &[u8]) -> Result<()> {
        let mut txn = self.txn.write().await;
        txn.delete(key).await
    }

    /// Check if readonly.
    pub async fn is_readonly(&self) -> bool {
        let txn = self.txn.read().await;
        txn.is_readonly()
    }

    /// Consume self and return the underlying transaction.
    ///
    /// This takes ownership of the inner RwLock and extracts the transaction.
    pub fn into_txn(self) -> Box<dyn Txn> {
        self.txn.into_inner()
    }
}

/// A view into a specific namespace of a shared transaction.
pub struct NamespaceView {
    txn: Arc<SharedTxn>,
    namespace: Namespace,
}

impl NamespaceView {
    /// Create a new namespace view.
    pub fn new(txn: Arc<SharedTxn>, namespace: Namespace) -> Self {
        Self { txn, namespace }
    }

    /// View a different namespace of the same transaction.
    ///
    /// Lets a caller holding one view reach a sibling namespace without
    /// threading the transaction (or a second view) through its signature.
    pub fn sibling(&self, namespace: Namespace) -> Self {
        Self {
            txn: self.txn.clone(),
            namespace,
        }
    }

    /// Get a value.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.txn.get(self.namespace, key).await
    }

    /// Check if a key exists.
    pub async fn has(&self, key: &[u8]) -> Result<bool> {
        self.txn.has(self.namespace, key).await
    }

    /// Existence check that enters the key into the transaction's read set.
    /// See [`storage::corekv::Reader::has_for_update`].
    pub async fn has_for_update(&self, key: &[u8]) -> Result<bool> {
        self.txn.has_for_update(self.namespace, key).await
    }

    /// Get value size.
    pub async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        self.txn.get_size(self.namespace, key).await
    }

    /// Set a value.
    pub async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.txn.set(self.namespace, key, value).await
    }

    /// Delete a key.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.txn.delete(self.namespace, key).await
    }

    /// Create an iterator.
    pub async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        self.txn.iterator(self.namespace, opts).await
    }
}

impl Clone for NamespaceView {
    fn clone(&self) -> Self {
        Self {
            txn: self.txn.clone(),
            namespace: self.namespace,
        }
    }
}

impl storage::corekv::private::Sealed for NamespaceView {}

/// Implement Reader trait for NamespaceView to enable index operations.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Reader for NamespaceView {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.txn.get(self.namespace, key).await
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        self.txn.has(self.namespace, key).await
    }

    async fn has_for_update(&self, key: &[u8]) -> Result<bool> {
        self.txn.has_for_update(self.namespace, key).await
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        self.txn.get_size(self.namespace, key).await
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        self.txn.iterator(self.namespace, opts).await
    }
}

/// Implement Writer trait for NamespaceView to enable index operations.
///
/// The `&mut self` signature is required by the trait, but NamespaceView uses
/// interior mutability (Arc<RwLock>) so actual mutation is handled internally.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Writer for NamespaceView {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.txn.set(self.namespace, key, value).await
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.txn.delete(self.namespace, key).await
    }
}

/// A view into the rootstore (no namespace prefix).
pub struct RootView {
    txn: Arc<SharedTxn>,
}

impl RootView {
    /// Create a new root view.
    pub fn new(txn: Arc<SharedTxn>) -> Self {
        Self { txn }
    }

    /// Get a value.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.txn.root_get(key).await
    }

    /// Set a value.
    pub async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.txn.root_set(key, value).await
    }

    /// Delete a key.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.txn.root_delete(key).await
    }
}

/// Helper to prefix a key with namespace.
fn prefix_key(namespace: Namespace, key: &[u8]) -> Vec<u8> {
    let mut prefixed = Vec::with_capacity(1 + key.len());
    prefixed.push(namespace.prefix());
    prefixed.extend_from_slice(key);
    prefixed
}

/// Helper to unprefix a key.
fn unprefix_key(namespace: Namespace, key: &[u8]) -> Result<Vec<u8>> {
    if key.is_empty() {
        return Err(Error::EmptyKey);
    }
    if key[0] != namespace.prefix() {
        return Err(Error::Other(format!(
            "Key has wrong prefix: expected {:?} (0x{:02x}), got 0x{:02x}",
            namespace.prefix() as char,
            namespace.prefix(),
            key[0]
        )));
    }
    Ok(key[1..].to_vec())
}

/// Helper to prefix iterator options.
fn prefix_iter_options(namespace: Namespace, opts: IterOptions) -> IterOptions {
    let mut prefixed_opts = IterOptions::new();

    if let Some(prefix) = opts.prefix() {
        prefixed_opts = prefixed_opts.with_prefix(prefix_key(namespace, prefix));
    } else if opts.start().is_none() && opts.end().is_none() {
        prefixed_opts = prefixed_opts.with_prefix(vec![namespace.prefix()]);
    }

    if let Some(start) = opts.start() {
        prefixed_opts = prefixed_opts.with_start(prefix_key(namespace, start));
    }
    if let Some(end) = opts.end() {
        prefixed_opts = prefixed_opts.with_end(prefix_key(namespace, end));
    }
    prefixed_opts = prefixed_opts
        .with_reverse(opts.reverse())
        .with_keys_only(opts.keys_only());

    prefixed_opts
}

/// Iterator that strips namespace prefix from keys.
struct NamespacedIterator {
    iter: Box<dyn Iterator>,
    namespace: Namespace,
}

impl storage::corekv::private::Sealed for NamespacedIterator {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Iterator for NamespacedIterator {
    async fn next(&mut self) -> Result<Option<storage::corekv::KvPair>> {
        match self.iter.next().await? {
            Some(pair) => {
                let unprefixed_key = unprefix_key(self.namespace, &pair.key)?;
                Ok(Some(storage::corekv::KvPair {
                    key: unprefixed_key,
                    value: pair.value,
                }))
            }
            None => Ok(None),
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.iter.close().await
    }

    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        let prefixed_key = prefix_key(self.namespace, key);
        self.iter.seek(&prefixed_key).await
    }

    async fn reset(&mut self) -> Result<()> {
        self.iter.reset().await
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::corekv::Store;
    use storage::RegolithStore;

    #[tokio::test]
    async fn test_shared_txn_namespace_isolation() {
        let store = RegolithStore::in_memory().unwrap();
        let txn = store.new_txn(false).await.unwrap();
        let shared = SharedTxn::new(txn);

        // Write to datastore namespace
        shared
            .set(Namespace::Datastore, b"key", b"datastore_value")
            .await
            .unwrap();

        // Write to systemstore namespace
        shared
            .set(Namespace::Systemstore, b"key", b"systemstore_value")
            .await
            .unwrap();

        // Read back - each should see its own value
        assert_eq!(
            shared.get(Namespace::Datastore, b"key").await.unwrap(),
            Some(Bytes::from_static(b"datastore_value"))
        );
        assert_eq!(
            shared.get(Namespace::Systemstore, b"key").await.unwrap(),
            Some(Bytes::from_static(b"systemstore_value"))
        );
    }

    #[tokio::test]
    async fn test_namespace_view() {
        let store = RegolithStore::in_memory().unwrap();
        let txn = store.new_txn(false).await.unwrap();
        let shared = SharedTxn::new(txn);

        let ds = NamespaceView::new(shared.clone(), Namespace::Datastore);
        let ss = NamespaceView::new(shared.clone(), Namespace::Systemstore);

        // Write through views
        ds.set(b"key", b"datastore_value").await.unwrap();
        ss.set(b"key", b"systemstore_value").await.unwrap();

        // Read back
        assert_eq!(
            ds.get(b"key").await.unwrap(),
            Some(Bytes::from_static(b"datastore_value"))
        );
        assert_eq!(
            ss.get(b"key").await.unwrap(),
            Some(Bytes::from_static(b"systemstore_value"))
        );
    }

    #[tokio::test]
    async fn test_root_view_sees_prefixed() {
        let store = RegolithStore::in_memory().unwrap();
        let txn = store.new_txn(false).await.unwrap();
        let shared = SharedTxn::new(txn);

        let ds = NamespaceView::new(shared.clone(), Namespace::Datastore);
        let root = RootView::new(shared.clone());

        // Write through datastore view
        ds.set(b"mykey", b"value").await.unwrap();

        // Root should see it with 'd' prefix
        let value = root.get(b"dmykey").await.unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"value")));
    }

    #[tokio::test]
    async fn test_namespace_iterator() {
        let store = RegolithStore::in_memory().unwrap();
        let txn = store.new_txn(false).await.unwrap();
        let shared = SharedTxn::new(txn);

        let ds = NamespaceView::new(shared.clone(), Namespace::Datastore);

        // Write multiple keys
        ds.set(b"key1", b"value1").await.unwrap();
        ds.set(b"key2", b"value2").await.unwrap();
        ds.set(b"key3", b"value3").await.unwrap();

        // Iterate
        let mut iter = ds.iterator(IterOptions::new()).await.unwrap();
        let mut count = 0;
        while let Some(pair) = iter.next().await.unwrap() {
            assert!(pair.key.starts_with(b"key"));
            count += 1;
        }
        assert_eq!(count, 3);
    }
}
