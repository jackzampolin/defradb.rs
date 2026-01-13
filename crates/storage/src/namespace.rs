/// Namespace isolation for multi-store architectures
///
/// This module provides byte-prefix namespacing to logically separate
/// stores within a single backend. Each store gets a single-byte prefix:
/// - 'd' (0x64): Datastore
/// - 'b' (0x62): Blockstore
/// - 'h' (0x68): Headstore
/// - 's' (0x73): Systemstore
/// - 'p' (0x70): Peerstore
/// - 'e' (0x65): Encstore
///
/// The NamespacedStore wraps any Store implementation and automatically
/// prepends the prefix to all keys, ensuring complete isolation between stores.

use crate::corekv::{Error, IterOptions, Iterator, KvPair, Reader, Result, Store, Txn, Writer};
use async_trait::async_trait;
use std::sync::Arc;

/// Store namespace prefixes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Namespace {
    /// Datastore - document and collection data
    Datastore,
    /// Blockstore - IPLD blocks and merkle tree nodes
    Blockstore,
    /// Headstore - merkle tree heads and definitions
    Headstore,
    /// Systemstore - metadata and configuration
    Systemstore,
    /// Peerstore - peer and replication metadata
    Peerstore,
    /// Encstore - encrypted blocks
    Encstore,
}

impl Namespace {
    /// Get the byte prefix for this namespace
    pub fn prefix(&self) -> u8 {
        match self {
            Namespace::Datastore => b'd',
            Namespace::Blockstore => b'b',
            Namespace::Headstore => b'h',
            Namespace::Systemstore => b's',
            Namespace::Peerstore => b'p',
            Namespace::Encstore => b'e',
        }
    }

    /// Get the namespace name
    pub fn name(&self) -> &'static str {
        match self {
            Namespace::Datastore => "datastore",
            Namespace::Blockstore => "blockstore",
            Namespace::Headstore => "headstore",
            Namespace::Systemstore => "systemstore",
            Namespace::Peerstore => "peerstore",
            Namespace::Encstore => "encstore",
        }
    }

    /// Add prefix to a key
    fn prefix_key(&self, key: &[u8]) -> Vec<u8> {
        let mut prefixed = Vec::with_capacity(1 + key.len());
        prefixed.push(self.prefix());
        prefixed.extend_from_slice(key);
        prefixed
    }

    /// Remove prefix from a key (for internal use)
    fn unprefix_key<'a>(&self, key: &'a [u8]) -> Result<&'a [u8]> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        if key[0] != self.prefix() {
            return Err(Error::Other(format!(
                "Key has wrong prefix: expected {}, got {}",
                self.prefix() as char,
                key[0] as char
            )));
        }
        Ok(&key[1..])
    }
}

/// A namespaced store that wraps a Store implementation and automatically
/// prefixes all keys with a namespace byte
pub struct NamespacedStore<S: Store> {
    store: Arc<S>,
    namespace: Namespace,
}

impl<S: Store> NamespacedStore<S> {
    /// Create a new namespaced store
    pub fn new(store: Arc<S>, namespace: Namespace) -> Self {
        Self { store, namespace }
    }

    /// Get the underlying store
    pub fn inner(&self) -> &Arc<S> {
        &self.store
    }

    /// Get the namespace
    pub fn namespace(&self) -> Namespace {
        self.namespace
    }
}

#[async_trait]
impl<S: Store> Store for NamespacedStore<S> {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        let txn = self.store.new_txn(readonly).await?;
        Ok(Box::new(NamespacedTxn {
            txn,
            namespace: self.namespace,
        }))
    }

    async fn close(&self) -> Result<()> {
        self.store.close().await
    }
}

/// A namespaced transaction that wraps a Txn implementation
pub struct NamespacedTxn {
    txn: Box<dyn Txn>,
    namespace: Namespace,
}

impl NamespacedTxn {
    /// Create a new namespaced transaction
    pub fn new(txn: Box<dyn Txn>, namespace: Namespace) -> Self {
        Self { txn, namespace }
    }
}

#[async_trait]
impl Reader for NamespacedTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let prefixed = self.namespace.prefix_key(key);
        self.txn.get(&prefixed).await
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        let prefixed = self.namespace.prefix_key(key);
        self.txn.has(&prefixed).await
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        let prefixed = self.namespace.prefix_key(key);
        self.txn.get_size(&prefixed).await
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        // Prefix the iterator options
        // IMPORTANT: If no prefix/start/end is specified, we MUST still scope to our namespace
        // to prevent cross-namespace iteration
        let mut prefixed_opts = IterOptions::new();

        if let Some(prefix) = opts.prefix() {
            // User specified a prefix - add namespace prefix to it
            prefixed_opts = prefixed_opts.with_prefix(self.namespace.prefix_key(prefix));
        } else if opts.start().is_none() && opts.end().is_none() {
            // No prefix, start, or end specified - default to namespace prefix
            // This ensures we only iterate within our namespace
            prefixed_opts = prefixed_opts.with_prefix(vec![self.namespace.prefix()]);
        }

        if let Some(start) = opts.start() {
            prefixed_opts = prefixed_opts.with_start(self.namespace.prefix_key(start));
        }
        if let Some(end) = opts.end() {
            prefixed_opts = prefixed_opts.with_end(self.namespace.prefix_key(end));
        }
        prefixed_opts = prefixed_opts
            .with_reverse(opts.reverse())
            .with_keys_only(opts.keys_only());

        let iter = self.txn.iterator(prefixed_opts).await?;
        Ok(Box::new(NamespacedIterator {
            iter,
            namespace: self.namespace,
        }))
    }
}

#[async_trait]
impl Writer for NamespacedTxn {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let prefixed = self.namespace.prefix_key(key);
        self.txn.set(&prefixed, value).await
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        let prefixed = self.namespace.prefix_key(key);
        self.txn.delete(&prefixed).await
    }
}

#[async_trait]
impl Txn for NamespacedTxn {
    async fn commit(self: Box<Self>) -> Result<()> {
        self.txn.commit().await
    }

    fn discard(self: Box<Self>) {
        self.txn.discard()
    }

    fn on_success(&mut self, callback: crate::corekv::TxnCallback) {
        self.txn.on_success(callback)
    }

    fn on_success_async(&mut self, callback: crate::corekv::AsyncTxnCallback) {
        self.txn.on_success_async(callback)
    }

    fn on_error(&mut self, callback: crate::corekv::TxnCallback) {
        self.txn.on_error(callback)
    }

    fn on_error_async(&mut self, callback: crate::corekv::AsyncTxnCallback) {
        self.txn.on_error_async(callback)
    }

    fn on_discard(&mut self, callback: crate::corekv::TxnCallback) {
        self.txn.on_discard(callback)
    }

    fn on_discard_async(&mut self, callback: crate::corekv::AsyncTxnCallback) {
        self.txn.on_discard_async(callback)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn is_readonly(&self) -> bool {
        self.txn.is_readonly()
    }
}

/// A namespaced iterator that strips prefixes from returned keys
pub struct NamespacedIterator {
    iter: Box<dyn Iterator>,
    namespace: Namespace,
}

#[async_trait]
impl Iterator for NamespacedIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        match self.iter.next().await? {
            Some(pair) => {
                // Strip the namespace prefix from the key
                let unprefixed_key = self.namespace.unprefix_key(&pair.key)?;
                Ok(Some(KvPair {
                    key: unprefixed_key.to_vec(),
                    value: pair.value,
                }))
            }
            None => Ok(None),
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.iter.close().await
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::MemoryStore;

    #[test]
    fn test_namespace_prefix() {
        assert_eq!(Namespace::Datastore.prefix(), b'd');
        assert_eq!(Namespace::Blockstore.prefix(), b'b');
        assert_eq!(Namespace::Headstore.prefix(), b'h');
        assert_eq!(Namespace::Systemstore.prefix(), b's');
        assert_eq!(Namespace::Peerstore.prefix(), b'p');
        assert_eq!(Namespace::Encstore.prefix(), b'e');
    }

    #[test]
    fn test_namespace_name() {
        assert_eq!(Namespace::Datastore.name(), "datastore");
        assert_eq!(Namespace::Blockstore.name(), "blockstore");
        assert_eq!(Namespace::Headstore.name(), "headstore");
        assert_eq!(Namespace::Systemstore.name(), "systemstore");
        assert_eq!(Namespace::Peerstore.name(), "peerstore");
        assert_eq!(Namespace::Encstore.name(), "encstore");
    }

    #[test]
    fn test_prefix_key() {
        let ns = Namespace::Datastore;
        let key = b"test_key";
        let prefixed = ns.prefix_key(key);

        assert_eq!(prefixed[0], b'd');
        assert_eq!(&prefixed[1..], key);
    }

    #[test]
    fn test_unprefix_key() {
        let ns = Namespace::Datastore;
        let key = b"test_key";
        let prefixed = ns.prefix_key(key);
        let unprefixed = ns.unprefix_key(&prefixed).unwrap();

        assert_eq!(unprefixed, key);
    }

    #[test]
    fn test_unprefix_wrong_prefix() {
        let ns = Namespace::Datastore;
        let prefixed = b"b/wrong/prefix";
        assert!(ns.unprefix_key(prefixed).is_err());
    }

    #[tokio::test]
    async fn test_namespaced_store_isolation() {
        let store = Arc::new(MemoryStore::new());

        let datastore = NamespacedStore::new(store.clone(), Namespace::Datastore);
        let blockstore = NamespacedStore::new(store.clone(), Namespace::Blockstore);

        // Write to datastore
        let mut txn = datastore.new_txn(false).await.unwrap();
        txn.set(b"key1", b"value1").await.unwrap();
        txn.commit().await.unwrap();

        // Write to blockstore with same key
        let mut txn = blockstore.new_txn(false).await.unwrap();
        txn.set(b"key1", b"value2").await.unwrap();
        txn.commit().await.unwrap();

        // Read from datastore - should get value1
        let txn = datastore.new_txn(true).await.unwrap();
        let value = txn.get(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Read from blockstore - should get value2
        let txn = blockstore.new_txn(true).await.unwrap();
        let value = txn.get(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value2".to_vec()));

        // Keys are isolated - blockstore shouldn't see datastore key
        let txn = blockstore.new_txn(true).await.unwrap();
        let has_key = txn.has(b"key1").await.unwrap();
        assert!(has_key); // Should have its own key1

        // But the actual values are different
        let value = txn.get(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value2".to_vec()));
        assert_ne!(value, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_namespaced_iterator() {
        let store = Arc::new(MemoryStore::new());
        let datastore = NamespacedStore::new(store.clone(), Namespace::Datastore);

        // Write multiple keys
        let mut txn = datastore.new_txn(false).await.unwrap();
        txn.set(b"key1", b"value1").await.unwrap();
        txn.set(b"key2", b"value2").await.unwrap();
        txn.set(b"key3", b"value3").await.unwrap();
        txn.commit().await.unwrap();

        // Iterate
        let txn = datastore.new_txn(true).await.unwrap();
        let mut iter = txn.iterator(IterOptions::default()).await.unwrap();

        let mut count = 0;
        while let Some(pair) = iter.next().await.unwrap() {
            // Keys should not have the 'd' prefix
            assert_ne!(pair.key[0], b'd');
            assert!(pair.key.starts_with(b"key"));
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_namespace_prefix_iteration() {
        let store = Arc::new(MemoryStore::new());
        let datastore = NamespacedStore::new(store.clone(), Namespace::Datastore);

        // Write keys with common prefix
        let mut txn = datastore.new_txn(false).await.unwrap();
        txn.set(b"user/1", b"alice").await.unwrap();
        txn.set(b"user/2", b"bob").await.unwrap();
        txn.set(b"post/1", b"hello").await.unwrap();
        txn.commit().await.unwrap();

        // Iterate with prefix
        let txn = datastore.new_txn(true).await.unwrap();
        let opts = IterOptions::new().with_prefix(b"user/".to_vec());
        let mut iter = txn.iterator(opts).await.unwrap();

        let mut count = 0;
        while let Some(pair) = iter.next().await.unwrap() {
            assert!(pair.key.starts_with(b"user/"));
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_namespace_no_prefix_collision() {
        let store = Arc::new(MemoryStore::new());

        // Create a key in datastore that starts with 'b' (blockstore prefix)
        // This tests that namespace isolation prevents cross-namespace access
        let datastore = NamespacedStore::new(store.clone(), Namespace::Datastore);
        let mut txn = datastore.new_txn(false).await.unwrap();
        txn.set(b"bmalicious_key", b"datastore_value").await.unwrap();
        txn.commit().await.unwrap();

        // Blockstore should NOT see this key, even though the key starts with 'b'
        let blockstore = NamespacedStore::new(store.clone(), Namespace::Blockstore);
        let txn = blockstore.new_txn(true).await.unwrap();

        // The key "malicious_key" should not exist in blockstore
        // (because the actual stored key is "d" + "bmalicious_key", not "b" + "malicious_key")
        let value = txn.get(b"malicious_key").await.unwrap();
        assert_eq!(value, None, "Blockstore should not see datastore key");

        // Also check the key with 'b' prefix doesn't exist in blockstore
        let value = txn.get(b"bmalicious_key").await.unwrap();
        assert_eq!(value, None, "Blockstore should not see key starting with 'b' from datastore");
    }

    #[tokio::test]
    async fn test_namespace_default_prefix_scoping() {
        // Test that iterating with no prefix still stays within namespace
        let store = Arc::new(MemoryStore::new());

        // Write to multiple namespaces
        let datastore = NamespacedStore::new(store.clone(), Namespace::Datastore);
        let blockstore = NamespacedStore::new(store.clone(), Namespace::Blockstore);

        let mut txn = datastore.new_txn(false).await.unwrap();
        txn.set(b"ds_key1", b"ds_value1").await.unwrap();
        txn.set(b"ds_key2", b"ds_value2").await.unwrap();
        txn.commit().await.unwrap();

        let mut txn = blockstore.new_txn(false).await.unwrap();
        txn.set(b"bs_key1", b"bs_value1").await.unwrap();
        txn.commit().await.unwrap();

        // Iterate datastore with no prefix - should only see datastore keys
        let txn = datastore.new_txn(true).await.unwrap();
        let opts = IterOptions::default(); // No prefix, start, or end
        let mut iter = txn.iterator(opts).await.unwrap();

        let mut ds_keys: Vec<String> = vec![];
        while let Some(pair) = iter.next().await.unwrap() {
            ds_keys.push(pair.key_str());
        }
        drop(txn);

        // Should only see datastore keys, not blockstore keys
        assert_eq!(ds_keys.len(), 2);
        assert!(ds_keys.contains(&"ds_key1".to_string()));
        assert!(ds_keys.contains(&"ds_key2".to_string()));
        assert!(!ds_keys.contains(&"bs_key1".to_string()));

        // Similarly, blockstore iteration should only see blockstore keys
        let txn = blockstore.new_txn(true).await.unwrap();
        let opts = IterOptions::default();
        let mut iter = txn.iterator(opts).await.unwrap();

        let mut bs_keys: Vec<String> = vec![];
        while let Some(pair) = iter.next().await.unwrap() {
            bs_keys.push(pair.key_str());
        }

        assert_eq!(bs_keys.len(), 1);
        assert!(bs_keys.contains(&"bs_key1".to_string()));
    }
}
