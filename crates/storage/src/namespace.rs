use crate::corekv::{Error, IterOptions, Iterator, KvPair, Reader, Result, Store, Txn, Writer};
use async_trait::async_trait;
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
/// - 'a' (0x61): Acpstore
///
/// The NamespacedStore wraps any Store implementation and automatically
/// prepends the prefix to all keys, ensuring complete isolation between stores.
use bytes::Bytes;
use std::sync::Arc;

/// Store namespace prefixes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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
    /// Acpstore - Access Control Policy tuples
    Acpstore,
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
            Namespace::Acpstore => b'a',
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
            Namespace::Acpstore => "acpstore",
        }
    }

    /// Add prefix to a key
    pub(crate) fn prefix_key(&self, key: &[u8]) -> Vec<u8> {
        let mut prefixed = Vec::with_capacity(1 + key.len());
        prefixed.push(self.prefix());
        prefixed.extend_from_slice(key);
        prefixed
    }

    /// Remove prefix from a key (for internal use)
    pub(crate) fn unprefix_key<'a>(&self, key: &'a [u8]) -> Result<&'a [u8]> {
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

impl<S: Store> crate::corekv::private::Sealed for NamespacedStore<S> {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store> Store for NamespacedStore<S> {
    #[cfg(not(target_arch = "wasm32"))]
    fn transaction_stats_handle(&self) -> Option<crate::backends::TransactionStatsHandle> {
        self.store.transaction_stats_handle()
    }

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

impl crate::corekv::private::Sealed for NamespacedTxn {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Reader for NamespacedTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
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

        // Always scope to namespace via prefix to prevent cross-namespace data leakage
        if let Some(prefix) = opts.prefix() {
            // User specified a prefix - add namespace prefix to it
            prefixed_opts = prefixed_opts.with_prefix(self.namespace.prefix_key(prefix));
        } else {
            // No user prefix specified - default to namespace prefix
            // This ensures we ALWAYS iterate within our namespace, even when start/end are specified
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
        if opts.commutative_set() {
            prefixed_opts = prefixed_opts.with_commutative_set();
        }

        let iter = self.txn.iterator(prefixed_opts).await?;
        Ok(Box::new(NamespacedIterator {
            iter,
            namespace: self.namespace,
        }))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

    fn callback_count(&self) -> usize {
        self.txn.callback_count()
    }
}

/// A namespaced iterator that strips prefixes from returned keys
pub struct NamespacedIterator {
    iter: Box<dyn Iterator>,
    namespace: Namespace,
}

impl crate::corekv::private::Sealed for NamespacedIterator {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        // Prefix the key before seeking in the underlying iterator
        let prefixed_key = self.namespace.prefix_key(key);
        self.iter.seek(&prefixed_key).await
    }

    async fn reset(&mut self) -> Result<()> {
        self.iter.reset().await
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }
}

// Tests extracted to crates/storage/tests/namespace_tests.rs
