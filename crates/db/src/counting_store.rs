//! A test-only [`Store`] decorator that counts real storage reads.
//!
//! Unlike counting documents handed to the query plan, this counts what a
//! query actually pulls out of the underlying store: keys yielded by an
//! iterator, and point `get`s. A fetcher that materializes an entire
//! collection before yielding a `LIMIT`-bounded slice cannot hide behind
//! plan-level document counts when instrumented with this decorator.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use storage::corekv::{
    private::Sealed, AsyncTxnCallback, IterOptions, Iterator, KvPair, Reader, Result, Store, Txn,
    TxnCallback, Writer,
};

#[derive(Default)]
struct Counts {
    keys_read: AtomicUsize,
    point_gets: AtomicUsize,
}

/// Wraps a store and counts what a query actually pulls out of it.
pub(crate) struct CountingStore<S: Store> {
    inner: S,
    counts: Arc<Counts>,
}

impl<S: Store> CountingStore<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            counts: Arc::new(Counts::default()),
        }
    }

    /// Keys yielded by an iterator.
    pub(crate) fn keys_read(&self) -> usize {
        self.counts.keys_read.load(Ordering::SeqCst)
    }

    /// Single-key lookups, which document assembly performs per document.
    pub(crate) fn point_gets(&self) -> usize {
        self.counts.point_gets.load(Ordering::SeqCst)
    }
}

impl<S: Store> Sealed for CountingStore<S> {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store> Store for CountingStore<S> {
    #[cfg(not(target_arch = "wasm32"))]
    fn transaction_stats_handle(&self) -> Option<storage::backends::TransactionStatsHandle> {
        self.inner.transaction_stats_handle()
    }

    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        let inner = self.inner.new_txn(readonly).await?;
        Ok(Box::new(CountingTxn {
            inner,
            counts: Arc::clone(&self.counts),
        }))
    }

    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}

/// A [`Txn`] wrapper that counts point `get`s and hands out counting iterators.
struct CountingTxn {
    inner: Box<dyn Txn>,
    counts: Arc<Counts>,
}

impl Sealed for CountingTxn {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Reader for CountingTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.counts.point_gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key).await
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        self.inner.has(key).await
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        self.inner.get_size(key).await
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        Ok(Box::new(CountingIterator {
            inner: self.inner.iterator(opts).await?,
            counts: Arc::clone(&self.counts),
        }))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Writer for CountingTxn {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.set(key, value).await
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.inner.delete(key).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Txn for CountingTxn {
    async fn commit(self: Box<Self>) -> Result<()> {
        self.inner.commit().await
    }

    fn discard(self: Box<Self>) {
        self.inner.discard()
    }

    fn on_success(&mut self, callback: TxnCallback) {
        self.inner.on_success(callback)
    }

    fn on_success_async(&mut self, callback: AsyncTxnCallback) {
        self.inner.on_success_async(callback)
    }

    fn on_error(&mut self, callback: TxnCallback) {
        self.inner.on_error(callback)
    }

    fn on_error_async(&mut self, callback: AsyncTxnCallback) {
        self.inner.on_error_async(callback)
    }

    fn on_discard(&mut self, callback: TxnCallback) {
        self.inner.on_discard(callback)
    }

    fn on_discard_async(&mut self, callback: AsyncTxnCallback) {
        self.inner.on_discard_async(callback)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn is_readonly(&self) -> bool {
        self.inner.is_readonly()
    }

    fn callback_count(&self) -> usize {
        self.inner.callback_count()
    }
}

/// An [`Iterator`] wrapper that counts each key yielded by `next`.
struct CountingIterator {
    inner: Box<dyn Iterator>,
    counts: Arc<Counts>,
}

impl Sealed for CountingIterator {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Iterator for CountingIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        let pulled = self.inner.next().await?;
        if pulled.is_some() {
            self.counts.keys_read.fetch_add(1, Ordering::SeqCst);
        }
        Ok(pulled)
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }

    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.inner.seek(key).await
    }

    async fn reset(&mut self) -> Result<()> {
        self.inner.reset().await
    }

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::corekv::{IterOptions, Store};

    #[tokio::test]
    async fn counts_keys_pulled_from_the_iterator_not_keys_present() {
        let store = CountingStore::new(storage::MemoryStore::new());

        let mut txn = store.new_txn(false).await.unwrap();
        for i in 0..100u32 {
            txn.set(format!("k{i:03}").as_bytes(), b"v").await.unwrap();
        }
        txn.commit().await.unwrap();

        let before = store.keys_read();
        let txn = store.new_txn(true).await.unwrap();
        let mut iter = txn.iterator(IterOptions::new()).await.unwrap();
        for _ in 0..3 {
            iter.next().await.unwrap().unwrap();
        }

        assert_eq!(store.keys_read() - before, 3);
    }

    #[tokio::test]
    async fn counts_point_gets_separately() {
        let store = CountingStore::new(storage::MemoryStore::new());

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"a", b"1").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        txn.get(b"a").await.unwrap();
        txn.get(b"missing").await.unwrap();

        assert_eq!(store.point_gets(), 2);
        assert_eq!(store.keys_read(), 0);
    }
}
