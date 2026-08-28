use bytes::Bytes;
use async_trait::async_trait;
use parking_lot::Mutex;
use rusty_leveldb::{WriteBatch, DB};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::iterator::LevelDbIterator;
use crate::backends::shared::CallbackManager;
use crate::corekv::{
    AsyncTxnCallback, Error, IterOptions, Iterator, Reader, Result, Txn, TxnCallback, Writer,
};

/// LevelDB transaction with snapshot isolation.
///
/// Transactions maintain a snapshot of the store at creation time and track
/// pending changes. Changes are applied atomically on commit using WriteBatch.
pub(crate) struct LevelDbTxn {
    /// Reference to the store's DB
    pub(crate) store: Rc<std::cell::RefCell<Option<DB>>>,

    /// Snapshot of store at transaction start (for reads)
    pub(crate) snapshot: BTreeMap<Vec<u8>, Vec<u8>>,

    /// Pending changes (Some(value) = set, None = delete)
    pub(crate) pending: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,

    /// Whether this is a read-only transaction
    pub(crate) readonly: bool,

    /// Whether the transaction has been discarded
    pub(crate) discarded: AtomicBool,

    /// Whether the transaction has been committed
    pub(crate) committed: AtomicBool,

    /// Transaction lifecycle callbacks
    pub(crate) callbacks: CallbackManager,
}

impl LevelDbTxn {
    /// Get a value, checking pending changes first, then snapshot.
    fn get_internal(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Check pending changes first
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return pending_value.clone();
        }

        // Fall back to snapshot
        self.snapshot.get(key).cloned()
    }

    /// Check if a key exists.
    fn has_internal(&self, key: &[u8]) -> bool {
        // Check pending changes first
        let pending = self.pending.lock();
        if let Some(pending_value) = pending.get(key) {
            return pending_value.is_some();
        }

        // Fall back to snapshot
        self.snapshot.contains_key(key)
    }
}

impl crate::corekv::private::Sealed for LevelDbTxn {}

#[async_trait(?Send)]
impl Reader for LevelDbTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        Ok(self.get_internal(key))
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        Ok(self.has_internal(key))
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        Ok(self.get_internal(key).map(|v| v.len()))
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        // Merge snapshot and pending changes
        let mut merged = self.snapshot.clone();
        let pending = self.pending.lock();
        for (key, value) in pending.iter() {
            match value {
                Some(v) => {
                    merged.insert(key.clone(), v.clone());
                }
                None => {
                    merged.remove(key);
                }
            }
        }

        Ok(Box::new(LevelDbIterator::new(merged, opts)?))
    }
}

#[async_trait(?Send)]
impl Writer for LevelDbTxn {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        if self.readonly {
            return Err(Error::ReadOnlyTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        self.pending
            .lock()
            .insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        if self.discarded.load(Ordering::Acquire) {
            return Err(Error::DiscardedTxn);
        }

        if self.readonly {
            return Err(Error::ReadOnlyTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        self.pending.lock().insert(key.to_vec(), None);
        Ok(())
    }
}

#[async_trait(?Send)]
impl Txn for LevelDbTxn {
    async fn commit(self: Box<Self>) -> Result<()> {
        // Check discarded first
        if self.discarded.load(Ordering::Acquire) {
            tracing::warn!("Attempted to commit a discarded transaction");
            CallbackManager::execute_callbacks(self.callbacks.take_error());
            CallbackManager::execute_async_callbacks(self.callbacks.take_error_async()).await;
            return Err(Error::DiscardedTxn);
        }

        // Check if already committed
        if self.committed.load(Ordering::Acquire) {
            tracing::warn!("Attempted to commit an already committed transaction");
            return Err(Error::Other("Transaction already committed".into()));
        }

        // Mark as committed
        self.committed.store(true, Ordering::Release);

        // Clone pending changes before accessing DB
        let pending = self.pending.lock().clone();

        // Apply pending changes to LevelDB using WriteBatch
        if !pending.is_empty() {
            let mut db_ref = self.store.borrow_mut();
            let db = db_ref.as_mut().ok_or(Error::DBClosed)?;

            let mut batch = WriteBatch::default();
            for (key, value) in pending.iter() {
                match value {
                    Some(v) => batch.put(key, v),
                    None => batch.delete(key),
                }
            }

            db.write(batch, true)
                .map_err(|e| Error::Backend(e.to_string()))?;
        }

        // Execute success callbacks
        CallbackManager::execute_callbacks(self.callbacks.take_success());
        CallbackManager::execute_async_callbacks(self.callbacks.take_success_async()).await;

        Ok(())
    }

    fn discard(self: Box<Self>) {
        self.discarded.store(true, Ordering::Release);

        // Execute sync discard callbacks
        CallbackManager::execute_callbacks(self.callbacks.take_discard());

        // Handle async callbacks
        let on_discard_async = self.callbacks.take_discard_async();
        if !on_discard_async.is_empty() {
            let callback_count = on_discard_async.len();
            tracing::warn!(
                count = callback_count,
                "Transaction has async discard callbacks. Spawning in background."
            );

            wasm_bindgen_futures::spawn_local(async move {
                CallbackManager::execute_async_callbacks(on_discard_async).await;
            });
        }
    }

    fn on_success(&mut self, callback: TxnCallback) {
        self.callbacks.register_success(callback);
    }

    fn on_success_async(&mut self, callback: AsyncTxnCallback) {
        self.callbacks.register_success_async(callback);
    }

    fn on_error(&mut self, callback: TxnCallback) {
        self.callbacks.register_error(callback);
    }

    fn on_error_async(&mut self, callback: AsyncTxnCallback) {
        self.callbacks.register_error_async(callback);
    }

    fn on_discard(&mut self, callback: TxnCallback) {
        self.callbacks.register_discard(callback);
    }

    fn on_discard_async(&mut self, callback: AsyncTxnCallback) {
        self.callbacks.register_discard_async(callback);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn is_readonly(&self) -> bool {
        self.readonly
    }

    fn callback_count(&self) -> usize {
        self.callbacks.count()
    }
}
