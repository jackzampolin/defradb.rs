//! LevelDB backend implementation using rusty-leveldb (WASM only).
//!
//! This backend provides a pure Rust LevelDB implementation for WASM targets.
//! In WASM, it can use OPFS via the `OpfsEnv` environment for browser persistence.
//!
//! # Platform Notes
//!
//! This module is WASM-only because rusty-leveldb uses `Rc` internally which
//! is not `Send + Sync`. For native platforms, use `RedbStore` instead which
//! provides full concurrency support.
//!
//! # Features
//!
//! - Pure Rust LSM-tree implementation (no C dependencies)
//! - Supports custom `Env` implementations for different storage backends
//! - Full transaction support with snapshot isolation
//! - Compatible with Go DefraDB's LevelDB storage

use async_trait::async_trait;
use parking_lot::Mutex;
use rusty_leveldb::{LdbIterator, Options, WriteBatch, DB};
use std::collections::BTreeMap;
use std::path::Path;
use std::rc::Rc;

use crate::corekv::{
    AsyncTxnCallback, Dropable, Error, IterOptions, Iterator, KvPair, Reader, Result, Store, Txn,
    TxnCallback, Writer,
};

/// LevelDB-backed key-value store for WASM.
///
/// This store wraps rusty-leveldb's `DB` type and provides the CoreKV
/// `Store` interface for DefraDB compatibility.
pub struct LevelDbStore {
    /// The underlying LevelDB database.
    /// Wrapped in Rc<RefCell> because rusty-leveldb::DB is not Send.
    db: Rc<std::cell::RefCell<Option<DB>>>,
    /// Path to the database (for error messages)
    #[allow(dead_code)]
    path: String,
    /// Whether the store is closed
    closed: Rc<std::cell::RefCell<bool>>,
}

impl LevelDbStore {
    /// Open or create a LevelDB database at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database directory
    ///
    /// # Returns
    ///
    /// A new `LevelDbStore` instance, or an error if the database could not be opened.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        let options = Options {
            create_if_missing: true,
            ..Options::default()
        };

        let db = DB::open(&path_str, options).map_err(|e| Error::Backend(e.to_string()))?;

        Ok(Self {
            db: Rc::new(std::cell::RefCell::new(Some(db))),
            path: path_str,
            closed: Rc::new(std::cell::RefCell::new(false)),
        })
    }

    /// Open or create a LevelDB database with custom options.
    ///
    /// This allows specifying a custom `Env` for OPFS or other storage backends.
    pub fn open_with_options<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        let db = DB::open(&path_str, options).map_err(|e| Error::Backend(e.to_string()))?;

        Ok(Self {
            db: Rc::new(std::cell::RefCell::new(Some(db))),
            path: path_str,
            closed: Rc::new(std::cell::RefCell::new(false)),
        })
    }

    /// Check if the store is closed.
    fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    /// Get a mutable reference to the DB.
    fn get_db_mut(&self) -> Result<std::cell::RefMut<'_, Option<DB>>> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }
        Ok(self.db.borrow_mut())
    }
}

#[async_trait(?Send)]
impl Store for LevelDbStore {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        // Create a snapshot for read isolation
        let mut db_ref = self.get_db_mut()?;
        let db = db_ref.as_mut().ok_or(Error::DBClosed)?;

        // Read current state into snapshot
        let mut snapshot = BTreeMap::new();
        let mut iter = db.new_iter().map_err(|e| Error::Backend(e.to_string()))?;

        // Position iterator at first element and iterate through all keys
        iter.seek_to_first();
        while iter.valid() {
            if let Some((key, value)) = iter.current() {
                snapshot.insert(key.to_vec(), value.to_vec());
            }
            iter.advance();
        }

        drop(db_ref);

        Ok(Box::new(LevelDbTxn {
            store: self.db.clone(),
            snapshot,
            pending: Mutex::new(BTreeMap::new()),
            readonly,
            discarded: Mutex::new(false),
            committed: Mutex::new(false),
            on_success: Mutex::new(Vec::new()),
            on_success_async: Mutex::new(Vec::new()),
            on_error: Mutex::new(Vec::new()),
            on_error_async: Mutex::new(Vec::new()),
            on_discard: Mutex::new(Vec::new()),
            on_discard_async: Mutex::new(Vec::new()),
        }))
    }

    async fn close(&self) -> Result<()> {
        *self.closed.borrow_mut() = true;
        // Drop the DB to flush and close
        *self.db.borrow_mut() = None;
        Ok(())
    }
}

#[async_trait(?Send)]
impl Dropable for LevelDbStore {
    async fn drop_all(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        let mut db_ref = self.get_db_mut()?;
        let db = db_ref.as_mut().ok_or(Error::DBClosed)?;

        // Collect all keys
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut iter = db.new_iter().map_err(|e| Error::Backend(e.to_string()))?;
        iter.seek_to_first();
        while iter.valid() {
            if let Some((key, _)) = iter.current() {
                keys.push(key.to_vec());
            }
            iter.advance();
        }

        // Delete all keys
        let mut batch = WriteBatch::default();
        for key in &keys {
            batch.delete(key);
        }

        db.write(batch, true)
            .map_err(|e| Error::Backend(e.to_string()))?;

        Ok(())
    }
}

/// LevelDB transaction with snapshot isolation.
///
/// Transactions maintain a snapshot of the store at creation time and track
/// pending changes. Changes are applied atomically on commit using WriteBatch.
struct LevelDbTxn {
    /// Reference to the store's DB
    store: Rc<std::cell::RefCell<Option<DB>>>,

    /// Snapshot of store at transaction start (for reads)
    snapshot: BTreeMap<Vec<u8>, Vec<u8>>,

    /// Pending changes (Some(value) = set, None = delete)
    pending: Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,

    /// Whether this is a read-only transaction
    readonly: bool,

    /// Whether the transaction has been discarded
    discarded: Mutex<bool>,

    /// Whether the transaction has been committed
    committed: Mutex<bool>,

    /// Callbacks for successful commit
    on_success: Mutex<Vec<TxnCallback>>,
    on_success_async: Mutex<Vec<AsyncTxnCallback>>,

    /// Callbacks for failed commit
    on_error: Mutex<Vec<TxnCallback>>,
    on_error_async: Mutex<Vec<AsyncTxnCallback>>,

    /// Callbacks for discard
    on_discard: Mutex<Vec<TxnCallback>>,
    on_discard_async: Mutex<Vec<AsyncTxnCallback>>,
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

    /// Execute sync callbacks with panic protection.
    fn execute_callbacks(callbacks: Vec<TxnCallback>) {
        for (i, callback) in callbacks.into_iter().enumerate() {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback));
            if let Err(panic_info) = result {
                tracing::error!(
                    callback_index = i,
                    panic = ?panic_info,
                    "Transaction callback panicked - continuing with remaining callbacks"
                );
            }
        }
    }

    /// Execute async callbacks with panic protection.
    async fn execute_async_callbacks(callbacks: Vec<AsyncTxnCallback>) {
        use futures::FutureExt;

        for (i, callback) in callbacks.into_iter().enumerate() {
            let future = callback();
            let result = std::panic::AssertUnwindSafe(future).catch_unwind().await;
            if let Err(panic_info) = result {
                tracing::error!(
                    callback_index = i,
                    panic = ?panic_info,
                    "Async callback panicked during execution - continuing with remaining callbacks"
                );
            }
        }
    }
}

#[async_trait(?Send)]
impl Reader for LevelDbTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        Ok(self.get_internal(key))
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        Ok(self.has_internal(key))
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        if *self.discarded.lock() {
            return Err(Error::DiscardedTxn);
        }

        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        Ok(self.get_internal(key).map(|v| v.len()))
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        if *self.discarded.lock() {
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
        if *self.discarded.lock() {
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
        if *self.discarded.lock() {
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
        if *self.discarded.lock() {
            tracing::warn!("Attempted to commit a discarded transaction");
            let on_error = std::mem::take(&mut *self.on_error.lock());
            let on_error_async = std::mem::take(&mut *self.on_error_async.lock());
            Self::execute_callbacks(on_error);
            Self::execute_async_callbacks(on_error_async).await;
            return Err(Error::DiscardedTxn);
        }

        // Check if already committed
        if *self.committed.lock() {
            tracing::warn!("Attempted to commit an already committed transaction");
            return Err(Error::Other("Transaction already committed".into()));
        }

        // Mark as committed
        *self.committed.lock() = true;

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
        let on_success = std::mem::take(&mut *self.on_success.lock());
        let on_success_async = std::mem::take(&mut *self.on_success_async.lock());
        Self::execute_callbacks(on_success);
        Self::execute_async_callbacks(on_success_async).await;

        Ok(())
    }

    fn discard(self: Box<Self>) {
        *self.discarded.lock() = true;

        // Execute sync discard callbacks
        let on_discard = std::mem::take(&mut *self.on_discard.lock());
        Self::execute_callbacks(on_discard);

        // Handle async callbacks
        let on_discard_async = std::mem::take(&mut *self.on_discard_async.lock());
        if !on_discard_async.is_empty() {
            let callback_count = on_discard_async.len();
            tracing::warn!(
                count = callback_count,
                "Transaction has async discard callbacks. Spawning in background."
            );

            wasm_bindgen_futures::spawn_local(async move {
                Self::execute_async_callbacks(on_discard_async).await;
            });
        }
    }

    fn on_success(&mut self, callback: TxnCallback) {
        self.on_success.lock().push(callback);
    }

    fn on_success_async(&mut self, callback: AsyncTxnCallback) {
        self.on_success_async.lock().push(callback);
    }

    fn on_error(&mut self, callback: TxnCallback) {
        self.on_error.lock().push(callback);
    }

    fn on_error_async(&mut self, callback: AsyncTxnCallback) {
        self.on_error_async.lock().push(callback);
    }

    fn on_discard(&mut self, callback: TxnCallback) {
        self.on_discard.lock().push(callback);
    }

    fn on_discard_async(&mut self, callback: AsyncTxnCallback) {
        self.on_discard_async.lock().push(callback);
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
        self.on_success.lock().len()
            + self.on_success_async.lock().len()
            + self.on_error.lock().len()
            + self.on_error_async.lock().len()
            + self.on_discard.lock().len()
            + self.on_discard_async.lock().len()
    }
}

/// Iterator over LevelDB key-value pairs.
struct LevelDbIterator {
    /// Sorted vector of key-value pairs
    data: Vec<(Vec<u8>, Vec<u8>)>,

    /// Current position in the iterator
    position: usize,

    /// Whether the iterator is closed
    closed: bool,

    /// Whether this is a keys-only iterator
    keys_only: bool,

    /// Whether this iterator is in reverse mode
    #[allow(dead_code)]
    reverse: bool,
}

impl LevelDbIterator {
    fn new(data: BTreeMap<Vec<u8>, Vec<u8>>, opts: IterOptions) -> Result<Self> {
        // Apply filters and convert to Vec
        let mut filtered: Vec<_> = data
            .into_iter()
            .filter(|(k, _)| {
                // Apply prefix filter
                if let Some(prefix) = opts.prefix() {
                    if !k.starts_with(prefix) {
                        return false;
                    }
                }

                // Apply start filter
                if let Some(start) = opts.start() {
                    if k.as_slice() < start {
                        return false;
                    }
                }

                // Apply end filter
                if let Some(end) = opts.end() {
                    if k.as_slice() >= end {
                        return false;
                    }
                }

                true
            })
            .collect();

        // Apply reverse ordering
        let reverse = opts.reverse();
        if reverse {
            filtered.reverse();
        }

        Ok(Self {
            data: filtered,
            position: 0,
            closed: false,
            keys_only: opts.keys_only(),
            reverse,
        })
    }
}

#[async_trait(?Send)]
impl Iterator for LevelDbIterator {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        if self.position >= self.data.len() {
            return Ok(None);
        }

        let (key, value) = &self.data[self.position];
        self.position += 1;

        if self.keys_only {
            Ok(Some(KvPair::key_only(key.clone())))
        } else {
            Ok(Some(KvPair::new(key.clone(), value.clone())))
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        let pos = if self.reverse {
            self.data.iter().position(|(k, _)| k.as_slice() <= key)
        } else {
            self.data.iter().position(|(k, _)| k.as_slice() >= key)
        };

        match pos {
            Some(p) => {
                self.position = p;
                Ok(true)
            }
            None => {
                self.position = self.data.len();
                Ok(false)
            }
        }
    }

    async fn reset(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::Iterator("Iterator has been closed".into()));
        }

        self.position = 0;
        Ok(())
    }

    fn is_valid(&self) -> bool {
        !self.closed
    }
}
