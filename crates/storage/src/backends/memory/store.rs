use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::transaction::MemoryTxn;
use crate::backends::shared::{CallbackManager, ConflictTracker};
use crate::corekv::{Dropable, Error, Result, Store, Txn};

/// In-memory key-value store using BTreeMap.
///
/// Data is stored in a BTreeMap wrapped in Arc<RwLock<>> for thread-safe
/// concurrent access. The store provides snapshot isolation for transactions
/// with optimistic write-write conflict detection.
#[derive(Clone)]
pub struct MemoryStore {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
    closed: Arc<AtomicBool>,
    conflict_tracker: Arc<ConflictTracker>,
}

impl MemoryStore {
    /// Create a new empty memory store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            closed: Arc::new(AtomicBool::new(false)),
            conflict_tracker: Arc::new(ConflictTracker::new()),
        }
    }

    /// Check if the store is closed.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::corekv::private::Sealed for MemoryStore {}

#[async_trait]
impl Store for MemoryStore {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        // Record version before taking snapshot for conflict detection
        let read_version = self.conflict_tracker.current_version();

        // Take a snapshot of current data for isolation
        let snapshot = self.data.read().await.clone();

        Ok(Box::new(MemoryTxn {
            store: Arc::clone(&self.data),
            conflict_tracker: Arc::clone(&self.conflict_tracker),
            read_version,
            snapshot,
            pending: Mutex::new(BTreeMap::new()),
            readonly,
            discarded: AtomicBool::new(false),
            committed: AtomicBool::new(false),
            callbacks: CallbackManager::new(),
        }))
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
}

#[async_trait]
impl Dropable for MemoryStore {
    async fn drop_all(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        // Clear all data
        let mut data = self.data.write().await;
        data.clear();
        Ok(())
    }
}
