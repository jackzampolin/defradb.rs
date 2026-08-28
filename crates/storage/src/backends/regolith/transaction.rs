//! A regolith-backed transaction.
//!
//! There is no lock here and no pending-write buffer of our own.
//! regolith's transaction is shareable, buffers its own writes, tracks
//! its own read set, and validates at commit, so this type is the corekv
//! surface over it plus the callback bookkeeping DefraDB expects.
//!
//! A read-only transaction does not begin one at all: it pins a snapshot,
//! which is the same view without the commit-time validation nothing will
//! ever ask for.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use regolith::{IsolationLevel, OptimisticTransactionDb, OwnedTransaction, TransactionError};

use super::handle::Handle;
use super::iterator::RegolithIterator;
use crate::backends::shared::{CallbackManager, TransactionStatsHandle};
use crate::corekv::{
    AsyncTxnCallback, Error, IterOptions, Iterator, Reader, Result, Txn, TxnCallback, Writer,
};

/// Transaction over a regolith store.
pub struct RegolithTxn {
    handle: Option<Arc<Handle>>,
    active_txns: Arc<AtomicUsize>,
    stats: TransactionStatsHandle,
    callbacks: CallbackManager,
    readonly: bool,
}

impl RegolithTxn {
    pub(crate) fn new(
        db: &Arc<OptimisticTransactionDb>,
        readonly: bool,
        isolation: IsolationLevel,
        active_txns: Arc<AtomicUsize>,
        stats: TransactionStatsHandle,
    ) -> Self {
        let handle = if readonly {
            Handle::ReadOnly(db.db().snapshot())
        } else {
            Handle::Writable(db.begin_transaction_owned(isolation))
        };
        Self {
            handle: Some(Arc::new(handle)),
            active_txns,
            stats,
            callbacks: CallbackManager::default(),
            readonly,
        }
    }

    fn handle(&self) -> Result<&Arc<Handle>> {
        self.handle.as_ref().ok_or(Error::DiscardedTxn)
    }

    fn writable(&self) -> Result<&OwnedTransaction> {
        match self.handle()?.as_ref() {
            Handle::Writable(txn) => Ok(txn),
            Handle::ReadOnly(_) => Err(Error::ReadOnlyTxn),
        }
    }

    /// Release this transaction's slot exactly once, so `close` is not
    /// left waiting on something already finished.
    fn release(&mut self) {
        if self.handle.take().is_some() {
            self.active_txns.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl Drop for RegolithTxn {
    fn drop(&mut self) {
        // A transaction dropped without `commit` or `discard` still holds
        // a slot. regolith rolls its own state back on drop; this only
        // has to stop `close` blocking forever.
        self.release();
    }
}

fn map_txn_error(error: TransactionError) -> Error {
    match error {
        TransactionError::Conflict { .. } => Error::TxnConflict,
        TransactionError::Busy(_) => Error::TxnConflict,
        other => Error::Backend(other.to_string()),
    }
}

impl crate::corekv::private::Sealed for RegolithTxn {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Reader for RegolithTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        // `get_slice` borrows the bytes the engine already holds and
        // `Bytes::from_owner` adopts that borrow, so a read copies
        // nothing on the way to the caller.
        let slice = match self.handle()?.as_ref() {
            Handle::ReadOnly(snapshot) => snapshot
                .get_slice(key)
                .map_err(|error| Error::Backend(error.to_string()))?,
            Handle::Writable(txn) => txn.get_slice(key).map_err(map_txn_error)?,
        };
        Ok(slice.map(Bytes::from_owner))
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        match self.handle()?.as_ref() {
            Handle::ReadOnly(snapshot) => snapshot
                .has(key)
                .map_err(|error| Error::Backend(error.to_string())),
            // A writing transaction has to consult its own buffer, which
            // `has` on the snapshot underneath would miss.
            Handle::Writable(txn) => Ok(txn.get_slice(key).map_err(map_txn_error)?.is_some()),
        }
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        match self.handle()?.as_ref() {
            Handle::ReadOnly(snapshot) => snapshot
                .get_size(key)
                .map_err(|error| Error::Backend(error.to_string())),
            Handle::Writable(txn) => {
                Ok(txn.get_slice(key).map_err(map_txn_error)?.map(|v| v.len()))
            }
        }
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        // The iterator holds the same handle, so it keeps the snapshot or
        // the transaction alive without borrowing this one. Scanning a
        // transaction that is still being written to is the expensive
        // shape, because the merged stream is rebuilt per page: open a
        // read-only transaction where the scan does not need to see
        // uncommitted writes.
        let handle = Arc::clone(self.handle()?);
        Ok(Box::new(RegolithIterator::open(handle, &opts)?))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Writer for RegolithTxn {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        self.writable()?.put(key, value).map_err(map_txn_error)
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        self.writable()?.delete(key).map_err(map_txn_error)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Txn for RegolithTxn {
    async fn commit(mut self: Box<Self>) -> Result<()> {
        let handle = self.handle.take().ok_or(Error::DiscardedTxn)?;
        self.active_txns.fetch_sub(1, Ordering::AcqRel);
        // regolith's `commit` consumes the transaction, so this has to be
        // its only owner. An iterator opened from it holds the same `Arc`,
        // so a caller that commits without closing one gets told, rather
        // than a commit that silently does not happen.
        let handle = Arc::try_unwrap(handle).map_err(|_| {
            Error::Other(
                "cannot commit while an iterator opened from this transaction is still open"
                    .to_string(),
            )
        })?;
        let outcome = match handle {
            // Nothing was written, so there is nothing to validate and
            // nothing to apply.
            Handle::ReadOnly(_) => Ok(()),
            Handle::Writable(txn) => txn.commit().map_err(map_txn_error),
        };
        match outcome {
            Ok(()) => {
                self.stats.record_commit();
                self.callbacks.run_success().await;
                Ok(())
            }
            Err(error) => {
                if matches!(error, Error::TxnConflict) {
                    self.stats.record_conflict();
                }
                self.callbacks.run_error().await;
                Err(error)
            }
        }
    }

    fn discard(mut self: Box<Self>) {
        // Dropping regolith's transaction rolls it back.
        self.release();
        self.callbacks.run_discard();
    }

    fn on_success(&mut self, callback: TxnCallback) {
        self.callbacks.on_success(callback);
    }

    fn on_success_async(&mut self, callback: AsyncTxnCallback) {
        self.callbacks.on_success_async(callback);
    }

    fn on_error(&mut self, callback: TxnCallback) {
        self.callbacks.on_error(callback);
    }

    fn on_error_async(&mut self, callback: AsyncTxnCallback) {
        self.callbacks.on_error_async(callback);
    }

    fn on_discard(&mut self, callback: TxnCallback) {
        self.callbacks.on_discard(callback);
    }

    fn on_discard_async(&mut self, callback: AsyncTxnCallback) {
        self.callbacks.on_discard_async(callback);
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
        self.callbacks.total()
    }
}
