//! Transaction registry for query execution.
//!
//! This module provides the `DbTransactionRegistry` which implements the query crate's
//! `TransactionRegistry` trait, enabling transaction-aware query execution.
//!
//! # Architecture
//!
//! ```text
//! query crate                          db crate
//! ───────────                          ────────
//! TransactionRegistry (trait)    <--   DbTransactionRegistry (impl)
//! TransactionContext (trait)     <--   DbTransactionContext (impl)
//! DocFetcher (trait)             <--   DbDocFetcher (impl)
//! ```

use async_trait::async_trait;
use document::Document;
use lens::{LensConfig, LensModule, TransformId};
use query::error::TransactionError;
use query::txn::{
    DeferredAcpMutations, GetTransactionResult, TransactionContext, TransactionHandle,
    TransactionRegistry,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use storage::corekv::{IterOptions, Key, Store};
use tracing::{error, warn};
use web_time::Instant;

use crate::collection::Collection;
use crate::database::DB;
use crate::error::{Error, Result};
use crate::read::lensed::fetcher::LensedDocFetcher;
use crate::txn::context::DbTransactionContext;
use crate::txn::lenses::TxnLensStore;

mod blocks;
mod cleanup;
mod collections;
mod docs;
mod lenses;
mod lifecycle;
mod schemas;

/// Default max idle age for explicit HTTP transactions.
pub const DEFAULT_TRANSACTION_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// Default interval between explicit HTTP transaction cleanup sweeps.
pub const DEFAULT_TRANSACTION_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// Result of a stale transaction cleanup operation.
///
/// Provides visibility into both successful cleanups and failures,
/// allowing callers to monitor for resource leaks.
#[derive(Debug, Clone, Default)]
pub struct CleanupResult {
    /// Number of transactions successfully cleaned up.
    pub cleaned: usize,
    /// Transactions that failed to clean up: (transaction_id, error_message).
    pub failed: Vec<(String, String)>,
}

impl CleanupResult {
    /// Returns true if all cleanup operations succeeded.
    pub fn is_complete(&self) -> bool {
        self.failed.is_empty()
    }

    /// Total number of transactions that were attempted to be cleaned.
    pub fn attempted(&self) -> usize {
        self.cleaned + self.failed.len()
    }
}

/// Transaction registry that manages database transactions for query execution.
///
/// Implements `query::TransactionRegistry` to provide transaction lifecycle
/// management to the query executor.
///
/// # Thread Safety
///
/// Uses `std::sync::RwLock` for the transaction map to allow synchronous
/// lookups (required by the trait), and `tokio::sync::Mutex` for the
/// underlying transaction to support async document fetching operations.
///
/// # Error Handling
///
/// If the internal lock becomes poisoned (due to a panic in another thread),
/// all operations will fail-fast: `get()` returns `LockPoisoned`, `get_ctx()`
/// returns an error, and `begin()`, `commit()`, and `rollback()` return errors.
/// A poisoned lock indicates a panic and potential data corruption - continuing
/// operation would be unsafe.
/// Abandonment still removes entries from a poisoned map, without clearing its
/// poison or attempting to commit, so cancellation can release resources.
pub struct DbTransactionRegistry<S: Store> {
    db: Arc<DB<S>>,
    transactions: RwLock<HashMap<String, Arc<DbTransactionContext<S>>>>,
    id_counter: AtomicU64,
    broadcaster: Option<Arc<dyn crate::event::emission::TxnBroadcaster>>,
}

impl<S: Store + 'static> DbTransactionRegistry<S> {
    /// Create a new transaction registry without a P2P broadcaster.
    ///
    /// Use `with_broadcaster` when running with the P2P stack so that
    /// committed transactional writes are forwarded to peers.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self {
            db,
            transactions: RwLock::new(HashMap::new()),
            id_counter: AtomicU64::new(0),
            broadcaster: None,
        }
    }

    /// Create a transaction registry that forwards committed writes to a
    /// `TxnBroadcaster`. Each contained transaction's success callbacks will
    /// call `broadcaster.broadcast_update` in addition to publishing to the
    /// local event bus.
    pub fn with_broadcaster(
        db: Arc<DB<S>>,
        broadcaster: Arc<dyn crate::event::emission::TxnBroadcaster>,
    ) -> Self {
        Self {
            db,
            transactions: RwLock::new(HashMap::new()),
            id_counter: AtomicU64::new(0),
            broadcaster: Some(broadcaster),
        }
    }

    /// Get the database instance.
    pub fn db(&self) -> &Arc<DB<S>> {
        &self.db
    }

    /// Get all collection names from the DB.
    ///
    /// Uses the process-wide cache. For transaction-scoped access,
    /// use the transaction's collection cache directly.
    pub fn collection_names(&self) -> Result<Vec<String>> {
        self.db.list_collections()
    }

    /// Get a collection by name from the DB.
    ///
    /// Uses the process-wide cache. For transaction-scoped access,
    /// use the transaction's collection cache directly.
    pub fn collection(&self, name: &str) -> Result<Option<Collection>> {
        self.db.get_collection(name)
    }

    /// Get an existing transaction by ID (for internal use).
    ///
    /// Returns `Ok(None)` if the transaction doesn't exist.
    /// Returns `Err(LockPoisoned)` if the lock is poisoned (indicates a panic elsewhere).
    pub fn get_ctx(&self, txn_id: &str) -> Result<Option<Arc<DbTransactionContext<S>>>> {
        match self.transactions.read() {
            Ok(guard) => {
                let ctx = guard.get(txn_id).cloned();
                if let Some(ctx) = &ctx {
                    ctx.touch();
                }
                Ok(ctx)
            }
            Err(poisoned) => {
                error!(
                    txn_id = %txn_id,
                    error = ?poisoned,
                    "Transaction registry lock poisoned - system may be in corrupted state"
                );
                Err(Error::LockPoisoned(format!(
                    "failed to acquire read lock for transaction '{}': a panic occurred elsewhere",
                    txn_id
                )))
            }
        }
    }

    /// Get the number of active transactions in the registry.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned.
    pub fn active_transaction_count(&self) -> Result<usize> {
        self.transactions
            .read()
            .map(|guard| guard.len())
            .map_err(|_| Error::LockPoisoned("failed to acquire read lock for count".to_string()))
    }
}
