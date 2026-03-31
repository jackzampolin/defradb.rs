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
use query::error::TransactionError;
use query::txn::{
    DeferredAcpMutations, GetTransactionResult, TransactionContext, TransactionHandle,
    TransactionRegistry,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use storage::corekv::Store;
use tracing::{error, warn};

use crate::collection::Collection;
use crate::database::DB;
use crate::error::{Error, Result};
use crate::lensed_fetcher::LensedDocFetcher;
use crate::txn_context::DbTransactionContext;

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
pub struct DbTransactionRegistry<S: Store> {
    db: Arc<DB<S>>,
    transactions: RwLock<HashMap<String, Arc<DbTransactionContext<S>>>>,
    id_counter: AtomicU64,
}

impl<S: Store + 'static> DbTransactionRegistry<S> {
    /// Create a new transaction registry.
    ///
    /// Collections are sourced from the DB's collection cache.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self {
            db,
            transactions: RwLock::new(HashMap::new()),
            id_counter: AtomicU64::new(0),
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
            Ok(guard) => Ok(guard.get(txn_id).cloned()),
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

    /// Get all documents from a collection within a transaction.
    pub async fn get_all_docs(&self, txn_id: &str, collection_name: &str) -> Result<Vec<Document>> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;

        ctx.doc_fetcher()
            .get_all(collection_name)
            .await
            .map_err(Error::Query)
    }

    /// Get documents by IDs from a collection within a transaction.
    ///
    /// Note: This convenience method returns only the found documents.
    /// For information about missing IDs, use the DocFetcher's get_by_ids directly.
    pub async fn get_docs_by_ids(
        &self,
        txn_id: &str,
        collection_name: &str,
        doc_ids: &[String],
    ) -> Result<Vec<Document>> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;

        ctx.doc_fetcher()
            .get_by_ids(collection_name, doc_ids)
            .await
            .map(|result| result.into_docs())
            .map_err(Error::Query)
    }

    /// Cleanup transactions older than the given duration.
    ///
    /// This method finds all transactions that were created more than `max_age` ago
    /// and rolls them back, freeing resources. This should be called periodically
    /// by a background task to prevent resource leaks from dropped `TransactionGuard`s.
    ///
    /// Returns a `CleanupResult` containing both successfully cleaned transactions
    /// and any failures. Check `result.is_complete()` to verify all cleanups succeeded.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned, indicating a panic elsewhere.
    pub async fn cleanup_stale_transactions(&self, max_age: Duration) -> Result<CleanupResult> {
        let now = std::time::Instant::now();

        // Collect stale transaction IDs while holding the read lock briefly
        let stale_ids: Vec<String> = {
            let guard = self.transactions.read().map_err(|_| {
                Error::LockPoisoned("failed to acquire read lock during cleanup".to_string())
            })?;

            guard
                .iter()
                .filter(|(_, ctx)| now.duration_since(ctx.created_at()) > max_age)
                .map(|(id, _)| id.clone())
                .collect()
        };

        let mut result = CleanupResult::default();
        for txn_id in stale_ids {
            // Remove and rollback each stale transaction
            let ctx = {
                let mut guard = self.transactions.write().map_err(|_| {
                    Error::LockPoisoned("failed to acquire write lock during cleanup".to_string())
                })?;
                guard.remove(&txn_id)
            };

            if let Some(ctx) = ctx {
                warn!(
                    txn_id = %txn_id,
                    age_secs = ?now.duration_since(ctx.created_at()).as_secs(),
                    "Cleaning up stale transaction (leaked TransactionGuard?)"
                );

                // Try to take and discard the transaction
                if let Some(txn) = ctx.take_txn().await {
                    if let Err(e) = txn.force_discard() {
                        error!(
                            txn_id = %txn_id,
                            error = %e,
                            "Failed to discard stale transaction during cleanup"
                        );
                        result.failed.push((txn_id.clone(), e.to_string()));
                    } else {
                        result.cleaned += 1;
                    }
                } else {
                    // Transaction was already consumed (committed/rolled back)
                    result.cleaned += 1;
                }
            }
        }

        Ok(result)
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

    /// Set a migration within an existing transaction.
    ///
    /// This registers a lens migration configuration within the specified transaction.
    /// The migration will only be visible after the transaction is committed.
    ///
    /// # Arguments
    ///
    /// * `txn_id` - The transaction ID from `begin_txn`
    /// * `config` - The lens configuration
    ///
    /// # Returns
    ///
    /// The transform ID that was registered.
    pub async fn set_migration_in_txn(
        &self,
        txn_id: &str,
        config: lens::LensConfig,
    ) -> Result<lens::TransformId> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;

        // Get the shared transaction from the fetcher
        let shared_txn = ctx.fetcher_shared_txn();
        let txn_guard = shared_txn.lock().await;
        let txn = txn_guard.as_ref().ok_or(Error::TxnNotActive)?;

        // Call the database's transaction-aware set_migration
        self.db.set_migration_in_txn(txn, config).await
    }

    /// Add a schema within an existing transaction.
    ///
    /// Parses the SDL and creates collections within the transaction.
    /// The collections are only visible after the transaction is committed,
    /// but can be used by queries within the same transaction.
    pub async fn add_schema_in_txn(
        &self,
        txn_id: &str,
        sdl: &str,
    ) -> Result<Vec<schema::CollectionVersion>> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;

        let shared_txn = ctx.fetcher_shared_txn();
        let mut txn_guard = shared_txn.lock().await;
        let txn = txn_guard.as_mut().ok_or(Error::TxnNotActive)?;

        let known_types: std::collections::HashSet<String> = self
            .db
            .list_collections()
            .map_err(|e| Error::Other(format!("failed to list collections: {}", e)))?
            .into_iter()
            .collect();

        let collections = query::parse_sdl_with_known_types(sdl, known_types)
            .map_err(|e| Error::Other(format!("failed to parse SDL: {}", e)))?;

        crate::definition_validation::validate_new_collections(&collections)
            .map_err(|e| Error::Other(format!("failed to validate schema: {}", e)))?;

        let mut finalized = Vec::new();
        for collection in collections {
            let schema = self.db.create_collection_with_txn(txn, collection).await?;
            finalized.push(schema);
        }

        // Register on_success callback to update the process-wide cache after commit
        let db = self.db.clone();
        let schemas_for_cache = finalized.clone();
        txn.on_success(Box::new(move || {
            if let Ok(mut cache) = db.collections.write() {
                for schema in &schemas_for_cache {
                    cache.insert(schema.name.clone(), Collection::new(schema.clone()));
                }
            }
        }))?;

        Ok(finalized)
    }

    /// Get all collection versions visible within a transaction.
    ///
    /// This reads from the transaction's systemstore, which includes both
    /// committed data and any uncommitted writes made within this transaction
    /// (e.g., placeholders from `set_migration_in_txn`).
    pub async fn get_collections_in_txn(
        &self,
        txn_id: &str,
    ) -> Result<Vec<schema::CollectionVersion>> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;

        let shared_txn = ctx.fetcher_shared_txn();
        let txn_guard = shared_txn.lock().await;
        let txn = txn_guard.as_ref().ok_or(Error::TxnNotActive)?;

        let systemstore = txn.systemstore()?;
        let prefix = storage::keys::systemstore::CollectionKey::collection_prefix();
        let opts = storage::corekv::IterOptions::new().with_prefix(prefix);
        let mut iter = systemstore.iterator(opts).await.map_err(Error::Storage)?;

        let mut versions = Vec::new();
        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            match serde_json::from_slice::<schema::CollectionVersion>(&pair.value) {
                Ok(col) => versions.push(col),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to deserialize collection version during txn scan"
                    );
                }
            }
        }
        iter.close().await.map_err(Error::Storage)?;

        Ok(versions)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> TransactionRegistry for DbTransactionRegistry<S> {
    async fn begin(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        let txn_id = self.id_counter.fetch_add(1, Ordering::SeqCst).to_string();

        let mut db_txn = self
            .db
            .new_txn(readonly)
            .await
            .map_err(|e| TransactionError::execution(format!("storage error: {}", e)))?;
        let deferred_acp_mutations = Arc::new(DeferredAcpMutations::new());
        let deferred_acp_mutations_for_commit = deferred_acp_mutations.clone();
        db_txn
            .on_success_async(Box::new(move || {
                Box::pin(async move {
                    deferred_acp_mutations_for_commit.run_all_logged().await;
                })
            }))
            .map_err(|e| {
                TransactionError::execution(format!(
                    "failed to register deferred ACP commit hook: {}",
                    e
                ))
            })?;

        // Transaction-scoped collection caching: collections are loaded lazily
        // from the SystemStore on first access within the transaction. Once loaded,
        // the collection metadata is cached for the transaction's duration.
        // Use LensedDocFetcher to support lens migrations within transactions.
        let lens_store = self.db.lens_store().clone();
        let fetcher = Arc::new(LensedDocFetcher::new(db_txn, lens_store));
        let ctx = Arc::new(DbTransactionContext::new(
            self.db.clone(),
            txn_id.clone(),
            readonly,
            fetcher,
            deferred_acp_mutations,
        ));

        self.transactions
            .write()
            .map_err(|_| TransactionError::lock_poisoned("failed to acquire write lock for begin"))?
            .insert(txn_id.clone(), ctx);

        Ok(TransactionHandle::new(txn_id))
    }

    fn get(&self, handle: &TransactionHandle) -> GetTransactionResult {
        match self.transactions.read() {
            Ok(guard) => match guard.get(handle.as_str()).cloned() {
                Some(ctx) => GetTransactionResult::Found(ctx as Arc<dyn TransactionContext>),
                None => GetTransactionResult::NotFound,
            },
            Err(poisoned) => {
                error!(
                    txn_id = %handle,
                    error = ?poisoned,
                    "Transaction registry lock poisoned - system may be in corrupted state"
                );
                GetTransactionResult::LockPoisoned
            }
        }
    }

    async fn commit(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        let ctx = self
            .transactions
            .write()
            .map_err(|_| {
                TransactionError::lock_poisoned(format!(
                    "failed to acquire write lock during commit of '{}'",
                    handle
                ))
            })?
            .remove(handle.as_str())
            .ok_or_else(|| {
                TransactionError::not_found(format!("transaction '{}' not found", handle))
            })?;

        let txn = ctx.take_txn().await.ok_or_else(|| {
            TransactionError::already_finalized(format!(
                "transaction '{}' was already consumed (double commit/rollback?)",
                handle
            ))
        })?;

        txn.force_commit().await.map_err(|e| {
            TransactionError::execution(format!("commit error for transaction '{}': {}", handle, e))
        })
    }

    async fn rollback(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        let ctx = self
            .transactions
            .write()
            .map_err(|_| {
                TransactionError::lock_poisoned(format!(
                    "failed to acquire write lock during rollback of '{}'",
                    handle
                ))
            })?
            .remove(handle.as_str())
            .ok_or_else(|| {
                TransactionError::not_found(format!("transaction '{}' not found", handle))
            })?;

        let txn = ctx.take_txn().await.ok_or_else(|| {
            TransactionError::already_finalized(format!(
                "transaction '{}' was already consumed (double commit/rollback?)",
                handle
            ))
        })?;

        txn.force_discard().map_err(|e| {
            TransactionError::execution(format!(
                "rollback error for transaction '{}': {}",
                handle, e
            ))
        })
    }
}

#[cfg(test)]
#[path = "txn_registry_tests.rs"]
mod tests;
