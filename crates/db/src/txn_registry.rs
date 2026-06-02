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
use std::time::{Duration, Instant};
use storage::corekv::Store;
use tracing::{error, warn};

use crate::collection::Collection;
use crate::database::DB;
use crate::error::{Error, Result};
use crate::lensed_fetcher::LensedDocFetcher;
use crate::txn_context::DbTransactionContext;
use crate::txn_lens_store::TxnLensStore;

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
pub struct DbTransactionRegistry<S: Store> {
    db: Arc<DB<S>>,
    transactions: RwLock<HashMap<String, Arc<DbTransactionContext<S>>>>,
    id_counter: AtomicU64,
    broadcaster: Option<Arc<dyn crate::event_emission::TxnBroadcaster>>,
}

impl<S: Store + 'static> DbTransactionRegistry<S> {
    /// Create a new transaction registry without a P2P broadcaster.
    ///
    /// Use [`with_broadcaster`] when running with the P2P stack so that
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
        broadcaster: Arc<dyn crate::event_emission::TxnBroadcaster>,
    ) -> Self {
        Self {
            db,
            transactions: RwLock::new(HashMap::new()),
            id_counter: AtomicU64::new(0),
            broadcaster: Some(broadcaster),
        }
    }

    async fn compute_lens_transform_id(config: &LensConfig) -> Result<TransformId> {
        let first_lens = config
            .lens()
            .ok_or_else(|| Error::Lens("lens config has no modules".into()))?;

        let wasm_bytes = if let Some(ref bytes) = first_lens.module {
            bytes.clone()
        } else if let Some(ref path) = first_lens.path {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let clean_path = path.strip_prefix("file://").unwrap_or(path);
                tokio::fs::read(clean_path)
                    .await
                    .map_err(|e| Error::Lens(format!("failed to read WASM from {}: {}", path, e)))?
            }
            #[cfg(target_arch = "wasm32")]
            {
                return Err(Error::Lens(format!(
                    "path-based lens modules not supported on wasm32 (path: {}); pass bytes instead",
                    path
                )));
            }
        } else {
            return Err(Error::Lens("lens module has neither path nor bytes".into()));
        };

        let arguments: Vec<(String, String)> = first_lens
            .arguments
            .as_ref()
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let (config_cid, _) =
            defra_core::build_lens_ipld_blocks(&wasm_bytes, first_lens.inverse, &arguments)
                .map_err(|e| Error::Lens(format!("failed to build lens IPLD blocks: {}", e)))?;

        Ok(TransformId::new(config_cid.to_string()))
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

    /// Get all documents from a collection within a transaction.
    pub async fn get_all_docs(&self, txn_id: &str, collection_name: &str) -> Result<Vec<Document>> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

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
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        ctx.doc_fetcher()
            .get_by_ids(collection_name, doc_ids)
            .await
            .map(|result| result.into_docs())
            .map_err(Error::Query)
    }

    /// Cleanup transactions idle for longer than the given duration.
    ///
    /// This method finds all transactions whose last observed request was more
    /// than `max_idle_age` ago and rolls them back, freeing resources. This
    /// should be called periodically by a background task to prevent resource
    /// leaks from dropped `TransactionGuard`s or orphaned HTTP transaction
    /// handles.
    ///
    /// Returns a `CleanupResult` containing both successfully cleaned transactions
    /// and any failures. Check `result.is_complete()` to verify all cleanups succeeded.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned, indicating a panic elsewhere.
    pub async fn cleanup_stale_transactions(
        &self,
        max_idle_age: Duration,
    ) -> Result<CleanupResult> {
        let now = Instant::now();

        // Collect stale transaction candidates while holding the read lock briefly.
        // Each candidate is re-checked after acquiring the transaction action lock
        // so a request that arrives during cleanup can refresh the idle clock.
        let stale_candidates: Vec<(String, Arc<DbTransactionContext<S>>)> = {
            let guard = self.transactions.read().map_err(|_| {
                Error::LockPoisoned("failed to acquire read lock during cleanup".to_string())
            })?;

            guard
                .iter()
                .filter(|(_, ctx)| ctx.idle_for(now) > max_idle_age)
                .map(|(id, ctx)| (id.clone(), ctx.clone()))
                .collect()
        };

        let mut result = CleanupResult::default();
        for (txn_id, candidate_ctx) in stale_candidates {
            let action_lock = candidate_ctx.action_lock();
            let _action_guard = action_lock.lock().await;

            let now = Instant::now();
            if candidate_ctx.idle_for(now) <= max_idle_age {
                continue;
            }

            // Remove and rollback each stale transaction. Re-check while holding
            // the write lock so a concurrent request that touched the context
            // after candidate collection cannot lose its transaction.
            let ctx = {
                let mut guard = self.transactions.write().map_err(|_| {
                    Error::LockPoisoned("failed to acquire write lock during cleanup".to_string())
                })?;

                // Holding the registry write lock blocks new get()/get_ctx()
                // touches while we do the final idle re-check and remove.
                // The idle timestamp itself is protected by the context mutex,
                // so a request cannot refresh it between this check and removal.
                match guard.get(&txn_id) {
                    Some(current)
                        if Arc::ptr_eq(current, &candidate_ctx)
                            && current.idle_for(Instant::now()) > max_idle_age =>
                    {
                        guard.remove(&txn_id)
                    }
                    _ => None,
                }
            };

            if let Some(ctx) = ctx {
                let idle_secs = ctx.idle_for(Instant::now()).as_secs();
                warn!(
                    txn_id = %txn_id,
                    idle_secs,
                    "Cleaning up idle transaction (orphaned HTTP handle or leaked TransactionGuard?)"
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

    /// Start a periodic task that cleans up transactions idle for longer than
    /// `max_idle_age`.
    ///
    /// # Panics
    ///
    /// Panics if `sweep_interval` is zero.
    #[cfg(all(not(target_arch = "wasm32"), feature = "native"))]
    pub fn start_stale_transaction_cleanup(
        self: &Arc<Self>,
        max_idle_age: Duration,
        sweep_interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        assert!(
            !sweep_interval.is_zero(),
            "transaction cleanup sweep interval must be non-zero"
        );

        let registry = Arc::clone(self);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(sweep_interval).await;

                match registry.cleanup_stale_transactions(max_idle_age).await {
                    Ok(result) if result.attempted() > 0 && result.is_complete() => {
                        tracing::info!(
                            cleaned = result.cleaned,
                            max_idle_age_secs = max_idle_age.as_secs(),
                            "Cleaned up idle transactions"
                        );
                    }
                    Ok(result) if result.attempted() > 0 => {
                        tracing::warn!(
                            cleaned = result.cleaned,
                            failed = result.failed.len(),
                            max_idle_age_secs = max_idle_age.as_secs(),
                            "Idle transaction cleanup completed with failures"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            max_idle_age_secs = max_idle_age.as_secs(),
                            "Idle transaction cleanup failed"
                        );
                    }
                }
            }
        })
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
        identity: Option<&identity::Did>,
    ) -> Result<lens::TransformId> {
        self.db
            .check_node_access(identity, acp::nac::NodePermission::MigrationSet)
            .await?;
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        // Get the shared transaction from the fetcher
        let shared_txn = ctx.fetcher_shared_txn();
        let mut txn_guard = shared_txn.lock().await;
        let txn = txn_guard.as_mut().ok_or(Error::TxnNotActive)?;
        let txn_lens_store = ctx.lens_store();

        let outcome = self
            .db
            .set_migration_in_txn_with_store(txn, txn_lens_store.clone(), config.clone())
            .await?;
        let transform_id = outcome.transform_id.clone();
        let updated_destination = outcome.updated_destination.clone();

        let db = self.db.clone();
        let transform_id_for_commit = transform_id.clone();
        let destination_version_id = updated_destination.version_id.clone();
        txn.on_success_async(Box::new(move || {
            let db = db.clone();
            let config = config.clone();
            let transform_id = transform_id_for_commit.clone();
            let updated_destination = updated_destination.clone();
            let destination_version_id = destination_version_id.clone();
            Box::pin(async move {
                if let Err(error) = db
                    .lens_store()
                    .add_with_id(transform_id.clone(), config)
                    .await
                {
                    tracing::warn!(
                        transform_id = %transform_id,
                        error = %error,
                        "failed to promote committed transaction migration lens"
                    );
                }

                if !updated_destination.name.is_empty() {
                    if let Ok(mut cache) = db.collections.write() {
                        if let Some(cached) = cache.get(&updated_destination.name) {
                            if cached.schema().version_id == destination_version_id {
                                cache.insert(
                                    updated_destination.name.clone(),
                                    Collection::new(updated_destination.clone()),
                                );
                            }
                        }
                    }

                    if let Err(error) = db
                        .maybe_reindex_after_migration(
                            &updated_destination.name,
                            &updated_destination.version_id,
                        )
                        .await
                    {
                        tracing::warn!(
                            collection = %updated_destination.name,
                            version_id = %updated_destination.version_id,
                            error = %error,
                            "failed to reindex committed transaction migration"
                        );
                    }
                }
            })
        }))?;

        Ok(transform_id)
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
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

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

        schema::definition_validation::validate_new_collections(&collections)
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
            for schema in &schemas_for_cache {
                let _ = db.unforbid_collection_id(&schema.collection_id);
            }
            if let Ok(mut cache) = db.collections.write() {
                for schema in &schemas_for_cache {
                    cache.insert(schema.name.clone(), Collection::new(schema.clone()));
                }
            }
        }))?;

        Ok(finalized)
    }

    /// Add a standalone lens within a transaction.
    ///
    /// The lens is visible only within this transaction until commit. On commit it is
    /// persisted through the regular DB lens path so restart behavior stays consistent.
    pub async fn add_lens_in_txn(&self, txn_id: &str, config: LensConfig) -> Result<TransformId> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        let shared_txn = ctx.fetcher_shared_txn();
        let mut txn_guard = shared_txn.lock().await;
        let txn = txn_guard.as_mut().ok_or(Error::TxnNotActive)?;
        let txn_lens_store = ctx.lens_store();

        let transform_id = Self::compute_lens_transform_id(&config).await?;
        if txn_lens_store.has_transform(&transform_id) {
            return Ok(transform_id);
        }

        let db = self.db.clone();
        let config_for_commit = config.clone();
        let transform_id_for_log = transform_id.to_string();
        txn.on_success_async(Box::new(move || {
            let db = db.clone();
            Box::pin(async move {
                if let Err(error) = db.add_lens(config_for_commit).await {
                    tracing::warn!(
                        transform_id = %transform_id_for_log,
                        error = %error,
                        "failed to persist committed transaction lens"
                    );
                }
            })
        }))?;

        txn_lens_store
            .add_with_id(transform_id.clone(), config)
            .await
            .map_err(Error::from)?;

        Ok(transform_id)
    }

    /// Verify a block signature using the existing transaction's blockstore view.
    pub async fn verify_block_signature_in_txn(
        &self,
        txn_id: &str,
        document_acp: &dyn acp::DocumentACP,
        cid_str: &str,
        public_key_hex: &str,
        key_type: crypto::KeyType,
        caller_identity: &acp::Identity,
    ) -> Result<()> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        let shared_txn = ctx.fetcher_shared_txn();
        let txn_guard = shared_txn.lock().await;
        let txn = txn_guard.as_ref().ok_or(Error::TxnNotActive)?;

        crate::block_verify::verify_block_signature_in_txn(
            &self.db,
            document_acp,
            txn,
            cid_str,
            public_key_hex,
            key_type,
            caller_identity,
        )
        .await
        .map_err(Error::Other)
    }

    /// List all lenses visible within a transaction.
    pub async fn list_lenses_in_txn(
        &self,
        txn_id: &str,
    ) -> Result<std::collections::HashMap<String, LensModule>> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        ctx.lens_store()
            .list()
            .await
            .map_err(|e| Error::Lens(e.to_string()))
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
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

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
                Ok(mut col) => {
                    crate::collection::populate_collection_root_id(&systemstore, &mut col).await?;
                    if self.db.is_collection_forbidden(&col.collection_id)?
                        && !txn.was_collection_created(&col.collection_id)
                    {
                        continue;
                    }
                    versions.push(col);
                }
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
        let lens_store = Arc::new(TxnLensStore::new(self.db.lens_store().clone()).map_err(
            |e| {
                TransactionError::execution(format!(
                    "failed to create transaction lens store: {}",
                    e
                ))
            },
        )?);
        let fetcher = Arc::new(LensedDocFetcher::new(db_txn, lens_store));
        let ctx = Arc::new(DbTransactionContext::new_with_broadcaster(
            self.db.clone(),
            txn_id.clone(),
            readonly,
            fetcher,
            deferred_acp_mutations,
            self.broadcaster.clone(),
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
                Some(ctx) => {
                    ctx.touch();
                    GetTransactionResult::Found(ctx as Arc<dyn TransactionContext>)
                }
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
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

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
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

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
