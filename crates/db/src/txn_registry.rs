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
use storage::corekv::{IterOptions, Key, Store};
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
        ctx.invalidate_migration_cache().await;

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
                db.bump_migration_generation();
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
        self.add_schema_in_txn_with_acp(txn_id, sdl, None, None)
            .await
    }

    /// Add a schema within an existing transaction, optionally registering
    /// branchable collection ACP objects before the storage commit.
    pub async fn add_schema_in_txn_with_acp(
        &self,
        txn_id: &str,
        sdl: &str,
        document_acp: Option<Arc<dyn acp::DocumentACP>>,
        creator: Option<identity::Did>,
    ) -> Result<Vec<schema::CollectionVersion>> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        self.db
            .check_node_access(None, acp::nac::NodePermission::CollectionPatch)
            .await?;
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

        if let (Some(document_acp), Some(creator)) = (document_acp, creator) {
            txn.stage_collection_acp_registration(document_acp, creator, finalized.clone());
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
        self.db
            .check_node_access(None, acp::nac::NodePermission::LensCreate)
            .await?;
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

    /// Verify a block's embedded signature inside an active transaction and
    /// return the signer DID.
    pub async fn verified_block_signer_did_in_txn(
        &self,
        txn_id: &str,
        document_acp: &dyn acp::DocumentACP,
        cid_str: &str,
        caller_identity: &acp::Identity,
    ) -> Result<String> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        let (blockstore, systemstore) = {
            let shared_txn = ctx.fetcher_shared_txn();
            let txn_guard = shared_txn.lock().await;
            let txn = txn_guard.as_ref().ok_or(Error::TxnNotActive)?;
            (txn.blockstore()?, txn.systemstore()?)
        };

        crate::block_verify::verified_block_signer_did_with_blockstore(
            &self.db,
            document_acp,
            blockstore,
            systemstore,
            cid_str,
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

    pub async fn set_collection_active_in_txn(
        &self,
        txn_id: &str,
        version_id: &str,
        is_active: bool,
    ) -> Result<schema::CollectionVersion> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        self.db
            .check_node_access(None, acp::nac::NodePermission::CollectionPatch)
            .await?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        let shared_txn = ctx.fetcher_shared_txn();
        let mut txn_guard = shared_txn.lock().await;
        let txn = txn_guard.as_mut().ok_or(Error::TxnNotActive)?;
        let systemstore = txn.systemstore()?;
        let key = storage::keys::systemstore::CollectionKey::new(version_id);
        let data = systemstore
            .get(&key.bytes())
            .await
            .map_err(Error::Storage)?
            .ok_or_else(|| Error::CollectionVersionNotFound(version_id.to_string()))?;
        let mut target: schema::CollectionVersion =
            serde_json::from_slice(&data).map_err(|error| {
                Error::collection_schema_json(
                    format!("failed to deserialize schema version '{}'", version_id),
                    error,
                )
            })?;
        crate::collection::populate_collection_root_id(&systemstore, &mut target).await?;
        let was_active = target.is_active;

        self.db
            .acquire_collection_write_locks_for_txn(
                txn,
                std::iter::once(target.collection_id.clone()),
            )
            .await?;

        if was_active && !is_active && target.is_materialized {
            let datastore = txn.datastore()?;
            let prefix = if target.query.is_some() {
                storage::keys::datastore::ViewCacheKey::collection_prefix(target.root_id)
            } else {
                format!("/d/{}/", target.collection_id).into_bytes()
            };
            let mut docs = datastore
                .iterator(IterOptions::new().with_prefix(prefix))
                .await
                .map_err(Error::Storage)?;
            let has_data = docs.next().await.map_err(Error::Storage)?.is_some();
            docs.close().await.map_err(Error::Storage)?;
            if has_data {
                return Err(Error::InvalidPatch(
                    "cannot delete a collection that has documents, first delete the documents and then delete the version"
                        .to_string(),
                ));
            }
        }

        target.is_active = is_active;
        systemstore
            .set(
                &key.bytes(),
                &serde_json::to_vec(&target).map_err(|error| {
                    Error::collection_schema_json(
                        format!("failed to serialize schema version '{}'", version_id),
                        error,
                    )
                })?,
            )
            .await
            .map_err(Error::Storage)?;

        let name_key = storage::keys::systemstore::CollectionNameKey::new(&target.name);
        if is_active {
            let prefix = storage::keys::systemstore::CollectionVersionKey::collection_prefix(
                &target.collection_id,
            );
            let mut iter = systemstore
                .iterator(IterOptions::new().with_prefix(prefix))
                .await
                .map_err(Error::Storage)?;
            let pairs = iter.collect_all().await.map_err(Error::Storage)?;
            iter.close().await.map_err(Error::Storage)?;
            for pair in pairs {
                let Some(other_version_id) = String::from_utf8_lossy(&pair.key)
                    .rsplit('/')
                    .next()
                    .map(str::to_string)
                else {
                    continue;
                };
                if other_version_id == version_id {
                    continue;
                }
                let other_key = storage::keys::systemstore::CollectionKey::new(&other_version_id);
                let Some(other_data) = systemstore
                    .get(&other_key.bytes())
                    .await
                    .map_err(Error::Storage)?
                else {
                    continue;
                };
                let Ok(mut other): std::result::Result<schema::CollectionVersion, _> =
                    serde_json::from_slice(&other_data)
                else {
                    continue;
                };
                if other.is_active {
                    other.is_active = false;
                    systemstore
                        .set(
                            &other_key.bytes(),
                            &serde_json::to_vec(&other).map_err(|error| {
                                Error::collection_schema_json(
                                    "failed to serialize inactive schema version",
                                    error,
                                )
                            })?,
                        )
                        .await
                        .map_err(Error::Storage)?;
                }
            }
            systemstore
                .set(&name_key.bytes(), version_id.as_bytes())
                .await
                .map_err(Error::Storage)?;
            txn.cache_collection(Collection::new(target.clone()));
        } else if was_active {
            systemstore
                .delete(&name_key.bytes())
                .await
                .map_err(Error::Storage)?;
            txn.uncache_collection(&target.name);
        }
        drop(systemstore);

        let db = self.db.clone();
        let committed = target.clone();
        txn.on_success(Box::new(move || {
            if let Ok(mut cache) = db.collections.write() {
                if committed.is_active {
                    cache.insert(committed.name.clone(), Collection::new(committed));
                } else if was_active {
                    cache.remove(&committed.name);
                }
            }
        }))?;

        Ok(target)
    }

    pub async fn delete_collections_in_txn(
        &self,
        txn_id: &str,
        targets: Vec<String>,
        active_only: bool,
    ) -> Result<()> {
        if targets.is_empty() {
            return Err(Error::InvalidPatch(
                "collection name required: pass at least one name to delete".into(),
            ));
        }
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        self.db
            .check_node_access(None, acp::nac::NodePermission::CollectionPatch)
            .await?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        let shared_txn = ctx.fetcher_shared_txn();
        let mut txn_guard = shared_txn.lock().await;
        let txn = txn_guard.as_mut().ok_or(Error::TxnNotActive)?;
        let systemstore = txn.systemstore()?;
        let mut iter = systemstore
            .iterator(
                IterOptions::new()
                    .with_prefix(storage::keys::systemstore::CollectionKey::collection_prefix()),
            )
            .await
            .map_err(Error::Storage)?;
        let pairs = iter.collect_all().await.map_err(Error::Storage)?;
        iter.close().await.map_err(Error::Storage)?;

        let mut versions = Vec::new();
        for pair in pairs {
            let mut version: schema::CollectionVersion = serde_json::from_slice(&pair.value)
                .map_err(|error| {
                    Error::collection_schema_json("failed to deserialize collection", error)
                })?;
            crate::collection::populate_collection_root_id(&systemstore, &mut version).await?;
            versions.push(version);
        }

        let mut deleting = std::collections::HashSet::new();
        for target in &targets {
            let selected: Vec<&schema::CollectionVersion> = if let Some(version) = versions
                .iter()
                .find(|version| version.version_id == *target)
            {
                vec![version]
            } else {
                let Some(active) = versions
                    .iter()
                    .find(|version| version.name == *target && version.is_active)
                else {
                    return Err(Error::CollectionNotFound(target.clone()));
                };
                if active_only {
                    vec![active]
                } else {
                    versions
                        .iter()
                        .filter(|version| version.collection_id == active.collection_id)
                        .collect()
                }
            };
            deleting.extend(
                selected
                    .into_iter()
                    .map(|version| version.version_id.clone()),
            );
        }

        for version in &versions {
            if deleting.contains(&version.version_id) {
                continue;
            }
            if version
                .previous_version
                .as_ref()
                .is_some_and(|previous| deleting.contains(&previous.source_collection_id))
            {
                return Err(Error::InvalidPatch(
                    "cannot delete a version that is used by a newer version, first delete the new version"
                        .to_string(),
                ));
            }
        }

        let removed_collection_ids: std::collections::HashSet<String> = versions
            .iter()
            .filter(|version| deleting.contains(&version.version_id))
            .filter(|version| {
                versions.iter().all(|other| {
                    other.collection_id != version.collection_id
                        || deleting.contains(&other.version_id)
                })
            })
            .map(|version| version.collection_id.clone())
            .collect();
        if versions.iter().any(|version| {
            !deleting.contains(&version.version_id)
                && version.fields.iter().any(|field| {
                    field
                        .kind
                        .relation_collection_id()
                        .is_some_and(|id| removed_collection_ids.contains(id))
                })
        }) {
            return Err(Error::InvalidPatch(
                "cannot remove a collection while another field references it".to_string(),
            ));
        }

        self.db
            .acquire_collection_write_locks_for_txn(
                txn,
                versions
                    .iter()
                    .filter(|version| deleting.contains(&version.version_id))
                    .map(|version| version.collection_id.clone()),
            )
            .await?;

        let datastore = txn.datastore()?;
        for version in versions
            .iter()
            .filter(|version| deleting.contains(&version.version_id))
        {
            if version.is_active && version.is_materialized {
                let prefix = if version.query.is_some() {
                    storage::keys::datastore::ViewCacheKey::collection_prefix(version.root_id)
                } else {
                    format!("/d/{}/", version.collection_id).into_bytes()
                };
                let mut docs = datastore
                    .iterator(IterOptions::new().with_prefix(prefix))
                    .await
                    .map_err(Error::Storage)?;
                let has_data = docs.next().await.map_err(Error::Storage)?.is_some();
                docs.close().await.map_err(Error::Storage)?;
                if has_data {
                    return Err(Error::InvalidPatch(
                        "cannot delete a collection that has documents, first delete the documents and then delete the version"
                            .to_string(),
                    ));
                }
            }
        }
        drop(datastore);

        let mut removed_names = std::collections::HashSet::new();
        for version in versions
            .iter()
            .filter(|version| deleting.contains(&version.version_id))
        {
            systemstore
                .delete(
                    &storage::keys::systemstore::CollectionKey::new(&version.version_id).bytes(),
                )
                .await
                .map_err(Error::Storage)?;
            systemstore
                .delete(
                    &storage::keys::systemstore::CollectionVersionKey::new(
                        &version.collection_id,
                        &version.version_id,
                    )
                    .bytes(),
                )
                .await
                .map_err(Error::Storage)?;
            if version.is_active {
                systemstore
                    .delete(
                        &storage::keys::systemstore::CollectionNameKey::new(&version.name).bytes(),
                    )
                    .await
                    .map_err(Error::Storage)?;
                removed_names.insert(version.name.clone());
                txn.uncache_collection(&version.name);
            }
        }
        drop(systemstore);

        let blockstore = txn.blockstore()?;
        for version in versions
            .iter()
            .filter(|version| deleting.contains(&version.version_id))
        {
            if let Ok(cid) = cid::Cid::try_from(version.version_id.as_str()) {
                blockstore
                    .delete(&cid.to_bytes())
                    .await
                    .map_err(Error::Storage)?;
            }
            for field in &version.fields {
                if let Ok(cid) = cid::Cid::try_from(field.id.as_str()) {
                    blockstore
                        .delete(&cid.to_bytes())
                        .await
                        .map_err(Error::Storage)?;
                }
            }
        }
        drop(blockstore);

        let db = self.db.clone();
        txn.on_success(Box::new(move || {
            if let Ok(mut cache) = db.collections.write() {
                for name in removed_names {
                    cache.remove(&name);
                }
            }
            for collection_id in removed_collection_ids {
                let _ = db.forbid_collection_id(&collection_id);
            }
        }))?;

        Ok(())
    }

    /// Apply the txn's recorded counter ops (#1044) then durably commit.
    ///
    /// LOCK LIFECYCLE — conforms to `proofs/tla/InteractiveTxnCounter.tla` GREEN
    /// (`InteractiveGate="AtCommitOnly"`, `GateMode="On"`): the process-wide batch
    /// gate is taken ONLY here (the bounded `IBeginFinalize`/`IFinalizeCommit`
    /// action), never while the interactive txn is active/idle. We briefly hold
    /// the gate while acquiring the touched-counter-doc per-doc guards in SORTED
    /// order (`IAcquire`), then DROP the gate (`IFinalizeCommit` releases the gate
    /// after acquiring the guards) and do the RMW under the held per-doc guards.
    /// The per-doc guards live on the `DbTxn` and release only AFTER the durable
    /// commit (`IExitCrit`), preserving the #1021 invariant that a concurrent
    /// merge never observes a partial RMW.
    ///
    /// GUARD SCOPE — the per-doc guard prevents a concurrent merge from
    /// interleaving its RMW with this finalize's RMW DURING the held window, but
    /// it does NOT serialize against a merge that COMMITTED before this finalize
    /// took the guard. In that case the interactive txn's `begin()` snapshot is
    /// stale and the storage SSI/OCC conflict tracker aborts the commit with a
    /// `TxnConflict` (the client retries the whole txn). No data loss, no
    /// double-apply — this matches Go's storage-txn isolation. So the guard's role
    /// is partial-RMW exclusion, not merge serialization; cross-merge convergence
    /// rests on OCC (`proofs/tla/Ssi.tla`), which is why
    /// `InteractiveTxnCounter.tla` models only the guard lifecycle, not store
    /// values / OCC.
    async fn finalize_and_commit(
        &self,
        handle: &TransactionHandle,
        mut txn: crate::txn::DbTxn<S>,
    ) -> std::result::Result<(), TransactionError> {
        let ops = txn.take_counter_ops();
        if ops.is_empty() {
            return txn.force_commit().await.map_err(|e| {
                TransactionError::execution(format!(
                    "commit error for transaction '{}': {}",
                    handle, e
                ))
            });
        }

        // Distinct touched doc ids, sorted for a deterministic (defensive)
        // acquire order under the gate.
        let mut sorted_doc_ids: Vec<String> = ops.iter().map(|op| op.doc_id.clone()).collect();
        sorted_doc_ids.sort();
        sorted_doc_ids.dedup();

        // IBeginFinalize / IAcquire: take the gate, acquire per-doc guards in
        // sorted order, then IFinalizeCommit drops the gate (the bounded hold).
        {
            let _gate = self.db.doc_write_queue().acquire_batch_gate().await;
            for id in &sorted_doc_ids {
                let guard = self.db.doc_write_queue().acquire(id).await;
                txn.insert_doc_guard(id.clone(), guard);
            }
        } // gate dropped here; per-doc guards remain on the txn until durable commit

        // RMW under the held per-doc guards, into the txn's datastore. The store
        // handles (NamespaceView clones) write through to this txn; they MUST be
        // dropped before `force_commit` or the backend reports "transaction still
        // has references", so they live only inside this block.
        let finalize_result = {
            let datastore = txn.datastore();
            let systemstore = txn.systemstore();
            match (datastore, systemstore) {
                (Ok(ds), Ok(ss)) => Self::apply_counter_ops_at_finalize(&ds, &ss, &ops).await,
                (Err(e), _) | (_, Err(e)) => {
                    Err(query::error::QueryError::execution(e.to_string()))
                }
            }
        };
        if let Err(e) = finalize_result {
            let _ = txn.force_discard();
            return Err(TransactionError::execution(format!(
                "counter finalize error for transaction '{}': {}",
                handle, e
            )));
        }

        // IExitCrit: force_commit commits durably, then release_doc_guards drops
        // the per-doc guards AFTER the durable commit.
        txn.force_commit().await.map_err(|e| {
            TransactionError::execution(format!("commit error for transaction '{}': {}", handle, e))
        })
    }

    /// Perform the recorded counter RMWs into the txn datastore and correct the
    /// materialized blob for update ops (the blob-mirror-at-commit). Called with
    /// the per-doc guards already held by the caller (the finalize driver).
    async fn apply_counter_ops_at_finalize(
        datastore: &datastore::NamespaceView,
        systemstore: &datastore::NamespaceView,
        ops: &[crate::txn::PendingCounterOp],
    ) -> query::error::Result<()> {
        use crate::auto_commit_mutator::helpers::apply_pending_counter_op;
        use crate::collection_loader::load_collection_from_systemstore;
        use std::collections::HashMap;

        // Load each touched collection once (keyed by name) and build its index manager.
        let mut collections: HashMap<String, (Collection, crate::index_manager::IndexManager)> =
            HashMap::new();
        for op in ops {
            if collections.contains_key(&op.collection_name) {
                continue;
            }
            let collection = load_collection_from_systemstore(systemstore, &op.collection_name)
                .await?
                .ok_or_else(|| {
                    query::error::QueryError::collection_not_found(&op.collection_name)
                })?;
            let short_id = collection.resolved_root_id();
            let index_manager = crate::index_manager::IndexManager::from_indexes(
                short_id,
                collection.schema(),
                collection.write_indexes(),
            )
            .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to create index manager for collection '{}': {}",
                    op.collection_name, e
                ))
            })?;
            collections.insert(op.collection_name.clone(), (collection, index_manager));
        }

        // Apply each op's RMW (or create-seed) to the authoritative store,
        // collecting the post-RMW values to mirror into the blob (updates only).
        // DocIDs are content-derived, NOT collection-scoped, so two collections can
        // share a doc_id — key the corrections by (collection_name, doc_id) so each
        // (collection, doc) is corrected against its own collection.
        // (collection_name, doc_id) -> (field -> post-RMW value)
        let mut corrections: HashMap<(String, String), Vec<(String, document::NormalValue)>> =
            HashMap::new();
        for op in ops {
            let (collection, _) = collections
                .get(&op.collection_name)
                .expect("collection loaded above");
            let post = apply_pending_counter_op(
                datastore,
                collection,
                &op.schema_version_id,
                &op.doc_id,
                &op.field,
                op.base.as_ref(),
                &op.delta,
                op.is_create,
            )
            .await?;
            if let Some(value) = post {
                corrections
                    .entry((op.collection_name.clone(), op.doc_id.clone()))
                    .or_default()
                    .push((op.field.clone(), value));
            }
        }

        // Blob correction: for update ops, re-read the doc and set each counter
        // field to its authoritative post-RMW store value, then re-write the blob
        // so the materialized document mirrors the accumulation store.
        for ((collection_name, doc_id), fields) in corrections {
            let (collection, index_manager) = collections
                .get(&collection_name)
                .expect("collection loaded above");
            let doc_id_typed = document::DocID::from_string(&doc_id)
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            let Some(doc_short_id) = collection
                .resolve_doc_short_id(systemstore, &doc_id_typed)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            else {
                continue;
            };
            let Some(mut doc) = collection
                .get_with_datastore(datastore, doc_short_id, &doc_id_typed)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            else {
                continue;
            };
            doc.set_id(doc_id_typed);
            for (field, value) in fields {
                doc.set(field, value);
            }
            collection
                .update_with_indexes(datastore, &doc, doc_short_id, index_manager)
                .await
                .map_err(|e| match e {
                    crate::error::Error::DocumentNotFound(id) => {
                        query::error::QueryError::document_not_found(id)
                    }
                    other => crate::error::index_write_query_error("update", other),
                })?;
        }

        Ok(())
    }

    async fn begin_with_options(
        &self,
        readonly: bool,
        defer_readonly_write_back: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        let txn_id = self.id_counter.fetch_add(1, Ordering::SeqCst).to_string();

        let mut db_txn = self
            .db
            .new_txn(readonly)
            .await
            .map_err(|e| TransactionError::execution(format!("storage error: {}", e)))?;
        // Non-Send on wasm32, where the callbacks it holds drop their `Send`
        // bounds for the single-threaded browser runtime.
        #[cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]
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
        let lens_store = Arc::new(TxnLensStore::new(self.db.lens_store().clone()).map_err(
            |e| {
                TransactionError::execution(format!(
                    "failed to create transaction lens store: {}",
                    e
                ))
            },
        )?);
        let fetcher = Arc::new(LensedDocFetcher::new(
            self.db.clone(),
            db_txn,
            lens_store,
            defer_readonly_write_back,
        ));
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
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> TransactionRegistry for DbTransactionRegistry<S> {
    async fn begin(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.begin_with_options(readonly, false).await
    }

    async fn begin_implicit_read(
        &self,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.begin_with_options(true, true).await
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

        self.finalize_and_commit(handle, txn).await
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

    async fn finish_implicit_read(
        &self,
        handle: &TransactionHandle,
        apply_read_effects: bool,
    ) -> std::result::Result<(), TransactionError> {
        let ctx = self
            .transactions
            .write()
            .map_err(|_| {
                TransactionError::lock_poisoned(format!(
                    "failed to acquire write lock while finalizing implicit read '{}'",
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
                "transaction '{}' was already consumed",
                handle
            ))
        })?;
        txn.force_discard().map_err(|e| {
            TransactionError::execution(format!(
                "failed to close implicit read transaction '{}': {}",
                handle, e
            ))
        })?;

        if apply_read_effects {
            ctx.persist_pending_migrations().await.map_err(|error| {
                TransactionError::execution(format!(
                    "deferred lens write-back failed for transaction '{}': {}",
                    handle, error
                ))
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "txn_registry_tests.rs"]
mod tests;
