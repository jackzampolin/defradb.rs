/// Database transaction wrapper matching Go's internal/db/txn.go.
///
/// DbTxn wraps a BasicTxn and adds:
/// - Explicit/implicit transaction handling
/// - Transaction-scoped collection cache (lazy loading)
/// - Reference to the database for collection operations
use crate::collection::{populate_collection_root_id, Collection};
use crate::collection_cache::CollectionCache;
use crate::error::{Error, Result};
use async_lock::MutexGuardArc;
use datastore::{AsyncCallback, BasicTxn, NamespaceView, RootView, TxnCallback};
use schema::CollectionVersion;
use std::collections::{BTreeMap, HashSet};
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionNameKey};

/// A counter mutation RECORDED during an interactive/explicit transaction but
/// not yet applied to the authoritative CRDT accumulation store. The
/// read-modify-write (and the per-doc guard that protects it) is deferred to the
/// commit-time finalize (#1044) so the interactive txn holds no guard/gate over
/// its user-controlled lifetime. See `InteractiveTxnCounter.tla`.
#[derive(Clone, Debug)]
pub struct PendingCounterOp {
    /// Collection name (used to reload the collection + index manager at finalize).
    pub collection_name: String,
    /// Schema version id of the collection at record time.
    pub schema_version_id: String,
    /// Collection id.
    pub collection_id: String,
    /// Document id the counter belongs to.
    pub doc_id: String,
    /// Counter field name.
    pub field: String,
    /// For updates: the recorded increment delta. For creates: the seeded value.
    pub delta: document::NormalValue,
    /// For UPDATE ops: the PRE-WRITE committed counter value, captured at record
    /// time, used as the reconcile (init-if-absent) base at finalize. This must
    /// be read BEFORE the provisional deferred blob write so the finalize does not
    /// re-read the already-overwritten provisional blob as the "committed" value —
    /// re-reading it would double-apply the delta for an increment-only PCounter
    /// (whose reconcile migrates a present store UPWARD when blob > store). `None`
    /// when the doc had no committed value for the field (seed 0). Unused for
    /// creates.
    pub base: Option<document::NormalValue>,
    /// Whether this op was recorded on a CREATE (seed) vs an UPDATE (delta RMW).
    pub is_create: bool,
}

/// Database transaction wrapper.
///
/// This wraps a BasicTxn and provides:
/// - Explicit/implicit transaction handling
/// - Transaction-scoped collection cache with lazy loading
/// - Access to the underlying store for collection operations
///
/// Explicit transactions are created by the user and must be explicitly
/// committed or discarded. When a method receives an explicit transaction,
/// it should NOT commit or discard it.
///
/// Implicit transactions are created internally by database methods.
/// They are automatically committed on success and discarded on error.
///
/// Transaction liveness is tracked by the `txn` field:
/// - `Some(txn)` = transaction is active
/// - `None` = transaction has been committed or discarded
///
/// The collection cache is populated lazily from the SystemStore on first
/// access, matching the Go DefraDB pattern for transaction isolation.
pub struct DbTxn<S: Store> {
    /// The underlying BasicTxn. `None` after commit/discard.
    txn: Option<BasicTxn>,
    /// Whether this is an explicit transaction.
    explicit: bool,
    /// Transaction-scoped collection cache (lazy loading from SystemStore).
    collection_cache: CollectionCache,
    /// Collection IDs created inside this transaction.
    locally_created_collection_ids: HashSet<String>,
    /// Per-doc write guards held so a local counter read-modify-write and a P2P
    /// merge on the same document never interleave (#1021). The merge handler
    /// shares the same `DocWriteQueue`. For the interactive/explicit path
    /// (#1044) these are acquired at the commit-time finalize (under the briefly
    /// held batch gate, a finalize local) and inserted here, so they release
    /// only when the `DbTxn` is consumed by
    /// `commit`/`discard`/`force_commit`/`force_discard` — i.e. AFTER the durable
    /// commit. See the finalize driver in `txn_registry.rs` and
    /// `InteractiveTxnCounter.tla`.
    doc_guards: BTreeMap<String, MutexGuardArc<()>>,
    /// Counter deltas RECORDED by the interactive mutator during the txn but not
    /// yet applied to the authoritative CRDT accumulation store. Drained and
    /// RMW'd at the commit-time finalize under per-doc guards (#1044).
    pending_counter_ops: Vec<PendingCounterOp>,
    /// Phantom data for the store type.
    _marker: std::marker::PhantomData<S>,
}

impl<S: Store> DbTxn<S> {
    /// Create a new implicit DbTxn.
    pub fn new(txn: BasicTxn) -> Self {
        Self {
            txn: Some(txn),
            explicit: false,
            collection_cache: CollectionCache::new(),
            locally_created_collection_ids: HashSet::new(),
            doc_guards: BTreeMap::new(),
            pending_counter_ops: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Create a new explicit DbTxn.
    pub fn new_explicit(txn: BasicTxn) -> Self {
        Self {
            txn: Some(txn),
            explicit: true,
            collection_cache: CollectionCache::new(),
            locally_created_collection_ids: HashSet::new(),
            doc_guards: BTreeMap::new(),
            pending_counter_ops: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Mark this transaction as explicit.
    ///
    /// Explicit transactions are not automatically committed/discarded
    /// when passed to database methods.
    pub fn make_explicit(&mut self) {
        self.explicit = true;
    }

    /// Check if this is an explicit transaction.
    pub fn is_explicit(&self) -> bool {
        self.explicit
    }

    /// Get the transaction ID.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn id(&self) -> Result<u64> {
        self.txn.as_ref().map(|t| t.id()).ok_or(Error::TxnNotActive)
    }

    /// Check if this is a read-only transaction.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn is_readonly(&self) -> Result<bool> {
        self.txn
            .as_ref()
            .map(|t| t.is_readonly())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the blockstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn blockstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.blockstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the datastore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn datastore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.datastore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the encstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn encstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.encstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the headstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn headstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.headstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the peerstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn peerstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.peerstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the systemstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn systemstore(&self) -> Result<NamespaceView> {
        self.txn
            .as_ref()
            .map(|t| t.systemstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Get the rootstore.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn rootstore(&self) -> Result<RootView> {
        self.txn
            .as_ref()
            .map(|t| t.rootstore())
            .ok_or(Error::TxnNotActive)
    }

    /// Register a callback for successful commit.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn on_success(&mut self, callback: TxnCallback) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_success(callback);
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    /// Register a callback for commit error.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn on_error(&mut self, callback: TxnCallback) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_error(callback);
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    /// Register an async callback for successful commit.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn on_success_async(&mut self, callback: AsyncCallback) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_success_async(callback);
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    /// Register a callback for discard.
    ///
    /// Returns an error if the transaction has been committed or discarded.
    pub fn on_discard(&mut self, callback: TxnCallback) -> Result<()> {
        if let Some(txn) = &mut self.txn {
            txn.on_discard(callback);
            Ok(())
        } else {
            Err(Error::TxnNotActive)
        }
    }

    // =========================================================================
    // Collection Cache Methods (Transaction-scoped caching)
    // =========================================================================

    /// Get a collection by name, loading from SystemStore if not in cache.
    ///
    /// This implements lazy loading - collections are loaded on first access.
    /// Returns `None` if the collection doesn't exist in the store.
    ///
    /// Note: This method is structured to avoid holding `&mut self` across awaits,
    /// which allows futures using this method to be `Send`.
    ///
    /// Storage layout:
    /// - `/collection/name/{name}` - Maps name to version_id (string)
    /// - `/collection/id/{version_id}` - Full collection JSON
    pub async fn get_collection(&mut self, name: &str) -> Result<Option<&Collection>> {
        // Check cache first
        if self.collection_cache.contains(name) {
            return Ok(self.collection_cache.get(name));
        }

        // Cache miss: extract systemstore synchronously, then do async operation
        let systemstore = self.systemstore()?;
        let name_key = CollectionNameKey::new(name);

        // Step 1: Get version_id from name mapping
        let maybe_version_id = systemstore
            .get(&name_key.bytes())
            .await
            .map_err(Error::Storage)?;

        let version_id = match maybe_version_id {
            Some(data) => {
                // Try to parse as version_id string first (new format)
                match String::from_utf8(data.clone()) {
                    Ok(vid) if !vid.starts_with('{') => vid, // Not JSON, it's a version_id
                    _ => {
                        // Fallback: Old format where full JSON is stored at name key
                        // This handles backward compatibility during migration
                        let mut schema: CollectionVersion =
                            serde_json::from_slice(&data).map_err(|e| {
                                tracing::error!(
                                    error = ?e,
                                    collection_name = %name,
                                    "Failed to deserialize schema for collection"
                                );
                                Error::collection_schema_json(
                                    format!(
                                        "failed to deserialize schema for collection '{}'",
                                        name
                                    ),
                                    e,
                                )
                            })?;
                        populate_collection_root_id(&systemstore, &mut schema).await?;
                        self.collection_cache.add(Collection::new(schema));
                        return Ok(self.collection_cache.get(name));
                    }
                }
            }
            None => return Ok(None),
        };

        // Step 2: Get full collection from /collection/id/{version_id}
        let collection_key = CollectionKey::new(&version_id);
        let maybe_data = systemstore
            .get(&collection_key.bytes())
            .await
            .map_err(Error::Storage)?;

        // Process result and update cache
        if let Some(data) = maybe_data {
            let mut schema: CollectionVersion = serde_json::from_slice(&data).map_err(|e| {
                tracing::error!(
                    error = ?e,
                    collection_name = %name,
                    version_id = %version_id,
                    "Failed to deserialize schema for collection"
                );
                Error::collection_schema_json(
                    format!("failed to deserialize schema for collection '{}'", name),
                    e,
                )
            })?;
            populate_collection_root_id(&systemstore, &mut schema).await?;
            self.collection_cache.add(Collection::new(schema));
            return Ok(self.collection_cache.get(name));
        }

        // version_id found but no collection data - inconsistent state
        tracing::warn!(
            collection_name = %name,
            version_id = %version_id,
            "Collection version_id found but definition missing"
        );
        Ok(None)
    }

    /// Load all collections from SystemStore into the cache.
    ///
    /// This is called when listing collections or when we need to iterate
    /// over all collections. After calling this, `is_fully_populated()` returns true.
    ///
    /// Storage layout:
    /// - `/collection/name/{name}` - Maps name to version_id (string)
    /// - `/collection/id/{version_id}` - Full collection JSON
    pub async fn load_all_collections(&mut self) -> Result<()> {
        if self.collection_cache.is_fully_populated() {
            return Ok(());
        }

        let prefix = CollectionNameKey::name_prefix();
        let mut collections = Vec::new();

        let systemstore = self.systemstore()?;
        let opts = IterOptions::new().with_prefix(prefix.clone());

        let mut iter = systemstore.iterator(opts).await.map_err(|e| {
            tracing::error!(error = ?e, "Failed to create iterator during collection load");
            Error::Storage(e)
        })?;

        // Collect version_ids first (to avoid holding iterator across lookups)
        let mut name_version_pairs: Vec<(String, String)> = Vec::new();

        while let Some(pair) = iter.next().await.map_err(|e| {
            tracing::error!(error = ?e, "Failed to iterate collections during load");
            Error::Storage(e)
        })? {
            // Validate UTF-8 in key
            let key_str = String::from_utf8(pair.key.to_vec()).map_err(|e| {
                tracing::error!(
                    error = ?e,
                    key_bytes = ?&pair.key[..pair.key.len().min(50)],
                    "Collection key contains invalid UTF-8"
                );
                Error::text_decode("collection key contains invalid UTF-8", e)
            })?;

            let prefix_str = String::from_utf8(prefix.clone()).map_err(|e| {
                Error::Other(format!("internal error: prefix is not valid UTF-8: {}", e))
            })?;

            let name = key_str
                .strip_prefix(&prefix_str)
                .ok_or_else(|| {
                    Error::Other(format!(
                        "collection key '{}' does not match expected prefix '{}'",
                        key_str, prefix_str
                    ))
                })?
                .to_string();

            // Check if value is a version_id string or full JSON (backward compat)
            let value_str = String::from_utf8(pair.value.clone()).ok();
            if let Some(ref s) = value_str {
                if !s.starts_with('{') {
                    // New format: value is version_id
                    name_version_pairs.push((name, s.clone()));
                    continue;
                }
            }

            // Old format: value is full JSON
            let schema: CollectionVersion = serde_json::from_slice(&pair.value).map_err(|e| {
                tracing::error!(
                    error = ?e,
                    collection_name = %name,
                    "Failed to deserialize schema for collection '{}': {}",
                    name,
                    e
                );
                Error::collection_schema_json(
                    format!("failed to deserialize schema for collection '{}'", name),
                    e,
                )
            })?;
            let mut schema = schema;
            populate_collection_root_id(&systemstore, &mut schema).await?;

            collections.push(Collection::new(schema));
        }

        iter.close().await.map_err(|e| {
            tracing::error!(error = ?e, "Failed to close iterator during collection load");
            Error::Storage(e)
        })?;

        // Load collections from version_ids (new format)
        for (name, version_id) in name_version_pairs {
            let collection_key = CollectionKey::new(&version_id);
            match systemstore.get(&collection_key.bytes()).await {
                Ok(Some(data)) => {
                    let schema: CollectionVersion = serde_json::from_slice(&data).map_err(|e| {
                        tracing::error!(
                            error = ?e,
                            collection_name = %name,
                            version_id = %version_id,
                            "Failed to deserialize schema"
                        );
                        Error::collection_schema_json(
                            format!("failed to deserialize schema for collection '{}'", name),
                            e,
                        )
                    })?;
                    let mut schema = schema;
                    populate_collection_root_id(&systemstore, &mut schema).await?;
                    collections.push(Collection::new(schema));
                }
                Ok(None) => {
                    tracing::warn!(
                        collection_name = %name,
                        version_id = %version_id,
                        "Collection version_id found but definition missing"
                    );
                }
                Err(e) => {
                    return Err(Error::Storage(e));
                }
            }
        }

        self.collection_cache.populate(collections);
        Ok(())
    }

    /// Add a collection to the transaction-scoped cache.
    ///
    /// The key is derived from the collection's name to prevent key-name mismatches.
    pub fn cache_collection(&mut self, collection: Collection) {
        self.collection_cache.add(collection);
    }

    /// Remove a collection from the transaction-scoped cache.
    ///
    /// Called by delete_collection to update the cache after writing to store.
    pub fn uncache_collection(&mut self, name: &str) {
        self.collection_cache.remove(name);
    }

    /// Get the collection cache.
    ///
    /// Use this for read-only access to iterate over cached collections.
    pub fn collection_cache(&self) -> &CollectionCache {
        &self.collection_cache
    }

    /// Get mutable access to the collection cache.
    ///
    /// Use this for advanced cache manipulation (e.g., populate from snapshot).
    pub fn collection_cache_mut(&mut self) -> &mut CollectionCache {
        &mut self.collection_cache
    }

    /// Mark a collection as created inside this transaction.
    pub fn mark_collection_created(&mut self, collection_id: impl Into<String>) {
        self.locally_created_collection_ids
            .insert(collection_id.into());
    }

    /// Check whether a collection was created inside this transaction.
    pub fn was_collection_created(&self, collection_id: &str) -> bool {
        self.locally_created_collection_ids.contains(collection_id)
    }

    // =========================================================================
    // Per-doc write guards (#1021)
    // =========================================================================

    /// Store a per-doc write guard on this txn, held until the txn is consumed
    /// by commit/discard. Idempotent per doc: a duplicate guard is dropped.
    pub fn insert_doc_guard(&mut self, doc_id: String, guard: MutexGuardArc<()>) {
        self.doc_guards.entry(doc_id).or_insert(guard);
    }

    // =========================================================================
    // Pending counter ops (#1044): recorded during the txn, RMW'd at finalize.
    // =========================================================================

    /// Record a counter op to be applied to the authoritative store at the
    /// commit-time finalize (under a per-doc guard).
    pub fn record_counter_op(&mut self, op: PendingCounterOp) {
        self.pending_counter_ops.push(op);
    }

    /// Drain the recorded counter ops (consumed by the finalize driver).
    pub fn take_counter_ops(&mut self) -> Vec<PendingCounterOp> {
        std::mem::take(&mut self.pending_counter_ops)
    }

    /// Whether any counter ops were recorded on this txn.
    pub fn has_pending_counter_ops(&self) -> bool {
        !self.pending_counter_ops.is_empty()
    }

    // =========================================================================
    // Transaction Lifecycle Methods
    // =========================================================================

    /// Commit the transaction.
    ///
    /// Returns an error for explicit transactions - use `force_commit()` instead.
    /// Returns an error if the transaction is not active.
    pub async fn commit(mut self) -> Result<()> {
        if self.explicit {
            return Err(Error::ExplicitTxnMustUseForce);
        }
        if !self.pending_counter_ops.is_empty() {
            return Err(Error::UnfinalizedCounterOps);
        }

        match self.txn.take() {
            Some(txn) => {
                txn.commit().await.map_err(Error::Datastore)?;
                // Release per-doc guards only AFTER the durable commit so a
                // concurrent merge observes the committed counter state, never a
                // partial RMW (#1021).
                self.release_doc_guards();
                Ok(())
            }
            None => Err(Error::TxnNotActive),
        }
    }

    /// Discard the transaction.
    ///
    /// Returns an error for explicit transactions - use `force_discard()` instead.
    /// Returns an error if the transaction is not active.
    pub fn discard(mut self) -> Result<()> {
        if self.explicit {
            return Err(Error::ExplicitTxnMustUseForce);
        }

        match self.txn.take() {
            Some(txn) => {
                txn.discard().map_err(Error::Datastore)?;
                self.release_doc_guards();
                Ok(())
            }
            None => Err(Error::TxnNotActive),
        }
    }

    /// Actually commit the transaction, even if explicit.
    ///
    /// This should only be called by the transaction creator.
    pub async fn force_commit(mut self) -> Result<()> {
        if !self.pending_counter_ops.is_empty() {
            return Err(Error::UnfinalizedCounterOps);
        }
        match self.txn.take() {
            Some(txn) => {
                txn.commit().await.map_err(Error::Datastore)?;
                // Release per-doc guards only AFTER the durable commit so a
                // concurrent merge observes the committed counter state (#1021).
                self.release_doc_guards();
                Ok(())
            }
            None => Err(Error::TxnNotActive),
        }
    }

    /// Actually discard the transaction, even if explicit.
    ///
    /// This should only be called by the transaction creator.
    pub fn force_discard(mut self) -> Result<()> {
        match self.txn.take() {
            Some(txn) => {
                txn.discard().map_err(Error::Datastore)?;
                self.release_doc_guards();
                Ok(())
            }
            None => Err(Error::TxnNotActive),
        }
    }

    /// Release the per-doc guards. Called only AFTER the txn is durably
    /// committed or discarded (#1021). The guards also drop automatically when
    /// the `DbTxn` itself is dropped (no explicit `Drop` impl is needed), but
    /// releasing here makes the release point unambiguous.
    fn release_doc_guards(&mut self) {
        self.doc_guards.clear();
    }
}
