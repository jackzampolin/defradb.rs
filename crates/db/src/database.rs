/// Database struct for DefraDB matching Go's internal/db/db.go.
///
/// The DB struct is the main entry point for DefraDB operations.
/// It manages the root store, creates transactions, and provides
/// access to collections.
use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::txn::DbTxn;
use crate::NacManagerApi;
use cid::Cid;
use datastore::BasicTxn;
// EmbeddingClientConfig extracted to standalone db-search crate (Phase 6 of #796).
pub use db_search::EmbeddingClientConfig;
use events::Bus;
use identity::{Identity, RawIdentity};
#[cfg(not(feature = "native"))]
use lens::MemoryTransformStore;
use lens::TransformStore;
#[cfg(feature = "native")]
use lens::WasmTransformStore;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use storage::corekv::Store;

/// Database options.
#[derive(Clone, Default)]
pub struct DbOptions {
    /// Maximum number of transaction retries.
    pub max_txn_retries: Option<u32>,
    /// Chunk size for large values in the blockstore.
    pub chunk_size: Option<usize>,
    /// Node identity for this database instance.
    ///
    /// The node identity is used for:
    /// - Signing documents and blocks
    /// - Authenticating with the ACP (Access Control Policy) system
    /// - Identifying this node in P2P interactions
    pub node_identity: Option<Arc<RawIdentity>>,
    /// Fallback OpenAI-compatible embedding base URL.
    pub embedding_url: String,
    /// Fallback embedding model name.
    pub embedding_model: String,
    /// Resolved embedding API key value.
    pub embedding_api_key: String,
}

impl std::fmt::Debug for DbOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbOptions")
            .field("max_txn_retries", &self.max_txn_retries)
            .field("chunk_size", &self.chunk_size)
            .field(
                "node_identity",
                &self.node_identity.as_ref().map(|id| {
                    id.did()
                        .map(|d| d.to_string())
                        .unwrap_or_else(|_| "<invalid>".to_string())
                }),
            )
            .field("embedding_url", &self.embedding_url)
            .field("embedding_model", &self.embedding_model)
            .field(
                "embedding_api_key_configured",
                &!self.embedding_api_key.is_empty(),
            )
            .finish()
    }
}

impl DbOptions {
    /// Creates new database options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the node identity for this database.
    pub fn with_node_identity(mut self, identity: RawIdentity) -> Self {
        self.node_identity = Some(Arc::new(identity));
        self
    }

    /// Sets the node identity from an Arc for this database.
    pub fn with_node_identity_arc(mut self, identity: Arc<RawIdentity>) -> Self {
        self.node_identity = Some(identity);
        self
    }

    /// Sets the maximum number of transaction retries.
    pub fn with_max_txn_retries(mut self, retries: u32) -> Self {
        self.max_txn_retries = Some(retries);
        self
    }

    /// Sets the chunk size for large values.
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = Some(size);
        self
    }

    /// Sets the fallback embedding base URL.
    pub fn with_embedding_url(mut self, url: impl Into<String>) -> Self {
        self.embedding_url = url.into();
        self
    }

    /// Sets the fallback embedding model name.
    pub fn with_embedding_model(mut self, model: impl Into<String>) -> Self {
        self.embedding_model = model.into();
        self
    }

    /// Sets the resolved embedding API key value.
    pub fn with_embedding_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.embedding_api_key = api_key.into();
        self
    }

    /// Returns the embedding client configuration for this database.
    pub fn embedding_config(&self) -> EmbeddingClientConfig {
        EmbeddingClientConfig::new()
            .with_url(self.embedding_url.clone())
            .with_model(self.embedding_model.clone())
            .with_api_key(self.embedding_api_key.clone())
    }
}

/// The main DefraDB database struct.
///
/// This matches Go's DB struct in internal/db/db.go.
pub struct DB<S: Store> {
    /// The underlying store.
    store: Arc<S>,
    /// Options for this database instance.
    options: DbOptions,
    /// Counter for generating unique transaction IDs.
    txn_id_counter: AtomicU64,
    /// Whether the database has been closed.
    closed: AtomicBool,
    /// In-memory collection cache (name -> Collection).
    pub(crate) collections: RwLock<HashMap<String, Collection>>,
    /// Event bus for subscription notifications.
    event_bus: Option<Arc<dyn Bus>>,
    /// Lens transform store for schema migrations.
    pub(crate) lens_store: Arc<dyn TransformStore>,
    /// Pending migrations registered before their destination version exists.
    /// Maps dest_version_id -> (source_version_id, transform_id_string).
    pub(crate) pending_migrations: RwLock<HashMap<String, (String, String)>>,
    /// Schema definition headstore: tracks latest CID and height per collection.
    /// Emulates Go's persistent headstore for CID computation during patching.
    /// Key: collection name, Value: (sorted heads as CIDs, max height)
    pub(crate) schema_heads: RwLock<HashMap<String, (Vec<Cid>, u64)>>,
    /// Collection IDs whose last local version has been deleted.
    ///
    /// Go's collection repository forbids these immediately, including for
    /// transactions that started before the deletion committed.
    pub(crate) forbidden_collection_ids: RwLock<HashSet<String>>,
    /// Optional KMS service. When set, the document write path generates
    /// encrypted-field DEKs through the KMS (which persists them in its
    /// KeyStore for cross-peer serving) instead of inline-and-blockstore.
    /// Set once at node startup via [`DB::set_kms`].
    kms: std::sync::OnceLock<std::sync::Arc<dyn kms::KmsService>>,
    /// Owning handle to the KMS durable blockstore. The KMS adapter
    /// (`DbEncBlockStore`) references this weakly to avoid the
    /// DB→KMS→`KeyStore`→adapter→blockstore→store Arc cycle that would pin the
    /// storage lock past node close (#976). Parking the owning `Arc` here means
    /// it shares the DB's lifetime — and its in-process block cache — and drops
    /// with the DB, releasing the lock. Set once at startup.
    kms_blockstore: std::sync::OnceLock<Arc<blockstore::DefraBlockstore<S>>>,
    /// Optional NAC manager. When set and enabled, node-level operations are
    /// gated through [`DB::check_node_access`]. Set once at node startup via
    /// [`DB::set_nac_manager`]. When unset, all `check_node_access` calls are
    /// no-ops (NAC not configured).
    nac_manager: std::sync::OnceLock<std::sync::Arc<dyn NacManagerApi>>,
}

impl<S: Store> DB<S> {
    /// Create a new database with the given store.
    ///
    /// This creates a DB with an empty collection cache. Use `open()` to
    /// load existing collections from the store.
    pub fn new(store: S) -> Result<Self> {
        Self::with_options(store, DbOptions::default())
    }

    /// Create a new database with the given store and options.
    ///
    /// This creates a DB with an empty collection cache. Use `open_with_options()`
    /// to load existing collections from the store.
    pub fn with_options(store: S, options: DbOptions) -> Result<Self> {
        let lens_store: Arc<dyn TransformStore> = Self::create_lens_store()?;
        Ok(Self {
            store: Arc::new(store),
            options,
            txn_id_counter: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            collections: RwLock::new(HashMap::new()),
            event_bus: None,
            lens_store,
            pending_migrations: RwLock::new(HashMap::new()),
            schema_heads: RwLock::new(HashMap::new()),
            forbidden_collection_ids: RwLock::new(HashSet::new()),
            kms: std::sync::OnceLock::new(),
            kms_blockstore: std::sync::OnceLock::new(),
            nac_manager: std::sync::OnceLock::new(),
        })
    }

    /// Open a database and load existing collections from the store.
    pub async fn open(store: S) -> Result<Self> {
        Self::open_with_options(store, DbOptions::default()).await
    }

    /// Open a database with options and load existing collections from the store.
    pub async fn open_with_options(store: S, options: DbOptions) -> Result<Self> {
        let db = Self::with_options(store, options)?;
        db.load_collections().await?;
        db.maybe_backfill_commit_priority_index().await?;
        db.reload_lens_configs().await?;
        Ok(db)
    }

    /// Create a new database from an Arc-wrapped store.
    ///
    /// Use this when you already have an `Arc<S>` and want to share
    /// the store between the database and other components (e.g., blockstore).
    ///
    /// Note: This creates a DB with an empty collection cache. Use `open_from_arc()`
    /// to load existing collections from the store.
    ///
    /// **Warning:** When multiple DB instances share a store via `from_arc()`,
    /// transaction IDs may collide if both instances create transactions concurrently.
    /// This is acceptable for read-heavy workloads but may cause issues with
    /// concurrent writes from multiple DB instances.
    pub fn from_arc(store: Arc<S>) -> Result<Self> {
        Self::from_arc_with_options(store, DbOptions::default())
    }

    /// Create a new database from an Arc-wrapped store with options.
    ///
    /// **Warning:** When multiple DB instances share a store via `from_arc()`,
    /// transaction IDs may collide if both instances create transactions concurrently.
    pub fn from_arc_with_options(store: Arc<S>, options: DbOptions) -> Result<Self> {
        let lens_store: Arc<dyn TransformStore> = Self::create_lens_store()?;
        Ok(Self {
            store,
            options,
            txn_id_counter: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            collections: RwLock::new(HashMap::new()),
            event_bus: None,
            lens_store,
            pending_migrations: RwLock::new(HashMap::new()),
            schema_heads: RwLock::new(HashMap::new()),
            forbidden_collection_ids: RwLock::new(HashSet::new()),
            kms: std::sync::OnceLock::new(),
            kms_blockstore: std::sync::OnceLock::new(),
            nac_manager: std::sync::OnceLock::new(),
        })
    }

    /// Open a database from an Arc-wrapped store and load existing collections.
    ///
    /// Use this when you already have an `Arc<S>` and want to share
    /// the store between the database and other components (e.g., blockstore),
    /// while also loading existing collections from the store.
    pub async fn open_from_arc(store: Arc<S>) -> Result<Self> {
        Self::open_from_arc_with_options(store, DbOptions::default()).await
    }

    /// Open a database from an Arc-wrapped store with options and load existing collections.
    pub async fn open_from_arc_with_options(store: Arc<S>, options: DbOptions) -> Result<Self> {
        let db = Self::from_arc_with_options(store, options)?;
        db.load_collections().await?;
        db.maybe_backfill_commit_priority_index().await?;
        db.reload_lens_configs().await?;
        Ok(db)
    }

    /// Set the event bus for subscription notifications.
    ///
    /// When an event bus is set, document mutations (create, update, delete)
    /// will emit update events that can be received by subscribers.
    pub fn set_event_bus(&mut self, bus: Arc<dyn Bus>) {
        self.event_bus = Some(bus);
    }

    /// Get a reference to the event bus, if configured.
    pub fn event_bus(&self) -> Option<&Arc<dyn Bus>> {
        self.event_bus.as_ref()
    }

    /// Install the KMS service. First call wins (OnceLock); subsequent calls
    /// are silently discarded. Called once at node startup.
    pub fn set_kms(&self, kms: std::sync::Arc<dyn kms::KmsService>) {
        let _ = self.kms.set(kms);
    }

    /// Get the KMS service, if one has been installed.
    pub fn kms(&self) -> Option<std::sync::Arc<dyn kms::KmsService>> {
        self.kms.get().cloned()
    }

    /// Install the NAC manager. First call wins (OnceLock); subsequent calls
    /// are silently discarded. Called once at node startup. When unset, all
    /// `check_node_access` calls are no-ops (NAC not configured).
    pub fn set_nac_manager(&self, nac: std::sync::Arc<dyn NacManagerApi>) {
        let _ = self.nac_manager.set(nac);
    }

    /// Get the NAC manager, if one has been installed.
    pub fn nac_manager(&self) -> Option<std::sync::Arc<dyn NacManagerApi>> {
        self.nac_manager.get().cloned()
    }

    /// Park the KMS durable blockstore on the DB so its owning `Arc` shares the
    /// DB's lifetime (and block cache) without forming a lock-pinning cycle
    /// (#976). First call wins. Returns the stored handle (the just-set one, or
    /// the existing one if already set) so the caller can build a `Weak` from
    /// the canonical instance.
    pub fn set_kms_blockstore(
        &self,
        blockstore: Arc<blockstore::DefraBlockstore<S>>,
    ) -> Arc<blockstore::DefraBlockstore<S>> {
        let _ = self.kms_blockstore.set(blockstore);
        self.kms_blockstore
            .get()
            .expect("kms_blockstore set above")
            .clone()
    }

    /// Get the next transaction ID.
    fn next_txn_id(&self) -> u64 {
        self.txn_id_counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Create a new transaction.
    ///
    /// If `readonly` is true, the transaction cannot perform writes.
    /// Returns `Error::DatabaseClosed` if the database has been closed.
    pub async fn new_txn(&self, readonly: bool) -> Result<DbTxn<S>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::DatabaseClosed);
        }
        let id = self.next_txn_id();
        let basic_txn = BasicTxn::new(&*self.store, id, readonly)
            .await
            .map_err(Error::Storage)?;
        Ok(DbTxn::new(basic_txn))
    }

    /// Execute a function within a transaction.
    ///
    /// If the function returns Ok, the transaction is committed.
    /// If the function returns Err, the transaction is discarded.
    pub async fn with_txn<F, T>(&self, readonly: bool, f: F) -> Result<T>
    where
        F: FnOnce(&DbTxn<S>) -> Result<T>,
    {
        let txn = self.new_txn(readonly).await?;
        let result = f(&txn);
        match result {
            Ok(value) => {
                txn.commit().await?;
                Ok(value)
            }
            Err(e) => {
                // Discard and log if it fails - return original error
                if let Err(discard_err) = txn.discard() {
                    tracing::warn!(
                        error = %discard_err,
                        original_error = %e,
                        "Transaction discard failed after operation error"
                    );
                }
                Err(e)
            }
        }
    }

    /// Execute an async function within a transaction.
    ///
    /// If the function returns Ok, the transaction is committed.
    /// If the function returns Err, the transaction is discarded.
    pub async fn with_txn_async<F, Fut, T>(&self, readonly: bool, f: F) -> Result<T>
    where
        F: FnOnce(DbTxn<S>) -> Fut,
        Fut: std::future::Future<Output = (DbTxn<S>, Result<T>)>,
    {
        let txn = self.new_txn(readonly).await?;
        let (txn, result) = f(txn).await;
        match result {
            Ok(value) => {
                txn.commit().await?;
                Ok(value)
            }
            Err(e) => {
                // Discard and log if it fails - return original error
                if let Err(discard_err) = txn.discard() {
                    tracing::warn!(
                        error = %discard_err,
                        original_error = %e,
                        "Transaction discard failed after async operation error"
                    );
                }
                Err(e)
            }
        }
    }

    /// Close the database.
    ///
    /// After closing, any attempt to create new transactions will return
    /// `Error::DatabaseClosed`. Matches Go's close-guard behavior (PR #4435).
    pub async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        self.store.close().await.map_err(Error::Storage)
    }

    /// Returns true if the database has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Get a reference to the underlying store.
    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    /// Get the database options.
    pub fn options(&self) -> &DbOptions {
        &self.options
    }

    /// Returns the node identity, if one was configured.
    ///
    /// The node identity is used for:
    /// - Signing documents and blocks
    /// - Authenticating with the ACP (Access Control Policy) system
    /// - Identifying this node in P2P interactions
    pub fn node_identity(&self) -> Option<Arc<RawIdentity>> {
        self.options.node_identity.clone()
    }

    /// Returns true if this database has a configured node identity.
    pub fn has_node_identity(&self) -> bool {
        self.options.node_identity.is_some()
    }

    /// Returns the node identity's DID, if configured and derivable.
    ///
    /// Used by ACP checks to apply the node-identity full-access shortcut.
    /// Returns `None` if the node identity is not configured or its public
    /// key cannot be converted into a DID.
    pub fn node_did(&self) -> Option<identity::Did> {
        self.options
            .node_identity
            .as_ref()
            .and_then(|id| id.did().ok())
    }

    /// Create the appropriate lens transform store for the current platform.
    #[cfg(feature = "native")]
    fn create_lens_store() -> Result<Arc<dyn TransformStore>> {
        let store = WasmTransformStore::with_sandbox(Some(lens::WasmSandboxConfig::restrictive()))
            .map_err(|e| Error::Lens(format!("failed to create lens transform store: {}", e)))?;
        Ok(Arc::new(store))
    }

    /// Create the appropriate lens transform store for the current platform.
    #[cfg(not(feature = "native"))]
    fn create_lens_store() -> Result<Arc<dyn TransformStore>> {
        Ok(Arc::new(MemoryTransformStore::new()))
    }

    /// Get the current transaction ID counter value.
    pub fn current_txn_id(&self) -> u64 {
        self.txn_id_counter.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use storage::backends::MemoryStore;

    struct StubKms;

    #[async_trait]
    impl kms::KmsService for StubKms {
        async fn get_keys(
            &self,
            _: &kms::RequestContext,
            _: &[kms::EncryptionCid],
        ) -> kms::Result<kms::KeyResults> {
            let (r, tx) = kms::KeyResults::new(1);
            drop(tx);
            Ok(r)
        }

        async fn generate_key(
            &self,
            _: &kms::RequestContext,
            _: kms::KeyScope,
        ) -> kms::Result<(kms::EncryptionCid, [u8; 32])> {
            Err(kms::Error::Unsupported("stub"))
        }

        async fn serve_request(
            &self,
            _: kms::PeerIdentity,
            _: kms::FetchEncryptionKeyRequest,
        ) -> kms::Result<kms::FetchEncryptionKeyReply> {
            Err(kms::Error::Unsupported("stub"))
        }
    }

    #[test]
    fn db_kms_accessor_round_trips() {
        let db = DB::new(MemoryStore::new()).unwrap();
        assert!(db.kms().is_none());

        let first: Arc<dyn kms::KmsService> = Arc::new(StubKms);
        db.set_kms(first);
        assert!(db.kms().is_some());

        // OnceLock: second set is silently ignored.
        let second: Arc<dyn kms::KmsService> = Arc::new(StubKms);
        db.set_kms(second);
        assert!(db.kms().is_some());
    }
}
