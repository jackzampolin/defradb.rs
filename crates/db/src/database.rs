/// Database struct for DefraDB matching Go's internal/db/db.go.
///
/// The DB struct is the main entry point for DefraDB operations.
/// It manages the root store, creates transactions, and provides
/// access to collections.
use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::txn::DbTxn;
use cid::Cid;
use datastore::BasicTxn;
use events::Bus;
use identity::{Identity, RawIdentity};
#[cfg(not(feature = "native"))]
use lens::MemoryTransformStore;
use lens::TransformStore;
#[cfg(feature = "native")]
use lens::WasmTransformStore;
use std::collections::HashMap;
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
        Ok(DbTxn::new(basic_txn, self.store.clone()))
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

    /// Extract a Merkle proof from the blockstore.
    ///
    /// This generates a proof demonstrating that the block at `leaf_cid` is part
    /// of the Merkle chain leading to `root_cid`. The proof can be used to verify
    /// data integrity without access to the full database.
    pub async fn extract_proof(
        &self,
        leaf_cid: &cid::Cid,
        root_cid: &cid::Cid,
    ) -> Result<Option<crypto::MerkleProof>>
    where
        S: 'static,
    {
        let blockstore = blockstore::DefraBlockstore::new(self.store.clone(), false);
        crypto::extract_proof(&blockstore, *leaf_cid, *root_cid)
            .await
            .map_err(|e| Error::Other(format!("failed to extract proof: {}", e)))
    }

    /// Extract and sign a Merkle proof.
    ///
    /// This is a convenience method that extracts a proof and signs it in one step.
    pub async fn extract_signed_proof(
        &self,
        leaf_cid: &cid::Cid,
        root_cid: &cid::Cid,
        private_key: &dyn crypto::PrivateKey,
    ) -> Result<Option<crypto::SignedMerkleProof>>
    where
        S: 'static,
    {
        let proof = self.extract_proof(leaf_cid, root_cid).await?;

        match proof {
            Some(p) => {
                let signed = crypto::SignedMerkleProof::sign(p, private_key)
                    .map_err(|e| Error::Other(format!("failed to sign proof: {}", e)))?;
                Ok(Some(signed))
            }
            None => Ok(None),
        }
    }
}
