/// Database struct for DefraDB matching Go's internal/db/db.go.
///
/// The DB struct is the main entry point for DefraDB operations.
/// It manages the root store, creates transactions, and provides
/// access to collections.
use crate::collection::Collection;
use crate::collection_name::CollectionName;
use crate::collection_snapshot::CollectionSnapshot;
use crate::error::{Error, Result};
use crate::txn::DbTxn;
use datastore::BasicTxn;
use events::Bus;
use identity::{Identity, RawIdentity};
#[cfg(not(feature = "native"))]
use lens::MemoryTransformStore;
use lens::TransformStore;
#[cfg(feature = "native")]
use lens::WasmTransformStore;
use schema::{CollectionSource, CollectionVersion};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::systemstore::{
    CollectionID, CollectionIDSequenceKey, CollectionKey, CollectionNameKey, CollectionVersionKey,
    IndexIDSequenceKey,
};

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
    pub(crate) store: Arc<S>,
    /// Options for this database instance.
    options: DbOptions,
    /// Counter for generating unique transaction IDs.
    txn_id_counter: AtomicU64,
    /// In-memory collection cache (name -> Collection).
    pub(crate) collections: RwLock<HashMap<String, Collection>>,
    /// Event bus for subscription notifications.
    event_bus: Option<Arc<dyn Bus>>,
    /// Lens transform store for schema migrations.
    pub(crate) lens_store: Arc<dyn TransformStore>,
    /// Pending migrations registered before their destination version exists.
    /// Maps dest_version_id -> (source_version_id, transform_id_string).
    pub(crate) pending_migrations: RwLock<HashMap<String, (String, String)>>,
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
            collections: RwLock::new(HashMap::new()),
            event_bus: None,
            lens_store,
            pending_migrations: RwLock::new(HashMap::new()),
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
            collections: RwLock::new(HashMap::new()),
            event_bus: None,
            lens_store,
            pending_migrations: RwLock::new(HashMap::new()),
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
    pub async fn new_txn(&self, readonly: bool) -> Result<DbTxn<S>> {
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
    pub async fn close(&self) -> Result<()> {
        self.store.close().await.map_err(Error::Storage)
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
        let store = WasmTransformStore::new()
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

    // NOTE: Lens migration methods (lens_store, set_migration, set_migration_in_txn,
    // maybe_reindex_after_migration, has_migration) are now in migration.rs

    /// Load all collections from the SystemStore into the in-memory cache.
    ///
    /// This also finalizes relations by:
    /// - Auto-generating `_id` fields for non-array relation fields
    /// - Auto-determining primary sides for one-to-many relations
    pub async fn load_collections(&self) -> Result<()> {
        let txn = self.new_txn(true).await?;
        let prefix = CollectionNameKey::name_prefix();
        let mut schemas: HashMap<String, CollectionVersion> = HashMap::new();

        // Block ensures systemstore reference is dropped before discard
        {
            let systemstore = txn.systemstore()?;
            let opts = IterOptions::new().with_prefix(prefix.clone());

            let mut iter = systemstore.iterator(opts).await.map_err(|e| {
                tracing::error!(error = ?e, "Failed to create iterator during collection load");
                Error::Storage(e)
            })?;

            while let Some(pair) = iter.next().await.map_err(|e| {
                tracing::error!(error = ?e, "Failed to iterate collections during database load");
                Error::Storage(e)
            })? {
                // Validate UTF-8 in key to catch data corruption early
                let key_str = String::from_utf8(pair.key.to_vec()).map_err(|e| {
                    tracing::error!(
                        error = ?e,
                        key_bytes = ?&pair.key[..pair.key.len().min(50)],
                        "Collection key contains invalid UTF-8"
                    );
                    Error::Serialization(format!("collection key contains invalid UTF-8: {}", e))
                })?;

                let prefix_str = String::from_utf8(prefix.clone()).map_err(|e| {
                    tracing::error!(
                        error = ?e,
                        prefix_bytes = ?&prefix[..prefix.len().min(50)],
                        "Internal error: collection key prefix contains invalid UTF-8"
                    );
                    Error::Other(format!("internal error: prefix is not valid UTF-8: {}", e))
                })?;

                let name = key_str
                    .strip_prefix(&prefix_str)
                    .ok_or_else(|| {
                        tracing::error!(
                            key = %key_str,
                            expected_prefix = %prefix_str,
                            "Collection key does not match expected prefix - possible data corruption"
                        );
                        Error::Other(format!(
                            "collection key '{}' does not match expected prefix '{}'",
                            key_str, prefix_str
                        ))
                    })?
                    .to_string();

                // The value at /collection/name/{name} is the version_id string, not full JSON
                let version_id = String::from_utf8(pair.value.to_vec()).map_err(|e| {
                    tracing::error!(
                        error = ?e,
                        collection_name = %name,
                        "Collection version ID contains invalid UTF-8"
                    );
                    Error::Serialization(format!(
                        "collection version ID for '{}' contains invalid UTF-8: {}",
                        name, e
                    ))
                })?;

                // Look up the full collection definition from /collection/id/{version_id}
                let collection_key = CollectionKey::new(&version_id);
                let collection_json = systemstore
                    .get(&collection_key.bytes())
                    .await
                    .map_err(|e| {
                        tracing::error!(
                            error = ?e,
                            collection_name = %name,
                            version_id = %version_id,
                            "Failed to get collection definition"
                        );
                        Error::Storage(e)
                    })?
                    .ok_or_else(|| {
                        tracing::error!(
                            collection_name = %name,
                            version_id = %version_id,
                            "Collection definition not found - data inconsistency"
                        );
                        Error::Other(format!(
                            "collection definition not found for '{}' with version_id '{}'",
                            name, version_id
                        ))
                    })?;

                let mut schema: CollectionVersion = serde_json::from_slice(&collection_json)
                    .map_err(|e| {
                        tracing::error!(
                            error = ?e,
                            collection_name = %name,
                            version_id = %version_id,
                            "Failed to deserialize schema for collection"
                        );
                        Error::Serialization(format!(
                            "failed to deserialize schema for collection '{}': {}",
                            name, e
                        ))
                    })?;

                // Load root_id from /collection/shortID/{collection_id}
                // (root_id is #[serde(skip)] so it's not in the JSON)
                let short_id_key = CollectionID::new(&schema.collection_id);
                if let Some(short_id_bytes) = systemstore
                    .get(&short_id_key.bytes())
                    .await
                    .map_err(Error::Storage)?
                {
                    if let Ok(short_id_str) = String::from_utf8(short_id_bytes) {
                        schema.root_id = short_id_str.parse::<u32>().unwrap_or(0);
                    }
                }

                schemas.insert(name, schema);
            }

            iter.close().await.map_err(|e| {
                tracing::error!(error = ?e, "Failed to close iterator during collection load");
                Error::Storage(e)
            })?;
        }

        if let Err(discard_err) = txn.discard() {
            tracing::error!(
                error = %discard_err,
                "Transaction discard failed after loading collections"
            );
            return Err(Error::Other(format!(
                "failed to discard transaction: {}",
                discard_err
            )));
        }

        // Finalize relations: auto-generate _id fields, set primary sides, and create unique indexes
        // Create a field ID generator that starts after the max existing field ID
        let max_field_id = schemas
            .values()
            .flat_map(|s| s.fields.iter())
            .filter_map(|f| f.id.parse::<u64>().ok())
            .max()
            .unwrap_or(0);
        let mut field_id_counter = max_field_id + 1000; // Start well above existing IDs

        // Create an index ID generator that starts after the max existing index ID
        let max_index_id = schemas
            .values()
            .flat_map(|s| s.indexes.iter())
            .map(|idx| idx.id)
            .max()
            .unwrap_or(0);
        let mut index_id_counter = max_index_id.max(10000); // Start at least at 10000

        CollectionVersion::finalize_relations_hashmap(
            &mut schemas,
            || {
                field_id_counter += 1;
                format!("gen-{}", field_id_counter)
            },
            || {
                index_id_counter += 1;
                index_id_counter
            },
        )
        .map_err(|e| {
            tracing::error!(error = ?e, "Failed to finalize relations during collection load");
            Error::Other(format!("failed to finalize relations: {}", e))
        })?;

        // Wrap schemas in Collection and store in cache
        let collections: HashMap<String, Collection> = schemas
            .into_iter()
            .map(|(name, schema)| (name, Collection::new(schema)))
            .collect();

        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(error = ?e, "Collection cache lock poisoned during load");
            Error::LockPoisoned("collection cache lock poisoned during load".into())
        })?;
        *cache = collections;

        Ok(())
    }

    /// Reload the collection cache from persistent storage.
    ///
    /// This method is useful for recovering from cache-store inconsistency
    /// without restarting the application. Call this after receiving a
    /// `CacheUpdateFailedAfterCommit` error.
    ///
    /// # Example
    ///
    /// ```ignore
    /// match db.create_collection(schema).await {
    ///     Ok(_) => println!("Collection created"),
    ///     Err(Error::CacheUpdateFailedAfterCommit(_)) => {
    ///         // Collection was persisted but cache update failed
    ///         db.reload_cache().await?;
    ///         println!("Cache recovered");
    ///     }
    ///     Err(e) => return Err(e),
    /// }
    /// ```
    pub async fn reload_cache(&self) -> Result<()> {
        tracing::info!("Reloading collection cache from persistent storage");
        self.load_collections().await
    }

    /// Create a new collection within an existing transaction.
    ///
    /// The collection is written to the store and added to the transaction's cache.
    /// The caller is responsible for committing or discarding the transaction.
    ///
    /// Storage layout:
    /// - `/collection/id/{version_id}` - Full collection JSON
    /// - `/collection/name/{name}` - Maps name to version_id (string)
    /// - `/collection/version/{collection_id}/{version_id}` - Version index
    ///
    /// # Errors
    ///
    /// - `InvalidCollectionName` if the collection name is invalid
    /// - `CollectionAlreadyExists` if a collection with this name already exists
    pub async fn create_collection_with_txn(
        &self,
        txn: &mut DbTxn<S>,
        mut schema: CollectionVersion,
    ) -> Result<CollectionVersion> {
        // Validate collection name
        let collection_name = CollectionName::new(&schema.name)?;

        // Validate schema (includes policy validation for path traversal prevention)
        schema.validate()?;
        let name = collection_name.as_str().to_string();
        let version_id = &schema.version_id.clone();
        let collection_id = &schema.collection_id.clone();

        // Check if collection exists in txn cache or store
        if txn.get_collection(&name).await?.is_some() {
            return Err(Error::CollectionAlreadyExists(name));
        }

        let systemstore = txn.systemstore()?;

        // Assign sequential short ID (matches Go's monotonic counter)
        let short_id = Self::next_collection_short_id(&systemstore).await?;
        schema.root_id = short_id;

        // Re-assign index IDs from the persistent sequence so they start at 1.
        // The SDL parser assigns placeholder IDs based on field_id_counter, but
        // Go assigns them via IndexManager.next_index_id() which uses a per-collection
        // sequence key. We replicate that here so IDs match Go exactly.
        if !schema.indexes.is_empty() {
            let col_short_id = crate::collection::collection_short_id(collection_id.as_str());
            let seq_key = IndexIDSequenceKey::new(format!("{}", col_short_id));
            let key_bytes = seq_key.bytes();
            let mut current: u32 =
                match systemstore.get(&key_bytes).await.map_err(Error::Storage)? {
                    Some(bytes) => {
                        if bytes.len() == 4 {
                            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
                        } else {
                            0
                        }
                    }
                    None => 0,
                };
            for idx in &mut schema.indexes {
                current += 1;
                idx.id = current;
            }
            systemstore
                .set(&key_bytes, &current.to_be_bytes())
                .await
                .map_err(Error::Storage)?;
        }

        // Store short ID mapping at /collection/shortID/{collection_id}
        let short_id_key = CollectionID::new(collection_id.as_str());
        systemstore
            .set(&short_id_key.bytes(), short_id.to_string().as_bytes())
            .await
            .map_err(Error::Storage)?;

        // 1. Store full schema at /collection/id/{version_id}
        let collection_key = CollectionKey::new(version_id.as_str());
        let data = serde_json::to_vec(&schema).map_err(|e| {
            Error::Serialization(format!(
                "failed to serialize schema for collection '{}': {}",
                name, e
            ))
        })?;
        systemstore
            .set(&collection_key.bytes(), &data)
            .await
            .map_err(Error::Storage)?;

        // Store field and collection definition blocks in blockstore for Bitswap sync.
        // Go stores these blocks so peers can fetch them via Bitswap during collection version sync.
        let blockstore = txn.blockstore()?;

        // Store each field definition block
        // IMPORTANT: Go uses priority=1 for ALL fields during AddSchema (not incrementing).
        // This was verified by comparing actual Go AddSchema output with manual CID generation.
        // Only fields with non-empty FieldID are stored (secondary relations are excluded).
        // Fields must be sorted: _docID first, then alphabetically by name (matches Go).
        let mut sorted_fields: Vec<&schema::FieldDescription> =
            schema.fields.iter().filter(|f| !f.id.is_empty()).collect();
        sorted_fields.sort_by(|a, b| {
            if a.name == "_docID" {
                std::cmp::Ordering::Less
            } else if b.name == "_docID" {
                std::cmp::Ordering::Greater
            } else {
                a.name.cmp(&b.name)
            }
        });

        let mut field_cids = Vec::with_capacity(sorted_fields.len());
        for field in &sorted_fields {
            let block_with_cid =
                schema::generate_field_block_with_priority_and_heads(field, 1, &[])
                    .map_err(Error::Schema)?;
            blockstore
                .set(&block_with_cid.cid.to_bytes(), &block_with_cid.bytes)
                .await
                .map_err(Error::Storage)?;
            field_cids.push(block_with_cid.cid);
        }

        // Store collection definition block (links to all field CIDs)
        let col_block = schema::generate_collection_block_full(
            Some(&schema.name),
            &field_cids,
            1, // Go uses priority=1 for collection blocks during AddSchema
            &[],
        )
        .map_err(Error::Schema)?;
        blockstore
            .set(&col_block.cid.to_bytes(), &col_block.bytes)
            .await
            .map_err(Error::Storage)?;

        // 2. Store name → version_id mapping at /collection/name/{name}
        let name_key = CollectionNameKey::new(&name);
        systemstore
            .set(&name_key.bytes(), version_id.as_bytes())
            .await
            .map_err(Error::Storage)?;

        // 3. Store version index at /collection/version/{collection_id}/{version_id}
        let version_key = CollectionVersionKey::new(collection_id.as_str(), version_id.as_str());
        systemstore
            .set(&version_key.bytes(), b"1")
            .await
            .map_err(Error::Storage)?;

        // Update txn-local cache
        let returned_schema = schema.clone();
        txn.cache_collection(Collection::new(schema));

        Ok(returned_schema)
    }

    /// Get the next sequential collection short ID from the system store.
    ///
    /// Reads the current value from `/seq/collection`, increments it, and stores
    /// the updated value. Returns the new ID. Matches Go's sequence.Next() pattern.
    async fn next_collection_short_id(systemstore: &datastore::NamespaceView) -> Result<u32> {
        let seq_key = CollectionIDSequenceKey::new();
        let current: u32 = match systemstore
            .get(&seq_key.bytes())
            .await
            .map_err(Error::Storage)?
        {
            Some(bytes) => {
                // Go stores as big-endian u64
                if bytes.len() == 8 {
                    let arr: [u8; 8] = bytes[..8].try_into().unwrap_or([0; 8]);
                    u64::from_be_bytes(arr) as u32
                } else {
                    // Try as string for backwards compat
                    String::from_utf8_lossy(&bytes).parse::<u32>().unwrap_or(0)
                }
            }
            None => 0,
        };
        let next_id = current + 1;
        // Store as big-endian u64 (matches Go's binary.BigEndian.PutUint64)
        systemstore
            .set(&seq_key.bytes(), &(next_id as u64).to_be_bytes())
            .await
            .map_err(Error::Storage)?;
        Ok(next_id)
    }

    /// Create a new collection with schema persistence.
    ///
    /// This is a convenience method that creates its own transaction.
    /// For multi-operation transactions, use `create_collection_with_txn`.
    ///
    /// # Errors
    ///
    /// - `InvalidCollectionName` if the collection name is invalid
    /// - `CollectionAlreadyExists` if a collection with this name already exists
    pub async fn create_collection(&self, schema: CollectionVersion) -> Result<()> {
        let name = schema.name.clone();

        let mut txn = self.new_txn(false).await?;
        match self.create_collection_with_txn(&mut txn, schema).await {
            Ok(updated_schema) => {
                txn.commit().await?;

                // Update process-wide cache with the schema that has root_id assigned
                let mut cache = self.collections.write().map_err(|e| {
                    tracing::error!(
                        error = ?e,
                        collection_name = %name,
                        "Collection cache lock poisoned during create"
                    );
                    Error::CacheUpdateFailedAfterCommit(name.clone())
                })?;
                cache.insert(name, Collection::new(updated_schema));
                Ok(())
            }
            Err(e) => {
                if let Err(discard_err) = txn.discard() {
                    tracing::warn!(
                        error = %discard_err,
                        original_error = %e,
                        "Transaction discard failed after create_collection error"
                    );
                }
                Err(e)
            }
        }
    }

    /// Create multiple collections atomically within a single transaction.
    ///
    /// All collections are created within a single transaction: if any collection
    /// creation fails, the entire operation is rolled back. This is used by `add_view`
    /// to ensure view collections (type + interface) are created atomically.
    pub async fn create_collections_atomic(
        &self,
        schemas: Vec<CollectionVersion>,
    ) -> Result<Vec<CollectionVersion>> {
        let mut txn = self.new_txn(false).await?;
        let mut created = Vec::new();

        for schema in schemas {
            match self.create_collection_with_txn(&mut txn, schema).await {
                Ok(updated_schema) => {
                    created.push(updated_schema);
                }
                Err(e) => {
                    if let Err(discard_err) = txn.discard() {
                        tracing::warn!(
                            error = %discard_err,
                            original_error = %e,
                            "Transaction discard failed after atomic create_collections error"
                        );
                    }
                    return Err(e);
                }
            }
        }

        txn.commit().await?;

        // Update process-wide cache with all created schemas
        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(error = ?e, "Collection cache lock poisoned during atomic create");
            Error::CacheUpdateFailedAfterCommit("atomic create_collections".into())
        })?;
        for schema in &created {
            cache.insert(schema.name.clone(), Collection::new(schema.clone()));
        }

        Ok(created)
    }

    /// Delete a collection and all its documents within an existing transaction.
    ///
    /// The collection is deleted from the store and removed from the transaction's cache.
    /// The caller is responsible for committing or discarding the transaction.
    ///
    /// # Errors
    ///
    /// - `CollectionNotFound` if the collection does not exist
    pub async fn delete_collection_with_txn(&self, txn: &mut DbTxn<S>, name: &str) -> Result<()> {
        // Get collection from txn cache/store
        let collection = txn
            .get_collection(name)
            .await?
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))?;
        let collection_id = collection.collection_id().to_string();

        // Delete schema from store
        let schema_key = CollectionNameKey::new(name);
        txn.systemstore()?
            .delete(&schema_key.bytes())
            .await
            .map_err(Error::Storage)?;

        // Delete documents
        let datastore = txn.datastore()?;
        let doc_prefix = format!("/d/{}/", collection_id);
        let opts = IterOptions::new().with_prefix(doc_prefix.as_bytes().to_vec());

        let mut iter = datastore.iterator(opts).await.map_err(|e| {
            tracing::error!(
                error = ?e,
                collection_name = %name,
                "Failed to create iterator for document deletion"
            );
            Error::Storage(e)
        })?;
        let mut keys_to_delete = Vec::new();

        while let Some(pair) = iter.next().await.map_err(|e| {
            tracing::error!(
                error = ?e,
                collection_name = %name,
                documents_found = keys_to_delete.len(),
                "Failed to iterate documents during collection deletion"
            );
            Error::Storage(e)
        })? {
            keys_to_delete.push(pair.key.clone());
        }
        iter.close().await.map_err(|e| {
            tracing::error!(
                error = ?e,
                collection_name = %name,
                "Failed to close iterator during collection deletion"
            );
            Error::Storage(e)
        })?;

        tracing::debug!(
            collection_name = %name,
            documents_to_delete = keys_to_delete.len(),
            "Deleting documents for collection"
        );

        for (i, key) in keys_to_delete.iter().enumerate() {
            datastore.delete(key).await.map_err(|e| {
                tracing::error!(
                    error = ?e,
                    collection_name = %name,
                    documents_deleted = i,
                    documents_total = keys_to_delete.len(),
                    "Failed to delete document during collection deletion"
                );
                Error::Storage(e)
            })?;
        }

        // Update txn-local cache
        txn.uncache_collection(name);

        Ok(())
    }

    /// Delete a collection and all its documents.
    ///
    /// This is a convenience method that creates its own transaction.
    /// For multi-operation transactions, use `delete_collection_with_txn`.
    ///
    /// # Errors
    ///
    /// - `CollectionNotFound` if the collection does not exist
    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        let name_owned = name.to_string();

        let mut txn = self.new_txn(false).await?;
        match self.delete_collection_with_txn(&mut txn, name).await {
            Ok(()) => {
                txn.commit().await?;

                // Update process-wide cache for callers not using transaction-scoped caching
                let mut cache = self.collections.write().map_err(|e| {
                    tracing::error!(
                        error = ?e,
                        collection_name = %name_owned,
                        "Collection cache lock poisoned during delete"
                    );
                    Error::CacheUpdateFailedAfterCommit(name_owned.clone())
                })?;
                cache.remove(&name_owned);
                Ok(())
            }
            Err(e) => {
                if let Err(discard_err) = txn.discard() {
                    tracing::warn!(
                        error = %discard_err,
                        original_error = %e,
                        "Transaction discard failed after delete_collection error"
                    );
                }
                Err(e)
            }
        }
    }

    /// Truncate a collection: delete all documents, heads, blocks, and index entries
    /// while preserving the collection schema.
    ///
    /// This resets the collection to an empty state as if it were just created.
    pub async fn truncate_collection(&self, name: &str) -> Result<()> {
        let collection = self
            .get_collection(name)?
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))?;

        let collection_id = collection.collection_id().to_string();
        // Hash-based short ID used by index manager and collection head keys
        let short_id = crate::collection::collection_short_id(&collection_id);

        let mut txn = self.new_txn(false).await?;
        match self
            .truncate_collection_inner(&mut txn, &collection_id, short_id)
            .await
        {
            Ok(()) => {
                txn.commit().await?;
                Ok(())
            }
            Err(e) => {
                if let Err(discard_err) = txn.discard() {
                    tracing::warn!(
                        error = %discard_err,
                        original_error = %e,
                        "Transaction discard failed after truncate_collection error"
                    );
                }
                Err(e)
            }
        }
    }

    /// Inner truncation logic within an existing transaction.
    async fn truncate_collection_inner(
        &self,
        txn: &mut DbTxn<S>,
        collection_id: &str,
        short_id: u32,
    ) -> Result<()> {
        use storage::keys::{HeadstoreColKey, HeadstoreDocKey, IndexDataStoreKey};

        let datastore = txn.datastore()?;
        let headstore = txn.headstore()?;
        let blockstore = txn.blockstore()?;

        // Document data key prefix: /d/<collection_id>/
        let doc_prefix = format!("/d/{}/", collection_id).into_bytes();
        // Deletion marker prefix: /del/<collection_id>/
        let del_prefix = format!("/del/{}/", collection_id).into_bytes();

        // 1. Collect doc_ids from document data keys (/d/<collection_id>/<doc_id>)
        let mut doc_ids: Vec<String> = Vec::new();
        {
            let opts = IterOptions::new().with_prefix(doc_prefix.clone());
            let mut iter = datastore.iterator(opts).await.map_err(Error::Storage)?;
            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                // Skip version keys (end with /v)
                if pair.key.ends_with(b"/v") {
                    continue;
                }
                // Key format: /d/<collection_id>/<doc_id>
                if let Some(pos) = pair.key.iter().rposition(|&b| b == b'/') {
                    let doc_id = String::from_utf8_lossy(&pair.key[pos + 1..]).to_string();
                    if !doc_id.is_empty() {
                        doc_ids.push(doc_id);
                    }
                }
            }
            iter.close().await.map_err(Error::Storage)?;
        }

        // 2. Delete all document data from datastore
        delete_prefix(&datastore, doc_prefix).await?;

        // 3. Delete all deletion markers from datastore
        delete_prefix(&datastore, del_prefix).await?;

        // 4. Delete index entries from datastore (uses hash-based short_id)
        let idx_prefix = IndexDataStoreKey::collection_prefix(short_id);
        delete_prefix(&datastore, idx_prefix).await?;

        // 5. Delete document head entries from headstore + collect block CIDs
        let mut block_cids: Vec<Vec<u8>> = Vec::new();
        for doc_id in &doc_ids {
            let head_prefix = HeadstoreDocKey::document_prefix(doc_id);
            let opts = IterOptions::new().with_prefix(head_prefix);
            let mut iter = headstore.iterator(opts).await.map_err(Error::Storage)?;
            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                // Extract CID string from key: /d/{doc_id}/{field_id}/{CID_string}
                if let Some(cid_str) = extract_last_path_segment_str(&pair.key) {
                    if let Ok(cid) = cid::Cid::try_from(cid_str.as_str()) {
                        block_cids.push(cid.to_bytes());
                    }
                }
            }
            iter.close().await.map_err(Error::Storage)?;

            // Now delete all head entries for this doc
            let head_prefix = HeadstoreDocKey::document_prefix(doc_id);
            delete_prefix(&headstore, head_prefix).await?;
        }

        // 6. Delete collection-level head entries from headstore (uses hash-based short_id)
        let col_head_prefix = HeadstoreColKey::collection_prefix(short_id);
        {
            let opts = IterOptions::new().with_prefix(col_head_prefix.clone());
            let mut iter = headstore.iterator(opts).await.map_err(Error::Storage)?;
            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                if let Some(cid_str) = extract_last_path_segment_str(&pair.key) {
                    if let Ok(cid) = cid::Cid::try_from(cid_str.as_str()) {
                        block_cids.push(cid.to_bytes());
                    }
                }
            }
            iter.close().await.map_err(Error::Storage)?;
        }
        delete_prefix(&headstore, col_head_prefix).await?;

        // 7. Delete blocks from blockstore
        for cid_bytes in &block_cids {
            let _ = blockstore.delete(cid_bytes).await;
        }

        tracing::info!(
            collection_id = %collection_id,
            short_id = short_id,
            doc_count = doc_ids.len(),
            block_count = block_cids.len(),
            "Truncated collection"
        );

        Ok(())
    }

    /// List all collection names using the transaction's cache.
    ///
    /// This loads all collections from the store into the transaction cache
    /// if they haven't been loaded yet.
    pub async fn list_collections_with_txn(&self, txn: &mut DbTxn<S>) -> Result<Vec<String>> {
        txn.load_all_collections().await?;
        Ok(txn.collection_cache().names())
    }

    /// List all collection names.
    ///
    /// Uses the process-wide cache. For transaction-scoped access, use `list_collections_with_txn`.
    pub fn list_collections(&self) -> Result<Vec<String>> {
        let cache = self.collections.read().map_err(|e| {
            tracing::error!(error = ?e, "Collection cache lock poisoned during list");
            Error::LockPoisoned("collection cache lock poisoned during list".into())
        })?;
        Ok(cache.keys().cloned().collect())
    }

    /// Add a collection to the runtime cache.
    ///
    /// This is used by the merge handler to add synced collections received via P2P
    /// to the cache so they're visible to `list_collections` and `get_collection`.
    /// The collection can be inactive (synced collections start inactive until manually activated).
    pub fn add_collection_to_cache(&self, schema: CollectionVersion) -> Result<()> {
        let name = schema.name.clone();
        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(error = ?e, collection_name = %name, "Collection cache lock poisoned during add_collection_to_cache");
            Error::LockPoisoned("collection cache lock poisoned during add_collection_to_cache".into())
        })?;
        cache.insert(name, Collection::new(schema));
        Ok(())
    }

    /// Get a collection by name using the transaction's cache.
    ///
    /// This performs lazy loading - the collection is loaded from the store
    /// on first access within the transaction.
    pub async fn get_collection_with_txn(
        &self,
        txn: &mut DbTxn<S>,
        name: &str,
    ) -> Result<Option<Collection>> {
        txn.get_collection(name).await.map(|opt| opt.cloned())
    }

    /// Get a collection by name.
    ///
    /// Uses the process-wide cache. For transaction-scoped access, use `get_collection_with_txn`.
    pub fn get_collection(&self, name: &str) -> Result<Option<Collection>> {
        let cache = self
            .collections
            .read()
            .map_err(|e| {
                tracing::error!(error = ?e, collection_name = %name, "Collection cache lock poisoned during get");
                Error::LockPoisoned("collection cache lock poisoned during get".into())
            })?;
        Ok(cache.get(name).cloned())
    }

    /// Check if a collection exists using the transaction's cache.
    ///
    /// This performs lazy loading - the collection is loaded from the store
    /// on first access within the transaction.
    pub async fn has_collection_with_txn(&self, txn: &mut DbTxn<S>, name: &str) -> Result<bool> {
        Ok(txn.get_collection(name).await?.is_some())
    }

    /// Check if a collection exists.
    ///
    /// Uses the process-wide cache. For transaction-scoped access, use `has_collection_with_txn`.
    pub fn has_collection(&self, name: &str) -> Result<bool> {
        let cache = self
            .collections
            .read()
            .map_err(|e| {
                tracing::error!(error = ?e, collection_name = %name, "Collection cache lock poisoned during has_collection");
                Error::LockPoisoned("collection cache lock poisoned during has_collection".into())
            })?;
        Ok(cache.contains_key(name))
    }

    /// Find a collection by its collection ID (schema version ID).
    ///
    /// This is useful for P2P sync where we receive blocks with schema_version_id
    /// and need to find the corresponding collection.
    ///
    /// Uses the process-wide cache.
    pub fn find_collection_by_id(&self, collection_id: &str) -> Result<Option<Collection>> {
        let cache = self.collections.read().map_err(|e| {
            tracing::error!(
                error = ?e,
                collection_id = %collection_id,
                "Collection cache lock poisoned during find_collection_by_id"
            );
            Error::LockPoisoned(
                "collection cache lock poisoned during find_collection_by_id".into(),
            )
        })?;
        Ok(cache
            .values()
            .find(|c| c.collection_id() == collection_id)
            .cloned())
    }

    /// Get a snapshot of all collections (for use by DbTransactionRegistry).
    ///
    /// Returns an immutable snapshot that provides snapshot isolation for transactions.
    pub fn collections_snapshot(&self) -> Result<CollectionSnapshot> {
        let cache = self.collections.read().map_err(|e| {
            tracing::error!(error = ?e, "Collection cache lock poisoned during snapshot");
            Error::LockPoisoned("collection cache lock poisoned during snapshot".into())
        })?;
        Ok(CollectionSnapshot::new(cache.clone()))
    }

    /// Set the active collection version.
    ///
    /// This activates the collection with the given version ID and deactivates
    /// any other versions of the same collection.
    ///
    /// # Arguments
    ///
    /// * `version_id` - The version ID of the collection to activate
    ///
    /// # Errors
    ///
    /// - `CollectionVersionNotFound` if no collection with the given version ID exists
    pub async fn set_active_collection_version(&self, version_id: &str) -> Result<()> {
        if version_id.is_empty() {
            return Err(Error::CollectionVersionIDEmpty);
        }

        // Load the target collection from persistent store by version_id
        let txn = self.new_txn(false).await?;

        // Extract the target schema and perform all systemstore operations in a block
        // so the systemstore reference is dropped before calling txn.commit()
        let (target_schema, name) = {
            let systemstore = txn.systemstore()?;

            let collection_key = CollectionKey::new(version_id);
            let target_bytes = systemstore
                .get(&collection_key.bytes())
                .await
                .map_err(Error::Storage)?
                .ok_or_else(|| Error::CollectionVersionNotFound(version_id.to_string()))?;

            let target_schema: CollectionVersion =
                serde_json::from_slice(&target_bytes).map_err(|e| {
                    Error::Serialization(format!(
                        "failed to deserialize collection version '{}': {}",
                        version_id, e
                    ))
                })?;

            let collection_id = target_schema.collection_id.clone();
            let name = target_schema.name.clone();

            // Load all versions sharing the same collection_id
            let version_prefix = CollectionVersionKey::collection_prefix(&collection_id);
            let mut iter = systemstore
                .iterator(IterOptions::new().with_prefix(version_prefix))
                .await
                .map_err(Error::Storage)?;

            let mut sibling_version_ids = Vec::new();
            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                let key_str = String::from_utf8_lossy(&pair.key);
                // Key format: /collection/version/{collection_id}/{version_id}
                if let Some(vid) = key_str.rsplit('/').next() {
                    sibling_version_ids.push(vid.to_string());
                }
            }
            drop(iter);

            // For each sibling version, activate the target and deactivate others
            for vid in &sibling_version_ids {
                let sibling_key = CollectionKey::new(vid.as_str());
                if let Some(sibling_bytes) = systemstore
                    .get(&sibling_key.bytes())
                    .await
                    .map_err(Error::Storage)?
                {
                    let mut sibling_schema: CollectionVersion =
                        serde_json::from_slice(&sibling_bytes).map_err(|e| {
                            Error::Serialization(format!(
                                "failed to deserialize sibling collection '{}': {}",
                                vid, e
                            ))
                        })?;

                    let should_be_active = vid == version_id;
                    if sibling_schema.is_active == should_be_active {
                        continue;
                    }

                    sibling_schema.is_active = should_be_active;
                    let data = serde_json::to_vec(&sibling_schema).map_err(|e| {
                        Error::Serialization(format!(
                            "failed to serialize schema for collection '{}': {}",
                            vid, e
                        ))
                    })?;
                    systemstore
                        .set(&sibling_key.bytes(), &data)
                        .await
                        .map_err(Error::Storage)?;
                }
            }

            // Update the name → version_id mapping to point to the new active version
            let name_key = CollectionNameKey::new(&name);
            systemstore
                .set(&name_key.bytes(), version_id.as_bytes())
                .await
                .map_err(Error::Storage)?;

            (target_schema, name)
        }; // systemstore reference dropped here

        txn.commit().await?;

        // Update the cache with the newly active version
        let mut active_schema = target_schema;
        active_schema.is_active = true;

        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(
                error = ?e,
                collection_name = %name,
                "Collection cache lock poisoned during set_active_collection_version update"
            );
            Error::CacheUpdateFailedAfterCommit(name.clone())
        })?;
        cache.insert(name, Collection::new(active_schema));

        Ok(())
    }

    /// Get a collection by its version ID.
    ///
    /// This searches all collections for one matching the given version ID.
    ///
    /// # Arguments
    ///
    /// * `version_id` - The version ID to search for
    ///
    /// # Returns
    ///
    /// The collection if found, None otherwise.
    pub fn get_collection_by_version_id(&self, version_id: &str) -> Result<Option<Collection>> {
        let cache = self.collections.read().map_err(|e| {
            tracing::error!(
                error = ?e,
                version_id = %version_id,
                "Collection cache lock poisoned during get_collection_by_version_id"
            );
            Error::LockPoisoned(
                "collection cache lock poisoned during get_collection_by_version_id".into(),
            )
        })?;

        Ok(cache
            .values()
            .find(|c| c.schema().version_id == version_id)
            .cloned())
    }

    /// Get a collection by version ID, searching both cache and KV store.
    pub(crate) async fn get_collection_by_version_id_full(
        &self,
        version_id: &str,
    ) -> Result<Option<Collection>> {
        // Check cache first
        if let Some(c) = self.get_collection_by_version_id(version_id)? {
            return Ok(Some(c));
        }
        // Search all stored versions (including inactive)
        let all_versions = self.get_all_collection_versions().await?;
        Ok(all_versions
            .into_iter()
            .find(|v| v.version_id == version_id)
            .map(Collection::new))
    }

    /// Get all collection versions from storage (active and inactive).
    ///
    /// This scans `/collection/id/` prefix to load ALL versions, matching
    /// Go's behavior of loading all versions for cross-collection validation.
    pub async fn get_all_collection_versions(&self) -> Result<Vec<CollectionVersion>> {
        let txn = self.new_txn(true).await?;
        let mut versions = Vec::new();
        let prefix = CollectionKey::collection_prefix();

        {
            let systemstore = txn.systemstore()?;
            let opts = IterOptions::new().with_prefix(prefix);
            let mut iter = systemstore.iterator(opts).await.map_err(Error::Storage)?;

            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                match serde_json::from_slice::<CollectionVersion>(&pair.value) {
                    Ok(col) => versions.push(col),
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "Failed to deserialize collection version during scan"
                        );
                    }
                }
            }

            iter.close().await.map_err(Error::Storage)?;
        }

        let _ = txn.discard();
        Ok(versions)
    }

    /// Check if a collection has any documents in the datastore.
    pub(crate) async fn collection_has_data(&self, collection_id: &str) -> Result<bool> {
        let txn = self.new_txn(true).await?;
        let has_data = {
            let datastore = txn.datastore()?;
            let doc_prefix = format!("/d/{}/", collection_id);
            let opts = IterOptions::new().with_prefix(doc_prefix.as_bytes().to_vec());
            let mut iter = datastore.iterator(opts).await.map_err(Error::Storage)?;
            let has_any = iter.next().await.map_err(Error::Storage)?.is_some();
            iter.close().await.map_err(Error::Storage)?;
            has_any
        };
        let _ = txn.discard();
        Ok(has_data)
    }

    /// Extract a Merkle proof from the blockstore.
    ///
    /// This generates a proof demonstrating that the block at `leaf_cid` is part
    /// of the Merkle chain leading to `root_cid`. The proof can be used to verify
    /// data integrity without access to the full database.
    ///
    /// # Arguments
    ///
    /// * `leaf_cid` - The CID of the leaf block (e.g., a document update)
    /// * `root_cid` - The CID of the root block (e.g., the collection head)
    ///
    /// # Returns
    ///
    /// * `Ok(Some(proof))` - A valid proof path exists from leaf to root
    /// * `Ok(None)` - No path exists (blocks are unrelated or one doesn't exist)
    /// * `Err(Error)` - An error occurred during proof extraction
    ///
    /// # Example
    ///
    /// ```ignore
    /// let proof = db.extract_proof(&leaf_cid, &root_cid).await?;
    /// if let Some(proof) = proof {
    ///     // Verify the proof
    ///     assert!(crypto::verify_proof(&proof)?);
    ///
    ///     // Optionally sign the proof
    ///     let signed = crypto::SignedMerkleProof::sign(proof, &private_key)?;
    /// }
    /// ```
    pub async fn extract_proof(
        &self,
        leaf_cid: &cid::Cid,
        root_cid: &cid::Cid,
    ) -> Result<Option<crypto::MerkleProof>>
    where
        S: 'static,
    {
        // Create a blockstore wrapper for proof extraction
        let blockstore = blockstore::DefraBlockstore::new(self.store.clone(), false);

        // Use the crypto crate's extract_proof function
        crypto::extract_proof(&blockstore, *leaf_cid, *root_cid)
            .await
            .map_err(|e| Error::Other(format!("failed to extract proof: {}", e)))
    }

    /// Extract and sign a Merkle proof.
    ///
    /// This is a convenience method that extracts a proof and signs it in one step.
    ///
    /// # Arguments
    ///
    /// * `leaf_cid` - The CID of the leaf block
    /// * `root_cid` - The CID of the root block
    /// * `private_key` - The private key to sign with (Ed25519 or secp256k1)
    ///
    /// # Returns
    ///
    /// * `Ok(Some(signed_proof))` - A signed proof
    /// * `Ok(None)` - No proof path exists
    /// * `Err(Error)` - An error occurred
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

/// Delete all keys matching a prefix from a namespace view.
async fn delete_prefix(store: &datastore::NamespaceView, prefix: Vec<u8>) -> Result<()> {
    let opts = IterOptions::new().with_prefix(prefix);
    let mut iter = store.iterator(opts).await.map_err(Error::Storage)?;
    let mut keys = Vec::new();
    while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
        keys.push(pair.key.clone());
    }
    iter.close().await.map_err(Error::Storage)?;
    for key in keys {
        store.delete(&key).await.map_err(Error::Storage)?;
    }
    Ok(())
}

/// Extract the last path segment from a `/`-separated key as a UTF-8 string.
/// For key `/d/doc123/C/bafyrei...`, returns `"bafyrei..."`.
fn extract_last_path_segment_str(key: &[u8]) -> Option<String> {
    if let Some(pos) = key.iter().rposition(|&b| b == b'/') {
        let segment = &key[pos + 1..];
        if !segment.is_empty() {
            return String::from_utf8(segment.to_vec()).ok();
        }
    }
    None
}
