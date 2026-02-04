/// Database struct for DefraDB matching Go's internal/db/db.go.
///
/// The DB struct is the main entry point for DefraDB operations.
/// It manages the root store, creates transactions, and provides
/// access to collections.
use crate::collection::{collection_short_id, Collection};
use crate::collection_name::CollectionName;
use crate::collection_snapshot::CollectionSnapshot;
use crate::error::{Error, Result};
use crate::index_manager::IndexManager;
use crate::schema_loader::get_collections_by_collection_id;
use crate::txn::DbTxn;
use datastore::BasicTxn;
use events::Bus;
use identity::{Identity, RawIdentity};
#[cfg(not(feature = "native"))]
use lens::MemoryTransformStore;
#[cfg(feature = "native")]
use lens::WasmTransformStore;
use lens::{
    build_targeted_history, CollectionHistoryLink, Lens, LensConfig, LensDoc, TransformId,
    TransformStore, DOC_ID_FIELD,
};
use schema::{CollectionSource, CollectionVersion, FieldKind, ScalarKind, ORPHAN_COLLECTION_ID};
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
    store: Arc<S>,
    /// Options for this database instance.
    options: DbOptions,
    /// Counter for generating unique transaction IDs.
    txn_id_counter: AtomicU64,
    /// In-memory collection cache (name -> Collection).
    collections: RwLock<HashMap<String, Collection>>,
    /// Event bus for subscription notifications.
    event_bus: Option<Arc<dyn Bus>>,
    /// Lens transform store for schema migrations.
    lens_store: Arc<dyn TransformStore>,
    /// Pending migrations registered before their destination version exists.
    /// Maps dest_version_id -> (source_version_id, transform_id_string).
    pending_migrations: RwLock<HashMap<String, (String, String)>>,
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

    // =========================================================================
    // Lens Migration Methods
    // =========================================================================

    /// Get a reference to the lens transform store.
    ///
    /// The lens store manages schema migration transforms that can be applied
    /// when documents are fetched from older schema versions.
    pub fn lens_store(&self) -> &Arc<dyn TransformStore> {
        &self.lens_store
    }

    /// Set a migration between two schema versions.
    ///
    /// This registers a lens transform that will be applied to documents
    /// when migrating from the source schema version to the destination.
    ///
    /// # Arguments
    ///
    /// * `config` - The lens configuration containing source/destination versions and transform
    ///
    /// # Returns
    ///
    /// The transform ID that was registered.
    pub async fn set_migration(&self, config: LensConfig) -> Result<TransformId> {
        let dest_version_id = config.destination_schema_version_id.clone();
        let source_version_id = config.source_schema_version_id.clone();

        let txn = self.new_txn(false).await?;

        // Look up source and destination versions, creating placeholders if needed
        // (matches Go's setMigration in internal/db/lens.go)
        let (source_col, mut dst_col) = {
            let systemstore = txn.systemstore()?;

            // Look up source version
            let src_key = CollectionKey::new(&source_version_id);
            let src_data = systemstore
                .get(&src_key.bytes())
                .await
                .map_err(Error::Storage)?;
            let source_col: CollectionVersion = match src_data {
                Some(data) => serde_json::from_slice(&data).map_err(|e| {
                    Error::Serialization(format!(
                        "failed to deserialize source schema '{}': {}",
                        source_version_id, e
                    ))
                })?,
                None => {
                    // Source doesn't exist — create a placeholder
                    let mut placeholder = CollectionVersion {
                        version_id: source_version_id.clone(),
                        collection_id: ORPHAN_COLLECTION_ID.to_string(),
                        is_materialized: true,
                        is_placeholder: true,
                        ..CollectionVersion::new("", "", "", Vec::new())
                    };
                    placeholder.is_active = false;
                    let data = serde_json::to_vec(&placeholder).map_err(|e| {
                        Error::Serialization(format!(
                            "failed to serialize source placeholder '{}': {}",
                            source_version_id, e
                        ))
                    })?;
                    systemstore
                        .set(&src_key.bytes(), &data)
                        .await
                        .map_err(Error::Storage)?;
                    placeholder
                }
            };

            // Look up destination version
            let dst_key = CollectionKey::new(&dest_version_id);
            let dst_data = systemstore
                .get(&dst_key.bytes())
                .await
                .map_err(Error::Storage)?;
            let dst_col: CollectionVersion = match dst_data {
                Some(data) => serde_json::from_slice(&data).map_err(|e| {
                    Error::Serialization(format!(
                        "failed to deserialize destination schema '{}': {}",
                        dest_version_id, e
                    ))
                })?,
                None => {
                    // Destination doesn't exist — create a placeholder
                    let mut placeholder = CollectionVersion {
                        name: source_col.name.clone(),
                        version_id: dest_version_id.clone(),
                        collection_id: source_col.collection_id.clone(),
                        is_materialized: true,
                        is_placeholder: true,
                        ..CollectionVersion::new("", "", "", Vec::new())
                    };
                    placeholder.is_active = false;
                    // Store destination placeholder (same as source placeholder above)
                    let data = serde_json::to_vec(&placeholder).map_err(|e| {
                        Error::Serialization(format!(
                            "failed to serialize destination placeholder '{}': {}",
                            dest_version_id, e
                        ))
                    })?;
                    systemstore
                        .set(&dst_key.bytes(), &data)
                        .await
                        .map_err(Error::Storage)?;
                    placeholder
                }
            };

            (source_col, dst_col)
        };

        // Validate version adjacency
        if let Some(ref prev) = dst_col.previous_version {
            if prev.source_collection_id != source_col.version_id {
                return Err(Error::InvalidPatch(format!(
                    "cannot migrate between non-adjacent collection versions. \
                     Destination '{}' already has previous version '{}', but migration source is '{}'",
                    dest_version_id, prev.source_collection_id, source_version_id
                )));
            }
        }

        // Register the transform in the lens store
        let transform_id = self
            .lens_store
            .add(config)
            .await
            .map_err(|e| Error::Lens(e.to_string()))?;

        // Set the destination's previous_version with source and transform
        dst_col.previous_version = Some(CollectionSource {
            source_collection_id: source_col.version_id.clone(),
            transform: Some(transform_id.to_string()),
        });

        // Save the destination version
        let collection_name = dst_col.name.clone();
        let dst_key = CollectionKey::new(&dest_version_id);
        let dst_data = serde_json::to_vec(&dst_col).map_err(|e| {
            Error::Serialization(format!(
                "failed to serialize destination schema '{}': {}",
                dest_version_id, e
            ))
        })?;

        {
            let systemstore = txn.systemstore()?;
            systemstore
                .set(&dst_key.bytes(), &dst_data)
                .await
                .map_err(Error::Storage)?;

            // Write CollectionVersionKey entries so get_collection_version_ids() can
            // find these versions via prefix scan on /collection/version/{collection_id}/
            if !source_col.collection_id.is_empty() {
                let src_version_key = CollectionVersionKey::new(
                    &source_col.collection_id,
                    &source_version_id,
                );
                systemstore
                    .set(&src_version_key.bytes(), b"1")
                    .await
                    .map_err(Error::Storage)?;
            }
            if !dst_col.collection_id.is_empty() {
                let dst_version_key = CollectionVersionKey::new(
                    &dst_col.collection_id,
                    &dest_version_id,
                );
                systemstore
                    .set(&dst_version_key.bytes(), b"1")
                    .await
                    .map_err(Error::Storage)?;
            }
        }
        txn.commit().await?;

        // Update in-memory cache if this is the active collection
        if !collection_name.is_empty() {
            let mut cache = self.collections.write().map_err(|e| {
                tracing::error!(error = ?e, "Collection cache lock poisoned during set_migration");
                Error::LockPoisoned("collection cache lock poisoned during set_migration".into())
            })?;

            if let Some(cached) = cache.get(&collection_name) {
                if cached.schema().version_id == dest_version_id {
                    cache.insert(collection_name.clone(), Collection::new(dst_col));
                }
            }
        }

        // Rebuild secondary indexes if the destination version is the active collection
        // and has indexes (matches Go's behavior of reindexing after migration registration)
        if !collection_name.is_empty() {
            if let Err(e) = self
                .maybe_reindex_after_migration(&collection_name, &dest_version_id)
                .await
            {
            }
        } else {
        }

        Ok(transform_id)
    }

    /// Rebuild secondary indexes for a collection after a migration is registered.
    ///
    /// If the destination version is the currently active version and the collection
    /// has indexes, this fetches all documents (applying lens migration), drops
    /// existing index entries, and rebuilds them with migrated values.
    async fn maybe_reindex_after_migration(
        &self,
        collection_name: &str,
        dest_version_id: &str,
    ) -> Result<()> {
        let collection = match self.get_collection(collection_name)? {
            Some(c) => c,
            None => {
                return Ok(());
            }
        };


        // Only reindex if destination is the current active version
        if collection.version_id() != dest_version_id {
            return Ok(());
        }

        // Only reindex if the collection has indexes
        if collection.get_indexes().is_empty() {
            return Ok(());
        }


        let collection_id = collection.collection_id().to_string();
        let target_version_id = collection.version_id().to_string();
        let short_id = collection_short_id(&collection_id);

        // Load all versions of this collection to build migration history
        let read_txn = self.new_txn(true).await?;
        let systemstore = read_txn.systemstore()?;
        let versions = get_collections_by_collection_id(&systemstore, &collection_id).await?;
        let _ = read_txn.discard();

        // Build targeted migration history
        let history = {
            let mut full_history: HashMap<String, CollectionHistoryLink> = HashMap::new();
            for version in &versions {
                let mut link =
                    CollectionHistoryLink::new(&version.version_id, &version.collection_id);
                if let Some(ref prev) = version.previous_version {
                    link = link.with_previous(&prev.source_collection_id);
                    if let Some(ref transform_id) = prev.transform {
                        link = link.with_transform(transform_id);
                    }
                }
                full_history.insert(version.version_id.clone(), link);
            }

            // Build next links
            let reverse_links: Vec<(String, String)> = full_history
                .values()
                .flat_map(|link| {
                    link.previous
                        .iter()
                        .map(|prev_id| (prev_id.clone(), link.version_id.clone()))
                        .collect::<Vec<_>>()
                })
                .collect();
            for (parent_id, child_id) in reverse_links {
                if let Some(parent_link) = full_history.get_mut(&parent_id) {
                    if !parent_link.next.contains(&child_id) {
                        parent_link.next.push(child_id);
                    }
                }
            }

            match build_targeted_history(&full_history, &target_version_id) {
                Some(h) => h,
                None => return Ok(()),
            }
        };

        let has_migrations = history.values().any(|link| link.transform.is_some());
        if !has_migrations {
            return Ok(());
        }

        // Create a write transaction for the reindex
        let write_txn = self.new_txn(false).await?;

        // Scope the datastore borrow so it's dropped before commit
        {
            let datastore = write_txn.datastore()?;

            // Fetch all documents (raw, with their stored schema versions)
            let raw_docs = collection.get_all_with_datastore(&datastore).await?;

            // Apply lens migration to each document that needs it
            let mut migrated_docs = Vec::with_capacity(raw_docs.len());
            for doc in raw_docs {
                let doc_version = doc
                    .schema_version_id()
                    .unwrap_or(&target_version_id)
                    .to_string();

                if doc_version == target_version_id {
                    migrated_docs.push(doc);
                    continue;
                }

                // Convert to LensDoc
                if let Ok(map) = doc.to_map() {
                    let mut lens_doc = LensDoc::new();
                    for (key, value) in map {
                        lens_doc.insert(key, value);
                    }

                    let mut lens =
                        Lens::new(self.lens_store.clone(), &target_version_id, history.clone());

                    if let Ok(()) = lens.put(&doc_version, lens_doc).await {
                        if let Some(Ok(migrated_lens_doc)) = lens.next().await {
                            let mut migrated = document::Document::new();
                            if let Some(id) = doc.id() {
                                migrated.set_id(id.clone());
                            }
                            for (field_name, value) in migrated_lens_doc {
                                if field_name != DOC_ID_FIELD {
                                    // Convert JSON value to native type based on schema field kind
                                    let native_value = json_to_native_value(&value, &field_name, collection.schema());
                                    migrated.set(&field_name, native_value);
                                }
                            }
                            migrated.set_schema_version_id(&target_version_id);
                            migrated_docs.push(migrated);
                            continue;
                        }
                    }
                }

                // If migration fails for a doc, keep original
                migrated_docs.push(doc);
            }

            // Rebuild indexes: drop all entries, re-index from migrated documents
            let index_manager = IndexManager::from_collection(short_id, collection.schema())
                .map_err(|e| Error::Other(format!("failed to create index manager: {}", e)))?;

            for index_desc in collection.get_indexes() {
                // Drop existing entries
                if let Some(index) = index_manager.get_index(&index_desc.name) {
                    index
                        .remove_all(&mut datastore.clone())
                        .await
                        .map_err(Error::Storage)?;
                }

                // Bulk re-index with migrated documents
                index_manager
                    .bulk_index(&datastore, &index_desc.name, &migrated_docs, collection.schema())
                    .await?;
            }

            tracing::debug!(
                collection = %collection_name,
                doc_count = migrated_docs.len(),
                index_count = collection.get_indexes().len(),
                "Rebuilt indexes after migration"
            );
        } // datastore reference dropped here

        write_txn.commit().await?;

        Ok(())
    }

    /// Check if a migration exists between two schema versions.
    pub fn has_migration(&self, transform_id: &TransformId) -> bool {
        self.lens_store.has_transform(transform_id)
    }

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
            let col_short_id =
                crate::collection::collection_short_id(collection_id.as_str());
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
        let mut sorted_fields: Vec<&schema::FieldDescription> = schema
            .fields
            .iter()
            .filter(|f| !f.id.is_empty())
            .collect();
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
    async fn get_collection_by_version_id_full(
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
    async fn collection_has_data(&self, collection_id: &str) -> Result<bool> {
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

    /// Patch a collection's schema using JSON patch operations.
    ///
    /// This creates a new schema version with a new version_id (CID) and links
    /// it to the previous version via `previous_version`. The old version is
    /// marked as inactive.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - The name of the collection to patch
    /// * `patch` - A JSON patch string (RFC 6902 format)
    ///
    /// # Returns
    ///
    /// The updated collection version (with new version_id).
    ///
    /// # Errors
    ///
    /// - `CollectionNotFound` if the collection doesn't exist
    /// - `InvalidPatch` if the patch is invalid or cannot be applied
    /// - `Schema` if the resulting schema is invalid
    pub async fn patch_collection(
        &self,
        collection_name: &str,
        patch: &str,
    ) -> Result<CollectionVersion> {
        // Parse the patch early - needed for both collection lookup fallbacks and processing
        let patch_ops: serde_json::Value =
            serde_json::from_str(patch).map_err(|e| Error::InvalidPatch(e.to_string()))?;

        // Get the current schema - try by name first, then by version ID (including KV store),
        // then check for collection-level move/copy targeting a non-existent collection
        let collection = match self.get_collection(collection_name)? {
            Some(c) => c,
            None => {
                // Try looking up by version ID - search both cache and KV store
                match self
                    .get_collection_by_version_id_full(collection_name)
                    .await?
                {
                    Some(c) => c,
                    None => {
                        // Collection not found by name or version ID.
                        // Check if the patch is a collection-level move/copy where the
                        // "path" targets a non-existent collection (e.g., move /Users → /Books)
                        return self
                            .handle_unknown_collection_patch(collection_name, &patch_ops)
                            .await;
                    }
                }
            }
        };

        let old_schema = collection.schema().clone();
        let actual_name = old_schema.name.clone();
        let old_version_id = old_schema.version_id.clone();
        let collection_id = old_schema.collection_id.clone();

        // Collect known collection names for Kind validation
        let known_collection_names: Vec<String> = self
            .list_collections()
            .unwrap_or_default()
            .into_iter()
            .collect();

        // Apply the patch to the schema JSON
        let mut schema_json = serde_json::to_value(&old_schema).map_err(|e| {
            Error::Serialization(format!("failed to serialize schema to JSON: {}", e))
        })?;

        // Ensure optional array fields are present in JSON even when empty.
        // Go always serializes these as null/empty arrays, but Rust's
        // skip_serializing_if omits them. Patches targeting these paths
        // (e.g., /VectorEmbeddings/-) need the key to exist.
        // Note: EncryptedIndexes is NOT pre-populated because Go doesn't expose
        // it in the JSON representation - patches targeting it should fail.
        if let serde_json::Value::Object(ref mut map) = schema_json {
            for key in &["Indexes", "VectorEmbeddings"] {
                map.entry(key.to_string())
                    .or_insert(serde_json::Value::Array(vec![]));
            }
        }

        // Apply JSON patch operations
        // Go DefraDB embeds collection name in patch paths: /CollectionName/Fields/-
        // We need to strip the collection name prefix to get paths relative to schema
        // Use both the passed-in name and the actual collection name for prefix matching
        let collection_prefix = format!("/{}/", collection_name);
        let actual_name_prefix = if actual_name != collection_name {
            Some(format!("/{}/", actual_name))
        } else {
            None
        };

        // Track whether the patch deactivates this collection or explicitly changes IsActive.
        // These require in-place updates rather than new version creation.
        let mut is_deactivation = false;
        let mut is_active_explicitly_set = false;

        if let serde_json::Value::Array(ops) = patch_ops {
            for op in ops {
                let operation = op.get("op").and_then(|v| v.as_str());
                let raw_path = op.get("path").and_then(|v| v.as_str());
                let value = op.get("value");

                // Strip collection name/version prefix from path if present (Go compatibility)
                let stripped_path = raw_path.map(|p| {
                    Self::strip_collection_prefix(
                        p,
                        &collection_prefix,
                        actual_name_prefix.as_deref(),
                    )
                });

                // Extract field name from path before substitution (for name mismatch validation)
                let field_name_from_path = stripped_path
                    .as_deref()
                    .and_then(|p| Self::extract_field_name_from_path(p));

                // Go compatibility: substitute field names for indices in /Fields/<name> paths
                let path =
                    stripped_path.map(|p| Self::substitute_field_name_in_path(&p, &schema_json));

                match (operation, path.as_deref()) {
                    (Some("replace"), Some(path)) | (Some("add"), Some(path)) => {
                        let mut value = value
                            .ok_or_else(|| {
                                Error::InvalidPatch(format!(
                                    "missing 'value' for operation at {}",
                                    path
                                ))
                            })?
                            .clone();

                        // Go compatibility: root-level add/replace is "adding collections"
                        if path == "/" {
                            let name = value
                                .as_object()
                                .and_then(|m| m.get("Name"))
                                .and_then(|n| n.as_str())
                                .unwrap_or(&actual_name);
                            return Err(Error::InvalidPatch(format!(
                                "adding collections via patch is not supported. Name: {}",
                                name
                            )));
                        }

                        // Track explicit IsActive changes for in-place update handling
                        if path == "/IsActive" {
                            is_active_explicitly_set = true;
                        }

                        // Go compatibility: validate VersionID replacement
                        if path.ends_with("/VersionID") {
                            let version_id_str = value.as_str().unwrap_or("");
                            if version_id_str.is_empty() {
                                return Err(Error::InvalidPatch(
                                    "collection ID cannot be empty".to_string(),
                                ));
                            }
                            // Validate CID format
                            if cid::Cid::try_from(version_id_str).is_err() {
                                return Err(Error::InvalidPatch(format!(
                                    "invalid cid: selected encoding not supported. VersionID: {}",
                                    version_id_str
                                )));
                            }
                            // Check if this CID exists as a known collection version
                            let all_versions =
                                self.get_all_collection_versions().await.unwrap_or_default();
                            let is_known =
                                all_versions.iter().any(|c| c.version_id == version_id_str);
                            if !is_known {
                                return Err(Error::InvalidPatch(
                                    "unknown CID, collection ids cannot be manually defined"
                                        .to_string(),
                                ));
                            }
                            // Known CIDs proceed - sources/ownership validation
                            // is handled by definition_validation post-patch.
                        }

                        // Go compatibility: validate and auto-generate FieldID when adding new fields
                        // If path ends with /Fields/- or /Fields/<n> and value has Name but no FieldID
                        if path.contains("/Fields/") {
                            if let serde_json::Value::Object(ref mut map) = value {
                                // Validate field name matches path index name (Go compatibility)
                                if let Some(ref path_name) = field_name_from_path {
                                    if let Some(value_name) =
                                        map.get("Name").and_then(|n| n.as_str())
                                    {
                                        if !value_name.is_empty()
                                            && value_name != path_name.as_str()
                                        {
                                            return Err(Error::InvalidPatch(format!(
                                                "the index used does not match the given name. index: {}, name: {}",
                                                path_name, value_name
                                            )));
                                        }
                                    }
                                    // If value doesn't have Name, set it from the path
                                    if !map.contains_key("Name") {
                                        map.insert(
                                            "Name".to_string(),
                                            serde_json::Value::String(path_name.clone()),
                                        );
                                    }
                                }

                                // Validate Kind value for new fields
                                if let Some(kind_val) = map.get("Kind") {
                                    Self::validate_patch_field_kind(
                                        kind_val,
                                        map.get("Name").and_then(|n| n.as_str()).unwrap_or(""),
                                        &known_collection_names,
                                    )?;
                                }
                                if map.contains_key("Name") && !map.contains_key("FieldID") {
                                    // Find max existing FieldID to avoid collisions with gaps
                                    let max_field_id = schema_json
                                        .get("Fields")
                                        .and_then(|f| f.as_array())
                                        .map(|arr| {
                                            arr.iter()
                                                .filter_map(|f| f.get("FieldID"))
                                                .filter_map(|id| {
                                                    id.as_str()
                                                        .and_then(|s| s.parse::<u64>().ok())
                                                        .or_else(|| id.as_u64())
                                                })
                                                .max()
                                                .unwrap_or(0)
                                        })
                                        .unwrap_or(0);
                                    let field_id = (max_field_id + 1).to_string();
                                    map.insert("FieldID".to_string(), field_id.into());
                                }
                            }
                        }

                        // Go compatibility: For top-level array fields that don't exist (like
                        // EncryptedIndexes which isn't exposed in Go's JSON), produce Go-compatible
                        // error messages.
                        let is_top_level_path =
                            path.starts_with('/') && !path[1..].contains('/');
                        if is_top_level_path {
                            let key = &path[1..];
                            let key_exists = schema_json
                                .as_object()
                                .map(|m| m.contains_key(key))
                                .unwrap_or(false);
                            if !key_exists {
                                // For add with array value, Go produces unmarshal error
                                if operation == Some("add") && value.is_array() {
                                    return Err(Error::InvalidPatch(
                                        "cannot unmarshal array into Go value".to_string(),
                                    ));
                                }
                                // For replace on non-existent key, Go produces "doc is missing key"
                                if operation == Some("replace") {
                                    return Err(Error::InvalidPatch(
                                        "doc is missing key".to_string(),
                                    ));
                                }
                            }
                        }

                        Self::json_pointer_set(&mut schema_json, path, value)?;
                    }
                    (Some("remove"), Some(path)) => {
                        if path == "/" {
                            // Root-level remove = deactivate collection
                            is_deactivation = true;
                        } else {
                            // Go compatibility: For top-level keys that don't exist (like
                            // EncryptedIndexes), produce Go-compatible error message.
                            let is_top_level_path =
                                path.starts_with('/') && !path[1..].contains('/');
                            if is_top_level_path {
                                let key = &path[1..];
                                let key_exists = schema_json
                                    .as_object()
                                    .map(|m| m.contains_key(key))
                                    .unwrap_or(false);
                                if !key_exists {
                                    return Err(Error::InvalidPatch(
                                        "unable to remove nonexistent key".to_string(),
                                    ));
                                }
                            }
                            Self::json_pointer_remove(&mut schema_json, path)?;
                        }
                    }
                    (Some("test"), Some(path)) => {
                        // RFC 6902 "test" operation: verify value at path equals expected
                        let expected_value = value
                            .ok_or_else(|| {
                                Error::InvalidPatch(format!(
                                    "missing 'value' for test operation at {}",
                                    path
                                ))
                            })?
                            .clone();

                        // Get the actual value at the path
                        let actual_value = Self::json_pointer_get(&schema_json, path);

                        // Compare: if path doesn't exist or values don't match, test fails
                        match actual_value {
                            Some(actual) if actual == expected_value => {
                                // Test passes - continue to next operation
                            }
                            _ => {
                                // Test fails - return error in Go-compatible format
                                // Include original path for context
                                let original_path = raw_path.unwrap_or(path);
                                return Err(Error::InvalidPatch(format!(
                                    "testing value {} failed: test failed",
                                    original_path
                                )));
                            }
                        }
                    }
                    (Some("copy"), Some(path)) => {
                        // RFC 6902 "copy" operation: copy value from "from" to "path"
                        let from_path =
                            op.get("from").and_then(|v| v.as_str()).ok_or_else(|| {
                                Error::InvalidPatch(format!(
                                    "missing 'from' for copy operation at {}",
                                    path
                                ))
                            })?;

                        // Go compatibility: copying collection-level is not supported
                        // This includes copying to root "/" or to paths that would create new collections
                        // Detect by checking if path doesn't contain /Fields (field-level operations)
                        if path == "/"
                            || (!path.contains("/Fields")
                                && !path.contains("/Name")
                                && !path.contains("/IsActive"))
                        {
                            // Extract the target name from the raw path for the error message
                            let target_name = raw_path
                                .and_then(|p| {
                                    let p = p.trim_start_matches('/');
                                    p.split('/').next()
                                })
                                .unwrap_or("Unknown");
                            return Err(Error::InvalidPatch(format!(
                                "adding collections via patch is not supported. Name: {}",
                                target_name
                            )));
                        }

                        // Substitute field names in from path too
                        let from_path =
                            Self::substitute_field_name_in_path(from_path, &schema_json);
                        // Strip collection prefix from "from" path if present
                        let from_path = Self::strip_collection_prefix(
                            &from_path,
                            &collection_prefix,
                            actual_name_prefix.as_deref(),
                        );

                        // Get the value to copy
                        let value_to_copy = Self::json_pointer_get(&schema_json, &from_path)
                            .ok_or_else(|| {
                                Error::InvalidPatch(format!("path not found: {}", from_path))
                            })?;

                        // Set at destination
                        Self::json_pointer_set(&mut schema_json, path, value_to_copy)?;
                    }
                    (Some("move"), Some(path)) => {
                        // RFC 6902 "move" operation: move value from "from" to "path"
                        let from_path =
                            op.get("from").and_then(|v| v.as_str()).ok_or_else(|| {
                                Error::InvalidPatch(format!(
                                    "missing 'from' for move operation at {}",
                                    path
                                ))
                            })?;

                        // Go compatibility: moving at collection-level is a no-op
                        // This includes moving to root "/" or paths that would move entire collections
                        // Detect by checking if path doesn't contain /Fields (field-level operations)
                        if path == "/"
                            || (!path.contains("/Fields")
                                && !path.contains("/Name")
                                && !path.contains("/IsActive"))
                        {
                            // Skip this operation - collection-level moves are no-ops
                            continue;
                        }

                        // Substitute field names in from path too
                        let from_path =
                            Self::substitute_field_name_in_path(from_path, &schema_json);
                        // Strip collection prefix from "from" path if present
                        let from_path = Self::strip_collection_prefix(
                            &from_path,
                            &collection_prefix,
                            actual_name_prefix.as_deref(),
                        );

                        // Get the value to move
                        let value_to_move = Self::json_pointer_get(&schema_json, &from_path)
                            .ok_or_else(|| {
                                Error::InvalidPatch(format!("path not found: {}", from_path))
                            })?;

                        // Remove from source first
                        Self::json_pointer_remove(&mut schema_json, &from_path)?;

                        // Set at destination
                        Self::json_pointer_set(&mut schema_json, path, value_to_move)?;
                    }
                    _ => {
                        return Err(Error::InvalidPatch(format!(
                            "unsupported or invalid patch operation: {:?}",
                            op
                        )));
                    }
                }
            }
        } else {
            return Err(Error::InvalidPatch(
                "patch must be an array of operations".to_string(),
            ));
        }

        // Go compatibility: auto-generate FieldID for any fields missing one
        // This handles cases where FieldID is removed (e.g., after copy operation)
        if let Some(fields) = schema_json.get_mut("Fields").and_then(|f| f.as_array_mut()) {
            // Find max existing FieldID
            let max_field_id: u64 = fields
                .iter()
                .filter_map(|f| f.get("FieldID"))
                .filter_map(|id| {
                    id.as_str()
                        .and_then(|s| s.parse::<u64>().ok())
                        .or_else(|| id.as_u64())
                })
                .max()
                .unwrap_or(0);

            let mut next_id = max_field_id + 1;
            for field in fields.iter_mut() {
                if let serde_json::Value::Object(ref mut map) = field {
                    if !map.contains_key("FieldID")
                        || map.get("FieldID") == Some(&serde_json::Value::Null)
                        || map.get("FieldID").and_then(|v| v.as_str()) == Some("")
                    {
                        map.insert("FieldID".to_string(), next_id.to_string().into());
                        next_id += 1;
                    }
                }
            }
        }

        // Go compatibility: check for empty collection name before deserialization
        let name_value = schema_json.get("Name");
        match name_value {
            None | Some(serde_json::Value::Null) => {
                return Err(Error::InvalidPatch(
                    "collection name can't be empty".to_string(),
                ));
            }
            Some(serde_json::Value::String(s)) if s.is_empty() => {
                return Err(Error::InvalidPatch(
                    "collection name can't be empty".to_string(),
                ));
            }
            _ => {}
        }

        // Field movement and duplicate checks are handled by the definition validators
        // (validate_field_not_moved, validate_field_not_duplicated) which run post-deserialization.
        // This matches Go's approach of collecting ALL validation errors at once.

        // Deserialize back to CollectionVersion
        let mut new_schema: CollectionVersion = serde_json::from_value(schema_json)
            .map_err(|e| Error::InvalidPatch(format!("invalid resulting schema: {}", e)))?;

        // Go compatibility: check for removed/empty required fields after deserialization.
        // Go's JSON unmarshaling uses zero values for missing fields; our serde defaults
        // replicate this. Now check for invalid empty values that indicate patch corruption.
        if new_schema.version_id.is_empty() {
            return Err(Error::InvalidPatch(
                "invalid cid: cid too short. VersionID: ".to_string(),
            ));
        }

        // Check for field-level corruption from patches (empty names from removed fields)
        {
            let old_field_names: std::collections::HashSet<&str> =
                old_schema.fields.iter().map(|f| f.name.as_str()).collect();
            let mut field_errors = Vec::new();

            for field in &new_schema.fields {
                if field.name.is_empty() {
                    if old_field_names.contains("") {
                        // This shouldn't happen - old field shouldn't have empty name
                        field_errors.push(
                            "Names must match /^[_a-zA-Z][_a-zA-Z0-9]*$/ but '' was found"
                                .to_string(),
                        );
                    } else {
                        // A field name was removed by the patch
                        field_errors.push(
                            "mutating an existing field is not supported. ProposedName: "
                                .to_string(),
                        );
                    }
                }
            }

            if !field_errors.is_empty() {
                return Err(Error::InvalidPatch(field_errors.join("\n")));
            }
        }

        // Go compatibility: auto-generate _fieldID for foreign object fields added via patch.
        // This matches Go's collection_define.go behavior for fields with Kind.IsObject() && !Kind.IsArray().
        {
            let max_field_id: u64 = new_schema
                .fields
                .iter()
                .filter_map(|f| f.id.parse::<u64>().ok())
                .max()
                .unwrap_or(0);
            let mut next_id = max_field_id + 1;
            new_schema
                .add_relation_id_fields(|| {
                    let id = next_id.to_string();
                    next_id += 1;
                    id
                })
                .map_err(|e| Error::InvalidPatch(format!("failed to add relation id fields: {}", e)))?;
        }

        // Handle in-place updates (deactivation, IsActive-only, or PreviousVersion/Transform-only).
        // These don't create a new schema version - they update the existing one.
        let is_isactive_only_change = is_active_explicitly_set
            && new_schema.fields == old_schema.fields
            && new_schema.name == old_schema.name;

        // Check if only PreviousVersion/Transform changed (lens migration linking).
        // This is an in-place update that adds a migration transform to an existing version.
        let is_transform_only_change = !is_deactivation
            && !is_active_explicitly_set
            && new_schema.fields == old_schema.fields
            && new_schema.name == old_schema.name
            && new_schema.is_active == old_schema.is_active
            && new_schema.previous_version != old_schema.previous_version;

        if is_deactivation || is_isactive_only_change || is_transform_only_change {
            if is_deactivation {
                new_schema.is_active = false;
            }
            // Keep original version_id
            new_schema.version_id = old_version_id.clone();
            // For IsActive-only or deactivation, restore original previous_version.
            // For Transform-only changes, keep the new previous_version (contains the transform).
            if !is_transform_only_change {
                new_schema.previous_version = old_schema.previous_version.clone();
            }

            // Validate: can't remove a version that is a dependency of another version
            // This check runs always for deactivation (even if already inactive),
            // matching Go's validateCollectionDoesNotHaveHigherVersion
            if is_deactivation {
                let all_versions = self.get_all_collection_versions().await?;
                for other in &all_versions {
                    if let Some(ref prev) = other.previous_version {
                        if prev.source_collection_id == old_version_id {
                            return Err(Error::InvalidPatch(
                                "cannot delete a version that is used by a newer version, first delete the new version".to_string(),
                            ));
                        }
                    }
                }
            }

            // Validate: can't remove a collection that has documents (only on active→inactive)
            if !new_schema.is_active && old_schema.is_active {
                let has_data = self.collection_has_data(&collection_id).await?;
                if has_data {
                    return Err(Error::InvalidPatch(
                        "cannot delete a collection that has documents, first delete the documents and then delete the version".to_string(),
                    ));
                }
            }

            // Run cross-collection validators to catch issues like multiple active versions
            let all_existing = self.get_all_collection_versions().await?;
            let new_collections: Vec<CollectionVersion> = all_existing
                .iter()
                .filter(|c| c.version_id != old_version_id)
                .cloned()
                .chain(std::iter::once(new_schema.clone()))
                .collect();
            crate::definition_validation::validate_collection_changes(
                &all_existing,
                &new_collections,
            )
            .map_err(Error::InvalidPatch)?;

            // Store the updated version
            let txn = self.new_txn(false).await?;
            {
                let systemstore = txn.systemstore()?;
                let key = CollectionKey::new(&old_version_id);
                let data = serde_json::to_vec(&new_schema).map_err(|e| {
                    Error::Serialization(format!(
                        "failed to serialize updated schema version '{}': {}",
                        old_version_id, e
                    ))
                })?;
                systemstore
                    .set(&key.bytes(), &data)
                    .await
                    .map_err(Error::Storage)?;

                // Update name pointer based on activation state
                let name_key = CollectionNameKey::new(&actual_name);
                if new_schema.is_active {
                    systemstore
                        .set(&name_key.bytes(), old_version_id.as_bytes())
                        .await
                        .map_err(Error::Storage)?;
                } else {
                    systemstore
                        .delete(&name_key.bytes())
                        .await
                        .map_err(Error::Storage)?;
                }
            }
            txn.commit().await?;

            // Update cache
            let mut cache = self.collections.write().map_err(|e| {
                tracing::error!(error = ?e, "Collection cache lock poisoned during in-place update");
                Error::CacheUpdateFailedAfterCommit(actual_name.clone())
            })?;
            if new_schema.is_active {
                cache.insert(actual_name.clone(), Collection::new(new_schema.clone()));
            } else {
                cache.remove(&actual_name);
            }

            tracing::info!(
                collection = %actual_name,
                version = %old_version_id,
                is_active = new_schema.is_active,
                "Updated collection version in place"
            );

            return Ok(new_schema);
        }

        // --- Normal path: create a new schema version ---

        // Go compatibility: default new fields with CType::None to CType::LwwRegister.
        // Go's patchCollection does this in collection_define.go for new fields that
        // don't have an explicit CRDT type. This must happen before CID generation.
        {
            let old_field_names: std::collections::HashSet<&str> =
                old_schema.fields.iter().map(|f| f.name.as_str()).collect();
            for field in &mut new_schema.fields {
                if !old_field_names.contains(field.name.as_str())
                    && field.crdt_type == schema::CType::None
                {
                    field.crdt_type = schema::CType::LwwRegister;
                }
            }
        }

        // Run Go-compatible cross-collection validators (before schema validate() which
        // uses different error messages). These validators cover duplicate fields,
        // CRDT/kind compatibility, and all Go-specific patch constraints.
        let all_existing = self.get_all_collection_versions().await?;
        let new_collections: Vec<CollectionVersion> = all_existing
            .iter()
            .filter(|c| c.version_id != old_version_id)
            .cloned()
            .chain(std::iter::once(new_schema.clone()))
            .collect();
        crate::definition_validation::validate_collection_changes(&all_existing, &new_collections)
            .map_err(Error::InvalidPatch)?;

        // Also run schema-level validation for checks not covered by definition validators
        // (e.g., relation field requires relation_name, policy format validation)
        new_schema.validate()?;

        // Compute version depth: count existing versions for this collection_id
        let version_depth = all_existing
            .iter()
            .filter(|c| c.collection_id == collection_id)
            .count() as u64;

        // Generate new version_id from schema content with proper priorities
        let new_version_id =
            Self::generate_patch_version_id(&mut new_schema, &old_schema, version_depth);

        // Update new schema with version info
        new_schema.version_id = new_version_id.clone();
        new_schema.previous_version = Some(CollectionSource::new(&old_version_id));

        // Check for pending migrations targeting this new version
        {
            let pending = self.pending_migrations.read().map_err(|e| {
                tracing::error!(error = ?e, "Pending migrations lock poisoned");
                Error::LockPoisoned(
                    "pending migrations lock poisoned during patch_collection".into(),
                )
            })?;
            if let Some((_source_id, transform_id)) = pending.get(&new_version_id) {
                if let Some(ref mut prev) = new_schema.previous_version {
                    prev.transform = Some(transform_id.clone());
                    tracing::debug!(
                        new_version = %new_version_id,
                        transform_id = %transform_id,
                        "Linked pending migration to new schema version"
                    );
                }
            }
        }

        // Go compatibility: respect explicit IsActive=false in the patch, otherwise default to true.
        // When IsActive was explicitly set to false in the patch, preserve it.
        // When the new version is inactive, keep the old version active.
        if !is_active_explicitly_set {
            new_schema.is_active = true;
        }

        // Create old schema copy for storage. If new schema is active, mark old as inactive.
        // If new schema is inactive (explicit IsActive=false), old version stays active.
        let mut old_schema_inactive = old_schema.clone();
        if new_schema.is_active {
            old_schema_inactive.is_active = false;
        }

        tracing::info!(
            collection = %collection_name,
            old_version = %old_version_id,
            new_version = %new_version_id,
            field_count = new_schema.fields.len(),
            "Creating new schema version"
        );

        // Begin transaction to store all version data
        let txn = self.new_txn(false).await?;

        // Prepare serialized data before getting systemstore reference
        let old_version_key = CollectionKey::new(&old_version_id);
        let old_version_data = serde_json::to_vec(&old_schema_inactive).map_err(|e| {
            Error::Serialization(format!(
                "failed to serialize old schema version '{}': {}",
                old_version_id, e
            ))
        })?;
        let new_version_key = CollectionKey::new(&new_version_id);
        let new_version_data = serde_json::to_vec(&new_schema).map_err(|e| {
            Error::Serialization(format!(
                "failed to serialize new schema version '{}': {}",
                new_version_id, e
            ))
        })?;
        let name_key = CollectionNameKey::new(collection_name);
        let version_index_key = CollectionVersionKey::new(&collection_id, &new_version_id);
        let old_version_index_key = CollectionVersionKey::new(&collection_id, &old_version_id);

        // Perform all writes in a scoped block so systemstore reference is dropped
        {
            let systemstore = txn.systemstore()?;

            // 1. Store old version at /collection/id/{old_version_id} with is_active = false
            systemstore
                .set(&old_version_key.bytes(), &old_version_data)
                .await
                .map_err(Error::Storage)?;

            // 2. Store new version at /collection/id/{new_version_id}
            systemstore
                .set(&new_version_key.bytes(), &new_version_data)
                .await
                .map_err(Error::Storage)?;

            // 3. Update /collection/name/{name} - only point to new version if it's active.
            // If new version is inactive, keep name pointing to old version (which stays active).
            if new_schema.is_active {
                systemstore
                    .set(&name_key.bytes(), new_version_id.as_bytes())
                    .await
                    .map_err(Error::Storage)?;
            }

            // 4. Add version index at /collection/version/{collection_id}/{new_version_id}
            systemstore
                .set(&version_index_key.bytes(), b"1")
                .await
                .map_err(Error::Storage)?;

            // 5. Also ensure old version is in the version index (may already exist)
            systemstore
                .set(&old_version_index_key.bytes(), b"1")
                .await
                .map_err(Error::Storage)?;
        } // systemstore reference dropped here

        txn.commit().await?;

        // Clean up any pending migration that was linked to this version
        {
            let mut pending = self.pending_migrations.write().map_err(|e| {
                tracing::error!(error = ?e, "Pending migrations lock poisoned during cleanup");
                Error::CacheUpdateFailedAfterCommit(collection_name.to_string())
            })?;
            pending.remove(&new_version_id);
        }

        // Update cache based on which version is active
        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(
                error = ?e,
                collection_name = %collection_name,
                "Collection cache lock poisoned during patch_collection update"
            );
            Error::CacheUpdateFailedAfterCommit(collection_name.to_string())
        })?;
        if new_schema.is_active {
            // New version is active - cache it
            cache.insert(
                collection_name.to_string(),
                Collection::new(new_schema.clone()),
            );
        }
        // If new version is inactive, old version stays in cache (already there)

        Ok(new_schema)
    }

    /// Generate a version ID (CID) from schema content during patching.
    ///
    /// Matches Go DefraDB's saveBlocks() behavior:
    /// - Existing fields (present in old_schema) are SKIPPED entirely
    /// - Only NEW fields get CIDs generated with priority=1 (empty headstore)
    /// - The collection block gets priority=version_depth+1, heads=[old_version_CID],
    ///   and links containing only new field CIDs
    fn generate_patch_version_id(
        schema: &mut CollectionVersion,
        old_schema: &CollectionVersion,
        version_depth: u64,
    ) -> String {
        use cid::Cid;
        use sha2::{Digest, Sha256};
        use std::str::FromStr;

        let collection_priority = version_depth + 1;

        // Build set of old field names for detecting which fields are new
        let old_field_names: std::collections::HashSet<&str> = old_schema
            .fields
            .iter()
            .filter(|f| !f.id.is_empty())
            .map(|f| f.name.as_str())
            .collect();

        // Go's saveBlocks skips fields that already have a FieldID (existing fields).
        // Only NEW fields (not in old_schema) get CID generation and DAGLink inclusion.
        // Go also skips secondary relation fields (those have empty FieldID in old schema too,
        // but Delta returns hasFieldChanged=false for them).
        let new_field_indices: Vec<usize> = {
            let mut indices: Vec<usize> = schema
                .fields
                .iter()
                .enumerate()
                .filter(|(_, f)| {
                    // New field: not in old schema and not a secondary relation
                    // (secondary relations have relation_name set and is_primary=false)
                    let is_new = !old_field_names.contains(f.name.as_str());
                    let is_secondary_relation = f.relation_name.is_some() && !f.is_primary;
                    is_new && !is_secondary_relation
                })
                .map(|(i, _)| i)
                .collect();
            // Sort: _docID first, then alphabetically
            indices.sort_by(|&a, &b| {
                let fa = &schema.fields[a];
                let fb = &schema.fields[b];
                if fa.name == "_docID" {
                    std::cmp::Ordering::Less
                } else if fb.name == "_docID" {
                    std::cmp::Ordering::Greater
                } else {
                    fa.name.cmp(&fb.name)
                }
            });
            indices
        };

        // Generate CIDs only for NEW fields with priority=1 (matching Go's empty headstore)
        let mut field_cids: Vec<Cid> = Vec::new();
        for &idx in &new_field_indices {
            let field = &schema.fields[idx];
            match schema::generate_field_cid_with_priority(field, 1) {
                Ok(cid) => {
                    schema.fields[idx].id = cid.to_string();
                    field_cids.push(cid);
                }
                Err(_e) => {}
            }
        }

        // Generate collection CID with old version as head.
        // Go's Delta only includes name when it changed. For field-only patches, name=None.
        let name_changed = schema.name != old_schema.name;
        let collection_name = if name_changed {
            Some(schema.name.as_str())
        } else {
            None
        };
        let old_version_cid = Cid::from_str(&old_schema.version_id).ok();
        let collection_heads: Vec<Cid> = old_version_cid.into_iter().collect();
        match schema::generate_collection_cid_full(
            collection_name,
            &field_cids,
            collection_priority,
            &collection_heads,
        ) {
            Ok(cid) => cid.to_string(),
            Err(_) => {
                // Fallback to simple hash if CID generation fails
                let mut hasher = Sha256::new();
                hasher.update(b"version:");
                hasher.update(schema.name.as_bytes());
                for field in &schema.fields {
                    hasher.update(field.name.as_bytes());
                    hasher.update(field.id.as_bytes());
                }
                let hash = hasher.finalize();
                format!(
                    "v{:x}",
                    &hash[..8].iter().fold(0u64, |acc, &b| (acc << 8) | b as u64)
                )
            }
        }
    }

    /// Strip collection name or version ID prefix from a path.
    ///
    /// Handles both the collection_name prefix (e.g., "/Users/") and the
    /// actual_name prefix (when looked up by version ID, the passed-in name
    /// differs from the real collection name).
    /// Validate a Kind value in a patch field addition.
    /// Returns error if the Kind is an unsupported numeric value or unknown string.
    fn validate_patch_field_kind(
        kind_val: &serde_json::Value,
        field_name: &str,
        known_collections: &[String],
    ) -> Result<()> {
        match kind_val {
            serde_json::Value::Number(n) => {
                let kind_num = n.as_u64().unwrap_or(0) as u8;
                // Valid numeric kinds: 1-14, 18-22 (0 is None, only for internal _docID)
                let valid = matches!(
                    kind_num,
                    1..=14 | 18..=22
                );
                if !valid {
                    return Err(Error::InvalidPatch(format!(
                        "no type found for given name. Type: {}",
                        kind_num
                    )));
                }
                Ok(())
            }
            serde_json::Value::String(s) => {
                // Known string kinds
                let known = matches!(
                    s.as_str(),
                    "ID" | "Boolean"
                        | "Int"
                        | "DateTime"
                        | "Float"
                        | "Float64"
                        | "Float32"
                        | "String"
                        | "Blob"
                        | "JSON"
                        | "[Boolean]"
                        | "[Boolean!]"
                        | "[Int]"
                        | "[Int!]"
                        | "[Float]"
                        | "[Float64]"
                        | "[Float!]"
                        | "[Float64!]"
                        | "[Float32]"
                        | "[Float32!]"
                        | "[String]"
                        | "[String!]"
                        | "Self"
                        | "[Self]"
                );
                if !known {
                    // Could be a collection name reference (e.g., "Users", "[Users]").
                    let ref_name = s
                        .strip_prefix('[')
                        .and_then(|s| s.strip_suffix(']'))
                        .unwrap_or(s.as_str());
                    if !known_collections.iter().any(|c| c == ref_name) {
                        return Err(Error::InvalidPatch(format!(
                            "no type found for given name. Field: {}, Kind: {}",
                            field_name, s
                        )));
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn strip_collection_prefix(
        path: &str,
        collection_prefix: &str,
        actual_name_prefix: Option<&str>,
    ) -> String {
        // Go DefraDB accepts paths with or without leading '/'.
        // Generate both variants for matching.
        let no_slash_prefix = collection_prefix.trim_start_matches('/');

        if path.starts_with(collection_prefix) {
            format!("/{}", &path[collection_prefix.len()..])
        } else if path.starts_with(no_slash_prefix) {
            // Handle paths without leading '/' (e.g., "User/Indexes/-")
            format!("/{}", &path[no_slash_prefix.len()..])
        } else {
            // Also handle exact match without trailing slash (collection-level operations).
            // E.g., path="/Users" with prefix="/Users/" → "/"
            let exact = collection_prefix.trim_end_matches('/');
            let exact_no_slash = exact.trim_start_matches('/');
            if path == exact || path == exact_no_slash {
                return "/".to_string();
            }
            if let Some(anp) = actual_name_prefix {
                let anp_no_slash = anp.trim_start_matches('/');
                if path.starts_with(anp) {
                    format!("/{}", &path[anp.len()..])
                } else if path.starts_with(anp_no_slash) {
                    format!("/{}", &path[anp_no_slash.len()..])
                } else {
                    let anp_exact = anp.trim_end_matches('/');
                    let anp_exact_no_slash = anp_exact.trim_start_matches('/');
                    if path == anp_exact || path == anp_exact_no_slash {
                        return "/".to_string();
                    }
                    path.to_string()
                }
            } else {
                path.to_string()
            }
        }
    }

    /// Handle patches targeting a collection that doesn't exist by name or version ID.
    ///
    /// This handles several cases:
    /// 1. Schema field names (EncryptedIndexes, Indexes, etc.) - produce JSON patch errors
    ///    because these fields don't exist in Go's JSON representation
    /// 2. Collection-level copy where the "path" targets a new collection name
    ///    (e.g., copy from /Users to /Book) → returns "adding collections not supported"
    /// 3. Collection-level move to a new name (no-op in Go) → finds source via "from"
    ///    and returns the original schema unchanged
    async fn handle_unknown_collection_patch(
        &self,
        collection_name: &str,
        patch_ops: &serde_json::Value,
    ) -> Result<CollectionVersion> {
        // Schema field names that don't exist in Go's JSON representation
        // When the "collection name" is actually one of these, produce Go-compatible
        // JSON patch errors instead of "adding collections" errors.
        const SCHEMA_FIELDS: &[&str] = &[
            "EncryptedIndexes",
            "VectorEmbeddings",
            "Indexes",
            "Fields",
            "Policy",
        ];

        // Check if the "collection name" is actually a schema field
        if SCHEMA_FIELDS.contains(&collection_name) {
            if let serde_json::Value::Array(ops) = patch_ops {
                for op in ops {
                    let operation = op.get("op").and_then(|v| v.as_str());
                    let value = op.get("value");

                    match operation {
                        Some("add") => {
                            // For add with array value, Go produces unmarshal error
                            if value.map(|v| v.is_array()).unwrap_or(false) {
                                return Err(Error::InvalidPatch(
                                    "cannot unmarshal array into Go value".to_string(),
                                ));
                            }
                            return Err(Error::InvalidPatch(
                                "cannot unmarshal array into Go value".to_string(),
                            ));
                        }
                        Some("remove") => {
                            return Err(Error::InvalidPatch(
                                "unable to remove nonexistent key".to_string(),
                            ));
                        }
                        Some("replace") => {
                            return Err(Error::InvalidPatch("doc is missing key".to_string()));
                        }
                        _ => {}
                    }
                }
            }
        }

        // Try to extract the actual collection name from the patch value's Name field.
        // This handles cases like path "/-" where the collection name in the path is "-"
        // but the actual name is in the value object.
        let effective_name = if let serde_json::Value::Array(ops) = patch_ops {
            ops.iter()
                .find_map(|op| {
                    op.get("value")
                        .and_then(|v| v.get("Name"))
                        .and_then(|n| n.as_str())
                        .map(String::from)
                })
                .unwrap_or_else(|| collection_name.to_string())
        } else {
            collection_name.to_string()
        };

        if let serde_json::Value::Array(ops) = patch_ops {
            // Look for move/copy operations to determine if this is a routing issue
            for op in ops {
                let operation = op.get("op").and_then(|v| v.as_str());
                let from_raw = op.get("from").and_then(|v| v.as_str());

                match operation {
                    Some("copy") | Some("add") | Some("replace") => {
                        // Adding/replacing collections via patch is not supported
                        return Err(Error::InvalidPatch(format!(
                            "adding collections via patch is not supported. Name: {}",
                            effective_name,
                        )));
                    }
                    Some("move") => {
                        // Collection-level move is a no-op - find source and return unchanged
                        if let Some(from) = from_raw {
                            let source_name =
                                from.trim_start_matches('/').split('/').next().unwrap_or("");
                            if let Some(source_col) = self.get_collection(source_name)? {
                                return Ok(source_col.schema().clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // No recognized operation - adding collections via patch is not supported
        Err(Error::InvalidPatch(format!(
            "adding collections via patch is not supported. Name: {}",
            effective_name,
        )))
    }

    /// Helper: Set a value at a JSON pointer path.
    fn json_pointer_set(
        json: &mut serde_json::Value,
        path: &str,
        value: serde_json::Value,
    ) -> Result<()> {
        // Convert JSON pointer to path segments
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if segments.is_empty() {
            return Err(Error::InvalidPatch("empty path".to_string()));
        }

        let mut current = json;
        for (i, segment) in segments.iter().enumerate() {
            if i == segments.len() - 1 {
                // Last segment - set the value
                match current {
                    serde_json::Value::Object(map) => {
                        map.insert(segment.to_string(), value);
                        return Ok(());
                    }
                    serde_json::Value::Array(arr) => {
                        // JSON Pointer uses "-" to mean "append to end of array"
                        if *segment == "-" {
                            arr.push(value);
                        } else {
                            let idx: usize = segment.parse().map_err(|_| {
                                Error::InvalidPatch(format!("invalid array index: {}", segment))
                            })?;
                            if idx >= arr.len() {
                                arr.push(value);
                            } else {
                                arr[idx] = value;
                            }
                        }
                        return Ok(());
                    }
                    _ => {
                        return Err(Error::InvalidPatch(format!(
                            "cannot set value at path {}",
                            path
                        )));
                    }
                }
            } else {
                // Navigate to the next level
                match current {
                    serde_json::Value::Object(map) => {
                        current = map.get_mut(*segment).ok_or_else(|| {
                            Error::InvalidPatch(format!("path not found: {}", path))
                        })?;
                    }
                    serde_json::Value::Array(arr) => {
                        let idx: usize = segment.parse().map_err(|_| {
                            Error::InvalidPatch(format!("invalid array index: {}", segment))
                        })?;
                        current = arr.get_mut(idx).ok_or_else(|| {
                            Error::InvalidPatch(format!("path not found: {}", path))
                        })?;
                    }
                    _ => {
                        return Err(Error::InvalidPatch(format!(
                            "cannot navigate path: {}",
                            path
                        )));
                    }
                }
            }
        }

        Err(Error::InvalidPatch("failed to set value".to_string()))
    }

    /// Helper: Remove a value at a JSON pointer path.
    fn json_pointer_remove(json: &mut serde_json::Value, path: &str) -> Result<()> {
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if segments.is_empty() {
            return Err(Error::InvalidPatch("empty path".to_string()));
        }

        let mut current = json;
        for (i, segment) in segments.iter().enumerate() {
            if i == segments.len() - 1 {
                // Last segment - remove the value
                match current {
                    serde_json::Value::Object(map) => {
                        map.remove(*segment);
                        return Ok(());
                    }
                    serde_json::Value::Array(arr) => {
                        let idx: usize = segment.parse().map_err(|_| {
                            Error::InvalidPatch(format!("invalid array index: {}", segment))
                        })?;
                        if idx < arr.len() {
                            arr.remove(idx);
                        }
                        return Ok(());
                    }
                    _ => {
                        return Err(Error::InvalidPatch(format!(
                            "cannot remove value at path {}",
                            path
                        )));
                    }
                }
            } else {
                // Navigate to the next level
                match current {
                    serde_json::Value::Object(map) => {
                        current = map.get_mut(*segment).ok_or_else(|| {
                            Error::InvalidPatch(format!("path not found: {}", path))
                        })?;
                    }
                    serde_json::Value::Array(arr) => {
                        let idx: usize = segment.parse().map_err(|_| {
                            Error::InvalidPatch(format!("invalid array index: {}", segment))
                        })?;
                        current = arr.get_mut(idx).ok_or_else(|| {
                            Error::InvalidPatch(format!("path not found: {}", path))
                        })?;
                    }
                    _ => {
                        return Err(Error::InvalidPatch(format!(
                            "cannot navigate path: {}",
                            path
                        )));
                    }
                }
            }
        }

        Err(Error::InvalidPatch("failed to remove value".to_string()))
    }

    /// Extract a field name from a path like `/Fields/email` or `/Fields/email/Name`.
    /// Returns None if the segment after /Fields/ is numeric, "-", or /Fields/ isn't present.
    fn extract_field_name_from_path(path: &str) -> Option<String> {
        let segments: Vec<&str> = path.split('/').collect();
        for (i, seg) in segments.iter().enumerate() {
            if *seg == "Fields" && i + 1 < segments.len() {
                let next = segments[i + 1];
                if next.parse::<usize>().is_ok() || next == "-" {
                    return None;
                }
                return Some(next.to_string());
            }
        }
        None
    }

    /// Helper: Get a value at a JSON pointer path (for test operation).
    fn json_pointer_get(json: &serde_json::Value, path: &str) -> Option<serde_json::Value> {
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if segments.is_empty() {
            return None;
        }

        let mut current = json;
        for segment in segments.iter() {
            match current {
                serde_json::Value::Object(map) => {
                    current = map.get(*segment)?;
                }
                serde_json::Value::Array(arr) => {
                    let idx: usize = segment.parse().ok()?;
                    current = arr.get(idx)?;
                }
                _ => return None,
            }
        }
        Some(current.clone())
    }

    /// Helper: Substitute field names for indices in paths like /Fields/<name>
    /// Go DefraDB allows using field names as array indices in patches.
    fn substitute_field_name_in_path(path: &str, schema_json: &serde_json::Value) -> String {
        // Check if path contains /Fields/ followed by a non-numeric segment
        if !path.contains("/Fields/") {
            return path.to_string();
        }

        let segments: Vec<&str> = path.split('/').collect();
        let mut result_segments: Vec<String> = Vec::new();

        let mut i = 0;
        while i < segments.len() {
            let segment = segments[i];

            if segment == "Fields" && i + 1 < segments.len() {
                result_segments.push("Fields".to_string());
                i += 1;

                let next_segment = segments[i];
                // Check if next segment is a number (already an index)
                if next_segment.parse::<usize>().is_ok() || next_segment == "-" {
                    result_segments.push(next_segment.to_string());
                } else {
                    // It's a field name - look up the index in the existing Fields array
                    if let Some(fields) = schema_json.get("Fields").and_then(|f| f.as_array()) {
                        let mut found = false;
                        for (idx, field) in fields.iter().enumerate() {
                            if let Some(name) = field.get("Name").and_then(|n| n.as_str()) {
                                if name == next_segment {
                                    result_segments.push(idx.to_string());
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if !found {
                            // Field name not found in existing fields - treat as append
                            // (Go interprets unknown field names as new field additions)
                            result_segments.push("-".to_string());
                        }
                    } else {
                        result_segments.push("-".to_string());
                    }
                }
            } else {
                result_segments.push(segment.to_string());
            }
            i += 1;
        }

        result_segments.join("/")
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

/// Convert a JSON value to a native NormalValue based on the field's schema type.
///
/// When documents are migrated through lens transforms, they come back as JSON values.
/// This function converts them to the appropriate native type (Int, Float, String, etc.)
/// based on the field's declared type in the schema.
fn json_to_native_value(
    value: &serde_json::Value,
    field_name: &str,
    schema: &schema::CollectionVersion,
) -> document::NormalValue {
    // Handle null values
    if value.is_null() {
        return document::NormalValue::Null;
    }

    // Find the field definition in the schema
    let field_kind = schema
        .fields
        .iter()
        .find(|f| f.name == field_name)
        .map(|f| &f.kind);

    match field_kind {
        Some(FieldKind::Scalar(scalar)) => match scalar {
            ScalarKind::Int => {
                if let Some(n) = value.as_i64() {
                    return document::NormalValue::Int(n);
                }
            }
            ScalarKind::Float64 => {
                if let Some(n) = value.as_f64() {
                    return document::NormalValue::Float64(n);
                }
            }
            ScalarKind::Float32 => {
                if let Some(n) = value.as_f64() {
                    return document::NormalValue::Float32(n as f32);
                }
            }
            ScalarKind::Bool => {
                if let Some(b) = value.as_bool() {
                    return document::NormalValue::Bool(b);
                }
            }
            ScalarKind::String | ScalarKind::DocID => {
                if let Some(s) = value.as_str() {
                    return document::NormalValue::String(s.to_string());
                }
            }
            ScalarKind::Blob => {
                // Blobs may be base64 encoded strings in JSON
                if let Some(s) = value.as_str() {
                    return document::NormalValue::Bytes(s.as_bytes().to_vec());
                }
            }
            ScalarKind::DateTime => {
                // DateTime as string - keep as string for now, the document layer handles parsing
                if let Some(s) = value.as_str() {
                    return document::NormalValue::String(s.to_string());
                }
            }
            ScalarKind::Json | ScalarKind::None => {
                // Keep as JSON
            }
        },
        _ => {}
    }

    // Fallback: keep as JSON (this preserves the original behavior for unknown types)
    document::NormalValue::Json(value.clone())
}
