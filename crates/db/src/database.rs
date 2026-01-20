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
use identity::{Identity, RawIdentity};
use schema::CollectionVersion;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::systemstore::CollectionNameKey;

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
}

impl<S: Store> DB<S> {
    /// Create a new database with the given store.
    ///
    /// This creates a DB with an empty collection cache. Use `open()` to
    /// load existing collections from the store.
    pub fn new(store: S) -> Self {
        Self::with_options(store, DbOptions::default())
    }

    /// Create a new database with the given store and options.
    ///
    /// This creates a DB with an empty collection cache. Use `open_with_options()`
    /// to load existing collections from the store.
    pub fn with_options(store: S, options: DbOptions) -> Self {
        Self {
            store: Arc::new(store),
            options,
            txn_id_counter: AtomicU64::new(0),
            collections: RwLock::new(HashMap::new()),
        }
    }

    /// Open a database and load existing collections from the store.
    pub async fn open(store: S) -> Result<Self> {
        Self::open_with_options(store, DbOptions::default()).await
    }

    /// Open a database with options and load existing collections from the store.
    pub async fn open_with_options(store: S, options: DbOptions) -> Result<Self> {
        let db = Self::with_options(store, options);
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
    pub fn from_arc(store: Arc<S>) -> Self {
        Self::from_arc_with_options(store, DbOptions::default())
    }

    /// Create a new database from an Arc-wrapped store with options.
    ///
    /// **Warning:** When multiple DB instances share a store via `from_arc()`,
    /// transaction IDs may collide if both instances create transactions concurrently.
    pub fn from_arc_with_options(store: Arc<S>, options: DbOptions) -> Self {
        Self {
            store,
            options,
            txn_id_counter: AtomicU64::new(0),
            collections: RwLock::new(HashMap::new()),
        }
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
        let db = Self::from_arc_with_options(store, options);
        db.load_collections().await?;
        Ok(db)
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

    /// Get the current transaction ID counter value.
    pub fn current_txn_id(&self) -> u64 {
        self.txn_id_counter.load(Ordering::SeqCst)
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

                let schema: CollectionVersion =
                    serde_json::from_slice(&pair.value).map_err(|e| {
                        tracing::error!(
                            error = ?e,
                            collection_name = %name,
                            "Failed to deserialize schema for collection"
                        );
                        Error::Serialization(format!(
                            "failed to deserialize schema for collection '{}': {}",
                            name, e
                        ))
                    })?;

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

        // Finalize relations: auto-generate _id fields and set primary sides
        // Create a field ID generator that starts after the max existing field ID
        let max_field_id = schemas
            .values()
            .flat_map(|s| s.fields.iter())
            .filter_map(|f| f.id.parse::<u64>().ok())
            .max()
            .unwrap_or(0);
        let mut field_id_counter = max_field_id + 1000; // Start well above existing IDs

        CollectionVersion::finalize_relations_hashmap(&mut schemas, || {
            field_id_counter += 1;
            format!("gen-{}", field_id_counter)
        })
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
    /// # Errors
    ///
    /// - `InvalidCollectionName` if the collection name is invalid
    /// - `CollectionAlreadyExists` if a collection with this name already exists
    pub async fn create_collection_with_txn(
        &self,
        txn: &mut DbTxn<S>,
        schema: CollectionVersion,
    ) -> Result<()> {
        // Validate collection name
        let collection_name = CollectionName::new(&schema.name)?;
        let name = collection_name.as_str().to_string();

        // Check if collection exists in txn cache or store
        if txn.get_collection(&name).await?.is_some() {
            return Err(Error::CollectionAlreadyExists(name));
        }

        // Write schema to store (within txn)
        let key = CollectionNameKey::new(&name);
        let data = serde_json::to_vec(&schema).map_err(|e| {
            Error::Serialization(format!(
                "failed to serialize schema for collection '{}': {}",
                name, e
            ))
        })?;
        txn.systemstore()?
            .set(&key.bytes(), &data)
            .await
            .map_err(Error::Storage)?;

        // Update txn-local cache
        txn.cache_collection(Collection::new(schema));

        Ok(())
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
        let collection = Collection::new(schema.clone());

        let mut txn = self.new_txn(false).await?;
        match self.create_collection_with_txn(&mut txn, schema).await {
            Ok(()) => {
                txn.commit().await?;

                // Update process-wide cache for callers not using transaction-scoped caching
                let mut cache = self.collections.write().map_err(|e| {
                    tracing::error!(
                        error = ?e,
                        collection_name = %name,
                        "Collection cache lock poisoned during create"
                    );
                    Error::CacheUpdateFailedAfterCommit(name.clone())
                })?;
                cache.insert(name, collection);
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
