use super::*;

impl<S: Store> crate::database::DB<S> {
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
            Error::LockPoisoned(
                "collection cache lock poisoned during add_collection_to_cache".into(),
            )
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
        let cache = self.collections.read().map_err(|e| {
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
        let cache = self.collections.read().map_err(|e| {
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

    pub(crate) fn forbid_collection_id(&self, collection_id: &str) -> Result<()> {
        let mut forbidden = self.forbidden_collection_ids.write().map_err(|e| {
            tracing::error!(
                error = ?e,
                collection_id = %collection_id,
                "Forbidden collection lock poisoned during forbid"
            );
            Error::LockPoisoned("forbidden collection lock poisoned during forbid".into())
        })?;
        forbidden.insert(collection_id.to_string());
        Ok(())
    }

    pub(crate) fn unforbid_collection_id(&self, collection_id: &str) -> Result<()> {
        let mut forbidden = self.forbidden_collection_ids.write().map_err(|e| {
            tracing::error!(
                error = ?e,
                collection_id = %collection_id,
                "Forbidden collection lock poisoned during unforbid"
            );
            Error::LockPoisoned("forbidden collection lock poisoned during unforbid".into())
        })?;
        forbidden.remove(collection_id);
        Ok(())
    }

    pub(crate) fn is_collection_forbidden(&self, collection_id: &str) -> Result<bool> {
        let forbidden = self.forbidden_collection_ids.read().map_err(|e| {
            tracing::error!(
                error = ?e,
                collection_id = %collection_id,
                "Forbidden collection lock poisoned during lookup"
            );
            Error::LockPoisoned("forbidden collection lock poisoned during lookup".into())
        })?;
        Ok(forbidden.contains(collection_id))
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
}
