//! Collection operations for DefraDB.
//!
//! This module contains all collection-related operations including:
//! - Loading collections from storage
//! - Creating, deleting, and truncating collections
//! - Querying and listing collections
//! - Managing collection versions and active states

use crate::collection::Collection;
use crate::collection_name::CollectionName;
use crate::collection_snapshot::CollectionSnapshot;
use crate::error::{Error, Result};
use crate::txn::DbTxn;
use datastore::NamespaceView;
use schema::CollectionVersion;
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::systemstore::{
    CollectionID, CollectionIDSequenceKey, CollectionKey, CollectionNameKey, CollectionVersionKey,
    IndexIDSequenceKey,
};
use tracing::instrument;

/// Helper to delete all keys with a given prefix from a namespace view.
async fn delete_prefix(store: &NamespaceView, prefix: Vec<u8>) -> Result<()> {
    let opts = IterOptions::new().with_prefix(prefix);
    let mut iter = store.iterator(opts).await.map_err(Error::Storage)?;
    let mut keys_to_delete = Vec::new();
    while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
        keys_to_delete.push(pair.key.to_vec());
    }
    iter.close().await.map_err(Error::Storage)?;
    for key in keys_to_delete {
        store.delete(&key).await.map_err(Error::Storage)?;
    }
    Ok(())
}

/// Helper to extract the last path segment as a string.
fn extract_last_path_segment_str(key: &[u8]) -> Option<String> {
    let key_str = std::str::from_utf8(key).ok()?;
    key_str.rsplit('/').next().map(|s| s.to_string())
}

impl<S: Store> crate::database::DB<S> {
    /// Load all collections from the SystemStore into the in-memory cache.
    ///
    /// This also finalizes relations by:
    /// - Auto-generating `_id` fields for non-array relation fields
    /// - Auto-determining primary sides for one-to-many relations
    #[instrument(skip(self), name = "db.load_collections")]
    pub async fn load_collections(&self) -> Result<()> {
        let txn = self.new_txn(true).await?;
        let prefix = CollectionNameKey::name_prefix();
        let mut schemas: std::collections::HashMap<String, CollectionVersion> =
            std::collections::HashMap::new();

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
                            json_preview = %String::from_utf8_lossy(&collection_json[..collection_json.len().min(200)]),
                            "Failed to deserialize collection schema"
                        );
                        Error::Serialization(format!(
                            "failed to deserialize schema for collection '{}': {}",
                            name, e
                        ))
                    })?;

                // If the collection doesn't have a root_id yet (existing data from before root_id
                // was added), look it up from the short ID mapping
                if schema.root_id == 0 {
                    let short_id_key = CollectionID::new(&schema.collection_id);
                    if let Some(short_id_bytes) = systemstore
                        .get(&short_id_key.bytes())
                        .await
                        .map_err(Error::Storage)?
                    {
                        if let Ok(short_id_str) = String::from_utf8(short_id_bytes.to_vec()) {
                            if let Ok(short_id) = short_id_str.parse::<u32>() {
                                schema.root_id = short_id;
                            }
                        }
                    }
                }

                // Store in map with collection name for relation finalization later
                schemas.insert(name.clone(), schema);
            }
            iter.close().await.map_err(|e| {
                tracing::error!(error = ?e, "Failed to close iterator during collection load");
                Error::Storage(e)
            })?;
        }

        // Discard read transaction
        let _ = txn.discard();

        // Finalize relations across all collections
        // Use no-op functions since we're just loading (field/index IDs are already assigned)
        CollectionVersion::finalize_relations_hashmap(&mut schemas, String::new, || 0)?;

        // Update cache
        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(error = ?e, "Collection cache lock poisoned during load");
            Error::LockPoisoned("collection cache lock poisoned during load".into())
        })?;

        for (name, schema) in schemas {
            tracing::trace!(
                collection_name = %name,
                version_id = %schema.version_id,
                collection_id = %schema.collection_id,
                field_count = schema.fields.len(),
                "Loaded collection"
            );
            cache.insert(name, Collection::new(schema));
        }

        tracing::info!(collection_count = cache.len(), "Loaded collections");
        Ok(())
    }

    /// Reload the collection cache from persistent storage.
    ///
    /// This is useful for recovering from a `CacheUpdateFailedAfterCommit` error,
    /// or for refreshing the cache after external modifications to the store.
    ///
    /// # Example
    ///
    /// ```ignore
    /// match db.create_collection(schema).await {
    ///     Ok(()) => println!("Collection created successfully"),
    ///     Err(Error::CacheUpdateFailedAfterCommit(_)) => {
    ///         // Data was committed but cache wasn't updated
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

    /// Get the next collection short ID from the sequence key.
    pub(crate) async fn next_collection_short_id(
        systemstore: &NamespaceView,
    ) -> Result<u32> {
        let seq_key = CollectionIDSequenceKey;
        let key_bytes = seq_key.bytes();
        let current: u32 = match systemstore.get(&key_bytes).await.map_err(Error::Storage)? {
            Some(bytes) => {
                if bytes.len() == 4 {
                    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
                } else {
                    0
                }
            }
            None => 0,
        };
        let next = current + 1;
        systemstore
            .set(&key_bytes, &next.to_be_bytes())
            .await
            .map_err(Error::Storage)?;
        Ok(next)
    }

    /// Create a collection within an existing transaction.
    ///
    /// This method validates the collection schema, assigns a unique short ID,
    /// stores the schema in the systemstore, and stores field/collection blocks
    /// in the blockstore for P2P sync.
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction to use
    /// * `schema` - The collection schema (will be validated and potentially modified)
    ///
    /// # Returns
    ///
    /// The finalized schema with assigned short ID and field IDs.
    ///
    /// # Errors
    ///
    /// - `InvalidCollectionName` if the collection name is invalid
    /// - `CollectionAlreadyExists` if a collection with this name already exists
    #[instrument(skip(self, txn, schema), fields(collection = %schema.name), name = "db.create_collection")]
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
            // Generate field block with priority=1 (matches Go)
            match schema::generate_field_block_with_priority_and_heads(field, 1, &[]) {
                Ok(block_with_cid) => {
                    blockstore
                        .set(&block_with_cid.cid.to_bytes(), &block_with_cid.bytes)
                        .await
                        .map_err(Error::Storage)?;
                    field_cids.push(block_with_cid.cid);
                }
                Err(e) => {
                    tracing::warn!(
                        field_name = %field.name,
                        error = %e,
                        "Failed to generate field block CID"
                    );
                }
            }
        }

        // Store the collection definition block
        // This includes the collection name, field CIDs, priority, and head CIDs.
        // For new collections (not patches), there are no heads, so we pass an empty slice.
        match schema::generate_collection_block_full(
            Some(&schema.name),
            &field_cids,
            1, // priority=1 for new collections
            &[], // no heads for new collections
        ) {
            Ok(block_with_cid) => {
                blockstore
                    .set(&block_with_cid.cid.to_bytes(), &block_with_cid.bytes)
                    .await
                    .map_err(Error::Storage)?;
            }
            Err(e) => {
                tracing::warn!(
                    collection_name = %name,
                    error = %e,
                    "Failed to generate collection block"
                );
            }
        }

        // 2. Store name -> version_id mapping at /collection/name/{name}
        let name_key = CollectionNameKey::new(&name);
        systemstore
            .set(&name_key.bytes(), version_id.as_bytes())
            .await
            .map_err(Error::Storage)?;

        // 3. Store version index at /collection/version/{collection_id}/{version_id}
        let version_index_key = CollectionVersionKey::new(collection_id.as_str(), version_id);
        systemstore
            .set(&version_index_key.bytes(), b"1")
            .await
            .map_err(Error::Storage)?;

        // Add to transaction's cache
        txn.cache_collection(Collection::new(schema.clone()));

        tracing::info!(
            collection_name = %name,
            version_id = %version_id,
            collection_id = %collection_id,
            field_count = schema.fields.len(),
            "Created collection"
        );

        Ok(schema)
    }

    /// Create a new collection.
    ///
    /// This creates a new transaction, calls `create_collection_with_txn`, commits,
    /// and updates the process-wide cache.
    #[instrument(skip(self, schema), fields(collection = %schema.name), name = "db.create_collection_auto")]
    pub async fn create_collection(&self, schema: CollectionVersion) -> Result<()> {
        let name = schema.name.clone();
        let mut txn = self.new_txn(false).await?;

        let finalized_schema = self.create_collection_with_txn(&mut txn, schema).await?;

        txn.commit().await?;

        // Update the process-wide cache after successful commit
        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(error = ?e, collection_name = %name, "Collection cache lock poisoned after create");
            Error::CacheUpdateFailedAfterCommit(name.clone())
        })?;
        cache.insert(name, Collection::new(finalized_schema));

        Ok(())
    }

    /// Create multiple collections atomically in a single transaction.
    ///
    /// This is useful for creating related collections (e.g., with relations between them)
    /// where all must succeed or none should be created.
    pub async fn create_collections_atomic(
        &self,
        schemas: Vec<CollectionVersion>,
    ) -> Result<Vec<CollectionVersion>> {
        let mut txn = self.new_txn(false).await?;
        let mut finalized_schemas = Vec::with_capacity(schemas.len());

        for schema in schemas {
            let finalized = self.create_collection_with_txn(&mut txn, schema).await?;
            finalized_schemas.push(finalized);
        }

        txn.commit().await?;

        // Update the process-wide cache after successful commit
        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(error = ?e, "Collection cache lock poisoned after atomic create");
            Error::CacheUpdateFailedAfterCommit("atomic collections".to_string())
        })?;

        for schema in &finalized_schemas {
            cache.insert(schema.name.clone(), Collection::new(schema.clone()));
        }

        Ok(finalized_schemas)
    }

    /// Delete a collection within an existing transaction.
    ///
    /// This removes:
    /// 1. The collection schema from `/collection/id/{version_id}`
    /// 2. The name mapping from `/collection/name/{name}`
    /// 3. The version index entry from `/collection/version/{collection_id}/{version_id}`
    ///
    /// Note: This does NOT delete documents. Use `truncate_collection` first if needed.
    #[instrument(skip(self, txn), fields(collection = %name), name = "db.delete_collection")]
    pub async fn delete_collection_with_txn(
        &self,
        txn: &mut DbTxn<S>,
        name: &str,
    ) -> Result<()> {
        // Get the collection to find its version_id and collection_id
        let collection = txn
            .get_collection(name)
            .await?
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))?;

        let version_id = collection.version_id().to_string();
        let collection_id = collection.collection_id().to_string();

        let systemstore = txn.systemstore()?;

        // 1. Delete the full schema from /collection/id/{version_id}
        let collection_key = CollectionKey::new(&version_id);
        systemstore
            .delete(&collection_key.bytes())
            .await
            .map_err(Error::Storage)?;

        // 2. Delete the name mapping from /collection/name/{name}
        let name_key = CollectionNameKey::new(name);
        systemstore
            .delete(&name_key.bytes())
            .await
            .map_err(Error::Storage)?;

        // 3. Delete the version index entry from /collection/version/{collection_id}/{version_id}
        let version_index_key = CollectionVersionKey::new(&collection_id, &version_id);
        systemstore
            .delete(&version_index_key.bytes())
            .await
            .map_err(Error::Storage)?;

        // Remove from transaction's cache
        txn.uncache_collection(name);

        tracing::info!(
            collection_name = %name,
            version_id = %version_id,
            collection_id = %collection_id,
            "Deleted collection"
        );

        Ok(())
    }

    /// Delete a collection.
    ///
    /// This creates a new transaction, calls `delete_collection_with_txn`, commits,
    /// and updates the process-wide cache.
    ///
    /// Note: This does NOT delete documents. Use `truncate_collection` first if needed.
    #[instrument(skip(self), fields(collection = %name), name = "db.delete_collection_auto")]
    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        let mut txn = self.new_txn(false).await?;

        match self.delete_collection_with_txn(&mut txn, name).await {
            Ok(()) => {
                txn.commit().await?;

                // Update the process-wide cache after successful commit
                let mut cache = self.collections.write().map_err(|e| {
                    tracing::error!(error = ?e, collection_name = %name, "Collection cache lock poisoned after delete");
                    Error::CacheUpdateFailedAfterCommit(name.to_string())
                })?;
                cache.remove(name);

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
    #[instrument(skip(self), fields(collection = %name), name = "db.truncate_collection")]
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
    #[instrument(skip(self), fields(version_id = %version_id), name = "db.set_active_version")]
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
                .ok_or(Error::CollectionVersionNotFound(version_id.to_string()))?;

            let mut target_schema: CollectionVersion =
                serde_json::from_slice(&target_bytes).map_err(|e| {
                    Error::Serialization(format!(
                        "failed to deserialize schema for version_id '{}': {}",
                        version_id, e
                    ))
                })?;

            let name = target_schema.name.clone();
            let collection_id = target_schema.collection_id.clone();

            // Update target to be active
            target_schema.is_active = true;

            // Store the updated target schema
            let target_data = serde_json::to_vec(&target_schema).map_err(|e| {
                Error::Serialization(format!(
                    "failed to serialize schema for version_id '{}': {}",
                    version_id, e
                ))
            })?;
            systemstore
                .set(&collection_key.bytes(), &target_data)
                .await
                .map_err(Error::Storage)?;

            // Update the name pointer to point to this version
            let name_key = CollectionNameKey::new(&name);
            systemstore
                .set(&name_key.bytes(), version_id.as_bytes())
                .await
                .map_err(Error::Storage)?;

            // Find and deactivate other versions with the same collection_id
            let version_prefix = CollectionVersionKey::collection_prefix(&collection_id);
            let opts = IterOptions::new().with_prefix(version_prefix);
            let mut iter = systemstore.iterator(opts).await.map_err(Error::Storage)?;

            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                // Extract version_id from key: /collection/version/{collection_id}/{version_id}
                let key_str = String::from_utf8_lossy(&pair.key);
                if let Some(other_version_id) = key_str.rsplit('/').next() {
                    if other_version_id != version_id {
                        // Load, deactivate, and store the other version
                        let other_key = CollectionKey::new(other_version_id);
                        if let Some(other_bytes) =
                            systemstore.get(&other_key.bytes()).await.map_err(Error::Storage)?
                        {
                            if let Ok(mut other_schema) =
                                serde_json::from_slice::<CollectionVersion>(&other_bytes)
                            {
                                if other_schema.is_active {
                                    other_schema.is_active = false;
                                    if let Ok(other_data) = serde_json::to_vec(&other_schema) {
                                        let _ = systemstore
                                            .set(&other_key.bytes(), &other_data)
                                            .await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            iter.close().await.map_err(Error::Storage)?;

            (target_schema, name)
        };

        txn.commit().await?;

        // Update the process-wide cache
        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(
                error = ?e,
                version_id = %version_id,
                "Collection cache lock poisoned during set_active_collection_version"
            );
            Error::CacheUpdateFailedAfterCommit(name.clone())
        })?;
        cache.insert(name.clone(), Collection::new(target_schema));

        tracing::info!(
            collection_name = %name,
            version_id = %version_id,
            "Set active collection version"
        );

        Ok(())
    }

    /// Get a collection by version ID from the cache.
    ///
    /// This searches the in-memory cache for a collection with the given version ID.
    /// It only returns active collections that are in the cache.
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
            .find(|c| c.version_id() == version_id)
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
}
