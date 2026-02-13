//! Collection operations for DefraDB.
//!
//! This module contains all collection-related operations including:
//! - Loading collections from storage
//! - Creating, deleting, and truncating collections
//! - Querying and listing collections
//! - Managing collection versions and active states

mod create;
mod delete;
mod lookup;
mod resolve;
mod version;

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
pub(super) async fn delete_prefix(store: &NamespaceView, prefix: Vec<u8>) -> Result<()> {
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
pub(super) fn extract_last_path_segment_str(key: &[u8]) -> Option<String> {
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
        {
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
        }

        // Reconstruct schema_heads from loaded collections.
        // For each collection, count all versions in its version chain to determine height,
        // then set the active version's CID as the head.
        {
            let all_versions = self.get_all_collection_versions().await?;
            // Group versions by collection_id
            let mut versions_by_collection: std::collections::HashMap<
                &str,
                Vec<&CollectionVersion>,
            > = std::collections::HashMap::new();
            for v in &all_versions {
                versions_by_collection
                    .entry(v.collection_id.as_str())
                    .or_default()
                    .push(v);
            }

            let mut heads_map = self.schema_heads.write().map_err(|e| {
                tracing::error!(error = ?e, "schema_heads lock poisoned during load");
                Error::LockPoisoned("schema_heads lock poisoned during load".into())
            })?;

            for versions in versions_by_collection.values() {
                // Count only non-placeholder versions for height computation.
                // Placeholders are created by set_migration before the real version
                // exists and should not affect the CID priority calculation.
                let height = versions.iter().filter(|v| !v.is_placeholder).count() as u64;
                // Find the active version to use as head
                if let Some(active) = versions.iter().find(|v| v.is_active) {
                    if let Ok(cid) = cid::Cid::try_from(active.version_id.as_str()) {
                        heads_map.insert(active.name.clone(), (vec![cid], height));
                    }
                }
            }
        }

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
}
