//! Schema loading from systemstore.
//!
//! This module provides functionality to load collection schemas from the
//! systemstore on database startup. It matches the Go DefraDB pattern of
//! storing collection definitions as JSON in the systemstore.
//!
//! # Collection Version History
//!
//! Collections can have multiple versions linked via the `previous_version` field.
//! The systemstore tracks all versions using:
//! - `/collection/id/{versionID}` - Full collection definition for each version
//! - `/collection/version/{collectionID}/{versionID}` - Index of all versions for a collection
//! - `/collection/name/{name}` - Maps active collection names to their current version
//!
//! The `get_collection_version_ids` and `get_collections_by_collection_id` functions
//! enable loading all versions for building the migration history graph.

use crate::error::{Error, Result};
use crate::DB;
use datastore::NamespaceView;
use schema::CollectionVersion;
use storage::corekv::{IterOptions, Iterator, Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionNameKey, CollectionVersionKey};

/// Load all active collections from the systemstore.
///
/// This iterates over `/collection/name/*` keys to find all collection names,
/// then looks up each collection's full definition from `/collection/id/<id>`.
///
/// Returns an empty Vec if no collections are stored (new database).
///
/// # Errors
///
/// Returns an error if:
/// - A collection name exists but its definition is missing (inconsistent state)
/// - A collection definition cannot be deserialized (corrupt data)
/// - Storage operations fail
pub async fn load_active_collections<S: Store>(db: &DB<S>) -> Result<Vec<CollectionVersion>> {
    let txn = db.new_txn(true).await?;
    let systemstore = txn.systemstore()?;

    let mut collections = Vec::new();
    let mut load_error: Option<Error> = None;

    // Iterate over all collection name mappings
    let prefix = CollectionNameKey::name_prefix();
    let opts = IterOptions::new().with_prefix(prefix);
    let mut iter = systemstore.iterator(opts).await.map_err(Error::Storage)?;

    // Process iterator, capturing any error for later
    loop {
        match iter.next().await {
            Ok(Some(kv)) => {
                // The value at /collection/name/<name> is the collection ID
                let collection_id = match String::from_utf8(kv.value) {
                    Ok(id) => id,
                    Err(e) => {
                        load_error = Some(Error::Other(format!(
                            "Invalid collection ID encoding: {}",
                            e
                        )));
                        break;
                    }
                };

                // Look up the full collection definition
                let collection_key = CollectionKey::new(&collection_id);
                let collection_json = match systemstore.get(&collection_key.bytes()).await {
                    Ok(Some(json)) => json,
                    Ok(None) => {
                        load_error = Some(Error::Other(format!(
                            "Collection definition not found for ID: {}",
                            collection_id
                        )));
                        break;
                    }
                    Err(e) => {
                        load_error = Some(Error::Storage(e));
                        break;
                    }
                };

                // Deserialize the collection
                match serde_json::from_slice::<CollectionVersion>(&collection_json) {
                    Ok(collection) => collections.push(collection),
                    Err(e) => {
                        load_error = Some(Error::Other(format!(
                            "Failed to deserialize collection {}: {}",
                            collection_id, e
                        )));
                        break;
                    }
                }
            }
            Ok(None) => break, // End of iteration
            Err(e) => {
                load_error = Some(Error::Storage(e));
                break;
            }
        }
    }

    // Always close the iterator, log if cleanup fails
    if let Err(e) = iter.close().await {
        tracing::warn!(error = %e, "Failed to close iterator during schema loading");
    }

    // Return error if any occurred during loading
    if let Some(err) = load_error {
        return Err(err);
    }

    tracing::info!(
        "Loaded {} collection(s) from systemstore",
        collections.len()
    );

    Ok(collections)
}

/// Get all version IDs for a collection.
///
/// This returns the collection ID itself (as the initial version) plus all
/// additional version IDs found in the `/collection/version/{collectionID}/*` index.
///
/// Matches Go's `description.GetCollectionVersionIDs`.
///
/// # Arguments
///
/// * `systemstore` - The systemstore namespace view
/// * `collection_id` - The collection ID (schema root)
///
/// # Returns
///
/// A list of version IDs, starting with the collection ID itself.
pub async fn get_collection_version_ids(
    systemstore: &NamespaceView,
    collection_id: &str,
) -> Result<Vec<String>> {
    // The collection ID is always the first version
    let mut version_ids = vec![collection_id.to_string()];

    // Iterate over the version index to find additional versions
    let prefix = CollectionVersionKey::collection_prefix(collection_id);
    let opts = IterOptions::new().with_prefix(prefix);
    let mut iter = systemstore.iterator(opts).await.map_err(Error::Storage)?;

    loop {
        match iter.next().await {
            Ok(Some(kv)) => {
                // Parse the key to extract the version ID
                // Key format: /collection/version/{collectionID}/{versionID}
                let key_str = String::from_utf8_lossy(&kv.key);
                if let Some(version_id) = key_str.rsplit('/').next() {
                    if !version_id.is_empty() && version_id != collection_id {
                        version_ids.push(version_id.to_string());
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                let _ = iter.close().await;
                return Err(Error::Storage(e));
            }
        }
    }

    iter.close().await.map_err(Error::Storage)?;

    tracing::debug!(
        collection_id = %collection_id,
        version_count = version_ids.len(),
        "Retrieved collection version IDs"
    );

    Ok(version_ids)
}

/// Load all versions of a collection by collection ID.
///
/// This loads every version of the collection from the systemstore,
/// including both active and inactive (historical) versions.
///
/// Matches Go's `description.GetCollectionsByCollectionID`.
///
/// # Arguments
///
/// * `systemstore` - The systemstore namespace view
/// * `collection_id` - The collection ID (schema root)
///
/// # Returns
///
/// A list of all collection versions for the given collection ID.
pub async fn get_collections_by_collection_id(
    systemstore: &NamespaceView,
    collection_id: &str,
) -> Result<Vec<CollectionVersion>> {
    // Get all version IDs for this collection
    let version_ids = get_collection_version_ids(systemstore, collection_id).await?;

    let mut collections = Vec::with_capacity(version_ids.len());

    // Load each version
    for version_id in version_ids {
        let collection_key = CollectionKey::new(&version_id);
        match systemstore.get(&collection_key.bytes()).await {
            Ok(Some(json)) => {
                let collection: CollectionVersion = serde_json::from_slice(&json).map_err(|e| {
                    Error::Other(format!(
                        "Failed to deserialize collection version {}: {}",
                        version_id, e
                    ))
                })?;
                collections.push(collection);
            }
            Ok(None) => {
                // Version in index but definition missing - log warning but continue
                tracing::warn!(
                    version_id = %version_id,
                    collection_id = %collection_id,
                    "Collection version in index but definition not found"
                );
            }
            Err(e) => {
                return Err(Error::Storage(e));
            }
        }
    }

    tracing::debug!(
        collection_id = %collection_id,
        loaded_count = collections.len(),
        "Loaded collection versions"
    );

    Ok(collections)
}

/// Load a single collection version by its version ID.
///
/// # Arguments
///
/// * `systemstore` - The systemstore namespace view
/// * `version_id` - The specific version ID to load
///
/// # Returns
///
/// The collection version if found, None otherwise.
pub async fn get_collection_by_version_id(
    systemstore: &NamespaceView,
    version_id: &str,
) -> Result<Option<CollectionVersion>> {
    let collection_key = CollectionKey::new(version_id);
    match systemstore.get(&collection_key.bytes()).await {
        Ok(Some(json)) => {
            let collection: CollectionVersion = serde_json::from_slice(&json).map_err(|e| {
                Error::Other(format!(
                    "Failed to deserialize collection version {}: {}",
                    version_id, e
                ))
            })?;
            Ok(Some(collection))
        }
        Ok(None) => Ok(None),
        Err(e) => Err(Error::Storage(e)),
    }
}
