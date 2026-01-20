//! Schema loading from systemstore.
//!
//! This module provides functionality to load collection schemas from the
//! systemstore on database startup. It matches the Go DefraDB pattern of
//! storing collection definitions as JSON in the systemstore.

use crate::error::{Error, Result};
use crate::DB;
use schema::CollectionVersion;
use storage::corekv::{IterOptions, Iterator, Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionNameKey};

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
