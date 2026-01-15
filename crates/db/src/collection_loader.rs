//! Shared collection loading utilities.
//!
//! This module provides common functions for loading collections from the SystemStore,
//! used by both DbDocFetcher and DbDocMutator.

use datastore::NamespaceView;
use schema::CollectionVersion;
use storage::corekv::Key;
use storage::keys::systemstore::CollectionNameKey;
use tracing::error;

use crate::collection::Collection;

/// Load a collection from the systemstore by name.
///
/// This is a standalone async function that doesn't hold any locks,
/// allowing it to be called outside the mutex lock scope.
///
/// Returns `Ok(Some(collection))` if found, `Ok(None)` if not found,
/// or an error if storage/deserialization fails.
pub(crate) async fn load_collection_from_systemstore(
    systemstore: &NamespaceView,
    name: &str,
) -> query::error::Result<Option<Collection>> {
    let key = CollectionNameKey::new(name);

    match systemstore.get(&key.bytes()).await.map_err(|e| {
        error!(
            error = ?e,
            collection_name = %name,
            "Storage error while loading collection from systemstore"
        );
        query::error::QueryError::execution(format!("storage error: {}", e))
    })? {
        Some(data) => {
            let schema: CollectionVersion = serde_json::from_slice(&data).map_err(|e| {
                error!(
                    error = ?e,
                    collection_name = %name,
                    "Failed to deserialize schema for collection"
                );
                query::error::QueryError::execution(format!(
                    "failed to deserialize schema for collection '{}': {}",
                    name, e
                ))
            })?;
            Ok(Some(Collection::new(schema)))
        }
        None => Ok(None),
    }
}
