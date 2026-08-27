//! Shared collection loading utilities.
//!
//! This module provides common functions for loading collections from the SystemStore,
//! used by both DbDocFetcher and DbDocMutator.

use std::sync::Arc;

use async_lock::Mutex as TokioMutex;
use datastore::NamespaceView;
use schema::CollectionVersion;
use storage::corekv::{Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionNameKey};
use tracing::{error, warn};

use crate::collection::{populate_collection_root_id, Collection};
use crate::index::IndexManager;
use crate::txn::DbTxn;

/// Load a collection from the systemstore by name.
///
/// This is a standalone async function that doesn't hold any locks,
/// allowing it to be called outside the mutex lock scope.
///
/// Storage layout uses two-step lookup:
/// - `/collection/name/{name}` contains the version_id string
/// - `/collection/id/{version_id}` contains the full JSON schema
///
/// Returns `Ok(Some(collection))` if found, `Ok(None)` if not found,
/// or an error if storage/deserialization fails.
pub(crate) async fn load_collection_from_systemstore(
    systemstore: &NamespaceView,
    name: &str,
) -> query::error::Result<Option<Collection>> {
    // Step 1: Get version_id from /collection/name/{name}
    let name_key = CollectionNameKey::new(name);

    let version_id = match systemstore.get(&name_key.bytes()).await.map_err(|e| {
        error!(
            error = ?e,
            collection_name = %name,
            "Storage error while loading collection name mapping"
        );
        query::error::QueryError::execution(format!("storage error: {}", e))
    })? {
        Some(data) => String::from_utf8(data).map_err(|e| {
            error!(
                error = ?e,
                collection_name = %name,
                "Invalid UTF-8 in version_id for collection"
            );
            query::error::QueryError::execution(format!(
                "invalid version_id encoding for collection '{}': {}",
                name, e
            ))
        })?,
        None => return Ok(None),
    };

    // Step 2: Get full schema from /collection/id/{version_id}
    let collection_key = CollectionKey::new(&version_id);

    match systemstore
        .get(&collection_key.bytes())
        .await
        .map_err(|e| {
            error!(
                error = ?e,
                collection_name = %name,
                version_id = %version_id,
                "Storage error while loading collection definition"
            );
            query::error::QueryError::execution(format!("storage error: {}", e))
        })? {
        Some(data) => {
            let mut schema: CollectionVersion = serde_json::from_slice(&data).map_err(|e| {
                error!(
                    error = ?e,
                    collection_name = %name,
                    version_id = %version_id,
                    "Failed to deserialize schema for collection"
                );
                query::error::QueryError::execution(format!(
                    "failed to deserialize schema for collection '{}': {}",
                    name, e
                ))
            })?;

            populate_collection_root_id(systemstore, &mut schema)
                .await
                .map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to load persisted root_id for collection '{}': {}",
                        name, e
                    ))
                })?;

            let actions =
                crate::database::action::index_action_statuses(systemstore, &schema.collection_id)
                    .await
                    .map_err(|e| {
                        query::error::QueryError::execution(format!(
                            "failed to load index actions for collection '{}': {}",
                            name, e
                        ))
                    })?;

            Ok(Some(Collection::with_index_actions(schema, &actions)))
        }
        None => {
            // version_id found but schema missing - inconsistent state
            warn!(
                collection_name = %name,
                version_id = %version_id,
                "Collection name mapping exists but schema definition not found"
            );
            Ok(None)
        }
    }
}

/// Get a collection by name with lazy loading from the SystemStore.
///
/// This function checks the transaction's cache first. On cache miss, it loads
/// the collection from the SystemStore and adds it to the cache.
///
/// Returns the collection, datastore, and systemstore (for DocID <-> short-ID
/// resolution) for document operations.
pub async fn get_collection_with_lazy_load<S: Store + 'static>(
    txn: &Arc<TokioMutex<Option<DbTxn<S>>>>,
    collection_name: &str,
) -> query::error::Result<(Collection, NamespaceView, NamespaceView)> {
    // Extract what we need from the transaction while holding the lock briefly
    let (collection_opt, systemstore, datastore) = {
        let txn_guard = txn.lock().await;
        let db_txn = txn_guard
            .as_ref()
            .ok_or_else(|| query::error::QueryError::execution("transaction already consumed"))?;
        let collection_opt = db_txn.collection_cache().get(collection_name).cloned();
        let systemstore = db_txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get systemstore for collection '{}': {}",
                collection_name, e
            ))
        })?;
        let datastore = db_txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })?;
        (collection_opt, systemstore, datastore)
    };

    // Return cached collection if found
    let collection = if let Some(col) = collection_opt {
        col
    } else {
        // Cache miss: load from SystemStore
        let loaded = load_collection_from_systemstore(&systemstore, collection_name).await?;
        let collection = loaded
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        // Add to cache - warn if transaction was consumed during load
        {
            let mut txn_guard = txn.lock().await;
            match txn_guard.as_mut() {
                Some(db_txn) => {
                    db_txn.cache_collection(collection.clone());
                }
                None => {
                    warn!(
                        collection_name = %collection_name,
                        "Transaction was consumed during collection load - cache not updated"
                    );
                }
            }
        }

        collection
    };

    Ok((collection, datastore, systemstore))
}

/// Get a collection by name with lazy loading and create an IndexManager.
///
/// This function is similar to `get_collection_with_lazy_load` but also creates
/// an IndexManager for the collection, which is needed for document mutations
/// that maintain index consistency (unique constraints, etc.).
///
/// Returns the collection, datastore, systemstore, and IndexManager for
/// document operations.
pub async fn get_collection_with_index_manager<S: Store + 'static>(
    txn: &Arc<TokioMutex<Option<DbTxn<S>>>>,
    collection_name: &str,
) -> query::error::Result<(Collection, NamespaceView, NamespaceView, IndexManager)> {
    let (collection, datastore, systemstore) =
        get_collection_with_lazy_load(txn, collection_name).await?;

    // Create an IndexManager from the collection schema.
    let short_id = collection.resolved_root_id();
    let index_manager =
        IndexManager::from_indexes(short_id, collection.schema(), collection.write_indexes())
            .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to create index manager for collection '{}': {}",
                    collection_name, e
                ))
            })?;

    Ok((collection, datastore, systemstore, index_manager))
}
