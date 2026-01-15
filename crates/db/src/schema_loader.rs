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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DB;
    use datastore::BasicTxn;
    use std::sync::Arc;
    use storage::backends::MemoryStore;
    use storage::corekv::Key;

    #[tokio::test]
    async fn test_load_empty_database() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        let collections = load_active_collections(&db).await.unwrap();
        assert!(
            collections.is_empty(),
            "New database should have no collections"
        );
    }

    #[tokio::test]
    async fn test_load_single_collection() {
        let store = Arc::new(MemoryStore::new());
        let db = DB::new((*store).clone());

        // Manually insert a collection into systemstore
        let collection = CollectionVersion::new("users", "bafytest123", "bafytest123", vec![]);

        // Store collection definition
        let collection_json = serde_json::to_vec(&collection).unwrap();
        let collection_key = CollectionKey::new(&collection.version_id);
        let name_key = CollectionNameKey::new(&collection.name);

        {
            let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
            let txn = crate::txn::DbTxn::new(basic_txn, store.clone());

            // Store the collection definition at /collection/id/<id>
            txn.systemstore()
                .unwrap()
                .set(&collection_key.bytes(), &collection_json)
                .await
                .unwrap();

            // Store the name -> id mapping at /collection/name/<name>
            txn.systemstore()
                .unwrap()
                .set(&name_key.bytes(), collection.version_id.as_bytes())
                .await
                .unwrap();

            txn.commit().await.unwrap();
        }

        // Now load collections
        let loaded = load_active_collections(&db).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "users");
        assert_eq!(loaded[0].version_id, "bafytest123");
    }

    #[tokio::test]
    async fn test_load_multiple_collections() {
        let store = Arc::new(MemoryStore::new());
        let db = DB::new((*store).clone());

        let collections = vec![
            ("users", "bafyuser123"),
            ("posts", "bafypost456"),
            ("comments", "bafycomment789"),
        ];

        {
            let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
            let txn = crate::txn::DbTxn::new(basic_txn, store.clone());

            for (name, id) in &collections {
                let collection = CollectionVersion::new(*name, *id, *id, vec![]);

                let collection_json = serde_json::to_vec(&collection).unwrap();
                let collection_key = CollectionKey::new(*id);
                let name_key = CollectionNameKey::new(*name);

                txn.systemstore()
                    .unwrap()
                    .set(&collection_key.bytes(), &collection_json)
                    .await
                    .unwrap();
                txn.systemstore()
                    .unwrap()
                    .set(&name_key.bytes(), id.as_bytes())
                    .await
                    .unwrap();
            }

            txn.commit().await.unwrap();
        }

        let loaded = load_active_collections(&db).await.unwrap();
        assert_eq!(loaded.len(), 3);

        // Verify all collections were loaded
        let names: Vec<&str> = loaded.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"users"));
        assert!(names.contains(&"posts"));
        assert!(names.contains(&"comments"));
    }

    #[tokio::test]
    async fn test_load_missing_collection_definition_returns_error() {
        let store = Arc::new(MemoryStore::new());
        let db = DB::new((*store).clone());

        // Store only the name mapping, NOT the collection definition
        {
            let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
            let txn = crate::txn::DbTxn::new(basic_txn, store.clone());

            let name_key = CollectionNameKey::new("orphan_collection");
            txn.systemstore()
                .unwrap()
                .set(&name_key.bytes(), b"missing_id_123")
                .await
                .unwrap();

            txn.commit().await.unwrap();
        }

        let result = load_active_collections(&db).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not found"),
            "Error should mention 'not found', got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_load_invalid_json_collection_returns_error() {
        let store = Arc::new(MemoryStore::new());
        let db = DB::new((*store).clone());

        // Store name mapping pointing to invalid JSON
        {
            let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
            let txn = crate::txn::DbTxn::new(basic_txn, store.clone());

            let name_key = CollectionNameKey::new("bad_collection");
            let collection_key = CollectionKey::new("bad_id_456");

            // Store name -> id mapping
            txn.systemstore()
                .unwrap()
                .set(&name_key.bytes(), b"bad_id_456")
                .await
                .unwrap();

            // Store invalid JSON as collection definition
            txn.systemstore()
                .unwrap()
                .set(&collection_key.bytes(), b"{ invalid json }")
                .await
                .unwrap();

            txn.commit().await.unwrap();
        }

        let result = load_active_collections(&db).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("deserialize"),
            "Error should mention 'deserialize', got: {}",
            err_msg
        );
    }
}
