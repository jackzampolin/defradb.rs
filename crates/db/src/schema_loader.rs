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
pub async fn load_active_collections<S: Store>(db: &DB<S>) -> Result<Vec<CollectionVersion>> {
    let txn = db.new_txn(true).await?;
    let systemstore = txn.systemstore()?;

    let mut collections = Vec::new();

    // Iterate over all collection name mappings
    let prefix = CollectionNameKey::name_prefix();
    let opts = IterOptions::new().with_prefix(prefix);
    let mut iter = systemstore.iterator(opts).await.map_err(Error::Storage)?;

    while let Some(kv) = iter.next().await.map_err(Error::Storage)? {
        // The value at /collection/name/<name> is the collection ID
        let collection_id = String::from_utf8(kv.value)
            .map_err(|e| Error::Other(format!("Invalid collection ID encoding: {}", e)))?;

        // Look up the full collection definition
        let collection_key = CollectionKey::new(&collection_id);
        let collection_json = systemstore
            .get(&collection_key.bytes())
            .await
            .map_err(Error::Storage)?
            .ok_or_else(|| {
                Error::Other(format!(
                    "Collection definition not found for ID: {}",
                    collection_id
                ))
            })?;

        // Deserialize the collection
        let collection: CollectionVersion =
            serde_json::from_slice(&collection_json).map_err(|e| {
                Error::Other(format!(
                    "Failed to deserialize collection {}: {}",
                    collection_id, e
                ))
            })?;

        collections.push(collection);
    }

    iter.close().await.map_err(Error::Storage)?;

    // Read-only transaction - no need to commit or discard explicitly
    // The transaction will be cleaned up when it's dropped
    drop(txn);

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
    use storage::backends::MemoryStore;
    use storage::corekv::Key;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_load_empty_database() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        let collections = load_active_collections(&db).await.unwrap();
        assert!(collections.is_empty(), "New database should have no collections");
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
}
