//! Tests for schema_loader module.

use std::sync::Arc;

use datastore::BasicTxn;
use db::database::DB;
use db::schema_loader::load_active_collections;
use db::txn::DbTxn;
use lens::{LensConfig, LensModule};
use schema::{CollectionVersion, ORPHAN_COLLECTION_ID};
use storage::backends::MemoryStore;
use storage::corekv::Key;
use storage::keys::systemstore::{CollectionID, CollectionKey, CollectionNameKey};

#[tokio::test]
async fn test_load_empty_database() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();

    let collections = load_active_collections(&db).await.unwrap();
    assert!(
        collections.is_empty(),
        "New database should have no collections"
    );
}

#[tokio::test]
async fn test_load_single_collection() {
    let store = Arc::new(MemoryStore::new());
    let db = DB::new((*store).clone()).unwrap();

    // Manually insert a collection into systemstore
    let mut collection = CollectionVersion::new("users", "bafytest123", "bafytest123", vec![]);
    collection.root_id = 1;

    // Store collection definition
    let collection_json = serde_json::to_vec(&collection).unwrap();
    let collection_key = CollectionKey::new(&collection.version_id);
    let name_key = CollectionNameKey::new(&collection.name);
    let short_id_key = CollectionID::new(&collection.collection_id);

    {
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());

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
        txn.systemstore()
            .unwrap()
            .set(&short_id_key.bytes(), b"1")
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
    let db = DB::new((*store).clone()).unwrap();

    let collections = vec![
        ("users", "bafyuser123"),
        ("posts", "bafypost456"),
        ("comments", "bafycomment789"),
    ];

    {
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());

        for (name, id) in &collections {
            let mut collection = CollectionVersion::new(*name, *id, *id, vec![]);
            let root_id = match *name {
                "users" => 1,
                "posts" => 2,
                "comments" => 3,
                _ => unreachable!(),
            };
            collection.root_id = root_id;

            let collection_json = serde_json::to_vec(&collection).unwrap();
            let collection_key = CollectionKey::new(*id);
            let name_key = CollectionNameKey::new(*name);
            let short_id_key = CollectionID::new(*id);

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
            txn.systemstore()
                .unwrap()
                .set(&short_id_key.bytes(), root_id.to_string().as_bytes())
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
async fn test_load_collection_requires_short_id_mapping() {
    let store = Arc::new(MemoryStore::new());
    let db = DB::new((*store).clone()).unwrap();

    let collection = CollectionVersion::new("users", "bafytest123", "bafytest123", vec![]);
    let collection_json = serde_json::to_vec(&collection).unwrap();
    let collection_key = CollectionKey::new(&collection.version_id);
    let name_key = CollectionNameKey::new(&collection.name);

    {
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());

        txn.systemstore()
            .unwrap()
            .set(&collection_key.bytes(), &collection_json)
            .await
            .unwrap();
        txn.systemstore()
            .unwrap()
            .set(&name_key.bytes(), collection.version_id.as_bytes())
            .await
            .unwrap();

        txn.commit().await.unwrap();
    }

    let err = load_active_collections(&db).await.unwrap_err().to_string();
    assert!(err.contains("missing persisted collection root_id"));
}

#[tokio::test]
async fn test_load_missing_collection_definition_returns_error() {
    let store = Arc::new(MemoryStore::new());
    let db = DB::new((*store).clone()).unwrap();

    // Store only the name mapping, NOT the collection definition
    {
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());

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
async fn test_db_store_accessor_returns_shared_store() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();

    // store() should return a reference to the underlying Arc<S>
    let store_ref = db.store();
    assert!(std::sync::Arc::strong_count(store_ref) >= 1);

    // Creating a transaction should work through the same store
    let txn = db.new_txn(true).await.unwrap();
    let _ = txn.discard();
}

#[tokio::test]
async fn test_db_store_accessor_same_instance() {
    let store = MemoryStore::new();
    let db = DB::new(store).unwrap();

    // Two calls to store() should return the same Arc
    let s1 = db.store();
    let s2 = db.store();
    assert!(std::sync::Arc::ptr_eq(s1, s2));
}

#[tokio::test]
async fn test_load_invalid_json_collection_returns_error() {
    let store = Arc::new(MemoryStore::new());
    let db = DB::new((*store).clone()).unwrap();

    // Store name mapping pointing to invalid JSON
    {
        let basic_txn = BasicTxn::new(&*store, 1, false).await.unwrap();
        let txn = DbTxn::new(basic_txn, store.clone());

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

#[tokio::test]
async fn test_set_migration_placeholder_persists_short_id_mapping() {
    let store = Arc::new(MemoryStore::new());
    let db = DB::new((*store).clone()).unwrap();

    let config = LensConfig::new(
        "missing-src",
        "missing-dst",
        LensModule::from_bytes(b"\0asm\x01\0\0\0".to_vec()),
    );
    db.set_migration(config).await.unwrap();

    let versions = db.get_all_collection_versions().await.unwrap();
    assert_eq!(versions.len(), 2);

    let basic_txn = BasicTxn::new(&*store, 2, true).await.unwrap();
    let txn = DbTxn::new(basic_txn, store.clone());
    let systemstore = txn.systemstore().unwrap();
    let short_id = systemstore
        .get(&CollectionID::new(ORPHAN_COLLECTION_ID).bytes())
        .await
        .unwrap();
    let _ = txn.discard();

    assert!(
        short_id.is_some(),
        "expected placeholder root_id mapping to exist"
    );
}
