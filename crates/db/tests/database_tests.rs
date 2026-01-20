//! Tests for the DB struct.

use std::sync::Arc;

use db::database::{DbOptions, DB};
use db::Error;
use schema::{CollectionVersion, FieldDescription, FieldKind, PolicyDescription};
use storage::backends::MemoryStore;

#[tokio::test]
async fn test_db_new_txn() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    let txn = db.new_txn(false).await.unwrap();
    assert_eq!(txn.id().unwrap(), 1);

    let txn2 = db.new_txn(false).await.unwrap();
    assert_eq!(txn2.id().unwrap(), 2);
}

#[tokio::test]
async fn test_db_txn_isolation() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    // Write in first transaction
    let txn1 = db.new_txn(false).await.unwrap();
    txn1.datastore()
        .unwrap()
        .set(b"key", b"value1")
        .await
        .unwrap();
    txn1.commit().await.unwrap();

    // Read in second transaction
    let txn2 = db.new_txn(true).await.unwrap();
    let value = txn2.datastore().unwrap().get(b"key").await.unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));
}

#[tokio::test]
async fn test_db_with_txn() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    // Execute with_txn that commits
    db.with_txn(false, |_txn| {
        // Sync closure - use with_txn_async for async operations
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_db_options() {
    let store = MemoryStore::new();
    let options = DbOptions::new()
        .with_max_txn_retries(5)
        .with_chunk_size(1024 * 1024);
    let db = DB::with_options(store, options);

    assert_eq!(db.options().max_txn_retries, Some(5));
    assert_eq!(db.options().chunk_size, Some(1024 * 1024));
}

#[tokio::test]
async fn test_db_with_txn_async_commits_on_success() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    // Execute async operation that succeeds
    db.with_txn_async(false, |txn| async move {
        txn.datastore()
            .unwrap()
            .set(b"key", b"value")
            .await
            .unwrap();
        (txn, Ok(()))
    })
    .await
    .unwrap();

    // Verify data was committed
    let txn = db.new_txn(true).await.unwrap();
    let value = txn.datastore().unwrap().get(b"key").await.unwrap();
    assert_eq!(value, Some(b"value".to_vec()));
}

#[tokio::test]
async fn test_db_with_txn_async_discards_on_error() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    // Execute async operation that fails
    let result: Result<(), Error> = db
        .with_txn_async(false, |txn| async move {
            txn.datastore()
                .unwrap()
                .set(b"key", b"value")
                .await
                .unwrap();
            (txn, Err(Error::Other("test error".into())))
        })
        .await;

    assert!(result.is_err());

    // Verify data was NOT committed (discarded)
    let txn = db.new_txn(true).await.unwrap();
    let value = txn.datastore().unwrap().get(b"key").await.unwrap();
    assert_eq!(value, None);
}

fn test_users_schema() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    )
}

#[tokio::test]
async fn test_create_collection_persists_schema() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    let schema = test_users_schema();
    db.create_collection(schema).await.unwrap();

    assert!(db.has_collection("Users").unwrap());
    let coll = db.get_collection("Users").unwrap().unwrap();
    assert_eq!(coll.name(), "Users");
}

#[tokio::test]
async fn test_create_duplicate_collection_fails() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    let schema = test_users_schema();
    db.create_collection(schema.clone()).await.unwrap();

    // Second create with same name should fail
    let result = db.create_collection(schema).await;
    assert!(
        matches!(result, Err(Error::CollectionAlreadyExists(_))),
        "Expected CollectionAlreadyExists, got: {:?}",
        result
    );
}

#[tokio::test]
async fn test_create_collection_with_invalid_name_fails() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    // Empty name should fail
    let empty_schema = CollectionVersion::new(
        "",
        "v1",
        "col-empty",
        vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
    );
    let result = db.create_collection(empty_schema).await;
    assert!(
        matches!(result, Err(Error::InvalidCollectionName(_))),
        "Expected InvalidCollectionName for empty name, got: {:?}",
        result
    );

    // Name with slash should fail
    let slash_schema = CollectionVersion::new(
        "Users/Posts",
        "v1",
        "col-slash",
        vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
    );
    let result = db.create_collection(slash_schema).await;
    assert!(
        matches!(result, Err(Error::InvalidCollectionName(_))),
        "Expected InvalidCollectionName for name with slash, got: {:?}",
        result
    );
}

#[tokio::test]
async fn test_list_collections_empty() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    let collections = db.list_collections().unwrap();
    assert!(collections.is_empty());
}

#[tokio::test]
async fn test_list_collections_multiple() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    db.create_collection(test_users_schema()).await.unwrap();
    db.create_collection(CollectionVersion::new(
        "Posts",
        "v1",
        "col-posts",
        vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
    ))
    .await
    .unwrap();

    let mut collections = db.list_collections().unwrap();
    collections.sort();
    assert_eq!(collections, vec!["Posts", "Users"]);
}

#[tokio::test]
async fn test_delete_collection_removes_data() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    db.create_collection(test_users_schema()).await.unwrap();
    assert!(db.has_collection("Users").unwrap());

    db.delete_collection("Users").await.unwrap();
    assert!(!db.has_collection("Users").unwrap());
}

#[tokio::test]
async fn test_delete_nonexistent_collection_fails() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    let result = db.delete_collection("Nonexistent").await;
    assert!(matches!(result, Err(Error::CollectionNotFound(_))));
}

#[tokio::test]
async fn test_has_collection() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    assert!(!db.has_collection("Users").unwrap());

    db.create_collection(test_users_schema()).await.unwrap();
    assert!(db.has_collection("Users").unwrap());
}

#[tokio::test]
async fn test_get_collection() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    assert!(db.get_collection("Users").unwrap().is_none());

    db.create_collection(test_users_schema()).await.unwrap();
    let coll = db.get_collection("Users").unwrap().unwrap();
    assert_eq!(coll.collection_id(), "col-users");
}

#[tokio::test]
async fn test_open_loads_existing_collections() {
    let store = MemoryStore::new();

    {
        let db = DB::new(store.clone());
        db.create_collection(test_users_schema()).await.unwrap();
    }

    let db = DB::open(store).await.unwrap();
    assert!(db.has_collection("Users").unwrap());
    let coll = db.get_collection("Users").unwrap().unwrap();
    assert_eq!(coll.name(), "Users");
}

#[tokio::test]
async fn test_open_with_options_loads_existing_collections() {
    let store = MemoryStore::new();

    {
        let db = DB::new(store.clone());
        db.create_collection(test_users_schema()).await.unwrap();
    }

    // Use open_with_options with custom options
    let opts = DbOptions::new()
        .with_max_txn_retries(10)
        .with_chunk_size(1024);
    let db = DB::open_with_options(store, opts).await.unwrap();

    // Verify collections loaded correctly
    assert!(db.has_collection("Users").unwrap());
    let coll = db.get_collection("Users").unwrap().unwrap();
    assert_eq!(coll.name(), "Users");

    // Verify options were applied
    assert_eq!(db.options().max_txn_retries, Some(10));
    assert_eq!(db.options().chunk_size, Some(1024));
}

#[tokio::test]
async fn test_open_empty_store_returns_empty_collections() {
    let store = MemoryStore::new();
    let db = DB::open(store).await.unwrap();
    assert!(db.list_collections().unwrap().is_empty());
}

#[tokio::test]
async fn test_collections_snapshot() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    db.create_collection(test_users_schema()).await.unwrap();

    let snapshot = db.collections_snapshot().unwrap();
    assert_eq!(snapshot.len(), 1);
    assert!(snapshot.contains("Users"));
}

#[tokio::test]
async fn test_concurrent_create_same_collection() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));

    let schema = test_users_schema();

    let db1 = db.clone();
    let schema1 = schema.clone();
    let handle1 = tokio::spawn(async move { db1.create_collection(schema1).await });

    let db2 = db.clone();
    let schema2 = schema.clone();
    let handle2 = tokio::spawn(async move { db2.create_collection(schema2).await });

    let (r1, r2) = tokio::join!(handle1, handle2);
    let results = [r1.unwrap(), r2.unwrap()];

    let successes = results.iter().filter(|r| r.is_ok()).count();
    let failures = results.iter().filter(|r| r.is_err()).count();

    assert_eq!(successes, 1, "Exactly one concurrent create should succeed");
    assert_eq!(failures, 1, "Exactly one concurrent create should fail");

    // Cache should have exactly one collection
    assert_eq!(db.list_collections().unwrap().len(), 1);
}

#[tokio::test]
async fn test_concurrent_delete_same_collection() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));

    db.create_collection(test_users_schema()).await.unwrap();

    let db1 = db.clone();
    let handle1 = tokio::spawn(async move { db1.delete_collection("Users").await });

    let db2 = db.clone();
    let handle2 = tokio::spawn(async move { db2.delete_collection("Users").await });

    let (r1, r2) = tokio::join!(handle1, handle2);
    let results = [r1.unwrap(), r2.unwrap()];

    let successes = results.iter().filter(|r| r.is_ok()).count();
    let failures = results.iter().filter(|r| r.is_err()).count();

    assert_eq!(successes, 1, "Exactly one concurrent delete should succeed");
    assert_eq!(failures, 1, "Exactly one concurrent delete should fail");

    // Cache should be empty
    assert!(db.list_collections().unwrap().is_empty());
}

#[tokio::test]
async fn test_load_collections_corrupted_json_returns_error() {
    use storage::corekv::Key;
    use storage::keys::systemstore::CollectionNameKey;

    let store = MemoryStore::new();

    // Write corrupted JSON directly to the store
    {
        let db = DB::new(store.clone());
        let txn = db.new_txn(false).await.unwrap();

        // Use block to ensure systemstore is dropped before commit
        {
            let systemstore = txn.systemstore().unwrap();
            let key = CollectionNameKey::new("CorruptedCollection");
            systemstore
                .set(&key.bytes(), b"not valid json {{{")
                .await
                .unwrap();
        }

        txn.commit().await.unwrap();
    }

    // Try to open the database - should fail on load_collections
    let result = DB::open(store).await;
    assert!(result.is_err(), "Expected error loading corrupted JSON");
    match result {
        Err(Error::Serialization(msg)) => {
            assert!(
                msg.contains("deserialize"),
                "Error should mention deserialization: {}",
                msg
            );
        }
        Err(e) => panic!("Expected Serialization error, got: {:?}", e),
        Ok(_) => panic!("Expected error but got Ok"),
    }
}

#[tokio::test]
async fn test_delete_collection_removes_all_documents_from_store() {
    use document::{Document, NormalValue};

    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store.clone()));

    // Create collection and add documents
    db.create_collection(test_users_schema()).await.unwrap();
    let collection = db.get_collection("Users").unwrap().unwrap();

    {
        let txn = db.new_txn(false).await.unwrap();
        for i in 0..5 {
            let mut doc = Document::new();
            doc.set("name", NormalValue::String(format!("User{}", i)));
            doc.set("age", NormalValue::Int(20 + i));
            doc.generate_and_set_doc_id().unwrap();
            collection.create(&txn, &doc).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    // Verify documents exist
    {
        let txn = db.new_txn(true).await.unwrap();
        let docs = collection.get_all(&txn).await.unwrap();
        assert_eq!(docs.len(), 5, "Should have 5 documents before delete");
        txn.discard().unwrap();
    }

    // Delete the collection
    db.delete_collection("Users").await.unwrap();

    // Verify documents are gone from the store by checking raw keys
    let count = {
        let txn = db.new_txn(true).await.unwrap();
        let doc_prefix = "/d/col-users/";
        let opts = storage::corekv::IterOptions::new().with_prefix(doc_prefix.as_bytes().to_vec());

        let count = {
            let datastore = txn.datastore().unwrap();
            let mut iter = datastore.iterator(opts).await.unwrap();

            let mut c = 0;
            while iter.next().await.unwrap().is_some() {
                c += 1;
            }
            iter.close().await.unwrap();
            c
        };

        txn.discard().unwrap();
        count
    };

    assert_eq!(count, 0, "All documents should be deleted from store");
}

#[tokio::test]
async fn test_schema_roundtrip_preserves_all_fields() {
    let store = MemoryStore::new();

    let original_schema = CollectionVersion::new(
        "TestCollection",
        "v1",
        "col-test-123",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
            FieldDescription::new("4", "active", FieldKind::bool()),
        ],
    );

    // Create collection and persist
    {
        let db = DB::new(store.clone());
        db.create_collection(original_schema.clone()).await.unwrap();
    }

    // Reopen and load from store
    let db = DB::open(store).await.unwrap();
    let loaded = db
        .get_collection("TestCollection")
        .unwrap()
        .expect("Collection should exist");
    let loaded_schema = loaded.schema();

    // Verify all fields are preserved
    assert_eq!(loaded_schema.name, original_schema.name);
    assert_eq!(loaded_schema.version_id, original_schema.version_id);
    assert_eq!(loaded_schema.collection_id, original_schema.collection_id);
    assert_eq!(
        loaded_schema.fields.len(),
        original_schema.fields.len(),
        "Field count should match"
    );

    for (loaded_field, original_field) in loaded_schema
        .fields
        .iter()
        .zip(original_schema.fields.iter())
    {
        assert_eq!(loaded_field.id, original_field.id, "Field ID mismatch");
        assert_eq!(
            loaded_field.name, original_field.name,
            "Field name mismatch"
        );
    }
}

#[tokio::test]
async fn test_delete_collection_cache_store_inconsistency() {
    // Test behavior when cache and store diverge (collection in cache but not store)
    // This tests the safety check in delete_collection that verifies store existence
    use storage::corekv::Key;
    use storage::keys::systemstore::CollectionNameKey;

    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store.clone()));

    // Create a collection normally
    db.create_collection(test_users_schema()).await.unwrap();
    assert!(db.has_collection("Users").unwrap());

    // Manually delete from store, bypassing cache (simulating inconsistency)
    {
        let txn = db.new_txn(false).await.unwrap();
        let key = CollectionNameKey::new("Users");
        {
            let systemstore = txn.systemstore().unwrap();
            systemstore.delete(&key.bytes()).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    // Cache still has it
    assert!(db.has_collection("Users").unwrap());

    // Now try to delete - should fail gracefully with CollectionNotFound
    // because the store check catches the inconsistency
    let result = db.delete_collection("Users").await;
    assert!(result.is_err());
    match result {
        Err(Error::CollectionNotFound(name)) => {
            assert_eq!(name, "Users");
        }
        Err(e) => panic!("Expected CollectionNotFound, got: {:?}", e),
        Ok(_) => panic!("Expected error but got Ok"),
    }
}

#[tokio::test]
async fn test_concurrent_create_and_delete_same_collection() {
    // Test concurrent create + delete of the same collection
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));

    // Create collection first
    db.create_collection(test_users_schema()).await.unwrap();

    // Now race create and delete
    let db1 = db.clone();
    let schema = test_users_schema();
    let handle1 = tokio::spawn(async move {
        // Delete then create
        db1.delete_collection("Users").await?;
        db1.create_collection(schema).await
    });

    let db2 = db.clone();
    let handle2 = tokio::spawn(async move {
        // Just delete
        db2.delete_collection("Users").await
    });

    let (r1, r2) = tokio::join!(handle1, handle2);

    // At least one should fail (either both tried to delete, or create raced with delete)
    let r1 = r1.unwrap();
    let r2 = r2.unwrap();

    // The important thing is no panics and the database is in a consistent state
    // Either collection exists or it doesn't
    let exists = db.has_collection("Users").unwrap();
    let list = db.list_collections().unwrap();

    // If collection exists, it should be in the list
    if exists {
        assert!(list.contains(&"Users".to_string()));
    } else {
        assert!(!list.contains(&"Users".to_string()));
    }

    // Log outcomes for debugging
    println!(
        "Concurrent create+delete results: r1={:?}, r2={:?}, exists={}",
        r1.is_ok(),
        r2.is_ok(),
        exists
    );
}

/// Documents the transaction-level caching behavior.
///
/// The collection cache is per-transaction and uses lazy loading:
/// - Collections are NOT pre-loaded when a transaction starts
/// - Collections are loaded from the store on first access
/// - Once cached, the same collection instance is reused within the transaction
///
/// Note: The underlying storage layer (MemoryStore) provides its own snapshot
/// isolation, so a transaction won't see changes committed after it started.
/// The "not true snapshot isolation" comment in the doc refers to the cache
/// behavior specifically - if you start a transaction and don't access a
/// collection, it's not snapshotted. But storage-level isolation still applies.
#[tokio::test]
async fn test_transaction_cache_lazy_loading_behavior() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));

    // Create collection first
    db.create_collection(test_users_schema()).await.unwrap();

    // Start transaction A
    let txn_a = db.new_txn(true).await.unwrap();

    // Cache starts empty - collections are NOT pre-loaded
    assert!(
        txn_a.collection_cache().is_empty(),
        "Transaction starts with empty cache (lazy loading)"
    );

    // First access loads from store and caches
    {
        let systemstore = txn_a.systemstore().unwrap();
        let key = storage::keys::systemstore::CollectionNameKey::new("Users");
        let data = systemstore
            .get(&storage::corekv::Key::bytes(&key))
            .await
            .unwrap();
        assert!(data.is_some(), "Collection should be loadable from store");
    }

    // The cache loading happens through DbDocFetcher/DbDocMutator's
    // get_collection_with_lazy_load function, which:
    // 1. Checks the transaction's cache first
    // 2. On miss, loads from store and adds to cache
    // 3. On subsequent accesses, returns cached version

    txn_a.discard().unwrap();
}

/// Verifies that the storage layer provides snapshot isolation,
/// preventing transactions from seeing changes committed after they started.
#[tokio::test]
async fn test_storage_snapshot_isolation() {
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));

    // Start transaction A BEFORE any collections exist
    let txn_a = db.new_txn(true).await.unwrap();

    // Create collection in a separate transaction
    db.create_collection(test_users_schema()).await.unwrap();

    // Transaction A (started before collection existed) should NOT see
    // the new collection due to storage-level snapshot isolation
    {
        let systemstore = txn_a.systemstore().unwrap();
        let key = storage::keys::systemstore::CollectionNameKey::new("Users");
        let data = systemstore
            .get(&storage::corekv::Key::bytes(&key))
            .await
            .unwrap();
        assert!(
            data.is_none(),
            "Storage snapshot isolation: txn A should NOT see collection created after it started"
        );
    }

    txn_a.discard().unwrap();

    // New transaction SHOULD see the collection
    let txn_b = db.new_txn(true).await.unwrap();
    {
        let systemstore = txn_b.systemstore().unwrap();
        let key = storage::keys::systemstore::CollectionNameKey::new("Users");
        let data = systemstore
            .get(&storage::corekv::Key::bytes(&key))
            .await
            .unwrap();
        assert!(
            data.is_some(),
            "New transaction should see previously committed collection"
        );
    }
    txn_b.discard().unwrap();
}

#[tokio::test]
async fn test_reload_cache_recovers_from_inconsistency() {
    // Test that reload_cache() can recover from cache-store inconsistency
    use storage::corekv::Key;
    use storage::keys::systemstore::CollectionNameKey;

    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store.clone()));

    // Create a collection normally
    db.create_collection(test_users_schema()).await.unwrap();
    assert!(db.has_collection("Users").unwrap());

    // Simulate cache-store divergence by directly adding to store
    {
        let txn = db.new_txn(false).await.unwrap();
        let schema = CollectionVersion::new(
            "HiddenCollection",
            "v1",
            "col-hidden",
            vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
        );
        let key = CollectionNameKey::new("HiddenCollection");
        {
            let systemstore = txn.systemstore().unwrap();
            let data = serde_json::to_vec(&schema).unwrap();
            systemstore.set(&key.bytes(), &data).await.unwrap();
        }
        txn.commit().await.unwrap();
    }

    // Cache doesn't know about HiddenCollection
    assert!(!db.has_collection("HiddenCollection").unwrap());
    assert_eq!(db.list_collections().unwrap().len(), 1);

    // Reload cache from store
    db.reload_cache().await.unwrap();

    // Now cache should reflect store state
    assert!(db.has_collection("Users").unwrap());
    assert!(db.has_collection("HiddenCollection").unwrap());
    assert_eq!(db.list_collections().unwrap().len(), 2);
}

#[tokio::test]
async fn test_db_from_arc_creates_working_database() {
    let store = Arc::new(MemoryStore::new());
    let db = DB::from_arc(store);

    // Verify transactions work
    let txn = db.new_txn(false).await.unwrap();
    assert_eq!(txn.id().unwrap(), 1);

    let txn2 = db.new_txn(false).await.unwrap();
    assert_eq!(txn2.id().unwrap(), 2);
}

#[tokio::test]
async fn test_db_from_arc_with_options() {
    let store = Arc::new(MemoryStore::new());
    let options = DbOptions::new()
        .with_max_txn_retries(10)
        .with_chunk_size(2048);
    let db = DB::from_arc_with_options(store, options);

    assert_eq!(db.options().max_txn_retries, Some(10));
    assert_eq!(db.options().chunk_size, Some(2048));
}

#[tokio::test]
async fn test_db_from_arc_shares_store_with_caller() {
    let store = Arc::new(MemoryStore::new());
    let db = DB::from_arc(store.clone());

    // Write via db
    let txn = db.new_txn(false).await.unwrap();
    txn.datastore()
        .unwrap()
        .set(b"shared_key", b"shared_value")
        .await
        .unwrap();
    txn.commit().await.unwrap();

    // Verify data is visible via same store (proves Arc sharing works)
    let db2 = DB::from_arc(store);
    let txn2 = db2.new_txn(true).await.unwrap();
    let value = txn2.datastore().unwrap().get(b"shared_key").await.unwrap();
    assert_eq!(value, Some(b"shared_value".to_vec()));
}

#[tokio::test]
async fn test_db_from_arc_txn_isolation() {
    let store = Arc::new(MemoryStore::new());
    let db = DB::from_arc(store);

    // Write in first transaction
    let txn1 = db.new_txn(false).await.unwrap();
    txn1.datastore()
        .unwrap()
        .set(b"key", b"value1")
        .await
        .unwrap();
    txn1.commit().await.unwrap();

    // Read in second transaction
    let txn2 = db.new_txn(true).await.unwrap();
    let value = txn2.datastore().unwrap().get(b"key").await.unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));
}

/// Integration test demonstrating the complete Merkle proof workflow:
/// 1. Create a database with a chain of blocks
/// 2. Extract a proof from leaf to root
/// 3. Verify the proof
/// 4. Sign the proof
/// 5. Verify the signed proof
#[tokio::test]
async fn test_merkle_proof_full_workflow() {
    use blockstore::{Blockstore, DefraBlockstore};
    use crypto::PrivateKey;
    use defra_core::block::{Block, CrdtDelta, LwwDeltaPayload};

    // Create database and blockstore
    let store = MemoryStore::new();
    let db = DB::new(store.clone());
    let blockstore = DefraBlockstore::new(Arc::new(store), false);

    // Helper to create a test delta
    fn create_delta(doc_id: &str, field: &str) -> CrdtDelta {
        CrdtDelta::Lww(LwwDeltaPayload {
            doc_id: doc_id.as_bytes().to_vec(),
            field_name: field.to_string(),
            priority: 1,
            schema_version_id: "v1".to_string(),
            data: b"test".to_vec(),
        })
    }

    // Create a chain of blocks: root <- block1 <- leaf
    let root = Block::new(create_delta("doc1", "v1"), vec![], vec![]);
    let root_cid = root.generate_cid().unwrap();
    let root_data = root.to_dag_cbor().unwrap();
    blockstore.put(&root_cid, &root_data).await.unwrap();

    let block1 = Block::new(create_delta("doc1", "v2"), vec![root_cid], vec![]);
    let block1_cid = block1.generate_cid().unwrap();
    let block1_data = block1.to_dag_cbor().unwrap();
    blockstore.put(&block1_cid, &block1_data).await.unwrap();

    let leaf = Block::new(create_delta("doc1", "v3"), vec![block1_cid], vec![]);
    let leaf_cid = leaf.generate_cid().unwrap();
    let leaf_data = leaf.to_dag_cbor().unwrap();
    blockstore.put(&leaf_cid, &leaf_data).await.unwrap();

    // Step 2: Extract proof from leaf to root
    let proof = db
        .extract_proof(&leaf_cid, &root_cid)
        .await
        .expect("extract_proof should not error")
        .expect("proof should exist");

    // Verify proof structure
    assert_eq!(proof.leaf_cid, leaf_cid);
    assert_eq!(proof.root_cid, root_cid);
    assert_eq!(proof.len(), 3, "Chain has 3 blocks: leaf -> block1 -> root");

    // Step 3: Verify the proof
    assert!(proof.verify().unwrap(), "Extracted proof should be valid");

    // Step 4: Sign the proof with Ed25519
    let private_key = crypto::generate_ed25519().unwrap();
    let signed_proof = db
        .extract_signed_proof(
            &leaf_cid,
            &root_cid,
            &private_key as &dyn crypto::PrivateKey,
        )
        .await
        .expect("extract_signed_proof should not error")
        .expect("signed proof should exist");

    // Step 5: Verify the signed proof
    assert!(
        signed_proof.verify_with_embedded_key().unwrap(),
        "Signed proof should verify with embedded key"
    );

    // Also verify with explicit public key
    let public_key = private_key.public_key();
    assert!(
        signed_proof.verify(public_key.as_ref()).unwrap(),
        "Signed proof should verify with explicit public key"
    );

    // Verify the underlying proof is still valid
    assert!(
        signed_proof.proof.verify().unwrap(),
        "Underlying proof should be valid"
    );

    // Test DAG-CBOR serialization roundtrip
    let proof_bytes = proof.to_dag_cbor().unwrap();
    let restored_proof = crypto::MerkleProof::from_dag_cbor(&proof_bytes).unwrap();
    assert_eq!(
        proof, restored_proof,
        "Proof should roundtrip through DAG-CBOR"
    );
}

#[tokio::test]
async fn test_merkle_proof_no_path_returns_none() {
    use blockstore::{Blockstore, DefraBlockstore};
    use defra_core::block::{Block, CrdtDelta, LwwDeltaPayload};

    // Create database and blockstore
    let store = MemoryStore::new();
    let db = DB::new(store.clone());
    let blockstore = DefraBlockstore::new(Arc::new(store), false);

    fn create_delta(doc_id: &str, field: &str) -> CrdtDelta {
        CrdtDelta::Lww(LwwDeltaPayload {
            doc_id: doc_id.as_bytes().to_vec(),
            field_name: field.to_string(),
            priority: 1,
            schema_version_id: "v1".to_string(),
            data: b"test".to_vec(),
        })
    }

    // Create two unrelated blocks (not connected)
    let block1 = Block::new(create_delta("doc1", "v1"), vec![], vec![]);
    let cid1 = block1.generate_cid().unwrap();
    blockstore
        .put(&cid1, &block1.to_dag_cbor().unwrap())
        .await
        .unwrap();

    let block2 = Block::new(create_delta("doc2", "v1"), vec![], vec![]);
    let cid2 = block2.generate_cid().unwrap();
    blockstore
        .put(&cid2, &block2.to_dag_cbor().unwrap())
        .await
        .unwrap();

    // Try to extract proof between unrelated blocks
    let result = db.extract_proof(&cid1, &cid2).await.unwrap();
    assert!(result.is_none(), "Should return None for unrelated blocks");
}

// =========================================================================
// Policy Validation at DB Layer Tests
// =========================================================================

#[tokio::test]
async fn test_create_collection_rejects_invalid_policy_path_separator() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    let schema = CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
    .with_policy(PolicyDescription::new("policy/traversal", "users"));

    let result = db.create_collection(schema).await;
    assert!(result.is_err(), "Should reject policy with path separator");
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::Schema(_)),
        "Expected Schema error, got: {:?}",
        err
    );
    assert!(
        err.to_string().contains("path separators"),
        "Error should mention path separators: {}",
        err
    );
}

#[tokio::test]
async fn test_create_collection_rejects_invalid_policy_dotdot() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    let schema = CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
    .with_policy(PolicyDescription::new("policy..secret", "users"));

    let result = db.create_collection(schema).await;
    assert!(
        result.is_err(),
        "Should reject policy with '..' sequence"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("'..'"),
        "Error should mention '..' sequences: {}",
        err
    );
}

#[tokio::test]
async fn test_create_collection_rejects_invalid_policy_null_byte() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    let schema = CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
    .with_policy(PolicyDescription::new("policy\0123", "users"));

    let result = db.create_collection(schema).await;
    assert!(result.is_err(), "Should reject policy with null byte");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("null bytes"),
        "Error should mention null bytes: {}",
        err
    );
}

#[tokio::test]
async fn test_create_collection_accepts_valid_policy() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    let schema = CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
    .with_policy(PolicyDescription::new("policy-123", "users"));

    let result = db.create_collection(schema).await;
    assert!(result.is_ok(), "Should accept valid policy");
    assert!(db.has_collection("Users").unwrap());
}

// =========================================================================
// Node Identity Tests
// =========================================================================

#[tokio::test]
async fn test_db_without_node_identity() {
    let store = MemoryStore::new();
    let db = DB::new(store);

    assert!(!db.has_node_identity());
    assert!(db.node_identity().is_none());
}

#[tokio::test]
async fn test_db_with_node_identity() {
    use identity::{Identity, RawIdentity};

    let store = MemoryStore::new();
    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();
    let expected_did = identity.did().unwrap();

    let options = DbOptions::new().with_node_identity(identity);
    let db = DB::with_options(store, options);

    assert!(db.has_node_identity());
    let node_id = db.node_identity().expect("should have identity");
    assert_eq!(node_id.did().unwrap(), expected_did);
}

#[tokio::test]
async fn test_db_options_builder_pattern() {
    use identity::RawIdentity;

    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();

    let options = DbOptions::new()
        .with_max_txn_retries(10)
        .with_chunk_size(1024)
        .with_node_identity(identity);

    assert_eq!(options.max_txn_retries, Some(10));
    assert_eq!(options.chunk_size, Some(1024));
    assert!(options.node_identity.is_some());
}

#[tokio::test]
async fn test_db_options_with_arc_identity() {
    use identity::RawIdentity;

    let private_key = crypto::generate_ed25519().unwrap();
    let identity = Arc::new(RawIdentity::from_private_key(private_key).unwrap());
    let arc_clone = identity.clone();

    let options = DbOptions::new().with_node_identity_arc(identity);

    // Verify the Arc is shared
    let stored_arc = options.node_identity.unwrap();
    assert!(Arc::ptr_eq(&stored_arc, &arc_clone));
}

#[tokio::test]
async fn test_db_open_with_node_identity() {
    use identity::{Identity, RawIdentity};

    let store = MemoryStore::new();
    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();
    let expected_did = identity.did().unwrap();

    let options = DbOptions::new().with_node_identity(identity);
    let db = DB::open_with_options(store, options).await.unwrap();

    assert!(db.has_node_identity());
    let node_id = db.node_identity().expect("should have identity");
    assert_eq!(node_id.did().unwrap(), expected_did);
}

#[tokio::test]
async fn test_db_from_arc_with_node_identity() {
    use identity::{Identity, RawIdentity};

    let store = Arc::new(MemoryStore::new());
    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();
    let expected_did = identity.did().unwrap();

    let options = DbOptions::new().with_node_identity(identity);
    let db = DB::from_arc_with_options(store, options);

    assert!(db.has_node_identity());
    let node_id = db.node_identity().expect("should have identity");
    assert_eq!(node_id.did().unwrap(), expected_did);
}

#[tokio::test]
async fn test_db_node_identity_can_sign() {
    use identity::{FullIdentity, Identity, RawIdentity};

    let store = MemoryStore::new();
    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();

    let options = DbOptions::new().with_node_identity(identity);
    let db = DB::with_options(store, options);

    let node_id = db.node_identity().expect("should have identity");
    let message = b"test message";
    let signature = node_id.sign(message).unwrap();

    // Verify signature using the public key
    let verified = node_id.pub_key().verify(message, &signature).unwrap();
    assert!(verified, "Signature should verify");
}

#[tokio::test]
async fn test_db_options_debug_shows_did() {
    use identity::RawIdentity;

    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();

    let options = DbOptions::new().with_node_identity(identity);
    let debug_str = format!("{:?}", options);

    assert!(debug_str.contains("did:key:"), "Debug should show DID");
}
