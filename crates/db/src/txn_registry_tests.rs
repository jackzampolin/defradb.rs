use super::*;
use document::NormalValue;
use lens::{LensConfig, LensModule};
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;

fn test_schema() -> Vec<CollectionVersion> {
    vec![CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    )]
}

/// Create a test DB with collections pre-registered.
async fn test_db_with_collections() -> Arc<DB<MemoryStore>> {
    let db = Arc::new(DB::new(MemoryStore::new()).unwrap());
    for schema in test_schema() {
        db.create_collection(schema).await.unwrap();
    }
    db
}

fn empty_wasm_module() -> LensModule {
    LensModule::from_bytes(vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00])
}

#[tokio::test]
async fn test_begin_transaction() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(false).await.unwrap();
    assert!(!txn_id.as_str().is_empty());
}

#[tokio::test]
async fn test_begin_readonly_transaction() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(true).await.unwrap();
    let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
    assert!(ctx.is_readonly());
}

#[tokio::test]
async fn test_begin_readwrite_transaction() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(false).await.unwrap();
    let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
    assert!(!ctx.is_readonly());
}

#[tokio::test]
async fn test_transaction_id_matches() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(false).await.unwrap();
    let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
    assert_eq!(ctx.id(), txn_id.as_str());
}

#[tokio::test]
async fn test_begin_and_commit() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(false).await.unwrap();
    let result = registry.commit(&txn_id).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_begin_and_rollback() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(false).await.unwrap();
    let result = registry.rollback(&txn_id).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_commit_nonexistent_returns_error() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let nonexistent: TransactionHandle = "nonexistent".parse().unwrap();
    let result = registry.commit(&nonexistent).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[tokio::test]
async fn test_rollback_nonexistent_returns_error() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let nonexistent: TransactionHandle = "nonexistent".parse().unwrap();
    let result = registry.rollback(&nonexistent).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[tokio::test]
async fn test_get_nonexistent_returns_not_found() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let nonexistent: TransactionHandle = "nonexistent".parse().unwrap();
    assert!(matches!(
        registry.get(&nonexistent),
        GetTransactionResult::NotFound
    ));
}

#[tokio::test]
async fn test_double_commit_returns_error() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(false).await.unwrap();
    registry.commit(&txn_id).await.unwrap();

    let result = registry.commit(&txn_id).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_double_rollback_returns_error() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(false).await.unwrap();
    registry.rollback(&txn_id).await.unwrap();

    let result = registry.rollback(&txn_id).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_commit_after_rollback_returns_error() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(false).await.unwrap();
    registry.rollback(&txn_id).await.unwrap();

    let result = registry.commit(&txn_id).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_rollback_after_commit_returns_error() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(false).await.unwrap();
    registry.commit(&txn_id).await.unwrap();

    let result = registry.rollback(&txn_id).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[tokio::test]
async fn test_get_returns_not_found_after_commit() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(false).await.unwrap();
    assert!(registry.get(&txn_id).is_found());

    registry.commit(&txn_id).await.unwrap();
    assert!(matches!(
        registry.get(&txn_id),
        GetTransactionResult::NotFound
    ));
}

#[tokio::test]
async fn test_get_returns_not_found_after_rollback() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(false).await.unwrap();
    assert!(registry.get(&txn_id).is_found());

    registry.rollback(&txn_id).await.unwrap();
    assert!(matches!(
        registry.get(&txn_id),
        GetTransactionResult::NotFound
    ));
}

#[tokio::test]
async fn test_set_migration_in_txn_is_only_visible_inside_transaction() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db.clone());

    let txn_id = registry.begin(false).await.unwrap();
    let config = LensConfig::new("users-v1", "users-v2", empty_wasm_module());

    let transform_id = registry
        .set_migration_in_txn(&txn_id, config, None)
        .await
        .unwrap();

    let global_lenses = db.lens_store().list().await.unwrap();
    assert!(
        !global_lenses.contains_key(&transform_id.to_string()),
        "uncommitted migration leaked into global lens list"
    );

    let txn_lenses = registry.list_lenses_in_txn(&txn_id).await.unwrap();
    assert!(
        txn_lenses.contains_key(&transform_id.to_string()),
        "transaction-local migration should be visible inside the transaction"
    );

    registry.rollback(&txn_id).await.unwrap();

    let global_after_rollback = db.lens_store().list().await.unwrap();
    assert!(
        !global_after_rollback.contains_key(&transform_id.to_string()),
        "rolled back migration leaked into global lens list"
    );
}

#[tokio::test]
async fn test_set_migration_in_txn_promotes_transform_on_commit() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db.clone());

    let txn_id = registry.begin(false).await.unwrap();
    let config = LensConfig::new("users-v1", "users-v2", empty_wasm_module());

    let transform_id = registry
        .set_migration_in_txn(&txn_id, config, None)
        .await
        .unwrap();

    registry.commit(&txn_id).await.unwrap();

    let global_lenses = db.lens_store().list().await.unwrap();
    assert!(
        global_lenses.contains_key(&transform_id.to_string()),
        "committed migration should be promoted into the global lens store"
    );
}

#[tokio::test]
async fn test_doc_fetcher_get_all_empty_collection() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(true).await.unwrap();
    let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
    let fetcher = ctx.doc_fetcher();

    let docs = fetcher.get_all("Users").await.unwrap();
    assert!(docs.is_empty());

    registry.rollback(&txn_id).await.unwrap();
}

#[tokio::test]
async fn test_doc_fetcher_get_all_unknown_collection() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(true).await.unwrap();
    let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
    let fetcher = ctx.doc_fetcher();

    let result = fetcher.get_all("NonExistent").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("NonExistent"));

    registry.rollback(&txn_id).await.unwrap();
}

#[tokio::test]
async fn test_doc_fetcher_get_by_ids_empty() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(true).await.unwrap();
    let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
    let fetcher = ctx.doc_fetcher();

    let result = fetcher.get_by_ids("Users", &[]).await.unwrap();
    assert!(result.docs().is_empty());
    assert!(result.missing_ids().is_empty());

    registry.rollback(&txn_id).await.unwrap();
}

#[tokio::test]
async fn test_doc_fetcher_get_by_ids_invalid_id_treated_as_not_found() {
    // Go DefraDB treats invalid doc IDs as "not found" rather than errors.
    // This matches behavior where querying for a non-existent ID returns empty results.
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(true).await.unwrap();
    let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
    let fetcher = ctx.doc_fetcher();

    let result = fetcher
        .get_by_ids("Users", &["not-a-valid-docid".to_string()])
        .await
        .unwrap();
    // Invalid doc ID is treated as not found, not an error
    assert!(result.docs().is_empty());
    assert_eq!(result.missing_ids(), &["not-a-valid-docid".to_string()]);

    registry.rollback(&txn_id).await.unwrap();
}

#[tokio::test]
async fn test_is_consumed_returns_false_before_take() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(true).await.unwrap();
    let ctx = registry.get_ctx(&txn_id).unwrap().unwrap();

    assert!(
        !ctx.is_consumed().await,
        "Transaction should not be consumed before take_txn"
    );

    registry.rollback(&txn_id).await.unwrap();
}

#[tokio::test]
async fn test_is_consumed_returns_true_after_take() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(true).await.unwrap();
    let ctx = registry.get_ctx(&txn_id).unwrap().unwrap();

    // Take the transaction
    let _txn = ctx.take_txn().await;

    assert!(
        ctx.is_consumed().await,
        "Transaction should be consumed after take_txn"
    );
}

#[tokio::test]
async fn test_doc_fetcher_after_txn_consumed_returns_error() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(true).await.unwrap();
    let ctx = registry.get_ctx(&txn_id).unwrap().unwrap();
    let fetcher = ctx.doc_fetcher();

    // Manually take the transaction to simulate commit/rollback having consumed it
    let _txn = ctx.take_txn().await;

    // Now try to use the fetcher - should fail with "transaction already consumed"
    let result = fetcher.get_all("Users").await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("transaction already consumed"));
}

#[tokio::test]
async fn test_transaction_sees_committed_data() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db.clone());
    let collection = db.get_collection("Users").unwrap().unwrap();

    // Write data in a separate transaction
    let write_txn = db.new_txn(false).await.unwrap();
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));
    doc.set("age", NormalValue::Int(30));
    doc.generate_and_set_doc_id().unwrap();
    collection.create(&write_txn, &doc).await.unwrap();
    write_txn.commit().await.unwrap();

    // Read via registry
    let txn_id = registry.begin(true).await.unwrap();
    let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
    let fetcher = ctx.doc_fetcher();

    let docs = fetcher.get_all("Users").await.unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].get("name").unwrap().as_str(), Some("Alice"));

    registry.rollback(&txn_id).await.unwrap();
}

#[tokio::test]
async fn test_get_by_ids_returns_matching_docs() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db.clone());
    let collection = db.get_collection("Users").unwrap().unwrap();

    // Create two documents
    let write_txn = db.new_txn(false).await.unwrap();

    let mut doc1 = Document::new();
    doc1.set("name", NormalValue::String("Alice".to_string()));
    doc1.generate_and_set_doc_id().unwrap();
    let doc1_id = doc1.id().unwrap().to_string();
    collection.create(&write_txn, &doc1).await.unwrap();

    let mut doc2 = Document::new();
    doc2.set("name", NormalValue::String("Bob".to_string()));
    doc2.generate_and_set_doc_id().unwrap();
    collection.create(&write_txn, &doc2).await.unwrap();

    write_txn.commit().await.unwrap();

    // Query for just one document
    let txn_id = registry.begin(true).await.unwrap();
    let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
    let fetcher = ctx.doc_fetcher();

    let result = fetcher.get_by_ids("Users", &[doc1_id]).await.unwrap();
    assert_eq!(result.docs().len(), 1);
    assert!(result.missing_ids().is_empty());
    assert_eq!(
        result.docs()[0].get("name").unwrap().as_str(),
        Some("Alice")
    );

    registry.rollback(&txn_id).await.unwrap();
}

#[tokio::test]
async fn test_multiple_concurrent_transactions() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn1 = registry.begin(true).await.unwrap();
    let txn2 = registry.begin(true).await.unwrap();
    let txn3 = registry.begin(false).await.unwrap();

    assert!(registry.get(&txn1).is_found());
    assert!(registry.get(&txn2).is_found());
    assert!(registry.get(&txn3).is_found());

    assert_ne!(txn1, txn2);
    assert_ne!(txn2, txn3);

    registry.rollback(&txn1).await.unwrap();
    registry.rollback(&txn2).await.unwrap();
    registry.rollback(&txn3).await.unwrap();
}

#[tokio::test]
async fn test_transaction_ids_are_unique() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let mut ids = Vec::new();
    for _ in 0..10 {
        let txn_id = registry.begin(true).await.unwrap();
        assert!(!ids.contains(&txn_id), "Duplicate ID: {}", txn_id);
        ids.push(txn_id);
    }

    for id in ids {
        registry.rollback(&id).await.unwrap();
    }
}

#[tokio::test]
async fn test_rollback_discards_uncommitted_writes() {
    let db = test_db_with_collections().await;
    let collection = db.get_collection("Users").unwrap().unwrap();

    // Write data in a transaction but rollback instead of commit
    let write_txn = db.new_txn(false).await.unwrap();
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("RollbackMe".to_string()));
    doc.set("age", NormalValue::Int(99));
    doc.generate_and_set_doc_id().unwrap();
    collection.create(&write_txn, &doc).await.unwrap();

    write_txn.force_discard().unwrap();

    // Verify data was NOT persisted
    let read_txn = db.new_txn(true).await.unwrap();
    let all_docs = collection.get_all(&read_txn).await.unwrap();
    assert!(
        all_docs.is_empty(),
        "Rolled-back data should not be visible, found {} docs",
        all_docs.len()
    );
    read_txn.force_discard().unwrap();
}

#[tokio::test]
async fn test_transaction_does_not_see_uncommitted_writes() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db.clone());
    let collection = db.get_collection("Users").unwrap().unwrap();

    // Start a reader transaction FIRST
    let reader_txn_id = registry.begin(true).await.unwrap();
    let reader_ctx = registry.get(&reader_txn_id).into_result().unwrap().unwrap();
    let reader_fetcher = reader_ctx.doc_fetcher();

    // Start a writer transaction and write WITHOUT committing
    let write_txn = db.new_txn(false).await.unwrap();
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Uncommitted".to_string()));
    doc.generate_and_set_doc_id().unwrap();
    collection.create(&write_txn, &doc).await.unwrap();

    // Reader should NOT see the uncommitted write
    let docs = reader_fetcher.get_all("Users").await.unwrap();
    assert!(
        docs.is_empty(),
        "Reader should not see uncommitted writes (dirty read protection)"
    );

    write_txn.force_discard().unwrap();
    registry.rollback(&reader_txn_id).await.unwrap();
}

#[tokio::test]
async fn test_concurrent_parallel_transaction_operations() {
    let db = test_db_with_collections().await;
    let registry = Arc::new(DbTransactionRegistry::new(db));

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let reg = registry.clone();
            tokio::spawn(async move {
                let txn_id = reg.begin(true).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;

                assert!(
                    reg.get(&txn_id).is_found(),
                    "Task {} should find its transaction",
                    i
                );

                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                reg.rollback(&txn_id).await.unwrap();

                assert!(
                    !reg.get(&txn_id).is_found(),
                    "Task {} transaction should be gone after rollback",
                    i
                );
            })
        })
        .collect();

    for handle in handles {
        handle.await.expect("Task should complete without panic");
    }
}

#[tokio::test]
async fn test_doc_fetcher_get_by_ids_unknown_collection() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn_id = registry.begin(true).await.unwrap();
    let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
    let fetcher = ctx.doc_fetcher();

    let result = fetcher
        .get_by_ids("NonExistent", &["some-id".to_string()])
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("NonExistent"));

    registry.rollback(&txn_id).await.unwrap();
}

#[tokio::test]
async fn test_get_by_ids_with_nonexistent_valid_id() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db.clone());
    let collection = db.get_collection("Users").unwrap().unwrap();

    // Create one document
    let write_txn = db.new_txn(false).await.unwrap();
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Exists".to_string()));
    doc.generate_and_set_doc_id().unwrap();
    let existing_id = doc.id().unwrap().to_string();
    collection.create(&write_txn, &doc).await.unwrap();
    write_txn.commit().await.unwrap();

    // Create a valid-format ID that doesn't exist
    let mut nonexistent_doc = Document::new();
    nonexistent_doc.set("name", NormalValue::String("Ghost".to_string()));
    nonexistent_doc.generate_and_set_doc_id().unwrap();
    let nonexistent_id = nonexistent_doc.id().unwrap().to_string();

    // Query for both
    let txn_id = registry.begin(true).await.unwrap();
    let ctx = registry.get(&txn_id).into_result().unwrap().unwrap();
    let fetcher = ctx.doc_fetcher();

    let result = fetcher
        .get_by_ids("Users", &[existing_id, nonexistent_id.clone()])
        .await
        .unwrap();

    assert_eq!(
        result.docs().len(),
        1,
        "Should only return existing document"
    );
    assert_eq!(
        result.docs()[0].get("name").unwrap().as_str(),
        Some("Exists")
    );

    // Verify missing IDs are reported
    assert_eq!(
        result.missing_ids().len(),
        1,
        "Should report one missing ID"
    );
    assert_eq!(result.missing_ids()[0], nonexistent_id);
    assert!(!result.is_complete(), "Result should not be complete");

    registry.rollback(&txn_id).await.unwrap();
}

#[tokio::test]
async fn test_concurrent_commit_same_transaction() {
    let db = test_db_with_collections().await;
    let registry = Arc::new(DbTransactionRegistry::new(db));

    let txn_id = registry.begin(false).await.unwrap();

    // Spawn two tasks trying to commit the same transaction
    let reg1 = registry.clone();
    let txn1 = txn_id.clone();
    let handle1 = tokio::spawn(async move { reg1.commit(&txn1).await });

    let reg2 = registry.clone();
    let txn2 = txn_id.clone();
    let handle2 = tokio::spawn(async move { reg2.commit(&txn2).await });

    let (r1, r2) = tokio::join!(handle1, handle2);
    let results = [r1.unwrap(), r2.unwrap()];

    // Exactly one should succeed, one should fail
    let successes = results.iter().filter(|r| r.is_ok()).count();
    let failures = results.iter().filter(|r| r.is_err()).count();
    assert_eq!(successes, 1, "Exactly one commit should succeed");
    assert_eq!(failures, 1, "Exactly one commit should fail");
}

#[tokio::test]
async fn test_concurrent_rollback_same_transaction() {
    let db = test_db_with_collections().await;
    let registry = Arc::new(DbTransactionRegistry::new(db));

    let txn_id = registry.begin(false).await.unwrap();

    // Spawn two tasks trying to rollback the same transaction
    let reg1 = registry.clone();
    let txn1 = txn_id.clone();
    let handle1 = tokio::spawn(async move { reg1.rollback(&txn1).await });

    let reg2 = registry.clone();
    let txn2 = txn_id.clone();
    let handle2 = tokio::spawn(async move { reg2.rollback(&txn2).await });

    let (r1, r2) = tokio::join!(handle1, handle2);
    let results = [r1.unwrap(), r2.unwrap()];

    // Exactly one should succeed, one should fail
    let successes = results.iter().filter(|r| r.is_ok()).count();
    let failures = results.iter().filter(|r| r.is_err()).count();
    assert_eq!(successes, 1, "Exactly one rollback should succeed");
    assert_eq!(failures, 1, "Exactly one rollback should fail");
}

#[tokio::test]
async fn test_concurrent_commit_and_rollback_same_transaction() {
    let db = test_db_with_collections().await;
    let registry = Arc::new(DbTransactionRegistry::new(db));

    let txn_id = registry.begin(false).await.unwrap();

    // Spawn one task trying to commit, another trying to rollback
    let reg1 = registry.clone();
    let txn1 = txn_id.clone();
    let handle1 = tokio::spawn(async move { reg1.commit(&txn1).await });

    let reg2 = registry.clone();
    let txn2 = txn_id.clone();
    let handle2 = tokio::spawn(async move { reg2.rollback(&txn2).await });

    let (r1, r2) = tokio::join!(handle1, handle2);
    let results = [r1.unwrap(), r2.unwrap()];

    // Exactly one should succeed, one should fail
    let successes = results.iter().filter(|r| r.is_ok()).count();
    let failures = results.iter().filter(|r| r.is_err()).count();
    assert_eq!(successes, 1, "Exactly one operation should succeed");
    assert_eq!(failures, 1, "Exactly one operation should fail");
}

#[tokio::test]
async fn test_cleanup_stale_transactions() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    // Begin some transactions
    let _txn1 = registry.begin(true).await.unwrap();
    let _txn2 = registry.begin(false).await.unwrap();

    assert_eq!(registry.active_transaction_count().unwrap(), 2);

    // Wait a bit so transactions become "stale"
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Cleanup with a very short max_age (0 means everything is stale)
    let result = registry
        .cleanup_stale_transactions(std::time::Duration::from_millis(0))
        .await
        .unwrap();

    assert_eq!(result.cleaned, 2, "Should have cleaned up 2 transactions");
    assert!(result.is_complete(), "All cleanups should succeed");
    assert_eq!(
        registry.active_transaction_count().unwrap(),
        0,
        "No transactions should remain"
    );
}

#[tokio::test]
async fn test_cleanup_only_old_transactions() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    // Begin an "old" transaction
    let _old_txn = registry.begin(true).await.unwrap();

    // Wait a bit
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Begin a "new" transaction
    let new_txn = registry.begin(true).await.unwrap();

    assert_eq!(registry.active_transaction_count().unwrap(), 2);

    // Cleanup with max_age that only catches the old transaction
    let result = registry
        .cleanup_stale_transactions(std::time::Duration::from_millis(40))
        .await
        .unwrap();

    assert_eq!(
        result.cleaned, 1,
        "Should have cleaned up 1 old transaction"
    );
    assert!(result.is_complete(), "All cleanups should succeed");
    assert_eq!(
        registry.active_transaction_count().unwrap(),
        1,
        "One new transaction should remain"
    );

    // The new transaction should still be usable
    assert!(registry.get(&new_txn).is_found());
    registry.rollback(&new_txn).await.unwrap();
}

#[tokio::test]
async fn test_cleanup_uses_idle_age_not_creation_age() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    let txn = registry.begin(true).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    assert!(registry.get(&txn).is_found());
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let result = registry
        .cleanup_stale_transactions(std::time::Duration::from_millis(40))
        .await
        .unwrap();

    assert_eq!(
        result.cleaned, 0,
        "Recently used transaction should not be cleaned by creation age"
    );
    assert!(registry.get(&txn).is_found());

    tokio::time::sleep(std::time::Duration::from_millis(45)).await;

    let result = registry
        .cleanup_stale_transactions(std::time::Duration::from_millis(40))
        .await
        .unwrap();

    assert_eq!(
        result.cleaned, 1,
        "Transaction should be cleaned after it is idle past the limit"
    );
    assert_eq!(registry.active_transaction_count().unwrap(), 0);
}

#[tokio::test]
async fn test_periodic_cleanup_task_removes_idle_transactions() {
    let db = test_db_with_collections().await;
    let registry = Arc::new(DbTransactionRegistry::new(db));

    let _txn = registry.begin(true).await.unwrap();
    assert_eq!(registry.active_transaction_count().unwrap(), 1);

    let cleanup_task = registry.start_stale_transaction_cleanup(
        std::time::Duration::from_millis(100),
        std::time::Duration::from_millis(25),
    );

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    cleanup_task.abort();

    assert_eq!(registry.active_transaction_count().unwrap(), 0);
}

#[tokio::test]
async fn test_active_transaction_count() {
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db);

    assert_eq!(registry.active_transaction_count().unwrap(), 0);

    let txn1 = registry.begin(true).await.unwrap();
    assert_eq!(registry.active_transaction_count().unwrap(), 1);

    let txn2 = registry.begin(false).await.unwrap();
    assert_eq!(registry.active_transaction_count().unwrap(), 2);

    registry.commit(&txn1).await.unwrap();
    assert_eq!(registry.active_transaction_count().unwrap(), 1);

    registry.rollback(&txn2).await.unwrap();
    assert_eq!(registry.active_transaction_count().unwrap(), 0);
}

#[tokio::test]
async fn test_snapshot_isolation_after_external_commit() {
    // This test verifies snapshot isolation: a transaction started before
    // another transaction commits should NOT see the committed data.
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db.clone());
    let collection = db.get_collection("Users").unwrap().unwrap();

    // Step 1: Start reader transaction A FIRST (gets snapshot at this point)
    let reader_txn_id = registry.begin(true).await.unwrap();
    let reader_ctx = registry.get(&reader_txn_id).into_result().unwrap().unwrap();
    let reader_fetcher = reader_ctx.doc_fetcher();

    // Verify initially empty
    let initial_docs = reader_fetcher.get_all("Users").await.unwrap();
    assert!(
        initial_docs.is_empty(),
        "Reader should see empty collection initially"
    );

    // Step 2: In a separate transaction, write and COMMIT data
    let write_txn = db.new_txn(false).await.unwrap();
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("CommittedData".to_string()));
    doc.set("age", NormalValue::Int(42));
    doc.generate_and_set_doc_id().unwrap();
    collection.create(&write_txn, &doc).await.unwrap();
    write_txn.commit().await.unwrap();

    // Step 3: Reader transaction A should STILL see empty (snapshot isolation)
    // because its snapshot was taken before the write committed
    let after_commit_docs = reader_fetcher.get_all("Users").await.unwrap();
    assert!(
        after_commit_docs.is_empty(),
        "Reader should NOT see committed data due to snapshot isolation (found {} docs)",
        after_commit_docs.len()
    );

    registry.rollback(&reader_txn_id).await.unwrap();

    // Step 4: A NEW transaction started after commit SHOULD see the data
    let new_reader_txn_id = registry.begin(true).await.unwrap();
    let new_reader_ctx = registry
        .get(&new_reader_txn_id)
        .into_result()
        .unwrap()
        .unwrap();
    let new_reader_fetcher = new_reader_ctx.doc_fetcher();

    let new_docs = new_reader_fetcher.get_all("Users").await.unwrap();
    assert_eq!(
        new_docs.len(),
        1,
        "New reader should see the committed data"
    );
    assert_eq!(
        new_docs[0].get("name").unwrap().as_str(),
        Some("CommittedData")
    );

    registry.rollback(&new_reader_txn_id).await.unwrap();
}

#[tokio::test]
async fn test_new_transaction_sees_recently_created_collection() {
    // Test that a transaction started AFTER a collection is created can see that collection
    let db = Arc::new(DB::new(MemoryStore::new()).unwrap());
    let registry = DbTransactionRegistry::new(db.clone());

    // Create collection after registry is created
    db.create_collection(CollectionVersion::new(
        "NewCollection",
        "v1",
        "col-new",
        vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
    ))
    .await
    .unwrap();

    // New transaction should see the collection
    let txn_id = registry.begin(true).await.unwrap();
    let collection_names = registry.collection_names().unwrap();
    assert!(
        collection_names.contains(&"NewCollection".to_string()),
        "New transaction should see recently created collection"
    );

    registry.rollback(&txn_id).await.unwrap();
}

#[tokio::test]
async fn test_collection_snapshot_isolation_during_deletion() {
    // Test snapshot isolation: a transaction that started before a collection
    // is deleted should still be able to query that collection
    let db = test_db_with_collections().await;
    let registry = DbTransactionRegistry::new(db.clone());
    let collection = db.get_collection("Users").unwrap().unwrap();

    // Add some data to the collection first
    {
        let write_txn = db.new_txn(false).await.unwrap();
        let mut doc = Document::new();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));
        doc.generate_and_set_doc_id().unwrap();
        collection.create(&write_txn, &doc).await.unwrap();
        write_txn.commit().await.unwrap();
    }

    // Start a transaction BEFORE deletion
    let reader_txn_id = registry.begin(true).await.unwrap();
    let reader_ctx = registry.get(&reader_txn_id).into_result().unwrap().unwrap();
    let reader_fetcher = reader_ctx.doc_fetcher();

    // Verify reader can see the collection with data
    let docs_before = reader_fetcher.get_all("Users").await.unwrap();
    assert_eq!(
        docs_before.len(),
        1,
        "Should see 1 document before deletion"
    );

    // Now delete the collection from the DB
    db.delete_collection("Users").await.unwrap();

    // The reader transaction should STILL be able to query the collection
    // because it has a snapshot from before the deletion
    let docs_after = reader_fetcher.get_all("Users").await.unwrap();
    assert_eq!(
        docs_after.len(),
        1,
        "Reader should still see document after deletion due to snapshot isolation"
    );
    assert_eq!(
        docs_after[0].get("name").unwrap().as_str(),
        Some("Alice"),
        "Should see the same document content"
    );

    // However, the DB should report the collection as gone
    assert!(
        !db.has_collection("Users").unwrap(),
        "DB should report collection as deleted"
    );

    registry.rollback(&reader_txn_id).await.unwrap();

    // A NEW transaction should NOT see the deleted collection
    let new_txn_id = registry.begin(true).await.unwrap();
    let new_ctx = registry.get(&new_txn_id).into_result().unwrap().unwrap();
    let new_fetcher = new_ctx.doc_fetcher();

    let result = new_fetcher.get_all("Users").await;
    assert!(
        result.is_err(),
        "New transaction should not see deleted collection"
    );
    assert!(
        result.unwrap_err().to_string().contains("Users"),
        "Error should mention the collection name"
    );

    registry.rollback(&new_txn_id).await.unwrap();
}

#[tokio::test]
async fn test_collections_snapshot_is_isolated_from_modifications() {
    // Test that modifying a snapshot does not affect the original cache
    let db = test_db_with_collections().await;

    let snapshot = db.collections_snapshot().unwrap();
    assert!(snapshot.contains("Users"));

    // The snapshot should be an independent copy
    // (This is implicitly tested by the fact that CollectionSnapshot
    // wraps an Arc and doesn't expose mutable methods)

    // Adding a new collection should not affect existing snapshots
    db.create_collection(CollectionVersion::new(
        "Posts",
        "v1",
        "col-posts",
        vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
    ))
    .await
    .unwrap();

    // Original snapshot should still only have Users
    assert_eq!(snapshot.len(), 1);
    assert!(snapshot.contains("Users"));
    assert!(!snapshot.contains("Posts"));

    // New snapshot should have both
    let new_snapshot = db.collections_snapshot().unwrap();
    assert_eq!(new_snapshot.len(), 2);
    assert!(new_snapshot.contains("Users"));
    assert!(new_snapshot.contains("Posts"));
}
