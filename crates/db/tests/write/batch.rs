use crate::common::fixture::make_test_db_with_bus;
use crate::common::schema::test_collection;
use async_lock::Mutex as TokioMutex;
use db::write::autocommit::batch::*;
use document::Document;
use events::EventName;
use query::mutator::DocMutator;
use query::mutator::MutationBatchController;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use std::sync::Arc;

fn branchable_test_collection() -> CollectionVersion {
    CollectionVersion::new(
        "TestBranchable",
        "v1",
        "col-test-branchable",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "x", FieldKind::int()),
        ],
    )
    .as_branchable()
}

#[tokio::test]
async fn batch_create_publishes_event_on_commit() {
    let (db, bus) = make_test_db_with_bus().await;
    db.create_collection(test_collection())
        .await
        .expect("schema");

    let mut sub = bus.subscribe(&[EventName::Update]);

    let txn = db.new_txn(false).await.expect("new_txn");
    let txn_arc = Arc::new(TokioMutex::new(Some(txn)));
    let mutator = BatchMutator::new(Arc::clone(&db), Arc::clone(&txn_arc));

    let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
    let result = mutator.create("TestDoc", doc).await.expect("create");

    // Before commit: no event should have fired
    assert!(
        sub.try_recv().is_err(),
        "no event should fire before commit"
    );

    mutator.commit().await.expect("commit");

    // After commit: one Update event arrives
    let msg = tokio::time::timeout(std::time::Duration::from_secs(1), sub.recv())
        .await
        .expect("event arrived within timeout")
        .expect("subscription not closed");

    let update = msg.as_update().expect("expected Update message");
    assert_eq!(update.doc_id, result.doc_id.to_string());
    assert_ne!(update.cid, cid::Cid::default(), "cid should be populated");
    assert!(
        !update.block.is_empty(),
        "block bytes should be populated (matches Go's sendUpdate)"
    );
}

#[tokio::test]
async fn batch_delete_missing_doc_publishes_no_event_and_writes_no_block() {
    // DeleteNode treats existed==false as a no-op; the mutator must not
    // create a tombstone commit or fire an Update event for a missing doc.
    let (db, bus) = make_test_db_with_bus().await;
    db.create_collection(test_collection())
        .await
        .expect("schema");

    let mut sub = bus.subscribe(&[EventName::Update]);

    let txn = db.new_txn(false).await.expect("new_txn");
    let txn_arc = Arc::new(TokioMutex::new(Some(txn)));
    let mutator = BatchMutator::new(Arc::clone(&db), Arc::clone(&txn_arc));

    let missing_doc_id = document::DocID::new_v0_from_seed("missing-doc");

    let result = mutator
        .delete("TestDoc", &missing_doc_id)
        .await
        .expect("delete should succeed even on missing doc");
    assert!(!result.existed, "doc should not have existed");

    mutator.commit().await.expect("commit");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        sub.try_recv().is_err(),
        "deleting a non-existent doc should not publish an Update event"
    );
}

#[tokio::test]
async fn batch_delete_branchable_surfaces_collection_block_for_broadcast() {
    // Go emits two updates for branchable deletes: the document composite
    // block AND the collection head block. DeleteResult must surface the
    // collection block so BroadcastMutator can re-broadcast it (matches
    // create/update's broadcast_cid/broadcast_block plumbing).
    let (db, _bus) = make_test_db_with_bus().await;
    db.create_collection(branchable_test_collection())
        .await
        .expect("schema");

    let txn = db.new_txn(false).await.expect("new_txn");
    let txn_arc = Arc::new(TokioMutex::new(Some(txn)));
    let mutator = BatchMutator::new(Arc::clone(&db), Arc::clone(&txn_arc));

    let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
    let create_result = mutator.create("TestBranchable", doc).await.expect("create");
    let doc_id = create_result.doc_id.clone();

    let delete_result = mutator
        .delete("TestBranchable", &doc_id)
        .await
        .expect("delete");

    assert!(delete_result.existed, "doc should have existed");
    assert!(
        delete_result.commit_cid.is_some(),
        "composite delete cid should be set"
    );
    assert!(
        delete_result.commit_block.is_some(),
        "composite delete block should be set"
    );
    assert!(
        delete_result.broadcast_cid.is_some(),
        "branchable collection cid should be surfaced for broadcast"
    );
    assert!(
        delete_result
            .broadcast_block
            .as_ref()
            .map(|b| !b.is_empty())
            .unwrap_or(false),
        "branchable collection block bytes should be surfaced for broadcast"
    );
    assert_ne!(
        delete_result.commit_cid, delete_result.broadcast_cid,
        "collection head cid must differ from the document composite cid"
    );

    mutator.commit().await.expect("commit");
}
