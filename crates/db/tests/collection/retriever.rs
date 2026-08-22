use db::collection::retriever::*;
use db::database::DB;
use db::write::doc::DbDocMutator;
use document::Document;
use query::mutator::DocMutator;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use schema::PolicyDescription;
use std::sync::Arc;
use storage::backends::MemoryStore;

fn test_collection_with_policy() -> CollectionVersion {
    CollectionVersion::new(
        "TestDoc",
        "v1",
        "col-test-doc",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "x", FieldKind::int()),
        ],
    )
    .with_policy(PolicyDescription::new("policy-abc", "users"))
}

#[tokio::test]
async fn resolves_collection_for_known_doc() {
    let db = Arc::new(DB::new(MemoryStore::new()).expect("create db"));
    db.create_collection(test_collection_with_policy())
        .await
        .expect("create collection");

    let txn = db.new_txn(false).await.expect("new_txn");
    let mutator = DbDocMutator::new(Arc::clone(&db), txn);
    let doc = Document::from_json_str(r#"{"x": 1}"#).expect("doc");
    let result = mutator.create("TestDoc", doc).await.expect("create");
    let txn = mutator.take_txn().await.expect("take txn");
    txn.commit().await.expect("commit");

    let doc_id = result.doc_id.to_string();
    let info = resolve_collection_from_doc_id(&db, &doc_id)
        .await
        .expect("resolve")
        .expect("doc has policy");

    assert_eq!(info.policy_id, "policy-abc");
    assert_eq!(info.resource_name, "users");
    // collection_id is the stable id passed to CollectionVersion::new.
    assert!(
        !info.collection_id.is_empty(),
        "collection_id should be populated"
    );
}

#[tokio::test]
async fn returns_none_for_unknown_doc() {
    let db = Arc::new(DB::new(MemoryStore::new()).expect("create db"));
    let info = resolve_collection_from_doc_id(&db, "bafy-not-a-real-doc")
        .await
        .expect("resolve");
    assert!(info.is_none());
}
