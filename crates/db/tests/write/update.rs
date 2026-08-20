use db::AutoCommitMutator;
use db::DB;
use document::Document;
use document::NormalValue;
use query::mutator::DocMutator;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use std::collections::HashSet;
use std::sync::Arc;
use storage::backends::MemoryStore;

fn test_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Patch",
        "v1",
        "col-patch",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "left", FieldKind::int()),
            FieldDescription::new("3", "right", FieldKind::int()),
        ],
    )
}

/// A query plan materializes the document before entering the auto-commit
/// mutator. Two concurrent plans can therefore hand the mutator separate
/// stale copies of the same document. `modified_fields` is the patch
/// boundary: applying the second copy must not replace an unrelated field
/// written by the first update.
#[tokio::test]
async fn stale_disjoint_field_update_without_precondition_preserves_committed_fields() {
    let db = Arc::new(DB::new(MemoryStore::new()).expect("create db"));
    db.create_collection(test_collection())
        .await
        .expect("schema");
    let mutator = AutoCommitMutator::new(Arc::clone(&db));

    let initial = Document::from_json_str(r#"{"left": 0, "right": 0}"#).expect("initial document");
    let created = mutator.create("Patch", initial).await.expect("create");

    let mut stale_left = mutator
        .get_for_update("Patch", &created.doc_id)
        .await
        .expect("fetch left copy")
        .expect("left copy");
    let mut stale_right = stale_left.clone();

    stale_left.set("left", NormalValue::Int(1));
    mutator
        .update("Patch", stale_left, HashSet::from(["left".to_string()]))
        .await
        .expect("update left");

    stale_right.set("right", NormalValue::Int(1));
    mutator
        .update("Patch", stale_right, HashSet::from(["right".to_string()]))
        .await
        .expect("update right");

    let final_doc = mutator
        .get_for_update("Patch", &created.doc_id)
        .await
        .expect("fetch final")
        .expect("final document");
    assert_eq!(final_doc.get("left"), Some(&NormalValue::Int(1)));
    assert_eq!(final_doc.get("right"), Some(&NormalValue::Int(1)));
}

/// Query mutations carry the snapshot against which their filter was
/// validated. If that snapshot changes while the update waits for the
/// per-document guard, the mutation must conflict instead of applying a
/// predicate that is no longer known to hold.
#[tokio::test]
async fn stale_conditional_update_returns_conflict() {
    let db = Arc::new(DB::new(MemoryStore::new()).expect("create db"));
    db.create_collection(test_collection())
        .await
        .expect("schema");
    let mutator = AutoCommitMutator::new(Arc::clone(&db));

    let initial = Document::from_json_str(r#"{"left": 0, "right": 0}"#).expect("initial document");
    let created = mutator.create("Patch", initial).await.expect("create");

    let mut first = mutator
        .get_for_update("Patch", &created.doc_id)
        .await
        .expect("fetch first copy")
        .expect("first copy");
    let expected_second = first.clone();
    let mut second = first.clone();

    first.set("left", NormalValue::Int(1));
    mutator
        .update("Patch", first, HashSet::from(["left".to_string()]))
        .await
        .expect("update left");

    second.set("right", NormalValue::Int(1));
    let error = mutator
        .update_if_unchanged(
            "Patch",
            expected_second,
            second,
            HashSet::from(["right".to_string()]),
        )
        .await
        .expect_err("stale conditional update must conflict");
    assert!(matches!(
        error,
        query::error::QueryError::TransactionConflict(ref message)
            if message == "transaction conflict. Please retry"
    ));

    let final_doc = mutator
        .get_for_update("Patch", &created.doc_id)
        .await
        .expect("fetch final")
        .expect("final document");
    assert_eq!(final_doc.get("left"), Some(&NormalValue::Int(1)));
    assert_eq!(final_doc.get("right"), Some(&NormalValue::Int(0)));
}
