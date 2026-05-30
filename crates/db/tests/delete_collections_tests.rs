//! Tests for the multi-collection delete orchestrator (Go #4688 parity).

use db::database::DB;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;

fn user_schema() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v-users-1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
}

fn book_schema() -> CollectionVersion {
    CollectionVersion::new(
        "Books",
        "v-books-1",
        "col-books",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
        ],
    )
}

#[tokio::test]
async fn delete_collections_removes_single_name() {
    let db = DB::new(MemoryStore::new()).unwrap();
    db.create_collections_atomic(vec![user_schema()])
        .await
        .unwrap();

    db.delete_collections(vec!["Users".into()], true)
        .await
        .unwrap();

    assert!(
        db.get_collection("Users").unwrap().is_none(),
        "Users collection should be gone after delete_collections"
    );
}

#[tokio::test]
async fn delete_collections_removes_multiple_names_atomically() {
    let db = DB::new(MemoryStore::new()).unwrap();
    db.create_collections_atomic(vec![user_schema(), book_schema()])
        .await
        .unwrap();

    db.delete_collections(vec!["Users".into(), "Books".into()], true)
        .await
        .unwrap();

    assert!(
        db.get_collection("Users").unwrap().is_none(),
        "Users collection should be gone"
    );
    assert!(
        db.get_collection("Books").unwrap().is_none(),
        "Books collection should be gone"
    );
}

#[tokio::test]
async fn delete_collections_errors_on_empty_names() {
    let db = DB::new(MemoryStore::new()).unwrap();
    let err = db
        .delete_collections(vec![], true)
        .await
        .expect_err("expected error for empty names");

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("name") || msg.contains("empty") || msg.contains("required"),
        "unexpected error message: {err}"
    );
}

#[tokio::test]
async fn delete_collections_errors_on_unknown_name() {
    let db = DB::new(MemoryStore::new()).unwrap();
    db.create_collections_atomic(vec![user_schema()])
        .await
        .unwrap();

    let err = db
        .delete_collections(vec!["Users".into(), "Ghost".into()], true)
        .await
        .expect_err("expected error for unknown collection name");

    let msg = err.to_string();
    assert!(
        msg.contains("Ghost") || msg.to_lowercase().contains("not found"),
        "unexpected error message: {err}"
    );

    // Atomicity: because Ghost failed before any state changed, Users must still exist.
    assert!(
        db.get_collection("Users").unwrap().is_some(),
        "Users should not have been deleted when batch validation failed"
    );
}

#[tokio::test]
async fn delete_collections_dedupes_repeated_names() {
    let db = DB::new(MemoryStore::new()).unwrap();
    db.create_collections_atomic(vec![user_schema()])
        .await
        .unwrap();

    db.delete_collections(vec!["Users".into(), "Users".into()], true)
        .await
        .expect("duplicate names should be deduplicated, not error");

    assert!(
        db.get_collection("Users").unwrap().is_none(),
        "Users collection should be gone"
    );
}
