//! Two interactive transactions updating distinct documents.
//!
//! Setting the same field to the same value in two different documents makes
//! the two writes look alike, and the integration repro
//! (`tools/integration-test/tests/issue1194_repro.rs`) says one of the commits
//! is refused for a conflict. This pins the same thing at the layer that
//! produces the writes, where the conflicting key is visible.

use db::AutoCommitMutator;
use db::DbDocMutator;
use db::DB;
use document::Document;
use document::NormalValue;
use query::mutator::DocMutator;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use std::collections::HashSet;
use std::sync::Arc;
use storage::RegolithStore;

fn snap_collection() -> CollectionVersion {
    CollectionVersion::new(
        "Snap",
        "v1",
        "col-snap",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "status", FieldKind::string()),
            FieldDescription::new("3", "content", FieldKind::string()),
        ],
    )
}

// Red until the storage engine ships the fix. regolith validates every written
// key, so two transactions writing one byte-identical block collide even though
// the write is idempotent; sourcenetwork/regolith#184 elides such a write at
// commit. Left visible rather than deleted: the gap is real, and a suite that
// simply does not mention it reads as though the case were covered.
#[ignore = "sourcenetwork/regolith#184: byte-identical concurrent writes conflict"]
#[tokio::test]
async fn identical_field_writes_to_distinct_documents_do_not_conflict() {
    let db = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).expect("create db"));
    db.create_collection(snap_collection())
        .await
        .expect("create collection");

    let auto = AutoCommitMutator::new(Arc::clone(&db));
    let first = auto
        .create(
            "Snap",
            Document::from_json_str(r#"{"status": "idle", "content": "a"}"#).expect("doc a"),
        )
        .await
        .expect("create a");
    let second = auto
        .create(
            "Snap",
            Document::from_json_str(r#"{"status": "idle", "content": "b"}"#).expect("doc b"),
        )
        .await
        .expect("create b");

    // Both transactions open before either commits, which is what makes them
    // concurrent rather than sequential.
    let txn_one = db.new_txn(false).await.expect("txn one");
    let txn_two = db.new_txn(false).await.expect("txn two");
    let mutator_one = DbDocMutator::new(Arc::clone(&db), txn_one);
    let mutator_two = DbDocMutator::new(Arc::clone(&db), txn_two);

    for (mutator, doc_id) in [
        (&mutator_one, &first.doc_id),
        (&mutator_two, &second.doc_id),
    ] {
        let mut doc = mutator
            .get_for_update("Snap", doc_id)
            .await
            .expect("fetch")
            .expect("document present");
        // The same field, the same value, in two different documents.
        doc.set("status", NormalValue::String("streaming".to_string()));
        mutator
            .update("Snap", doc, HashSet::from(["status".to_string()]))
            .await
            .expect("update");
    }

    mutator_one
        .take_txn()
        .await
        .expect("take txn one")
        .commit()
        .await
        .expect("first commit");
    mutator_two
        .take_txn()
        .await
        .expect("take txn two")
        .commit()
        .await
        .expect("second commit: distinct documents must not conflict");
}
