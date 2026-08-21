use db::database::DB;
use db::merge::replication::*;
use db::AutoCommitMutator;
use document::Document;
use query::mutator::DocMutator;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use std::sync::Arc;
use storage::backends::MemoryStore;

#[tokio::test]
async fn load_document_head_blocks_returns_current_composite_block() {
    let store = Arc::new(MemoryStore::new());
    let db = Arc::new(DB::from_arc(store).unwrap());
    db.create_collection(CollectionVersion::new(
        "Transcript",
        "v1",
        "col-transcript",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "body", FieldKind::string()),
            FieldDescription::new("3", "idx", FieldKind::int()),
        ],
    ))
    .await
    .unwrap();

    let mutator = AutoCommitMutator::new(db.clone());
    let result = mutator
        .create_many("Transcript", vec![make_transcript("first", 1)])
        .await
        .unwrap()
        .pop()
        .unwrap();

    let doc_id = result.doc_id.to_string();
    let commit_cid = result.commit_cid.expect("commit cid");
    let commit_block = result.commit_block.expect("commit block");

    let blocks = load_document_head_blocks(&db, &doc_id).await.unwrap();

    assert_eq!(blocks, vec![(commit_cid, commit_block)]);
}

fn make_transcript(body: &str, idx: i64) -> Document {
    let mut doc = Document::new();
    doc.set("body", body.to_string());
    doc.set("idx", idx);
    doc
}
