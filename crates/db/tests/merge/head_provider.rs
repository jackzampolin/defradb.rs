use db::merge::head_provider::*;
use db::AutoCommitMutator;
use db::DB;
use defra_core::block::generate_cid_from_bytes;
use defra_core::Block;
use defra_core::CompositeDeltaPayload;
use defra_core::CrdtDelta;
use document::Document;
use p2p::sync::DocumentHeadProvider;
use query::mutator::DocMutator;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use std::sync::Arc;
use storage::corekv::Key;
use storage::keys::headstore::HeadstoreDocKey;
use storage::keys::headstore::HeadstorePriorityKey;
use storage::RegolithStore;

#[tokio::test]
async fn batch_create_docs_expose_composite_heads() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
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
    let docs = vec![
        make_transcript("first", 1),
        make_transcript("second", 2),
        make_transcript("third", 3),
    ];
    let results = mutator.create_many("Transcript", docs).await.unwrap();
    assert_eq!(results.len(), 3);

    let provider = DbHeadProvider::new(db);
    for result in results {
        let doc_id = result.doc_id.to_string();
        let heads = provider.get_document_heads(&doc_id).await.unwrap();
        assert!(
            !heads.is_empty(),
            "expected composite heads for batch-created doc {}",
            doc_id
        );
    }
}

#[tokio::test]
async fn falls_back_to_priority_index_when_composite_head_entry_is_missing() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
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
    let doc_short_id = doc_short_id_for(&db, &doc_id).await;

    let txn = db.new_txn(false).await.unwrap();
    txn.headstore()
        .unwrap()
        .delete(&HeadstoreDocKey::new(doc_short_id, "C", commit_cid).bytes())
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let provider = DbHeadProvider::new(db);
    let heads = provider.get_document_heads(&doc_id).await.unwrap();
    assert_eq!(heads, vec![commit_cid]);
}

#[tokio::test]
async fn falls_back_to_blockstore_scan_when_head_indexes_are_missing() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
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
    let priority = Block::from_dag_cbor(&commit_block)
        .expect("decode commit block")
        .delta
        .priority();
    let doc_short_id = doc_short_id_for(&db, &doc_id).await;

    let txn = db.new_txn(false).await.unwrap();
    txn.headstore()
        .unwrap()
        .delete(&HeadstoreDocKey::new(doc_short_id, "C", commit_cid).bytes())
        .await
        .unwrap();
    txn.headstore()
        .unwrap()
        .delete(&HeadstorePriorityKey::new(doc_short_id, priority, commit_cid).bytes())
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let provider = DbHeadProvider::new(db);
    let heads = provider.get_document_heads(&doc_id).await.unwrap();
    assert_eq!(heads, vec![commit_cid]);
}

#[tokio::test]
async fn falls_back_to_ownership_index_when_head_indexes_are_missing() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let db = Arc::new(DB::from_arc(store).unwrap());

    let block = Block::new_with_options(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "schema-v1".to_string(),
            priority: 7,
            status: 1,
        }),
        vec![],
        vec![],
        None,
        None,
    );
    let block_bytes = block.to_dag_cbor().unwrap();
    let cid = generate_cid_from_bytes(&block_bytes).unwrap();
    let doc_id = db::block::builder::derive_doc_id(&cid);

    let txn = db.new_txn(false).await.unwrap();
    txn.blockstore()
        .unwrap()
        .set(&cid.to_bytes(), &block_bytes)
        .await
        .unwrap();
    {
        let systemstore = txn.systemstore().unwrap();
        let short_id = db.next_doc_short_id().await.unwrap();
        db::docid::map::set_doc_id_mapping(&systemstore, 1, short_id, &doc_id)
            .await
            .unwrap();
        db::docid::map::set_block_doc_id_mapping(&systemstore, &cid.to_string(), &doc_id)
            .await
            .unwrap();
    }
    txn.commit().await.unwrap();

    let provider = DbHeadProvider::new(db);
    let heads = provider.get_document_heads(&doc_id).await.unwrap();
    assert_eq!(heads, vec![cid]);
}

async fn doc_short_id_for(db: &Arc<DB<RegolithStore>>, doc_id: &str) -> u64 {
    let txn = db.new_txn(true).await.unwrap();
    let systemstore = txn.systemstore().unwrap();
    db::docid::map::get_doc_ref(&systemstore, doc_id)
        .await
        .unwrap()
        .expect("doc mapping")
        .doc_short_id
}

fn make_transcript(body: &str, idx: i64) -> Document {
    let mut doc = Document::new();
    doc.set("body", body.to_string());
    doc.set("idx", idx);
    doc
}
