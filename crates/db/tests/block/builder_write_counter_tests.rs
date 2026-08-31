use cid::Cid;
use datastore::NamespaceView;
use datastore::SharedTxn;
use db::block::builder::*;
use document::CType;
use document::Document;
use document::NormalValue;
use std::collections::HashSet;
use storage::corekv::Store;
use storage::namespace::Namespace;
use storage::RegolithStore;

async fn first_counter_update() -> Cid {
    let store = RegolithStore::in_memory().unwrap();
    let txn = store.new_txn(false).await.unwrap();
    let shared = SharedTxn::new(txn);
    let blockstore = NamespaceView::new(shared.clone(), Namespace::Blockstore);
    let headstore = NamespaceView::new(shared, Namespace::Headstore);
    let identity = DocStorageIdentity::new(1, 1);

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));
    let created = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        None,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    doc.set_id(document::DocID::from_string(&created.doc_id).unwrap());
    doc.set_with_crdt("count", CType::PnCounter, NormalValue::Int(1))
        .unwrap();
    doc.set_counter_delta("count".to_string(), NormalValue::Int(1));
    let modified = HashSet::from(["count".to_string()]);

    write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        Some(&modified),
        None,
        None,
        None,
    )
    .await
    .unwrap()
    .field_cids[0]
}

#[tokio::test]
async fn first_counter_updates_on_existing_document_have_distinct_cids() {
    let (left, right) = tokio::join!(first_counter_update(), first_counter_update());

    assert_ne!(left, right);
}
