use cid::Cid;
use datastore::NamespaceView;
use datastore::SharedTxn;
use db::block::builder::*;
use defra_core::block::Block;
use document::Document;
use document::NormalValue;
use std::collections::HashSet;
use storage::backends::MemoryStore;
use storage::corekv::Store;
use storage::namespace::Namespace;

async fn block(blockstore: &NamespaceView, cid: &Cid) -> Block {
    let bytes = blockstore
        .get(&cid.to_bytes())
        .await
        .unwrap()
        .expect("block stored");
    Block::from_dag_cbor(&bytes).unwrap()
}

#[tokio::test]
async fn field_priority_is_independent_of_sibling_updates() {
    let store = MemoryStore::new();
    let txn = store.new_txn(false).await.unwrap();
    let shared = SharedTxn::new(txn);
    let blockstore = NamespaceView::new(shared.clone(), Namespace::Blockstore);
    let headstore = NamespaceView::new(shared, Namespace::Headstore);
    let identity = DocStorageIdentity::new(1, 1);

    let mut doc = Document::new();
    doc.set("a", NormalValue::Int(1));
    doc.set("b", NormalValue::Int(1));
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

    doc.set("a", NormalValue::Int(2));
    write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        Some(&HashSet::from(["a".to_string()])),
        None,
        None,
        None,
    )
    .await
    .unwrap();

    doc.set("b", NormalValue::Int(2));
    let updated = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        Some(&HashSet::from(["b".to_string()])),
        None,
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        block(&blockstore, &updated.field_cids[0])
            .await
            .delta
            .priority(),
        2
    );
    assert_eq!(block(&blockstore, &updated.cid).await.delta.priority(), 3);
}
