use datastore::NamespaceView;
use datastore::SharedTxn;
use db::block::cleanup::*;
use defra_core::Block;
use defra_core::CompositeDeltaPayload;
use defra_core::CrdtDelta;
use defra_core::DAGLink;
use defra_core::LwwDeltaPayload;
use storage::corekv::Store;
use storage::namespace::Namespace;
use storage::RegolithStore;

async fn stores() -> (NamespaceView, NamespaceView) {
    let store = RegolithStore::in_memory().unwrap();
    let txn = SharedTxn::new(store.new_txn(false).await.unwrap());
    (
        NamespaceView::new(txn.clone(), Namespace::Blockstore),
        NamespaceView::new(txn, Namespace::Systemstore),
    )
}

#[tokio::test]
async fn deleting_one_owner_keeps_a_shared_block() {
    let (blockstore, systemstore) = stores().await;
    let block = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "name".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            data: vec![1],
        }),
        vec![],
        vec![],
    );
    let cid = block.generate_cid().unwrap();
    blockstore
        .set(&cid.to_bytes(), &block.to_dag_cbor().unwrap())
        .await
        .unwrap();
    for owner in ["doc-a", "doc-b"] {
        db::docid::map::set_block_doc_id_mapping(&systemstore, &cid.to_string(), owner)
            .await
            .unwrap();
    }

    delete_owned_commit(&blockstore, &systemstore, &cid, "doc-a")
        .await
        .unwrap();

    assert!(blockstore.get(&cid.to_bytes()).await.unwrap().is_some());
    assert_eq!(
        db::docid::map::get_doc_ids_for_block(&systemstore, &cid.to_string())
            .await
            .unwrap(),
        vec!["doc-b".to_string()]
    );
}

#[tokio::test]
async fn deleting_a_dag_keeps_the_shared_subtree() {
    let (blockstore, systemstore) = stores().await;
    let field = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "name".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            data: vec![1],
        }),
        vec![],
        vec![],
    );
    let field_cid = field.generate_cid().unwrap();
    let root = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "v1".to_string(),
            priority: 1,
            status: 1,
        }),
        vec![],
        vec![DAGLink::new("name", field_cid)],
    );
    let root_cid = root.generate_cid().unwrap();
    for (cid, block) in [(field_cid, field), (root_cid, root)] {
        blockstore
            .set(&cid.to_bytes(), &block.to_dag_cbor().unwrap())
            .await
            .unwrap();
    }
    db::docid::map::set_block_doc_id_mapping(&systemstore, &root_cid.to_string(), "doc-a")
        .await
        .unwrap();
    for owner in ["doc-a", "doc-b"] {
        db::docid::map::set_block_doc_id_mapping(&systemstore, &field_cid.to_string(), owner)
            .await
            .unwrap();
    }

    delete_owned_dag(&blockstore, &systemstore, &[root_cid], "doc-a")
        .await
        .unwrap();

    assert!(blockstore
        .get(&root_cid.to_bytes())
        .await
        .unwrap()
        .is_none());
    assert!(blockstore
        .get(&field_cid.to_bytes())
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn deleting_all_aliases_removes_the_owned_dag() {
    let (blockstore, systemstore) = stores().await;
    let encryption = b"encryption".to_vec();
    let signature = b"signature".to_vec();
    let encryption_cid = defra_core::block::generate_cid_from_bytes(&encryption).unwrap();
    let signature_cid = defra_core::block::generate_cid_from_bytes(&signature).unwrap();
    let block = Block::new_with_options(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "name".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            data: vec![1],
        }),
        vec![],
        vec![],
        Some(encryption_cid),
        Some(signature_cid),
    );
    let cid = block.generate_cid().unwrap();
    for (cid, bytes) in [
        (cid, block.to_dag_cbor().unwrap()),
        (encryption_cid, encryption),
        (signature_cid, signature),
    ] {
        blockstore.set(&cid.to_bytes(), &bytes).await.unwrap();
    }
    for alias in ["doc-v0", "doc-v1"] {
        for owned_cid in [cid, encryption_cid] {
            db::docid::map::set_block_doc_id_mapping(&systemstore, &owned_cid.to_string(), alias)
                .await
                .unwrap();
        }
    }

    delete_owned_dag_for_owners(
        &blockstore,
        &systemstore,
        &[cid],
        &["doc-v0".to_string(), "doc-v1".to_string()],
    )
    .await
    .unwrap();

    for deleted_cid in [cid, encryption_cid, signature_cid] {
        assert!(blockstore
            .get(&deleted_cid.to_bytes())
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn deleting_one_commit_owner_clears_child_ownership_but_keeps_shared_bytes() {
    let (blockstore, systemstore) = stores().await;
    let field = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "name".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            data: vec![1],
        }),
        vec![],
        vec![],
    );
    let field_cid = field.generate_cid().unwrap();
    let root = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "v1".to_string(),
            priority: 1,
            status: 1,
        }),
        vec![],
        vec![DAGLink::new("name", field_cid)],
    );
    let root_cid = root.generate_cid().unwrap();
    for (cid, block) in [(field_cid, field), (root_cid, root)] {
        blockstore
            .set(&cid.to_bytes(), &block.to_dag_cbor().unwrap())
            .await
            .unwrap();
        for owner in ["doc-a", "doc-b"] {
            db::docid::map::set_block_doc_id_mapping(&systemstore, &cid.to_string(), owner)
                .await
                .unwrap();
        }
    }

    delete_owned_commit(&blockstore, &systemstore, &root_cid, "doc-a")
        .await
        .unwrap();

    for cid in [root_cid, field_cid] {
        assert!(blockstore.get(&cid.to_bytes()).await.unwrap().is_some());
        assert_eq!(
            db::docid::map::get_doc_ids_for_block(&systemstore, &cid.to_string())
                .await
                .unwrap(),
            vec!["doc-b".to_string()]
        );
    }
}
