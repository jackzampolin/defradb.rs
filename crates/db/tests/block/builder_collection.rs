use cid::Cid;
use datastore::NamespaceView;
use datastore::SharedTxn;
use db::block::builder::collection::*;
use defra_core::block::generate_cid_from_bytes;
use defra_core::Block;
use std::sync::Arc;
use storage::corekv::IterOptions;
use storage::corekv::Store;
use storage::keys::headstore::HeadstoreColKey;
use storage::namespace::Namespace;
use storage::RegolithStore;

fn test_cid(value: &[u8]) -> Cid {
    generate_cid_from_bytes(value).unwrap()
}

fn views(shared: &Arc<SharedTxn>) -> (NamespaceView, NamespaceView) {
    (
        NamespaceView::new(Arc::clone(shared), Namespace::Blockstore),
        NamespaceView::new(Arc::clone(shared), Namespace::Headstore),
    )
}

async fn commit(shared: Arc<SharedTxn>) {
    Arc::try_unwrap(shared)
        .ok()
        .expect("transaction views dropped")
        .into_txn()
        .commit()
        .await
        .unwrap();
}

async fn collection_heads(store: &RegolithStore, collection_id: u32) -> Vec<Cid> {
    let shared = SharedTxn::new(store.new_txn(true).await.unwrap());
    let (_, headstore) = views(&shared);
    let mut iterator = headstore
        .iterator(IterOptions::new().with_prefix(HeadstoreColKey::collection_prefix(collection_id)))
        .await
        .unwrap();
    let mut heads = Vec::new();
    while let Some(pair) = iterator.next().await.unwrap() {
        let cid = String::from_utf8(pair.key)
            .unwrap()
            .rsplit('/')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        heads.push(cid);
    }
    iterator.close().await.unwrap();
    heads.sort_by_cached_key(Cid::to_string);
    heads
}

#[tokio::test]
async fn concurrent_collection_transitions_preserve_and_merge_sibling_heads() {
    const COLLECTION_ID: u32 = 7;

    let store = RegolithStore::in_memory().unwrap();
    let seed_txn = SharedTxn::new(store.new_txn(false).await.unwrap());
    let (seed_blocks, seed_heads) = views(&seed_txn);
    write_collection_block(
        &seed_blocks,
        &seed_heads,
        COLLECTION_ID,
        "schema-v1",
        test_cid(b"seed document"),
        None,
    )
    .await
    .unwrap();
    drop((seed_blocks, seed_heads));
    commit(seed_txn).await;

    let first_txn = SharedTxn::new(store.new_txn(false).await.unwrap());
    let second_txn = SharedTxn::new(store.new_txn(false).await.unwrap());
    let (first_blocks, first_heads) = views(&first_txn);
    let (second_blocks, second_heads) = views(&second_txn);

    let (first_cid, _) = write_collection_block(
        &first_blocks,
        &first_heads,
        COLLECTION_ID,
        "schema-v1",
        test_cid(b"first document"),
        None,
    )
    .await
    .unwrap();
    let (second_cid, _) = write_collection_block(
        &second_blocks,
        &second_heads,
        COLLECTION_ID,
        "schema-v1",
        test_cid(b"second document"),
        None,
    )
    .await
    .unwrap();
    drop((first_blocks, first_heads, second_blocks, second_heads));

    commit(first_txn).await;
    commit(second_txn).await;

    let mut expected_siblings = vec![first_cid, second_cid];
    expected_siblings.sort_by_cached_key(Cid::to_string);
    assert_eq!(
        collection_heads(&store, COLLECTION_ID).await,
        expected_siblings
    );

    let merge_txn = SharedTxn::new(store.new_txn(false).await.unwrap());
    let (merge_blocks, merge_heads) = views(&merge_txn);
    let (merged_cid, merged_bytes) = write_collection_block(
        &merge_blocks,
        &merge_heads,
        COLLECTION_ID,
        "schema-v1",
        test_cid(b"merge document"),
        None,
    )
    .await
    .unwrap();
    let merged_block = Block::from_dag_cbor(&merged_bytes).unwrap();
    assert_eq!(merged_block.heads, Some(expected_siblings));
    drop((merge_blocks, merge_heads));
    commit(merge_txn).await;

    assert_eq!(
        collection_heads(&store, COLLECTION_ID).await,
        vec![merged_cid]
    );
}
