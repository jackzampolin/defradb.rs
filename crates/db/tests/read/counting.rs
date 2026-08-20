use crate::common::counting_store::CountingStore;
use storage::corekv::IterOptions;
use storage::corekv::Store;
#[tokio::test]
async fn counts_keys_pulled_from_the_iterator_not_keys_present() {
    let store = CountingStore::new(storage::MemoryStore::new());

    let mut txn = store.new_txn(false).await.unwrap();
    for i in 0..100u32 {
        txn.set(format!("k{i:03}").as_bytes(), b"v").await.unwrap();
    }
    txn.commit().await.unwrap();

    let before = store.keys_read();
    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();
    for _ in 0..3 {
        iter.next().await.unwrap().unwrap();
    }

    assert_eq!(store.keys_read() - before, 3);
}

#[tokio::test]
async fn counts_point_gets_separately() {
    let store = CountingStore::new(storage::MemoryStore::new());

    let mut txn = store.new_txn(false).await.unwrap();
    txn.set(b"a", b"1").await.unwrap();
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    txn.get(b"a").await.unwrap();
    txn.get(b"missing").await.unwrap();

    assert_eq!(store.point_gets(), 2);
    assert_eq!(store.keys_read(), 0);
}
