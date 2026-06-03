use super::*;
use crate::corekv::{Dropable, IterOptions, Store, Txn};

mod shared_tests {
    use super::*;
    use crate::generate_backend_concurrency_tests;
    use crate::generate_backend_dropable_tests;
    use crate::generate_backend_tests;
    use tempfile::TempDir;

    struct TestLarkStore {
        store: LarkStore,
        _temp_dir: TempDir,
    }

    impl crate::corekv::private::Sealed for TestLarkStore {}

    #[async_trait::async_trait]
    impl Store for TestLarkStore {
        async fn new_txn(&self, readonly: bool) -> crate::corekv::Result<Box<dyn Txn>> {
            self.store.new_txn(readonly).await
        }
        async fn close(&self) -> crate::corekv::Result<()> {
            self.store.close().await
        }
    }

    #[async_trait::async_trait]
    impl Dropable for TestLarkStore {
        async fn drop_all(&self) -> crate::corekv::Result<()> {
            self.store.drop_all().await
        }
    }

    async fn create_store() -> TestLarkStore {
        let temp_dir = TempDir::new().unwrap();
        let store = LarkStore::open(temp_dir.path()).unwrap();
        TestLarkStore {
            store,
            _temp_dir: temp_dir,
        }
    }

    async fn create_arc_store() -> std::sync::Arc<TestLarkStore> {
        std::sync::Arc::new(create_store().await)
    }

    generate_backend_tests!(create_store);
    generate_backend_concurrency_tests!(create_arc_store);
    generate_backend_dropable_tests!(create_store);
}

#[tokio::test]
async fn readonly_reads_preserve_snapshot_after_later_writes() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let store = LarkStore::open(temp_dir.path()).unwrap();

    let mut setup = store.new_txn(false).await.unwrap();
    setup.set(b"a", b"old").await.unwrap();
    setup.set(b"b", b"keep").await.unwrap();
    setup.commit().await.unwrap();

    let readonly = store.new_txn(true).await.unwrap();

    let mut writer = store.new_txn(false).await.unwrap();
    writer.set(b"a", b"new").await.unwrap();
    writer.delete(b"b").await.unwrap();
    writer.set(b"c", b"later").await.unwrap();
    writer.commit().await.unwrap();

    assert_eq!(readonly.get(b"a").await.unwrap(), Some(b"old".to_vec()));
    assert!(readonly.has(b"b").await.unwrap());
    assert_eq!(readonly.get_size(b"b").await.unwrap(), Some(4));
    assert_eq!(readonly.get(b"c").await.unwrap(), None);

    let mut iter = readonly.iterator(IterOptions::new()).await.unwrap();
    let mut items = Vec::new();
    while let Some(pair) = iter.next().await.unwrap() {
        items.push((pair.key, pair.value));
    }
    assert_eq!(
        items,
        vec![
            (b"a".to_vec(), b"old".to_vec()),
            (b"b".to_vec(), b"keep".to_vec())
        ]
    );

    readonly.discard();
    store.close().await.unwrap();
}
