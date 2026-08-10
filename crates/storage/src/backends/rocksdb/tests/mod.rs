use super::*;
use crate::corekv::{Dropable, IterOptions, Reader, Store, Txn};

mod chunked_scan;

mod shared_tests {
    use super::*;
    use crate::generate_backend_concurrency_tests;
    use crate::generate_backend_dropable_tests;
    use crate::generate_backend_tests;
    use tempfile::TempDir;

    struct TestRocksDbStore {
        store: RocksDbStore,
        _temp_dir: TempDir,
    }

    impl crate::corekv::private::Sealed for TestRocksDbStore {}

    #[async_trait::async_trait]
    impl Store for TestRocksDbStore {
        async fn new_txn(&self, readonly: bool) -> crate::corekv::Result<Box<dyn Txn>> {
            self.store.new_txn(readonly).await
        }
        async fn close(&self) -> crate::corekv::Result<()> {
            self.store.close().await
        }
    }

    #[async_trait::async_trait]
    impl Dropable for TestRocksDbStore {
        async fn drop_all(&self) -> crate::corekv::Result<()> {
            self.store.drop_all().await
        }
    }

    async fn create_store() -> TestRocksDbStore {
        let temp_dir = TempDir::new().unwrap();
        let store = RocksDbStore::open(temp_dir.path()).unwrap();
        TestRocksDbStore {
            store,
            _temp_dir: temp_dir,
        }
    }

    async fn create_arc_store() -> std::sync::Arc<TestRocksDbStore> {
        std::sync::Arc::new(create_store().await)
    }

    generate_backend_tests!(create_store);
    generate_backend_concurrency_tests!(create_arc_store);
    generate_backend_dropable_tests!(create_store);
}
