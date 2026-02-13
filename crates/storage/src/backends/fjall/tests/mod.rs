use super::*;
use crate::corekv::{Dropable, Store, Txn};

mod shared_tests {
    use super::*;
    use crate::generate_backend_concurrency_tests;
    use crate::generate_backend_dropable_tests;
    use crate::generate_backend_tests;
    use tempfile::TempDir;

    struct TestFjallStore {
        store: FjallStore,
        _temp_dir: TempDir,
    }

    #[async_trait::async_trait]
    impl Store for TestFjallStore {
        async fn new_txn(&self, readonly: bool) -> crate::corekv::Result<Box<dyn Txn>> {
            self.store.new_txn(readonly).await
        }
        async fn close(&self) -> crate::corekv::Result<()> {
            self.store.close().await
        }
    }

    #[async_trait::async_trait]
    impl Dropable for TestFjallStore {
        async fn drop_all(&self) -> crate::corekv::Result<()> {
            self.store.drop_all().await
        }
    }

    async fn create_store() -> TestFjallStore {
        let temp_dir = TempDir::new().unwrap();
        let store = FjallStore::open(temp_dir.path()).unwrap();
        TestFjallStore {
            store,
            _temp_dir: temp_dir,
        }
    }

    async fn create_arc_store() -> std::sync::Arc<TestFjallStore> {
        std::sync::Arc::new(create_store().await)
    }

    generate_backend_tests!(create_store);
    generate_backend_concurrency_tests!(create_arc_store);
    generate_backend_dropable_tests!(create_store);
}
