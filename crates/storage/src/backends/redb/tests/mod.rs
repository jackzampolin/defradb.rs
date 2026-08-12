use super::*;
use crate::corekv::{Dropable, IterOptions, Reader, Store, Txn, Writer};

mod shared_tests {
    use super::*;
    use crate::generate_backend_concurrency_tests;
    use crate::generate_backend_dropable_tests;
    use crate::generate_backend_tests;
    use tempfile::TempDir;

    /// Test wrapper that holds both store and temp directory for cleanup.
    /// When this wrapper is dropped, the TempDir is automatically cleaned up.
    struct TestRedbStore {
        store: RedbStore,
        _temp_dir: TempDir,
    }

    impl crate::corekv::private::Sealed for TestRedbStore {}

    #[async_trait::async_trait]
    impl Store for TestRedbStore {
        async fn new_txn(&self, readonly: bool) -> crate::corekv::Result<Box<dyn Txn>> {
            self.store.new_txn(readonly).await
        }
        async fn close(&self) -> crate::corekv::Result<()> {
            self.store.close().await
        }
    }

    #[async_trait::async_trait]
    impl Dropable for TestRedbStore {
        async fn drop_all(&self) -> crate::corekv::Result<()> {
            self.store.drop_all().await
        }
    }

    async fn create_store() -> TestRedbStore {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.redb");
        let store = RedbStore::open(&path).unwrap();
        TestRedbStore {
            store,
            _temp_dir: temp_dir,
        }
    }

    async fn create_arc_store() -> std::sync::Arc<TestRedbStore> {
        std::sync::Arc::new(create_store().await)
    }

    // Generate all standard backend tests
    generate_backend_tests!(create_store);

    // Generate concurrency tests
    generate_backend_concurrency_tests!(create_arc_store);

    // Generate Dropable tests (RedbStore implements Dropable)
    generate_backend_dropable_tests!(create_store);
}

mod callbacks;
mod chunked_scan;
mod iterators;
mod persistence;
mod store_config;
mod stress;
mod transaction;
