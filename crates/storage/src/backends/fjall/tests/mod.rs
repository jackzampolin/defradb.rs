use super::*;
use crate::corekv::{Dropable, IterOptions, Reader, Store, Txn};

mod chunked_scan;

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

    impl crate::corekv::private::Sealed for TestFjallStore {}

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

fn find_files_matching(dir: &std::path::Path, pattern: &str) -> Vec<std::path::PathBuf> {
    let mut results = Vec::new();
    let entries = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("failed to read directory '{}': {}", dir.display(), e));
    for entry in entries {
        let entry =
            entry.unwrap_or_else(|e| panic!("failed to read entry in '{}': {}", dir.display(), e));
        let path = entry.path();
        if path.is_dir() {
            results.extend(find_files_matching(&path, pattern));
        } else if path.to_string_lossy().contains(pattern) {
            results.push(path);
        }
    }
    results
}

async fn write_large_values(store: &FjallStore, count: u32) {
    use crate::corekv::{Store, Writer};

    let large_value = vec![0xABu8; 1024];
    for i in 0..count {
        let mut txn = store.new_txn(false).await.unwrap();
        let key = format!("key-{:04}", i);
        txn.set(key.as_bytes(), &large_value).await.unwrap();
        txn.commit().await.unwrap();
    }
}

#[tokio::test]
async fn kv_separation_creates_blob_files() {
    use config::FjallStoreOptions;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let opts = FjallStoreOptions::default()
        .with_kv_separation(true)
        .with_max_memtable_size(4 * 1024); // Small memtable to force flush during test

    let store = FjallStore::open_with_options(temp_dir.path(), opts).unwrap();
    assert!(store.is_kv_separated(), "keyspace should use KV separation");

    // Write values larger than the KV separation threshold
    write_large_values(&store, 100).await;

    // Allow time for background compaction to flush memtable to disk
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let data_fjall = temp_dir.path().join("data.fjall");
    let blob_files = find_files_matching(&data_fjall, "blobs");

    assert!(
        !blob_files.is_empty(),
        "Expected blob files to be created with KV separation enabled"
    );
}

#[tokio::test]
async fn kv_separation_disabled_no_blob_files() {
    use config::FjallStoreOptions;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let opts = FjallStoreOptions::default()
        .with_kv_separation(false)
        .with_max_memtable_size(4 * 1024);

    let store = FjallStore::open_with_options(temp_dir.path(), opts).unwrap();
    assert!(
        !store.is_kv_separated(),
        "keyspace should NOT use KV separation"
    );

    write_large_values(&store, 100).await;

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let data_fjall = temp_dir.path().join("data.fjall");
    let blob_files = find_files_matching(&data_fjall, "blobs");

    assert!(
        blob_files.is_empty(),
        "No blob files should exist with KV separation disabled"
    );
}

#[tokio::test]
async fn kv_separation_stale_keyspace_not_separated() {
    use config::FjallStoreOptions;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();

    {
        let opts = FjallStoreOptions::default().with_kv_separation(false);
        let store = FjallStore::open_with_options(temp_dir.path(), opts).unwrap();
        assert!(!store.is_kv_separated());
        store.close().await.unwrap();
    }

    // Reopen with KV separation enabled — should error because fjall only
    // applies create_options on first creation.
    {
        let opts = FjallStoreOptions::default().with_kv_separation(true);
        match FjallStore::open_with_options(temp_dir.path(), opts) {
            Ok(_) => {
                panic!("should fail when KV separation is requested on a non-separated keyspace")
            }
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("KV separation requested"),
                    "error should mention KV separation mismatch, got: {msg}"
                );
            }
        }
    }
}
