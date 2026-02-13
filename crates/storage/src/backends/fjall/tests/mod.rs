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

/// Recursively find files under a directory matching a path substring.
fn find_files_matching(dir: &std::path::Path, pattern: &str) -> Vec<std::path::PathBuf> {
    let mut results = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                results.extend(find_files_matching(&path, pattern));
            } else if path.to_string_lossy().contains(pattern) {
                results.push(path);
            }
        }
    }
    results
}

/// Print the directory tree up to a given depth.
fn print_tree(dir: &std::path::Path, depth: usize, indent: usize) {
    if depth == 0 {
        return;
    }
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            if path.is_dir() {
                eprintln!("{:indent$}{}/", "", name, indent = indent);
                print_tree(&path, depth - 1, indent + 2);
            } else {
                let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                eprintln!("{:indent$}{} ({} bytes)", "", name, size, indent = indent);
            }
        }
    }
}

#[tokio::test]
async fn kv_separation_creates_blob_files() {
    use crate::corekv::{Store, Txn, Writer};
    use config::FjallStoreOptions;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let opts = FjallStoreOptions::default()
        .with_kv_separation(true)
        .with_max_memtable_size(4 * 1024); // 4 KiB to force quick flush

    let store = FjallStore::open_with_options(temp_dir.path(), opts).unwrap();
    assert!(store.is_kv_separated(), "keyspace should use KV separation");

    // Write values larger than the 256-byte separation threshold
    let large_value = vec![0xABu8; 1024]; // 1 KiB value
    for i in 0..100u32 {
        let mut txn = store.new_txn(false).await.unwrap();
        let key = format!("key-{:04}", i);
        txn.set(key.as_bytes(), &large_value).await.unwrap();
        let txn_box: Box<dyn Txn> = txn;
        txn_box.commit().await.unwrap();
    }

    // Give compaction workers time to flush
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let data_fjall = temp_dir.path().join("data.fjall");

    // Print directory tree for debugging
    eprintln!("Directory tree after writes:");
    print_tree(&data_fjall, 5, 0);

    let blob_files = find_files_matching(&data_fjall, "blobs");
    for f in &blob_files {
        let size = std::fs::metadata(f).map(|m| m.len()).unwrap_or(0);
        eprintln!("Found blob file: {} ({} bytes)", f.display(), size);
    }

    assert!(
        !blob_files.is_empty(),
        "Expected blob files to be created with KV separation enabled"
    );
}

#[tokio::test]
async fn kv_separation_disabled_no_blob_files() {
    use crate::corekv::{Store, Txn, Writer};
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

    let large_value = vec![0xABu8; 1024];
    for i in 0..100u32 {
        let mut txn = store.new_txn(false).await.unwrap();
        let key = format!("key-{:04}", i);
        txn.set(key.as_bytes(), &large_value).await.unwrap();
        let txn_box: Box<dyn Txn> = txn;
        txn_box.commit().await.unwrap();
    }

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

    // First: create keyspace WITHOUT KV separation
    {
        let opts = FjallStoreOptions::default().with_kv_separation(false);
        let store = FjallStore::open_with_options(temp_dir.path(), opts).unwrap();
        assert!(!store.is_kv_separated());
        store.close().await.unwrap();
    }

    // Second: reopen with KV separation enabled — keyspace should NOT be separated
    // because fjall only applies create_options on first creation.
    {
        let opts = FjallStoreOptions::default().with_kv_separation(true);
        let store = FjallStore::open_with_options(temp_dir.path(), opts).unwrap();
        assert!(
            !store.is_kv_separated(),
            "Reopening a non-separated keyspace with kv_separation=true should NOT \
             retroactively enable separation. The data directory must be deleted first."
        );
    }
}
