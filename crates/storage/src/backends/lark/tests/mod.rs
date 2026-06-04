use super::*;
use crate::backends::DurabilityMode;
use crate::corekv::{Dropable, IterOptions, Store, Txn};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Seek, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

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

#[tokio::test]
async fn data_survives_close_reopen() {
    let temp_dir = tempfile::TempDir::new().unwrap();

    {
        let store = LarkStore::open(temp_dir.path()).unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"persistent_key", b"persistent_value")
            .await
            .unwrap();
        txn.commit().await.unwrap();
        store.close().await.unwrap();
    }

    {
        let store = LarkStore::open(temp_dir.path()).unwrap();
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(
            txn.get(b"persistent_key").await.unwrap(),
            Some(b"persistent_value".to_vec())
        );
        txn.discard();
        store.close().await.unwrap();
    }
}

#[tokio::test]
async fn uncommitted_data_is_lost_on_reopen() {
    let temp_dir = tempfile::TempDir::new().unwrap();

    {
        let store = LarkStore::open(temp_dir.path()).unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"uncommitted_key", b"value").await.unwrap();
        txn.discard();
        store.close().await.unwrap();
    }

    {
        let store = LarkStore::open(temp_dir.path()).unwrap();
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(txn.get(b"uncommitted_key").await.unwrap(), None);
        txn.discard();
        store.close().await.unwrap();
    }
}

#[tokio::test]
async fn persistence_through_multiple_sessions() {
    let temp_dir = tempfile::TempDir::new().unwrap();

    {
        let store = LarkStore::open(temp_dir.path()).unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key1", b"value1").await.unwrap();
        txn.set(b"key2", b"value2").await.unwrap();
        txn.commit().await.unwrap();
        store.close().await.unwrap();
    }

    {
        let store = LarkStore::open(temp_dir.path()).unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key1", b"modified").await.unwrap();
        txn.set(b"key3", b"value3").await.unwrap();
        txn.delete(b"key2").await.unwrap();
        txn.commit().await.unwrap();
        store.close().await.unwrap();
    }

    {
        let store = LarkStore::open(temp_dir.path()).unwrap();
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(txn.get(b"key1").await.unwrap(), Some(b"modified".to_vec()));
        assert_eq!(txn.get(b"key2").await.unwrap(), None);
        assert_eq!(txn.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
        txn.discard();
        store.close().await.unwrap();
    }
}

#[tokio::test]
async fn encrypted_data_survives_reopen_and_rejects_wrong_key() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let key_a = [11u8; 32];
    let key_b = [12u8; 32];
    let storage_key = b"encrypted:persistent_key";
    let plaintext = b"persistent_value";

    {
        let inner = LarkStore::open_with_options(
            temp_dir.path(),
            LarkStoreOptions::new().with_durability(DurabilityMode::Immediate),
        )
        .unwrap();
        let store = crate::encrypted_store::EncryptedStore::new(inner, key_a);
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(storage_key, plaintext).await.unwrap();
        txn.commit().await.unwrap();
        store.close().await.unwrap();
    }

    {
        let store = LarkStore::open_with_options(
            temp_dir.path(),
            LarkStoreOptions::new().with_durability(DurabilityMode::Immediate),
        )
        .unwrap();
        let txn = store.new_txn(true).await.unwrap();
        let raw = txn.get(storage_key).await.unwrap().unwrap();
        assert_ne!(raw, plaintext);
        assert!(raw.len() > plaintext.len());
        txn.discard();
        store.close().await.unwrap();
    }

    {
        let inner = LarkStore::open_with_options(
            temp_dir.path(),
            LarkStoreOptions::new().with_durability(DurabilityMode::Immediate),
        )
        .unwrap();
        let store = crate::encrypted_store::EncryptedStore::new(inner, key_a);
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(
            txn.get(storage_key).await.unwrap(),
            Some(plaintext.to_vec())
        );
        txn.discard();
        store.close().await.unwrap();
    }

    {
        let inner = LarkStore::open_with_options(
            temp_dir.path(),
            LarkStoreOptions::new().with_durability(DurabilityMode::Immediate),
        )
        .unwrap();
        let store = crate::encrypted_store::EncryptedStore::new(inner, key_b);
        let txn = store.new_txn(true).await.unwrap();
        let err = txn.get(storage_key).await.unwrap_err();
        assert!(
            matches!(err, crate::corekv::Error::Other(ref message) if message.contains("decryption failed")),
            "expected wrong key to fail decryption, got: {err:?}"
        );
        txn.discard();
        store.close().await.unwrap();
    }
}

#[tokio::test]
#[ignore]
async fn low_level_crash_immediate_durability_preserves_synced_shadow_log() {
    let target_ops = std::env::var("LARK_LOW_LEVEL_CRASH_OPS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_000);

    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let shadow_path = temp_dir.path().join("shadow.log");
    let progress_path = temp_dir.path().join("shadow.progress");

    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--ignored")
        .arg("low_level_crash_writer_helper")
        .env("LARK_LOW_LEVEL_CRASH_HELPER", "1")
        .env("LARK_LOW_LEVEL_CRASH_DB", &db_path)
        .env("LARK_LOW_LEVEL_CRASH_SHADOW", &shadow_path)
        .env("LARK_LOW_LEVEL_CRASH_PROGRESS", &progress_path)
        .env(
            "LARK_LOW_LEVEL_CRASH_MAX_OPS",
            (target_ops * 20).to_string(),
        )
        .env(
            "LARK_LOW_LEVEL_CRASH_HOLD_AFTER_OPS",
            target_ops.to_string(),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let timeout_secs = 60_u64.max((target_ops as u64).saturating_div(40));
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if read_synced_shadow_progress(&progress_path) >= target_ops {
            break;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("crash writer exited before target ops: {status}");
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {timeout_secs}s waiting for {target_ops} synced shadow ops"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    child.kill().unwrap();
    let _ = child.wait();

    let synced_ops = read_synced_shadow_progress(&progress_path);
    assert!(
        synced_ops >= target_ops,
        "expected at least {target_ops} synced ops, got {synced_ops}"
    );
    verify_synced_shadow_log(&db_path, &shadow_path, synced_ops).await;
}

#[tokio::test]
#[ignore]
async fn low_level_crash_writer_helper() {
    if std::env::var("LARK_LOW_LEVEL_CRASH_HELPER").as_deref() != Ok("1") {
        return;
    }

    let db_path = std::env::var_os("LARK_LOW_LEVEL_CRASH_DB").unwrap();
    let shadow_path = std::env::var_os("LARK_LOW_LEVEL_CRASH_SHADOW").unwrap();
    let progress_path = std::env::var_os("LARK_LOW_LEVEL_CRASH_PROGRESS").unwrap();
    let max_ops = std::env::var("LARK_LOW_LEVEL_CRASH_MAX_OPS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000);
    let hold_after_ops = std::env::var("LARK_LOW_LEVEL_CRASH_HOLD_AFTER_OPS")
        .ok()
        .and_then(|value| value.parse().ok());

    let opts = LarkStoreOptions::new().with_durability(DurabilityMode::Immediate);
    let store = LarkStore::open_with_options(db_path, opts).unwrap();
    let mut shadow = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(shadow_path)
        .unwrap();
    let mut progress = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(progress_path)
        .unwrap();
    let mut rng = CrashRng::new(0xdefa_db1a_1a4c_u64);

    for op_idx in 0..max_ops {
        let key = random_crash_key(&mut rng, 10_000);
        let delete = rng.next_u64() % 10 == 0;
        let line = if delete {
            let mut txn = store.new_txn(false).await.unwrap();
            txn.delete(&key).await.unwrap();
            txn.commit().await.unwrap();
            format!("DEL {}\n", hex::encode(&key))
        } else {
            let value = random_crash_value(&mut rng, 512, op_idx as u64);
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(&key, &value).await.unwrap();
            txn.commit().await.unwrap();
            format!("PUT {} {}\n", hex::encode(&key), hex::encode(&value))
        };

        shadow.write_all(line.as_bytes()).unwrap();
        shadow.sync_all().unwrap();
        progress.set_len(0).unwrap();
        progress.rewind().unwrap();
        writeln!(progress, "{}", op_idx + 1).unwrap();
        progress.sync_all().unwrap();

        if hold_after_ops.is_some_and(|target| op_idx + 1 >= target) {
            loop {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

async fn verify_synced_shadow_log(db_path: &Path, shadow_path: &Path, synced_ops: usize) {
    let (expected, touched) = replay_synced_shadow_log(shadow_path, synced_ops);
    let opts = LarkStoreOptions::new().with_durability(DurabilityMode::Immediate);
    let store = LarkStore::open_with_options(db_path, opts).unwrap();
    let txn = store.new_txn(true).await.unwrap();

    for key in touched {
        let got = txn.get(&key).await.unwrap();
        match expected.get(&key) {
            Some(value) => assert_eq!(got, Some(value.clone()), "key {:?}", key),
            None => assert_eq!(got, None, "key {:?}", key),
        }
    }

    txn.discard();
    store.close().await.unwrap();
}

fn replay_synced_shadow_log(
    shadow_path: &Path,
    synced_ops: usize,
) -> (BTreeMap<Vec<u8>, Vec<u8>>, BTreeSet<Vec<u8>>) {
    let bytes = std::fs::read(shadow_path).unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    let mut expected = BTreeMap::new();
    let mut touched = BTreeSet::new();
    let mut lines_seen = 0;

    for line in text.lines().take(synced_ops) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        match parts.as_slice() {
            ["PUT", key, value] => {
                let key = hex::decode(key).unwrap();
                let value = hex::decode(value).unwrap();
                touched.insert(key.clone());
                expected.insert(key, value);
            }
            ["DEL", key] => {
                let key = hex::decode(key).unwrap();
                touched.insert(key.clone());
                expected.remove(&key);
            }
            _ => panic!("invalid shadow line: {line}"),
        }
        lines_seen += 1;
    }

    assert_eq!(
        lines_seen, synced_ops,
        "shadow log ended before synced progress marker"
    );
    (expected, touched)
}

fn read_synced_shadow_progress(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
}

fn random_crash_key(rng: &mut CrashRng, key_range: u64) -> Vec<u8> {
    (rng.next_u64() % key_range).to_be_bytes().to_vec()
}

fn random_crash_value(rng: &mut CrashRng, value_size: usize, op_idx: u64) -> Vec<u8> {
    let mut value = vec![0; value_size];
    value[..8].copy_from_slice(&op_idx.to_be_bytes());
    for chunk in value[8..].chunks_mut(8) {
        let random = rng.next_u64().to_be_bytes();
        let len = chunk.len();
        chunk.copy_from_slice(&random[..len]);
    }
    value
}

struct CrashRng(u64);

impl CrashRng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}
