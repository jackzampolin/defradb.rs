use super::*;
use tempfile::TempDir;

/// Insert `count` zero-padded keys (so byte order matches insertion order),
/// then assert a full forward scan yields exactly that ordered key set.
///
/// Exercises `DEFAULT_CHUNK_SIZE` (256) boundaries: below, at, and above a
/// single chunk, plus the empty-store edge case.
async fn assert_full_scan_matches(count: u32) {
    let temp_dir = TempDir::new().unwrap();
    let store = FjallStore::open(temp_dir.path()).unwrap();

    let expected: Vec<Vec<u8>> = (0..count)
        .map(|i| format!("key_{:05}", i).into_bytes())
        .collect();

    let mut txn = store.new_txn(false).await.unwrap();
    for key in &expected {
        txn.set(key, b"v").await.unwrap();
    }
    txn.commit().await.unwrap();

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();
    let mut seen = Vec::new();
    while let Some(kv) = iter.next().await.unwrap() {
        seen.push(kv.key);
    }
    assert_eq!(seen, expected, "chunked forward scan for count={count}");

    // The reverse path stays eager; cross-check it agrees on the same data.
    let mut reverse_iter = txn
        .iterator(IterOptions::new().with_reverse(true))
        .await
        .unwrap();
    let mut seen_reverse = Vec::new();
    while let Some(kv) = reverse_iter.next().await.unwrap() {
        seen_reverse.push(kv.key);
    }
    let mut expected_reverse = expected.clone();
    expected_reverse.reverse();
    assert_eq!(
        seen_reverse, expected_reverse,
        "reverse (eager) scan for count={count}"
    );
}

#[tokio::test]
async fn test_fjall_chunked_scan_crosses_several_chunks() {
    assert_full_scan_matches(1000).await;
}

#[tokio::test]
async fn test_fjall_chunked_scan_exact_chunk_multiple() {
    assert_full_scan_matches(256).await;
}

#[tokio::test]
async fn test_fjall_chunked_scan_one_over_chunk() {
    assert_full_scan_matches(257).await;
}

#[tokio::test]
async fn test_fjall_chunked_scan_single_key() {
    assert_full_scan_matches(1).await;
}

#[tokio::test]
async fn test_fjall_chunked_scan_empty_store() {
    assert_full_scan_matches(0).await;
}

/// Build a store with `count` zero-padded keys and return the expected order.
/// The `TempDir` must outlive the store, so it is returned too.
async fn seeded_store(count: u32) -> (TempDir, FjallStore, Vec<Vec<u8>>) {
    let temp_dir = TempDir::new().unwrap();
    let store = FjallStore::open(temp_dir.path()).unwrap();
    let expected: Vec<Vec<u8>> = (0..count)
        .map(|i| format!("key_{:05}", i).into_bytes())
        .collect();

    let mut txn = store.new_txn(false).await.unwrap();
    for key in &expected {
        txn.set(key, b"v").await.unwrap();
    }
    txn.commit().await.unwrap();

    (temp_dir, store, expected)
}

/// A backward seek after the snapshot has already discarded several
/// windows must re-scan from the true range start, not from wherever the
/// snapshot happens to be — otherwise it silently returns the wrong (or an
/// incomplete) tail. `DEFAULT_CHUNK_SIZE` is 256, so walking to key 300
/// guarantees the window backing keys 0..256 is long gone before the seek.
#[tokio::test]
async fn test_fjall_chunked_seek_backward_across_discarded_windows() {
    let (_temp_dir, store, expected) = seeded_store(1000).await;

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    for expected_key in expected.iter().take(301) {
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(&kv.key, expected_key);
    }

    assert!(
        iter.seek(&expected[5]).await.unwrap(),
        "seek to an existing key must find it"
    );

    let mut seen = Vec::new();
    while let Some(kv) = iter.next().await.unwrap() {
        seen.push(kv.key);
    }
    assert_eq!(
        seen,
        expected[5..].to_vec(),
        "no skips or repeats after a backward seek past discarded windows"
    );
}

/// A forward seek past the currently loaded window (but still within the
/// range) must also land exactly on the target with no skips or repeats.
/// This exercises the same reset-and-walk fallback as the backward case,
/// from the other direction.
#[tokio::test]
async fn test_fjall_chunked_seek_forward_beyond_loaded_window() {
    let (_temp_dir, store, expected) = seeded_store(1000).await;

    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();

    for expected_key in expected.iter().take(301) {
        let kv = iter.next().await.unwrap().unwrap();
        assert_eq!(&kv.key, expected_key);
    }

    // The second window covers roughly [256, 512); 600 is beyond it.
    assert!(
        iter.seek(&expected[600]).await.unwrap(),
        "seek to an existing key must find it"
    );

    let mut seen = Vec::new();
    while let Some(kv) = iter.next().await.unwrap() {
        seen.push(kv.key);
    }
    assert_eq!(
        seen,
        expected[600..].to_vec(),
        "no skips or repeats after a forward seek beyond the loaded window"
    );
}
