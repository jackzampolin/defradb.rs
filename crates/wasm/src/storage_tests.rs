//! Browser tests for LevelDB storage layer.
//!
//! Exercises the KV store directly to catch edge cases in the
//! LevelDB/OPFS glue code. Uses both in-memory and OPFS-backed stores.

#[cfg(test)]
mod tests {
    use storage::corekv::{IterOptions, Reader, Store, Writer};
    use storage::LevelDbStore;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    // ── Basic read/write ────────────────────────────────────────────

    #[wasm_bindgen_test]
    async fn test_write_then_read() {
        let store = LevelDbStore::open("test_write_read").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key1", b"value1").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let val = txn.get(b"key1").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[wasm_bindgen_test]
    async fn test_read_nonexistent_key() {
        let store = LevelDbStore::open("test_read_missing").unwrap();
        let txn = store.new_txn(true).await.unwrap();
        let val = txn.get(b"no_such_key").await.unwrap();
        assert_eq!(val, None);
    }

    #[wasm_bindgen_test]
    async fn test_has_existing_key() {
        let store = LevelDbStore::open("test_has_key").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"exists", b"yes").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        assert!(txn.has(b"exists").await.unwrap());
        assert!(!txn.has(b"nope").await.unwrap());
    }

    #[wasm_bindgen_test]
    async fn test_get_size() {
        let store = LevelDbStore::open("test_get_size").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"sized", b"hello").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let size = txn.get_size(b"sized").await.unwrap();
        assert_eq!(size, Some(5));
        assert_eq!(txn.get_size(b"missing").await.unwrap(), None);
    }

    // ── Overwrites ──────────────────────────────────────────────────

    #[wasm_bindgen_test]
    async fn test_overwrite_key() {
        let store = LevelDbStore::open("test_overwrite").unwrap();

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key", b"first").await.unwrap();
        txn.commit().await.unwrap();

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"key", b"second").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let val = txn.get(b"key").await.unwrap();
        assert_eq!(val, Some(b"second".to_vec()));
    }

    // ── Deletes ─────────────────────────────────────────────────────

    #[wasm_bindgen_test]
    async fn test_delete_key() {
        let store = LevelDbStore::open("test_delete").unwrap();

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"del_me", b"bye").await.unwrap();
        txn.commit().await.unwrap();

        let mut txn = store.new_txn(false).await.unwrap();
        txn.delete(b"del_me").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(txn.get(b"del_me").await.unwrap(), None);
        assert!(!txn.has(b"del_me").await.unwrap());
    }

    #[wasm_bindgen_test]
    async fn test_delete_nonexistent_key() {
        let store = LevelDbStore::open("test_delete_missing").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        // Deleting a key that doesn't exist should succeed silently
        txn.delete(b"ghost").await.unwrap();
        txn.commit().await.unwrap();
    }

    // ── Transaction semantics ───────────────────────────────────────

    #[wasm_bindgen_test]
    async fn test_discard_rolls_back() {
        let store = LevelDbStore::open("test_discard").unwrap();

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"rollback_key", b"should_not_persist").await.unwrap();
        txn.discard();

        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(txn.get(b"rollback_key").await.unwrap(), None);
    }

    #[wasm_bindgen_test]
    async fn test_readonly_txn_rejects_writes() {
        let store = LevelDbStore::open("test_readonly").unwrap();
        let mut txn = store.new_txn(true).await.unwrap();
        let result = txn.set(b"nope", b"denied").await;
        assert!(result.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_empty_key_rejected() {
        let store = LevelDbStore::open("test_empty_key").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        assert!(txn.set(b"", b"val").await.is_err());
        assert!(txn.get(b"").await.is_err());
        assert!(txn.has(b"").await.is_err());
        assert!(txn.delete(b"").await.is_err());
    }

    #[wasm_bindgen_test]
    async fn test_read_own_writes_within_txn() {
        let store = LevelDbStore::open("test_read_own_writes").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"rw_key", b"rw_val").await.unwrap();
        // Should see our own pending write before commit
        let val = txn.get(b"rw_key").await.unwrap();
        assert_eq!(val, Some(b"rw_val".to_vec()));
    }

    #[wasm_bindgen_test]
    async fn test_snapshot_isolation() {
        let store = LevelDbStore::open("test_snapshot").unwrap();

        // Write initial data
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"iso_key", b"original").await.unwrap();
        txn.commit().await.unwrap();

        // Open read txn (takes snapshot)
        let read_txn = store.new_txn(true).await.unwrap();

        // Write new value in separate txn
        let mut write_txn = store.new_txn(false).await.unwrap();
        write_txn.set(b"iso_key", b"updated").await.unwrap();
        write_txn.commit().await.unwrap();

        // Read txn should still see original (snapshot isolation)
        let val = read_txn.get(b"iso_key").await.unwrap();
        assert_eq!(val, Some(b"original".to_vec()));

        // New read txn sees updated value
        let fresh_txn = store.new_txn(true).await.unwrap();
        let val = fresh_txn.get(b"iso_key").await.unwrap();
        assert_eq!(val, Some(b"updated".to_vec()));
    }

    // ── Multiple keys ───────────────────────────────────────────────

    #[wasm_bindgen_test]
    async fn test_batch_write_many_keys() {
        let store = LevelDbStore::open("test_batch").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        for i in 0..100u32 {
            let key = format!("batch_{:04}", i);
            let val = format!("value_{}", i);
            txn.set(key.as_bytes(), val.as_bytes()).await.unwrap();
        }
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        for i in 0..100u32 {
            let key = format!("batch_{:04}", i);
            let val = format!("value_{}", i);
            let got = txn.get(key.as_bytes()).await.unwrap();
            assert_eq!(got, Some(val.into_bytes()), "mismatch at key {}", key);
        }
    }

    // ── Large values ────────────────────────────────────────────────

    #[wasm_bindgen_test]
    async fn test_large_value() {
        let store = LevelDbStore::open("test_large_val").unwrap();
        // 64 KB value
        let big_value: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"big", &big_value).await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let got = txn.get(b"big").await.unwrap().unwrap();
        assert_eq!(got.len(), 65536);
        assert_eq!(got, big_value);
    }

    // ── Iterator ────────────────────────────────────────────────────

    #[wasm_bindgen_test]
    async fn test_iterator_all_keys() {
        let store = LevelDbStore::open("test_iter_all").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"a", b"1").await.unwrap();
        txn.set(b"b", b"2").await.unwrap();
        txn.set(b"c", b"3").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = txn.iterator(IterOptions::new()).await.unwrap();
        let mut keys = Vec::new();
        while let Some(pair) = iter.next().await.unwrap() {
            keys.push(pair.key_str());
        }
        assert!(keys.contains(&"a".to_string()));
        assert!(keys.contains(&"b".to_string()));
        assert!(keys.contains(&"c".to_string()));
    }

    #[wasm_bindgen_test]
    async fn test_iterator_with_prefix() {
        let store = LevelDbStore::open("test_iter_prefix").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"users/1", b"alice").await.unwrap();
        txn.set(b"users/2", b"bob").await.unwrap();
        txn.set(b"posts/1", b"hello").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let opts = IterOptions::new().with_prefix(b"users/".to_vec());
        let mut iter = txn.iterator(opts).await.unwrap();
        let mut found = Vec::new();
        while let Some(pair) = iter.next().await.unwrap() {
            found.push(pair.value_str());
        }
        assert_eq!(found.len(), 2);
        assert!(found.contains(&"alice".to_string()));
        assert!(found.contains(&"bob".to_string()));
    }

    #[wasm_bindgen_test]
    async fn test_iterator_keys_only() {
        let store = LevelDbStore::open("test_iter_keys").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"k1", b"v1").await.unwrap();
        txn.set(b"k2", b"v2").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let opts = IterOptions::new().with_keys_only(true);
        let mut iter = txn.iterator(opts).await.unwrap();
        while let Some(pair) = iter.next().await.unwrap() {
            assert!(pair.is_key_only(), "Expected key-only pair");
        }
    }

    #[wasm_bindgen_test]
    async fn test_iterator_seek() {
        let store = LevelDbStore::open("test_iter_seek").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"aaa", b"1").await.unwrap();
        txn.set(b"bbb", b"2").await.unwrap();
        txn.set(b"ccc", b"3").await.unwrap();
        txn.set(b"ddd", b"4").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = txn.iterator(IterOptions::new()).await.unwrap();
        // Seek to "bbb" — should position at "bbb"
        let found = iter.seek(b"bbb").await.unwrap();
        assert!(found);
        let pair = iter.next().await.unwrap().unwrap();
        assert_eq!(pair.key_str(), "bbb");
    }

    #[wasm_bindgen_test]
    async fn test_iterator_reverse() {
        let store = LevelDbStore::open("test_iter_reverse").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"r_a", b"1").await.unwrap();
        txn.set(b"r_b", b"2").await.unwrap();
        txn.set(b"r_c", b"3").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let opts = IterOptions::new()
            .with_prefix(b"r_".to_vec())
            .with_reverse(true);
        let mut iter = txn.iterator(opts).await.unwrap();
        let mut keys = Vec::new();
        while let Some(pair) = iter.next().await.unwrap() {
            keys.push(pair.key_str());
        }
        assert_eq!(keys, vec!["r_c", "r_b", "r_a"]);
    }

    // ── Binary data ─────────────────────────────────────────────────

    #[wasm_bindgen_test]
    async fn test_binary_keys_and_values() {
        let store = LevelDbStore::open("test_binary").unwrap();
        let key: Vec<u8> = vec![0x00, 0xFF, 0x01, 0xFE];
        let value: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(&key, &value).await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let got = txn.get(&key).await.unwrap().unwrap();
        assert_eq!(got, value);
    }

    #[wasm_bindgen_test]
    async fn test_zero_length_value() {
        let store = LevelDbStore::open("test_empty_val").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"empty_val", b"").await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let val = txn.get(b"empty_val").await.unwrap();
        assert_eq!(val, Some(vec![]));
        assert!(txn.has(b"empty_val").await.unwrap());
    }

    // ── Store close ─────────────────────────────────────────────────

    #[wasm_bindgen_test]
    async fn test_store_close_then_reject_ops() {
        let store = LevelDbStore::open("test_store_close").unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"pre_close", b"data").await.unwrap();
        txn.commit().await.unwrap();

        store.close().await.unwrap();

        // New transactions should fail after close
        let result = store.new_txn(true).await;
        assert!(result.is_err());
    }

    // ── OPFS persistence ────────────────────────────────────────────

    #[wasm_bindgen_test]
    async fn test_opfs_write_persist_reopen() {
        let db_name = "test_opfs_persist_rw";

        // Write data and persist
        {
            let store = LevelDbStore::open_with_opfs(db_name).await.unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"persist_key", b"persist_value").await.unwrap();
            txn.commit().await.unwrap();
            store.persist().await.unwrap();
            store.close().await.unwrap();
        }

        // Reopen and verify data survived
        {
            let store = LevelDbStore::open_with_opfs(db_name).await.unwrap();
            let txn = store.new_txn(true).await.unwrap();
            let val = txn.get(b"persist_key").await.unwrap();
            assert_eq!(
                val,
                Some(b"persist_value".to_vec()),
                "Data should survive persist + reopen"
            );
        }
    }

    #[wasm_bindgen_test]
    async fn test_opfs_multiple_keys_persist() {
        let db_name = "test_opfs_multi_keys";

        {
            let store = LevelDbStore::open_with_opfs(db_name).await.unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            for i in 0..20u32 {
                let key = format!("opfs_key_{:03}", i);
                let val = format!("opfs_val_{}", i);
                txn.set(key.as_bytes(), val.as_bytes()).await.unwrap();
            }
            txn.commit().await.unwrap();
            store.persist().await.unwrap();
            store.close().await.unwrap();
        }

        {
            let store = LevelDbStore::open_with_opfs(db_name).await.unwrap();
            let txn = store.new_txn(true).await.unwrap();
            for i in 0..20u32 {
                let key = format!("opfs_key_{:03}", i);
                let expected = format!("opfs_val_{}", i);
                let val = txn.get(key.as_bytes()).await.unwrap();
                assert_eq!(
                    val,
                    Some(expected.into_bytes()),
                    "Key {} missing after reopen",
                    key
                );
            }
        }
    }

    #[wasm_bindgen_test]
    async fn test_opfs_delete_persists() {
        let db_name = "test_opfs_delete";

        {
            let store = LevelDbStore::open_with_opfs(db_name).await.unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"keep", b"yes").await.unwrap();
            txn.set(b"remove", b"bye").await.unwrap();
            txn.commit().await.unwrap();

            let mut txn = store.new_txn(false).await.unwrap();
            txn.delete(b"remove").await.unwrap();
            txn.commit().await.unwrap();

            store.persist().await.unwrap();
            store.close().await.unwrap();
        }

        {
            let store = LevelDbStore::open_with_opfs(db_name).await.unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(txn.get(b"keep").await.unwrap(), Some(b"yes".to_vec()));
            assert_eq!(txn.get(b"remove").await.unwrap(), None);
        }
    }

    #[wasm_bindgen_test]
    async fn test_opfs_overwrite_persists() {
        let db_name = "test_opfs_overwrite";

        {
            let store = LevelDbStore::open_with_opfs(db_name).await.unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"mutable", b"v1").await.unwrap();
            txn.commit().await.unwrap();

            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"mutable", b"v2").await.unwrap();
            txn.commit().await.unwrap();

            store.persist().await.unwrap();
            store.close().await.unwrap();
        }

        {
            let store = LevelDbStore::open_with_opfs(db_name).await.unwrap();
            let txn = store.new_txn(true).await.unwrap();
            assert_eq!(txn.get(b"mutable").await.unwrap(), Some(b"v2".to_vec()));
        }
    }

    #[wasm_bindgen_test]
    async fn test_opfs_large_value_persists() {
        let db_name = "test_opfs_large";
        let big: Vec<u8> = (0..32768).map(|i| (i % 251) as u8).collect();

        {
            let store = LevelDbStore::open_with_opfs(db_name).await.unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"big_opfs", &big).await.unwrap();
            txn.commit().await.unwrap();
            store.persist().await.unwrap();
            store.close().await.unwrap();
        }

        {
            let store = LevelDbStore::open_with_opfs(db_name).await.unwrap();
            let txn = store.new_txn(true).await.unwrap();
            let got = txn.get(b"big_opfs").await.unwrap().unwrap();
            assert_eq!(got.len(), 32768);
            assert_eq!(got, big);
        }
    }

    #[wasm_bindgen_test]
    async fn test_opfs_iterator_after_reopen() {
        let db_name = "test_opfs_iter_reopen";

        {
            let store = LevelDbStore::open_with_opfs(db_name).await.unwrap();
            let mut txn = store.new_txn(false).await.unwrap();
            txn.set(b"ns/alpha", b"1").await.unwrap();
            txn.set(b"ns/beta", b"2").await.unwrap();
            txn.set(b"ns/gamma", b"3").await.unwrap();
            txn.set(b"other/x", b"4").await.unwrap();
            txn.commit().await.unwrap();
            store.persist().await.unwrap();
            store.close().await.unwrap();
        }

        {
            let store = LevelDbStore::open_with_opfs(db_name).await.unwrap();
            let txn = store.new_txn(true).await.unwrap();
            let opts = IterOptions::new().with_prefix(b"ns/".to_vec());
            let mut iter = txn.iterator(opts).await.unwrap();
            let mut keys = Vec::new();
            while let Some(pair) = iter.next().await.unwrap() {
                keys.push(pair.key_str());
            }
            assert_eq!(keys.len(), 3);
            assert!(keys.contains(&"ns/alpha".to_string()));
            assert!(keys.contains(&"ns/beta".to_string()));
            assert!(keys.contains(&"ns/gamma".to_string()));
        }
    }
}
