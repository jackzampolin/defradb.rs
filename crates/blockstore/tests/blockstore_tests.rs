//! Integration tests for the blockstore crate
//!
//! Organized into submodules:
//! - `basic_crud` — Put, get, has, delete, size, all_cids, deduplication
//! - `hash_verification` — hash_on_read toggle, valid/corrupted/unsupported hashes
//! - `merge_tracking` — P2P merge lifecycle, local mode, filtering
//! - `go_compat` — Go behavior parity (CIDv0, immutability, key format)
//! - `concurrency` — Concurrent reads, writes, deletes, merge ops
//! - `stress` — Many-block and batch scaling
//! - `error_paths` — Malformed keys, key format invariants

use std::str::FromStr;
use std::sync::Arc;

use blockstore::{Blockstore, DefraBlockstore, Error};
use bytes::Bytes;
use cid::Cid;
use storage::backends::MemoryStore;
use storage::corekv::{Key, Store};
use storage::stores::blockstore::BlockstoreTxn;

fn test_cid() -> Cid {
    Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
}

fn test_cid2() -> Cid {
    Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap()
}

fn test_cid3() -> Cid {
    Cid::from_str("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku").unwrap()
}

/// Create a CID from data using SHA2-256 (for hash verification tests)
fn cid_from_data(data: &[u8]) -> Cid {
    use multihash::Multihash;
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();

    let hash = Multihash::wrap(0x12, &digest).unwrap();
    Cid::new_v1(0x55, hash) // raw codec
}

mod basic_crud {
    use super::*;

    #[tokio::test]
    async fn put_get() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let cid = test_cid();
        let data = b"hello world";

        blockstore.put(&cid, data).await.unwrap();

        let retrieved = blockstore.get(&cid).await.unwrap();
        assert_eq!(retrieved, Some(Bytes::copy_from_slice(data)));
    }

    #[tokio::test]
    async fn get_nonexistent_block() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let result = blockstore.get(&test_cid()).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn has() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let cid = test_cid();
        let data = b"test data";

        assert!(!blockstore.has(&cid).await.unwrap());
        blockstore.put(&cid, data).await.unwrap();
        assert!(blockstore.has(&cid).await.unwrap());
    }

    #[tokio::test]
    async fn delete() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let cid = test_cid();
        let data = b"to be deleted";

        blockstore.put(&cid, data).await.unwrap();
        assert!(blockstore.has(&cid).await.unwrap());

        blockstore.delete(&cid).await.unwrap();

        assert!(!blockstore.has(&cid).await.unwrap());
        assert_eq!(blockstore.get(&cid).await.unwrap(), None);
    }

    #[tokio::test]
    async fn get_size() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let cid = test_cid();
        let data = b"test data for size";

        assert_eq!(blockstore.get_size(&cid).await.unwrap(), None);

        blockstore.put(&cid, data).await.unwrap();

        let size = blockstore.get_size(&cid).await.unwrap();
        assert_eq!(size, Some(data.len()));
    }

    #[tokio::test]
    async fn put_many() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let cid1 = test_cid();
        let cid2 = test_cid2();
        let data1 = b"block one";
        let data2 = b"block two";

        let blocks: Vec<(&Cid, &[u8])> = vec![(&cid1, data1.as_slice()), (&cid2, data2.as_slice())];
        blockstore.put_many(&blocks).await.unwrap();

        assert_eq!(
            blockstore.get(&cid1).await.unwrap(),
            Some(Bytes::copy_from_slice(data1))
        );
        assert_eq!(
            blockstore.get(&cid2).await.unwrap(),
            Some(Bytes::copy_from_slice(data2))
        );
    }

    #[tokio::test]
    async fn put_many_empty() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let blocks: Vec<(&Cid, &[u8])> = vec![];
        blockstore.put_many(&blocks).await.unwrap();
    }

    #[tokio::test]
    async fn all_cids() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let cid1 = test_cid();
        let cid2 = test_cid2();
        let cid3 = test_cid3();

        let cids = blockstore.all_cids().await.unwrap();
        assert!(cids.is_empty());

        blockstore.put(&cid1, b"data1").await.unwrap();
        blockstore.put(&cid2, b"data2").await.unwrap();
        blockstore.put(&cid3, b"data3").await.unwrap();

        let cids = blockstore.all_cids().await.unwrap();
        assert_eq!(cids.len(), 3);
        assert!(cids.contains(&cid1));
        assert!(cids.contains(&cid2));
        assert!(cids.contains(&cid3));
    }

    #[tokio::test]
    async fn deduplication() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let cid = test_cid();
        let data = b"original data";

        blockstore.put(&cid, data).await.unwrap();
        blockstore.put(&cid, data).await.unwrap();

        let cids = blockstore.all_cids().await.unwrap();
        assert_eq!(cids.len(), 1);
        assert_eq!(cids[0], cid);
    }
}

mod hash_verification {
    use super::*;

    #[tokio::test]
    async fn disabled_by_default() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);
        assert!(!blockstore.rehash_enabled());
    }

    #[tokio::test]
    async fn enable_disable() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        blockstore.hash_on_read(true);
        assert!(blockstore.rehash_enabled());

        blockstore.hash_on_read(false);
        assert!(!blockstore.rehash_enabled());
    }

    #[tokio::test]
    async fn valid_data() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let data = b"test data for hash verification";
        let cid = cid_from_data(data);

        blockstore.put(&cid, data).await.unwrap();
        blockstore.hash_on_read(true);

        let result = blockstore.get(&cid).await.unwrap();
        assert_eq!(result, Some(Bytes::copy_from_slice(data)));
    }

    #[tokio::test]
    async fn corrupted_data() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let original_data = b"original data";
        let corrupted_data = b"corrupted data";
        let cid = cid_from_data(original_data);

        {
            let mut txn = blockstore.new_store_txn(false).await.unwrap();
            let bs_txn = txn.as_any_mut().downcast_mut::<BlockstoreTxn>().unwrap();
            bs_txn.put_block(&cid, corrupted_data).await.unwrap();
            txn.commit().await.unwrap();
        }

        blockstore.hash_on_read(false);
        let result = blockstore.get(&cid).await.unwrap();
        assert_eq!(result, Some(Bytes::from(corrupted_data.to_vec())));

        blockstore.hash_on_read(true);
        let result = blockstore.get(&cid).await;
        assert!(result.is_err());
        match result {
            Err(Error::HashMismatch { cid: cid_str }) => {
                assert_eq!(cid_str, cid.to_string());
            }
            other => panic!("Expected HashMismatch error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn nonexistent_block() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        blockstore.hash_on_read(true);

        let result = blockstore.get(&test_cid()).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn unsupported_algorithm_skipped() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        use multihash::Multihash;
        let data = b"identity hash data";
        let hash = Multihash::wrap(0x00, data).unwrap();
        let cid = Cid::new_v1(0x55, hash);

        blockstore.put(&cid, data).await.unwrap();
        blockstore.hash_on_read(true);

        let result = blockstore.get(&cid).await.unwrap();
        assert_eq!(result, Some(Bytes::copy_from_slice(data)));
    }

    #[tokio::test]
    async fn blake2b_skipped() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        use multihash::Multihash;
        let data = b"blake2b test data";
        let fake_digest = [0u8; 32];
        let hash = Multihash::wrap(0xb220, &fake_digest).unwrap();
        let cid = Cid::new_v1(0x55, hash);

        blockstore.put(&cid, data).await.unwrap();
        blockstore.hash_on_read(true);

        let result = blockstore.get(&cid).await.unwrap();
        assert_eq!(result, Some(Bytes::copy_from_slice(data)));
    }

    #[tokio::test]
    async fn empty_block() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let data: &[u8] = b"";
        let cid = cid_from_data(data);

        blockstore.put(&cid, data).await.unwrap();

        let retrieved = blockstore.get(&cid).await.unwrap();
        assert_eq!(retrieved, Some(Bytes::new()));
        assert_eq!(blockstore.get_size(&cid).await.unwrap(), Some(0));
        assert!(blockstore.has(&cid).await.unwrap());

        blockstore.hash_on_read(true);
        let verified = blockstore.get(&cid).await.unwrap();
        assert_eq!(verified, Some(Bytes::new()));
    }

    #[tokio::test]
    async fn large_block() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let data: Vec<u8> = (0..262144).map(|i| (i % 256) as u8).collect();
        let cid = cid_from_data(&data);

        blockstore.put(&cid, &data).await.unwrap();

        let retrieved = blockstore.get(&cid).await.unwrap();
        assert_eq!(retrieved, Some(Bytes::from(data.clone())));
        assert_eq!(blockstore.get_size(&cid).await.unwrap(), Some(262144));

        blockstore.hash_on_read(true);
        let verified = blockstore.get(&cid).await.unwrap();
        assert_eq!(verified, Some(Bytes::from(data)));
    }

    #[tokio::test]
    async fn large_block_many() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let data1: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
        let data2: Vec<u8> = (0..65536).map(|i| ((i + 100) % 256) as u8).collect();
        let data3: Vec<u8> = (0..65536).map(|i| ((i + 200) % 256) as u8).collect();

        let cid1 = cid_from_data(&data1);
        let cid2 = cid_from_data(&data2);
        let cid3 = cid_from_data(&data3);

        let blocks: Vec<(&Cid, &[u8])> = vec![
            (&cid1, data1.as_slice()),
            (&cid2, data2.as_slice()),
            (&cid3, data3.as_slice()),
        ];
        blockstore.put_many(&blocks).await.unwrap();

        assert_eq!(
            blockstore.get(&cid1).await.unwrap(),
            Some(Bytes::from(data1))
        );
        assert_eq!(
            blockstore.get(&cid2).await.unwrap(),
            Some(Bytes::from(data2))
        );
        assert_eq!(
            blockstore.get(&cid3).await.unwrap(),
            Some(Bytes::from(data3))
        );
    }
}

mod merge_tracking {
    use super::*;

    #[tokio::test]
    async fn p2p_lifecycle() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let cid = test_cid();
        let data = b"p2p block";

        blockstore.put(&cid, data).await.unwrap();
        assert!(!blockstore.is_merged(&cid).await.unwrap());

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert!(unmerged.contains(&cid));

        blockstore.mark_as_merged(&cid).await.unwrap();
        assert!(blockstore.is_merged(&cid).await.unwrap());

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert!(!unmerged.contains(&cid));
    }

    #[tokio::test]
    async fn local_mode_no_tracking() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let cid = test_cid();
        blockstore.put(&cid, b"local block").await.unwrap();

        assert!(blockstore.is_merged(&cid).await.unwrap());

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert!(unmerged.is_empty());
    }

    #[tokio::test]
    async fn is_merged_nonexistent_p2p() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);
        assert!(!blockstore.is_merged(&test_cid()).await.unwrap());
    }

    #[tokio::test]
    async fn is_merged_nonexistent_local() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);
        assert!(!blockstore.is_merged(&test_cid()).await.unwrap());
    }

    #[tokio::test]
    async fn unmerged_filtering() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let cid1 = test_cid();
        let cid2 = test_cid2();

        blockstore.put(&cid1, b"data1").await.unwrap();
        blockstore.put(&cid2, b"data2").await.unwrap();

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert_eq!(unmerged.len(), 2);

        blockstore.mark_as_merged(&cid1).await.unwrap();

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert_eq!(unmerged.len(), 1);
        assert!(unmerged.contains(&cid2));
        assert!(!unmerged.contains(&cid1));
    }

    #[tokio::test]
    async fn all_cids_excludes_merge_markers() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let cid1 = test_cid();
        let cid2 = test_cid2();

        blockstore.put(&cid1, b"data1").await.unwrap();
        blockstore.put(&cid2, b"data2").await.unwrap();

        let cids = blockstore.all_cids().await.unwrap();
        assert_eq!(cids.len(), 2);
        assert!(cids.contains(&cid1));
        assert!(cids.contains(&cid2));
    }

    #[tokio::test]
    async fn delete_removes_merge_marker() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let cid = test_cid();
        blockstore.put(&cid, b"data").await.unwrap();
        assert!(!blockstore.is_merged(&cid).await.unwrap());

        blockstore.delete(&cid).await.unwrap();

        assert!(!blockstore.has(&cid).await.unwrap());
        assert!(!blockstore.is_merged(&cid).await.unwrap());

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert!(!unmerged.contains(&cid));
    }

    #[tokio::test]
    async fn mark_nonexistent_is_noop() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let cid = test_cid();
        let result = blockstore.mark_as_merged(&cid).await;
        assert!(result.is_ok());
        assert!(!blockstore.has(&cid).await.unwrap());
        assert!(!blockstore.is_merged(&cid).await.unwrap());
    }

    #[tokio::test]
    async fn delete_then_is_merged() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let cid = test_cid();
        blockstore
            .put(&cid, b"block to merge then delete")
            .await
            .unwrap();
        blockstore.mark_as_merged(&cid).await.unwrap();
        assert!(blockstore.is_merged(&cid).await.unwrap());

        blockstore.delete(&cid).await.unwrap();

        assert!(
            !blockstore.is_merged(&cid).await.unwrap(),
            "is_merged should return false for deleted block"
        );
        assert!(!blockstore.has(&cid).await.unwrap());
        assert_eq!(blockstore.get(&cid).await.unwrap(), None);

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert!(!unmerged.contains(&cid));
    }

    #[tokio::test]
    async fn delete_unmerged_block() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let cid = test_cid();
        blockstore.put(&cid, b"unmerged block").await.unwrap();
        assert!(!blockstore.is_merged(&cid).await.unwrap());

        blockstore.delete(&cid).await.unwrap();

        assert!(!blockstore.is_merged(&cid).await.unwrap());

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert!(!unmerged.contains(&cid));
    }

    #[tokio::test]
    async fn merge_delete_reput() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let cid = test_cid();

        blockstore.put(&cid, b"first").await.unwrap();
        blockstore.mark_as_merged(&cid).await.unwrap();
        assert!(blockstore.is_merged(&cid).await.unwrap());

        blockstore.delete(&cid).await.unwrap();
        assert!(!blockstore.is_merged(&cid).await.unwrap());

        blockstore.put(&cid, b"second").await.unwrap();

        assert!(
            !blockstore.is_merged(&cid).await.unwrap(),
            "Re-added block should be unmerged"
        );

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert!(
            unmerged.contains(&cid),
            "Re-added block should appear in unmerged list"
        );
    }
}

mod go_compat {
    use super::*;

    #[tokio::test]
    async fn get_with_default_cid_returns_none() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let zero_bytes = vec![0x12, 0x20];
        let result = Cid::try_from(zero_bytes.as_slice());
        assert!(result.is_err());

        let empty_data_cid = cid_from_data(b"");
        let result = blockstore.get(&empty_data_cid).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn operations_with_cidv0() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let cidv0 = Cid::from_str("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n").unwrap();
        let data = b"cidv0 test data";

        blockstore.put(&cidv0, data).await.unwrap();
        assert!(blockstore.has(&cidv0).await.unwrap());
        assert_eq!(
            blockstore.get(&cidv0).await.unwrap(),
            Some(Bytes::copy_from_slice(data))
        );
        assert_eq!(blockstore.get_size(&cidv0).await.unwrap(), Some(data.len()));

        let cids = blockstore.all_cids().await.unwrap();
        assert!(cids.contains(&cidv0));

        blockstore.delete(&cidv0).await.unwrap();
        assert!(!blockstore.has(&cidv0).await.unwrap());
    }

    #[tokio::test]
    async fn put_already_merged_stays_merged() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let cid = test_cid();
        let data = b"original data";

        blockstore.put(&cid, data).await.unwrap();
        assert!(!blockstore.is_merged(&cid).await.unwrap());

        blockstore.mark_as_merged(&cid).await.unwrap();
        assert!(blockstore.is_merged(&cid).await.unwrap());

        blockstore.put(&cid, data).await.unwrap();

        assert!(
            blockstore.is_merged(&cid).await.unwrap(),
            "Re-putting an already-merged block should not create a new merge marker"
        );

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert!(
            !unmerged.contains(&cid),
            "Re-put block should not appear in unmerged list"
        );
    }

    #[tokio::test]
    async fn put_many_with_existing_merged() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let cid1 = test_cid();
        let cid2 = test_cid2();
        let data1 = b"data one";
        let data2 = b"data two";

        blockstore.put(&cid1, data1).await.unwrap();
        blockstore.mark_as_merged(&cid1).await.unwrap();
        assert!(blockstore.is_merged(&cid1).await.unwrap());

        let blocks: Vec<(&Cid, &[u8])> = vec![(&cid1, data1.as_slice()), (&cid2, data2.as_slice())];
        blockstore.put_many(&blocks).await.unwrap();

        assert!(
            blockstore.is_merged(&cid1).await.unwrap(),
            "Existing merged block should stay merged after put_many"
        );
        assert!(!blockstore.is_merged(&cid2).await.unwrap());

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert!(!unmerged.contains(&cid1));
        assert!(unmerged.contains(&cid2));
    }

    #[tokio::test]
    async fn same_cid_different_data_no_overwrite() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let cid = test_cid();
        let original_data = b"original data";
        let new_data = b"different data";

        blockstore.put(&cid, original_data).await.unwrap();
        blockstore.put(&cid, new_data).await.unwrap();

        let retrieved = blockstore.get(&cid).await.unwrap();
        assert_eq!(retrieved, Some(Bytes::copy_from_slice(original_data)));
    }

    #[tokio::test]
    async fn put_many_duplicate_cids_in_batch() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let cid = test_cid();
        let data1 = b"first data";
        let data2 = b"second data";

        let blocks: Vec<(&Cid, &[u8])> = vec![(&cid, data1.as_slice()), (&cid, data2.as_slice())];
        blockstore.put_many(&blocks).await.unwrap();

        let retrieved = blockstore.get(&cid).await.unwrap();
        assert_eq!(retrieved, Some(Bytes::copy_from_slice(data1)));

        let cids = blockstore.all_cids().await.unwrap();
        assert_eq!(cids.len(), 1);
    }

    #[tokio::test]
    async fn delete_nonexistent_is_noop() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        let cid = test_cid();
        let result = blockstore.delete(&cid).await;
        assert!(result.is_ok());
        assert!(!blockstore.has(&cid).await.unwrap());
    }

    #[tokio::test]
    async fn delete_nonexistent_p2p() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        let cid = test_cid();
        let result = blockstore.delete(&cid).await;
        assert!(result.is_ok());

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert!(!unmerged.contains(&cid));
    }

    #[tokio::test]
    async fn key_format_compatibility() {
        use storage::keys::blockstore::{BlockstoreKey, ToMergeIndexKey, MERGE_PREFIX};

        let cid = test_cid();

        let block_key = BlockstoreKey::new(cid);
        let block_bytes = block_key.bytes();
        assert_eq!(
            block_bytes,
            cid.to_bytes(),
            "Block key should be raw CID bytes"
        );

        let merge_key = ToMergeIndexKey::new(cid);
        let merge_bytes = merge_key.bytes();

        assert_eq!(merge_bytes[0], MERGE_PREFIX);
        assert_eq!(merge_bytes[0], b'm');
        assert_eq!(merge_bytes[0], 0x6D);
        assert_eq!(&merge_bytes[1..], cid.to_bytes().as_slice());
        assert_eq!(merge_bytes.len(), 1 + cid.to_bytes().len());

        let cidv0 = Cid::from_str("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n").unwrap();
        let merge_key_v0 = ToMergeIndexKey::new(cidv0);
        let merge_bytes_v0 = merge_key_v0.bytes();
        assert_eq!(merge_bytes_v0[0], b'm');
        assert_eq!(&merge_bytes_v0[1..], cidv0.to_bytes().as_slice());
    }
}

mod concurrency {
    use super::*;

    #[tokio::test]
    async fn put_different_cids() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, false));

        let cid1 = test_cid();
        let cid2 = test_cid2();
        let cid3 = test_cid3();
        let data1 = b"concurrent data 1";
        let data2 = b"concurrent data 2";
        let data3 = b"concurrent data 3";

        let bs1 = blockstore.clone();
        let bs2 = blockstore.clone();
        let bs3 = blockstore.clone();

        let (r1, r2, r3) = tokio::join!(
            async move { bs1.put(&cid1, data1).await },
            async move { bs2.put(&cid2, data2).await },
            async move { bs3.put(&cid3, data3).await }
        );

        r1.unwrap();
        r2.unwrap();
        r3.unwrap();

        assert_eq!(
            blockstore.get(&cid1).await.unwrap(),
            Some(Bytes::copy_from_slice(data1))
        );
        assert_eq!(
            blockstore.get(&cid2).await.unwrap(),
            Some(Bytes::copy_from_slice(data2))
        );
        assert_eq!(
            blockstore.get(&cid3).await.unwrap(),
            Some(Bytes::copy_from_slice(data3))
        );
    }

    #[tokio::test]
    async fn put_same_cid() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, false));

        let cid = test_cid();
        let data1 = b"first writer";
        let data2 = b"second writer";

        let bs1 = blockstore.clone();
        let bs2 = blockstore.clone();

        let (r1, r2) = tokio::join!(async move { bs1.put(&cid, data1).await }, async move {
            bs2.put(&cid, data2).await
        });

        r1.unwrap();
        r2.unwrap();

        let retrieved = blockstore.get(&cid).await.unwrap();
        assert!(
            retrieved == Some(Bytes::copy_from_slice(data1))
                || retrieved == Some(Bytes::copy_from_slice(data2))
        );

        let cids = blockstore.all_cids().await.unwrap();
        assert_eq!(cids.len(), 1);
    }

    #[tokio::test]
    async fn get_and_put() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, false));

        let cid = test_cid();
        let data = b"concurrent access data";

        blockstore.put(&cid, data).await.unwrap();

        let bs1 = blockstore.clone();
        let bs2 = blockstore.clone();
        let bs3 = blockstore.clone();

        let cid2 = test_cid2();
        let (r1, r2, r3) = tokio::join!(
            async move { bs1.get(&cid).await },
            async move { bs2.get(&cid).await },
            async move { bs3.put(&cid2, b"other data").await }
        );

        assert_eq!(r1.unwrap(), Some(Bytes::copy_from_slice(data)));
        assert_eq!(r2.unwrap(), Some(Bytes::copy_from_slice(data)));
        r3.unwrap();
    }

    #[tokio::test]
    async fn hash_on_read_toggle() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, false));

        let data = b"hash verification data";
        let cid = cid_from_data(data);
        blockstore.put(&cid, data).await.unwrap();

        let bs1 = blockstore.clone();
        let bs2 = blockstore.clone();
        let bs3 = blockstore.clone();

        let (_, _, r3) = tokio::join!(
            async move {
                bs1.hash_on_read(true);
            },
            async move {
                bs2.hash_on_read(false);
            },
            async move { bs3.get(&cid).await }
        );

        assert_eq!(r3.unwrap(), Some(Bytes::copy_from_slice(data)));
    }

    #[tokio::test]
    async fn p2p_merge_tracking() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, true));

        let cid1 = test_cid();
        let cid2 = test_cid2();

        blockstore.put(&cid1, b"block1").await.unwrap();
        blockstore.put(&cid2, b"block2").await.unwrap();

        let bs1 = blockstore.clone();
        let bs2 = blockstore.clone();

        let (r1, r2) = tokio::join!(async move { bs1.mark_as_merged(&cid1).await }, async move {
            bs2.mark_as_merged(&cid2).await
        });

        r1.unwrap();
        r2.unwrap();

        assert!(blockstore.is_merged(&cid1).await.unwrap());
        assert!(blockstore.is_merged(&cid2).await.unwrap());

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert!(unmerged.is_empty());
    }

    #[tokio::test]
    async fn delete_during_read() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, false));

        let cid = test_cid();
        let data = b"data that may be deleted";

        blockstore.put(&cid, data).await.unwrap();

        for _ in 0..10 {
            if !blockstore.has(&cid).await.unwrap() {
                blockstore.put(&cid, data).await.unwrap();
            }

            let bs_read = blockstore.clone();
            let bs_delete = blockstore.clone();

            let (read_result, delete_result) =
                tokio::join!(async move { bs_read.get(&cid).await }, async move {
                    bs_delete.delete(&cid).await
                });

            assert!(delete_result.is_ok());

            let read_value = read_result.unwrap();
            assert!(
                read_value.is_none() || read_value == Some(Bytes::copy_from_slice(data)),
                "Read during delete should return None or valid data, got {:?}",
                read_value
            );
        }
    }

    #[tokio::test]
    async fn delete_and_has() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, false));

        let cid = test_cid();
        blockstore.put(&cid, b"data").await.unwrap();

        for _ in 0..10 {
            if !blockstore.has(&cid).await.unwrap() {
                blockstore.put(&cid, b"data").await.unwrap();
            }

            let bs_has = blockstore.clone();
            let bs_delete = blockstore.clone();

            let (has_result, delete_result) =
                tokio::join!(async move { bs_has.has(&cid).await }, async move {
                    bs_delete.delete(&cid).await
                });

            assert!(delete_result.is_ok());
            assert!(has_result.is_ok());
        }
    }
}

mod stress {
    use super::*;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn many_blocks() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        const NUM_BLOCKS: usize = 500;
        let mut cids = Vec::with_capacity(NUM_BLOCKS);

        for i in 0..NUM_BLOCKS {
            let data = format!("block data {}", i);
            let cid = cid_from_data(data.as_bytes());
            blockstore.put(&cid, data.as_bytes()).await.unwrap();
            cids.push(cid);
        }

        let all = blockstore.all_cids().await.unwrap();
        assert_eq!(
            all.len(),
            NUM_BLOCKS,
            "all_cids should return all {} blocks",
            NUM_BLOCKS
        );

        for cid in &cids {
            assert!(all.contains(cid), "Missing CID: {}", cid);
        }

        for (i, cid) in cids.iter().enumerate().step_by(50) {
            let expected = format!("block data {}", i);
            let data = blockstore.get(cid).await.unwrap();
            assert_eq!(data, Some(Bytes::from(expected.into_bytes())));
        }

        for cid in &cids {
            assert!(blockstore.has(cid).await.unwrap());
        }
    }

    #[tokio::test]
    async fn many_blocks_p2p_merge() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, true);

        const NUM_BLOCKS: usize = 200;
        let mut cids = Vec::with_capacity(NUM_BLOCKS);

        for i in 0..NUM_BLOCKS {
            let data = format!("p2p block {}", i);
            let cid = cid_from_data(data.as_bytes());
            blockstore.put(&cid, data.as_bytes()).await.unwrap();
            cids.push(cid);
        }

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert_eq!(unmerged.len(), NUM_BLOCKS);

        for cid in cids.iter().take(NUM_BLOCKS / 2) {
            blockstore.mark_as_merged(cid).await.unwrap();
        }

        let unmerged = blockstore.get_unmerged().await.unwrap();
        assert_eq!(
            unmerged.len(),
            NUM_BLOCKS / 2,
            "Half should still be unmerged"
        );

        for (i, cid) in cids.iter().enumerate() {
            let is_merged = blockstore.is_merged(cid).await.unwrap();
            if i < NUM_BLOCKS / 2 {
                assert!(is_merged, "Block {} should be merged", i);
            } else {
                assert!(!is_merged, "Block {} should be unmerged", i);
            }
        }

        let all = blockstore.all_cids().await.unwrap();
        assert_eq!(all.len(), NUM_BLOCKS);
    }

    #[tokio::test]
    async fn put_many_batch() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        const BATCH_SIZE: usize = 100;

        let blocks: Vec<(Cid, Vec<u8>)> = (0..BATCH_SIZE)
            .map(|i| {
                let data = format!("batch block {}", i).into_bytes();
                let cid = cid_from_data(&data);
                (cid, data)
            })
            .collect();

        let block_refs: Vec<(&Cid, &[u8])> =
            blocks.iter().map(|(c, d)| (c, d.as_slice())).collect();

        blockstore.put_many(&block_refs).await.unwrap();

        for (cid, expected_data) in &blocks {
            let data = blockstore.get(cid).await.unwrap();
            assert_eq!(data, Some(Bytes::from(expected_data.clone())));
        }

        let all = blockstore.all_cids().await.unwrap();
        assert_eq!(all.len(), BATCH_SIZE);
    }

    #[tokio::test]
    async fn concurrent_operations() {
        use std::sync::atomic::AtomicUsize;

        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, false));

        let mut cids = Vec::new();
        for i in 0..50 {
            let data = format!("preload {}", i);
            let cid = cid_from_data(data.as_bytes());
            blockstore.put(&cid, data.as_bytes()).await.unwrap();
            cids.push(cid);
        }

        let success_count = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for &cid in &cids {
            let bs = blockstore.clone();
            let counter = success_count.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..10 {
                    if bs.get(&cid).await.is_ok() {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for i in 0..20 {
            let bs = blockstore.clone();
            let counter = success_count.clone();
            handles.push(tokio::spawn(async move {
                let data = format!("concurrent write {}", i);
                let cid = cid_from_data(data.as_bytes());
                if bs.put(&cid, data.as_bytes()).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let total_ops = success_count.load(Ordering::Relaxed);
        assert!(
            total_ops >= 500,
            "Expected at least 500 successful ops, got {}",
            total_ops
        );
    }
}

mod error_paths {
    use super::*;

    #[tokio::test]
    async fn all_cids_skips_malformed_keys() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store.clone(), false);

        let cid1 = test_cid();
        let cid2 = test_cid2();
        blockstore.put(&cid1, b"data1").await.unwrap();
        blockstore.put(&cid2, b"data2").await.unwrap();

        let malformed_key = vec![0xAA, 0xBB, 0xCC, 0xDD];
        {
            let mut txn = store.new_txn(false).await.unwrap();
            let mut namespaced_key = vec![b'b'];
            namespaced_key.extend_from_slice(&malformed_key);
            txn.set(&namespaced_key, b"garbage").await.unwrap();
            txn.commit().await.unwrap();
        }

        let cids = blockstore.all_cids().await.unwrap();
        assert_eq!(
            cids.len(),
            2,
            "Should return only valid CIDs, skipping malformed key"
        );
        assert!(cids.contains(&cid1));
        assert!(cids.contains(&cid2));
    }

    #[tokio::test]
    async fn cid_bytes_cannot_start_with_merge_prefix() {
        use storage::keys::blockstore::ToMergeIndexKey;

        let cidv1 = test_cid();
        let cidv1_bytes = cidv1.to_bytes();
        assert_ne!(
            cidv1_bytes[0], b'm',
            "CIDv1 should not start with 'm' - would break is_merge_key filtering"
        );
        assert_eq!(
            cidv1_bytes[0], 0x01,
            "CIDv1 should start with version byte 0x01"
        );
        assert!(!ToMergeIndexKey::is_merge_key(&cidv1_bytes));

        let cidv0 = Cid::from_str("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n").unwrap();
        let cidv0_bytes = cidv0.to_bytes();
        assert_ne!(
            cidv0_bytes[0], b'm',
            "CIDv0 should not start with 'm' - would break is_merge_key filtering"
        );
        assert_eq!(
            cidv0_bytes[0], 0x12,
            "CIDv0 should start with sha2-256 code 0x12"
        );
        assert!(!ToMergeIndexKey::is_merge_key(&cidv0_bytes));

        let raw_cid = cid_from_data(b"test data");
        let raw_bytes = raw_cid.to_bytes();
        assert_eq!(raw_bytes[0], 0x01, "Raw CIDv1 should start with 0x01");
        assert!(!ToMergeIndexKey::is_merge_key(&raw_bytes));
    }
}
