//! Tests for Blockstore - IPLD blocks and merkle tree nodes
//!
//! These tests verify block storage, merge tracking for P2P operations,
//! and data corruption detection.

use bytes::Bytes;
use cid::Cid;
use std::str::FromStr;
use std::sync::Arc;
use storage::corekv::{Store, Writer};
use storage::keys::blockstore::{MERGE_PREFIX, OBJECT_MARKER};
use storage::namespace::Namespace;
use storage::stores::blockstore::{Blockstore, BlockstoreTxn};
use storage::RegolithStore;

fn test_cid() -> Cid {
    Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
}

fn test_cid2() -> Cid {
    Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap()
}

#[tokio::test]
async fn test_blockstore_put_get() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Blockstore::new(store, false);

    let cid = test_cid();
    let data = b"block data here";

    // Put block
    let mut txn = blockstore.new_txn(false).await.unwrap();
    {
        let txn_bs = txn.as_any_mut().downcast_mut::<BlockstoreTxn>().unwrap();
        txn_bs.put_block(&cid, data).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Get block
    let txn = blockstore.new_txn(true).await.unwrap();
    let txn_bs = txn.as_any().downcast_ref::<BlockstoreTxn>().unwrap();
    let retrieved = txn_bs.get_block(&cid).await.unwrap();
    assert_eq!(retrieved, Some(Bytes::from(data.to_vec())));
}

#[tokio::test]
async fn test_blockstore_merge_tracking() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Blockstore::new(store, true); // P2P mode

    let cid = test_cid();
    let data = b"block data";

    // Put block (should be marked as unmerged in P2P mode)
    let mut txn = blockstore.new_txn(false).await.unwrap();
    {
        let txn_bs = txn.as_any_mut().downcast_mut::<BlockstoreTxn>().unwrap();
        txn_bs.put_block(&cid, data).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Check if merged (should be false)
    let txn = blockstore.new_txn(true).await.unwrap();
    let txn_bs = txn.as_any().downcast_ref::<BlockstoreTxn>().unwrap();
    let is_merged = txn_bs.is_merged(&cid).await.unwrap();
    assert!(!is_merged);
    drop(txn);

    // Mark as merged
    let mut txn = blockstore.new_txn(false).await.unwrap();
    {
        let txn_bs = txn.as_any_mut().downcast_mut::<BlockstoreTxn>().unwrap();
        txn_bs.mark_as_merged(&cid).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Check again (should be true now)
    let txn = blockstore.new_txn(true).await.unwrap();
    let txn_bs = txn.as_any().downcast_ref::<BlockstoreTxn>().unwrap();
    let is_merged = txn_bs.is_merged(&cid).await.unwrap();
    assert!(is_merged);
}

#[tokio::test]
async fn test_blockstore_get_unmerged() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Blockstore::new(store, true); // P2P mode

    let cid1 = test_cid();
    let cid2 = test_cid2();

    // Put two blocks
    let mut txn = blockstore.new_txn(false).await.unwrap();
    {
        let txn_bs = txn.as_any_mut().downcast_mut::<BlockstoreTxn>().unwrap();
        txn_bs.put_block(&cid1, b"data1").await.unwrap();
        txn_bs.put_block(&cid2, b"data2").await.unwrap();
    }
    txn.commit().await.unwrap();

    // Get unmerged CIDs
    let txn = blockstore.new_txn(true).await.unwrap();
    let txn_bs = txn.as_any().downcast_ref::<BlockstoreTxn>().unwrap();
    let unmerged = txn_bs.get_unmerged_cids().await.unwrap();
    assert_eq!(unmerged.len(), 2);
    assert!(unmerged.contains(&cid1));
    assert!(unmerged.contains(&cid2));
    drop(txn);

    // Mark one as merged
    let mut txn = blockstore.new_txn(false).await.unwrap();
    {
        let txn_bs = txn.as_any_mut().downcast_mut::<BlockstoreTxn>().unwrap();
        txn_bs.mark_as_merged(&cid1).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Get unmerged CIDs again
    let txn = blockstore.new_txn(true).await.unwrap();
    let txn_bs = txn.as_any().downcast_ref::<BlockstoreTxn>().unwrap();
    let unmerged = txn_bs.get_unmerged_cids().await.unwrap();
    assert_eq!(unmerged.len(), 1);
    assert!(unmerged.contains(&cid2));
}

#[tokio::test]
async fn test_blockstore_non_p2p_no_tracking() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Blockstore::new(store, false); // Non-P2P mode

    let cid = test_cid();

    // Put block
    let mut txn = blockstore.new_txn(false).await.unwrap();
    {
        let txn_bs = txn.as_any_mut().downcast_mut::<BlockstoreTxn>().unwrap();
        txn_bs.put_block(&cid, b"data").await.unwrap();
    }
    txn.commit().await.unwrap();

    // Should be immediately "merged" (no tracking in non-P2P mode)
    let txn = blockstore.new_txn(true).await.unwrap();
    let txn_bs = txn.as_any().downcast_ref::<BlockstoreTxn>().unwrap();
    let is_merged = txn_bs.is_merged(&cid).await.unwrap();
    assert!(is_merged);
}

#[tokio::test]
async fn test_get_unmerged_cids_detects_corruption() {
    // get_unmerged_cids should return an error if it encounters merge keys
    // that cannot be parsed. This indicates data corruption and the caller
    // should be aware that results are incomplete.
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Blockstore::new(store.clone(), true); // P2P mode

    // Add a valid block first
    let cid = test_cid();
    let mut txn = blockstore.new_txn(false).await.unwrap();
    {
        let txn_bs = txn.as_any_mut().downcast_mut::<BlockstoreTxn>().unwrap();
        txn_bs.put_block(&cid, b"valid block").await.unwrap();
    }
    txn.commit().await.unwrap();

    // Now inject a corrupted merge key directly into the underlying store.
    // The key has the correct 'm' prefix but invalid CID bytes after.
    // This simulates storage corruption.
    {
        let mut txn = store.new_txn(false).await.unwrap();
        // Build the corrupted key: namespace prefix ('b') + 'm' + garbage
        let mut corrupted_merge_key = vec![Namespace::Blockstore.prefix()]; // 'b'
        corrupted_merge_key.push(MERGE_PREFIX); // 'm'
        corrupted_merge_key.extend_from_slice(&[0xFF, 0xFF, 0xFF]); // Invalid CID bytes
        txn.set(&corrupted_merge_key, &[OBJECT_MARKER])
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    // get_unmerged_cids should detect the corruption and return an error
    let txn = blockstore.new_txn(true).await.unwrap();
    let txn_bs = txn.as_any().downcast_ref::<BlockstoreTxn>().unwrap();
    let result = txn_bs.get_unmerged_cids().await;

    assert!(
        result.is_err(),
        "Should return error on corrupted merge key"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Data corruption detected") || err_msg.contains("could not be parsed"),
        "Error should indicate data corruption: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_delete_block_cleans_up_merge_marker() {
    // Verify delete_block removes both the block and its merge marker

    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let blockstore = Blockstore::new(store, true); // P2P mode

    let cid = test_cid();

    // Put block (creates merge marker)
    let mut txn = blockstore.new_txn(false).await.unwrap();
    {
        let txn_bs = txn.as_any_mut().downcast_mut::<BlockstoreTxn>().unwrap();
        txn_bs.put_block(&cid, b"data").await.unwrap();
    }
    txn.commit().await.unwrap();

    // Verify block and merge marker exist
    let txn = blockstore.new_txn(true).await.unwrap();
    let txn_bs = txn.as_any().downcast_ref::<BlockstoreTxn>().unwrap();
    assert!(txn_bs.has_block(&cid).await.unwrap());
    assert!(!txn_bs.is_merged(&cid).await.unwrap()); // has marker = not merged
    drop(txn);

    // Delete block
    let mut txn = blockstore.new_txn(false).await.unwrap();
    {
        let txn_bs = txn.as_any_mut().downcast_mut::<BlockstoreTxn>().unwrap();
        txn_bs.delete_block(&cid).await.unwrap();
    }
    txn.commit().await.unwrap();

    // Both block and merge marker should be gone
    let txn = blockstore.new_txn(true).await.unwrap();
    let txn_bs = txn.as_any().downcast_ref::<BlockstoreTxn>().unwrap();
    assert!(!txn_bs.has_block(&cid).await.unwrap());
    // is_merged returns true when marker doesn't exist, but block also doesn't exist
    // The important thing is the CID doesn't appear in unmerged list
    let unmerged = txn_bs.get_unmerged_cids().await.unwrap();
    assert!(
        !unmerged.contains(&cid),
        "Deleted block should not be in unmerged list"
    );
}
