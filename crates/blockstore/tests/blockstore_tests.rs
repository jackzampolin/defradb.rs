//! Integration tests for the blockstore crate
//!
//! Tests for DefraBlockstore implementation including:
//! - Basic CRUD operations
//! - Hash verification (hash_on_read)
//! - Merge tracking (P2P mode)
//! - Go compatibility
//! - Concurrency
//! - Edge cases and stress tests

use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use blockstore::{Blockstore, DefraBlockstore, Error};
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
    // Another valid CIDv1
    Cid::from_str("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku").unwrap()
}

/// Create a CID from data using SHA2-256 (for hash verification tests)
fn cid_from_data(data: &[u8]) -> Cid {
    use multihash::MultihashGeneric;
    use sha2::{Digest, Sha256};

    // Compute SHA2-256 hash
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();

    // Create multihash with SHA2-256 code (0x12)
    let hash = MultihashGeneric::<64>::wrap(0x12, &digest).unwrap();
    Cid::new_v1(0x55, hash) // raw codec
}

// ==================== Basic CRUD Tests ====================

#[tokio::test]
async fn test_basic_put_get() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let cid = test_cid();
    let data = b"hello world";

    // Put
    blockstore.put(&cid, data).await.unwrap();

    // Get
    let retrieved = blockstore.get(&cid).await.unwrap();
    assert_eq!(retrieved, Some(data.to_vec()));
}

#[tokio::test]
async fn test_get_nonexistent_block() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let cid = test_cid();

    // Get non-existent block should return None, not error
    let result = blockstore.get(&cid).await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_has() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let cid = test_cid();
    let data = b"test data";

    // Should not exist initially
    assert!(!blockstore.has(&cid).await.unwrap());

    // Put
    blockstore.put(&cid, data).await.unwrap();

    // Should exist now
    assert!(blockstore.has(&cid).await.unwrap());
}

#[tokio::test]
async fn test_delete() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let cid = test_cid();
    let data = b"to be deleted";

    // Put
    blockstore.put(&cid, data).await.unwrap();
    assert!(blockstore.has(&cid).await.unwrap());

    // Delete
    blockstore.delete(&cid).await.unwrap();

    // Should not exist anymore
    assert!(!blockstore.has(&cid).await.unwrap());
    assert_eq!(blockstore.get(&cid).await.unwrap(), None);
}

#[tokio::test]
async fn test_get_size() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let cid = test_cid();
    let data = b"test data for size";

    // Should return None for non-existent block
    assert_eq!(blockstore.get_size(&cid).await.unwrap(), None);

    // Put
    blockstore.put(&cid, data).await.unwrap();

    // Should return size
    let size = blockstore.get_size(&cid).await.unwrap();
    assert_eq!(size, Some(data.len()));
}

#[tokio::test]
async fn test_put_many() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let cid1 = test_cid();
    let cid2 = test_cid2();
    let data1 = b"block one";
    let data2 = b"block two";

    // Put many
    let blocks: Vec<(&Cid, &[u8])> = vec![(&cid1, data1.as_slice()), (&cid2, data2.as_slice())];
    blockstore.put_many(&blocks).await.unwrap();

    // Verify both exist
    assert_eq!(blockstore.get(&cid1).await.unwrap(), Some(data1.to_vec()));
    assert_eq!(blockstore.get(&cid2).await.unwrap(), Some(data2.to_vec()));
}

#[tokio::test]
async fn test_put_many_empty() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    // Should handle empty input
    let blocks: Vec<(&Cid, &[u8])> = vec![];
    blockstore.put_many(&blocks).await.unwrap();
}

#[tokio::test]
async fn test_all_cids() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let cid1 = test_cid();
    let cid2 = test_cid2();
    let cid3 = test_cid3();

    // Initially empty
    let cids = blockstore.all_cids().await.unwrap();
    assert!(cids.is_empty());

    // Put some blocks
    blockstore.put(&cid1, b"data1").await.unwrap();
    blockstore.put(&cid2, b"data2").await.unwrap();
    blockstore.put(&cid3, b"data3").await.unwrap();

    // Should list all CIDs
    let cids = blockstore.all_cids().await.unwrap();
    assert_eq!(cids.len(), 3);
    assert!(cids.contains(&cid1));
    assert!(cids.contains(&cid2));
    assert!(cids.contains(&cid3));
}

#[tokio::test]
async fn test_deduplication() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let cid = test_cid();
    let data = b"original data";

    // Put twice with same CID
    blockstore.put(&cid, data).await.unwrap();
    blockstore.put(&cid, data).await.unwrap();

    // Should only appear once in all_cids
    let cids = blockstore.all_cids().await.unwrap();
    assert_eq!(cids.len(), 1);
    assert_eq!(cids[0], cid);
}

// ==================== HashOnRead Tests ====================

#[tokio::test]
async fn test_hash_on_read_disabled_by_default() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    // Hash on read should be disabled by default
    assert!(!blockstore.rehash_enabled());
}

#[tokio::test]
async fn test_hash_on_read_enable_disable() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    // Enable
    blockstore.hash_on_read(true);
    assert!(blockstore.rehash_enabled());

    // Disable
    blockstore.hash_on_read(false);
    assert!(!blockstore.rehash_enabled());
}

#[tokio::test]
async fn test_hash_on_read_valid_data() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let data = b"test data for hash verification";
    let cid = cid_from_data(data);

    // Put the block
    blockstore.put(&cid, data).await.unwrap();

    // Enable hash on read
    blockstore.hash_on_read(true);

    // Get should succeed with valid data
    let result = blockstore.get(&cid).await.unwrap();
    assert_eq!(result, Some(data.to_vec()));
}

#[tokio::test]
async fn test_hash_on_read_corrupted_data() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let original_data = b"original data";
    let corrupted_data = b"corrupted data";
    let cid = cid_from_data(original_data);

    // Directly write corrupted data to the store using the CID
    // This simulates data corruption
    {
        let mut txn = blockstore.new_store_txn(false).await.unwrap();
        let bs_txn = txn.as_any_mut().downcast_mut::<BlockstoreTxn>().unwrap();
        bs_txn.put_block(&cid, corrupted_data).await.unwrap();
        txn.commit().await.unwrap();
    }

    // Without hash_on_read, we get the corrupted data
    blockstore.hash_on_read(false);
    let result = blockstore.get(&cid).await.unwrap();
    assert_eq!(result, Some(corrupted_data.to_vec()));

    // With hash_on_read enabled, it should detect the mismatch
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
async fn test_hash_on_read_nonexistent_block() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let cid = test_cid();

    // Enable hash on read
    blockstore.hash_on_read(true);

    // Get non-existent block should return None, not error
    // (no hash to verify when block doesn't exist)
    let result = blockstore.get(&cid).await.unwrap();
    assert_eq!(result, None);
}

// ==================== Merge Tracking Tests (P2P mode) ====================

#[tokio::test]
async fn test_p2p_merge_tracking_lifecycle() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, true); // P2P mode

    let cid = test_cid();
    let data = b"p2p block";

    // Put block - should be unmerged initially
    blockstore.put(&cid, data).await.unwrap();

    // Check unmerged status
    assert!(!blockstore.is_merged(&cid).await.unwrap());

    // Get unmerged - should include this CID
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert!(unmerged.contains(&cid));

    // Mark as merged
    blockstore.mark_as_merged(&cid).await.unwrap();

    // Now should be merged
    assert!(blockstore.is_merged(&cid).await.unwrap());

    // Should not appear in unmerged list
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert!(!unmerged.contains(&cid));
}

#[tokio::test]
async fn test_local_mode_no_merge_tracking() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false); // Local mode

    let cid = test_cid();
    let data = b"local block";

    // Put block
    blockstore.put(&cid, data).await.unwrap();

    // Should be immediately "merged" in local mode
    assert!(blockstore.is_merged(&cid).await.unwrap());

    // Unmerged list should be empty
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert!(unmerged.is_empty());
}

#[tokio::test]
async fn test_is_merged_nonexistent_block() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, true); // P2P mode

    let cid = test_cid();

    // is_merged for non-existent block should return false (matches Go behavior)
    assert!(!blockstore.is_merged(&cid).await.unwrap());
}

#[tokio::test]
async fn test_is_merged_nonexistent_block_local_mode() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false); // Local mode

    let cid = test_cid();

    // is_merged for non-existent block should return false even in local mode
    assert!(!blockstore.is_merged(&cid).await.unwrap());
}

#[tokio::test]
async fn test_get_unmerged_filtering() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, true); // P2P mode

    let cid1 = test_cid();
    let cid2 = test_cid2();

    // Put two blocks
    blockstore.put(&cid1, b"data1").await.unwrap();
    blockstore.put(&cid2, b"data2").await.unwrap();

    // Both unmerged
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert_eq!(unmerged.len(), 2);

    // Merge one
    blockstore.mark_as_merged(&cid1).await.unwrap();

    // Only one should be unmerged now
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert_eq!(unmerged.len(), 1);
    assert!(unmerged.contains(&cid2));
    assert!(!unmerged.contains(&cid1));
}

#[tokio::test]
async fn test_all_cids_excludes_merge_markers() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, true); // P2P mode

    let cid1 = test_cid();
    let cid2 = test_cid2();

    // Put blocks (creates merge markers in P2P mode)
    blockstore.put(&cid1, b"data1").await.unwrap();
    blockstore.put(&cid2, b"data2").await.unwrap();

    // all_cids should only return actual block CIDs, not merge markers
    let cids = blockstore.all_cids().await.unwrap();
    assert_eq!(cids.len(), 2);
    assert!(cids.contains(&cid1));
    assert!(cids.contains(&cid2));
}

#[tokio::test]
async fn test_delete_removes_merge_marker() {
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, true); // P2P mode

    let cid = test_cid();

    // Put block
    blockstore.put(&cid, b"data").await.unwrap();

    // Verify unmerged
    assert!(!blockstore.is_merged(&cid).await.unwrap());

    // Delete block
    blockstore.delete(&cid).await.unwrap();

    // Block should be gone
    assert!(!blockstore.has(&cid).await.unwrap());

    // is_merged returns false for non-existent block
    assert!(!blockstore.is_merged(&cid).await.unwrap());

    // Not in unmerged list
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert!(!unmerged.contains(&cid));
}

// ==================== Go Compatibility Tests ====================

#[tokio::test]
async fn test_get_with_default_cid_returns_none() {
    // Go implementation checks `!k.Defined()` and returns ErrNotFound
    // Rust CID library doesn't have a "defined" concept the same way,
    // but we should handle edge cases gracefully
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    // CIDv0 with all zeros - this is technically parseable but represents
    // an invalid/empty multihash
    let zero_bytes = vec![0x12, 0x20]; // SHA2-256 code + 32 byte length, but no digest
    let result = Cid::try_from(zero_bytes.as_slice());
    // This should fail to parse (incomplete multihash)
    assert!(result.is_err());

    // A properly formed but "empty content" CID (hash of empty data)
    // This is valid and should return None when not stored
    let empty_data_cid = cid_from_data(b"");
    let result = blockstore.get(&empty_data_cid).await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_operations_with_cidv0() {
    // CIDv0 is the legacy format used by IPFS - ensure compatibility
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    // CIDv0 example (Qm... format, base58btc encoded)
    let cidv0 = Cid::from_str("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n").unwrap();
    let data = b"cidv0 test data";

    // All operations should work with CIDv0
    blockstore.put(&cidv0, data).await.unwrap();
    assert!(blockstore.has(&cidv0).await.unwrap());
    assert_eq!(blockstore.get(&cidv0).await.unwrap(), Some(data.to_vec()));
    assert_eq!(blockstore.get_size(&cidv0).await.unwrap(), Some(data.len()));

    let cids = blockstore.all_cids().await.unwrap();
    assert!(cids.contains(&cidv0));

    blockstore.delete(&cidv0).await.unwrap();
    assert!(!blockstore.has(&cidv0).await.unwrap());
}

#[tokio::test]
async fn test_put_already_merged_block_stays_merged() {
    // Critical Go compatibility test:
    // When a block is put, merged, then put again, it should stay merged
    // Go skips put entirely if block exists, so no new merge marker is created
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, true); // P2P mode

    let cid = test_cid();
    let data = b"original data";

    // Step 1: Put block (creates merge marker)
    blockstore.put(&cid, data).await.unwrap();
    assert!(!blockstore.is_merged(&cid).await.unwrap());

    // Step 2: Mark as merged (removes marker)
    blockstore.mark_as_merged(&cid).await.unwrap();
    assert!(blockstore.is_merged(&cid).await.unwrap());

    // Step 3: Put same block again
    blockstore.put(&cid, data).await.unwrap();

    // Step 4: Should STILL be merged (put was no-op because block existed)
    assert!(
        blockstore.is_merged(&cid).await.unwrap(),
        "Re-putting an already-merged block should not create a new merge marker"
    );

    // Unmerged list should be empty
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert!(
        !unmerged.contains(&cid),
        "Re-put block should not appear in unmerged list"
    );
}

#[tokio::test]
async fn test_put_many_with_existing_merged_block() {
    // Same test but for put_many - existing merged blocks should stay merged
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, true); // P2P mode

    let cid1 = test_cid();
    let cid2 = test_cid2();
    let data1 = b"data one";
    let data2 = b"data two";

    // Put and merge cid1
    blockstore.put(&cid1, data1).await.unwrap();
    blockstore.mark_as_merged(&cid1).await.unwrap();
    assert!(blockstore.is_merged(&cid1).await.unwrap());

    // Now put_many with cid1 (existing merged) and cid2 (new)
    let blocks: Vec<(&Cid, &[u8])> = vec![(&cid1, data1.as_slice()), (&cid2, data2.as_slice())];
    blockstore.put_many(&blocks).await.unwrap();

    // cid1 should still be merged
    assert!(
        blockstore.is_merged(&cid1).await.unwrap(),
        "Existing merged block should stay merged after put_many"
    );

    // cid2 should be unmerged (newly added)
    assert!(!blockstore.is_merged(&cid2).await.unwrap());

    // Only cid2 should be in unmerged list
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert!(!unmerged.contains(&cid1));
    assert!(unmerged.contains(&cid2));
}

#[tokio::test]
async fn test_put_same_cid_different_data_no_overwrite() {
    // Critical: Content-addressed stores MUST be immutable by CID
    // If put() overwrote data, it would violate content-addressing invariants
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let cid = test_cid();
    let original_data = b"original data";
    let new_data = b"different data";

    // Put original
    blockstore.put(&cid, original_data).await.unwrap();

    // Put with same CID but different data (should be ignored per Go behavior)
    blockstore.put(&cid, new_data).await.unwrap();

    // Should still have original data
    let retrieved = blockstore.get(&cid).await.unwrap();
    assert_eq!(retrieved, Some(original_data.to_vec()));
}

#[tokio::test]
async fn test_put_many_duplicate_cids_in_batch() {
    // Verify behavior when same CID appears multiple times in one batch
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let cid = test_cid();
    let data1 = b"first data";
    let data2 = b"second data";

    // Put same CID twice in one batch - first write should win
    let blocks: Vec<(&Cid, &[u8])> = vec![(&cid, data1.as_slice()), (&cid, data2.as_slice())];
    blockstore.put_many(&blocks).await.unwrap();

    // Should have first data (first write wins within transaction)
    let retrieved = blockstore.get(&cid).await.unwrap();
    assert_eq!(retrieved, Some(data1.to_vec()));

    // Should only appear once in all_cids
    let cids = blockstore.all_cids().await.unwrap();
    assert_eq!(cids.len(), 1);
}

#[tokio::test]
async fn test_mark_as_merged_nonexistent_block() {
    // Verify mark_as_merged behavior for non-existent blocks
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, true); // P2P mode

    let cid = test_cid();

    // Should not error when marking non-existent block as merged
    // This is a no-op - deleting a merge key that doesn't exist
    let result = blockstore.mark_as_merged(&cid).await;
    assert!(result.is_ok());

    // Block still doesn't exist
    assert!(!blockstore.has(&cid).await.unwrap());

    // is_merged still returns false (block doesn't exist)
    assert!(!blockstore.is_merged(&cid).await.unwrap());
}

// ==================== Edge Case Tests ====================

#[tokio::test]
async fn test_empty_block() {
    // Empty blocks are valid in IPLD (e.g., empty directory nodes)
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let data: &[u8] = b"";
    let cid = cid_from_data(data);

    // Put empty block
    blockstore.put(&cid, data).await.unwrap();

    // Verify retrieval
    let retrieved = blockstore.get(&cid).await.unwrap();
    assert_eq!(retrieved, Some(vec![]));

    // Verify size
    assert_eq!(blockstore.get_size(&cid).await.unwrap(), Some(0));

    // Verify has
    assert!(blockstore.has(&cid).await.unwrap());

    // Verify hash_on_read works with empty data
    blockstore.hash_on_read(true);
    let verified = blockstore.get(&cid).await.unwrap();
    assert_eq!(verified, Some(vec![]));
}

#[tokio::test]
async fn test_delete_nonexistent_block() {
    // Delete should be idempotent - deleting non-existent block is a no-op
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    let cid = test_cid();

    // Should succeed silently (idempotent)
    let result = blockstore.delete(&cid).await;
    assert!(result.is_ok());

    // Still doesn't exist
    assert!(!blockstore.has(&cid).await.unwrap());
}

#[tokio::test]
async fn test_delete_nonexistent_block_p2p_mode() {
    // Verify idempotent delete in P2P mode too
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, true); // P2P mode

    let cid = test_cid();

    // Should succeed silently
    let result = blockstore.delete(&cid).await;
    assert!(result.is_ok());

    // Not in unmerged list
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert!(!unmerged.contains(&cid));
}

#[tokio::test]
async fn test_hash_on_read_unsupported_algorithm_skipped() {
    // Verify unsupported hash algorithms are skipped with warning (not error)
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    // Create a CID with identity hash (code 0x00) - no actual hashing
    use multihash::MultihashGeneric;
    let data = b"identity hash data";
    let hash = MultihashGeneric::<64>::wrap(0x00, data).unwrap(); // Identity hash
    let cid = Cid::new_v1(0x55, hash); // raw codec

    // Store the block
    blockstore.put(&cid, data).await.unwrap();

    // Enable hash_on_read
    blockstore.hash_on_read(true);

    // Should succeed - unsupported algorithm is skipped, not errored
    let result = blockstore.get(&cid).await.unwrap();
    assert_eq!(result, Some(data.to_vec()));
}

#[tokio::test]
async fn test_hash_on_read_blake2b_skipped() {
    // Verify blake2b-256 (code 0xb220) is skipped
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    use multihash::MultihashGeneric;
    let data = b"blake2b test data";
    // Create a fake blake2b-256 multihash (just for testing skip behavior)
    let fake_digest = [0u8; 32];
    let hash = MultihashGeneric::<64>::wrap(0xb220, &fake_digest).unwrap();
    let cid = Cid::new_v1(0x55, hash);

    // Store with this CID
    blockstore.put(&cid, data).await.unwrap();

    // Enable hash_on_read
    blockstore.hash_on_read(true);

    // Should succeed - blake2b is skipped
    let result = blockstore.get(&cid).await.unwrap();
    assert_eq!(result, Some(data.to_vec()));
}

#[tokio::test]
async fn test_large_block() {
    // Test with 256KB block (typical IPFS chunk size)
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    // Generate 256KB of data
    let data: Vec<u8> = (0..262144).map(|i| (i % 256) as u8).collect();
    let cid = cid_from_data(&data);

    // Put large block
    blockstore.put(&cid, &data).await.unwrap();

    // Verify retrieval
    let retrieved = blockstore.get(&cid).await.unwrap();
    assert_eq!(retrieved, Some(data.clone()));

    // Verify size
    assert_eq!(blockstore.get_size(&cid).await.unwrap(), Some(262144));

    // Verify hash_on_read works with large data
    blockstore.hash_on_read(true);
    let verified = blockstore.get(&cid).await.unwrap();
    assert_eq!(verified, Some(data));
}

#[tokio::test]
async fn test_large_block_many() {
    // Test put_many with multiple large blocks
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    // Generate three 64KB blocks
    let data1: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
    let data2: Vec<u8> = (0..65536).map(|i| ((i + 100) % 256) as u8).collect();
    let data3: Vec<u8> = (0..65536).map(|i| ((i + 200) % 256) as u8).collect();

    let cid1 = cid_from_data(&data1);
    let cid2 = cid_from_data(&data2);
    let cid3 = cid_from_data(&data3);

    // Put all at once
    let blocks: Vec<(&Cid, &[u8])> = vec![
        (&cid1, data1.as_slice()),
        (&cid2, data2.as_slice()),
        (&cid3, data3.as_slice()),
    ];
    blockstore.put_many(&blocks).await.unwrap();

    // Verify all exist
    assert_eq!(blockstore.get(&cid1).await.unwrap(), Some(data1));
    assert_eq!(blockstore.get(&cid2).await.unwrap(), Some(data2));
    assert_eq!(blockstore.get(&cid3).await.unwrap(), Some(data3));
}

// ==================== Concurrency Tests ====================

#[tokio::test]
async fn test_concurrent_put_different_cids() {
    // Verify concurrent puts to different CIDs work correctly
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

    // Concurrent puts
    let (r1, r2, r3) = tokio::join!(
        async move { bs1.put(&cid1, data1).await },
        async move { bs2.put(&cid2, data2).await },
        async move { bs3.put(&cid3, data3).await }
    );

    r1.unwrap();
    r2.unwrap();
    r3.unwrap();

    // Verify all exist with correct data
    assert_eq!(blockstore.get(&cid1).await.unwrap(), Some(data1.to_vec()));
    assert_eq!(blockstore.get(&cid2).await.unwrap(), Some(data2.to_vec()));
    assert_eq!(blockstore.get(&cid3).await.unwrap(), Some(data3.to_vec()));
}

#[tokio::test]
async fn test_concurrent_put_same_cid() {
    // Verify concurrent puts to same CID (both should succeed, first wins)
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, false));

    let cid = test_cid();
    let data1 = b"first writer";
    let data2 = b"second writer";

    let bs1 = blockstore.clone();
    let bs2 = blockstore.clone();

    // Concurrent puts to same CID
    let (r1, r2) = tokio::join!(async move { bs1.put(&cid, data1).await }, async move {
        bs2.put(&cid, data2).await
    });

    // Both should succeed (one writes, one is no-op)
    r1.unwrap();
    r2.unwrap();

    // Data should be consistent (whichever wrote first)
    let retrieved = blockstore.get(&cid).await.unwrap();
    assert!(retrieved == Some(data1.to_vec()) || retrieved == Some(data2.to_vec()));

    // Only one copy should exist
    let cids = blockstore.all_cids().await.unwrap();
    assert_eq!(cids.len(), 1);
}

#[tokio::test]
async fn test_concurrent_get_and_put() {
    // Verify concurrent get and put operations
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, false));

    let cid = test_cid();
    let data = b"concurrent access data";

    // First put the data
    blockstore.put(&cid, data).await.unwrap();

    let bs1 = blockstore.clone();
    let bs2 = blockstore.clone();
    let bs3 = blockstore.clone();

    // Concurrent reads and a write (to different CID)
    let cid2 = test_cid2();
    let (r1, r2, r3) = tokio::join!(
        async move { bs1.get(&cid).await },
        async move { bs2.get(&cid).await },
        async move { bs3.put(&cid2, b"other data").await }
    );

    // All should succeed
    assert_eq!(r1.unwrap(), Some(data.to_vec()));
    assert_eq!(r2.unwrap(), Some(data.to_vec()));
    r3.unwrap();
}

#[tokio::test]
async fn test_concurrent_hash_on_read_toggle() {
    // Verify hash_on_read toggle is thread-safe
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, false));

    let data = b"hash verification data";
    let cid = cid_from_data(data);
    blockstore.put(&cid, data).await.unwrap();

    let bs1 = blockstore.clone();
    let bs2 = blockstore.clone();
    let bs3 = blockstore.clone();

    // Concurrent toggle and reads
    let (_, _, r3) = tokio::join!(
        async move {
            bs1.hash_on_read(true);
        },
        async move {
            bs2.hash_on_read(false);
        },
        async move { bs3.get(&cid).await }
    );

    // Read should succeed regardless of toggle state
    assert_eq!(r3.unwrap(), Some(data.to_vec()));
}

#[tokio::test]
async fn test_concurrent_p2p_merge_tracking() {
    // Verify concurrent merge operations in P2P mode
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true)); // P2P mode

    let cid1 = test_cid();
    let cid2 = test_cid2();

    // Put blocks
    blockstore.put(&cid1, b"block1").await.unwrap();
    blockstore.put(&cid2, b"block2").await.unwrap();

    let bs1 = blockstore.clone();
    let bs2 = blockstore.clone();

    // Concurrent merge operations
    let (r1, r2) = tokio::join!(async move { bs1.mark_as_merged(&cid1).await }, async move {
        bs2.mark_as_merged(&cid2).await
    });

    r1.unwrap();
    r2.unwrap();

    // Both should be merged
    assert!(blockstore.is_merged(&cid1).await.unwrap());
    assert!(blockstore.is_merged(&cid2).await.unwrap());

    // Unmerged list should be empty
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert!(unmerged.is_empty());
}

#[tokio::test]
async fn test_concurrent_delete_during_read() {
    // Verify behavior when delete races with read
    // Either outcome is acceptable: read returns data OR read returns None
    // But there should be no errors or panics
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, false));

    let cid = test_cid();
    let data = b"data that may be deleted";

    // Put initial data
    blockstore.put(&cid, data).await.unwrap();

    // Run multiple iterations to increase chance of race
    for _ in 0..10 {
        // Re-add the block if it was deleted
        if !blockstore.has(&cid).await.unwrap() {
            blockstore.put(&cid, data).await.unwrap();
        }

        let bs_read = blockstore.clone();
        let bs_delete = blockstore.clone();

        // Race: concurrent read and delete
        let (read_result, delete_result) =
            tokio::join!(async move { bs_read.get(&cid).await }, async move {
                bs_delete.delete(&cid).await
            });

        // Delete should always succeed (idempotent)
        assert!(delete_result.is_ok());

        // Read should succeed (returning Some or None, but no error)
        let read_value = read_result.unwrap();
        // Either we got the data or it was already deleted - both are valid
        assert!(
            read_value.is_none() || read_value == Some(data.to_vec()),
            "Read during delete should return None or valid data, got {:?}",
            read_value
        );
    }
}

#[tokio::test]
async fn test_concurrent_delete_and_has() {
    // Verify has() behavior during concurrent delete
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

        // Both operations should succeed without error
        assert!(delete_result.is_ok());
        assert!(has_result.is_ok());
        // has() returns true or false depending on race timing - both valid
    }
}

// ==================== Merge Lifecycle Edge Cases ====================

#[tokio::test]
async fn test_is_merged_after_delete_of_merged_block() {
    // Test lifecycle: put -> merge -> delete -> is_merged
    // After deleting a merged block, is_merged should return false
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, true); // P2P mode

    let cid = test_cid();
    let data = b"block to merge then delete";

    // Step 1: Put block (creates merge marker in P2P mode)
    blockstore.put(&cid, data).await.unwrap();
    assert!(!blockstore.is_merged(&cid).await.unwrap());

    // Step 2: Mark as merged
    blockstore.mark_as_merged(&cid).await.unwrap();
    assert!(blockstore.is_merged(&cid).await.unwrap());

    // Step 3: Delete the merged block
    blockstore.delete(&cid).await.unwrap();

    // Step 4: Verify is_merged returns false (block doesn't exist)
    assert!(
        !blockstore.is_merged(&cid).await.unwrap(),
        "is_merged should return false for deleted block"
    );

    // Also verify block is actually gone
    assert!(!blockstore.has(&cid).await.unwrap());
    assert_eq!(blockstore.get(&cid).await.unwrap(), None);

    // And not in unmerged list
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert!(!unmerged.contains(&cid));
}

#[tokio::test]
async fn test_is_merged_after_delete_of_unmerged_block() {
    // Test lifecycle: put -> delete (without merging) -> is_merged
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, true); // P2P mode

    let cid = test_cid();

    // Put block (unmerged)
    blockstore.put(&cid, b"unmerged block").await.unwrap();
    assert!(!blockstore.is_merged(&cid).await.unwrap());

    // Delete without merging
    blockstore.delete(&cid).await.unwrap();

    // is_merged should return false
    assert!(!blockstore.is_merged(&cid).await.unwrap());

    // Verify cleanup - not in unmerged list either
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert!(!unmerged.contains(&cid));
}

#[tokio::test]
async fn test_merge_then_reput_then_delete() {
    // Complex lifecycle: put -> merge -> delete -> put again -> check state
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, true); // P2P mode

    let cid = test_cid();

    // Initial: put and merge
    blockstore.put(&cid, b"first").await.unwrap();
    blockstore.mark_as_merged(&cid).await.unwrap();
    assert!(blockstore.is_merged(&cid).await.unwrap());

    // Delete
    blockstore.delete(&cid).await.unwrap();
    assert!(!blockstore.is_merged(&cid).await.unwrap());

    // Re-put (should create new merge marker in P2P mode)
    blockstore.put(&cid, b"second").await.unwrap();

    // Should be unmerged again (new block, new merge marker)
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

// ==================== Stress Tests ====================

#[tokio::test]
async fn test_stress_many_blocks() {
    // Stress test with many blocks to verify scaling behavior
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    const NUM_BLOCKS: usize = 500;
    let mut cids = Vec::with_capacity(NUM_BLOCKS);

    // Generate and store many blocks
    for i in 0..NUM_BLOCKS {
        let data = format!("block data {}", i);
        let cid = cid_from_data(data.as_bytes());
        blockstore.put(&cid, data.as_bytes()).await.unwrap();
        cids.push(cid);
    }

    // Verify all_cids returns all blocks
    let all = blockstore.all_cids().await.unwrap();
    assert_eq!(
        all.len(),
        NUM_BLOCKS,
        "all_cids should return all {} blocks",
        NUM_BLOCKS
    );

    // Verify all CIDs are present
    for cid in &cids {
        assert!(all.contains(cid), "Missing CID: {}", cid);
    }

    // Verify random access still works
    for (i, cid) in cids.iter().enumerate().step_by(50) {
        let expected = format!("block data {}", i);
        let data = blockstore.get(cid).await.unwrap();
        assert_eq!(data, Some(expected.into_bytes()));
    }

    // Verify has() for all blocks
    for cid in &cids {
        assert!(blockstore.has(cid).await.unwrap());
    }
}

#[tokio::test]
async fn test_stress_many_blocks_p2p_merge_tracking() {
    // Stress test merge tracking with many blocks in P2P mode
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, true); // P2P mode

    const NUM_BLOCKS: usize = 200;
    let mut cids = Vec::with_capacity(NUM_BLOCKS);

    // Store many blocks (all unmerged initially)
    for i in 0..NUM_BLOCKS {
        let data = format!("p2p block {}", i);
        let cid = cid_from_data(data.as_bytes());
        blockstore.put(&cid, data.as_bytes()).await.unwrap();
        cids.push(cid);
    }

    // All should be unmerged
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert_eq!(unmerged.len(), NUM_BLOCKS);

    // Merge half of them
    for cid in cids.iter().take(NUM_BLOCKS / 2) {
        blockstore.mark_as_merged(cid).await.unwrap();
    }

    // Verify correct split
    let unmerged = blockstore.get_unmerged().await.unwrap();
    assert_eq!(
        unmerged.len(),
        NUM_BLOCKS / 2,
        "Half should still be unmerged"
    );

    // Verify merged status
    for (i, cid) in cids.iter().enumerate() {
        let is_merged = blockstore.is_merged(cid).await.unwrap();
        if i < NUM_BLOCKS / 2 {
            assert!(is_merged, "Block {} should be merged", i);
        } else {
            assert!(!is_merged, "Block {} should be unmerged", i);
        }
    }

    // all_cids should still return all blocks (merged or not)
    let all = blockstore.all_cids().await.unwrap();
    assert_eq!(all.len(), NUM_BLOCKS);
}

#[tokio::test]
async fn test_stress_put_many_batch() {
    // Stress test put_many with large batches
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store, false);

    const BATCH_SIZE: usize = 100;

    // Generate batch of blocks
    let blocks: Vec<(Cid, Vec<u8>)> = (0..BATCH_SIZE)
        .map(|i| {
            let data = format!("batch block {}", i).into_bytes();
            let cid = cid_from_data(&data);
            (cid, data)
        })
        .collect();

    // Convert to references for put_many
    let block_refs: Vec<(&Cid, &[u8])> = blocks.iter().map(|(c, d)| (c, d.as_slice())).collect();

    // Put all at once
    blockstore.put_many(&block_refs).await.unwrap();

    // Verify all were stored
    for (cid, expected_data) in &blocks {
        let data = blockstore.get(cid).await.unwrap();
        assert_eq!(data.as_ref(), Some(expected_data));
    }

    // Verify count
    let all = blockstore.all_cids().await.unwrap();
    assert_eq!(all.len(), BATCH_SIZE);
}

#[tokio::test]
async fn test_stress_concurrent_operations() {
    // Stress test with many concurrent operations
    use std::sync::atomic::AtomicUsize;

    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, false));

    // Pre-populate with some blocks
    let mut cids = Vec::new();
    for i in 0..50 {
        let data = format!("preload {}", i);
        let cid = cid_from_data(data.as_bytes());
        blockstore.put(&cid, data.as_bytes()).await.unwrap();
        cids.push(cid);
    }

    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    // Spawn many concurrent readers
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

    // Spawn concurrent writers (new blocks)
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

    // Wait for all operations
    for handle in handles {
        handle.await.unwrap();
    }

    // All operations should have succeeded
    let total_ops = success_count.load(Ordering::Relaxed);
    assert!(
        total_ops >= 500,
        "Expected at least 500 successful ops, got {}",
        total_ops
    );
}

// ==================== Error Path Tests ====================

#[tokio::test]
async fn test_all_cids_skips_malformed_keys_like_go() {
    // Go implementation (corekv/blockstore/blockstore.go:163-167):
    // k, err := cid.Cast(key)
    // if err != nil {
    //     log.ErrorContextE(ctx, "Error parsing key from binary", err)
    //     continue  // Skips unparseable keys
    // }
    //
    // This test verifies Rust matches Go's behavior of logging and skipping
    // keys that cannot be parsed as CIDs.
    let store = Arc::new(MemoryStore::new());
    let blockstore = DefraBlockstore::new(store.clone(), false);

    // Add some valid blocks
    let cid1 = test_cid();
    let cid2 = test_cid2();
    blockstore.put(&cid1, b"data1").await.unwrap();
    blockstore.put(&cid2, b"data2").await.unwrap();

    // Directly write a malformed key to the underlying store
    // This simulates data corruption - a key that's neither a valid CID
    // nor a merge marker. Use a key that won't parse as CID.
    // CIDv1 starts with 0x01, CIDv0 with 0x12. Use 0xAA to guarantee failure.
    let malformed_key = vec![0xAA, 0xBB, 0xCC, 0xDD];
    {
        let mut txn = store.new_txn(false).await.unwrap();
        // Write to blockstore namespace (prefix 'b')
        let mut namespaced_key = vec![b'b'];
        namespaced_key.extend_from_slice(&malformed_key);
        txn.set(&namespaced_key, b"garbage").await.unwrap();
        txn.commit().await.unwrap();
    }

    // all_cids should skip the malformed key and return only valid CIDs
    // (matching Go behavior of logging error and continuing)
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
async fn test_go_key_format_compatibility() {
    // Verify our key encoding matches Go's format exactly.
    // Go (defradb/internal/datastore/blockstore.go:54-59):
    // func newToMergeKey(cid []byte) []byte {
    //     l := len(cid)
    //     key := make([]byte, l+1)
    //     copy(key[1:], cid)
    //     key[0] = toMergeIndexPrefix  // 'm' = 0x6D
    //     return key
    // }
    use storage::keys::blockstore::{BlockstoreKey, ToMergeIndexKey, MERGE_PREFIX};

    let cid = test_cid();

    // Block key: raw CID bytes (no prefix)
    let block_key = BlockstoreKey::new(cid);
    let block_bytes = block_key.bytes();
    assert_eq!(
        block_bytes,
        cid.to_bytes(),
        "Block key should be raw CID bytes"
    );

    // Merge key: 'm' prefix + CID bytes
    let merge_key = ToMergeIndexKey::new(cid);
    let merge_bytes = merge_key.bytes();

    // First byte must be 'm' (0x6D)
    assert_eq!(merge_bytes[0], MERGE_PREFIX);
    assert_eq!(merge_bytes[0], b'm');
    assert_eq!(merge_bytes[0], 0x6D);

    // Rest must be exact CID bytes
    assert_eq!(&merge_bytes[1..], cid.to_bytes().as_slice());

    // Total length: 1 (prefix) + CID bytes
    assert_eq!(merge_bytes.len(), 1 + cid.to_bytes().len());

    // Verify CIDv0 format too (legacy IPFS)
    let cidv0 = Cid::from_str("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n").unwrap();
    let merge_key_v0 = ToMergeIndexKey::new(cidv0);
    let merge_bytes_v0 = merge_key_v0.bytes();
    assert_eq!(merge_bytes_v0[0], b'm');
    assert_eq!(&merge_bytes_v0[1..], cidv0.to_bytes().as_slice());
}

#[tokio::test]
async fn test_cid_bytes_cannot_start_with_merge_prefix() {
    // Safety test: Verify that valid CID binary encodings cannot start
    // with 'm' (0x6D = 109), which would cause false positives in
    // is_merge_key() filtering.
    //
    // CIDv0: Starts with 0x12 (sha2-256 multihash code)
    // CIDv1: Starts with 0x01 (version byte)
    //
    // 0x6D (109) is not a valid CID start byte.
    use storage::keys::blockstore::ToMergeIndexKey;

    // CIDv1 test
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

    // CIDv0 test (Qm... format)
    let cidv0 = Cid::from_str("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n").unwrap();
    let cidv0_bytes = cidv0.to_bytes();
    assert_ne!(
        cidv0_bytes[0], b'm',
        "CIDv0 should not start with 'm' - would break is_merge_key filtering"
    );
    // CIDv0 starts with the multihash directly (0x12 for sha2-256)
    assert_eq!(
        cidv0_bytes[0], 0x12,
        "CIDv0 should start with sha2-256 code 0x12"
    );
    assert!(!ToMergeIndexKey::is_merge_key(&cidv0_bytes));

    // Edge case: raw codec CIDv1
    let raw_cid = cid_from_data(b"test data");
    let raw_bytes = raw_cid.to_bytes();
    assert_eq!(raw_bytes[0], 0x01, "Raw CIDv1 should start with 0x01");
    assert!(!ToMergeIndexKey::is_merge_key(&raw_bytes));
}
