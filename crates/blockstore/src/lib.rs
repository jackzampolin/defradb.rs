//! IPFS-compatible blockstore with merge tracking for DefraDB
//!
//! This crate provides a public API layer for IPLD block storage with
//! CRDT merge tracking support for P2P synchronization.
//!
//! # Architecture
//!
//! ```text
//! Application Layer
//!     ↓
//! Blockstore Trait (this crate - public API)
//!     ↓
//! storage::stores::Blockstore (internal implementation)
//!     ↓
//! CoreKV Backend (Memory/RocksDB)
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use blockstore::{Blockstore, DefraBlockstore};
//! use storage::backends::MemoryStore;
//! use std::sync::Arc;
//!
//! // Create a blockstore
//! let store = Arc::new(MemoryStore::new());
//! let blockstore = DefraBlockstore::new(store, false);
//!
//! // Store a block
//! let cid = /* compute CID from data */;
//! blockstore.put(&cid, b"block data").await?;
//!
//! // Retrieve a block
//! let data = blockstore.get(&cid).await?;
//!
//! // Enable hash verification on read
//! blockstore.hash_on_read(true);
//! let verified_data = blockstore.get(&cid).await?; // Will verify hash
//! ```

mod error;
mod traits;

pub use error::{Error, Result};
pub use traits::Blockstore;

use async_trait::async_trait;
use cid::Cid;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::blockstore::{BlockstoreKey, ToMergeIndexKey};
use storage::stores::blockstore::BlockstoreTxn;
use storage::stores::Blockstore as InternalBlockstore;

/// DefraDB blockstore implementation
///
/// Wraps the internal storage::stores::Blockstore with a clean public API.
/// Supports both P2P mode (with merge tracking) and local mode (no tracking).
pub struct DefraBlockstore<S: Store> {
    store: InternalBlockstore<S>,
    /// Whether to verify hash on read
    rehash: AtomicBool,
}

impl<S: Store + 'static> DefraBlockstore<S> {
    /// Create a new blockstore
    ///
    /// # Arguments
    ///
    /// * `store` - The underlying key-value store
    /// * `is_p2p` - If true, enables merge tracking for P2P synchronization.
    ///              Blocks put in P2P mode are initially marked as "unmerged"
    ///              until explicitly merged via `mark_as_merged`.
    pub fn new(store: Arc<S>, is_p2p: bool) -> Self {
        Self {
            store: InternalBlockstore::new(store, is_p2p),
            rehash: AtomicBool::new(false),
        }
    }

    /// Verify that data matches the CID hash
    ///
    /// This implementation supports SHA2-256 (code 0x12), which is the most common
    /// hash algorithm used in IPFS/IPLD.
    fn verify_hash(&self, cid: &Cid, data: &[u8]) -> Result<()> {
        use sha2::{Digest, Sha256};

        let mh = cid.hash();
        let code = mh.code();

        // Compute hash based on the multihash code
        let computed_digest: Vec<u8> = match code {
            0x12 => {
                // SHA2-256
                let mut hasher = Sha256::new();
                hasher.update(data);
                hasher.finalize().to_vec()
            }
            _ => {
                // For unsupported hash algorithms, skip verification with a warning
                tracing::warn!(
                    hash_code = code,
                    cid = %cid,
                    "Hash verification skipped: unsupported hash algorithm"
                );
                return Ok(());
            }
        };

        // Compare the computed digest with the CID's digest
        if mh.digest() != computed_digest.as_slice() {
            return Err(Error::HashMismatch {
                cid: cid.to_string(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl<S: Store + 'static> Blockstore for DefraBlockstore<S> {
    async fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        let txn = self.store.new_txn(true).await?;
        let bs_txn = txn
            .as_any()
            .downcast_ref::<BlockstoreTxn>()
            .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
        let result = bs_txn.get_block(cid).await?;

        // If hash_on_read is enabled and we got data, verify the hash
        if self.rehash.load(Ordering::Relaxed) {
            if let Some(ref data) = result {
                self.verify_hash(cid, data)?;
            }
        }

        Ok(result)
    }

    async fn put(&self, cid: &Cid, data: &[u8]) -> Result<()> {
        // Optimization: Has is cheaper than Set, so check if we already have it
        // This matches the Go implementation behavior
        if self.has(cid).await? {
            return Ok(());
        }

        let mut txn = self.store.new_txn(false).await?;
        {
            let bs_txn = txn
                .as_any_mut()
                .downcast_mut::<BlockstoreTxn>()
                .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
            bs_txn.put_block(cid, data).await?;
        }
        txn.commit().await?;
        Ok(())
    }

    async fn put_many(&self, blocks: &[(&Cid, &[u8])]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let mut txn = self.store.new_txn(false).await?;
        {
            let bs_txn = txn
                .as_any_mut()
                .downcast_mut::<BlockstoreTxn>()
                .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
            for (cid, data) in blocks {
                // Optimization: skip if already exists (matches Go behavior)
                if bs_txn.has_block(cid).await? {
                    continue;
                }
                bs_txn.put_block(cid, data).await?;
            }
        }
        txn.commit().await?;
        Ok(())
    }

    async fn has(&self, cid: &Cid) -> Result<bool> {
        let txn = self.store.new_txn(true).await?;
        let bs_txn = txn
            .as_any()
            .downcast_ref::<BlockstoreTxn>()
            .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
        let result = bs_txn.has_block(cid).await?;
        Ok(result)
    }

    async fn delete(&self, cid: &Cid) -> Result<()> {
        let mut txn = self.store.new_txn(false).await?;
        {
            let bs_txn = txn
                .as_any_mut()
                .downcast_mut::<BlockstoreTxn>()
                .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
            bs_txn.delete_block(cid).await?;
        }
        txn.commit().await?;
        Ok(())
    }

    async fn get_size(&self, cid: &Cid) -> Result<Option<usize>> {
        let txn = self.store.new_txn(true).await?;
        let block_key = BlockstoreKey::new(*cid);
        let result = txn.get_size(&block_key.bytes()).await?;
        Ok(result)
    }

    async fn all_cids(&self) -> Result<Vec<Cid>> {
        let txn = self.store.new_txn(true).await?;

        // Iterate all keys that don't start with merge prefix
        // Block keys are raw CID bytes, merge keys start with 'm'
        let opts = IterOptions::new().with_keys_only(true);
        let mut iter = txn.iterator(opts).await?;

        let mut cids = Vec::new();
        while let Some(pair) = iter.next().await? {
            // Skip merge marker keys (start with 'm')
            if ToMergeIndexKey::is_merge_key(&pair.key) {
                continue;
            }

            // Parse the key as a CID
            match BlockstoreKey::from_bytes(&pair.key) {
                Ok(key) => cids.push(key.cid),
                Err(e) => {
                    tracing::warn!(
                        key_bytes = ?pair.key,
                        error = %e,
                        "Skipping key that could not be parsed as CID"
                    );
                }
            }
        }

        Ok(cids)
    }

    fn hash_on_read(&self, enabled: bool) {
        self.rehash.store(enabled, Ordering::Relaxed);
    }

    async fn is_merged(&self, cid: &Cid) -> Result<bool> {
        // Match Go semantics: return false if block doesn't exist
        let txn = self.store.new_txn(true).await?;
        let bs_txn = txn
            .as_any()
            .downcast_ref::<BlockstoreTxn>()
            .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;

        // First check if block exists
        let has_block = bs_txn.has_block(cid).await?;
        if !has_block {
            return Ok(false);
        }

        // Block exists, check merge status
        let result = bs_txn.is_merged(cid).await?;
        Ok(result)
    }

    async fn mark_as_merged(&self, cid: &Cid) -> Result<()> {
        let mut txn = self.store.new_txn(false).await?;
        {
            let bs_txn = txn
                .as_any_mut()
                .downcast_mut::<BlockstoreTxn>()
                .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
            bs_txn.mark_as_merged(cid).await?;
        }
        txn.commit().await?;
        Ok(())
    }

    async fn get_unmerged(&self) -> Result<Vec<Cid>> {
        let txn = self.store.new_txn(true).await?;
        let bs_txn = txn
            .as_any()
            .downcast_ref::<BlockstoreTxn>()
            .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
        let result = bs_txn.get_unmerged_cids().await?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use storage::backends::MemoryStore;

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
        use multihash::Multihash;
        use sha2::{Digest, Sha256};

        // Compute SHA2-256 hash
        let mut hasher = Sha256::new();
        hasher.update(data);
        let digest = hasher.finalize();

        // Create multihash with SHA2-256 code (0x12)
        let hash = Multihash::<64>::wrap(0x12, &digest).unwrap();
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
        assert!(!blockstore.rehash.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_hash_on_read_enable_disable() {
        let store = Arc::new(MemoryStore::new());
        let blockstore = DefraBlockstore::new(store, false);

        // Enable
        blockstore.hash_on_read(true);
        assert!(blockstore.rehash.load(Ordering::Relaxed));

        // Disable
        blockstore.hash_on_read(false);
        assert!(!blockstore.rehash.load(Ordering::Relaxed));
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
            let mut txn = blockstore.store.new_txn(false).await.unwrap();
            let bs_txn = txn
                .as_any_mut()
                .downcast_mut::<BlockstoreTxn>()
                .unwrap();
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
}
