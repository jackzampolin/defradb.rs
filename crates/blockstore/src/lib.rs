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
    ///   Blocks put in P2P mode are initially marked as "unmerged"
    ///   until explicitly merged via `mark_as_merged`.
    pub fn new(store: Arc<S>, is_p2p: bool) -> Self {
        Self {
            store: InternalBlockstore::new(store, is_p2p),
            rehash: AtomicBool::new(false),
        }
    }

    /// Check if hash verification on read is currently enabled
    pub fn rehash_enabled(&self) -> bool {
        self.rehash.load(Ordering::Relaxed)
    }

    /// Create a new transaction on the underlying store (for testing)
    pub async fn new_store_txn(
        &self,
        read_only: bool,
    ) -> Result<Box<dyn storage::corekv::Txn>> {
        Ok(self.store.new_txn(read_only).await?)
    }

    /// Verify that data matches the CID hash
    ///
    /// # Supported Algorithms
    ///
    /// Currently supports SHA2-256 (code 0x12), which is the most common hash
    /// algorithm used in IPFS/IPLD and DefraDB.
    ///
    /// # Go Compatibility Note
    ///
    /// The Go implementation uses `cid.Prefix().Sum(data)` which delegates to the
    /// go-multihash library and supports all registered hash algorithms. This Rust
    /// implementation explicitly handles SHA2-256 only. Unsupported algorithms are
    /// logged and skipped (verification passes) rather than erroring, matching the
    /// principle of being permissive on read. If DefraDB ever uses non-SHA256 hashes,
    /// this function should be extended to support them.
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

// ==================== ProofBlockstore Implementation ====================

/// Implement ProofBlockstore for DefraBlockstore to enable Merkle proof extraction.
///
/// This allows the crypto crate's `extract_proof` function to work with
/// DefraBlockstore instances, enabling Merkle proof generation over the
/// block DAG.
#[async_trait]
impl<S: Store + 'static> crypto::ProofBlockstore for DefraBlockstore<S> {
    async fn get_block(&self, cid: &Cid) -> defra_core::Result<Option<Vec<u8>>> {
        // Delegate to the Blockstore::get implementation and convert error type
        self.get(cid)
            .await
            .map_err(|e| defra_core::Error::Storage(e.to_string()))
    }
}
