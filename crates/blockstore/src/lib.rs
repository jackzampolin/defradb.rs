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
mod verify;

pub use error::{Error, Result};
pub use traits::Blockstore;
pub use verify::verify_block_cid;

use async_trait::async_trait;
use bytes::Bytes;
use cid::Cid;
use lru::LruCache;
use multihash_codetable::Code;
use parking_lot::Mutex;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::blockstore::{BlockstoreKey, ToMergeIndexKey};
use storage::stores::blockstore::BlockstoreTxn;
use storage::stores::Blockstore as InternalBlockstore;
use tracing::instrument;

/// Default number of entries in the block cache (matches Go's 1M entry LRU).
const DEFAULT_BLOCK_CACHE_SIZE: usize = 1_000_000;

/// Number of entries in the merged CID cache.
///
/// Tracks CIDs that have been merged so `is_merged()` can skip storage reads
/// during P2P DAG sync hot loops. Sized smaller than the block cache since
/// only merged CIDs are tracked (not full block data).
const DEFAULT_MERGED_CACHE_SIZE: usize = 100_000;

/// DefraDB blockstore implementation
///
/// Wraps the internal storage::stores::Blockstore with a clean public API.
/// Supports both P2P mode (with merge tracking) and local mode (no tracking).
///
/// Includes a process-wide LRU cache for recently accessed blocks, reducing
/// storage reads during merge, query, and P2P sync operations.
pub struct DefraBlockstore<S: Store> {
    store: InternalBlockstore<S>,
    /// Whether to verify hash on read
    rehash: AtomicBool,
    /// LRU cache for recently accessed blocks (CID → block bytes).
    /// Uses `Bytes` for O(1) clone on cache hits instead of O(n) `Vec<u8>::clone`.
    cache: Mutex<LruCache<Cid, Bytes>>,
    /// LRU cache for merged CIDs — fast path for `is_merged()` checks
    /// during P2P DAG sync. Only positive results are cached (merged = true).
    merged_cache: Mutex<LruCache<Cid, ()>>,
}

impl<S: Store> fmt::Debug for DefraBlockstore<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefraBlockstore")
            .field("rehash", &self.rehash.load(Ordering::Relaxed))
            .finish()
    }
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
            cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(DEFAULT_BLOCK_CACHE_SIZE).unwrap(),
            )),
            merged_cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(DEFAULT_MERGED_CACHE_SIZE).unwrap(),
            )),
        }
    }

    /// Check if hash verification on read is currently enabled
    pub fn rehash_enabled(&self) -> bool {
        self.rehash.load(Ordering::Relaxed)
    }

    /// Create a new transaction on the underlying store (for testing)
    pub async fn new_store_txn(&self, read_only: bool) -> Result<Box<dyn storage::corekv::Txn>> {
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
        if code == u64::from(Code::Sha2_256) {
            let mut hasher = Sha256::new();
            hasher.update(data);
            let computed_digest = hasher.finalize();

            if mh.digest() != computed_digest.as_slice() {
                return Err(Error::HashMismatch {
                    cid: cid.to_string(),
                });
            }
            Ok(())
        } else {
            tracing::warn!(
                hash_code = code,
                cid = %cid,
                "Hash verification skipped: unsupported hash algorithm"
            );
            Ok(())
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> Blockstore for DefraBlockstore<S> {
    #[instrument(level = "trace", skip(self))]
    async fn get(&self, cid: &Cid) -> Result<Option<Bytes>> {
        // Check LRU cache first (skip cache when hash verification is enabled,
        // since cached data may not have been verified yet)
        if !self.rehash.load(Ordering::Acquire) {
            let mut cache = self.cache.lock();
            if let Some(data) = cache.get(cid) {
                return Ok(Some(data.clone()));
            }
        }

        let txn = self.store.new_txn(true).await?;
        let bs_txn = txn
            .as_any()
            .downcast_ref::<BlockstoreTxn>()
            .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
        let result = bs_txn.get_block(cid).await?;

        // If hash_on_read is enabled and we got data, verify the hash
        if self.rehash.load(Ordering::Acquire) {
            if let Some(ref data) = result {
                self.verify_hash(cid, data)?;
            }
        }

        // Populate cache on miss, convert Vec<u8> → Bytes
        if let Some(data) = result {
            let bytes = Bytes::from(data);
            self.cache.lock().put(*cid, bytes.clone());
            return Ok(Some(bytes));
        }

        Ok(None)
    }

    #[instrument(level = "trace", skip(self))]
    async fn put(&self, cid: &Cid, data: &[u8]) -> Result<()> {
        // Check cache for existence (avoids storage read for recently-written blocks)
        if self.cache.lock().contains(cid) {
            return Ok(());
        }

        let mut txn = self.store.new_txn(false).await?;
        let already_exists = {
            let bs_txn = txn
                .as_any_mut()
                .downcast_mut::<BlockstoreTxn>()
                .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
            if bs_txn.has_block(cid).await? {
                true
            } else {
                bs_txn.put_block(cid, data).await?;
                false
            }
        };
        if already_exists {
            txn.discard();
            return Ok(());
        }
        txn.commit().await?;

        // Write-through: populate cache
        self.cache.lock().put(*cid, Bytes::copy_from_slice(data));
        Ok(())
    }

    async fn put_many(&self, blocks: &[(&Cid, &[u8])]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let blocks_to_write: Vec<_> = {
            let cache = self.cache.lock();
            blocks
                .iter()
                .copied()
                .filter(|(cid, _)| !cache.contains(cid))
                .collect()
        };
        if blocks_to_write.is_empty() {
            return Ok(());
        }

        let mut txn = self.store.new_txn(false).await?;
        let mut written: Vec<(Cid, Bytes)> = Vec::new();
        {
            let bs_txn = txn
                .as_any_mut()
                .downcast_mut::<BlockstoreTxn>()
                .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
            for (cid, data) in blocks_to_write {
                if bs_txn.has_block(cid).await? {
                    continue;
                }
                bs_txn.put_block(cid, data).await?;
                written.push((*cid, Bytes::copy_from_slice(data)));
            }
        }
        txn.commit().await?;

        // Write-through: populate cache for all written blocks
        let mut cache = self.cache.lock();
        for (cid, data) in written {
            cache.put(cid, data);
        }
        Ok(())
    }

    async fn has(&self, cid: &Cid) -> Result<bool> {
        // Check cache first
        if self.cache.lock().contains(cid) {
            return Ok(true);
        }

        let txn = self.store.new_txn(true).await?;
        let bs_txn = txn
            .as_any()
            .downcast_ref::<BlockstoreTxn>()
            .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
        let result = bs_txn.has_block(cid).await?;
        Ok(result)
    }

    async fn delete(&self, cid: &Cid) -> Result<()> {
        // Evict from both caches
        self.cache.lock().pop(cid);
        self.merged_cache.lock().pop(cid);

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
        self.rehash.store(enabled, Ordering::Release);
    }

    async fn is_merged(&self, cid: &Cid) -> Result<bool> {
        // Fast path: check merged cache
        if self.merged_cache.lock().contains(cid) {
            return Ok(true);
        }

        // Slow path: check storage
        let txn = self.store.new_txn(true).await?;
        let bs_txn = txn
            .as_any()
            .downcast_ref::<BlockstoreTxn>()
            .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;

        let has_block = bs_txn.has_block(cid).await?;
        if !has_block {
            return Ok(false);
        }

        let result = bs_txn.is_merged(cid).await?;

        // Cache positive results
        if result {
            self.merged_cache.lock().put(*cid, ());
        }

        Ok(result)
    }

    async fn mark_as_merged(&self, cid: &Cid) -> Result<()> {
        let mut txn = self.store.new_txn(false).await?;
        let block_exists;
        {
            let bs_txn = txn
                .as_any_mut()
                .downcast_mut::<BlockstoreTxn>()
                .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
            block_exists = bs_txn.has_block(cid).await?;
            bs_txn.mark_as_merged(cid).await?;
        }
        txn.commit().await?;
        if block_exists {
            self.merged_cache.lock().put(*cid, ());
        }
        Ok(())
    }

    async fn mark_batch_as_merged(&self, cids: &[Cid]) -> Result<()> {
        if cids.is_empty() {
            return Ok(());
        }
        let mut txn = self.store.new_txn(false).await?;
        {
            let bs_txn = txn
                .as_any_mut()
                .downcast_mut::<BlockstoreTxn>()
                .ok_or_else(|| Error::Internal("Failed to downcast transaction".to_string()))?;
            for cid in cids {
                bs_txn.mark_as_merged(cid).await?;
            }
        }
        txn.commit().await?;
        let mut cache = self.merged_cache.lock();
        for cid in cids {
            cache.put(*cid, ());
        }
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
