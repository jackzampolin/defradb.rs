//! Blockstore trait definition
//!
//! The `Blockstore` trait defines the public API for IPLD block storage
//! with merge tracking support for P2P synchronization.

use async_trait::async_trait;
use cid::Cid;
use storage::corekv::MaybeSendSync;

use crate::Result;

/// IPFS-compatible blockstore trait
///
/// This trait provides the public API for storing and retrieving IPLD blocks.
/// Implementations support optional merge tracking for P2P synchronization.
///
/// # Block Storage
///
/// Blocks are stored and retrieved by their Content Identifier (CID).
/// CIDs are cryptographic hashes of the block content, ensuring
/// content-addressed storage with automatic deduplication.
///
/// # Hash Verification (HashOnRead)
///
/// When enabled via `hash_on_read(true)`, the blockstore verifies that
/// retrieved data matches the CID hash. This provides protection against
/// data corruption but adds computational overhead.
///
/// # Merge Tracking (P2P Mode)
///
/// In P2P mode, blocks received from peers are initially marked as "unmerged".
/// This allows the CRDT merge system to process them before they're considered
/// part of the local state. The lifecycle is:
///
/// 1. `put()` - Store block, mark as unmerged (P2P mode only)
/// 2. `get_unmerged()` - Get all blocks pending merge
/// 3. (CRDT system processes and merges blocks)
/// 4. `mark_as_merged()` - Remove from unmerged tracking
///
/// In local mode (non-P2P), blocks are immediately considered merged.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Blockstore: MaybeSendSync {
    /// Retrieve a block by its CID
    ///
    /// If `hash_on_read` is enabled, verifies that the retrieved data
    /// matches the CID hash before returning. Returns an error if the
    /// hash doesn't match.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(data))` - Block data if found (and verified if hash_on_read enabled)
    /// * `Ok(None)` - Block not found
    /// * `Err(Error::HashMismatch)` - Data doesn't match CID (if hash_on_read enabled)
    /// * `Err(Error)` - Operation failed
    async fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>>;

    /// Store a block with the given CID
    ///
    /// In P2P mode, the block is marked as unmerged until `mark_as_merged` is called.
    /// In local mode, the block is immediately considered merged.
    ///
    /// Storing a block with a CID that already exists is a no-op (deduplication).
    async fn put(&self, cid: &Cid, data: &[u8]) -> Result<()>;

    /// Store multiple blocks in a single transaction
    ///
    /// This is more efficient than calling `put` multiple times for batch operations.
    async fn put_many(&self, blocks: &[(&Cid, &[u8])]) -> Result<()>;

    /// Check if a block exists
    async fn has(&self, cid: &Cid) -> Result<bool>;

    /// Delete a block
    ///
    /// Also removes any merge tracking for this block.
    async fn delete(&self, cid: &Cid) -> Result<()>;

    /// Get the size of a block without reading the entire content
    ///
    /// # Returns
    ///
    /// * `Ok(Some(size))` - Block size in bytes
    /// * `Ok(None)` - Block not found
    /// * `Err(Error)` - Operation failed
    async fn get_size(&self, cid: &Cid) -> Result<Option<usize>>;

    /// Get all CIDs stored in the blockstore
    ///
    /// Note: This may be expensive for large blockstores.
    async fn all_cids(&self) -> Result<Vec<Cid>>;

    /// Enable or disable hash verification on read
    ///
    /// When enabled, `get()` will verify that the retrieved data matches
    /// the CID hash. This provides data integrity verification but adds
    /// computational overhead.
    ///
    /// Default: disabled
    fn hash_on_read(&self, enabled: bool);

    // Merge tracking methods (for P2P sync)

    /// Check if a block has been merged
    ///
    /// Returns `false` if:
    /// - The block doesn't exist, OR
    /// - The block exists but has an unmerged marker (P2P mode)
    ///
    /// Returns `true` if:
    /// - The block exists AND has no unmerged marker
    async fn is_merged(&self, cid: &Cid) -> Result<bool>;

    /// Mark a block as merged
    ///
    /// After calling this, `is_merged` will return true and the block
    /// will no longer appear in `get_unmerged` results.
    async fn mark_as_merged(&self, cid: &Cid) -> Result<()>;

    /// Mark multiple blocks as merged in a single transaction.
    ///
    /// Default implementation calls `mark_as_merged` per CID. Implementations
    /// can override to batch into a single transaction for better performance.
    async fn mark_batch_as_merged(&self, cids: &[Cid]) -> Result<()> {
        for cid in cids {
            self.mark_as_merged(cid).await?;
        }
        Ok(())
    }

    /// Get all unmerged block CIDs
    ///
    /// Returns CIDs of blocks that have been received via P2P but not yet
    /// processed by the CRDT merge system.
    ///
    /// In local mode, always returns an empty vector.
    async fn get_unmerged(&self) -> Result<Vec<Cid>>;
}
