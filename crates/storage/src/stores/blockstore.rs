/// Blockstore - IPLD blocks and merkle tree nodes
///
/// The Blockstore handles storage of IPLD blocks with merge tracking for CRDT operations.
/// It tracks which blocks have been merged into the permanent store vs. pending merge.
use crate::corekv::{IterOptions, Iterator, Key, Reader, Result, Store, Txn, Writer};
use crate::keys::blockstore::{BlockstoreKey, ToMergeIndexKey, OBJECT_MARKER};
use crate::namespace::{Namespace, NamespacedStore};
use async_trait::async_trait;
use cid::Cid;
use std::sync::Arc;

/// Blockstore provides storage for IPLD blocks with merge tracking
pub struct Blockstore<S: Store> {
    store: NamespacedStore<S>,
    /// Whether this is a P2P blockstore (affects merge tracking behavior)
    is_p2p: bool,
}

impl<S: Store> Blockstore<S> {
    /// Create a new Blockstore with specified namespace
    pub fn new_with_namespace(store: Arc<S>, is_p2p: bool, namespace: Namespace) -> Self {
        Self {
            store: NamespacedStore::new(store, namespace),
            is_p2p,
        }
    }

    /// Create a new Blockstore (uses Blockstore namespace by default)
    pub fn new(store: Arc<S>, is_p2p: bool) -> Self {
        Self::new_with_namespace(store, is_p2p, Namespace::Blockstore)
    }
}

#[async_trait]
impl<S: Store> Store for Blockstore<S> {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        let txn = self.store.new_txn(readonly).await?;
        Ok(Box::new(BlockstoreTxn {
            txn,
            is_p2p: self.is_p2p,
        }))
    }

    async fn close(&self) -> Result<()> {
        self.store.close().await
    }
}

/// Blockstore transaction with merge tracking
pub struct BlockstoreTxn {
    txn: Box<dyn Txn>,
    is_p2p: bool,
}

impl BlockstoreTxn {
    /// Put a block with automatic merge tracking
    pub async fn put_block(&mut self, cid: &Cid, data: &[u8]) -> Result<()> {
        let block_key = BlockstoreKey::new(*cid);

        // Write the block data
        self.set(&block_key.bytes(), data).await?;

        // If this is a P2P store, track it as unmerged
        if self.is_p2p {
            let merge_key = ToMergeIndexKey::new(*cid);
            // Use 0xff marker value to match Go implementation for cross-impl compatibility
            self.set(&merge_key.bytes(), &[OBJECT_MARKER]).await?;
        }

        Ok(())
    }

    /// Get a block by CID
    pub async fn get_block(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        let block_key = BlockstoreKey::new(*cid);
        self.get(&block_key.bytes()).await
    }

    /// Check if a block exists
    pub async fn has_block(&self, cid: &Cid) -> Result<bool> {
        let block_key = BlockstoreKey::new(*cid);
        self.has(&block_key.bytes()).await
    }

    /// Check if a block has been merged
    pub async fn is_merged(&self, cid: &Cid) -> Result<bool> {
        let merge_key = ToMergeIndexKey::new(*cid);
        let has_merge_marker = self.has(&merge_key.bytes()).await?;
        // If the merge marker doesn't exist, the block is merged
        Ok(!has_merge_marker)
    }

    /// Mark a block as merged (removes from merge tracking)
    pub async fn mark_as_merged(&mut self, cid: &Cid) -> Result<()> {
        let merge_key = ToMergeIndexKey::new(*cid);
        self.delete(&merge_key.bytes()).await
    }

    /// Get all unmerged block CIDs
    ///
    /// Returns a list of CIDs for blocks that have not yet been merged.
    /// If any keys fail to parse, returns an error to prevent silent data loss.
    /// The error includes the count of successfully parsed CIDs for debugging.
    pub async fn get_unmerged_cids(&self) -> Result<Vec<Cid>> {
        let mut cids = Vec::new();
        let mut parse_errors = 0;

        let opts = IterOptions::new().with_prefix(ToMergeIndexKey::merge_prefix());

        let mut iter = self.iterator(opts).await?;
        while let Some(pair) = iter.next().await? {
            // Parse the key to extract CID
            match ToMergeIndexKey::from_bytes(&pair.key) {
                Ok(merge_key) => {
                    cids.push(merge_key.cid);
                }
                Err(e) => {
                    parse_errors += 1;
                    tracing::error!(
                        key_bytes = ?pair.key,
                        error = %e,
                        "Failed to parse merge index key - possible data corruption"
                    );
                }
            }
        }

        // Return error if any keys failed to parse - this indicates data corruption
        // and the caller should be aware that the result is incomplete
        if parse_errors > 0 {
            return Err(crate::corekv::Error::Other(format!(
                "Data corruption detected: {} merge index keys could not be parsed (recovered {} CIDs). \
                This may indicate storage corruption or a version mismatch.",
                parse_errors, cids.len()
            )));
        }

        Ok(cids)
    }

    /// Delete a block and its merge tracking
    pub async fn delete_block(&mut self, cid: &Cid) -> Result<()> {
        let block_key = BlockstoreKey::new(*cid);
        self.delete(&block_key.bytes()).await?;

        // Also delete merge tracking if it exists
        let merge_key = ToMergeIndexKey::new(*cid);
        if self.has(&merge_key.bytes()).await? {
            self.delete(&merge_key.bytes()).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Reader for BlockstoreTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.txn.get(key).await
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        self.txn.has(key).await
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        self.txn.get_size(key).await
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        self.txn.iterator(opts).await
    }
}

#[async_trait]
impl Writer for BlockstoreTxn {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.txn.set(key, value).await
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.txn.delete(key).await
    }
}

#[async_trait]
impl Txn for BlockstoreTxn {
    async fn commit(self: Box<Self>) -> Result<()> {
        self.txn.commit().await
    }

    fn discard(self: Box<Self>) {
        self.txn.discard()
    }

    fn on_success(&mut self, callback: crate::corekv::TxnCallback) {
        self.txn.on_success(callback)
    }

    fn on_success_async(&mut self, callback: crate::corekv::AsyncTxnCallback) {
        self.txn.on_success_async(callback)
    }

    fn on_error(&mut self, callback: crate::corekv::TxnCallback) {
        self.txn.on_error(callback)
    }

    fn on_error_async(&mut self, callback: crate::corekv::AsyncTxnCallback) {
        self.txn.on_error_async(callback)
    }

    fn on_discard(&mut self, callback: crate::corekv::TxnCallback) {
        self.txn.on_discard(callback)
    }

    fn on_discard_async(&mut self, callback: crate::corekv::AsyncTxnCallback) {
        self.txn.on_discard_async(callback)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn is_readonly(&self) -> bool {
        self.txn.is_readonly()
    }
}

// Tests extracted to crates/storage/tests/blockstore_tests.rs
