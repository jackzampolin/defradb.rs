// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! BitswapStore adapter for DefraBlockstore.
//!
//! This module implements the `BitswapStore` trait from libp2p-bitswap-next
//! for our async `DefraBlockstore`. The trait methods are async, which maps
//! directly to our async blockstore interface.
//!
//! # Go Compatibility
//!
//! Go DefraDB uses Bitswap for block exchange. This adapter enables the same
//! block exchange protocol in Rust, ensuring interoperability.

use std::sync::Arc;

use async_trait::async_trait;
use libipld::{Block, Cid, DefaultParams, Result as IpldResult};
use libp2p_bitswap_next::BitswapStore;

use blockstore::Blockstore;

/// Adapter that implements `BitswapStore` for any `Blockstore`.
///
/// The BitswapStore trait from libp2p-bitswap-next uses async methods,
/// which maps directly to our async DefraBlockstore interface.
///
/// # Thread Safety
///
/// This adapter is thread-safe and can be cloned. The underlying blockstore
/// is shared via `Arc`.
pub struct BitswapStoreAdapter<B: Blockstore> {
    blockstore: Arc<B>,
}

impl<B: Blockstore> Clone for BitswapStoreAdapter<B> {
    fn clone(&self) -> Self {
        Self {
            blockstore: self.blockstore.clone(),
        }
    }
}

impl<B: Blockstore> BitswapStoreAdapter<B> {
    /// Create a new BitswapStore adapter.
    ///
    /// # Arguments
    ///
    /// * `blockstore` - The underlying async blockstore
    pub fn new(blockstore: Arc<B>) -> Self {
        Self { blockstore }
    }
}

#[async_trait]
impl<B: Blockstore + 'static> BitswapStore for BitswapStoreAdapter<B> {
    type Params = DefaultParams;

    /// Check if the blockstore contains a block with the given CID.
    async fn contains(&mut self, cid: &Cid) -> IpldResult<bool> {
        self.blockstore
            .has(cid)
            .await
            .map_err(|e| libipld::error::Error::msg(e.to_string()))
    }

    /// Get block data by CID.
    async fn get(&mut self, cid: &Cid) -> IpldResult<Option<Vec<u8>>> {
        self.blockstore
            .get(cid)
            .await
            .map_err(|e| libipld::error::Error::msg(e.to_string()))
    }

    /// Insert a block into the blockstore.
    ///
    /// This is called when receiving blocks from other peers via Bitswap.
    async fn insert(&mut self, block: &Block<Self::Params>) -> IpldResult<()> {
        self.blockstore
            .put(block.cid(), block.data())
            .await
            .map_err(|e| libipld::error::Error::msg(e.to_string()))
    }

    /// Find missing blocks in a DAG starting from the given CID.
    ///
    /// This traverses the block's IPLD links and returns CIDs of blocks
    /// that are not present in the blockstore. Used for DAG synchronization.
    async fn missing_blocks(&mut self, cid: &Cid) -> IpldResult<Vec<Cid>> {
        let mut stack = vec![*cid];
        let mut missing = vec![];

        while let Some(cid) = stack.pop() {
            if let Some(data) = self.get(&cid).await? {
                // Parse block and extract references
                let block = Block::<Self::Params>::new_unchecked(cid, data);
                block.references(&mut stack)?;
            } else {
                missing.push(cid);
            }
        }

        Ok(missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockstore::DefraBlockstore;
    use libipld::cbor::DagCborCodec;
    use libipld::multihash::Code;
    use libipld::Ipld;
    use std::str::FromStr;
    use storage::backends::MemoryStore;

    fn create_test_adapter() -> BitswapStoreAdapter<DefraBlockstore<MemoryStore>> {
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, false));
        BitswapStoreAdapter::new(blockstore)
    }

    fn create_block(ipld: Ipld) -> Block<DefaultParams> {
        Block::encode(DagCborCodec, Code::Blake3_256, &ipld).unwrap()
    }

    fn test_cid() -> Cid {
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
    }

    #[tokio::test]
    async fn test_contains_empty() {
        let mut adapter = create_test_adapter();
        let cid = test_cid();
        assert!(!adapter.contains(&cid).await.unwrap());
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let mut adapter = create_test_adapter();
        let block = create_block(Ipld::String("hello".to_string()));

        adapter.insert(&block).await.unwrap();

        assert!(adapter.contains(block.cid()).await.unwrap());
        let data = adapter.get(block.cid()).await.unwrap();
        assert_eq!(data, Some(block.data().to_vec()));
    }

    #[tokio::test]
    async fn test_get_missing() {
        let mut adapter = create_test_adapter();
        let cid = test_cid();
        assert_eq!(adapter.get(&cid).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_missing_blocks_single() {
        let mut adapter = create_test_adapter();
        let cid = test_cid();

        // Block not present, should be in missing list
        let missing = adapter.missing_blocks(&cid).await.unwrap();
        assert_eq!(missing, vec![cid]);
    }

    #[tokio::test]
    async fn test_missing_blocks_present() {
        let mut adapter = create_test_adapter();
        let block = create_block(Ipld::Integer(42));

        // Insert the block
        adapter.insert(&block).await.unwrap();

        // No links and block is present, should have no missing blocks
        let missing = adapter.missing_blocks(block.cid()).await.unwrap();
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn test_missing_blocks_with_links() {
        let mut adapter = create_test_adapter();

        // Create a leaf block
        let leaf = create_block(Ipld::String("leaf".to_string()));
        let leaf_cid = *leaf.cid();

        // Create a parent block linking to the leaf
        let parent = create_block(Ipld::Link(leaf_cid));

        // Insert only the parent
        adapter.insert(&parent).await.unwrap();

        // Missing blocks should include the leaf
        let missing = adapter.missing_blocks(parent.cid()).await.unwrap();
        assert_eq!(missing, vec![leaf_cid]);
    }

    #[tokio::test]
    async fn test_missing_blocks_complete_dag() {
        let mut adapter = create_test_adapter();

        // Create a leaf block
        let leaf = create_block(Ipld::String("leaf".to_string()));
        let leaf_cid = *leaf.cid();

        // Create a parent block linking to the leaf
        let parent = create_block(Ipld::Link(leaf_cid));

        // Insert both
        adapter.insert(&leaf).await.unwrap();
        adapter.insert(&parent).await.unwrap();

        // Should have no missing blocks
        let missing = adapter.missing_blocks(parent.cid()).await.unwrap();
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn test_shared_blockstore() {
        // Create two adapters sharing the same blockstore
        let store = Arc::new(MemoryStore::new());
        let blockstore = Arc::new(DefraBlockstore::new(store, false));

        let mut adapter1 = BitswapStoreAdapter::new(blockstore.clone());
        let mut adapter2 = BitswapStoreAdapter::new(blockstore);

        let block = create_block(Ipld::Integer(123));

        // Insert via adapter1
        adapter1.insert(&block).await.unwrap();

        // Should be visible via adapter2
        assert!(adapter2.contains(block.cid()).await.unwrap());
    }
}
