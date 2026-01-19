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
