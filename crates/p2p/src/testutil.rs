// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Test utilities for P2P crate.
//!
//! This module provides common test helpers and mocks used across unit and integration tests.

use async_trait::async_trait;
use libipld::{Block, Cid, DefaultParams, Result as IpldResult};
use libp2p_bitswap_next::BitswapStore;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Mock BitswapStore for testing.
///
/// A simple in-memory implementation of BitswapStore that can be used
/// in unit and integration tests without requiring a real blockstore.
#[derive(Clone)]
pub struct MockBitswapStore {
    blocks: Arc<Mutex<HashMap<Cid, Vec<u8>>>>,
}

impl MockBitswapStore {
    /// Create a new empty mock store.
    pub fn new() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Pre-populate the store with a block.
    pub fn with_block(self, cid: Cid, data: Vec<u8>) -> Self {
        self.blocks.lock().unwrap().insert(cid, data);
        self
    }

    /// Get the number of blocks in the store.
    pub fn len(&self) -> usize {
        self.blocks.lock().unwrap().len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.blocks.lock().unwrap().is_empty()
    }
}

impl Default for MockBitswapStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BitswapStore for MockBitswapStore {
    type Params = DefaultParams;

    async fn contains(&mut self, cid: &Cid) -> IpldResult<bool> {
        Ok(self.blocks.lock().unwrap().contains_key(cid))
    }

    async fn get(&mut self, cid: &Cid) -> IpldResult<Option<Vec<u8>>> {
        Ok(self.blocks.lock().unwrap().get(cid).cloned())
    }

    async fn insert(&mut self, block: &Block<Self::Params>) -> IpldResult<()> {
        self.blocks
            .lock()
            .unwrap()
            .insert(*block.cid(), block.data().to_vec());
        Ok(())
    }

    async fn missing_blocks(&mut self, cid: &Cid) -> IpldResult<Vec<Cid>> {
        if self.blocks.lock().unwrap().contains_key(cid) {
            Ok(vec![])
        } else {
            Ok(vec![*cid])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_store_basic_operations() {
        let mut store = MockBitswapStore::new();
        assert!(store.is_empty());

        // Create a test CID
        let cid =
            Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();

        // Check missing
        assert!(!store.contains(&cid).await.unwrap());
        assert_eq!(store.missing_blocks(&cid).await.unwrap(), vec![cid]);

        // Get missing
        assert!(store.get(&cid).await.unwrap().is_none());
    }

    #[test]
    fn test_mock_store_with_block() {
        let cid =
            Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();
        let store = MockBitswapStore::new().with_block(cid, b"test data".to_vec());

        assert!(!store.is_empty());
        assert_eq!(store.len(), 1);
    }
}
