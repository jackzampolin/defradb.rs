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

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use cid::Cid;
use iroh_bitswap::{Block, Store};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// Mock BitswapStore for testing.
///
/// A simple in-memory implementation of iroh_bitswap::Store that can be used
/// in unit and integration tests without requiring a real blockstore.
#[derive(Clone, Debug)]
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
impl Store for MockBitswapStore {
    async fn get_size(&self, cid: &Cid) -> Result<usize> {
        self.blocks
            .lock()
            .unwrap()
            .get(cid)
            .map(|data| data.len())
            .ok_or_else(|| anyhow!("block not found: {}", cid))
    }

    async fn get(&self, cid: &Cid) -> Result<Block> {
        let data = self
            .blocks
            .lock()
            .unwrap()
            .get(cid)
            .cloned()
            .ok_or_else(|| anyhow!("block not found: {}", cid))?;
        Ok(Block::new(Bytes::from(data), *cid))
    }

    async fn has(&self, cid: &Cid) -> Result<bool> {
        Ok(self.blocks.lock().unwrap().contains_key(cid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_store_basic_operations() {
        let store = MockBitswapStore::new();
        assert!(store.is_empty());

        // Create a test CID
        let cid =
            Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();

        // Check not present
        assert!(!store.has(&cid).await.unwrap());

        // Get missing returns error
        assert!(store.get(&cid).await.is_err());
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
