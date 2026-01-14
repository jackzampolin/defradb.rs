// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Merge handler trait and outcome types for CRDT block merging.
//!
//! This module defines the interface between the P2P layer and the database
//! layer for applying CRDT merges.

use async_trait::async_trait;
use cid::Cid;

/// Outcome of a merge operation.
///
/// Used by `MergeHandler::handle_block` to communicate the result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeOutcome {
    /// Block was merged successfully into the database.
    Merged,
    /// Block was skipped (already applied, rejected by CRDT, etc.).
    Skipped {
        /// Human-readable reason for skipping.
        reason: String,
    },
}

impl MergeOutcome {
    /// Create a skipped outcome with the given reason.
    pub fn skipped(reason: impl Into<String>) -> Self {
        Self::Skipped {
            reason: reason.into(),
        }
    }

    /// Returns true if this outcome indicates the block was merged.
    pub fn is_merged(&self) -> bool {
        matches!(self, Self::Merged)
    }

    /// Returns true if this outcome indicates the block was skipped.
    pub fn is_skipped(&self) -> bool {
        matches!(self, Self::Skipped { .. })
    }
}

/// Handler for merging incoming blocks.
///
/// Implement this trait in the database layer to handle CRDT merges.
/// The P2P layer calls this when a new block is received.
#[async_trait]
pub trait MergeHandler: Send + Sync {
    /// Error type for merge operations
    type Error: std::error::Error + Send + Sync + 'static;

    /// Handle an incoming block.
    ///
    /// This method should:
    /// 1. Decode the block data into a CRDT delta
    /// 2. Load/create the appropriate CRDT for the document
    /// 3. Execute the merge within a transaction
    /// 4. Return the merge outcome
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the block
    /// * `block_data` - The raw block data
    /// * `doc_id` - The document this block belongs to
    /// * `collection_id` - The collection ID
    /// * `creator` - The peer that created this block
    ///
    /// # Returns
    ///
    /// * `Ok(MergeOutcome::Merged)` - Block was merged successfully
    /// * `Ok(MergeOutcome::Skipped { reason })` - Block was skipped
    /// * `Err(e)` - Merge failed
    async fn handle_block(
        &self,
        cid: &Cid,
        block_data: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator: &str,
    ) -> Result<MergeOutcome, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_outcome_helpers() {
        let merged = MergeOutcome::Merged;
        assert!(merged.is_merged());
        assert!(!merged.is_skipped());

        let skipped = MergeOutcome::skipped("already applied");
        assert!(!skipped.is_merged());
        assert!(skipped.is_skipped());
    }
}
