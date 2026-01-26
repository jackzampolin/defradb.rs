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

/// Metadata about a block being merged.
///
/// During normal operation, all fields are populated from the PushLog message.
/// During crash recovery, metadata may be unavailable (fields are None) and
/// implementations must extract it from the block data itself.
#[derive(Debug, Clone)]
pub struct BlockMetadata<'a> {
    /// Document ID this block belongs to (None during recovery)
    pub doc_id: Option<&'a str>,
    /// Collection ID (None during recovery)
    pub collection_id: Option<&'a str>,
    /// Peer that created this block (None during recovery)
    pub creator: Option<&'a str>,
    /// Whether this is a recovery operation (block was stored but not merged before crash)
    pub is_recovery: bool,
}

impl<'a> BlockMetadata<'a> {
    /// Create metadata for a normal (non-recovery) merge operation.
    pub fn normal(doc_id: &'a str, collection_id: &'a str, creator: &'a str) -> Self {
        Self {
            doc_id: Some(doc_id),
            collection_id: Some(collection_id),
            creator: Some(creator),
            is_recovery: false,
        }
    }

    /// Create metadata for a recovery operation where metadata is unavailable.
    pub fn recovery() -> Self {
        Self {
            doc_id: None,
            collection_id: None,
            creator: None,
            is_recovery: true,
        }
    }

    /// Check if any metadata is missing.
    pub fn is_incomplete(&self) -> bool {
        self.doc_id.is_none() || self.collection_id.is_none() || self.creator.is_none()
    }
}

/// Handler for merging incoming blocks.
///
/// Implement this trait in the database layer to handle CRDT merges.
/// The P2P layer calls this when a new block is received.
///
/// # Recovery Mode
///
/// During crash recovery, `handle_block` is called with `metadata.is_recovery = true`
/// and all metadata fields set to `None`. In this case, implementations MUST:
/// 1. Extract doc_id, collection_id, and creator from the block data itself
/// 2. Return an error if extraction fails (do NOT silently skip or merge incorrectly)
///
/// This ensures data integrity is maintained even after crashes.
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
    /// * `metadata` - Block metadata (may be incomplete during recovery)
    ///
    /// # Recovery Mode
    ///
    /// When `metadata.is_recovery` is true, the metadata fields will be `None`.
    /// The implementation MUST extract metadata from block_data in this case.
    /// Return an error if extraction fails - do not silently use defaults.
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
        metadata: BlockMetadata<'_>,
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
