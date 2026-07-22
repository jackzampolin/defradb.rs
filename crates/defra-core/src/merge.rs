//! Merge handler trait and outcome types for CRDT block merging.
//!
//! This module defines the interface between the P2P layer and the database
//! layer for applying CRDT merges.

use async_trait::async_trait;
use bytes::Bytes;
use cid::Cid;

use crate::thread_bounds::MaybeSendSync;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplicitReplayAuthorization {
    pub source_peer_id: String,
    pub target_peer_id: String,
    pub collection_id: String,
    pub authorizer_did: String,
    pub expires_at: u64,
}

/// Owned block data for batch processing.
///
/// Unlike `BlockMetadata` which borrows strings, this struct owns all data
/// so it can be collected into a batch and processed together.
#[derive(Debug, Clone)]
pub struct MergeBlock {
    pub cid: Cid,
    pub block_data: Bytes,
    pub doc_id: String,
    pub collection_id: String,
    pub creator: String,
    /// The actual transport peer that sent this block to us.
    pub sender_peer: Option<String>,
    /// True when the block arrived via the explicit replicator push path.
    pub is_explicit_replicator: bool,
    /// Capability-based explicit replay authorization carried by two-stream pushes.
    pub explicit_replay_authorization: Option<ExplicitReplayAuthorization>,
    /// Creator identity verified from the block's embedded signature.
    /// When present, this MUST be preferred over self-reported `creator`.
    pub verified_creator: Option<String>,
}

/// Metadata recovered from block contents during startup crash recovery.
///
/// Normal replication carries this metadata in the push message. Recovery only
/// has a CID and raw block bytes, so handlers must reconstruct these fields
/// from durable block contents before the block is merged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredBlockMetadata {
    pub doc_id: String,
    pub collection_id: String,
    pub creator: String,
    pub verified_creator: Option<String>,
}

impl RecoveredBlockMetadata {
    pub fn new(
        doc_id: impl Into<String>,
        collection_id: impl Into<String>,
        creator: impl Into<String>,
    ) -> Self {
        Self {
            doc_id: doc_id.into(),
            collection_id: collection_id.into(),
            creator: creator.into(),
            verified_creator: None,
        }
    }

    pub fn with_verified_creator(mut self, verified_creator: Option<String>) -> Self {
        self.verified_creator = verified_creator;
        self
    }

    pub fn is_complete(&self) -> bool {
        !self.doc_id.is_empty() && !self.collection_id.is_empty() && !self.creator.is_empty()
    }
}

/// Outcome of a merge operation.
///
/// Used by `MergeHandler::handle_block` to communicate the result.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MergeOutcome {
    /// Block was merged successfully into the database.
    Merged,
    /// Block was skipped (already applied, rejected by CRDT, etc.).
    Skipped {
        /// Human-readable reason for skipping.
        reason: String,
        /// Whether the skip is terminal for this CID.
        ///
        /// Terminal skips can be marked as merged. Retryable skips must remain
        /// unmerged so the block can be retried after policy/state changes.
        terminal: bool,
    },
    /// Block was deterministically rejected on its content: it will never
    /// succeed on replay. Not every unique-index conflict qualifies — a live
    /// conflict between two alive documents resolves deterministically at
    /// merge time instead (smallest docID wins) and converges rather than
    /// rejecting. This variant is for the cases that resolution can't heal
    /// (e.g. internal index-state inconsistency) and any other content-level
    /// rejection. Unlike a terminal skip, the receiver does not discharge the
    /// block as merged — it quarantines it instead, so local re-drive stops
    /// while a remote re-push can still retry.
    Rejected {
        /// Human-readable reason for the rejection.
        reason: String,
    },
}

impl MergeOutcome {
    /// Create a terminal skipped outcome with the given reason.
    pub fn terminal_skip(reason: impl Into<String>) -> Self {
        Self::Skipped {
            reason: reason.into(),
            terminal: true,
        }
    }

    /// Create a retryable skipped outcome with the given reason.
    pub fn retryable_skip(reason: impl Into<String>) -> Self {
        Self::Skipped {
            reason: reason.into(),
            terminal: false,
        }
    }

    /// Create a rejected outcome: a deterministic content rejection that will
    /// never succeed on replay. The receiver quarantines rather than
    /// discharging the block as merged.
    pub fn rejected(reason: impl Into<String>) -> Self {
        Self::Rejected {
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

    /// Returns true if this outcome indicates the block was deterministically rejected.
    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Rejected { .. })
    }

    /// Returns true if this outcome is a terminal skip.
    pub fn is_terminal_skip(&self) -> bool {
        matches!(self, Self::Skipped { terminal: true, .. })
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
    /// The actual transport peer that sent this block to us.
    pub sender_peer: Option<&'a str>,
    /// True when the block arrived via the explicit replicator push path.
    pub is_explicit_replicator: bool,
    /// Capability-based explicit replay authorization carried by two-stream pushes.
    pub explicit_replay_authorization: Option<ExplicitReplayAuthorization>,
    /// Creator identity verified from the block's embedded signature.
    /// Set by the merge handler after cryptographic verification.
    /// When present, this MUST be preferred over self-reported `creator`.
    pub verified_creator: Option<String>,
    /// Whether this is a recovery operation (block was stored but not merged before crash)
    pub is_recovery: bool,
    /// Whether this is a schema-level block (CollectionDefinition, FieldDefinition).
    ///
    /// Schema blocks are governed by node-level access control (NAC) rather than
    /// document-level ACP. Doc-level permission checks must be skipped for schema blocks.
    pub is_schema_block: bool,
}

impl<'a> BlockMetadata<'a> {
    /// Create metadata for a normal (non-recovery) merge operation.
    pub fn normal(
        doc_id: &'a str,
        collection_id: &'a str,
        creator: &'a str,
        sender_peer: Option<&'a str>,
        is_explicit_replicator: bool,
    ) -> Self {
        Self {
            doc_id: Some(doc_id),
            collection_id: Some(collection_id),
            creator: Some(creator),
            sender_peer,
            is_explicit_replicator,
            explicit_replay_authorization: None,
            verified_creator: None,
            is_recovery: false,
            is_schema_block: false,
        }
    }

    /// Create metadata for a schema-level block sync (CollectionDefinition, FieldDefinition).
    ///
    /// Schema blocks are fetched via Bitswap during normal P2P schema sync. They are
    /// controlled by node-level access control (NAC) rather than document-level ACP,
    /// so doc-level permission checks are skipped.
    pub fn schema_sync() -> Self {
        Self {
            doc_id: None,
            collection_id: None,
            creator: None,
            sender_peer: None,
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
            verified_creator: None,
            is_recovery: false,
            is_schema_block: true,
        }
    }

    /// Create metadata for a crash recovery operation where metadata is unavailable.
    ///
    /// Only use this during the startup recovery phase when processing blocks that
    /// were stored but not yet merged before a crash. Do NOT use for normal P2P
    /// block processing.
    pub fn recovery() -> Self {
        Self {
            doc_id: None,
            collection_id: None,
            creator: None,
            sender_peer: None,
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
            verified_creator: None,
            is_recovery: true,
            is_schema_block: false,
        }
    }

    /// Create crash-recovery metadata after a handler has extracted it.
    ///
    /// The `is_recovery` flag remains true so merge implementations can keep
    /// recovery-specific behavior while still receiving complete identifiers.
    pub fn recovered(
        doc_id: &'a str,
        collection_id: &'a str,
        creator: &'a str,
        verified_creator: Option<String>,
    ) -> Self {
        Self {
            doc_id: Some(doc_id),
            collection_id: Some(collection_id),
            creator: Some(creator),
            sender_peer: None,
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
            verified_creator,
            is_recovery: true,
            is_schema_block: false,
        }
    }

    pub fn with_explicit_replay_authorization(
        mut self,
        authorization: Option<ExplicitReplayAuthorization>,
    ) -> Self {
        self.explicit_replay_authorization = authorization;
        self
    }

    /// Returns the most trustworthy creator identity available.
    /// Prefers cryptographically verified identity over self-reported metadata.
    pub fn effective_creator(&self) -> Option<&str> {
        self.verified_creator.as_deref().or(self.creator)
    }

    pub fn explicit_replay_authorizer_for(&self, collection_id: &str) -> Option<&str> {
        self.explicit_replay_authorization
            .as_ref()
            .filter(|authorization| authorization.collection_id == collection_id)
            .and_then(|authorization| {
                (self.effective_creator() == Some(authorization.authorizer_did.as_str()))
                    .then_some(authorization.authorizer_did.as_str())
            })
    }

    pub fn allows_explicit_replay_for(&self, collection_id: &str) -> bool {
        if self.explicit_replay_authorization.is_some() {
            return self.explicit_replay_authorizer_for(collection_id).is_some();
        }

        self.is_explicit_replicator
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
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait MergeHandler: MaybeSendSync {
    /// Error type for merge operations
    type Error: std::error::Error + MaybeSendSync + 'static;

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
    /// * `Ok(MergeOutcome::Rejected { reason })` - Block was deterministically rejected
    /// * `Err(e)` - Merge failed
    async fn handle_block(
        &self,
        cid: &Cid,
        block_data: &[u8],
        metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error>;

    /// Validate explicit replay authorization before merge work starts.
    ///
    /// Implementations that honor `ExplicitReplayAuthorization` should reject
    /// authorizations that do not apply to this block's collection or creator.
    /// The replication loop invokes this for both single-block and batch paths
    /// before dispatching the block to merge code.
    async fn validate_authorization(
        &self,
        _authorization: Option<&ExplicitReplayAuthorization>,
        _block: &MergeBlock,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Recover complete metadata for a crash-recovery block.
    ///
    /// Returning `None` means the handler could not recover all required
    /// fields, and the replication recovery path will refuse to merge the block.
    async fn recover_block_metadata(
        &self,
        _cid: &Cid,
        _block_data: &[u8],
    ) -> Result<Option<RecoveredBlockMetadata>, Self::Error> {
        Ok(None)
    }

    /// Process a batch of blocks. Default impl calls handle_block() per block.
    ///
    /// Implementations can override this to process all blocks within a single
    /// shared transaction, reducing fsync overhead during P2P catch-up.
    async fn handle_block_batch(
        &self,
        blocks: &[MergeBlock],
    ) -> Vec<Result<MergeOutcome, Self::Error>> {
        let mut results = Vec::with_capacity(blocks.len());
        for block in blocks {
            if let Err(error) = self
                .validate_authorization(block.explicit_replay_authorization.as_ref(), block)
                .await
            {
                results.push(Err(error));
                continue;
            }

            let metadata = BlockMetadata::normal(
                &block.doc_id,
                &block.collection_id,
                &block.creator,
                block.sender_peer.as_deref(),
                block.is_explicit_replicator,
            )
            .with_explicit_replay_authorization(block.explicit_replay_authorization.clone());
            results.push(
                self.handle_block(&block.cid, &block.block_data, metadata)
                    .await,
            );
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_outcome_helpers() {
        let merged = MergeOutcome::Merged;
        assert!(merged.is_merged());
        assert!(!merged.is_skipped());

        let skipped = MergeOutcome::terminal_skip("already applied");
        assert!(!skipped.is_merged());
        assert!(skipped.is_skipped());
        assert!(skipped.is_terminal_skip());

        let retryable = MergeOutcome::retryable_skip("pending ACP");
        assert!(retryable.is_skipped());
        assert!(!retryable.is_terminal_skip());
    }

    #[test]
    fn effective_creator_prefers_verified() {
        let mut meta = BlockMetadata::normal("doc1", "col1", "did:key:SELF_REPORTED", None, false);
        meta.verified_creator = Some("did:key:VERIFIED_SIGNER".to_string());
        assert_eq!(meta.effective_creator(), Some("did:key:VERIFIED_SIGNER"));
    }

    #[test]
    fn effective_creator_falls_back_to_self_reported() {
        let meta = BlockMetadata::normal("doc1", "col1", "did:key:SELF_REPORTED", None, false);
        assert_eq!(meta.effective_creator(), Some("did:key:SELF_REPORTED"));
    }

    #[test]
    fn effective_creator_none_when_both_absent() {
        let meta = BlockMetadata::recovery();
        assert_eq!(meta.effective_creator(), None);
    }

    #[test]
    fn effective_creator_verified_only_no_self_reported() {
        let mut meta = BlockMetadata::recovery();
        meta.verified_creator = Some("did:key:VERIFIED".to_string());
        assert_eq!(meta.effective_creator(), Some("did:key:VERIFIED"));
    }

    #[test]
    fn recovered_metadata_is_complete_only_when_all_required_fields_exist() {
        assert!(RecoveredBlockMetadata::new("doc1", "col1", "did:key:creator").is_complete());
        assert!(!RecoveredBlockMetadata::new("", "col1", "did:key:creator").is_complete());
        assert!(!RecoveredBlockMetadata::new("doc1", "", "did:key:creator").is_complete());
        assert!(!RecoveredBlockMetadata::new("doc1", "col1", "").is_complete());
    }

    #[test]
    fn explicit_replay_authorizer_falls_back_to_self_reported_creator() {
        let meta = BlockMetadata::normal("doc1", "col1", "did:key:OWNER", None, false)
            .with_explicit_replay_authorization(Some(ExplicitReplayAuthorization {
                source_peer_id: "peer-a".to_string(),
                target_peer_id: "peer-b".to_string(),
                collection_id: "col1".to_string(),
                authorizer_did: "did:key:OWNER".to_string(),
                expires_at: 1,
            }));

        assert_eq!(
            meta.explicit_replay_authorizer_for("col1"),
            Some("did:key:OWNER")
        );
        assert!(meta.allows_explicit_replay_for("col1"));
    }

    #[test]
    fn explicit_replay_authorizer_prefers_verified_creator() {
        let mut meta = BlockMetadata::normal("doc1", "col1", "12D3KooWPEER", None, false);
        meta.verified_creator = Some("did:key:OWNER".to_string());
        meta = meta.with_explicit_replay_authorization(Some(ExplicitReplayAuthorization {
            source_peer_id: "peer-a".to_string(),
            target_peer_id: "peer-b".to_string(),
            collection_id: "col1".to_string(),
            authorizer_did: "did:key:OWNER".to_string(),
            expires_at: 1,
        }));

        assert_eq!(
            meta.explicit_replay_authorizer_for("col1"),
            Some("did:key:OWNER")
        );
        assert!(meta.allows_explicit_replay_for("col1"));
    }

    #[test]
    fn explicit_replay_authorizer_rejects_creator_mismatch() {
        let meta = BlockMetadata::normal("doc1", "col1", "did:key:OTHER", None, true)
            .with_explicit_replay_authorization(Some(ExplicitReplayAuthorization {
                source_peer_id: "peer-a".to_string(),
                target_peer_id: "peer-b".to_string(),
                collection_id: "col1".to_string(),
                authorizer_did: "did:key:OWNER".to_string(),
                expires_at: 1,
            }));

        assert_eq!(meta.explicit_replay_authorizer_for("col1"), None);
        assert!(!meta.allows_explicit_replay_for("col1"));
    }
}
