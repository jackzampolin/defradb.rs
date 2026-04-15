//! Core traits for CRDT operations

use async_trait::async_trait;
use defra_core::{types::DocId, Result};
use std::any::Any;
use storage::corekv::MaybeSendSync;
use storage::{Reader, ReaderWriter};

/// Result of a merge operation, providing visibility into what happened
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MergeResult {
    /// Delta was applied and state was updated
    Applied,
    /// Delta was rejected because its priority was lower than current
    RejectedLowerPriority { current: u64, incoming: u64 },
    /// Delta was rejected due to lexicographic tie-breaking (same priority, current value wins)
    RejectedTieBreak,
    /// Delta was skipped because it was already applied (idempotency via nonce)
    SkippedAlreadyApplied { nonce: i64 },
}

impl MergeResult {
    /// Returns true if the delta was applied
    pub fn was_applied(&self) -> bool {
        matches!(self, MergeResult::Applied)
    }

    /// Returns true if the delta was rejected (not applied due to conflict resolution)
    pub fn was_rejected(&self) -> bool {
        matches!(
            self,
            MergeResult::RejectedLowerPriority { .. } | MergeResult::RejectedTieBreak
        )
    }

    /// Returns true if the delta was skipped due to idempotency
    pub fn was_skipped(&self) -> bool {
        matches!(self, MergeResult::SkippedAlreadyApplied { .. })
    }
}

/// Context for CRDT operations
pub struct Context {
    /// Document identifier
    pub doc_id: DocId,
    /// Schema version
    pub schema_version: String,
    /// When true, skip storage reads (document doesn't exist yet)
    pub is_create: bool,
}

/// Delta trait - represents an incremental change to a CRDT
///
/// All CRDT deltas must implement this trait to support priority-based
/// conflict resolution.
pub trait Delta: MaybeSendSync {
    /// Get the priority of this delta for conflict resolution
    fn get_priority(&self) -> u64;

    /// Set the priority of this delta
    fn set_priority(&mut self, priority: u64);

    /// Get document ID associated with this delta
    fn doc_id(&self) -> &[u8];

    /// Get field name this delta applies to
    fn field_name(&self) -> &str;

    /// Get schema version ID
    fn schema_version_id(&self) -> &str;

    /// Downcast to concrete type (for type-safe delta handling)
    fn as_any(&self) -> &dyn Any;
}

/// ReplicatedData trait - represents a CRDT that can merge deltas
///
/// This is the core trait for all CRDT implementations in DefraDB.
/// It defines how to merge incoming deltas and manage state.
///
/// # Storage Pattern
///
/// CRDTs do not own storage - instead, they receive a mutable reference
/// to a `ReaderWriter` (typically a transaction) for each operation.
/// This pattern:
/// - Matches Go DefraDB's design where CRDTs receive `corekv.ReaderWriter`
/// - Allows the caller to control transaction lifecycle (commit/rollback)
/// - Enables multiple CRDT operations within a single transaction
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait ReplicatedData: MaybeSendSync {
    /// Merge an incoming delta into this CRDT
    ///
    /// # Arguments
    /// * `rw` - Storage transaction for reading/writing state
    /// * `ctx` - Context for the operation
    /// * `delta` - The delta to merge
    ///
    /// # Returns
    /// * `Ok(MergeResult)` indicating what happened (applied, rejected, or skipped)
    /// * `Err(...)` if merge failed (invalid delta, storage error, etc.)
    ///
    /// # Transaction Semantics
    ///
    /// The caller is responsible for committing or rolling back the transaction.
    /// Multiple merge calls can share the same transaction for atomic updates.
    async fn merge(
        &self,
        rw: &mut dyn ReaderWriter,
        ctx: &Context,
        delta: &dyn Delta,
    ) -> Result<MergeResult>;
}

/// Trait for CRDTs that support value retrieval
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait ValueReader: ReplicatedData {
    /// Get the current value from storage
    async fn value(&self, reader: &dyn Reader) -> Result<Vec<u8>>;
}

/// Trait for CRDTs that support priority retrieval
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait PriorityReader: ReplicatedData {
    /// Get the current priority from storage
    async fn priority(&self, reader: &dyn Reader) -> Result<u64>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_creation() {
        let ctx = Context {
            doc_id: DocId::new_unchecked("bae-test"),
            schema_version: "v1".to_string(),
            is_create: false,
        };
        assert_eq!(ctx.doc_id.as_str(), "bae-test");
        assert_eq!(ctx.schema_version, "v1");
    }
}
