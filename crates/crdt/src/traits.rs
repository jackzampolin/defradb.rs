//! Core traits for CRDT operations

use async_trait::async_trait;
use defra_core::{types::DocId, Result};
use std::any::Any;

/// Context for CRDT operations
pub struct Context {
    /// Document identifier
    pub doc_id: DocId,
    /// Schema version
    pub schema_version: String,
}

/// Delta trait - represents an incremental change to a CRDT
///
/// All CRDT deltas must implement this trait to support priority-based
/// conflict resolution.
pub trait Delta: Send + Sync {
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
#[async_trait]
pub trait ReplicatedData: Send + Sync {
    /// Merge an incoming delta into this CRDT
    ///
    /// # Arguments
    /// * `ctx` - Context for the operation
    /// * `delta` - The delta to merge
    ///
    /// # Returns
    /// * `Ok(())` if merge successful
    /// * `Err(...)` if merge failed (invalid delta, storage error, etc.)
    async fn merge(&mut self, ctx: &Context, delta: &dyn Delta) -> Result<()>;

    /// Get the headstore key prefix for this CRDT
    ///
    /// The headstore tracks the latest CID for each document.
    /// This method returns the key prefix used to store head information.
    fn headstore_prefix(&self) -> Vec<u8>;
}

/// Trait for CRDTs that support value retrieval
#[async_trait]
pub trait ValueReader: ReplicatedData {
    /// Get the current value from storage
    async fn value(&self) -> Result<Vec<u8>>;
}

/// Trait for CRDTs that support priority retrieval
#[async_trait]
pub trait PriorityReader: ReplicatedData {
    /// Get the current priority from storage
    async fn priority(&self) -> Result<u64>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_creation() {
        let ctx = Context {
            doc_id: DocId::new("bae-test"),
            schema_version: "v1".to_string(),
        };
        assert_eq!(ctx.doc_id.as_str(), "bae-test");
        assert_eq!(ctx.schema_version, "v1");
    }
}
