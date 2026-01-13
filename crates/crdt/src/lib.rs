//! CRDT (Conflict-free Replicated Data Types) implementation for DefraDB
//!
//! This crate implements delta-state CRDTs with Merkle-DAG integration for
//! distributed conflict resolution.
//!
//! ## CRDT Types
//!
//! - **LWW Register**: Last-Write-Wins register for field-level conflict resolution
//! - **Counter**: PN-Counter for increment/decrement operations
//! - **Composite**: Document-level CRDT composition
//!
//! ## Key Concepts
//!
//! - **Priority**: Timestamp-based ordering for deterministic conflict resolution
//! - **Delta**: Incremental state changes for efficient synchronization
//! - **Merge**: Conflict resolution algorithm for concurrent updates

pub mod composite;
pub mod counter;
pub mod lww;
pub mod priority;
pub mod traits;

pub use composite::{CompositeDAG, CompositeDelta};
pub use counter::{Counter, CounterDelta};
pub use lww::{Lww, LwwDelta};
pub use priority::{decode_priority, encode_priority};
pub use traits::{Delta, ReplicatedData};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_roundtrip() {
        let priority = 12345u64;
        let encoded = encode_priority(priority);
        let decoded = decode_priority(&encoded).unwrap();
        assert_eq!(priority, decoded);
    }
}
