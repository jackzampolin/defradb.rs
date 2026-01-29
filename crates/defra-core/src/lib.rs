//! Core types and traits for DefraDB
//!
//! This crate defines the fundamental types, traits, and interfaces
//! used throughout the DefraDB ecosystem.

pub mod block;
pub mod collection;
pub mod document;
pub mod error;
pub mod ipld;
pub mod store;
pub mod thread_bounds;
pub mod transaction;
pub mod types;

pub use block::{
    Block, CollectionDefinitionDeltaPayload, CollectionDeltaPayload, CompositeDeltaPayload,
    CounterDeltaPayload, CrdtDelta, DAGLink, Encryption, FieldDefinitionDeltaPayload,
    LwwDeltaPayload, Signature, SignatureHeader, SignatureType, DAG_CBOR_CODEC, SHA2_256_CODE,
};
pub use error::{Error, Result};
pub use ipld::{collect_block_links, extract_links, walk_ipld, IpldVisitor};

/// Version information for DefraDB
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
    }
}
