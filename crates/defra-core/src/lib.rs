//! Core types and traits for DefraDB
//!
//! This crate defines the fundamental types, traits, and interfaces
//! used throughout the DefraDB ecosystem.

pub mod block;
pub mod collection;
pub mod document;
pub mod error;
pub mod store;
pub mod transaction;
pub mod types;

pub use block::{
    Block, CollectionDeltaPayload, CompositeDeltaPayload, CounterDeltaPayload, CrdtDelta, DAGLink,
    Encryption, LwwDeltaPayload, Signature, SignatureHeader, SignatureType, DAG_CBOR_CODEC,
    SHA2_256_CODE,
};
pub use error::{Error, Result};

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
