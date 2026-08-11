//! Core types and traits for DefraDB
//!
//! This crate defines the fundamental types, traits, and interfaces
//! used throughout the DefraDB ecosystem.

pub mod action;
pub mod batch_signing;
pub mod block;
pub mod block_delta;
pub mod block_signature;
pub mod browser_sync;
pub mod cbor;
pub mod collection;
pub mod current_identity;
pub mod dac_bypass;
pub mod doc_id;
pub mod document;
pub mod encryption;
pub mod error;
pub mod ipld;
pub mod lens_block;
pub mod merge;
pub mod signing;
pub mod store;
pub mod thread_bounds;
pub mod transaction;
pub mod types;

pub use action::{Action, ActionExecution, ActionStatus};
pub use block::{
    Block, CollectionDefinitionDeltaPayload, CollectionDeltaPayload, CollectionSetDeltaPayload,
    CompositeDeltaPayload, CounterDeltaPayload, CrdtDelta, DAGLink, DocumentStatus, Encryption,
    FieldDefinitionDeltaPayload, LwwDeltaPayload, Signature, SignatureHeader, SignatureType,
    DAG_CBOR_CODEC, SHA2_256_CODE,
};
pub use collection::collection_short_id;
pub use encryption::EncryptionKey;
pub use error::{Error, Result};
pub use ipld::{collect_block_links, extract_links, walk_ipld, IpldVisitor};
pub use lens_block::{
    build_lens_ipld_blocks, is_lens_block, CidBlock, LensConfigBlock, LensKeyValue,
    LensModuleBlock, LensWasmBlock,
};
pub use signing::SigningKeyType;

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
