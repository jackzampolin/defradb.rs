//! Wire message types for DefraDB P2P protocol.
//!
//! Messages are CBOR encoded for wire compatibility with the Go implementation.
//! CBOR byte strings (major type 2) require serde_bytes annotations for Vec<u8>.
//!
//! # CBOR Serialization Guide
//!
//! Go's fxamacker/cbor has specific behaviors for `[]byte` that Rust must match:
//!
//! | Rust Type | Go Equivalent | Serializer | When to Use |
//! |-----------|---------------|------------|-------------|
//! | `Vec<u8>` | `[]byte` (non-nil) | `serde_bytes` | Required bytes field |
//! | `Option<Vec<u8>>` | `[]byte` (nullable) | `optional_bytes` | Optional signature field |
//! | `Vec<u8>` (may be empty) | `[]byte` (nil=null) | `nullable_bytes` | Public key (empty→CBOR null) |
//!
//! ## `serde_bytes`
//! Standard CBOR byte string encoding. Use for `Vec<u8>` fields that always contain data.
//! Example: CID bytes, block data.
//!
//! ## `optional_bytes`
//! Handles `Option<Vec<u8>>` where `None` → CBOR null, `Some(bytes)` → CBOR byte string.
//! Use for fields that are conditionally present (e.g., signature before signing).
//!
//! ## `nullable_bytes`
//! Handles `Vec<u8>` where empty `Vec` → CBOR null (matching Go's `nil []byte`).
//! Use for fields like pubkey where Go sends CBOR null for unset values.
//! WARNING: On round-trip, empty bytes become CBOR null which becomes empty bytes.

mod branchable;
mod car;
mod cbor;
mod docsync;
mod identity;
mod metadata;
mod pushlog;
mod se;
mod traits;

// Re-export all public types
pub use branchable::{BranchableSyncReply, BranchableSyncRequest};
pub use car::CarFetchRequest;
pub use docsync::{DocSyncItem, DocSyncReply, DocSyncRequest, MAX_DOC_IDS};
pub use identity::{IdentityRequest, IdentityResponse};
pub use metadata::MetaData;
pub use pushlog::{PushLogBroadcast, PushLogReply, PushLogRequest};
pub use se::{
    PushSEArtifactsReply, PushSEArtifactsRequest, QuerySEArtifactsReply, QuerySEArtifactsRequest,
    SEArtifact, SEFieldQuery,
};
pub use traits::Message;

// Re-export CBOR helpers for use by other modules that need wire compatibility
pub use cbor::{nullable_bytes, optional_bytes, vec_of_bytes};
