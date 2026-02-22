//! Searchable Encryption (SE) module.
//!
//! This module provides the core primitives for searchable encryption in DefraDB.
//! It enables equality queries on encrypted fields without revealing the actual data
//! to the remote storage nodes.
//!
//! # Architecture
//!
//! Searchable encryption in DefraDB uses a deterministic HMAC-based scheme:
//!
//! 1. **Tag Generation**: When a document is created/updated, search tags are generated
//!    for each indexed field using `generate_equality_tag`. The tag is computed as:
//!    ```text
//!    HMAC-SHA256(key, "eq:identity:collection:field" || value)[:16]
//!    ```
//!
//! 2. **Artifact Replication**: Tags are packaged into `Artifact` structs and pushed
//!    to replicator nodes. The producer stores no artifacts locally.
//!
//! 3. **Query Processing**: At query time, the same tag is regenerated for the search
//!    value and sent to replicators. They return matching document IDs without ever
//!    seeing the actual data.
//!
//! # Security Properties
//!
//! - **Deterministic**: Same input always produces same tag (enables search)
//! - **Identity-isolated**: Different users get different tags (multi-tenant safety)
//! - **Collision-resistant**: 128-bit tags provide strong security margin
//!
//! # Limitations
//!
//! - **Frequency analysis**: Repeated values produce the same tag, revealing frequency
//! - **Equality only**: Currently only supports exact match queries
//!
//! # Example
//!
//! ```
//! use crypto::se::{generate_equality_tag, Artifact, SEARCH_TAG_SIZE};
//!
//! // Generate a search tag for a field value
//! let key = [0u8; 32]; // In practice, use a proper SE key
//! let identity = b"user_pubkey";
//! let collection = "users_v1";
//! let field = "age";
//! let value = &[0x88, 0x15]; // Encoded integer 21
//!
//! let tag = generate_equality_tag(&key, identity, collection, field, value).unwrap();
//!
//! // Create an artifact for replication
//! let artifact = Artifact::new(collection, "bae123", field, tag.to_vec());
//! ```
//!
//! # Go Compatibility
//!
//! This module is designed to be byte-for-byte compatible with Go DefraDB's
//! `internal/se/core/` package. The same inputs must produce identical outputs
//! for cross-implementation interoperability.

mod artifact;
mod tag;

pub use artifact::{Artifact, ArtifactBatch, ArtifactType, OperationType};
pub use tag::{generate_equality_tag, generate_equality_tag_str, SEARCH_TAG_SIZE, SE_KEY_LEN};
