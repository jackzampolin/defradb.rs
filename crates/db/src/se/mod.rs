//! Searchable Encryption (SE) Coordinator
//!
//! This module provides searchable encryption support for DefraDB. It enables
//! equality queries on encrypted fields without revealing the actual values
//! to remote storage nodes.
//!
//! # Architecture
//!
//! The SE system uses a producer-consumer model:
//!
//! - **Producer** (document creator): Generates artifacts when documents are
//!   created/updated, pushes them to replicators, does NOT store locally.
//!
//! - **Consumer** (replicator): Receives and stores artifacts, responds to
//!   search queries from producers.
//!
//! # Artifact Flow
//!
//! 1. Document created/updated
//! 2. Producer generates search tags for encrypted-indexed fields
//! 3. Artifacts pushed to replicator nodes via P2P
//! 4. At query time, producer regenerates tag and queries replicators
//! 5. Replicators return matching document IDs
//!
//! # Go Compatibility
//!
//! This implementation matches Go DefraDB's `internal/se/` package.

mod artifact_gen;
mod coordinator;
pub mod receiver;
mod storage;
pub mod validate;

pub use artifact_gen::{generate_doc_artifacts, generate_field_artifact};
pub use coordinator::{FieldValueQuery, SECoordinator};
pub use receiver::{deserialize_artifacts, receive_and_store};
pub use storage::{fetch_doc_ids, store_artifacts, FieldQuery};
pub use validate::validate_artifact;
