//! P2P merge, broadcast, and document pushing for DefraDB.
//!
//! This crate contains all P2P-related database logic that was previously
//! behind `#[cfg(feature = "p2p")]` in the `db` crate.

pub mod acp_merge_handler;
pub mod broadcast_mutator;
pub mod head_provider;
pub mod merge_handler;
pub mod peer_identity;
pub mod push_docs;
pub mod push_docs_common;
mod push_docs_creator;
mod push_docs_replay;
pub mod push_docs_transport;
pub mod replication;
pub mod se;

// Re-export primary types
pub use acp_merge_handler::{AcpMergeError, AcpMergeHandler};
pub use broadcast_mutator::BroadcastMutator;
pub use head_provider::DbHeadProvider;
pub use merge_handler::{DbMergeHandler, MergeError};
pub use peer_identity::{
    create_peer_to_did_mapper, peer_id_to_did, public_key_to_did, PeerIdentityError,
};
pub use push_docs::{
    push_existing_docs, push_existing_docs_with_config, retry_doc, PushExistingDocsSeOptions,
};
pub use push_docs_replay::ReplayPushConfig;
pub use push_docs_transport::{
    push_existing_docs_via_transport, push_existing_docs_via_transport_with_config,
    retry_doc_via_transport,
};
pub use replication::{
    attach_failure_channel, create_acp_merge_handler, create_broadcast_mutator,
    create_head_provider, create_merge_handler, create_replication_stack,
    load_document_head_blocks, load_persisted_collections, ReplicationStack,
};
pub use se::{
    fetch_doc_ids, generate_doc_artifacts, generate_field_artifact, store_artifacts, FieldQuery,
    FieldValueQuery, SECoordinator,
};
