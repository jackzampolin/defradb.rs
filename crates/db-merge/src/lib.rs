//! P2P merge, broadcast, and document pushing for DefraDB.
//!
//! This crate contains all P2P-related database logic that was previously
//! behind `#[cfg(feature = "p2p")]` in the `db` crate.

pub mod acp_merge_handler;
#[cfg(not(target_arch = "wasm32"))]
pub mod broadcast_mutator;
pub mod browser_sync;
#[cfg(not(target_arch = "wasm32"))]
pub mod head_provider;
pub mod merge_handler;
#[cfg(not(target_arch = "wasm32"))]
pub mod peer_identity;
#[cfg(not(target_arch = "wasm32"))]
pub mod push_docs;
pub mod push_docs_common;
#[cfg(not(target_arch = "wasm32"))]
mod push_docs_creator;
#[cfg(not(target_arch = "wasm32"))]
mod push_docs_replay;
#[cfg(not(target_arch = "wasm32"))]
pub mod push_docs_transport;
#[cfg(not(target_arch = "wasm32"))]
pub mod replication;
pub mod se;
#[cfg(not(target_arch = "wasm32"))]
pub mod se_key_handle;
#[cfg(not(target_arch = "wasm32"))]
pub mod se_query_transport;
#[cfg(not(target_arch = "wasm32"))]
pub mod txn_broadcaster;

pub use acp_merge_handler::{AcpMergeError, AcpMergeHandler};
#[cfg(not(target_arch = "wasm32"))]
pub use broadcast_mutator::{BroadcastMutator, BroadcastSeOptions, SeArtifactRepusher};
pub use browser_sync::{
    BrowserSyncDocumentRef, BrowserSyncEngine, BrowserSyncError, ValidatedBrowserSyncDocument,
};
#[cfg(not(target_arch = "wasm32"))]
pub use head_provider::DbHeadProvider;
pub use merge_handler::{DbMergeHandler, MergeError};
#[cfg(not(target_arch = "wasm32"))]
pub use peer_identity::{
    create_peer_to_did_mapper, peer_id_to_did, public_key_to_did, PeerIdentityError,
};
#[cfg(not(target_arch = "wasm32"))]
pub use push_docs::{
    push_existing_docs, push_existing_docs_with_config, retry_collection_commit, retry_doc,
    PushExistingDocsSeOptions,
};
#[cfg(not(target_arch = "wasm32"))]
pub use push_docs_replay::ReplayPushConfig;
#[cfg(not(target_arch = "wasm32"))]
pub use push_docs_transport::{
    push_existing_docs_via_transport, push_existing_docs_via_transport_with_config,
    retry_collection_commit_via_transport, retry_doc_via_transport,
};
#[cfg(not(target_arch = "wasm32"))]
pub use replication::{
    attach_failure_channel, create_acp_merge_handler, create_broadcast_mutator,
    create_head_provider, create_merge_handler, create_replication_stack,
    load_document_head_blocks, load_persisted_collections, ReplicationStack,
};
pub use se::{
    fetch_doc_ids, generate_doc_artifacts, generate_field_artifact, store_artifacts, FieldQuery,
};
#[cfg(not(target_arch = "wasm32"))]
pub use se::{FieldValueQuery, SECoordinator};
#[cfg(not(target_arch = "wasm32"))]
pub use se_key_handle::{
    empty_se_key_handle, filled_se_key_handle, load_se_key, store_se_key, SeKeyHandle,
    SeKeyMaterial,
};
#[cfg(not(target_arch = "wasm32"))]
pub use se_query_transport::DbMergeSeQueryTransport;
#[cfg(not(target_arch = "wasm32"))]
pub use txn_broadcaster::SyncTxnBroadcaster;
