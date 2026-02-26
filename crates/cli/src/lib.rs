//! DefraDB CLI library
//!
//! This library provides the core functionality for the DefraDB CLI.
//! It is primarily used by the `defra` binary but can also be used for testing.

pub mod acp_adapter;
pub mod backup_adapter;
pub mod block_adapter;
pub mod cli;
pub mod collection_mgmt_adapter;
pub mod commands;
pub mod config;
pub mod doc_acp_adapter;
pub mod dump_adapter;
pub mod encrypted_index_adapter;
pub mod error;
pub mod hub_rs_acp_adapter;
pub mod index_adapter;
#[cfg(feature = "iroh")]
pub mod iroh_p2p_adapter;
pub mod lens_adapter;
pub mod logging;
pub mod nac_adapter;
pub mod p2p_adapter;
pub mod schema_adapter;
pub mod sourcehub_acp_adapter;
pub mod transport_doc_pusher;
pub mod transport_version_syncer;
pub mod txn_adapter;
pub mod version_syncer;
pub mod view_adapter;
