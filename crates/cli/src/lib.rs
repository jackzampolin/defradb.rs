//! DefraDB CLI library
//!
//! This library provides the core functionality for the DefraDB CLI.
//! It is primarily used by the `defra` binary but can also be used for testing.

pub(crate) mod acp_adapter;
pub(crate) mod backup_adapter;
pub(crate) mod block_adapter;
pub mod cli;
pub(crate) mod collection_mgmt_adapter;
pub mod commands;
pub mod config;
pub(crate) mod doc_acp_adapter;
pub(crate) mod dump_adapter;
pub(crate) mod encrypted_index_adapter;
pub mod error;
pub(crate) mod index_adapter;
#[cfg(feature = "iroh")]
pub(crate) mod iroh_p2p_adapter;
pub(crate) mod lens_adapter;
pub mod logging;
#[allow(dead_code)]
pub(crate) mod nac_adapter;
#[allow(dead_code)]
pub(crate) mod p2p_adapter;
pub(crate) mod schema_adapter;
pub(crate) mod sourcehub_acp_adapter;
#[allow(dead_code)]
pub(crate) mod transport_doc_pusher;
#[allow(dead_code)]
pub(crate) mod transport_version_syncer;
pub(crate) mod txn_adapter;
pub(crate) mod version_syncer;
pub(crate) mod view_adapter;
