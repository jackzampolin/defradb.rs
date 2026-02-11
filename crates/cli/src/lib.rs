//! DefraDB CLI library
//!
//! This library provides the core functionality for the DefraDB CLI.
//! It is primarily used by the `defra` binary but can also be used for testing.

pub mod acp_adapter;
pub mod cli;
pub mod collection_mgmt_adapter;
pub mod commands;
pub mod config;
pub mod doc_acp_adapter;
pub mod encrypted_index_adapter;
pub mod error;
pub mod index_adapter;
pub mod lens_adapter;
pub mod logging;
pub mod nac_adapter;
pub mod p2p_adapter;
pub mod schema_adapter;
pub mod txn_adapter;
pub mod view_adapter;
