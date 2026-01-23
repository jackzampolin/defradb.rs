
//! DefraDB CLI library
//!
//! This library provides the core functionality for the DefraDB CLI.
//! It is primarily used by the `defra` binary but can also be used for testing.

pub mod cli;
pub mod commands;
pub mod config;
pub mod error;
pub mod logging;
pub mod p2p_adapter;
pub mod schema_adapter;
