//! Replication loop for processing sync events and executing CRDT merges.
//!
//! The replication loop is the bridge between the P2P layer and the database.
//! It consumes SyncEvents, loads blocks from the blockstore, delegates merge
//! operations to the database layer, and marks blocks as merged.
//!
//! # Architecture
//!
//! ```text
//! SyncManager emits SyncEvent::BlockReceived
//!         ↓
//! ReplicationLoop receives event
//!         ↓
//! Load block from blockstore
//!         ↓
//! MergeHandler::handle_block() [database layer]
//!         ↓
//! SyncCoordinator::mark_as_merged()
//! ```

mod config;
mod handlers;
mod loop_runner;
mod recovery;
mod result;

pub use config::ReplicationConfig;
pub use loop_runner::ReplicationLoop;
pub use recovery::recover_unmerged;
pub use result::ReplicationResult;

#[cfg(test)]
mod tests;
