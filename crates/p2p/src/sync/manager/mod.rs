//! Sync manager for coordinating P2P block synchronization.
//!
//! The SyncManager handles:
//! - Processing incoming PushLog messages
//! - Storing blocks in the blockstore with merge tracking
//! - Emitting events for database-layer CRDT merging
//! - Broadcasting local changes to the network
//!
//! # Architecture Note
//!
//! The P2P layer handles block storage and network coordination.
//! The actual CRDT merge is performed by the database layer.
//! This matches Go's architecture where p2p calls db.Merge().

mod config;
pub(crate) mod diagnostics;
mod events;
pub(crate) mod links;
mod pending;
mod process;

pub use config::{
    default_rate_limit_backoff, SyncConfig, DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
    DEFAULT_MAX_CONCURRENT_PUSH_TASKS, DEFAULT_PUSH_SEND_TIMEOUT, DEFAULT_RATE_LIMIT_BACKOFF_SECS,
    DEFAULT_RATE_LIMIT_BURST, DEFAULT_RATE_LIMIT_RATE,
};
pub(crate) use diagnostics::record_gossip_decode_failure_sample;
pub use diagnostics::{
    GossipDecodeFailureSample, GossipTransport, SyncDiagnostics, SyncDiagnosticsSnapshot,
};
pub use events::SyncEvent;
pub use process::SyncManager;
