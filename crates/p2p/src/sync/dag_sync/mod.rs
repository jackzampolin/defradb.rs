//! DAG synchronization using Bitswap.
//!
//! This module provides DAG sync capabilities that mirror Go DefraDB's `syncDAG`
//! pattern. When a block is received via PushLog, this module:
//! 1. Stores the block in the local blockstore
//! 2. Extracts all links from the block
//! 3. Fetches missing linked blocks via Bitswap
//! 4. Recursively syncs linked blocks
//!
//! # Go Implementation Reference
//!
//! Go's `syncDAG` in `internal/db/p2p/sync_dag.go`:
//! - Uses a LinkSystem for IPLD storage
//! - Recursively loads all linked blocks
//! - Concurrent fetching with context cancellation
//!
//! # Example
//!
//! ```ignore
//! use p2p::sync::DagSync;
//!
//! let dag_sync = DagSync::new(blockstore, peer_state);
//!
//! // When receiving a PushLog message with a block:
//! let missing = dag_sync.get_missing_links(&block).await?;
//! if !missing.is_empty() {
//!     // Start Bitswap sync for missing blocks
//!     let query_id = behaviour.bitswap_sync(block_cid, peers, missing.iter().cloned());
//! }
//! ```

mod config;
mod plan;
mod state;
mod sync;

pub use config::DagSyncConfig;
pub use plan::{NeedsFetchData, SyncPlan};
pub use state::DagSyncState;
pub use sync::DagSync;
