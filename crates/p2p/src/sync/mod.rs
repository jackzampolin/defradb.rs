
//! P2P synchronization module for DefraDB.
//!
//! This module provides block synchronization between DefraDB peers.
//! It handles:
//! - Receiving blocks from the network (via PushLog messages)
//! - Storing blocks in the blockstore with merge tracking
//! - Applying CRDT merges to integrate remote changes
//! - Broadcasting local changes to the network
//!
//! # Architecture
//!
//! The sync flow follows the Go implementation:
//!
//! ```text
//! Network (PubSub/Replicator)
//!         ↓
//! PushLogRequest received
//!         ↓
//! SyncManager.process_pushlog()
//!         ↓
//! ┌───────┴───────┐
//! │ Process Queue │  ← Deduplicates concurrent syncs for same CID
//! └───────┬───────┘
//!         ↓
//! Check if already merged (blockstore.is_merged())
//!         ↓ (if not merged)
//! Store block in blockstore
//!         ↓
//! Apply CRDT merge
//!         ↓
//! Mark as merged
//!         ↓
//! Broadcast to network (optional)
//! ```

mod broadcaster;
mod collection_store;
mod coordinator;
mod dag_sync;
mod manager;
mod merge;
mod peer_state;
mod queue;
mod replication;

pub use broadcaster::{BroadcastResult, Broadcaster};
pub use collection_store::{NoOpCollectionStorage, P2PCollectionStorage, P2PCollectionStore};
pub use coordinator::{LoadReplicatorsResult, SetReplicatorResult, SyncCoordinator};
pub use dag_sync::{DagSync, DagSyncConfig, DagSyncState, NeedsFetchData, SyncPlan};
pub use manager::{SyncConfig, SyncEvent, SyncManager};
pub use merge::{BlockMetadata, MergeHandler, MergeOutcome};
pub use peer_state::{PeerStateTracker, PeerStats};
pub use queue::ProcessQueue;
pub use replication::{ReplicationConfig, ReplicationLoop, ReplicationResult};
