//! P2P synchronization module for DefraDB.
//!
//! This module provides block synchronization between DefraDB peers.
//! It handles:
//! - Receiving blocks from the network (via PushLog messages)
//! - Storing blocks in the blockstore with merge tracking
//! - Applying CRDT merges to integrate remote changes
//! - Broadcasting local changes to the network

mod broadcaster;
pub(crate) mod car;
mod collection_store;
mod coordinator;
mod dag_sync;
mod head_provider;
mod manager;
mod merge;
mod peer_state;
mod queue;
pub(crate) mod rate_limiter;
mod replication;

pub use broadcaster::{BroadcastResult, Broadcaster};
pub use collection_store::{NoOpCollectionStorage, P2PCollectionStorage, P2PCollectionStore};
#[cfg(feature = "iroh-transport")]
pub use coordinator::IrohSyncCoordinator;
pub use coordinator::{
    CreateReplicatorResult, Libp2pSyncCoordinator, LoadReplicatorsResult, PushFailure,
    SyncCoordinator,
};
pub use dag_sync::{DagSync, DagSyncConfig, DagSyncState, NeedsFetchData, SyncPlan};
pub use head_provider::{DocumentHeadProvider, NoOpHeadProvider};
pub use manager::{
    SyncConfig, SyncEvent, SyncManager, DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
    DEFAULT_MAX_CONCURRENT_PUSH_TASKS, DEFAULT_RATE_LIMIT_BURST, DEFAULT_RATE_LIMIT_RATE,
};
pub use merge::{BlockMetadata, MergeBlock, MergeHandler, MergeOutcome};
pub use peer_state::{PeerStateTracker, PeerStats};
pub use queue::ProcessQueue;
pub use replication::{recover_unmerged, ReplicationConfig, ReplicationLoop, ReplicationResult};
