//! P2P synchronization module for DefraDB.
//!
//! This module provides block synchronization between DefraDB peers.
//! It handles:
//! - Receiving blocks from the network (via PushLog messages)
//! - Storing blocks in the blockstore with merge tracking
//! - Applying CRDT merges to integrate remote changes
//! - Broadcasting local changes to the network

use std::time::Duration;

mod broadcast_coalescer;
mod broadcaster;
pub(crate) mod car;
mod collection_store;
mod coordinator;
mod dag_sync;
mod event_dispatcher;
mod head_provider;
mod manager;
mod merge;
mod peer_state;
pub mod pending_store;
mod push_backlog;
pub(crate) mod push_fanout_coalescer;
mod queue;
pub(crate) mod rate_limiter;
mod replication;

pub use broadcaster::{BroadcastResult, Broadcaster};
pub use collection_store::{NoOpCollectionStorage, P2PCollectionStorage, P2PCollectionStore};
#[cfg(feature = "iroh-transport")]
pub use coordinator::IrohSyncCoordinator;
#[cfg(feature = "libp2p-transport")]
pub use coordinator::Libp2pSyncCoordinator;
pub use coordinator::{
    CreateReplicatorResult, HeadAckFence, HeadHintCarAuthority, HeadHintCarGrant,
    LoadReplicatorsResult, PushFailure, SyncCoordinator, SyncShutdownHandle, SyncStatus,
};
pub use dag_sync::{DagSync, DagSyncConfig, DagSyncState, NeedsFetchData, SyncPlan};
pub(crate) use event_dispatcher::classify_p2p_event;
pub(crate) use event_dispatcher::DispatchDiagnostics;
pub use event_dispatcher::{DispatchAdmission, DispatchClass, DispatchEvent, DispatchSnapshot};
pub use head_provider::{DocumentHeadProvider, NoOpHeadProvider};
#[cfg(any(feature = "libp2p-transport", feature = "iroh-transport"))]
pub(crate) use manager::record_gossip_decode_failure_sample;
pub use manager::{
    default_rate_limit_backoff, GossipDecodeFailureSample, GossipTransport, SyncConfig,
    SyncDiagnostics, SyncDiagnosticsSnapshot, SyncEvent, SyncManager,
    DEFAULT_MAX_ACTIVE_PUSHES_PER_PEER, DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
    DEFAULT_MAX_CONCURRENT_PUSH_TASKS, DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
    DEFAULT_MAX_PENDING_DAGS, DEFAULT_PUSH_QUEUE_BYTE_CAPACITY, DEFAULT_PUSH_QUEUE_CAPACITY,
    DEFAULT_PUSH_SEND_TIMEOUT, DEFAULT_RATE_LIMIT_BACKOFF_SECS, DEFAULT_RATE_LIMIT_BURST,
    DEFAULT_RATE_LIMIT_RATE,
};
pub use merge::{BlockMetadata, MergeBlock, MergeHandler, MergeOutcome, RecoveredBlockMetadata};
pub use peer_state::{PeerStateTracker, PeerStats};
pub use pending_store::{
    PendingDagStorage, PendingDagStore, PersistedPendingDag, PersistedQuarantinedDag,
    PersistedReplayAuthorization,
};
pub use push_backlog::{
    EnqueueOutcome, PeerBacklogSnapshot, PushBacklog, PushBacklogSnapshot, PushJobSpec,
};
pub use queue::ProcessQueue;
pub use replication::{recover_unmerged, ReplicationConfig, ReplicationLoop, ReplicationResult};

/// Cadence for draining persisted push retries.
pub const PERSISTED_RETRY_SWEEP_INTERVAL: Duration = Duration::from_secs(2);
