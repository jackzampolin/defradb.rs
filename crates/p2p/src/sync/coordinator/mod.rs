//! Sync coordinator for DefraDB P2P synchronization.
//!
//! The coordinator ties together:
//! - P2P transport for network communication
//! - SyncManager for block storage and merge tracking
//! - Broadcaster for publishing updates
//!
//! # Architecture
//!
//! ```text
//! Database Layer
//!       ↓
//! SyncCoordinator<B, T>
//!       ├── Broadcaster<T> (publish updates)
//!       ├── SyncManager (store blocks, emit events)
//!       └── Event loop (receive TransportEvents)
//!       ↓
//! T: P2PTransport (network)
//! ```
//!
//! # Security Model: Two-Level Access Control
//!
//! The P2P sync layer implements **collection-level** access control only.
//! **Document-level** ACP is the responsibility of the database merge layer.
//!
//! ## Collection-Level (P2P Layer)
//!
//! - Enforced via `check_access()` before processing any sync message
//! - A peer must be registered as a replicator for a collection
//! - Unauthorized peers cannot push documents to collections they don't replicate
//!
//! ## Document-Level (Database Merge Layer)
//!
//! - The P2P layer provides creator/doc_id/collection_id in `SyncEvent::BlockReceived`
//! - The database merge handler should:
//!   1. Identify the creator's DID (from the signed block or peer mapping)
//!   2. Check if the creator has UPDATE permission on the document
//!   3. If permission denied, log and skip the merge (don't crash)
//!
//! This two-level model allows:
//! - Fast collection-level filtering at the network layer
//! - Fine-grained document-level checks at the merge layer
//! - CRDT convergence (eventually consistent merge, possibly with rejected updates)

mod access;
mod accessors;
mod broadcast;
mod constructor;
pub(crate) mod dag_fetcher;
mod event_handler;
mod replicators;
mod result_types;
mod subscriptions;

pub use result_types::{CreateReplicatorResult, LoadReplicatorsResult};

use std::sync::Arc;

use blockstore::Blockstore;

use crate::bitswap::{AccessMode, ReplicatorRegistry};
use crate::transport::P2PTransport;

use super::broadcaster::Broadcaster;
use super::collection_store::P2PCollectionStorage;
use super::head_provider::DocumentHeadProvider;
use super::manager::SyncManager;
use super::peer_state::PeerStateTracker;
use super::rate_limiter::PeerRateLimiter;

#[cfg(test)]
pub(crate) use super::manager::{
    DEFAULT_MAX_CONCURRENT_DAG_FETCHES, DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
};

/// A push failure notification sent when a PushLog to a replicator peer fails.
///
/// The FFI layer consumes these to record failures in the Peerstore for retry.
#[derive(Debug, Clone)]
pub struct PushFailure {
    pub peer_id: String,
    pub doc_id: String,
    pub collection_id: String,
}

/// Coordinator for P2P synchronization.
///
/// This is the main integration point between the P2P layer and the database.
/// Generic over `T: P2PTransport` to support different transport backends.
pub struct SyncCoordinator<B: Blockstore, T: P2PTransport> {
    /// Transport for sending responses and managing connections
    pub(super) transport: T,

    /// Broadcaster for publishing updates
    pub(super) broadcaster: Broadcaster<T>,

    /// Sync manager for block storage
    pub(super) manager: SyncManager<B>,

    /// Peer state tracker
    pub(super) peer_state: Arc<PeerStateTracker>,

    /// Local peer ID (for creator field in broadcasts)
    pub(super) local_peer_id: String,

    /// Access control mode
    pub(super) access_mode: AccessMode,

    /// Replicator registry for access control checks
    pub(super) replicators: Arc<ReplicatorRegistry>,

    /// Set of subscribed collection IDs for P2P sync (in-memory cache)
    pub(super) subscribed_collections: Arc<tokio::sync::RwLock<std::collections::HashSet<String>>>,

    /// Persistent storage for P2P collection subscriptions
    pub(super) collection_store: Arc<dyn P2PCollectionStorage>,

    /// Document head provider for DocSync responses
    pub(super) head_provider: Arc<dyn DocumentHeadProvider>,

    /// Channel for reporting push failures to the FFI layer for retry tracking.
    pub(super) failure_tx: Option<tokio::sync::mpsc::UnboundedSender<PushFailure>>,

    /// Semaphore limiting concurrent DAG fetch tasks (configurable via SyncConfig).
    pub(super) dag_fetch_semaphore: Arc<tokio::sync::Semaphore>,

    /// Semaphore limiting concurrent push tasks (configurable via SyncConfig).
    pub(super) push_semaphore: Arc<tokio::sync::Semaphore>,

    /// Per-peer rate limiter applied at event dispatch to throttle abusive peers.
    pub(super) rate_limiter: Arc<PeerRateLimiter>,
}

/// Type alias for SyncCoordinator using the libp2p transport.
pub type Libp2pSyncCoordinator<B> =
    SyncCoordinator<B, crate::host::libp2p_transport::Libp2pTransport>;

/// Type alias for SyncCoordinator using the iroh transport.
#[cfg(feature = "iroh-transport")]
pub type IrohSyncCoordinator<B> = SyncCoordinator<B, crate::iroh::IrohTransport>;

#[cfg(test)]
mod access_tests;
