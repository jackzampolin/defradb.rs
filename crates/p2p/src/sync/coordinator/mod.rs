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
//! # Security Model: Go-Compatible Ingress + Merge-Time ACP
//!
//! Rust follows the Go DefraDB mental model:
//!
//! - **Replicator registration** expresses outbound replay intent and explicit
//!   replay trust. It controls what we push and which peers are treated as
//!   explicit replicators.
//! - **Inbound PushLog acceptance** requires collection replicator membership,
//!   except for requests carrying explicit replay authorization.
//! - **Inbound Gossip acceptance** also allows locally subscribed collection
//!   topics so subscription-based sync can receive updates without registering
//!   the publisher as an explicit replicator.
//! - **Document-level ACP** remains the authoritative policy boundary for whether
//!   replicated document content is actually mergeable/readable locally.
//!
//! Collection-scoped access checks are also used for protocols that ask the
//! receiver to actively serve or enumerate state. Unscoped fetch protocols
//! (DocSync/CAR) admit registered replicators and observed data-topic
//! subscribers while still denying peers that are merely connected.

mod access;
mod accessors;
mod authorizer;
mod broadcast;
mod constructor;
pub(crate) mod dag_context;
pub(crate) mod dag_fetcher;
mod event_handler;
mod pubsub_client;
mod pubsub_services;
mod replicators;
mod result_types;
mod subscriptions;

pub use result_types::{CreateReplicatorResult, LoadReplicatorsResult};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use acp::DocumentACP;
use blockstore::Blockstore;
use cid::Cid;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

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

struct SyncShutdownState {
    is_shutting_down: AtomicBool,
    background_tasks: Mutex<Vec<JoinHandle<()>>>,
}

const BACKGROUND_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(500);

/// Shared shutdown state for coordinator-owned background replication work.
#[derive(Clone)]
pub struct SyncShutdownHandle {
    inner: Arc<SyncShutdownState>,
}

impl SyncShutdownHandle {
    fn new() -> Self {
        Self {
            inner: Arc::new(SyncShutdownState {
                is_shutting_down: AtomicBool::new(false),
                background_tasks: Mutex::new(Vec::new()),
            }),
        }
    }

    fn begin_shutdown(&self) -> bool {
        !self.inner.is_shutting_down.swap(true, Ordering::AcqRel)
    }

    pub fn is_shutting_down(&self) -> bool {
        self.inner.is_shutting_down.load(Ordering::Acquire)
    }

    pub async fn shutdown(&self) {
        if !self.begin_shutdown() {
            return;
        }

        self.drain_background_tasks(BACKGROUND_TASK_SHUTDOWN_TIMEOUT)
            .await;
    }

    fn register_task(&self, handle: JoinHandle<()>) {
        let mut tasks = self.inner.background_tasks.lock();
        if self.is_shutting_down() {
            handle.abort();
        } else {
            tasks.push(handle);
        }
    }

    async fn drain_background_tasks(&self, timeout: Duration) {
        let mut handles = {
            let mut tasks = self.inner.background_tasks.lock();
            std::mem::take(&mut *tasks)
        };

        let started = tokio::time::Instant::now();

        for handle in &mut handles {
            let elapsed = started.elapsed();
            let Some(remaining) = timeout.checked_sub(elapsed) else {
                break;
            };

            match tokio::time::timeout(remaining, handle).await {
                Ok(Ok(())) | Ok(Err(_)) => {}
                Err(_) => {
                    tracing::debug!(
                        timeout_ms = timeout.as_millis() as u64,
                        "Coordinator background task exceeded shutdown drain window; aborting remaining tasks"
                    );
                    break;
                }
            }
        }

        for handle in handles {
            if !handle.is_finished() {
                handle.abort();
            }
        }
    }
}

/// Runtime services and async limits used by coordinator handlers.
pub(super) struct SyncRuntime<T: P2PTransport> {
    /// Transport for sending responses and managing connections.
    pub(super) transport: T,

    /// Broadcaster for publishing updates.
    pub(super) broadcaster: Broadcaster<T>,

    /// Channel for reporting push failures to the FFI layer for retry tracking.
    pub(super) failure_tx: Option<tokio::sync::mpsc::Sender<PushFailure>>,

    /// Semaphore limiting concurrent DAG fetch tasks (configurable via SyncConfig).
    pub(super) dag_fetch_semaphore: Arc<tokio::sync::Semaphore>,

    /// Semaphore limiting concurrent push tasks (configurable via SyncConfig).
    pub(super) push_semaphore: Arc<tokio::sync::Semaphore>,

    /// Per-peer rate limiter applied at event dispatch to throttle abusive peers.
    pub(super) rate_limiter: Arc<PeerRateLimiter>,

    /// Shutdown state for coordinator-owned background tasks.
    pub(super) shutdown: SyncShutdownHandle,
}

/// Access control and peer identity state for the coordinator.
pub(super) struct SyncAccessState {
    /// Peer state tracker.
    pub(super) peer_state: Arc<PeerStateTracker>,

    /// Local peer ID (for creator field in broadcasts).
    pub(super) local_peer_id: String,

    /// Access control mode.
    pub(super) access_mode: AccessMode,

    /// Replicator registry for access control checks.
    pub(super) replicators: Arc<ReplicatorRegistry>,
}

/// Subscription and document head support state for the coordinator.
pub(super) struct SyncSubscriptionState {
    /// Set of subscribed collection IDs for P2P sync (in-memory cache).
    pub(super) subscribed_collections: Arc<tokio::sync::RwLock<std::collections::HashSet<String>>>,

    /// Persistent storage for P2P collection subscriptions.
    pub(super) collection_store: Arc<dyn P2PCollectionStorage>,

    /// Document head provider for DocSync responses.
    pub(super) head_provider: Arc<dyn DocumentHeadProvider>,
}

/// Coordinator for P2P synchronization.
///
/// This is the main integration point between the P2P layer and the database.
/// Generic over `T: P2PTransport` to support different transport backends.
pub struct SyncCoordinator<B: Blockstore, T: P2PTransport> {
    /// Runtime services and async coordination primitives.
    pub(super) runtime: SyncRuntime<T>,

    /// Sync manager for block storage.
    pub(super) manager: SyncManager<B>,

    /// Access control and peer identity state.
    pub(super) access: SyncAccessState,

    /// Subscription and doc-sync support state.
    pub(super) subscriptions: SyncSubscriptionState,

    /// Shared peer-authorization backend. Used by both the two-stream
    /// access helpers and the `pubsub_rpc` handlers so both paths make
    /// the same decision for the same peer state.
    pub(super) authorizer: Arc<authorizer::RuntimeAuthorizer<T>>,

    /// Optional document ACP used for local ACP relationship snapshot replay.
    pub(super) document_acp: std::sync::OnceLock<Arc<dyn DocumentACP>>,

    /// Pubsub_rpc DocSync/BranchableSync services (#828). `None` on
    /// transports whose local peer id isn't a libp2p PeerId (e.g. iroh).
    pub(super) pubsub_services: Option<pubsub_services::PubsubServices>,
}

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    pub(crate) fn clear_pending_dag(&self, root_cid: &Cid) -> bool {
        self.manager.clear_pending_dag(root_cid)
    }

    pub fn shutdown_handle(&self) -> SyncShutdownHandle {
        self.runtime.shutdown.clone()
    }

    pub async fn shutdown(&self) {
        if let Some(services) = self.pubsub_services.as_ref() {
            services.set_ready(false);
            let cancelled = services.cancel_in_flight();
            if cancelled > 0 {
                tracing::debug!(
                    cancelled,
                    "Cancelled in-flight pubsub_rpc requests during coordinator shutdown"
                );
            }
        }
        self.runtime.dag_fetch_semaphore.close();
        self.runtime.push_semaphore.close();
        self.runtime.shutdown.shutdown().await;
    }

    pub(crate) fn spawn_background_task<F>(&self, task_name: &'static str, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        if self.runtime.shutdown.is_shutting_down() {
            tracing::debug!(task = task_name, "Skipping background task during shutdown");
            return;
        }

        let handle = tokio::spawn(future);
        self.runtime.shutdown.register_task(handle);
    }

    #[cfg(test)]
    pub(crate) fn pending_dag_count(&self) -> usize {
        self.manager.pending_dag_count()
    }
}

/// Type alias for SyncCoordinator using the libp2p transport.
pub type Libp2pSyncCoordinator<B> =
    SyncCoordinator<B, crate::host::libp2p_transport::Libp2pTransport>;

/// Type alias for SyncCoordinator using the iroh transport.
#[cfg(feature = "iroh-transport")]
pub type IrohSyncCoordinator<B> = SyncCoordinator<B, crate::iroh::IrohTransport>;

#[cfg(test)]
mod access_tests;

#[cfg(test)]
mod shutdown_tests {
    use super::SyncShutdownHandle;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn shutdown_waits_for_in_flight_background_task_completion() {
        let shutdown = SyncShutdownHandle::new();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_for_task = Arc::clone(&completed);

        shutdown.register_task(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            completed_for_task.store(true, Ordering::SeqCst);
        }));

        shutdown.shutdown().await;

        assert!(
            completed.load(Ordering::SeqCst),
            "shutdown should allow in-flight background tasks to finish"
        );
    }

    #[tokio::test]
    async fn shutdown_uses_single_global_budget_for_background_tasks() {
        let shutdown = SyncShutdownHandle::new();

        for _ in 0..3 {
            shutdown.register_task(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }));
        }

        let started = tokio::time::Instant::now();
        shutdown.shutdown().await;
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_secs(7),
            "shutdown should use one shared deadline, got {:?}",
            elapsed
        );
    }
}
