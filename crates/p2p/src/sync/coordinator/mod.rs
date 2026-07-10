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
//! - **Direct replicator PushLog acceptance** follows Go's replicator comm
//!   channel and skips receiver-side collection access before merge.
//! - **Pubsub PushLog acceptance** requires collection replicator membership,
//!   a local collection subscription, or explicit replay authorization.
//! - **Inbound Gossip acceptance** is topic-scoped to local collection
//!   subscriptions and rejects outbound replicator targets so one-way
//!   replicators do not receive reverse-direction gossip from their targets.
//! - **Document-level ACP** remains the authoritative policy boundary for whether
//!   replicated document content is actually mergeable/readable locally.
//!
//! Pull-sync protocols that mirror Go's `doc-sync` / `sync-branchable` RPCs
//! may be served to connected peers. Document-level ACP remains the
//! authoritative policy boundary for whether replicated document content is
//! mergeable/readable locally.

mod access;
mod accessors;
mod authorizer;
mod broadcast;
mod constructor;
pub(crate) mod dag_context;
pub(crate) mod dag_fetcher;
mod event_handler;
#[cfg(feature = "libp2p-transport")]
mod pubsub_client;
#[cfg(feature = "libp2p-transport")]
mod pubsub_services;
mod push_worker;
mod replicators;
mod result_types;
mod subscriptions;

pub use result_types::{CreateReplicatorResult, LoadReplicatorsResult};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use acp::DocumentACP;
use blockstore::Blockstore;
use cid::Cid;
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

use crate::bitswap::{AccessMode, ReplicatorRegistry};
use crate::replicator::ReplicationFilterMatcher;
use crate::transport::{P2PTransport, PeerId};

use super::broadcaster::Broadcaster;
use super::collection_store::P2PCollectionStorage;
use super::head_provider::DocumentHeadProvider;
use super::manager::SyncManager;
use super::peer_state::PeerStateTracker;
use super::rate_limiter::PeerRateLimiter;

#[cfg(test)]
pub(crate) use super::manager::{
    DEFAULT_MAX_CONCURRENT_DAG_FETCHES, DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
    DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
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

/// Stable diagnostic snapshot of P2P-owned sync resources (#1099).
///
/// Exposed over the P2P operations surface so downstream runtimes can
/// conformance-test and alert on the effective (not just configured) state:
/// live queue occupancy, per-peer backlog, worker slots, pending-DAG depth,
/// retained task handles, and overload counters.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SyncStatus {
    pub push_backlog: crate::sync::push_backlog::PushBacklogSnapshot,
    pub pending_dags: usize,
    pub pending_dag_capacity: usize,
    /// Durable pending-DAG registrations (may exceed `pending_dags`: records
    /// outlive TTL-evicted in-memory entries until their roots merge).
    pub persisted_pending_dags: usize,
    pub persisted_pending_dag_capacity: usize,
    pub pending_resync_in_flight: bool,
    pub retained_background_tasks: usize,
    pub missing_link_retries: u64,
    pub pending_dag_resolved: u64,
    pub pending_dag_expired: u64,
}

struct SyncShutdownState {
    is_shutting_down: AtomicBool,
    background_tasks: Mutex<Vec<JoinHandle<()>>>,
}

const BACKGROUND_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(500);

/// Shared limiter for poll-based DAG fetches.
///
/// The global semaphore bounds total resource usage, while the per-peer
/// semaphore prevents one source peer from queueing enough fetches to occupy
/// every global permit.
#[derive(Clone)]
pub(crate) struct DagFetchLimiter {
    global: Arc<Semaphore>,
    per_peer: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
    peer_limit: usize,
}

pub(crate) struct DagFetchPermits {
    _global: OwnedSemaphorePermit,
    _peer: OwnedSemaphorePermit,
}

impl DagFetchLimiter {
    pub(crate) fn new(global_limit: usize) -> Self {
        let global_limit = global_limit.max(1);
        Self {
            global: Arc::new(Semaphore::new(global_limit)),
            per_peer: Arc::new(Mutex::new(HashMap::new())),
            peer_limit: global_limit.saturating_sub(1).max(1),
        }
    }

    pub(crate) async fn acquire(&self, source_peer: &PeerId) -> Option<DagFetchPermits> {
        let peer_key = source_peer.to_string();
        let peer_semaphore = {
            let mut per_peer = self.per_peer.lock();
            per_peer
                .entry(peer_key)
                .or_insert_with(|| Arc::new(Semaphore::new(self.peer_limit)))
                .clone()
        };

        let Ok(peer_permit) = peer_semaphore.acquire_owned().await else {
            return None;
        };
        let Ok(global_permit) = self.global.clone().acquire_owned().await else {
            return None;
        };

        Some(DagFetchPermits {
            _global: global_permit,
            _peer: peer_permit,
        })
    }

    fn close(&self) {
        self.global.close();
        for semaphore in self.per_peer.lock().values() {
            semaphore.close();
        }
    }
}

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
        // Retire completed handles on every registration so retained handles
        // track live tasks instead of total spawn count (#1099).
        tasks.retain(|task| !task.is_finished());
        if self.is_shutting_down() {
            handle.abort();
        } else {
            tasks.push(handle);
        }
    }

    /// Number of currently retained background task handles.
    pub fn retained_task_count(&self) -> usize {
        self.inner.background_tasks.lock().len()
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
    /// Behind a shared slot so the fixed push workers observe a channel that
    /// is installed after construction (`set_failure_channel`).
    pub(super) failure_tx: Arc<Mutex<Option<tokio::sync::mpsc::Sender<PushFailure>>>>,

    /// Limiter for concurrent DAG fetch tasks (configurable via SyncConfig).
    pub(super) dag_fetch_limiter: DagFetchLimiter,

    /// Bounded admission queue for outbound replicator pushes, drained by the
    /// fixed worker pool spawned at construction (#1099).
    pub(super) push_backlog: Arc<super::push_backlog::PushBacklog>,

    /// Per-peer rate limiter for gossip dispatch (abuse ladder; drop-only).
    pub(super) rate_limiter: Arc<PeerRateLimiter>,

    /// Per-peer rate limiter for request intake (pacing backoff; refusals are
    /// nacked with `RATE_LIMITED_MESSAGE` so pushers retry at the refill rate).
    pub(super) request_rate_limiter: Arc<PeerRateLimiter>,

    /// Maximum document IDs accepted in a single DocSync request.
    pub(super) max_doc_sync_request_doc_ids: usize,

    /// Shutdown state for coordinator-owned background tasks.
    pub(super) shutdown: SyncShutdownHandle,

    /// Filter matcher used to evaluate replication filters during push.
    pub(super) filter_matcher: Arc<dyn ReplicationFilterMatcher>,
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

    /// CID-aware block classifier shared by Bitswap and CAR serve paths.
    pub(super) classifier: Arc<dyn crate::bitswap::BlockClassifier>,

    /// Late-bound ACP resolver/gate shared by Bitswap and CAR serve paths.
    pub(super) serve_acp: Arc<crate::bitswap::LateBoundServeAcp>,

    /// Optional document ACP used for local ACP relationship snapshot replay.
    pub(super) document_acp: std::sync::OnceLock<Arc<dyn DocumentACP>>,

    /// KMS pubsub transport. Set by the embedded-node layer when a transport
    /// that supports raw gossip is in use. Left empty otherwise.
    #[cfg(feature = "libp2p-transport")]
    pub(super) kms_transport: std::sync::OnceLock<Arc<crate::kms::PubsubKeyTransport<T>>>,

    /// Pubsub_rpc DocSync/BranchableSync services (#828). `None` on
    /// transports whose local peer id isn't a libp2p PeerId (e.g. iroh).
    #[cfg(feature = "libp2p-transport")]
    pub(super) pubsub_services: Option<pubsub_services::PubsubServices>,
}

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    pub(crate) fn clear_pending_dag(&self, root_cid: &Cid) -> bool {
        self.manager.clear_pending_dag(root_cid)
    }

    /// Install the KMS pubsub transport. First-call-wins (OnceLock semantics);
    /// subsequent calls are silently discarded.
    #[cfg(feature = "libp2p-transport")]
    pub fn install_kms_transport(&self, transport: Arc<crate::kms::PubsubKeyTransport<T>>) {
        let _ = self.kms_transport.set(transport);
    }

    /// Point-in-time snapshot of sync resource state for diagnostics (#1099).
    pub fn sync_status(&self) -> SyncStatus {
        let diagnostics = self.manager.diagnostics().snapshot();
        SyncStatus {
            push_backlog: self.runtime.push_backlog.snapshot(),
            pending_dags: self.manager.pending_dag_count(),
            pending_dag_capacity: self.manager.max_pending_dags(),
            persisted_pending_dags: self.manager.persisted_pending_count(),
            persisted_pending_dag_capacity: self.manager.persisted_pending_capacity(),
            pending_resync_in_flight: self.manager.pending_resync_in_flight(),
            retained_background_tasks: self.runtime.shutdown.retained_task_count(),
            missing_link_retries: diagnostics.missing_link_retries,
            pending_dag_resolved: diagnostics.pending_dag_resolved,
            pending_dag_expired: diagnostics.pending_dag_expired,
        }
    }

    /// Install the durable pending-DAG store (#1099). First-call-wins.
    pub fn install_pending_dag_store(
        &self,
        store: Arc<dyn crate::sync::pending_store::PendingDagStorage>,
    ) {
        self.manager.install_pending_dag_store(store);
    }

    /// Reconcile persisted pending-DAG registrations after restart and
    /// re-drive unmerged ones through the normal fetch path. Emits
    /// `DagNeedsFetch` sync events, so run it only once a sync-event consumer
    /// is live (or from a spawned task) to avoid filling the event channel.
    /// Returns the re-driven count.
    pub async fn restore_pending_dags(&self) -> usize {
        self.manager.resync_persisted_pending_dags().await
    }

    /// Periodic bounded drain for durable pending-DAG registrations: sweeps
    /// at `interval` until shutdown, so records skipped at capacity (or whose
    /// in-memory entries TTL-expired) are re-driven even when no peer
    /// reconnects and the node never restarts. The steady-state early-exit
    /// inside the sweep makes idle ticks free. Run from a spawned task.
    pub async fn run_pending_dag_resync(&self, interval: Duration) {
        loop {
            if self.runtime.shutdown.is_shutting_down() {
                return;
            }
            self.manager.resync_persisted_pending_dags().await;
            tokio::time::sleep(interval).await;
        }
    }

    pub fn shutdown_handle(&self) -> SyncShutdownHandle {
        self.runtime.shutdown.clone()
    }

    pub async fn shutdown(&self) {
        #[cfg(feature = "libp2p-transport")]
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
        self.runtime.dag_fetch_limiter.close();
        self.runtime.push_backlog.close();
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
#[cfg(feature = "libp2p-transport")]
pub type Libp2pSyncCoordinator<B> =
    SyncCoordinator<B, crate::host::libp2p_transport::Libp2pTransport>;

/// Type alias for SyncCoordinator using the iroh transport.
#[cfg(feature = "iroh-transport")]
pub type IrohSyncCoordinator<B> = SyncCoordinator<B, crate::iroh::IrohTransport>;

#[cfg(test)]
mod access_tests;

#[cfg(test)]
mod dag_fetch_limiter_tests {
    use super::DagFetchLimiter;
    use crate::transport::PeerId;
    use std::time::Duration;

    #[tokio::test]
    async fn single_peer_cannot_hold_every_global_permit() {
        let limiter = DagFetchLimiter::new(4);
        let flooder = PeerId::new("flooder".to_string());
        let legitimate = PeerId::new("legitimate".to_string());

        let mut flooder_permits = Vec::new();
        for _ in 0..3 {
            flooder_permits.push(limiter.acquire(&flooder).await.expect("flooder permit"));
        }

        assert!(
            tokio::time::timeout(Duration::from_millis(20), limiter.acquire(&flooder))
                .await
                .is_err(),
            "one peer should be capped below the global limit"
        );

        let legitimate_permit =
            tokio::time::timeout(Duration::from_millis(20), limiter.acquire(&legitimate))
                .await
                .expect("legitimate peer should get reserved capacity")
                .expect("limiter open");

        drop(legitimate_permit);
        drop(flooder_permits);
    }
}

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

    /// #1099: completed handles must not accumulate for the process lifetime.
    #[tokio::test]
    async fn register_task_prunes_finished_handles() {
        let shutdown = SyncShutdownHandle::new();
        let mut handles = Vec::new();
        for _ in 0..50 {
            let handle = tokio::spawn(async {});
            handles.push(handle.abort_handle());
            shutdown.register_task(handle);
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while handles.iter().any(|handle| !handle.is_finished()) {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        shutdown.register_task(tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }));

        assert!(
            shutdown.retained_task_count() <= 2,
            "finished handles must be pruned on registration, retained {}",
            shutdown.retained_task_count()
        );
        shutdown.shutdown().await;
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
