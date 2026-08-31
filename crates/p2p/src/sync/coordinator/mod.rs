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
//! - **Inbound Gossip acceptance** treats a local collection subscription as
//!   receive intent, regardless of outbound replicator configuration. Without
//!   a subscription, outbound targets remain invalid gossip sources.
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
pub(crate) mod dag_retry;
mod event_handler;
#[cfg(feature = "libp2p-transport")]
mod pubsub_client;
#[cfg(feature = "libp2p-transport")]
mod pubsub_services;
mod push_worker;
mod replicators;
mod result_types;
mod selective_car_access;
mod subscriptions;

pub use result_types::{CreateReplicatorResult, LoadReplicatorsResult};
pub use selective_car_access::{HeadHintCarAuthority, HeadHintCarGrant};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use acp::DocumentACP;
use blockstore::Blockstore;
use cid::Cid;
use parking_lot::Mutex;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

use crate::bitswap::{AccessMode, ReplicatorRegistry};
use crate::replicator::ReplicationFilterMatcher;
use crate::transport::{P2PTransport, PeerId};

use super::broadcaster::Broadcaster;
use super::collection_store::P2PCollectionStorage;
use super::head_provider::DocumentHeadProvider;
use super::manager::{PendingDag, SyncManager};
use super::peer_state::PeerStateTracker;
use super::rate_limiter::PeerRateLimiter;

#[cfg(test)]
pub(crate) use super::manager::{
    DEFAULT_MAX_CONCURRENT_DAG_FETCHES, DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
    DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
};

/// A durable retry update emitted by outbound PushLog admission/workers.
///
/// The runtime consumes observations to register scope markers before send,
/// failures to retain/reschedule them, and current acknowledgements to clear them.
#[derive(Debug)]
pub struct PushFailure {
    pub peer_id: String,
    pub doc_id: String,
    pub collection_id: String,
    pub cid: String,
    pub head_priority: u64,
    /// False for a pre-send observation; true for a terminal failure that keeps
    /// the already-registered scope marker on its durable ladder.
    pub create_retry: bool,
    /// Successful acknowledgement for this exact scope/head.
    pub acknowledged: bool,
    /// Register-before-send handshake with the durable recorder.
    pub durable_tx: Option<tokio::sync::oneshot::Sender<bool>>,
}

/// Volatile fence for acknowledgements of presence-only durable markers.
///
/// The durable marker intentionally stores no CID. This fence covers the only
/// acknowledgements that can survive long enough to race a newer live head in
/// the same process. Across restart there are no surviving in-flight sends, so
/// the presence marker alone is the conservative source of truth.
#[derive(Default)]
pub struct HeadAckFence {
    current: std::collections::HashMap<(String, String, String), (u64, String)>,
}

impl HeadAckFence {
    fn scope_key(event: &PushFailure) -> (String, String, String) {
        (
            event.peer_id.clone(),
            event.doc_id.clone(),
            event.collection_id.clone(),
        )
    }

    fn head_token(event: &PushFailure) -> (u64, String) {
        (event.head_priority, event.cid.clone())
    }

    pub fn observe_durable(&mut self, event: &PushFailure) {
        self.current
            .insert(Self::scope_key(event), Self::head_token(event));
    }

    pub fn ack_is_current(&self, event: &PushFailure) -> bool {
        self.current.get(&Self::scope_key(event)) == Some(&Self::head_token(event))
    }

    pub fn clear_current_ack(&mut self, event: &PushFailure) {
        if self.ack_is_current(event) {
            self.current.remove(&Self::scope_key(event));
        }
    }
}

#[cfg(test)]
mod head_ack_fence_tests {
    use super::*;

    fn event(cid: &str, priority: u64) -> PushFailure {
        PushFailure {
            peer_id: "peer".to_string(),
            doc_id: "doc".to_string(),
            collection_id: "collection".to_string(),
            cid: cid.to_string(),
            head_priority: priority,
            create_retry: false,
            acknowledged: false,
            durable_tx: None,
        }
    }

    #[test]
    fn stale_ack_cannot_clear_a_newer_scope_head() {
        let mut fence = HeadAckFence::default();
        let old = event("old", 1);
        let new = event("new", 2);
        fence.observe_durable(&old);
        fence.observe_durable(&new);

        assert!(!fence.ack_is_current(&old));
        fence.clear_current_ack(&old);
        assert!(fence.ack_is_current(&new));
        fence.clear_current_ack(&new);
        assert!(!fence.ack_is_current(&new));
    }
}

/// Stable diagnostic snapshot of P2P-owned sync resources (#1099).
///
/// Exposed over the P2P operations surface so downstream runtimes can
/// conformance-test and alert on the effective (not just configured) state:
/// live queue occupancy, per-peer backlog, worker slots, pending-DAG depth,
/// retained task handles, and overload counters.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SyncStatus {
    /// Live queue occupancy and retry/retirement counters.
    pub push_backlog: crate::sync::push_backlog::PushBacklogSnapshot,
    /// Gossip updates folded into a newer update during the short window.
    pub broadcast_coalesced_total: u64,
    /// Replicator fan-outs folded before enumerating peers.
    pub push_updates_coalesced_total: u64,
    /// Gossip messages rejected because an unsubscribed sender was configured
    /// only as an outbound replicator target.
    pub gossip_direction_filtered_total: u64,
    pub pending_dags: usize,
    pub pending_dag_capacity: usize,
    pub pending_dag_high_water: u64,
    /// Durable pending-DAG registrations (may exceed `pending_dags`: records
    /// outlive TTL-evicted in-memory entries until their roots merge).
    pub persisted_pending_dags: usize,
    pub persisted_pending_dag_capacity: usize,
    pub persisted_pending_dag_high_water: u64,
    pub pending_resync_in_flight: bool,
    pub retained_background_tasks: usize,
    /// Current/high-water occupancy and terminal overload counters for the
    /// one shared inbound request scheduler.
    pub request_dispatch: crate::sync::DispatchSnapshot,
    /// Current/high-water occupancy of bounded non-authoritative mutation
    /// gossip/artifact work. Durable head markers are installed before this
    /// pool is entered, so shedding here cannot lose a sync obligation.
    pub non_authoritative_broadcast_tasks: usize,
    pub non_authoritative_broadcast_high_water: usize,
    pub non_authoritative_broadcast_rejected_total: u64,
    pub missing_link_retries: u64,
    pub car_requested_cids: u64,
    pub car_present_cids: u64,
    pub car_served_cids: u64,
    pub car_filtered_cids: u64,
    pub provider_rotations: u64,
    pub pending_dag_resolved: u64,
    /// Push-originated missing DAGs durably registered before a success ack.
    pub pending_dag_registered: u64,
    pub pending_dag_expired: u64,
    pub single_flight_suppressed: u64,
    pub already_merged_fast_path: u64,
    pub pending_dag_capacity_shed: u64,
    /// Retry-clock ticks that dispatched a due pending-DAG fetch (#1116 stage 2).
    pub pending_dag_retry_dispatched: u64,
    /// Retry-clock/claim attempts that found no due entry (#1116 stage 2).
    pub pending_dag_retry_suppressed: u64,
    /// Due roots deferred because none of their qualified providers is
    /// currently connected. The existing per-root clock owns the next try.
    pub pending_dag_fetch_deferred_unavailable: u64,
    /// Useful CAR responses coalesced behind an existing local storage owner.
    /// These release the fetch lease without consuming provider-failure attempts.
    pub pending_dag_fetch_deferred_contention: u64,
    /// Roots whose bounded fetch exhausted all attempts/providers.
    pub pending_dag_fetch_exhausted: u64,
    /// Durable pending-DAG obligations discharged by terminal merge/mark.
    pub pending_dag_terminal_merged: u64,
    /// Milliseconds until the earliest due receiver obligation, including a
    /// complete DAG awaiting a terminal merge outcome.
    pub next_pending_retry_in_ms: Option<u64>,
    /// Pending-DAG roots quarantined after a deterministic merge rejection
    /// (#1128); see `SyncManager::quarantine_pending_dag`.
    pub pending_dag_terminal_quarantined: u64,
    /// Current gauge of quarantined pending-DAG roots (#1128).
    pub quarantined_pending_dags: usize,
}

struct SyncShutdownState {
    is_shutting_down: AtomicBool,
    /// Wakes tasks parked in [`SyncShutdownHandle::cancelled`] the moment
    /// shutdown begins, so a periodic loop exits on the signal instead of at
    /// the end of its sleep. Carries no state of its own; `is_shutting_down`
    /// remains the single source of truth and stays a plain atomic because it
    /// is read on hot paths.
    shutdown_notify: Notify,
    background_tasks: Mutex<Vec<JoinHandle<()>>>,
    non_authoritative_broadcast_slots: Arc<Semaphore>,
    non_authoritative_broadcast_high_water: AtomicUsize,
    non_authoritative_broadcast_rejected: AtomicU64,
    /// Scheduled and running poll fetches keyed by pending-DAG root. One
    /// registry bounds the event handoff and the retained task, so there is no
    /// hidden pre-semaphore task queue (#1159).
    pending_dag_fetch_task_limit: usize,
    pending_dag_fetch_tasks: Mutex<HashMap<Cid, PendingDagFetchTask>>,
}

enum PendingDagFetchTask {
    Scheduled,
    Running(JoinHandle<()>),
}

const BACKGROUND_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(500);
const NON_AUTHORITATIVE_BROADCAST_TASK_LIMIT: usize = 32;

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
    fn new(pending_dag_fetch_task_limit: usize) -> Self {
        Self {
            inner: Arc::new(SyncShutdownState {
                is_shutting_down: AtomicBool::new(false),
                shutdown_notify: Notify::new(),
                background_tasks: Mutex::new(Vec::new()),
                non_authoritative_broadcast_slots: Arc::new(Semaphore::new(
                    NON_AUTHORITATIVE_BROADCAST_TASK_LIMIT,
                )),
                non_authoritative_broadcast_high_water: AtomicUsize::new(0),
                non_authoritative_broadcast_rejected: AtomicU64::new(0),
                pending_dag_fetch_task_limit: pending_dag_fetch_task_limit.max(1),
                pending_dag_fetch_tasks: Mutex::new(HashMap::new()),
            }),
        }
    }

    fn begin_shutdown(&self) -> bool {
        let won = !self.inner.is_shutting_down.swap(true, Ordering::AcqRel);
        if won {
            // Wake every parked `cancelled()` waiter. Ordering matters: the
            // flag is set first, so a waiter that registers between the swap
            // and this call observes the flag and never parks.
            self.inner.shutdown_notify.notify_waiters();
        }
        won
    }

    pub fn is_shutting_down(&self) -> bool {
        self.inner.is_shutting_down.load(Ordering::Acquire)
    }

    /// Resolves as soon as shutdown begins, and immediately if it already has.
    ///
    /// Periodic loops select on this against their sleep so they exit on the
    /// signal rather than at the end of an interval, matching Go's
    /// `select { case <-ctx.Done(): ... }` shape in
    /// `internal/db/p2p/replicator.go`.
    pub async fn cancelled(&self) {
        // Register before observing the flag: a `notify_waiters` that lands
        // after this point wakes us, and one that landed before it is
        // reflected in the flag we are about to read. Checking first would
        // leave a window where neither happens and the caller parks forever.
        let notified = self.inner.shutdown_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if self.is_shutting_down() {
            return;
        }

        notified.await;
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

    fn try_acquire_non_authoritative_broadcast_slot(&self) -> Option<OwnedSemaphorePermit> {
        let permit =
            match Arc::clone(&self.inner.non_authoritative_broadcast_slots).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    self.inner
                        .non_authoritative_broadcast_rejected
                        .fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            };
        let current = NON_AUTHORITATIVE_BROADCAST_TASK_LIMIT.saturating_sub(
            self.inner
                .non_authoritative_broadcast_slots
                .available_permits(),
        );
        self.inner
            .non_authoritative_broadcast_high_water
            .fetch_max(current, Ordering::Relaxed);
        Some(permit)
    }

    fn non_authoritative_broadcast_stats(&self) -> (usize, usize, u64) {
        (
            NON_AUTHORITATIVE_BROADCAST_TASK_LIMIT.saturating_sub(
                self.inner
                    .non_authoritative_broadcast_slots
                    .available_permits(),
            ),
            self.inner
                .non_authoritative_broadcast_high_water
                .load(Ordering::Relaxed),
            self.inner
                .non_authoritative_broadcast_rejected
                .load(Ordering::Relaxed),
        )
    }

    fn prune_pending_dag_fetches(tasks: &mut HashMap<Cid, PendingDagFetchTask>) {
        tasks.retain(|_, task| match task {
            PendingDagFetchTask::Scheduled => true,
            PendingDagFetchTask::Running(task) => !task.is_finished(),
        });
    }

    fn reserve_pending_dag_fetch(&self, root_cid: Cid) -> bool {
        let mut tasks = self.inner.pending_dag_fetch_tasks.lock();
        Self::prune_pending_dag_fetches(&mut tasks);
        if self.is_shutting_down()
            || tasks.contains_key(&root_cid)
            || tasks.len() >= self.inner.pending_dag_fetch_task_limit
        {
            return false;
        }

        tasks.insert(root_cid, PendingDagFetchTask::Scheduled);
        true
    }

    fn release_pending_dag_fetch_reservation(&self, root_cid: &Cid) {
        let mut tasks = self.inner.pending_dag_fetch_tasks.lock();
        if matches!(tasks.get(root_cid), Some(PendingDagFetchTask::Scheduled)) {
            tasks.remove(root_cid);
        }
    }

    fn available_pending_dag_fetch_slots(&self) -> usize {
        let mut tasks = self.inner.pending_dag_fetch_tasks.lock();
        Self::prune_pending_dag_fetches(&mut tasks);
        self.inner
            .pending_dag_fetch_task_limit
            .saturating_sub(tasks.len())
    }

    fn spawn_pending_dag_fetch<F>(&self, root_cid: Cid, future: F) -> bool
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.inner.pending_dag_fetch_tasks.lock();
        Self::prune_pending_dag_fetches(&mut tasks);
        if self.is_shutting_down() {
            return false;
        }

        match tasks.get(&root_cid) {
            Some(PendingDagFetchTask::Scheduled) => {}
            Some(PendingDagFetchTask::Running(_)) => return false,
            None if tasks.len() < self.inner.pending_dag_fetch_task_limit => {}
            None => return false,
        }

        tasks.insert(root_cid, PendingDagFetchTask::Running(tokio::spawn(future)));
        true
    }

    /// Number of live retained background task handles. Prunes finished
    /// handles first so a burst of completed tasks does not overstate live
    /// work between registrations.
    pub fn retained_task_count(&self) -> usize {
        let mut tasks = self.inner.background_tasks.lock();
        tasks.retain(|task| !task.is_finished());
        let background_count = tasks.len();
        drop(tasks);

        let mut pending_dag_fetches = self.inner.pending_dag_fetch_tasks.lock();
        Self::prune_pending_dag_fetches(&mut pending_dag_fetches);
        background_count + pending_dag_fetches.len()
    }

    async fn drain_background_tasks(&self, timeout: Duration) {
        let mut handles = {
            let mut tasks = self.inner.background_tasks.lock();
            std::mem::take(&mut *tasks)
        };
        handles.extend({
            let mut tasks = self.inner.pending_dag_fetch_tasks.lock();
            std::mem::take(&mut *tasks)
                .into_values()
                .filter_map(|task| match task {
                    PendingDagFetchTask::Scheduled => None,
                    PendingDagFetchTask::Running(task) => Some(task),
                })
        });

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

    /// Channel for reporting durable retry updates to the host runtime.
    /// Behind a shared slot so the fixed push workers observe a channel that
    /// is installed after construction (`set_failure_channel`).
    pub(super) failure_tx: Arc<Mutex<Option<tokio::sync::mpsc::Sender<PushFailure>>>>,

    /// Limiter for concurrent DAG fetch tasks (configurable via SyncConfig).
    pub(super) dag_fetch_limiter: DagFetchLimiter,

    /// Bounded admission queue for outbound replicator pushes, drained by the
    /// fixed worker pool spawned at construction (#1099).
    pub(super) push_backlog: Arc<super::push_backlog::PushBacklog>,

    pub(super) broadcast_coalescer: Arc<super::broadcast_coalescer::BroadcastCoalescer>,

    pub(super) push_fanout_coalescer: Arc<super::push_fanout_coalescer::PushFanoutCoalescer>,

    /// Temporary per-peer CAR grants scoped to DAGs in active outbound pushes.
    pub(super) selective_car_access: Arc<selective_car_access::SelectiveCarAccess>,

    /// Per-peer rate limiter for gossip dispatch (abuse ladder; drop-only).
    pub(super) rate_limiter: Arc<PeerRateLimiter>,

    /// Per-peer rate limiter for request intake. Refusals are nacked with
    /// `RATE_LIMITED_MESSAGE`; a sender retains its marker and retries on the
    /// durable document ladder.
    pub(super) request_rate_limiter: Arc<PeerRateLimiter>,

    /// Maximum document IDs accepted in a single DocSync request.
    pub(super) max_doc_sync_request_doc_ids: usize,

    /// Shutdown state for coordinator-owned background tasks.
    pub(super) shutdown: SyncShutdownHandle,

    /// Instance-local admission and lifecycle diagnostics for the shared
    /// transport event dispatcher.
    pub(super) dispatch_diagnostics: Arc<crate::sync::DispatchDiagnostics>,

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

    /// Gossip messages rejected by the receive-side direction guard.
    pub(super) gossip_direction_filtered: AtomicU64,
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
    #[cfg(feature = "kms")]
    pub(super) kms_transport: std::sync::OnceLock<Arc<crate::kms::PubsubKeyTransport<T>>>,

    /// Pubsub_rpc DocSync/BranchableSync services (#828). `None` on
    /// transports whose local peer id isn't a libp2p PeerId (e.g. iroh).
    #[cfg(feature = "libp2p-transport")]
    pub(super) pubsub_services: Option<pubsub_services::PubsubServices>,
}

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Drain one transport event stream through the shared bounded scheduler.
    pub async fn run_event_dispatcher<E, Handler, HandlerFuture>(
        &self,
        events: tokio::sync::mpsc::Receiver<E>,
        handler: Handler,
    ) where
        E: crate::sync::DispatchEvent + Send + 'static,
        Handler: Fn(E, crate::sync::DispatchAdmission) -> HandlerFuture + Clone + Send + 'static,
        HandlerFuture: std::future::Future<Output = ()> + Send + 'static,
    {
        crate::sync::event_dispatcher::run_event_dispatcher(
            events,
            Arc::clone(&self.runtime.dispatch_diagnostics),
            handler,
        )
        .await;
    }

    /// Install the KMS pubsub transport. First-call-wins (OnceLock semantics);
    /// subsequent calls are silently discarded.
    #[cfg(feature = "kms")]
    pub fn install_kms_transport(&self, transport: Arc<crate::kms::PubsubKeyTransport<T>>) {
        let _ = self.kms_transport.set(transport);
    }

    /// Point-in-time snapshot of sync resource state for diagnostics (#1099).
    pub fn sync_status(&self) -> SyncStatus {
        let diagnostics = self.manager.diagnostics().snapshot();
        let (
            non_authoritative_broadcast_tasks,
            non_authoritative_broadcast_high_water,
            non_authoritative_broadcast_rejected_total,
        ) = self.runtime.shutdown.non_authoritative_broadcast_stats();
        SyncStatus {
            push_backlog: self.runtime.push_backlog.snapshot(),
            broadcast_coalesced_total: self.runtime.broadcast_coalescer.coalesced(),
            push_updates_coalesced_total: self.runtime.push_fanout_coalescer.coalesced(),
            gossip_direction_filtered_total: self
                .access
                .gossip_direction_filtered
                .load(Ordering::Relaxed),
            pending_dags: self.manager.pending_dag_count(),
            pending_dag_capacity: self.manager.max_pending_dags(),
            pending_dag_high_water: diagnostics.pending_dag_high_water,
            persisted_pending_dags: self.manager.persisted_pending_count(),
            persisted_pending_dag_capacity: self.manager.persisted_pending_capacity(),
            persisted_pending_dag_high_water: diagnostics.persisted_pending_dag_high_water,
            pending_resync_in_flight: self.manager.pending_resync_in_flight(),
            retained_background_tasks: self.runtime.shutdown.retained_task_count(),
            request_dispatch: self.runtime.dispatch_diagnostics.snapshot(),
            non_authoritative_broadcast_tasks,
            non_authoritative_broadcast_high_water,
            non_authoritative_broadcast_rejected_total,
            missing_link_retries: diagnostics.missing_link_retries,
            car_requested_cids: diagnostics.car_requested_cids,
            car_present_cids: diagnostics.car_present_cids,
            car_served_cids: diagnostics.car_served_cids,
            car_filtered_cids: diagnostics.car_filtered_cids,
            provider_rotations: diagnostics.provider_rotations,
            pending_dag_resolved: diagnostics.pending_dag_resolved,
            pending_dag_registered: diagnostics.pending_dag_registered,
            pending_dag_expired: diagnostics.pending_dag_expired,
            single_flight_suppressed: diagnostics.single_flight_suppressed,
            already_merged_fast_path: diagnostics.already_merged_fast_path,
            pending_dag_capacity_shed: diagnostics.pending_dag_capacity_shed,
            pending_dag_retry_dispatched: diagnostics.pending_dag_retry_dispatched,
            pending_dag_retry_suppressed: diagnostics.pending_dag_retry_suppressed,
            pending_dag_fetch_deferred_unavailable: diagnostics
                .pending_dag_fetch_deferred_unavailable,
            pending_dag_fetch_deferred_contention: diagnostics
                .pending_dag_fetch_deferred_contention,
            pending_dag_fetch_exhausted: diagnostics.pending_dag_fetch_exhausted,
            pending_dag_terminal_merged: diagnostics.pending_dag_terminal_merged,
            next_pending_retry_in_ms: self.manager.next_pending_retry_in_ms(),
            pending_dag_terminal_quarantined: diagnostics.pending_dag_terminal_quarantined,
            quarantined_pending_dags: self.manager.quarantined_pending_count(),
        }
    }

    /// Install the durable pending-DAG store (#1099). First-call-wins.
    /// Hydrates the durable-cap accounting before returning.
    pub async fn install_pending_dag_store(
        &self,
        store: Arc<dyn crate::sync::pending_store::PendingDagStorage>,
    ) {
        self.manager.install_pending_dag_store(store).await;
    }

    /// Reconcile persisted pending-DAG registrations after restart. Incomplete
    /// roots are restored as immediately due; the receiver retry clock remains
    /// the sole owner that claims and dispatches their fetches. Returns the
    /// restored count.
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
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = self.runtime.shutdown.cancelled() => return,
            }
        }
    }

    /// The receiver's sole re-arm loop (#1116 stage 2): every `interval`, claim
    /// only as many due roots as the bounded fetch owner can accept.
    /// Registration, partial progress, reconnect, and restart only make roots
    /// due; none of them emits `DagNeedsFetch` independently.
    pub async fn run_pending_dag_retry_clock(&self, interval: Duration) {
        loop {
            if self.runtime.shutdown.is_shutting_down() {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = self.runtime.shutdown.cancelled() => return,
            }
            self.dispatch_due_pending_dag_fetches(tokio::time::Instant::now());
        }
    }

    fn dispatch_due_pending_dag_fetches(&self, now: tokio::time::Instant) -> usize {
        let due = self.manager.due_pending_dag_retries(now);
        let event_tx = self.manager.event_sender();
        let mut available = self.runtime.shutdown.available_pending_dag_fetch_slots();
        let mut count = 0;
        for (root_cid, dag) in due {
            if available == 0 {
                break;
            }
            if !self.runtime.shutdown.reserve_pending_dag_fetch(root_cid) {
                continue;
            }
            available -= 1;

            let Ok(event_permit) = event_tx.try_reserve() else {
                self.runtime
                    .shutdown
                    .release_pending_dag_fetch_reservation(&root_cid);
                break;
            };
            if !self.manager.try_claim_pending_dag_dispatch(&root_cid, now) {
                self.runtime
                    .shutdown
                    .release_pending_dag_fetch_reservation(&root_cid);
                available += 1;
                continue;
            }

            self.dispatch_pending_dag_fetch(root_cid, &dag, event_permit);
            count += 1;
        }
        count
    }

    #[cfg(test)]
    pub(crate) fn dispatch_due_pending_dag_fetches_for_test(
        &self,
        now: tokio::time::Instant,
    ) -> usize {
        self.dispatch_due_pending_dag_fetches(now)
    }

    /// Build the provider list for a fetch dispatch from positive per-CID
    /// availability evidence plus the authenticated DAG origin. A newly
    /// connected or root-only peer may expedite the receiver clock, but it
    /// must not become a linked-DAG provider merely by doing so (#1512).
    fn dispatch_pending_dag_fetch(
        &self,
        root_cid: Cid,
        dag: &PendingDag,
        event_permit: tokio::sync::mpsc::Permit<'_, crate::sync::SyncEvent>,
    ) {
        let missing: Vec<_> = dag.missing.iter().copied().collect();
        let mut providers = self.manager.get_providers_for_cids(&missing);
        if let Some(source_peer) = dag.source_peer.clone() {
            if !providers.contains(&source_peer) {
                providers.push(source_peer);
            }
        }
        for provider in dag
            .alternate_providers
            .iter()
            .take(crate::sync::pending_store::MAX_PENDING_DAG_ALTERNATE_PROVIDERS)
        {
            if !providers.contains(provider) {
                providers.push(provider.clone());
            }
        }
        tracing::debug!(
            root_cid = %root_cid,
            missing_count = missing.len(),
            fetch_failures = dag.fetch_failures,
            "Dispatching pending DAG fetch"
        );
        event_permit.send(crate::sync::SyncEvent::DagNeedsFetch {
            root_cid,
            missing,
            providers,
            doc_id: dag.doc_id.clone(),
            collection_id: dag.collection_id.clone(),
            creator: dag.creator.clone(),
            sender_peer: dag.source_peer.clone(),
            is_explicit_replicator: dag.is_explicit_replicator,
            explicit_replay_authorization: dag.explicit_replay_authorization.clone(),
        });
    }

    #[cfg(test)]
    fn dispatch_pending_dag_fetch_for_test(&self, root_cid: Cid, dag: &PendingDag) {
        let event_tx = self.manager.event_sender();
        let event_permit = event_tx
            .try_reserve()
            .expect("test event receiver must have capacity");
        self.dispatch_pending_dag_fetch(root_cid, dag, event_permit);
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

    /// Spawn work owned by this coordinator so shutdown can drain or cancel it.
    pub fn spawn_background_task<F>(&self, task_name: &'static str, future: F)
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

    /// Spawn mutation-adjacent gossip/artifact work in a distinct bounded
    /// pool. Callers must install durable document/collection head markers
    /// before using this method; overflow therefore sheds only redundant,
    /// non-authoritative dissemination work.
    pub fn spawn_non_authoritative_broadcast_task<F>(&self, task_name: &'static str, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        if self.runtime.shutdown.is_shutting_down() {
            tracing::debug!(
                task = task_name,
                "Skipping background broadcast during shutdown"
            );
            return;
        }
        let Some(permit) = self
            .runtime
            .shutdown
            .try_acquire_non_authoritative_broadcast_slot()
        else {
            tracing::warn!(
                task = task_name,
                limit = NON_AUTHORITATIVE_BROADCAST_TASK_LIMIT,
                "Non-authoritative background broadcast pool full; durable head marker retains delivery ownership"
            );
            return;
        };
        let handle = tokio::spawn(async move {
            future.await;
            drop(permit);
        });
        self.runtime.shutdown.register_task(handle);
    }

    pub(crate) fn spawn_pending_dag_fetch_task<F>(
        &self,
        root_cid: Cid,
        task_name: &'static str,
        future: F,
    ) -> bool
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        if self
            .runtime
            .shutdown
            .spawn_pending_dag_fetch(root_cid, future)
        {
            true
        } else {
            self.runtime
                .shutdown
                .release_pending_dag_fetch_reservation(&root_cid);
            self.manager
                .diagnostics()
                .record_pending_dag_retry_suppressed();
            tracing::debug!(
                task = task_name,
                root_cid = %root_cid,
                "Suppressing duplicate pending-DAG fetch task"
            );
            false
        }
    }

    pub(crate) fn release_pending_dag_fetch_reservation(&self, root_cid: &Cid) {
        self.runtime
            .shutdown
            .release_pending_dag_fetch_reservation(root_cid);
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
    use super::{SyncShutdownHandle, NON_AUTHORITATIVE_BROADCAST_TASK_LIMIT};
    use cid::Cid;
    use multihash_codetable::{Code, MultihashDigest};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn shutdown_waits_for_in_flight_background_task_completion() {
        let shutdown = SyncShutdownHandle::new(4);
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

    #[test]
    fn non_authoritative_broadcast_slots_are_bounded_and_observable() {
        let shutdown = SyncShutdownHandle::new(4);
        let mut permits = Vec::new();
        for _ in 0..NON_AUTHORITATIVE_BROADCAST_TASK_LIMIT {
            permits.push(
                shutdown
                    .try_acquire_non_authoritative_broadcast_slot()
                    .expect("slot within limit"),
            );
        }
        assert!(
            shutdown
                .try_acquire_non_authoritative_broadcast_slot()
                .is_none(),
            "overflow must be actionable instead of allocating another task"
        );
        assert_eq!(
            shutdown.non_authoritative_broadcast_stats(),
            (
                NON_AUTHORITATIVE_BROADCAST_TASK_LIMIT,
                NON_AUTHORITATIVE_BROADCAST_TASK_LIMIT,
                1,
            )
        );
        drop(permits);
        assert_eq!(shutdown.non_authoritative_broadcast_stats().0, 0);
    }

    /// #1099: completed handles must not accumulate for the process lifetime.
    #[tokio::test]
    async fn register_task_prunes_finished_handles() {
        let shutdown = SyncShutdownHandle::new(4);
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

    /// A retry-clock tick must not retain another multi-minute poll fetch for
    /// a root whose previous fetch is still alive (#1159 production soak).
    #[tokio::test]
    async fn pending_dag_fetches_are_single_flight_per_root() {
        let shutdown = SyncShutdownHandle::new(4);
        let root = Cid::new_v1(0x55, Code::Sha2_256.digest(b"pending-root"));
        let first_release = Arc::new(tokio::sync::Notify::new());
        let first_release_for_task = Arc::clone(&first_release);

        assert!(shutdown.spawn_pending_dag_fetch(root, async move {
            first_release_for_task.notified().await;
        }));
        assert!(
            !shutdown.spawn_pending_dag_fetch(root, async {}),
            "a live fetch must suppress a second task for the same root"
        );
        assert_eq!(shutdown.retained_task_count(), 1);

        first_release.notify_one();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while shutdown.retained_task_count() != 0 {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::task::yield_now().await;
        }

        assert!(
            shutdown.spawn_pending_dag_fetch(root, async {}),
            "the root must become eligible after its prior fetch finishes"
        );
        shutdown.shutdown().await;
    }

    #[tokio::test]
    async fn scheduled_and_running_pending_fetches_share_one_bound() {
        let shutdown = SyncShutdownHandle::new(2);
        let first = Cid::new_v1(0x55, Code::Sha2_256.digest(b"first"));
        let second = Cid::new_v1(0x55, Code::Sha2_256.digest(b"second"));
        let third = Cid::new_v1(0x55, Code::Sha2_256.digest(b"third"));
        let release = Arc::new(tokio::sync::Notify::new());
        let task_release = Arc::clone(&release);

        assert!(shutdown.reserve_pending_dag_fetch(first));
        assert!(shutdown.spawn_pending_dag_fetch(first, async move {
            task_release.notified().await;
        }));
        assert!(shutdown.reserve_pending_dag_fetch(second));
        assert_eq!(shutdown.available_pending_dag_fetch_slots(), 0);
        assert!(
            !shutdown.reserve_pending_dag_fetch(third),
            "a scheduled event must consume the same bound as a running task"
        );
        assert_eq!(shutdown.retained_task_count(), 2);

        shutdown.release_pending_dag_fetch_reservation(&second);
        assert!(shutdown.reserve_pending_dag_fetch(third));
        release.notify_one();
        shutdown.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_uses_single_global_budget_for_background_tasks() {
        let shutdown = SyncShutdownHandle::new(4);

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

    /// #1309: a periodic loop must exit on the shutdown signal, not at the end
    /// of its sleep. The pending-DAG sweeps used a bare `sleep(interval)`, so a
    /// task spawned before shutdown kept its `Arc<SyncCoordinator>` (and through
    /// it the store) alive for up to the interval: 60s for the resync sweep.
    ///
    /// The interval here is an hour on purpose. Without the cancellation arm
    /// this test cannot pass by waiting; it can only pass by being woken.
    #[tokio::test]
    async fn periodic_loop_exits_on_the_signal_not_the_interval() {
        let shutdown = SyncShutdownHandle::new(4);
        let exited = Arc::new(AtomicBool::new(false));

        let loop_shutdown = shutdown.clone();
        let loop_exited = Arc::clone(&exited);
        let task = tokio::spawn(async move {
            loop {
                if loop_shutdown.is_shutting_down() {
                    break;
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(3600)) => {}
                    _ = loop_shutdown.cancelled() => break,
                }
            }
            loop_exited.store(true, Ordering::SeqCst);
        });

        // Let the loop reach its sleep so the wakeup, not the entry check, is
        // what ends it.
        tokio::task::yield_now().await;
        assert!(
            !exited.load(Ordering::SeqCst),
            "loop must still be parked before shutdown is signalled"
        );

        shutdown.shutdown().await;

        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("loop must wake on the signal, not wait out its interval")
            .expect("loop task should not panic");
        assert!(exited.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancelled_returns_immediately_when_shutdown_already_began() {
        let shutdown = SyncShutdownHandle::new(4);
        shutdown.shutdown().await;

        tokio::time::timeout(Duration::from_secs(5), shutdown.cancelled())
            .await
            .expect("cancelled() must not park once shutdown has begun");
    }

    /// The register-then-check ordering in `cancelled()` is what makes this
    /// pass: a waiter that observed the flag as false must still be woken by
    /// the `notify_waiters` that follows the flag store.
    #[tokio::test]
    async fn cancelled_does_not_miss_a_shutdown_racing_its_registration() {
        for _ in 0..256 {
            let shutdown = SyncShutdownHandle::new(4);
            let waiter_shutdown = shutdown.clone();
            let waiter = tokio::spawn(async move { waiter_shutdown.cancelled().await });

            let signaller = tokio::spawn(async move { shutdown.shutdown().await });

            tokio::time::timeout(Duration::from_secs(5), waiter)
                .await
                .expect("cancelled() lost the wakeup and parked forever")
                .expect("waiter should not panic");
            signaller.await.expect("signaller should not panic");
        }
    }
}
