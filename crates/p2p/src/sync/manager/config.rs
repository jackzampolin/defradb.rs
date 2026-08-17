//! Sync manager configuration.

use std::time::Duration;

use crate::message::MAX_DOC_IDS;

/// Default maximum number of concurrent DAG fetch tasks.
///
/// Lowered from 16 to 4 for mobile client compatibility — 16 concurrent
/// fetchers exhaust rate limiters and crash QUIC on constrained networks.
pub const DEFAULT_MAX_CONCURRENT_DAG_FETCHES: usize = 4;

/// Default maximum number of concurrent push tasks.
///
/// This is the size of the fixed outbound push worker pool: exactly this many
/// worker tasks drain the push backlog for the coordinator's lifetime.
pub const DEFAULT_MAX_CONCURRENT_PUSH_TASKS: usize = 8;

/// Default maximum queued outbound push jobs (#1099).
///
/// Jobs queue as compact specs (head block + identifiers); overflow is
/// rejected with an explicit outcome that feeds the persisted retry ladder.
pub const DEFAULT_PUSH_QUEUE_CAPACITY: usize = 1024;

/// Default maximum resident bytes across queued outbound push jobs (#1099).
///
/// Prevents a few large head blocks from defeating the item bound. A single
/// job larger than this is admitted only when the queue is empty.
pub const DEFAULT_PUSH_QUEUE_BYTE_CAPACITY: usize = 32 * 1024 * 1024;

/// Default maximum push jobs concurrently in flight to one peer (#1099).
///
/// Must stay below `max_concurrent_push_tasks` so one nonresponsive peer
/// cannot occupy every worker.
pub const DEFAULT_MAX_ACTIVE_PUSHES_PER_PEER: usize = 4;

/// Default maximum document IDs accepted in one DocSync request.
pub const DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS: usize = MAX_DOC_IDS;

/// Default per-peer rate limit burst capacity.
pub const DEFAULT_RATE_LIMIT_BURST: u32 = 500;

/// Default per-peer rate limit refill rate (tokens per second).
pub const DEFAULT_RATE_LIMIT_RATE: f64 = 50.0;

/// Default per-peer rate-limit backoff ladder, in seconds.
///
/// Mirrors the persisted replicator retry ladder: seconds-to-hours, capped at
/// 12 hours, so abusive peers are retried less aggressively over time.
pub const DEFAULT_RATE_LIMIT_BACKOFF_SECS: &[u64] = &[
    30, 60, 120, 240, 480, 960, 1920, 3600, 7200, 14400, 28800, 43200,
];

/// Default timeout for one outbound PushLog send to a replicator peer.
pub const DEFAULT_PUSH_SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Default maximum number of entries in the pending DAGs map.
///
/// Prevents unbounded memory growth when many DAGs arrive faster than they
/// can be resolved via Bitswap. Overflow is nacked back to the pusher with
/// `RATE_LIMITED_MESSAGE` (#1088 W1) so its retry ladder keeps the doc queued.
pub const DEFAULT_MAX_PENDING_DAGS: usize = 1000;

/// Default rate-limit backoff ladder as durations.
pub fn default_rate_limit_backoff() -> Vec<Duration> {
    DEFAULT_RATE_LIMIT_BACKOFF_SECS
        .iter()
        .map(|seconds| Duration::from_secs(*seconds))
        .collect()
}

/// Configuration for the SyncManager.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Size of the event channel buffer.
    pub event_buffer_size: usize,

    /// Maximum number of concurrent DAG fetch tasks spawned by the coordinator.
    ///
    /// Caps fan-out from DocSync, BranchableSync, and push-driven DAG recovery to
    /// prevent resource exhaustion from a peer advertising a large number of head CIDs.
    pub max_concurrent_dag_fetches: usize,

    /// Maximum number of concurrent push tasks for sending blocks to replicators.
    ///
    /// Sizes the fixed worker pool that drains the outbound push backlog, so
    /// fan-out from `push_to_replicators` cannot
    /// exhaust resources when many documents are created in a burst.
    pub max_concurrent_push_tasks: usize,

    /// Maximum queued outbound push jobs before admission rejects overflow.
    pub push_queue_capacity: usize,

    /// Maximum resident bytes across queued outbound push jobs.
    pub push_queue_byte_capacity: usize,

    /// Maximum push jobs concurrently in flight to a single peer.
    pub max_active_pushes_per_peer: usize,

    /// Maximum document IDs accepted in a single DocSync request.
    ///
    /// Keeps pull-based document sync bounded while allowing deployments to tune
    /// static document-ID batch sizes for filtered-replication workflows.
    pub max_doc_sync_request_doc_ids: usize,

    /// Per-peer rate limit burst capacity (max tokens in bucket).
    pub rate_limit_burst: u32,

    /// Per-peer rate limit refill rate (tokens per second).
    pub rate_limit_rate: f64,

    /// Per-peer backoff ladder after rate-limit refusals.
    pub rate_limit_backoff: Vec<Duration>,

    /// Timeout for one outbound PushLog send to a replicator peer.
    pub push_send_timeout: Duration,

    /// Maximum number of pending-DAG registrations held while Bitswap
    /// completes missing links. Overflow is rejected with a backpressure nack.
    /// Each known source peer may occupy at most one quarter of this capacity.
    /// Values below 1 are normalized to 1 (a zero cap would reject every
    /// missing-link push forever).
    pub max_pending_dags: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            event_buffer_size: 256,
            max_concurrent_dag_fetches: DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
            max_concurrent_push_tasks: DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
            push_queue_capacity: DEFAULT_PUSH_QUEUE_CAPACITY,
            push_queue_byte_capacity: DEFAULT_PUSH_QUEUE_BYTE_CAPACITY,
            max_active_pushes_per_peer: DEFAULT_MAX_ACTIVE_PUSHES_PER_PEER,
            max_doc_sync_request_doc_ids: DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
            rate_limit_burst: DEFAULT_RATE_LIMIT_BURST,
            rate_limit_rate: DEFAULT_RATE_LIMIT_RATE,
            rate_limit_backoff: default_rate_limit_backoff(),
            push_send_timeout: DEFAULT_PUSH_SEND_TIMEOUT,
            max_pending_dags: DEFAULT_MAX_PENDING_DAGS,
        }
    }
}
