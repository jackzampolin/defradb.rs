//! Sync manager configuration.

/// Default maximum number of concurrent DAG fetch tasks.
///
/// Lowered from 16 to 4 for mobile client compatibility — 16 concurrent
/// fetchers exhaust rate limiters and crash QUIC on constrained networks.
pub const DEFAULT_MAX_CONCURRENT_DAG_FETCHES: usize = 4;

/// Default maximum number of concurrent push tasks.
pub const DEFAULT_MAX_CONCURRENT_PUSH_TASKS: usize = 8;

/// Default per-peer rate limit burst capacity.
pub const DEFAULT_RATE_LIMIT_BURST: u32 = 500;

/// Default per-peer rate limit refill rate (tokens per second).
pub const DEFAULT_RATE_LIMIT_RATE: f64 = 50.0;

/// Configuration for the SyncManager.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Size of the event channel buffer.
    pub event_buffer_size: usize,

    /// Maximum number of concurrent DAG fetch tasks spawned by the coordinator.
    ///
    /// Caps fan-out from DocSync and BranchableSync replies to prevent resource
    /// exhaustion from a peer advertising a large number of head CIDs.
    pub max_concurrent_dag_fetches: usize,

    /// Maximum number of concurrent push tasks for sending blocks to replicators.
    ///
    /// Caps fan-out from `push_dag_to_replicators` and `push_to_replicators` to
    /// prevent resource exhaustion when many documents are created in a burst.
    pub max_concurrent_push_tasks: usize,

    /// Per-peer rate limit burst capacity (max tokens in bucket).
    pub rate_limit_burst: u32,

    /// Per-peer rate limit refill rate (tokens per second).
    pub rate_limit_rate: f64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            event_buffer_size: 256,
            max_concurrent_dag_fetches: DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
            max_concurrent_push_tasks: DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
            rate_limit_burst: DEFAULT_RATE_LIMIT_BURST,
            rate_limit_rate: DEFAULT_RATE_LIMIT_RATE,
        }
    }
}
