//! Sync manager configuration.

/// Default maximum number of concurrent DAG fetch tasks.
pub const DEFAULT_MAX_CONCURRENT_DAG_FETCHES: usize = 16;

/// Default maximum number of concurrent push tasks.
pub const DEFAULT_MAX_CONCURRENT_PUSH_TASKS: usize = 32;

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
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            event_buffer_size: 256,
            max_concurrent_dag_fetches: DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
            max_concurrent_push_tasks: DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
        }
    }
}
