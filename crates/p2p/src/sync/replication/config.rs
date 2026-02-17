//! Configuration for the replication loop.

/// Configuration for the replication loop.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Whether to continue on merge errors or stop the loop
    pub continue_on_error: bool,
    /// Whether to re-broadcast successfully merged blocks
    pub rebroadcast_on_merge: bool,
    /// Max blocks per batch (matches Go's MergeBatchWithTxn batch size)
    pub batch_size: usize,
    /// Maximum concurrent merge workers for `run_parallel()`
    pub max_workers: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            continue_on_error: true,
            rebroadcast_on_merge: false,
            batch_size: 50,
            max_workers: 32,
        }
    }
}
