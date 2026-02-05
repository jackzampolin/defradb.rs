//! Configuration for the replication loop.

/// Configuration for the replication loop.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Whether to continue on merge errors or stop the loop
    pub continue_on_error: bool,
    /// Whether to re-broadcast successfully merged blocks
    pub rebroadcast_on_merge: bool,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            continue_on_error: true,
            rebroadcast_on_merge: false,
        }
    }
}
