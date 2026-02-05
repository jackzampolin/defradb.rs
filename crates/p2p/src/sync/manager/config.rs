//! Sync manager configuration.

/// Configuration for the SyncManager.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Size of the event channel buffer.
    pub event_buffer_size: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            event_buffer_size: 256,
        }
    }
}
