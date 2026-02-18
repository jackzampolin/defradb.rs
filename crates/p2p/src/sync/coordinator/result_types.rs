//! Result types for coordinator operations.

/// Result of setting a replicator with auto-subscribe.
#[derive(Debug, Clone)]
pub struct CreateReplicatorResult {
    /// Collections that were successfully subscribed.
    pub subscribed: Vec<String>,
    /// Collections that failed to subscribe (with error messages).
    pub failed_subscriptions: Vec<(String, String)>,
}

impl CreateReplicatorResult {
    /// Returns true if all subscriptions succeeded.
    pub fn all_subscribed(&self) -> bool {
        self.failed_subscriptions.is_empty()
    }

    /// Returns true if any subscription failed.
    pub fn has_failures(&self) -> bool {
        !self.failed_subscriptions.is_empty()
    }
}

/// Result of loading multiple replicators.
#[derive(Debug, Clone, Default)]
pub struct LoadReplicatorsResult {
    /// Number of replicators successfully loaded.
    pub loaded: usize,
    /// Peer IDs that were skipped due to invalid format.
    pub skipped_invalid_ids: Vec<String>,
    /// Peer IDs that failed to load with error messages.
    pub failed: Vec<(String, String)>,
    /// Collections that failed to subscribe (across all replicators).
    pub failed_subscriptions: Vec<(String, String)>,
}
