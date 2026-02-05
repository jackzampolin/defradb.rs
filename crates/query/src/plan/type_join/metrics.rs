//! Shared metrics types for type join nodes

/// Metrics for simulating Go-compatible child scan behavior.
///
/// In Go DefraDB, the child scan is re-initialized per parent, and metrics
/// accumulate across all parent scans. This struct tracks those accumulated
/// metrics to match Go's explain execute output.
#[derive(Debug, Default, Clone)]
pub struct JoinChildMetrics {
    /// Number of Next() iterations on the child scan
    pub iterations: u64,
    /// Number of documents fetched from storage
    pub doc_fetches: u64,
    /// Number of fields fetched from storage
    pub field_fetches: u64,
    /// Number of index entries fetched
    pub index_fetches: u64,
}

impl JoinChildMetrics {
    /// Create new zeroed metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset all metrics to zero.
    pub fn reset(&mut self) {
        self.iterations = 0;
        self.doc_fetches = 0;
        self.field_fetches = 0;
        self.index_fetches = 0;
    }

    /// Convert to JSON object for explain execute output.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "iterations": self.iterations,
            "docFetches": self.doc_fetches,
            "fieldFetches": self.field_fetches,
            "indexFetches": self.index_fetches
        })
    }
}
