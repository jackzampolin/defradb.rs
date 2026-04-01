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
    /// Number of documents that passed the selectNode filter
    pub filter_matches: u64,
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
        self.filter_matches = 0;
    }

    /// Convert scanNode metrics to JSON for explain execute output.
    pub fn scan_node_json(&self) -> serde_json::Value {
        serde_json::json!({
            "iterations": self.iterations,
            "docFetches": self.doc_fetches,
            "fieldFetches": self.field_fetches,
            "indexFetches": self.index_fetches
        })
    }

    /// Convert scanNode metrics to JSON with an overridden indexFetches value.
    pub fn scan_node_json_with_index_fetches(&self, index_fetches: u64) -> serde_json::Value {
        serde_json::json!({
            "iterations": self.iterations,
            "docFetches": self.doc_fetches,
            "fieldFetches": self.field_fetches,
            "indexFetches": index_fetches
        })
    }

    /// Convert to a full selectTopNode > selectNode > scanNode JSON structure.
    ///
    /// In Go, the parent side plan is a selectNode wrapping a scanNode.
    /// The selectNode tracks its own iterations (same as scanNode iterations
    /// when no filter rejection) and filterMatches.
    #[allow(dead_code)]
    pub fn to_select_node_json(&self) -> serde_json::Value {
        serde_json::json!({
            "selectTopNode": {
                "selectNode": {
                    "iterations": self.iterations,
                    "filterMatches": self.filter_matches,
                    "scanNode": self.scan_node_json()
                }
            }
        })
    }
}
