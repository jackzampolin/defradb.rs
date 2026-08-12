//! Which engine is behind a [`VectorIndexEngine`](super::VectorIndexEngine).
//!
//! Distinct from `schema::IndexKind`, which says whether an index is ordered or
//! a vector index at all. This one only distinguishes vector engines from each
//! other.

/// The engines that exist. The string form is what a diagnostic reports, so it
/// is defined next to them rather than spelled out at every call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EngineKind {
    /// Hierarchical Navigable Small World graph.
    Hnsw,
    /// Exhaustive scan. Exact, and linear in the corpus.
    Flat,
}

impl EngineKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EngineKind::Hnsw => "HNSW",
            EngineKind::Flat => "FLAT",
        }
    }
}
