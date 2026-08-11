//! Which algorithm an index uses.

/// The kinds that exist. The string form is what a description and a diagnostic
/// report, so it is defined next to the kinds rather than spelled out at every
/// call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IndexKind {
    /// Hierarchical Navigable Small World graph.
    Hnsw,
    /// Exhaustive scan. Exact, and linear in the corpus.
    Flat,
}

impl IndexKind {
    pub fn as_str(self) -> &'static str {
        match self {
            IndexKind::Hnsw => "HNSW",
            IndexKind::Flat => "FLAT",
        }
    }
}
