//! Construction and search parameters, with Go's names and defaults.

/// Links per node per layer above layer 0, when the caller gives none. Also the
/// fallback for a nonsensical `m`.
pub const DEFAULT_M: usize = 16;

/// Candidate-list size while inserting.
pub const DEFAULT_EF_CONSTRUCTION: usize = 128;

/// Candidate-list size while searching, when the caller does not override it.
pub const DEFAULT_EF_SEARCH: usize = 64;

/// Tunable parameters of an HNSW graph.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Params {
    /// Bidirectional links created per node per layer, except layer 0.
    pub m: usize,
    /// Maximum links a node may hold at layer 0.
    pub m_max0: usize,
    /// Candidate-list size while inserting.
    pub ef_construction: usize,
    /// Default candidate-list size while searching.
    pub ef_search: usize,
    /// Level-generation normalization factor, `1 / ln(m)`.
    pub ml: f64,
}

impl Params {
    /// Go's `DefaultParams(m)`.
    ///
    /// `m < 2` falls back to [`DEFAULT_M`]: `ml` is `1 / ln(1)`, which is
    /// infinite, so level generation would ask every node for an unbounded
    /// number of layers.
    pub fn new(m: usize) -> Self {
        let m = if m < 2 { DEFAULT_M } else { m };
        Self {
            m,
            m_max0: 2 * m,
            ef_construction: DEFAULT_EF_CONSTRUCTION,
            ef_search: DEFAULT_EF_SEARCH,
            ml: 1.0 / (m as f64).ln(),
        }
    }

    /// Maximum links allowed at `layer`.
    pub fn max_links(&self, layer: usize) -> usize {
        if layer == 0 {
            self.m_max0
        } else {
            self.m
        }
    }
}

impl Default for Params {
    fn default() -> Self {
        Self::new(DEFAULT_M)
    }
}
