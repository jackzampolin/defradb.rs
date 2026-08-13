//! The candidate a search carries around, and its ordering.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::vector::store::NodeId;

/// A node paired with its distance to the current query.
///
/// It carries the node's own vector so the neighbor-selection heuristic can
/// measure candidates against each other without going back to the store. That
/// is the difference between one store read per candidate and `m` per
/// candidate.
///
/// Shared rather than owned because every hop clones a candidate into both the
/// frontier and the result set, and an embedding is a few kilobytes.
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub id: NodeId,
    pub distance: f64,
    pub vector: Arc<[f32]>,
}

impl Eq for Candidate {}

impl Ord for Candidate {
    /// Nearest is *least*, so `BinaryHeap<Candidate>` pops the farthest (the
    /// result set drops its worst) and `BinaryHeap<Reverse<Candidate>>` pops
    /// the nearest (the frontier explores closest-first).
    ///
    /// `total_cmp` rather than `partial_cmp`: it is total for every `f64`
    /// including NaN, so no comparison can panic even though the metrics
    /// already promise never to produce one. The id breaks ties, which keeps
    /// heap order deterministic for equidistant nodes.
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
