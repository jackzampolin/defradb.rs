//! Choosing which candidate neighbours become edges.

use super::Candidate;
use crate::index::vector::core::Metric;

/// Taking the nearest `max` loses the long edges a walk needs, so every graph
/// kind prunes for direction and differs only in how it measures it.
pub trait EdgeSelector {
    /// `base` is the node the edges belong to; `candidates` carry their
    /// distance to it.
    fn select(
        &self,
        metric: Metric,
        base: &[f32],
        candidates: &[Candidate],
        max: usize,
    ) -> Vec<Candidate>;
}
