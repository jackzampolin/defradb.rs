//! HNSW's SELECT-NEIGHBORS-HEURISTIC (paper Algorithm 4).

use crate::index::vector::core::Metric;
use crate::index::vector::engine::ann::{Candidate, EdgeSelector};

/// Keeps a candidate only when it is closer to the query than to every
/// neighbour already kept. `extendCandidates` and `keepPrunedConnections` are
/// not implemented, matching the Go reference.
#[derive(Debug, Clone, Copy, Default)]
pub struct Heuristic;

impl EdgeSelector for Heuristic {
    fn select(
        &self,
        metric: Metric,
        _base: &[f32],
        candidates: &[Candidate],
        max: usize,
    ) -> Vec<Candidate> {
        let mut sorted = candidates.to_vec();
        sorted.sort();

        let mut selected: Vec<Candidate> = Vec::with_capacity(max);
        for candidate in sorted {
            if selected.len() >= max {
                break;
            }
            let diverse = selected.iter().all(|kept| {
                metric.distance_stored(&candidate.vector, &kept.vector) >= candidate.distance
            });
            if diverse {
                selected.push(candidate);
            }
        }
        selected
    }
}
