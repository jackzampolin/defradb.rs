//! The single-layer greedy walk.

use std::collections::{BinaryHeap, HashSet};

use super::codec::BuiltState;
use super::Ssg;
use crate::index::error::Result;
use crate::index::vector::engine::ann::{Admit, Candidate, Neighbor};
use crate::index::vector::store::{NodeId, VectorNodeStore};
use defra_core::vector::Element;

impl<S: VectorNodeStore> Ssg<S> {
    pub(super) async fn walk<E: Element, A: Admit>(
        &self,
        state: &BuiltState,
        query: &[E],
        k: usize,
        effort: Option<usize>,
        admit: &A,
    ) -> Result<Vec<Neighbor>> {
        if k == 0 {
            return Ok(Vec::new());
        }

        let metric = self.metric();
        let query = metric.prepare(query);

        let pool = effort
            .map(|e| e.max(1))
            .unwrap_or(self.params().pool as usize)
            .max(k);

        let Some(entry) = self.store().get_node(state.entry_point).await? else {
            return Ok(Vec::new());
        };

        let mut seen: HashSet<NodeId> = HashSet::with_capacity(pool * 2);
        seen.insert(entry.id);

        // Nearest-first frontier, farthest-first results: the same pairing the
        // graph kinds use, so the walk stops when nothing can improve on what
        // is already held.
        let mut frontier: BinaryHeap<std::cmp::Reverse<Candidate>> = BinaryHeap::new();
        let mut results: BinaryHeap<Candidate> = BinaryHeap::new();

        let start = Candidate {
            distance: metric.distance_stored(&query, &entry.vector),
            id: entry.id,
            vector: entry.vector.into(),
        };
        frontier.push(std::cmp::Reverse(start.clone()));
        if !entry.deleted && admit.admits(entry.id) {
            results.push(start);
        }

        while let Some(std::cmp::Reverse(current)) = frontier.pop() {
            if results.len() >= pool {
                if let Some(worst) = results.peek() {
                    if current.distance > worst.distance {
                        break;
                    }
                }
            }

            for neighbour in self.neighbours(current.id).await? {
                if !seen.insert(neighbour) {
                    continue;
                }
                let Some(node) = self.store().get_node(neighbour).await? else {
                    continue;
                };
                let candidate = Candidate {
                    distance: metric.distance_stored(&query, &node.vector),
                    id: node.id,
                    vector: node.vector.into(),
                };

                // A rejected node is still walked through, exactly as a
                // tombstone is: skipping it would strand whatever lies behind.
                frontier.push(std::cmp::Reverse(candidate.clone()));
                if node.deleted || !admit.admits(node.id) {
                    continue;
                }
                results.push(candidate);
                if results.len() > pool {
                    results.pop();
                }
            }
        }

        let mut ranked = results.into_sorted_vec();
        ranked.truncate(k);
        Ok(ranked
            .into_iter()
            .map(|c| Neighbor {
                id: c.id,
                distance: c.distance,
            })
            .collect())
    }
}
