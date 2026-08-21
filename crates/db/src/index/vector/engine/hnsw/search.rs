//! Layer descent and the ef-bounded greedy search of one layer.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

use super::{Candidate, Hnsw};
use crate::index::error::{Error, Result};
use crate::index::vector::core::Element;
use crate::index::vector::engine::ann::{Admit, AdmitAll, Neighbor};
use crate::index::vector::store::{NodeId, VectorNodeStore};

impl<S: VectorNodeStore> Hnsw<S> {
    /// Up to `k` nearest live nodes to `query`, nearest first.
    ///
    /// K-NN-SEARCH (paper Algorithm 5): descend greedily from the entry point
    /// at the top layer down to layer 1, then search layer 0 with
    /// `ef = max(ef_search, k)`. An empty graph gives an empty result, not an
    /// error.
    pub async fn search_with_ef<E: Element>(
        &self,
        query: &[E],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<Neighbor>> {
        self.search_with_ef_where(query, k, ef_search, &AdmitAll)
            .await
    }

    /// [`search_with_ef`](Self::search_with_ef) restricted to the nodes
    /// `admit` accepts.
    ///
    /// Returns a full `k` whenever `k` admitted nodes are reachable: the walk
    /// stops early only once it holds `ef >= k` *admitted* results nothing in
    /// the frontier can improve on, and otherwise runs until the reachable
    /// graph is exhausted. A shortfall means the corpus has fewer matches.
    pub async fn search_with_ef_where<E: Element, A: Admit>(
        &self,
        query: &[E],
        k: usize,
        ef_search: usize,
        admit: &A,
    ) -> Result<Vec<Neighbor>> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let ef = ef_search.max(k);

        let Some(meta) = self.store.get_meta().await? else {
            return Ok(Vec::new());
        };
        let Some(entry) = self.store.get_node(meta.entry_point).await? else {
            return Ok(Vec::new());
        };

        let query = self.prepared(query);
        if query.len() != entry.vector.len() {
            return Err(Error::VectorDimensionMismatch {
                indexed: entry.vector.len(),
                got: query.len(),
            });
        }
        let mut current = self.candidate(&query, entry);

        for layer in (1..=meta.top_layer).rev() {
            // The descent routes rather than answers, so it admits everything:
            // a layer with no admitted node would otherwise strand the walk.
            if let Some(best) = self
                .search_greedy(&query, current.id, layer)
                .await?
                .next_best()
            {
                current = best;
            }
        }

        let mut found = self
            .search_layer(&query, vec![current], ef, 0, admit)
            .await?;
        found.truncate(k);
        Ok(found
            .into_iter()
            .map(|c| Neighbor {
                id: c.id,
                distance: c.distance,
            })
            .collect())
    }

    /// The single closest node reachable in `layer` from one entry point.
    /// Used to descend toward the layer where the real work happens.
    pub(super) async fn search_greedy(
        &self,
        query: &[f32],
        entry: NodeId,
        layer: usize,
    ) -> Result<Vec<Candidate>> {
        let seed = Candidate {
            id: entry,
            distance: 0.0,
            vector: Vec::new().into(),
        };
        self.search_layer(query, vec![seed], 1, layer, &AdmitAll)
            .await
    }

    /// SEARCH-LAYER (paper Algorithm 2), seeded with a set of entry points
    /// rather than one: an insert feeds each layer every neighbor found on the
    /// layer above, which is a wider starting frontier and finds better links.
    ///
    /// Tombstoned nodes are walked through, so the graph stays connected, but
    /// never returned. The result is sorted nearest-first.
    ///
    /// Entry points are re-read from the store rather than trusted, because the
    /// descent passes ids with a placeholder distance.
    pub(super) async fn search_layer<A: Admit>(
        &self,
        query: &[f32],
        entry_points: Vec<Candidate>,
        ef: usize,
        layer: usize,
        admit: &A,
    ) -> Result<Vec<Candidate>> {
        let mut visited: HashSet<NodeId> = HashSet::new();
        // Nearest pops first: the frontier explores closest-first.
        let mut frontier: BinaryHeap<Reverse<Candidate>> = BinaryHeap::new();
        // Farthest pops first: the worst result is cheap to drop past `ef`.
        let mut results: BinaryHeap<Candidate> = BinaryHeap::new();

        for entry in entry_points {
            if !visited.insert(entry.id) {
                continue;
            }
            let Some(node) = self.store.get_node(entry.id).await? else {
                continue;
            };
            let admitted = !node.deleted && admit.admits(node.id);
            let candidate = self.candidate(query, node);
            if admitted {
                results.push(candidate.clone());
            }
            frontier.push(Reverse(candidate));
        }

        while let Some(Reverse(nearest)) = frontier.peek() {
            let nearest_distance = nearest.distance;
            // Once the results are full and the nearest unexplored node is
            // farther than the worst result, nothing reachable can beat it.
            // Only the outer loop breaks: neighbors are unordered, so within a
            // node a closer one can follow a farther one.
            if results.len() >= ef
                && results
                    .peek()
                    .is_some_and(|worst| nearest_distance > worst.distance)
            {
                break;
            }
            let Some(Reverse(nearest)) = frontier.pop() else {
                break;
            };

            let Some(node) = self.store.get_node(nearest.id).await? else {
                continue;
            };

            for &neighbor_id in node.neighbors(layer) {
                if !visited.insert(neighbor_id) {
                    continue;
                }
                let Some(neighbor) = self.store.get_node(neighbor_id).await? else {
                    continue;
                };
                // Rejected nodes are still routes, exactly as tombstones are.
                let admitted = !neighbor.deleted && admit.admits(neighbor.id);
                let candidate = self.candidate(query, neighbor);

                // Extend the frontier only where it could still matter. This
                // bounds how far the walk spreads; it does not end the loop.
                let worst = results.peek().map(|c| c.distance);
                if results.len() < ef || worst.is_some_and(|worst| candidate.distance < worst) {
                    if admitted {
                        results.push(candidate.clone());
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                    frontier.push(Reverse(candidate));
                }
            }
        }

        let mut out = results.into_vec();
        out.sort();
        Ok(out)
    }
}

/// Reads better at the descent call site than `into_iter().next()`.
trait NextBest {
    fn next_best(self) -> Option<Candidate>;
}

impl NextBest for Vec<Candidate> {
    fn next_best(self) -> Option<Candidate> {
        self.into_iter().next()
    }
}
