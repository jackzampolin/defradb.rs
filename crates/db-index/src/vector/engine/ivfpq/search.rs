//! Probing lists and scanning codes.

use std::collections::BinaryHeap;

use super::codec::{self, TrainedState};
use super::IvfPq;
use crate::error::Result;
use crate::vector::core::{normalize, Element, Metric};
use crate::vector::engine::ann::{Admit, Neighbor, Quantizer};
use crate::vector::store::{NodeId, VectorNodeStore};

impl<S: VectorNodeStore> IvfPq<S> {
    pub(super) async fn search_lists<E: Element, A: Admit>(
        &self,
        state: &TrainedState,
        query: &[E],
        k: usize,
        effort: Option<usize>,
        admit: &A,
    ) -> Result<Vec<Neighbor>> {
        if k == 0 {
            return Ok(Vec::new());
        }

        let mut query: Vec<f32> = query.iter().map(|x| f32::narrow(x.widen())).collect();
        if self.metric() == Metric::Cosine {
            normalize(&mut query);
        }

        let (coarse, quantizer) = self.trained_parts(state).await?;

        // `effort` overrides nprobe the way ef_search does for a graph.
        let nprobe = effort
            .map(|e| e.max(1))
            .unwrap_or(self.params().nprobe as usize)
            .min(coarse.k)
            .max(1);

        let mut lists: Vec<(usize, f64)> = (0..coarse.k)
            .map(|index| {
                (
                    index,
                    Metric::Euclidean.distance(query.as_slice(), coarse.get(index)),
                )
            })
            .collect();
        lists.sort_by(|a, b| a.1.total_cmp(&b.1));
        lists.truncate(nprobe);

        let mut best: BinaryHeap<Ranked> = BinaryHeap::with_capacity(k + 1);
        let mut residual = vec![0.0f32; state.dimensions as usize];

        for (list, _) in lists {
            for (slot, (q, c)) in residual.iter_mut().zip(query.iter().zip(coarse.get(list))) {
                *slot = q - c;
            }
            let table = quantizer.distance_table(&residual);

            let mut hits: Vec<(NodeId, f64)> = Vec::new();
            self.store()
                .iterate_aux(
                    codec::LIST,
                    &codec::list_prefix(list as u32),
                    |key, code| {
                        let id = codec::node_from_list_key(key)?;
                        if admit.admits(id) {
                            hits.push((id, quantizer.distance(&table, code)));
                        }
                        Ok(())
                    },
                )
                .await?;

            for (id, distance) in hits {
                best.push(Ranked { id, distance });
                if best.len() > k {
                    best.pop();
                }
            }
        }

        // A tombstoned document keeps its code, so liveness is checked once on
        // the survivors rather than on every candidate.
        let mut ranked: Vec<Ranked> = best.into_sorted_vec();
        let mut out = Vec::with_capacity(ranked.len());
        for candidate in ranked.drain(..) {
            let live = self
                .store()
                .get_node(candidate.id)
                .await?
                .is_some_and(|node| !node.deleted);
            if live {
                out.push(Neighbor {
                    id: candidate.id,
                    distance: candidate.distance,
                });
            }
        }
        Ok(out)
    }
}

/// Farthest on top, so the heap drops its worst.
#[derive(Debug, PartialEq)]
struct Ranked {
    id: NodeId,
    distance: f64,
}

impl Eq for Ranked {}

impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| other.id.cmp(&self.id))
    }
}

impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
