//! Probing lists and scanning full-precision vectors.

use std::collections::BinaryHeap;

use super::IvfFlat;
use crate::index::error::Result;
use crate::index::vector::engine::ann::{Admit, Neighbor};
use crate::index::vector::engine::ivf::{self, TrainedState};
use crate::index::vector::store::{NodeId, VectorNodeStore};
use defra_core::vector::Element;

impl<S: VectorNodeStore> IvfFlat<S> {
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

        let query = self.metric().prepare(query);
        let metric = self.metric();

        let centroids = self.trained_centroids(state).await?;
        let lists = ivf::probe_lists(
            query.as_slice(),
            centroids,
            self.params().nprobe as usize,
            effort,
        );

        // A max-heap of size k: the worst of the current best is on top, so a
        // new candidate is one comparison and at most one pop, exactly as
        // `Flat` scores its own scan.
        let mut best: BinaryHeap<Ranked> = BinaryHeap::with_capacity(k + 1);
        for list in lists {
            self.store()
                .iterate_aux(ivf::LIST, &ivf::list_prefix(list as u32), |key, value| {
                    let id = ivf::node_from_list_key(key)?;
                    if !admit.admits(id) {
                        return Ok(());
                    }
                    let vector = ivf::decode_vector(value)?;
                    let distance = metric.distance_stored(query.as_slice(), &vector);
                    best.push(Ranked { id, distance });
                    if best.len() > k {
                        best.pop();
                    }
                    Ok(())
                })
                .await?;
        }

        // A tombstoned document keeps its list entry, so liveness is checked
        // once on the survivors rather than on every candidate.
        let ranked: Vec<Ranked> = best.into_sorted_vec();
        let mut out = Vec::with_capacity(ranked.len());
        for candidate in ranked {
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

/// Farthest on top, so the heap drops its worst; ties break by ascending id.
/// This has to agree with [`Flat`](crate::index::vector::engine::flat::Flat)
/// and [`Hnsw`](crate::index::vector::engine::hnsw::Hnsw), because IVF_FLAT is
/// differentially tested against `Flat` at `nprobe == nlist`. IVF-PQ
/// tie-breaks the other way (#1469); that is its own bug to fix, not this
/// engine's to inherit.
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
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
