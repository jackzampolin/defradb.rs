//! Exhaustive scan: the exact answer, linear in the corpus.
//!
//! The oracle a differential test needs: an approximate index cannot be checked
//! against another approximate one. It also keeps [`VectorIndexEngine`] honest,
//! since a trait with one implementor is a guess. Not exposed as a
//! user-selectable kind.

use super::ann::{Admit, EngineKind, Neighbor, VectorIndexEngine};
use crate::error::Result;
use crate::vector::core::{Element, Metric};
use crate::vector::store::{Node, NodeId, VectorNodeStore};

/// Every live node, scored and ranked.
#[derive(Debug, Clone)]
pub struct Flat<S> {
    store: S,
    metric: Metric,
}

impl<S> Flat<S> {
    pub fn new(store: S, metric: Metric) -> Self {
        Self { store, metric }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    pub fn into_store(self) -> S {
        self.store
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S: VectorNodeStore> VectorIndexEngine for Flat<S> {
    fn kind(&self) -> EngineKind {
        EngineKind::Flat
    }

    /// No graph to maintain, so a node is stored with no layers at all.
    async fn insert<E: Element>(&mut self, id: NodeId, vector: &[E]) -> Result<()> {
        let mut vector: Vec<f32> = vector.iter().map(|x| f32::narrow(x.widen())).collect();
        if self.metric == Metric::Cosine {
            crate::vector::core::normalize(&mut vector);
        }
        self.store
            .put_node(Node {
                id,
                vector,
                layers: Vec::new(),
                deleted: false,
            })
            .await
    }

    async fn delete(&mut self, id: NodeId) -> Result<bool> {
        let Some(mut node) = self.store.get_node(id).await? else {
            return Ok(false);
        };
        if node.deleted {
            return Ok(false);
        }
        node.deleted = true;
        self.store.put_node(node).await?;
        Ok(true)
    }

    /// `effort` is ignored: an exhaustive scan has no accuracy to trade. Holds
    /// `k` results rather than scoring the whole corpus and sorting.
    async fn search_where<E: Element, A: Admit>(
        &self,
        query: &[E],
        k: usize,
        _effort: Option<usize>,
        admit: &A,
    ) -> Result<Vec<Neighbor>> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let mut query: Vec<f32> = query.iter().map(|x| f32::narrow(x.widen())).collect();
        if self.metric == Metric::Cosine {
            crate::vector::core::normalize(&mut query);
        }

        // A max-heap of size k: the worst of the current best is on top, so a
        // new candidate is one comparison and at most one pop.
        let mut best: std::collections::BinaryHeap<Ranked> =
            std::collections::BinaryHeap::with_capacity(k + 1);
        let metric = self.metric;
        self.store
            .iterate_nodes(|node| {
                if !admit.admits(node.id) {
                    return Ok(());
                }
                let distance = if metric == Metric::Cosine {
                    metric.distance_normalized(&query, &node.vector)
                } else {
                    metric.distance(&query, &node.vector)
                };
                best.push(Ranked {
                    id: node.id,
                    distance,
                });
                if best.len() > k {
                    best.pop();
                }
                Ok(())
            })
            .await?;

        let mut out: Vec<Ranked> = best.into_vec();
        out.sort();
        Ok(out
            .into_iter()
            .map(|r| Neighbor {
                id: r.id,
                distance: r.distance,
            })
            .collect())
    }
}

/// Nearest is least, so the heap's top is the worst kept result.
#[derive(Debug, Clone, Copy, PartialEq)]
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
