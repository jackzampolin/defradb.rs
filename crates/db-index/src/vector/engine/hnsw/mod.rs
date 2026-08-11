//! The HNSW engine: graph construction, layer descent, candidate heaps.
//!
//! Ported from Go's `internal/index/hnsw` (PR 5096), which is itself Malkov &
//! Yashunin, "Efficient and robust approximate nearest neighbor search using
//! Hierarchical Navigable Small World graphs" (arXiv:1603.09320). Parameter
//! Parameter names, defaults, traversal order and link maintenance follow that
//! reference. Two things deliberately do not, both marked where they occur.
//! Neither is a compatibility break: a vector index is local state, and the two
//! implementations are not required to read each other's files, so the
//! reference is a starting point rather than a specification.
//!
//! - **A new node is stored before its back-links are added**, so a neighbor at
//!   capacity can weigh it against the links it already has. In the reference
//!   the node is not stored yet, reads as absent during that pruning, and loses
//!   the back-link it was just given (see [`insert`](Hnsw::insert)).
//! - **Distances stay `f64`**, where the reference narrows each to `f32`.
//!
//! The level assignment also differs, but not by choice: it comes from a
//! pseudo-random draw. The distribution is the reference's,
//! `floor(-ln(u) * ml)`, and the seed is the caller's either way. Node heights
//! are local state that nothing compares across runtimes.
//!
//! The engine holds no lock. The reference serializes writers with a mutex
//! because its store is a shared map; here every mutation runs inside a
//! transaction, which is what provides the single-writer discipline, and a
//! `Mutex` held across an `await` is exactly what this workspace lints against.

mod candidate;
mod insert;
mod level;
mod search;

pub use candidate::Candidate;
pub use level::LevelSampler;

use super::ann::{IndexKind, Neighbor, VectorIndexEngine};
use crate::error::Result;
use crate::vector::core::{metric::normalize, Element, Metric};
use crate::vector::params::Params;
use crate::vector::store::{Node, NodeId, VectorNodeStore};

/// An approximate-nearest-neighbor graph over a [`VectorNodeStore`].
#[derive(Debug)]
pub struct Hnsw<S> {
    store: S,
    params: Params,
    metric: Metric,
    sampler: LevelSampler,
}

impl<S> Hnsw<S> {
    /// `seed` makes level generation deterministic for a given sequence of
    /// inserts, which is what makes a measured recall figure reproducible.
    pub fn new(store: S, metric: Metric, params: Params, seed: u64) -> Self {
        Self {
            store,
            params,
            metric,
            sampler: LevelSampler::new(seed),
        }
    }

    pub fn params(&self) -> &Params {
        &self.params
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// Unwraps the graph, for a caller that needs the store back.
    pub fn into_store(self) -> S {
        self.store
    }

    /// The form a vector is stored and compared in, and where an incoming
    /// width becomes the stored one.
    ///
    /// Nodes hold `f32`: it is what embedding models emit and what the
    /// reference stores, and an `f64` query gains nothing against an `f32`
    /// corpus. A vector with no usable direction is stored unchanged, matching
    /// the reference; rejecting it belongs at the index boundary, where there
    /// is a document to name.
    fn prepared<E: Element>(&self, vector: &[E]) -> Vec<f32> {
        let mut out: Vec<f32> = vector.iter().map(|x| f32::narrow(x.widen())).collect();
        if self.metric == Metric::Cosine {
            normalize(&mut out);
        }
        out
    }

    /// Distance between two vectors already in stored form.
    fn distance(&self, a: &[f32], b: &[f32]) -> f64 {
        if self.metric == Metric::Cosine {
            self.metric.distance_normalized(a, b)
        } else {
            self.metric.distance(a, b)
        }
    }
}

impl<S: VectorNodeStore> Hnsw<S> {
    /// Tombstones `id`, returning whether this call was the one that did it.
    ///
    /// Links are left intact on purpose, even when the tombstoned node is the
    /// entry point: traversal still routes through it, so the graph stays
    /// connected. Unlinking and reclaiming the space needs a rebuild pass,
    /// which the epoch key layout supports and nothing yet triggers.
    pub async fn delete(&mut self, id: NodeId) -> Result<bool> {
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

    /// Builds a candidate for a node already loaded from the store.
    fn candidate(&self, query: &[f32], node: Node) -> Candidate {
        Candidate {
            id: node.id,
            distance: self.distance(query, &node.vector),
            vector: node.vector.into(),
        }
    }

    /// Loads `ids` and pairs each with its distance to `query`.
    ///
    /// Ids that are not in the store are skipped: a link can outlive the node
    /// it points at, and one fewer link is not a failure.
    async fn candidates_from_ids(&self, query: &[f32], ids: &[NodeId]) -> Result<Vec<Candidate>> {
        let mut out = Vec::with_capacity(ids.len());
        for &id in ids {
            if let Some(node) = self.store.get_node(id).await? {
                out.push(self.candidate(query, node));
            }
        }
        Ok(out)
    }

    /// SELECT-NEIGHBORS-HEURISTIC (paper Algorithm 4).
    ///
    /// Walks candidates nearest-first and keeps `e` only when it is closer to
    /// the query than to every neighbor already kept. That diversity condition
    /// is what stops a node's links from all pointing into the same cluster,
    /// and it is worth materially more recall than taking the nearest `m`.
    ///
    /// The paper's optional `extendCandidates` and `keepPrunedConnections`
    /// refinements are not implemented, matching the reference.
    fn select_neighbors(&self, candidates: &[Candidate], m: usize) -> Vec<Candidate> {
        let mut sorted = candidates.to_vec();
        sorted.sort();

        let mut selected: Vec<Candidate> = Vec::with_capacity(m);
        for candidate in sorted {
            if selected.len() >= m {
                break;
            }
            let diverse = selected
                .iter()
                .all(|kept| self.distance(&candidate.vector, &kept.vector) >= candidate.distance);
            if diverse {
                selected.push(candidate);
            }
        }
        selected
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S: VectorNodeStore> VectorIndexEngine for Hnsw<S> {
    fn kind(&self) -> IndexKind {
        IndexKind::Hnsw
    }

    async fn insert<E: Element>(&mut self, id: NodeId, vector: &[E]) -> Result<()> {
        Hnsw::insert(self, id, vector).await
    }

    async fn delete(&mut self, id: NodeId) -> Result<bool> {
        Hnsw::delete(self, id).await
    }

    /// `effort` is `ef_search`, defaulting to the configured value.
    async fn search<E: Element>(
        &self,
        query: &[E],
        k: usize,
        effort: Option<usize>,
    ) -> Result<Vec<Neighbor>> {
        self.search_with_ef(query, k, effort.unwrap_or(self.params.ef_search))
            .await
    }
}
