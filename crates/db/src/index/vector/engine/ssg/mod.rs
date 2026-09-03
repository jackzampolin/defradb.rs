//! SSG: one flat graph, pruned so a node's edges point in different
//! directions.
//!
//! Fu, Wang, Cai 2019 (arXiv:1907.06146). The paper starts from a kNN graph
//! built by NN-Descent, which holds `N * K` neighbour entries resident: roughly
//! 384 MB at a million nodes. That does not stream, so the kNN graph here is
//! HNSW's layer 0 instead, which is built incrementally as documents arrive and
//! is already persisted.
//!
//! **That is a deviation from the paper**, and what it costs in recall is
//! measured rather than assumed.
//!
//! Until a build runs, the index answers from the HNSW graph it is built on, so
//! a young index is approximate-but-good rather than exact-but-linear.

mod build;
mod codec;
mod params;
mod search;

pub use build::SsgBuildReport;
pub use codec::BuiltState;
pub use params::{SsgParams, DEFAULT_ANGLE, DEFAULT_POOL, DEFAULT_R, MAX_POOL, MAX_R};

use crate::index::error::Result;
use crate::index::vector::engine::ann::{Admit, EngineKind, Neighbor, VectorIndexEngine};
use crate::index::vector::engine::hnsw::Hnsw;
use crate::index::vector::params::Params;
use crate::index::vector::store::{NodeId, VectorNodeStore};
use defra_core::vector::{Element, Metric};

#[derive(Debug, Clone)]
pub struct Ssg<S> {
    staging: Hnsw<S>,
    metric: Metric,
    params: SsgParams,
}

impl<S: VectorNodeStore> Ssg<S> {
    pub fn try_new(
        store: S,
        metric: Metric,
        graph: Params,
        params: SsgParams,
        seed: u64,
    ) -> Result<Self> {
        params.validate()?;
        Ok(Self {
            staging: Hnsw::new(store, metric, graph, seed),
            metric,
            params,
        })
    }

    pub fn store(&self) -> &S {
        self.staging.store()
    }

    pub fn store_mut(&mut self) -> &mut S {
        self.staging.store_mut()
    }

    pub fn params(&self) -> SsgParams {
        self.params
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    /// The built state, or `None` while the index still answers from HNSW.
    pub async fn built(&self) -> Result<Option<BuiltState>> {
        match self.store().get_aux(codec::STATE, b"").await? {
            Some(bytes) => codec::decode_state(&bytes).map(Some),
            None => Ok(None),
        }
    }

    pub async fn is_built(&self) -> Result<bool> {
        Ok(self.built().await?.is_some())
    }

    /// The edges kept for `id` after pruning.
    pub async fn neighbours(&self, id: NodeId) -> Result<Vec<NodeId>> {
        match self
            .store()
            .get_aux(codec::ADJACENCY, &codec::node_key(id))
            .await?
        {
            Some(bytes) => codec::decode_neighbours(&bytes),
            None => Ok(Vec::new()),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S: VectorNodeStore> VectorIndexEngine for Ssg<S> {
    fn kind(&self) -> EngineKind {
        EngineKind::Ssg
    }

    /// Writes go to the HNSW graph, which is both the staging structure and the
    /// input a later build reads.
    async fn insert<E: Element>(&mut self, id: NodeId, vector: &[E]) -> Result<()> {
        self.staging.insert(id, vector).await?;
        if self.is_built().await? {
            self.attach(id).await?;
        }
        Ok(())
    }

    async fn delete(&mut self, id: NodeId) -> Result<bool> {
        self.staging.delete(id).await
    }

    async fn search_where<E: Element, A: Admit>(
        &self,
        query: &[E],
        k: usize,
        effort: Option<usize>,
        admit: &A,
    ) -> Result<Vec<Neighbor>> {
        match self.built().await? {
            None => self.staging.search_where(query, k, effort, admit).await,
            Some(state) => self.walk(&state, query, k, effort, admit).await,
        }
    }
}
