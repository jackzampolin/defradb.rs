//! IVF_FLAT: coarse lists of full-precision vectors.
//!
//! The same partitioning [`ivfpq`](super::ivfpq) uses, with nothing
//! compressed: a list holds the full vector rather than a product-quantized
//! code, so inside the probed lists the ranking is exactly
//! [`Flat`]'s. The only recall loss is a true neighbour sitting in an
//! unprobed list; there is no quantization error at all, which is what makes
//! `nprobe == nlist` byte-identical to `Flat`, ties included.
//!
//! A list entry is `encode_vector(&node.vector)`: `4 * dimensions` extra
//! bytes per document, alongside the node itself, so a probe is one
//! contiguous scan that yields id and vector together and feeds the SIMD
//! kernel directly, rather than a random read per candidate. That second copy
//! of the corpus is the cost this layout trades for the scan staying
//! sequential; said here rather than left as a disk-usage surprise.
//!
//! An index accepts writes from the first document and answers them exactly
//! by exhaustive scan, exactly as [`Flat`] does, because that is what it is
//! until it has enough vectors to fit centroids. Once the threshold is
//! reached it trains from a byte-bounded sample, writes centroids and
//! inverted lists, and answers from those instead.

mod build;
mod params;
mod search;

pub use build::BuildReport;
pub use params::{IvfFlatParams, DEFAULT_NPROBE, DEFAULT_SAMPLE_BYTES, MAX_NLIST, TRAIN_PER_LIST};

use crate::index::error::{Error, Result};
use crate::index::vector::engine::ann::{
    Admit, Centroids, EngineKind, Neighbor, VectorIndexEngine,
};
use crate::index::vector::engine::flat::Flat;
use crate::index::vector::engine::ivf::{self, TrainedState};
use crate::index::vector::store::{NodeId, VectorNodeStore};
use defra_core::vector::{Element, Metric};
use std::sync::OnceLock;

/// A coarse-partitioned index whose lists hold full-precision vectors.
#[derive(Debug, Clone)]
pub struct IvfFlat<S> {
    staging: Flat<S>,
    metric: Metric,
    params: IvfFlatParams,
    seed: u64,
    /// Centroids are fixed once trained, and decoding them costs more than
    /// scanning the lists they route to. Loaded once per engine.
    trained_cache: OnceLock<Centroids>,
}

impl<S: VectorNodeStore> IvfFlat<S> {
    /// Fails for a metric the coarse partitioning cannot serve, rather than
    /// quietly ranking on a heuristic that costs recall in a way no test on a
    /// cosine corpus would catch.
    ///
    /// Only the *coarse* step is restricted: the scan inside a probed list
    /// ranks by the real metric at full precision, whatever it is. The
    /// partitioning itself is squared Euclidean over the centroids, which is
    /// the metric that distance *is*, so `EUCLIDEAN` shares cells with it by
    /// construction; under cosine it is sound too, because vectors are
    /// normalized on insert (`|a-b|^2 = 2 - 2a.b`). A magnitude-sensitive
    /// metric like `DOT` does not share cells with nearest-neighbour search
    /// under L2 at all, so it stays refused until that is measured, mirroring
    /// exactly which metrics IVF-PQ's own `try_new` refuses.
    pub fn try_new(store: S, metric: Metric, params: IvfFlatParams, seed: u64) -> Result<Self> {
        params.validate()?;
        if metric != Metric::Cosine && metric != Metric::Euclidean {
            return Err(Error::Other(format!(
                "IVF_FLAT partitions lists by centroid distance, which does not share cells \
                 with {metric:?}"
            )));
        }
        Ok(Self {
            staging: Flat::new(store, metric),
            metric,
            params,
            seed,
            trained_cache: OnceLock::new(),
        })
    }

    pub fn store(&self) -> &S {
        self.staging.store()
    }

    pub fn store_mut(&mut self) -> &mut S {
        self.staging.store_mut()
    }

    pub fn params(&self) -> IvfFlatParams {
        self.params
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The trained state, or `None` while the index is still exact.
    pub async fn trained(&self) -> Result<Option<TrainedState>> {
        match self.store().get_aux(ivf::STATE, b"").await? {
            Some(bytes) => ivf::decode_state(&bytes).map(Some),
            None => Ok(None),
        }
    }

    pub async fn is_trained(&self) -> Result<bool> {
        Ok(self.trained().await?.is_some())
    }

    /// The coarse centroids, decoded once.
    pub(super) async fn trained_centroids(&self, state: &TrainedState) -> Result<&Centroids> {
        if let Some(centroids) = self.trained_cache.get() {
            return Ok(centroids);
        }
        let centroids = ivf::load_centroids(self.store(), state.nlist, state.dimensions).await?;
        Ok(self.trained_cache.get_or_init(|| centroids))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S: VectorNodeStore> VectorIndexEngine for IvfFlat<S> {
    fn kind(&self) -> EngineKind {
        EngineKind::IvfFlat
    }

    /// The vector is always stored, trained or not: it is what a rebuild reads
    /// and what keeps the index exact until one happens.
    ///
    /// Re-inserting under an id that already has one is an update, and its
    /// list assignment can move. The previous entry has to go with it: a list
    /// holds the vector of whatever was assigned to it, so a stale one
    /// returns the document a second time, ranked at a distance it no longer
    /// has.
    async fn insert<E: Element>(&mut self, id: NodeId, vector: &[E]) -> Result<()> {
        let trained = self.trained().await?;

        // Read before the write, because `staging.insert` overwrites the node
        // and the old vector is what names the list to clear.
        let previous = match trained {
            Some(_) => self.store().get_node(id).await?,
            None => None,
        };

        self.staging.insert(id, vector).await?;

        if let Some(state) = trained {
            let stored =
                self.store().get_node(id).await?.ok_or_else(|| {
                    Error::Other("vector index: a just-stored node is gone".into())
                })?;
            let list = self.assign(&state, &stored.vector).await?;

            if let Some(previous) = previous {
                let previous_list = self.assign(&state, &previous.vector).await?;
                if previous_list != list {
                    self.store_mut()
                        .delete_aux(ivf::LIST, &ivf::list_key(previous_list, id))
                        .await?;
                }
            }

            self.store_mut()
                .put_aux(
                    ivf::LIST,
                    &ivf::list_key(list, id),
                    &ivf::encode_vector(&stored.vector),
                )
                .await?;
        }
        Ok(())
    }

    /// Tombstones the node. Its list entry stays and is skipped on the scan,
    /// the same way the graph kinds skip a tombstone.
    async fn delete(&mut self, id: NodeId) -> Result<bool> {
        self.staging.delete(id).await
    }

    async fn should_build(&self) -> Result<bool> {
        self.should_build().await
    }

    async fn build(&mut self) -> Result<()> {
        self.build().await.map(|_| ())
    }

    async fn search_where<E: Element, A: Admit>(
        &self,
        query: &[E],
        k: usize,
        effort: Option<usize>,
        admit: &A,
    ) -> Result<Vec<Neighbor>> {
        match self.trained().await? {
            None => self.staging.search_where(query, k, effort, admit).await,
            Some(state) => self.search_lists(&state, query, k, effort, admit).await,
        }
    }
}
