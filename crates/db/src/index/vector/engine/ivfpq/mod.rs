//! IVF-PQ: coarse lists of product-quantized codes.
//!
//! An index accepts writes from the first document and answers them exactly by
//! exhaustive scan, exactly as [`Flat`](super::flat::Flat) does, because that is
//! what it is until it has enough vectors to fit centroids. Once the threshold
//! is reached it trains from a byte-bounded sample, writes centroids, codebooks
//! and inverted lists, and answers from those instead.
//!
//! ADC is squared-L2. Under cosine that ranks identically to the metric, since
//! vectors are normalized on insert and `|a-b|^2 = 2 - 2a.b`; under a
//! magnitude-sensitive metric it does not, so those are refused at
//! construction.

mod build;
mod codec;
mod params;
mod search;

pub use codec::TrainedState;
pub use params::{
    IvfPqParams, DEFAULT_NBITS, DEFAULT_NPROBE, DEFAULT_SAMPLE_BYTES, MAX_M, MAX_NLIST,
    TRAIN_PER_LIST,
};

use crate::index::error::{Error, Result};
use crate::index::vector::core::{Element, Metric};
use crate::index::vector::engine::ann::{
    Admit, Centroids, EngineKind, Neighbor, VectorIndexEngine,
};
use crate::index::vector::engine::flat::Flat;
use crate::index::vector::quantize::ProductQuantizer;
use crate::index::vector::store::{NodeId, VectorNodeStore};
use std::sync::OnceLock;

/// A coarse-quantized, product-compressed index.
#[derive(Debug, Clone)]
pub struct IvfPq<S> {
    staging: Flat<S>,
    metric: Metric,
    params: IvfPqParams,
    seed: u64,
    /// Centroids and codebooks are fixed once trained, and decoding them costs
    /// more than scanning the lists they route to. Loaded once per engine.
    trained_cache: OnceLock<(Centroids, ProductQuantizer)>,
}

impl<S: VectorNodeStore> IvfPq<S> {
    /// Fails for a metric ADC cannot rank, rather than quietly ranking on
    /// squared-L2 when the caller asked for something else.
    pub fn try_new(store: S, metric: Metric, params: IvfPqParams, seed: u64) -> Result<Self> {
        params.validate()?;
        if metric != Metric::Cosine && metric != Metric::Euclidean {
            return Err(Error::Other(format!(
                "IVF-PQ ranks by squared distance, which does not order {metric:?}"
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

    pub fn params(&self) -> IvfPqParams {
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
        match self.store().get_aux(codec::STATE, b"").await? {
            Some(bytes) => codec::decode_state(&bytes).map(Some),
            None => Ok(None),
        }
    }

    pub async fn is_trained(&self) -> Result<bool> {
        Ok(self.trained().await?.is_some())
    }

    /// The coarse centroids and the quantizer, decoded once.
    pub(super) async fn trained_parts(
        &self,
        state: &TrainedState,
    ) -> Result<&(Centroids, ProductQuantizer)> {
        if let Some(parts) = self.trained_cache.get() {
            return Ok(parts);
        }
        let parts = (
            self.load_coarse_centroids(state).await?,
            self.load_quantizer(state).await?,
        );
        Ok(self.trained_cache.get_or_init(|| parts))
    }

    async fn load_quantizer(&self, state: &TrainedState) -> Result<ProductQuantizer> {
        let mut books = Vec::with_capacity(state.m as usize);
        for sub in 0..state.m {
            let bytes = self
                .store()
                .get_aux(codec::CODEBOOK, &sub.to_be_bytes())
                .await?
                .ok_or_else(|| Error::Other(format!("vector index: codebook {sub} is missing")))?;
            books.push(codec::decode_centroids(&bytes)?);
        }
        ProductQuantizer::from_books(state.dimensions as usize, books)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S: VectorNodeStore> VectorIndexEngine for IvfPq<S> {
    fn kind(&self) -> EngineKind {
        EngineKind::IvfPq
    }

    /// The vector is always stored, trained or not: it is what a rebuild reads
    /// and what keeps the index exact until one happens.
    async fn insert<E: Element>(&mut self, id: NodeId, vector: &[E]) -> Result<()> {
        self.staging.insert(id, vector).await?;

        if let Some(state) = self.trained().await? {
            let stored =
                self.store().get_node(id).await?.ok_or_else(|| {
                    Error::Other("vector index: a just-stored node is gone".into())
                })?;
            let (list, code) = self.assign(&state, &stored.vector).await?;
            self.store_mut()
                .put_aux(codec::LIST, &codec::list_key(list, id), &code)
                .await?;
        }
        Ok(())
    }

    /// Tombstones the node. Its code stays in its list and is skipped on the
    /// scan, the same way the graph kinds skip a tombstone.
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
        match self.trained().await? {
            None => self.staging.search_where(query, k, effort, admit).await,
            Some(state) => self.search_lists(&state, query, k, effort, admit).await,
        }
    }
}
