//! The vector index as the collection layer sees it.
//!
//! Owns nothing durable: every method builds an engine over the caller's
//! transaction, uses it, and drops it. That is what keeps index maintenance
//! inside the write that triggered it.

use async_trait::async_trait;
use document::NormalValue;
use schema::{DistanceMetric, IndexDescription, VectorAlgorithm, VectorIndexDescription};
use storage::corekv::{MaybeSend, Reader, Writer};
use storage::index::CollectionIndex;

use super::core::Element;
use super::core::Metric;
use super::engine::ann::{Admit, Neighbor, VectorIndexEngine};
use super::engine::dispatch::Engine;
use super::engine::flat::Flat;
use super::engine::hnsw::Hnsw;
use super::engine::ivfpq::{IvfPq, IvfPqParams};
use super::kv_store::KvNodeStore;
use super::params::Params;
use super::store::NodeId;
use crate::error::{Error, Result};

/// The only epoch until something triggers a rebuild. The key layout carries
/// the component so that day does not require migrating every entry.
const LIVE_EPOCH: u32 = 0;

/// A collection's vector index.
#[derive(Debug)]
pub struct VectorIndex {
    collection_short_id: u32,
    desc: IndexDescription,
    vector: VectorIndexDescription,
    params: Params,
    ivfpq: IvfPqParams,
    metric: Metric,
    seed: u64,
}

impl VectorIndex {
    /// Fails when the description is not a vector index, or carries parameters
    /// that would let one index creation do unbounded work.
    pub fn try_new(collection_short_id: u32, desc: IndexDescription) -> Result<Self> {
        let vector = *desc
            .vector()
            .ok_or_else(|| Error::Other(format!("index '{}' is not a vector index", desc.name)))?;

        let hnsw = vector.hnsw.unwrap_or_default();
        let mut params = Params::new(hnsw.m as usize);
        params.ef_construction = hnsw.ef_construction as usize;
        params.ef_search = hnsw.ef_search as usize;
        // Each algorithm validates only its own block, so parameters belonging
        // to another are inert rather than a reason to refuse the index.
        if vector.algorithm == VectorAlgorithm::Hnsw {
            params.validate()?;
        }

        let configured = vector.ivfpq.unwrap_or_default();
        let ivfpq = IvfPqParams {
            nlist: configured.nlist,
            nprobe: configured.nprobe,
            m: configured.m,
            sample_bytes: configured.sample_bytes,
        };

        let metric = match vector.metric {
            DistanceMetric::Cosine => Metric::Cosine,
            DistanceMetric::Dot => Metric::NegativeDot,
        };

        // Refused here rather than at query time, so a description that can
        // never answer is rejected where it is created.
        if vector.algorithm == VectorAlgorithm::IvfPq {
            IvfPq::try_new(super::store::MemoryNodeStore::new(), metric, ivfpq, 0)?;
        }

        Ok(Self {
            collection_short_id,
            // Stable across restarts, and distinct per index, so two indexes on
            // one collection do not assign identical layer heights.
            seed: (u64::from(collection_short_id) << 32) | u64::from(desc.id),
            desc,
            vector,
            params,
            ivfpq,
            metric,
        })
    }

    pub fn vector_description(&self) -> &VectorIndexDescription {
        &self.vector
    }

    /// The `k` nearest documents to `query`, nearest first.
    pub async fn search<T: Reader + Writer + MaybeSend, E: Element>(
        &self,
        txn: &mut T,
        query: &[E],
        k: usize,
        effort: Option<usize>,
    ) -> Result<Vec<Neighbor>> {
        self.engine(txn)?.search(query, k, effort).await
    }

    /// [`search`](Self::search) restricted to the documents `admit` accepts.
    pub async fn search_where<T: Reader + Writer + MaybeSend, E: Element, A: Admit>(
        &self,
        txn: &mut T,
        query: &[E],
        k: usize,
        effort: Option<usize>,
        admit: &A,
    ) -> Result<Vec<Neighbor>> {
        self.engine(txn)?
            .search_where(query, k, effort, admit)
            .await
    }

    /// Returns the configured algorithm behind [`VectorIndexEngine`] rather
    /// than a concrete type, so a second algorithm is a match arm here and
    /// nothing else in this file changes.
    fn engine<'txn, T: Reader + Writer + MaybeSend>(
        &self,
        txn: &'txn mut T,
    ) -> Result<impl VectorIndexEngine + 'txn> {
        let store = KvNodeStore::new(txn, self.collection_short_id, self.desc.id, LIVE_EPOCH);
        Ok(match self.vector.algorithm {
            VectorAlgorithm::Hnsw => {
                Engine::Hnsw(Hnsw::new(store, self.metric, self.params, self.seed))
            }
            VectorAlgorithm::Flat => Engine::Flat(Flat::new(store, self.metric)),
            VectorAlgorithm::IvfPq => {
                Engine::IvfPq(IvfPq::try_new(store, self.metric, self.ivfpq, self.seed)?)
            }
        })
    }

    /// The vector a document contributes, if any.
    ///
    /// `None` means the document is simply not in the index, which is the same
    /// answer a null field gives. A vector with no usable direction lands here
    /// too: under cosine it cannot be ranked against anything, so indexing it
    /// would store a point no query could ever return.
    fn vector_of<'v>(&self, values: &'v [NormalValue]) -> Result<Option<Indexable<'v>>> {
        let Some(value) = values.first() else {
            return Ok(None);
        };
        let indexable = match value {
            NormalValue::Float32Array(v) => Indexable::Narrow(v),
            NormalValue::Float64Array(v) => Indexable::Wide(v),
            NormalValue::NillableFloat32Array(Some(v)) => Indexable::Narrow(v),
            NormalValue::NillableFloat64Array(Some(v)) => Indexable::Wide(v),
            // Go's `Similarity` declares the query vector as "Int, Float32 or
            // Float64", so a peer can send an integer one and this must index
            // it rather than refuse it.
            NormalValue::IntArray(v) => Indexable::Integral(v),
            NormalValue::NillableIntArray(Some(v)) => Indexable::Integral(v),
            NormalValue::Null
            | NormalValue::NillableFloat32Array(None)
            | NormalValue::NillableFloat64Array(None)
            | NormalValue::NillableIntArray(None) => return Ok(None),
            other => {
                return Err(Error::InvalidDocument(format!(
                    "index '{}' expects a vector field, got {other:?}",
                    self.desc.name
                )))
            }
        };

        // Dimension agreement is enforced here and nowhere else. The kernels
        // and metrics below are total by design so a graph walk never has to
        // re-check; this is the one place a document can be turned away.
        let declared = self.vector.dimensions as usize;
        if declared != 0 && indexable.len() != declared {
            return Err(Error::InvalidDocument(format!(
                "index '{}' expects {declared} dimensions, got {}",
                self.desc.name,
                indexable.len()
            )));
        }
        if indexable.is_empty() || !indexable.has_direction(self.metric) {
            return Ok(None);
        }
        Ok(Some(indexable))
    }
}

/// A document's vector at whichever width it arrived in. JSON and GraphQL
/// deliver `f64`; an embedding model's output arrives as `f32`. Neither is
/// converted here, because the engine takes both.
enum Indexable<'v> {
    Narrow(&'v [f32]),
    Wide(&'v [f64]),
    Integral(&'v [i64]),
}

impl Indexable<'_> {
    fn len(&self) -> usize {
        match self {
            Indexable::Narrow(v) => v.len(),
            Indexable::Wide(v) => v.len(),
            Indexable::Integral(v) => v.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the metric can rank this vector at all.
    fn has_direction(&self, metric: Metric) -> bool {
        if metric != Metric::Cosine {
            return true;
        }
        let squared = match self {
            Indexable::Narrow(v) => super::core::squared_norm(v),
            Indexable::Wide(v) => super::core::squared_norm(v),
            Indexable::Integral(v) => super::core::squared_norm(v),
        };
        squared.is_finite() && squared >= super::core::NORM_THRESHOLD
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl CollectionIndex for VectorIndex {
    fn description(&self) -> &IndexDescription {
        &self.desc
    }

    async fn save<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_short_id: u64,
        values: &[NormalValue],
    ) -> storage::corekv::Result<()> {
        let indexable = self.vector_of(values).map_err(into_storage)?;
        let Some(indexable) = indexable else {
            return Ok(());
        };
        let id = NodeId(doc_short_id);
        let mut engine = self.engine(txn).map_err(into_storage)?;
        match indexable {
            Indexable::Narrow(v) => engine.insert(id, v).await,
            Indexable::Wide(v) => engine.insert(id, v).await,
            Indexable::Integral(v) => engine.insert(id, v).await,
        }
        .map_err(into_storage)
    }

    /// Re-inserting under the same id replaces the node's vector and rebuilds
    /// its own links. Links pointing *at* it stay valid, since the id is what
    /// they name.
    async fn update<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_short_id: u64,
        _old_values: &[NormalValue],
        new_values: &[NormalValue],
    ) -> storage::corekv::Result<()> {
        // A field that became null leaves a node behind, so the old entry is
        // tombstoned rather than left ranking against a value that is gone.
        if self.vector_of(new_values).map_err(into_storage)?.is_none() {
            return self.delete(txn, doc_short_id, _old_values).await;
        }
        self.save(txn, doc_short_id, new_values).await
    }

    async fn delete<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_short_id: u64,
        _values: &[NormalValue],
    ) -> storage::corekv::Result<()> {
        self.engine(txn)
            .map_err(into_storage)?
            .delete(NodeId(doc_short_id))
            .await
            .map(|_| ())
            .map_err(into_storage)
    }

    async fn remove_all<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
    ) -> storage::corekv::Result<()> {
        KvNodeStore::new(txn, self.collection_short_id, self.desc.id, LIVE_EPOCH)
            .clear()
            .await
            .map_err(into_storage)
    }
}

/// `CollectionIndex` speaks the storage error type; the vector stack has its
/// own. A storage error that came in through the port is unwrapped rather than
/// wrapped twice.
fn into_storage(error: Error) -> storage::corekv::Error {
    match error {
        Error::Storage(inner) => inner,
        other => storage::corekv::Error::Other(other.to_string()),
    }
}
