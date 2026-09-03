//! Fixtures shared by the vector benchmarks.

#![allow(dead_code)]

use db::index::vector::engine::ann::VectorIndexEngine;
use db::index::vector::engine::flat::Flat;
use db::index::vector::engine::hnsw::Hnsw;
use db::index::vector::engine::ivfflat::{IvfFlat, IvfFlatParams};
use db::index::vector::engine::ivfpq::{IvfPq, IvfPqParams};
use db::index::vector::engine::ssg::{Ssg, SsgParams};
use db::index::vector::params::{Params, DEFAULT_M};
use db::index::vector::store::{MemoryNodeStore, NodeId};
use defra_core::vector::Metric;

pub const SEED: u64 = 0x0BE9_C4A1;

/// Deterministic across machines and releases, which a random-number crate's
/// stream is not.
pub struct Corpus(u64);

impl Corpus {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn gaussian(&mut self) -> f32 {
        let u1 = self.unit().max(f64::MIN_POSITIVE);
        let u2 = self.unit();
        ((-2.0 * u1.ln()).sqrt() * (core::f64::consts::TAU * u2).cos()) as f32
    }

    pub fn vector(&mut self, dimensions: usize) -> Vec<f32> {
        (0..dimensions)
            .map(|_| (self.unit() * 2.0 - 1.0) as f32)
            .collect()
    }

    pub fn vectors(&mut self, count: usize, dimensions: usize) -> Vec<Vec<f32>> {
        (0..count).map(|_| self.vector(dimensions)).collect()
    }

    /// Clustered, which is the shape a real embedding corpus has. Uniform
    /// vectors in high dimensions are all near-orthogonal and make every index
    /// look the same.
    pub fn clustered(
        &mut self,
        count: usize,
        dimensions: usize,
        clusters: usize,
        spread: f32,
    ) -> Vec<Vec<f32>> {
        let centers: Vec<Vec<f32>> = (0..clusters).map(|_| self.vector(dimensions)).collect();
        (0..count)
            .map(|i| {
                let center = &centers[i % clusters];
                (0..dimensions)
                    .map(|j| center[j] + self.gaussian() * spread)
                    .collect()
            })
            .collect()
    }
}

/// Every kind, behind the one trait they all implement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Flat,
    Hnsw,
    IvfPq,
    IvfFlat,
    Ssg,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Flat => "flat",
            Kind::Hnsw => "hnsw",
            Kind::IvfPq => "ivfpq",
            Kind::IvfFlat => "ivfflat",
            Kind::Ssg => "ssg",
        }
    }

    /// The kinds that build a structure after the fact, and therefore have a
    /// second state to measure.
    pub fn is_batch_built(self) -> bool {
        matches!(self, Kind::IvfPq | Kind::IvfFlat | Kind::Ssg)
    }
}

pub const ALL_KINDS: [Kind; 5] = [
    Kind::Flat,
    Kind::Hnsw,
    Kind::IvfPq,
    Kind::IvfFlat,
    Kind::Ssg,
];

#[derive(Clone)]
pub enum Index {
    Flat(Flat<MemoryNodeStore>),
    Hnsw(Hnsw<MemoryNodeStore>),
    IvfPq(IvfPq<MemoryNodeStore>),
    IvfFlat(IvfFlat<MemoryNodeStore>),
    Ssg(Ssg<MemoryNodeStore>),
}

macro_rules! on_index {
    ($self:ident, $index:ident => $call:expr) => {
        match $self {
            Index::Flat($index) => $call,
            Index::Hnsw($index) => $call,
            Index::IvfPq($index) => $call,
            Index::IvfFlat($index) => $call,
            Index::Ssg($index) => $call,
        }
    };
}

impl Index {
    pub fn new(kind: Kind) -> Self {
        Self::with_metric(kind, Metric::Cosine)
    }

    /// SIFT's published ground truth is Euclidean, so a benchmark measured
    /// against it must rank the same way or an exact scan does not score 1.0.
    pub fn with_metric(kind: Kind, metric: Metric) -> Self {
        let store = MemoryNodeStore::new();
        match kind {
            Kind::Flat => Index::Flat(Flat::new(store, metric)),
            Kind::Hnsw => Index::Hnsw(Hnsw::new(store, metric, Params::new(DEFAULT_M), SEED)),
            Kind::IvfPq => Index::IvfPq(
                IvfPq::try_new(
                    store,
                    metric,
                    // nlist derived as 4*sqrt(N): a hardcoded small nlist
                    // makes nprobe probe a large fraction of the corpus, which
                    // measures a misconfigured index rather than the kind.
                    IvfPqParams::default(),
                    SEED,
                )
                .expect("cosine is rankable by squared distance"),
            ),
            Kind::IvfFlat => Index::IvfFlat(
                IvfFlat::try_new(store, metric, IvfFlatParams::default(), SEED)
                    .expect("cosine or euclidean partitions soundly"),
            ),
            Kind::Ssg => Index::Ssg(
                Ssg::try_new(
                    store,
                    metric,
                    Params::new(DEFAULT_M),
                    SsgParams::default(),
                    SEED,
                )
                .expect("valid SSG parameters"),
            ),
        }
    }

    pub async fn insert(&mut self, id: NodeId, vector: &[f32]) {
        on_index!(self, index => index.insert(id, vector).await.expect("insert"));
    }

    pub async fn delete(&mut self, id: NodeId) -> bool {
        on_index!(self, index => index.delete(id).await.expect("delete"))
    }

    pub async fn search(&self, query: &[f32], k: usize) -> usize {
        on_index!(self, index => index.search(query, k, None).await.expect("search").len())
    }

    pub async fn search_ids(&self, query: &[f32], k: usize) -> Vec<u64> {
        on_index!(self, index => index
            .search(query, k, None)
            .await
            .expect("search")
            .into_iter()
            .map(|n| n.id.0)
            .collect())
    }

    /// Builds the structure a batch-built kind answers from. A no-op for the
    /// kinds that have none, so a caller can treat every kind alike.
    pub async fn build(&mut self) {
        match self {
            Index::IvfPq(index) => {
                index.build().await.expect("ivf-pq build");
            }
            Index::IvfFlat(index) => {
                index.build().await.expect("ivf_flat build");
            }
            Index::Ssg(index) => {
                index.build().await.expect("ssg build");
            }
            _ => {}
        }
    }

    pub async fn filled(kind: Kind, vectors: &[Vec<f32>], built: bool) -> Self {
        Self::filled_with(kind, Metric::Cosine, vectors, built).await
    }

    pub async fn filled_with(
        kind: Kind,
        metric: Metric,
        vectors: &[Vec<f32>],
        built: bool,
    ) -> Self {
        let mut index = Index::with_metric(kind, metric);
        for (i, vector) in vectors.iter().enumerate() {
            index.insert(NodeId(i as u64 + 1), vector).await;
        }
        if built {
            index.build().await;
        }
        index
    }
}
