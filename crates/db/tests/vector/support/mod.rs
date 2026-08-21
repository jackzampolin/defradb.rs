//! Fixtures shared by the vector test binaries.
//!
//! `tests/common/mod.rs` is not itself a test target, so this compiles once per
//! binary that declares `mod common;` and nothing here runs on its own.

#![allow(dead_code)]

use db::index::vector::core::Metric;
use db::index::vector::engine::ann::VectorIndexEngine;
use db::index::vector::engine::flat::Flat;
use db::index::vector::engine::hnsw::Hnsw;
use db::index::vector::params::Params;
use db::index::vector::params::DEFAULT_M;
use db::index::vector::store::MemoryNodeStore;
use db::index::vector::store::NodeId;

/// A fixed corpus needs a generator that produces the same vectors on every
/// machine and every release. SplitMix64 over a named seed does; a
/// random-number crate's stream is only stable within a minor version.
pub struct Corpus {
    state: u64,
}

impl Corpus {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[-1, 1)`.
    fn next_component(&mut self) -> f32 {
        let unit = (self.next_u64() >> 40) as f32 / (1u32 << 24) as f32;
        unit * 2.0 - 1.0
    }

    pub fn vector(&mut self, dimensions: usize) -> Vec<f32> {
        (0..dimensions).map(|_| self.next_component()).collect()
    }

    pub fn vectors(&mut self, count: usize, dimensions: usize) -> Vec<Vec<f32>> {
        (0..count).map(|_| self.vector(dimensions)).collect()
    }

    fn gaussian(&mut self) -> f32 {
        let unit = |x: u64| (x >> 11) as f64 / (1u64 << 53) as f64;
        let u1 = unit(self.next_u64()).max(f64::MIN_POSITIVE);
        let u2 = unit(self.next_u64());
        ((-2.0 * u1.ln()).sqrt() * (core::f64::consts::TAU * u2).cos()) as f32
    }

    /// Centers with gaussian noise around each: the shape a real embedding
    /// corpus has, rather than the near-orthogonal uniform degenerate case.
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

/// Seeds are named so a failure is reproducible from the message alone.
pub const CORPUS_SEED: u64 = 0x5EED_C0FF_EE00_1234;
pub const GRAPH_SEED: u64 = 0x0000_1234_5678_9ABC;
pub const QUERY_SEED: u64 = 0xABCD_EF01_2345_6789;

pub fn graph(seed: u64) -> Hnsw<MemoryNodeStore> {
    Hnsw::new(
        MemoryNodeStore::new(),
        Metric::Cosine,
        Params::new(DEFAULT_M),
        seed,
    )
}

pub async fn build(vectors: &[Vec<f32>], seed: u64) -> Hnsw<MemoryNodeStore> {
    let mut index = graph(seed);
    for (i, vector) in vectors.iter().enumerate() {
        index
            .insert(NodeId(i as u64), vector)
            .await
            .expect("in-memory insert cannot fail");
    }
    index
}

/// Ranks by scoring every corpus vector directly. Proves `Flat` is exact
/// before `Flat` is trusted to judge `Hnsw`.
pub fn scored(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<NodeId> {
    let mut scored: Vec<(NodeId, f64)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (NodeId(i as u64), Metric::Cosine.distance(query, v)))
        .collect();
    scored.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    scored.into_iter().take(k).map(|(id, _)| id).collect()
}

pub async fn flat(vectors: &[Vec<f32>]) -> Flat<MemoryNodeStore> {
    let mut index = Flat::new(MemoryNodeStore::new(), Metric::Cosine);
    for (i, vector) in vectors.iter().enumerate() {
        index.insert(NodeId(i as u64), vector).await.unwrap();
    }
    index
}
