//! What every approximate-nearest-neighbor index kind has in common.
//!
//! The kinds themselves are siblings of this module: [`hnsw`](super::hnsw) and
//! [`flat`](super::flat) today, an IVF-PQ or SSG kind later. Nothing here knows
//! about any of them, which is the point: a new kind adds a sibling module and
//! implements [`VectorIndexEngine`], and the store port, the key layout and the
//! `CollectionIndex` wiring are untouched.
//!
//! This mirrors pulsejetdb's `index/core` sitting beside `index/hnsw`,
//! `index/flat`, `index/ivfpq`, `index/ssg` and `index/pq`.

mod candidate;
mod clusterer;
mod edge_selector;
mod engine;
mod engine_kind;
mod neighbor;
mod quantizer;
mod sampler;

pub use candidate::Candidate;
pub use clusterer::{Centroids, Clusterer, Fit};
pub use edge_selector::EdgeSelector;
pub use engine::{Admit, AdmitAll, VectorIndexEngine};
pub use engine_kind::EngineKind;
pub use neighbor::Neighbor;
pub use quantizer::Quantizer;
pub use sampler::Sampler;
