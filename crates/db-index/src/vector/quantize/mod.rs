//! Implementations of the sampling, clustering and quantization traits.
//!
//! The traits themselves live in [`engine::ann`](crate::vector::engine::ann),
//! beside every other abstraction a kind implements. Nothing here imports
//! storage.

mod kmeans;
mod pq;
mod sample;

pub use kmeans::KMeans;
pub use pq::{ProductQuantizer, CODEBOOK_SIZE};
pub use sample::Reservoir;
