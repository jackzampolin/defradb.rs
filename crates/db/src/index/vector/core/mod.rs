//! Vector index primitives shared by every vector index kind.

pub mod kernel;
pub mod metric;

pub use kernel::{dot, squared_euclidean, Element, Tier, ALL_TIERS};
pub use metric::{norm, normalize, squared_norm, Metric, MAX_COSINE_DISTANCE, NORM_THRESHOLD};
