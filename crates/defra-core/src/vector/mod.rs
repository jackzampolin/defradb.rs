//! Vector maths shared by everything that compares two vectors.
//!
//! It lives here rather than beside the index engines because the query planner
//! scores documents with it too: an unrouted `SIMILARITY` selection must rank a
//! collection exactly the way the index would have, so both call one
//! implementation rather than two that agree until they do not.

pub mod kernel;
pub mod metric;

pub use kernel::{dot, squared_euclidean, Element, Tier, ALL_TIERS};
pub use metric::{norm, normalize, squared_norm, Metric, MAX_COSINE_DISTANCE, NORM_THRESHOLD};
