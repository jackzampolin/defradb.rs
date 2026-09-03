//! Vector index kinds, and the abstraction they share.
//!
//! [`ann`] holds what every kind has in common; each kind is a sibling module.
//! Adding one means adding a module here and implementing
//! [`VectorIndexEngine`](ann::VectorIndexEngine) -- not a new `IndexType`
//! variant with its own accessor and its own insertion path, which is what the
//! full-text index cost and what #1326 exists to prevent.

pub mod ann;
pub mod dispatch;
pub mod flat;
pub mod hnsw;
pub mod ivf;
pub mod ivfflat;
pub mod ivfpq;
pub mod select;
pub mod ssg;
