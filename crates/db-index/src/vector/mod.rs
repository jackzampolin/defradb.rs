//! Vector indexing.
//!
//! Layered so a future index kind supplies only a new engine: `core` holds the
//! metric and distance primitives, `store` the persistence port, `engine` the
//! HNSW graph. Nothing below `store` knows what a database is.
pub mod codec;
pub mod core;
pub mod engine;
pub mod index;
pub mod kv_store;
pub mod params;
pub mod store;
