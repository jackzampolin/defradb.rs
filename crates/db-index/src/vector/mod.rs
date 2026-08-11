//! Vector indexing.
//!
//! Layered so a future index kind supplies only a new engine:
//! `core` holds metric and distance primitives shared by every kind.
pub mod core;
