//! Fixtures shared by the db test binaries.
//!
//! Each binary pulls the whole module in and uses a subset, so unused items
//! here are expected rather than dead.
#![allow(dead_code)]

pub mod counting_store;
pub mod fixture;
pub mod schema;
pub mod stream;
