//! Secondary index manager for DefraDB collections.
//!
//! Extracted from the main `db` crate as part of the #669 decomposition
//! epic. Owns index lifecycle (create/drop/load), bulk indexing, and
//! per-document maintenance during mutations.
//!
//! Depends only on storage/datastore/schema/document — no dependency
//! back to `db`, so `db` can depend on this crate cleanly. A thin
//! `From<db_index::Error> for db::Error` impl lives in the `db` crate
//! for call sites that still thread errors through the broader db
//! error hierarchy.

pub mod error;
pub mod index_manager;

pub use error::{Error, Result};
pub use index_manager::{fulltext_index_name, BulkIndexResult, IndexManager};
