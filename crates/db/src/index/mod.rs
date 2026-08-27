//! Secondary index management: maintenance, vector engines and value extraction.

pub mod error;
pub mod manager;
pub mod vector;

pub use error::{Error, Result};
pub use manager::{fulltext_index_name, BulkIndexResult, IndexManager};
