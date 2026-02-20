//! Index endpoint handlers.
//!
//! Two route patterns are supported:
//! - Flat: /api/v0/index (with collection in request body/query)
//! - Go-compatible: /api/v0/collections/{name}/indexes (collection in path)
//!
//! All endpoints enforce NAC permissions when NAC is enabled.

mod go_api;
mod rust_api;

pub use go_api::{
    go_create_index, go_delete_index, go_list_all_indexes, go_list_indexes, GoCreateIndexRequest,
    GoIndexDescription, GoIndexedFieldDescription,
};
pub use rust_api::{
    create_index, delete_index, list_indexes, CreateIndexRequest, DeleteIndexQuery,
    ListIndexesQuery,
};
