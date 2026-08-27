//! Collection management operations for FFI.
//!
//! This module exposes collection lifecycle and management functions
//! that match Go's collection management behavior.
//!
//! The read/write wrappers are the current code-generation prototype for #692:
//! they keep explicit `extern "C"` items for cbindgen, but share the repeated
//! node/runtime/NAC/database prelude through `ffi_node_db_async_body!`.

mod migration;
mod purge;
mod read;
mod view;
mod write;

pub use migration::{
    delete_collection_versions, materialize_collection, set_migration, set_migration_in_txn,
};
pub use purge::delete_documents;
pub use read::{
    find_collection_by_id, get_collection_by_name, get_collection_by_version_id, has_collection,
};
pub use view::{add_view, gc_downsample_histories, refresh_views};
pub use write::{
    delete_collection, delete_collections, delete_collections_in_txn, patch_collection,
    set_active_collection_version, set_collection_active_in_txn, truncate_collection,
    truncate_collection_with_filter,
};
