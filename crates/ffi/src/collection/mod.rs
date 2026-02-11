//! Collection management operations for FFI.
//!
//! This module exposes collection lifecycle and management functions
//! that match Go's collection management behavior.

mod migration;
mod read;
mod view;
mod write;

pub use migration::{delete_collection_versions, set_migration, set_migration_in_txn};
pub use read::{
    find_collection_by_id, get_collection_by_name, get_collection_by_version_id, has_collection,
};
pub use view::{add_view, refresh_views};
pub use write::{
    delete_collection, patch_collection, set_active_collection_version, truncate_collection,
};
