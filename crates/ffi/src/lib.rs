//! FFI bindings for DefraDB Rust implementation.
//!
//! This crate provides C-compatible FFI functions that allow DefraDB.rs
//! to be used from Go (and other languages). It matches the interface
//! defined in Go's `cbindings/` directory.
//!
//! # Architecture
//!
//! ```text
//! Go Code
//!     ↓ CGO
//! C Header (defra.h)
//!     ↓ FFI
//! This crate (staticlib)
//!     ↓
//! db, query, storage crates
//! ```
//!
//! # Usage from Go
//!
//! 1. Build the static library: `cargo build --release -p ffi`
//! 2. Generate headers: `cbindgen --crate ffi --output defra.h`
//! 3. Link from Go with CGO directives
//!
//! # MVP Functions
//!
//! - `defra_init()` - Initialize the library
//! - `defra_version()` - Get library version
//! - `new_node()` - Create a new database node
//! - `node_close()` - Close and cleanup a node
//! - `add_schema()` - Add GraphQL SDL schema
//! - `get_collections()` - List all collections
//! - `exec_request()` - Execute GraphQL queries/mutations
//! - `defra_free_string()` - Free strings allocated by FFI functions

pub mod acp;
pub mod backup;
pub mod collection;
pub mod document;
pub mod index;
pub mod lens;
pub mod nac_check;
pub mod node;
pub mod p2p;
mod policy_yaml;
pub mod query;
pub mod runtime;
pub mod schema;
pub mod state;
pub mod subscription;
pub mod txn;
pub mod types;

use std::ffi::{c_char, CString};

/// Error message for invalid node handle.
pub const ERR_INVALID_NODE_HANDLE: &str = "invalid node handle";

/// Gets the tokio runtime, returning early with an error if not initialized.
///
/// Usage: `let rt = get_runtime!(FfiResult);`
///
/// The result type must have an `error(msg: impl Into<String>)` constructor.
#[macro_export]
macro_rules! get_runtime {
    ($result_type:ty) => {
        match $crate::runtime::RUNTIME.get() {
            Some(rt) => rt,
            None => {
                return <$result_type>::error("runtime not initialized - call defra_init() first")
            }
        }
    };
}

// Re-export FFI functions at crate root
pub use acp::{
    add_dac_actor_relationship, add_dac_policy, add_nac_actor_relationship, create_identity,
    delete_dac_actor_relationship, delete_nac_actor_relationship, disable_nac, enable_nac,
    get_dac_policy, get_nac_status, get_node_identity, list_dac_policies, re_enable_nac,
};
pub use backup::{basic_export, basic_import};
pub use collection::{
    add_view, delete_collection, find_collection_by_id, get_collection_by_name,
    get_collection_by_version_id, has_collection, patch_collection, refresh_views,
    set_active_collection_version, set_migration,
};
pub use document::{collection_create, is_json_array, parse_duration, parse_string_array};
pub use index::{create_index, drop_index, get_all_indexes, get_indexes};
pub use lens::{lens_add, lens_list};
pub use node::{new_node, node_close};
pub use p2p::{
    new_node_with_p2p, p2p_active_peers, p2p_add_collections, p2p_connect, p2p_delete_replicator,
    p2p_get_all_collections, p2p_get_all_replicators, p2p_peer_info, p2p_remove_collections,
    p2p_set_replicator,
};
pub use query::exec_request;
pub use schema::{add_schema, get_collections};
pub use subscription::{
    close_subscription, create_merge_complete_subscription, create_subscription, poll_subscription,
};
pub use txn::{begin_txn, commit_txn, exec_request_in_txn, rollback_txn};
pub use types::defra_free_string;

/// Initialize the FFI library.
///
/// This must be called once before any other FFI functions.
/// Safe to call multiple times.
#[no_mangle]
pub extern "C" fn defra_init() {
    // Ignore return value - errors will surface when operations are attempted
    let _ = runtime::init_runtime();
    // Enable deterministic nonce for testing (matches Go's init() detection)
    crypto::encryption::nonce::USE_DETERMINISTIC_NONCE
        .store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Get the library version.
///
/// Returns a null-terminated string that must be freed with `defra_free_string`.
#[no_mangle]
pub extern "C" fn defra_version() -> *mut c_char {
    let version = env!("CARGO_PKG_VERSION");
    // CARGO_PKG_VERSION is a compile-time constant without null bytes
    CString::new(version)
        .unwrap_or_else(|_| CString::new("unknown").unwrap())
        .into_raw()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;
    use std::ptr;

    #[test]
    fn test_defra_init() {
        defra_init();
        // Should be idempotent
        defra_init();
    }

    #[test]
    fn test_defra_version() {
        let version = defra_version();
        let version_str = unsafe { CStr::from_ptr(version).to_string_lossy() };
        assert!(!version_str.is_empty());
        // Should match Cargo.toml version
        assert!(version_str.starts_with("0."));

        // Clean up
        unsafe { defra_free_string(version) };
    }

    #[test]
    fn test_full_workflow() {
        use std::ffi::CString;
        use types::NodeInitOptions;

        // Initialize
        defra_init();

        // Create node
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0, "new_node failed");
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type Person { name: String, age: Int }").unwrap();
        let result = unsafe { add_schema(node, ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0, "add_schema failed");
        if !result.value.is_null() {
            unsafe { defra_free_string(result.value) };
        }

        // Get collections
        let result = unsafe { get_collections(node, ptr::null()) };
        assert_eq!(result.status, 0, "get_collections failed");
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Person"), "should contain Person collection");
        unsafe { defra_free_string(result.value) };

        // Create a person
        let mutation = CString::new(
            r#"mutation { create_Person(input: {name: "Bob", age: 30}) { _docID name age } }"#,
        )
        .unwrap();
        let result = unsafe { exec_request(node, ptr::null(), mutation.as_ptr(), ptr::null(), ptr::null()) };
        assert_eq!(result.status, 0, "mutation failed");
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Bob"), "should contain Bob");
        unsafe { defra_free_string(result.value) };

        // Query people
        let query_str = CString::new("{ Person { name age } }").unwrap();
        let result = unsafe { exec_request(node, ptr::null(), query_str.as_ptr(), ptr::null(), ptr::null()) };
        assert_eq!(result.status, 0, "query failed");
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Bob"), "query should return Bob");
        assert!(value.contains("30"), "query should return age 30");
        unsafe { defra_free_string(result.value) };

        // Close node
        let result = node_close(node);
        assert_eq!(result.status, 0, "node_close failed");
    }
}
