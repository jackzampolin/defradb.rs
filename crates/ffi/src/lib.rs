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

pub mod acp;
pub mod block;
pub mod collection;
pub mod document;
pub mod index;
pub mod lens;
pub mod node;
pub mod p2p;
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

// Re-export FFI functions at crate root for cbindgen
pub use acp::{
    acp_add_dac_actor_relationship, acp_add_dac_policy, acp_add_nac_actor_relationship,
    acp_delete_dac_actor_relationship, acp_delete_nac_actor_relationship, acp_disable_nac,
    acp_get_nac_status, acp_re_enable_nac, get_node_identity, identity_free, identity_new,
};
pub use block::block_verify_signature;
pub use collection::{
    collection_delete, collection_describe, collection_patch, set_active_collection, view_add,
    view_refresh,
};
pub use document::{
    collection_create, collection_get, collection_list_doc_ids, collection_truncate,
    collection_update,
};
pub use index::{
    encrypted_index_create, encrypted_index_delete, encrypted_index_list, index_create, index_drop,
    index_list,
};
pub use lens::{lens_add, lens_list, lens_set};
pub use node::{new_node, node_close};
pub use p2p::{
    p2p_active_peers, p2p_add_collections, p2p_branchable_collection_sync,
    p2p_collection_sync_versions, p2p_connect, p2p_delete_replicator, p2p_document_add,
    p2p_document_get_all, p2p_document_remove, p2p_document_sync, p2p_get_all_collections,
    p2p_get_all_replicators, p2p_peer_info, p2p_remove_collections, p2p_set_replicator,
};
pub use query::execute_query;
pub use schema::add_schema;
pub use subscription::{close_subscription, poll_subscription};
pub use txn::{transaction_commit, transaction_create, transaction_discard};
pub use types::defra_free_string;

/// Initialize the FFI library.
///
/// This must be called once before any other FFI functions.
/// Safe to call multiple times.
#[export_name = "DefraInit"]
pub extern "C" fn defra_init() {
    let _ = runtime::init_runtime();
}

/// Get the library version.
///
/// Returns a null-terminated string that must be freed with `defra_free_string`.
#[export_name = "DefraVersion"]
pub extern "C" fn defra_version() -> *mut c_char {
    let version = env!("CARGO_PKG_VERSION");
    CString::new(version)
        .unwrap_or_else(|_| CString::new("unknown").unwrap())
        .into_raw()
}

/// Get version info matching Go's VersionGet interface.
///
/// # Arguments
///
/// * `flag_full` - If non-zero, return full version string
/// * `flag_json` - If non-zero, return JSON object
#[export_name = "VersionGet"]
pub extern "C" fn version_get(
    flag_full: std::ffi::c_int,
    flag_json: std::ffi::c_int,
) -> types::FfiResult {
    let version = env!("CARGO_PKG_VERSION");

    if flag_json != 0 {
        let json = serde_json::json!({
            "version": version,
            "commit": option_env!("GIT_HASH").unwrap_or(""),
            "buildDate": option_env!("BUILD_DATE").unwrap_or(""),
            "goVersion": "",
            "platform": "rust"
        })
        .to_string();
        types::FfiResult::success(json)
    } else if flag_full != 0 {
        types::FfiResult::success(format!("defradb v{} (rust)", version))
    } else {
        types::FfiResult::success(version.to_string())
    }
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
        assert!(version_str.starts_with("0."));

        unsafe { defra_free_string(version) };
    }

    #[test]
    fn test_version_get_short() {
        let result = version_get(0, 0);
        assert_eq!(result.status, 0);
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.starts_with("0."), "should be version number");
        unsafe { defra_free_string(result.value) };
    }

    #[test]
    fn test_version_get_full() {
        let result = version_get(1, 0);
        assert_eq!(result.status, 0);
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("defradb"), "should contain defradb");
        assert!(value.contains("rust"), "should indicate rust platform");
        unsafe { defra_free_string(result.value) };
    }

    #[test]
    fn test_version_get_json() {
        let result = version_get(0, 1);
        assert_eq!(result.status, 0);
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        let parsed: serde_json::Value = serde_json::from_str(&value).unwrap();
        assert!(parsed["version"].as_str().unwrap().starts_with("0."));
        assert_eq!(parsed["platform"].as_str().unwrap(), "rust");
        unsafe { defra_free_string(result.value) };
    }

    #[test]
    fn test_full_workflow() {
        use std::ffi::CString;
        use types::NodeInitOptions;

        defra_init();

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0, "new_node failed");
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type Person { name: String, age: Int }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr(), 0) };
        assert_eq!(result.status, 0, "add_schema failed");
        if !result.value.is_null() {
            unsafe { defra_free_string(result.value) };
        }

        // Create a person
        let mutation = CString::new(
            r#"mutation { create_Person(input: {name: "Bob", age: 30}) { _docID name age } }"#,
        )
        .unwrap();
        let result = unsafe { execute_query(node, mutation.as_ptr(), 0, ptr::null(), ptr::null()) };
        assert_eq!(result.status, 0, "mutation failed");
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Bob"), "should contain Bob");
        unsafe { defra_free_string(result.value) };

        // Query people
        let query_str = CString::new("{ Person { name age } }").unwrap();
        let result =
            unsafe { execute_query(node, query_str.as_ptr(), 0, ptr::null(), ptr::null()) };
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
