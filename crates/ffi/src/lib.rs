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
pub mod action;
pub mod backup;
pub mod batch;
pub mod block;
pub mod collection;
pub mod document;
pub mod encrypted_index;
pub mod helpers;
pub mod index;
pub mod lens;
pub mod mobile;
pub mod mobile_config;
pub mod nac_check;
pub mod node;
pub mod p2p;
mod policy_yaml;
#[cfg(feature = "profiling")]
pub mod profiling;
pub mod query;
pub mod runtime;
pub mod schema;
pub mod se_key;
pub mod state;
pub mod subscription;
pub mod txn;
pub mod types;

pub use helpers::{get_node_database, get_node_runner, get_rt, require_c_str};
pub use types::FfiPanicResult;

use std::ffi::{c_char, CString};

/// Error message for invalid node handle.
pub const ERR_INVALID_NODE_HANDLE: &str = "invalid node handle";

const DETERMINISTIC_TEST_CRYPTO_ENV: &str = "DEFRA_ALLOW_DETERMINISTIC_TEST_CRYPTO";
const RUST_FFI_CLIENT_ENV: &str = "DEFRA_CLIENT_RUST_FFI";

fn deterministic_test_crypto_env_enabled() -> bool {
    matches!(
        std::env::var(DETERMINISTIC_TEST_CRYPTO_ENV).as_deref(),
        Ok("1")
    )
}

fn rust_ffi_client_env_enabled() -> bool {
    matches!(std::env::var(RUST_FFI_CLIENT_ENV).as_deref(), Ok("true"))
}

fn should_use_deterministic_test_crypto_for_process() -> bool {
    let arg0 = std::env::args().next();
    should_use_deterministic_test_crypto_for_process_state(
        deterministic_test_crypto_env_enabled(),
        rust_ffi_client_env_enabled(),
        arg0.as_deref(),
    )
}

fn should_use_deterministic_test_crypto_for_process_state(
    env_enabled: bool,
    rust_ffi_client_enabled: bool,
    arg0: Option<&str>,
) -> bool {
    env_enabled
        && (rust_ffi_client_enabled
            || arg0.is_some_and(|arg0| should_use_deterministic_test_crypto(env_enabled, arg0)))
}

fn should_use_deterministic_test_crypto(env_enabled: bool, arg0: &str) -> bool {
    env_enabled
        && (arg0.ends_with(".test")
            || arg0.contains("/defradb/tests/")
            || arg0.contains("/__debug_bin"))
}

/// Extract a human-readable message from a caught panic payload.
pub fn extract_panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    let detail = panic
        .downcast_ref::<String>()
        .map(|s| s.as_str())
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("unknown panic");
    format!("internal error (panic): {}", detail)
}

/// Wrap an FFI function body in `catch_unwind` to prevent panics from
/// crossing the FFI boundary.
///
/// The return type must implement [`FfiPanicResult`]. Caught panics are
/// converted to error results via that trait.
///
/// With `panic = "abort"` (current release profile), panics abort the
/// process before unwinding so `catch_unwind` is a no-op. In debug/test
/// builds (which default to `panic = "unwind"`), panics are caught and
/// converted to error returns.
#[macro_export]
macro_rules! ffi_entry {
    ($($body:tt)*) => {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            $($body)*
        })) {
            Ok(result) => result,
            Err(panic) => {
                let msg = $crate::extract_panic_message(&panic);
                $crate::FfiPanicResult::from_panic(msg)
            }
        }
    };
}

/// Early-return on `Result<T, FfiResult>::Err`.
#[macro_export]
macro_rules! try_ffi {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(e) => return e,
        }
    };
}

/// Run an async block, convert `Result<String, String>` to `FfiResult`.
#[macro_export]
macro_rules! ffi_async {
    ($rt:expr, $body:block) => {{
        let result: Result<String, String> = $rt.block_on(async $body);
        match result {
            Ok(json) => $crate::types::FfiResult::success(json),
            Err(e) => $crate::types::FfiResult::error(e),
        }
    }};
}

/// Run an async block for void operations, convert `Result<(), String>` to `FfiResult`.
#[macro_export]
macro_rules! ffi_async_ok {
    ($rt:expr, $body:block) => {{
        let result: Result<(), String> = $rt.block_on(async $body);
        match result {
            Ok(()) => $crate::types::FfiResult::ok(),
            Err(e) => $crate::types::FfiResult::error(e),
        }
    }};
}

/// Expand the common `FfiResult` wrapper body for the
/// node-permission-database async path.
///
/// The exported `extern "C"` function item stays explicit so cbindgen can keep
/// emitting it in `defra.h`, while the repeated runtime / NAC / C-string /
/// database prelude is shared in one place.
#[macro_export]
macro_rules! ffi_node_db_async_body {
    (
        node = $node_ptr:ident,
        identity = $identity_did:ident,
        database = $database:ident,
        permission = $permission:expr
        $(, $arg:ident => $parsed:ident : $arg_name:literal)*
        $(,)?
        ;
        $body:block
    ) => {
        $crate::ffi_entry! {
            let rt = $crate::try_ffi!($crate::helpers::get_rt());
            $crate::try_ffi!($crate::nac_check::check_nac_for_node(
                rt,
                $node_ptr,
                $identity_did,
                $permission
            ));
            $(
                let ffi_arg_ptr = $arg;
                let ffi_arg_name = $arg_name;
                let $parsed = $crate::try_ffi!(unsafe {
                    $crate::helpers::require_c_str(ffi_arg_ptr, ffi_arg_name)
                });
            )*
            let $database = $crate::try_ffi!($crate::helpers::get_node_database($node_ptr));

            // Bind the caller's identity into the ambient context so DB-layer
            // NAC checks (which receive no explicit identity) resolve the actual
            // caller instead of the wildcard. The body runs on this thread via
            // `block_on`, so the thread-local set here is visible throughout it.
            // The guard restores the prior value on drop, so it never leaks into
            // the next request on this pooled thread. Mirrors `query::exec`.
            let __ffi_identity: Option<String> =
                unsafe { $crate::types::c_str_to_string($identity_did) }
                    .filter(|s| !s.is_empty());
            let _ffi_identity_guard =
                defra_core::current_identity::scoped_current_identity(__ffi_identity);

            $crate::ffi_async!(rt, $body)
        }
    };
}

// Re-export FFI functions at crate root
pub use acp::{
    add_dac_actor_relationship, add_dac_policy, add_nac_actor_relationship,
    bind_identity_bearer_token, create_identity, delete_dac_actor_relationship,
    delete_nac_actor_relationship, disable_nac, enable_nac, get_dac_policy, get_nac_status,
    get_node_identity, list_dac_policies, node_set_default_identity, re_enable_nac,
    register_remote_identity, register_remote_identity_bytes, RegisterIdentity,
};
pub use action::list_actions;
pub use backup::{basic_export, basic_import};
pub use batch::{batch_sign, batch_start};
pub use block::{block_verify_signature, block_verify_signature_in_txn};
pub use collection::{
    add_view, delete_collection, delete_collection_versions, delete_collections,
    delete_collections_in_txn, delete_documents, find_collection_by_id, gc_downsample_histories,
    get_collection_by_name, get_collection_by_version_id, has_collection, materialize_collection,
    patch_collection, refresh_views, set_active_collection_version, set_collection_active_in_txn,
    set_migration, truncate_collection,
};
pub use document::{collection_create, is_json_array, parse_duration, parse_string_array};
pub use encrypted_index::{
    add_encrypted_index, delete_encrypted_index, list_all_encrypted_indexes, list_encrypted_indexes,
};
pub use index::{create_index, delete_index, get_indexes, list_all_indexes};
pub use lens::{lens_add, lens_add_in_txn, lens_list, lens_list_in_txn};
pub use mobile::{
    defra_mobile_add_replicator, defra_mobile_close_node, defra_mobile_connect,
    defra_mobile_disconnect, defra_mobile_ensure_schema, defra_mobile_execute, defra_mobile_init,
    defra_mobile_notify_network_change, defra_mobile_open_node, defra_mobile_peer_info,
    defra_mobile_shareable_address, defra_mobile_sync_collection,
};
pub use node::{new_node, node_close};
pub use p2p::{
    new_node_with_p2p, p2p_active_peers, p2p_add_collections, p2p_add_documents,
    p2p_add_replicator, p2p_add_replicator_with_filter, p2p_connect, p2p_delete_collections,
    p2p_delete_documents, p2p_delete_replicator, p2p_disconnect, p2p_list_collections,
    p2p_list_documents, p2p_list_replicators, p2p_notify_network_change, p2p_peer_info,
    p2p_shareable_address, p2p_sync_branchable_collection, p2p_sync_collection_versions,
    p2p_sync_documents,
};
pub use query::exec_request;
pub use schema::{add_schema, add_schema_in_txn, get_collections, get_collections_in_txn};
pub use se_key::set_se_encryption_key;
pub use subscription::{
    close_graphql_subscription, close_subscription, create_merge_complete_subscription,
    create_subscription, poll_graphql_subscription, poll_subscription,
};
pub use txn::{begin_txn, commit_txn, exec_request_in_txn, rollback_txn};
pub use types::defra_free_string;

/// Initialize the FFI library.
///
/// This must be called once before any other FFI functions.
/// Safe to call multiple times.
#[no_mangle]
pub extern "C" fn defra_init() {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if should_use_deterministic_test_crypto_for_process() {
            crypto::encryption::nonce::set_deterministic_nonce(true);
            defra_core::encryption::set_deterministic_encryption_key(true);
        }
        // Ignore return value - errors will surface when operations are attempted
        let _ = runtime::init_runtime();
    }));
}

/// Get the library version.
///
/// Returns a null-terminated string that must be freed with `defra_free_string`.
#[no_mangle]
pub extern "C" fn defra_version() -> *mut c_char {
    match std::panic::catch_unwind(|| {
        let version = env!("CARGO_PKG_VERSION");
        // CARGO_PKG_VERSION is a compile-time constant without null bytes
        CString::new(version)
            .unwrap_or_else(|_| CString::new("unknown").unwrap())
            .into_raw()
    }) {
        Ok(ptr) => ptr,
        Err(_) => std::ptr::null_mut(),
    }
}

#[cfg(test)]
mod negative_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crypto::encryption::nonce::{deterministic_nonce_enabled, set_deterministic_nonce};
    use defra_core::encryption::{
        deterministic_encryption_key_enabled, set_deterministic_encryption_key,
    };
    use std::ffi::CStr;
    use std::ptr;

    #[test]
    fn test_defra_init() {
        set_deterministic_nonce(false);
        set_deterministic_encryption_key(false);
        defra_init();
        // Should be idempotent
        defra_init();
        assert!(
            !deterministic_nonce_enabled(),
            "defra_init must not enable deterministic nonces"
        );
        assert!(
            !deterministic_encryption_key_enabled(),
            "defra_init must not enable deterministic encryption keys"
        );
    }

    #[test]
    fn test_go_test_binary_detection_for_deterministic_test_crypto() {
        assert!(
            !should_use_deterministic_test_crypto(false, "/tmp/go-build123/tests.test"),
            "release FFI test crypto requires the explicit environment gate"
        );

        assert!(should_use_deterministic_test_crypto(
            true,
            "/tmp/go-build123/tests.test"
        ));
        assert!(should_use_deterministic_test_crypto(
            true,
            "/Users/me/defradb/tests/integration.test"
        ));
        assert!(should_use_deterministic_test_crypto(
            true,
            "/private/tmp/__debug_bin123"
        ));
        assert!(!should_use_deterministic_test_crypto(
            true,
            "/usr/local/bin/defradb"
        ));
    }

    #[test]
    fn test_ffi_test_env_detection_for_deterministic_test_crypto() {
        assert!(!should_use_deterministic_test_crypto_for_process_state(
            false,
            true,
            Some("/usr/local/bin/defradb")
        ));
        assert!(should_use_deterministic_test_crypto_for_process_state(
            true,
            true,
            Some("/usr/local/bin/defradb")
        ));
        assert!(!should_use_deterministic_test_crypto_for_process_state(
            true,
            false,
            Some("/usr/local/bin/defradb")
        ));
        assert!(should_use_deterministic_test_crypto_for_process_state(
            true,
            false,
            Some("/tmp/go-build123/tests.test")
        ));
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
            r#"mutation { add_Person(input: {name: "Bob", age: 30}) { _docID name age } }"#,
        )
        .unwrap();
        let result = unsafe {
            exec_request(
                node,
                ptr::null(),
                mutation.as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
            )
        };
        assert_eq!(result.status, 0, "mutation failed");
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Bob"), "should contain Bob");
        unsafe { defra_free_string(result.value) };

        // Query people
        let query_str = CString::new("{ Person { name age } }").unwrap();
        let result = unsafe {
            exec_request(
                node,
                ptr::null(),
                query_str.as_ptr(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
            )
        };
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
