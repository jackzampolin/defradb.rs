//! Schema operations for FFI.
//!
//! This module exposes schema management functions that match
//! Go's cbindings/schema.go behavior.

use std::ffi::c_char;
use std::sync::Arc;

use acp::nac::NodePermission;

use crate::helpers::{get_node_database, get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::policy_yaml;
use crate::state::{PolicyStore, NODES};
use crate::types::FfiResult;
use crate::{ffi_async, try_ffi, ERR_INVALID_NODE_HANDLE};

/// Add a schema to the database.
///
/// The schema should be a GraphQL SDL string defining types.
///
/// Returns a JSON array of CollectionVersion objects on success.
///
/// # Example SDL
///
/// ```graphql
/// type User {
///     name: String
///     age: Int
/// }
/// ```
///
/// # Safety
///
/// `schema_sdl` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn add_schema(
    node_ptr: usize,
    identity_did: *const c_char,
    schema_sdl: *const c_char,
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::CollectionPatch
    ));
    let schema_str = try_ffi!(require_c_str(schema_sdl, "schema_sdl"));

    // Validate node handle and get both database and policy store
    let (database, policy_store) = match NODES.get(node_ptr, |state| {
        (state.database.clone(), state.policy_store.clone())
    }) {
        Some(pair) => pair,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    ffi_async!(rt, {
        // Get existing collection names so the SDL parser can resolve external type references
        // (e.g., relations to already-created collections)
        let known_types: std::collections::HashSet<String> = database
            .list_collections()
            .unwrap_or_default()
            .into_iter()
            .collect();

        // Parse the SDL into collection versions, passing known types for resolution
        let collections = query::parse_sdl_with_known_types(&schema_str, known_types)
            .map_err(|e| format!("failed to parse schema: {}", e))?;

        // Run global validators (embedding type checks, etc.)
        db::definition_validation::validate_new_collections(&collections)
            .map_err(|e| format!("failed to validate schema: {}", e))?;

        // Validate policies on collections before creating them
        for collection in &collections {
            if let Some(ref policy) = collection.policy {
                validate_collection_policy(policy, &policy_store)?;
            }
        }

        // Create each collection
        let mut created_versions = Vec::new();
        for schema in collections {
            let version = schema.clone();
            database
                .create_collection(schema)
                .await
                .map_err(|e| format!("failed to create collection: {}", e))?;
            created_versions.push(version);
        }

        // Return JSON array of created collection versions
        let json = serde_json::to_string(&created_versions)
            .map_err(|e| format!("failed to serialize result: {}", e))?;

        Ok(json)
    })
}

/// Get all collections from the database.
///
/// Returns a JSON array of collection descriptions.
///
/// # Safety
///
/// Caller must ensure all pointer arguments are valid, non-null, and point to valid C strings.
#[no_mangle]
pub unsafe extern "C" fn get_collections(
    node_ptr: usize,
    identity_did: *const c_char,
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::CollectionGet
    ));
    let database = try_ffi!(get_node_database(node_ptr));

    ffi_async!(rt, {
        // Return all collection versions from the system store (active + inactive + placeholders).
        // The Go wrapper handles IncludeInactive filtering on its side.
        let collections = database
            .get_all_collection_versions()
            .await
            .map_err(|e| format!("failed to get collections: {}", e))?;

        // Return JSON array
        let json = serde_json::to_string(&collections)
            .map_err(|e| format!("failed to serialize result: {}", e))?;

        Ok(json)
    })
}

/// Get all collection versions visible within a specific transaction.
///
/// This reads from the transaction's systemstore, which includes uncommitted
/// writes (e.g., placeholders from set_migration_in_txn).
///
/// # Safety
///
/// Caller must ensure all pointer arguments are valid, non-null, and point to valid C strings.
#[no_mangle]
pub unsafe extern "C" fn get_collections_in_txn(
    node_ptr: usize,
    txn_id: *const c_char,
    identity_did: *const c_char,
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::CollectionGet
    ));
    let txn_str = try_ffi!(require_c_str(txn_id, "txn_id"));

    let registry = match NODES.get(node_ptr, |state| state.txn_registry.clone()) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    ffi_async!(rt, {
        let collections = registry
            .get_collections_in_txn(&txn_str)
            .await
            .map_err(|e| format!("failed to get collections in txn: {}", e))?;

        let json = serde_json::to_string(&collections)
            .map_err(|e| format!("failed to serialize result: {}", e))?;

        Ok(json)
    })
}

/// Validate that a collection's policy references a valid, well-formed policy.
fn validate_collection_policy(
    policy: &schema::PolicyDescription,
    store: &Arc<PolicyStore>,
) -> Result<(), String> {
    // 1. Check policy exists in the store
    let policy_yaml = store
        .get_policy(&policy.id)
        .ok_or("policyID specified does not exist with acp")?;

    // 2. Parse the YAML to inspect structure
    let parsed = policy_yaml::parse_policy_yaml(&policy_yaml)
        .map_err(|e| format!("failed to parse policy: {}", e))?;

    // 3. Check the referenced resource exists
    let resource = parsed
        .find_resource(&policy.resource_name)
        .ok_or("resource does not exist on the specified policy")?;

    // 4. Check required permissions (read, update, delete)
    for required in &["read", "update", "delete"] {
        if !resource.has_permission(required) {
            return Err("resource is missing required permission on policy.".to_string());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::types::NodeInitOptions;
    use std::ffi::CString;

    #[test]
    fn test_add_schema_and_get_collections() {
        // Initialize runtime
        assert!(crate::runtime::init_runtime());

        // Create node
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0, "add_schema should succeed");

        // Get collections
        let result = unsafe { get_collections(node, std::ptr::null()) };
        assert_eq!(result.status, 0, "get_collections should succeed");
        assert!(!result.value.is_null());

        // Check value contains User
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("User"), "should contain User collection");

        // Cleanup
        unsafe {
            crate::types::defra_free_string(result.value);
        }
        node_close(node);
    }
}
