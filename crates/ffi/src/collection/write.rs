use crate::ffi_node_db_async_body;
use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::{ffi_async, ffi_entry, try_ffi, ERR_INVALID_NODE_HANDLE};
use acp::nac::NodePermission;

/// Delete a collection by name.
///
/// Deletes the collection and all its documents.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `name` - The collection name to delete
///
/// # Returns
///
/// - Status 0: Success (value is empty)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `name` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn delete_collection(
    node_ptr: usize,
    identity_did: *const std::ffi::c_char,
    name: *const std::ffi::c_char,
) -> crate::types::FfiResult {
    ffi_node_db_async_body! {
        node = node_ptr,
        identity = identity_did,
        database = database,
        permission = NodePermission::CollectionPatch,
        name => name_str: "name";
        {
        if name_str.is_empty() {
            return Err("collection name can't be empty".to_string());
        }

        database
            .delete_collection(&name_str)
            .await
            .map_err(|e| format!("failed to delete collection: {}", e))?;

        Ok("{}".to_string())
    }
    }
}

/// Delete one or more collections by name.
///
/// # Safety
///
/// `names_json` must be a valid null-terminated UTF-8 JSON array of strings.
#[no_mangle]
pub unsafe extern "C" fn delete_collections(
    node_ptr: usize,
    identity_did: *const std::ffi::c_char,
    names_json: *const std::ffi::c_char,
    active_only: bool,
) -> crate::types::FfiResult {
    ffi_node_db_async_body! {
        node = node_ptr,
        identity = identity_did,
        database = database,
        permission = NodePermission::CollectionPatch,
        names_json => names_str: "names_json";
        {
        let names: Vec<String> = serde_json::from_str(&names_str)
            .map_err(|e| format!("failed to parse collection names JSON: {}", e))?;

        database
            .delete_collections(names, active_only)
            .await
            .map_err(|e| format!("failed to delete collections: {}", e))?;

        Ok("{}".to_string())
    }
    }
}

/// Delete collections or collection versions within an existing transaction.
///
/// # Safety
///
/// `txn_id` and `targets_json` must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn delete_collections_in_txn(
    node_ptr: usize,
    txn_id: *const std::ffi::c_char,
    identity_did: *const std::ffi::c_char,
    targets_json: *const std::ffi::c_char,
    active_only: bool,
) -> crate::types::FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::CollectionPatch
        ));
        let txn_id = try_ffi!(require_c_str(txn_id, "txn_id"));
        let targets_json = try_ffi!(require_c_str(targets_json, "targets_json"));
        let registry = match NODES.get(node_ptr, |state| state.txn_registry.clone()) {
            Some(registry) => registry,
            None => return crate::types::FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            crate::types::c_str_to_string(identity_did).filter(|value| !value.is_empty()),
        );

        ffi_async!(rt, {
            let targets: Vec<String> = serde_json::from_str(&targets_json)
                .map_err(|error| format!("failed to parse collection targets JSON: {}", error))?;
            registry
                .delete_collections_in_txn(&txn_id, targets, active_only)
                .await
                .map_err(|error| format!("failed to delete collections: {}", error))?;
            Ok("{}".to_string())
        })
    }
}

/// Set the active collection version.
///
/// This activates the collection with the given version ID and deactivates
/// any other versions of the same collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `version_id` - The version ID of the collection to activate
///
/// # Returns
///
/// - Status 0: Success (value is "{}")
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `version_id` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn set_active_collection_version(
    node_ptr: usize,
    identity_did: *const std::ffi::c_char,
    version_id: *const std::ffi::c_char,
) -> crate::types::FfiResult {
    ffi_node_db_async_body! {
        node = node_ptr,
        identity = identity_did,
        database = database,
        permission = NodePermission::CollectionPatch,
        version_id => version_str: "version_id";
        {
        database
            .set_active_collection_version(&version_str)
            .await
            .map_err(|e| format!("failed to set active collection version: {}", e))?;

        Ok("{}".to_string())
    }
    }
}

/// Set a collection version's active state within an existing transaction.
///
/// # Safety
///
/// `txn_id` and `version_id` must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn set_collection_active_in_txn(
    node_ptr: usize,
    txn_id: *const std::ffi::c_char,
    identity_did: *const std::ffi::c_char,
    version_id: *const std::ffi::c_char,
    is_active: bool,
) -> crate::types::FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::CollectionPatch
        ));
        let txn_id = try_ffi!(require_c_str(txn_id, "txn_id"));
        let version_id = try_ffi!(require_c_str(version_id, "version_id"));
        let registry = match NODES.get(node_ptr, |state| state.txn_registry.clone()) {
            Some(registry) => registry,
            None => return crate::types::FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };
        let _identity_guard = defra_core::current_identity::scoped_current_identity(
            crate::types::c_str_to_string(identity_did).filter(|value| !value.is_empty()),
        );

        ffi_async!(rt, {
            let version = registry
                .set_collection_active_in_txn(&txn_id, &version_id, is_active)
                .await
                .map_err(|error| format!("failed to update collection active state: {}", error))?;
            serde_json::to_string(&version)
                .map_err(|error| format!("failed to serialize collection version: {}", error))
        })
    }
}

/// Patch a collection's schema using JSON patch operations.
///
/// This applies the given JSON patch to the collection's schema,
/// validates the result, and updates the collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `collection_name` - The name of the collection to patch
/// * `patch` - A JSON patch string (RFC 6902 format)
///
/// # Returns
///
/// - Status 0: Success (value contains the updated CollectionVersion as JSON)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `collection_name` and `patch` must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn patch_collection(
    node_ptr: usize,
    identity_did: *const std::ffi::c_char,
    collection_name: *const std::ffi::c_char,
    patch: *const std::ffi::c_char,
) -> crate::types::FfiResult {
    ffi_node_db_async_body! {
        node = node_ptr,
        identity = identity_did,
        database = database,
        permission = NodePermission::CollectionPatch,
        collection_name => name_str: "collection_name",
        patch => patch_str: "patch";
        {
        let updated_schema = database
            .patch_collection(&name_str, &patch_str, None)
            .await
            .map_err(|e| format!("failed to patch collection: {}", e))?;

        let json = serde_json::to_string(&updated_schema)
            .map_err(|e| format!("failed to serialize updated schema: {}", e))?;

        Ok(json)
    }
    }
}

/// Truncate a collection: delete all documents while preserving the schema.
///
/// This removes all document data, CRDT heads, blocks, and index entries
/// for the collection. The collection schema remains intact.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `name` - The collection name to truncate
///
/// # Returns
///
/// - Status 0: Success (value is "{}")
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `name` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn truncate_collection(
    node_ptr: usize,
    identity_did: *const std::ffi::c_char,
    name: *const std::ffi::c_char,
) -> crate::types::FfiResult {
    ffi_node_db_async_body! {
        node = node_ptr,
        identity = identity_did,
        database = database,
        permission = NodePermission::CollectionTruncate,
        name => name_str: "name";
        {
        database
            .truncate_collection(&name_str, None)
            .await
            .map_err(|e| format!("failed to truncate collection: {}", e))?;

        Ok("{}".to_string())
    }
    }
}

/// Truncate documents matching a JSON filter while preserving the collection schema.
///
/// # Safety
///
/// `name` and `filter_json` must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn truncate_collection_with_filter(
    node_ptr: usize,
    identity_did: *const std::ffi::c_char,
    name: *const std::ffi::c_char,
    filter_json: *const std::ffi::c_char,
) -> crate::types::FfiResult {
    ffi_node_db_async_body! {
        node = node_ptr,
        identity = identity_did,
        database = database,
        permission = NodePermission::CollectionTruncate,
        name => name_str: "name",
        filter_json => filter_str: "filter_json";
        {
        let filter: serde_json::Value = serde_json::from_str(&filter_str)
            .map_err(|error| format!("invalid filter JSON: {error}"))?;
        let conditions = filter
            .as_object()
            .cloned()
            .ok_or_else(|| "filter must be a non-null JSON object".to_string())?;

        database
            .truncate_collection_with_filter(
                &name_str,
                query::Filter::from_conditions(conditions),
                None,
            )
            .await
            .map_err(|e| format!("failed to truncate collection: {}", e))?;

        Ok("{}".to_string())
    }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::read::has_collection;
    use crate::node::{new_node, node_close};
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;

    #[test]
    fn test_delete_collection() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type ToDelete { field: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Verify it exists
        let name = CString::new("ToDelete").unwrap();
        let result = unsafe { has_collection(node, std::ptr::null(), name.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "true");
        unsafe { crate::types::defra_free_string(result.value) };

        // Delete it
        let name = CString::new("ToDelete").unwrap();
        let result = unsafe { delete_collection(node, std::ptr::null(), name.as_ptr()) };
        assert_eq!(result.status, 0, "delete_collection should succeed");
        unsafe { crate::types::defra_free_string(result.value) };

        // Verify it's gone
        let name = CString::new("ToDelete").unwrap();
        let result = unsafe { has_collection(node, std::ptr::null(), name.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "false");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_delete_collection_empty_name_returns_validation_error() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let name = CString::new("").unwrap();
        let result = unsafe { delete_collection(node, std::ptr::null(), name.as_ptr()) };
        assert_eq!(result.status, 1);
        let error = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
        assert_eq!(error, "collection name can't be empty");
        unsafe { crate::types::defra_free_string(result.error) };

        node_close(node);
    }

    #[test]
    fn test_delete_collections() {
        assert!(crate::runtime::init_runtime());

        let result = new_node(NodeInitOptions::default());
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new(
            "type FirstCollection { field: String }\ntype SecondCollection { field: String }",
        )
        .unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        let names = CString::new(r#"["FirstCollection","SecondCollection"]"#).unwrap();
        let result = unsafe { delete_collections(node, std::ptr::null(), names.as_ptr(), false) };
        assert_eq!(result.status, 0, "delete_collections should succeed");
        unsafe { crate::types::defra_free_string(result.value) };

        for name in ["FirstCollection", "SecondCollection"] {
            let name = CString::new(name).unwrap();
            let result = unsafe { has_collection(node, std::ptr::null(), name.as_ptr()) };
            assert_eq!(result.status, 0);
            let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
            assert_eq!(value, "false");
            unsafe { crate::types::defra_free_string(result.value) };
        }

        node_close(node);
    }

    #[test]
    fn test_truncate_collection_with_filter_validates_json_object() {
        assert!(crate::runtime::init_runtime());

        let result = new_node(NodeInitOptions::default());
        assert_eq!(result.status, 0);
        let node = result.node_ptr;
        let sdl = CString::new("type FilteredTruncate { field: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        let name = CString::new("FilteredTruncate").unwrap();
        let filter = CString::new("{}").unwrap();
        let result = unsafe {
            truncate_collection_with_filter(node, std::ptr::null(), name.as_ptr(), filter.as_ptr())
        };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        for invalid in ["null", "[]"] {
            let filter = CString::new(invalid).unwrap();
            let result = unsafe {
                truncate_collection_with_filter(
                    node,
                    std::ptr::null(),
                    name.as_ptr(),
                    filter.as_ptr(),
                )
            };
            assert_eq!(result.status, 1);
            unsafe { crate::types::defra_free_string(result.error) };
        }

        node_close(node);
    }

    #[test]
    fn test_set_active_collection_version() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type Active { data: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Extract version ID from result
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        let collections: Vec<serde_json::Value> = serde_json::from_str(&value).unwrap();
        let version_id = collections[0]["VersionID"].as_str().unwrap();
        unsafe { crate::types::defra_free_string(result.value) };

        // Set active version (should succeed)
        let version_cstr = CString::new(version_id).unwrap();
        let result =
            unsafe { set_active_collection_version(node, std::ptr::null(), version_cstr.as_ptr()) };
        assert_eq!(
            result.status, 0,
            "set_active_collection_version should succeed"
        );
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_set_active_collection_version_not_found() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let version_id = CString::new("nonexistent-version-id").unwrap();
        let result =
            unsafe { set_active_collection_version(node, std::ptr::null(), version_id.as_ptr()) };
        assert_eq!(result.status, 1, "should fail for non-existent version");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_patch_collection() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type Patchable { original: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Patch the collection - change is_active to false
        let patch = CString::new(r#"[{"op":"replace","path":"/IsActive","value":false}]"#).unwrap();
        let name = CString::new("Patchable").unwrap();
        let result =
            unsafe { patch_collection(node, std::ptr::null(), name.as_ptr(), patch.as_ptr()) };
        assert_eq!(result.status, 0, "patch_collection should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Patchable"), "should contain Patchable");
        assert!(
            value.contains("\"IsActive\":false"),
            "should have IsActive:false"
        );
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_patch_collection_not_found() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let patch = CString::new(r#"[{"op":"replace","path":"/IsActive","value":false}]"#).unwrap();
        let name = CString::new("NonExistent").unwrap();
        let result =
            unsafe { patch_collection(node, std::ptr::null(), name.as_ptr(), patch.as_ptr()) };
        assert_eq!(result.status, 1, "should fail for non-existent collection");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_patch_collection_invalid_patch() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type PatchTest { field: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Invalid patch - not valid JSON
        let patch = CString::new("not valid json").unwrap();
        let name = CString::new("PatchTest").unwrap();
        let result =
            unsafe { patch_collection(node, std::ptr::null(), name.as_ptr(), patch.as_ptr()) };
        assert_eq!(result.status, 1, "should fail for invalid patch");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }
}
