use crate::ffi_node_db_async_body;
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
        database
            .delete_collection(&name_str)
            .await
            .map_err(|e| format!("failed to delete collection: {}", e))?;

        Ok("{}".to_string())
    }
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
