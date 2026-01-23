//! Collection management operations for FFI.
//!
//! This module exposes collection lifecycle and management functions
//! that match Go's collection management behavior.

use std::ffi::c_char;

use crate::runtime::RUNTIME;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};

/// Get a collection by name.
///
/// Returns a JSON object containing the collection's schema (CollectionVersion)
/// if found, or an error if the collection doesn't exist.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `name` - The collection name
///
/// # Returns
///
/// - Status 0: Success (value contains JSON CollectionVersion)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `name` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn get_collection_by_name(node_ptr: usize, name: *const c_char) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let name_str = match c_str_to_string(name) {
        Some(s) => s,
        None => return FfiResult::error("name is null"),
    };

    let result = rt.block_on(async {
        let database = NODES
            .get(node_ptr, |state| state.database.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let collection = database
            .get_collection(&name_str)
            .map_err(|e| format!("failed to get collection: {}", e))?
            .ok_or_else(|| format!("collection '{}' not found", name_str))?;

        let json = serde_json::to_string(collection.schema())
            .map_err(|e| format!("failed to serialize collection: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Check if a collection exists by name.
///
/// Returns a JSON boolean: `true` if the collection exists, `false` otherwise.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `name` - The collection name to check
///
/// # Returns
///
/// - Status 0: Success (value contains "true" or "false")
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `name` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn has_collection(node_ptr: usize, name: *const c_char) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let name_str = match c_str_to_string(name) {
        Some(s) => s,
        None => return FfiResult::error("name is null"),
    };

    let result = rt.block_on(async {
        let database = NODES
            .get(node_ptr, |state| state.database.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let exists = database
            .has_collection(&name_str)
            .map_err(|e| format!("failed to check collection: {}", e))?;

        Ok::<String, String>(exists.to_string())
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

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
pub unsafe extern "C" fn delete_collection(node_ptr: usize, name: *const c_char) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let name_str = match c_str_to_string(name) {
        Some(s) => s,
        None => return FfiResult::error("name is null"),
    };

    let result = rt.block_on(async {
        let database = NODES
            .get(node_ptr, |state| state.database.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        database
            .delete_collection(&name_str)
            .await
            .map_err(|e| format!("failed to delete collection: {}", e))?;

        Ok::<String, String>("{}".to_string())
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Find a collection by its collection ID (schema version ID).
///
/// This is useful for P2P sync where we receive blocks with schema_version_id
/// and need to find the corresponding collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `collection_id` - The collection ID (schema version ID)
///
/// # Returns
///
/// - Status 0: Success (value contains JSON CollectionVersion or "null" if not found)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `collection_id` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn find_collection_by_id(
    node_ptr: usize,
    collection_id: *const c_char,
) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let id_str = match c_str_to_string(collection_id) {
        Some(s) => s,
        None => return FfiResult::error("collection_id is null"),
    };

    let result = rt.block_on(async {
        let database = NODES
            .get(node_ptr, |state| state.database.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let collection = database
            .find_collection_by_id(&id_str)
            .map_err(|e| format!("failed to find collection: {}", e))?;

        let json = match collection {
            Some(c) => serde_json::to_string(c.schema())
                .map_err(|e| format!("failed to serialize collection: {}", e))?,
            None => "null".to_string(),
        };

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
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
    version_id: *const c_char,
) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let version_str = match c_str_to_string(version_id) {
        Some(s) => s,
        None => return FfiResult::error("version_id is null"),
    };

    let result = rt.block_on(async {
        let database = NODES
            .get(node_ptr, |state| state.database.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        database
            .set_active_collection_version(&version_str)
            .await
            .map_err(|e| format!("failed to set active collection version: {}", e))?;

        Ok::<String, String>("{}".to_string())
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
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
    collection_name: *const c_char,
    patch: *const c_char,
) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let name_str = match c_str_to_string(collection_name) {
        Some(s) => s,
        None => return FfiResult::error("collection_name is null"),
    };

    let patch_str = match c_str_to_string(patch) {
        Some(s) => s,
        None => return FfiResult::error("patch is null"),
    };

    let result = rt.block_on(async {
        let database = NODES
            .get(node_ptr, |state| state.database.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let updated_schema = database
            .patch_collection(&name_str, &patch_str)
            .await
            .map_err(|e| format!("failed to patch collection: {}", e))?;

        let json = serde_json::to_string(&updated_schema)
            .map_err(|e| format!("failed to serialize updated schema: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Get a collection by its version ID.
///
/// This searches all collections for one matching the given version ID.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `version_id` - The version ID to search for
///
/// # Returns
///
/// - Status 0: Success (value contains JSON CollectionVersion or "null" if not found)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `version_id` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn get_collection_by_version_id(
    node_ptr: usize,
    version_id: *const c_char,
) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let version_str = match c_str_to_string(version_id) {
        Some(s) => s,
        None => return FfiResult::error("version_id is null"),
    };

    let result = rt.block_on(async {
        let database = NODES
            .get(node_ptr, |state| state.database.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let collection = database
            .get_collection_by_version_id(&version_str)
            .map_err(|e| format!("failed to get collection: {}", e))?;

        let json = match collection {
            Some(c) => serde_json::to_string(c.schema())
                .map_err(|e| format!("failed to serialize collection: {}", e))?,
            None => "null".to_string(),
        };

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;

    #[test]
    fn test_get_collection_by_name() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Get collection by name
        let name = CString::new("User").unwrap();
        let result = unsafe { get_collection_by_name(node, name.as_ptr()) };
        assert_eq!(result.status, 0, "get_collection_by_name should succeed");
        assert!(!result.value.is_null());

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("User"), "should contain User collection");
        assert!(value.contains("name"), "should contain name field");

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    #[test]
    fn test_get_collection_by_name_not_found() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let name = CString::new("NonExistent").unwrap();
        let result = unsafe { get_collection_by_name(node, name.as_ptr()) };
        assert_eq!(
            result.status, 1,
            "should return error for non-existent collection"
        );
        assert!(!result.error.is_null());

        let error = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
        assert!(
            error.contains("not found"),
            "error should mention not found"
        );

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_get_collection_by_name_null_pointer() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let result = unsafe { get_collection_by_name(node, std::ptr::null()) };
        assert_eq!(result.status, 1, "should return error for null name");
        assert!(!result.error.is_null());

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_has_collection() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type Person { name: String }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Check existing collection
        let name = CString::new("Person").unwrap();
        let result = unsafe { has_collection(node, name.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "true");
        unsafe { crate::types::defra_free_string(result.value) };

        // Check non-existing collection
        let name = CString::new("NonExistent").unwrap();
        let result = unsafe { has_collection(node, name.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "false");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_delete_collection() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type ToDelete { field: String }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Verify it exists
        let name = CString::new("ToDelete").unwrap();
        let result = unsafe { has_collection(node, name.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "true");
        unsafe { crate::types::defra_free_string(result.value) };

        // Delete it
        let name = CString::new("ToDelete").unwrap();
        let result = unsafe { delete_collection(node, name.as_ptr()) };
        assert_eq!(result.status, 0, "delete_collection should succeed");
        unsafe { crate::types::defra_free_string(result.value) };

        // Verify it's gone
        let name = CString::new("ToDelete").unwrap();
        let result = unsafe { has_collection(node, name.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "false");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_find_collection_by_id() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type FindMe { data: String }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Extract collection ID from add_schema result
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        let collections: Vec<serde_json::Value> = serde_json::from_str(&value).unwrap();
        let collection_id = collections[0]["CollectionID"].as_str().unwrap();

        unsafe { crate::types::defra_free_string(result.value) };

        // Find by collection ID
        let id_cstr = CString::new(collection_id).unwrap();
        let result = unsafe { find_collection_by_id(node, id_cstr.as_ptr()) };
        assert_eq!(result.status, 0, "find_collection_by_id should succeed");
        assert!(!result.value.is_null());

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("FindMe"), "should contain FindMe collection");

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    #[test]
    fn test_find_collection_by_id_not_found() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let id = CString::new("bafkreibnonexistent").unwrap();
        let result = unsafe { find_collection_by_id(node, id.as_ptr()) };
        assert_eq!(result.status, 0, "should succeed with null value");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "null", "should return null for non-existent ID");

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
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Extract version ID from result
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        let collections: Vec<serde_json::Value> = serde_json::from_str(&value).unwrap();
        let version_id = collections[0]["VersionID"].as_str().unwrap();
        unsafe { crate::types::defra_free_string(result.value) };

        // Set active version (should succeed)
        let version_cstr = CString::new(version_id).unwrap();
        let result = unsafe { set_active_collection_version(node, version_cstr.as_ptr()) };
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
        let result = unsafe { set_active_collection_version(node, version_id.as_ptr()) };
        assert_eq!(result.status, 1, "should fail for non-existent version");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_get_collection_by_version_id() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type VersionTest { field: String }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Extract version ID
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        let collections: Vec<serde_json::Value> = serde_json::from_str(&value).unwrap();
        let version_id = collections[0]["VersionID"].as_str().unwrap();
        unsafe { crate::types::defra_free_string(result.value) };

        // Get by version ID
        let version_cstr = CString::new(version_id).unwrap();
        let result = unsafe { get_collection_by_version_id(node, version_cstr.as_ptr()) };
        assert_eq!(
            result.status, 0,
            "get_collection_by_version_id should succeed"
        );

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("VersionTest"), "should contain VersionTest");
        unsafe { crate::types::defra_free_string(result.value) };

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
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Patch the collection - change is_active to false
        let patch = CString::new(r#"[{"op":"replace","path":"/IsActive","value":false}]"#).unwrap();
        let name = CString::new("Patchable").unwrap();
        let result = unsafe { patch_collection(node, name.as_ptr(), patch.as_ptr()) };
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
        let result = unsafe { patch_collection(node, name.as_ptr(), patch.as_ptr()) };
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
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Invalid patch - not valid JSON
        let patch = CString::new("not valid json").unwrap();
        let name = CString::new("PatchTest").unwrap();
        let result = unsafe { patch_collection(node, name.as_ptr(), patch.as_ptr()) };
        assert_eq!(result.status, 1, "should fail for invalid patch");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }
}
