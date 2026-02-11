use std::ffi::c_char;

use acp::nac::NodePermission;

use crate::helpers::{get_node_database, get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::types::FfiResult;
use crate::{ffi_async, try_ffi};

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
pub unsafe extern "C" fn get_collection_by_name(
    node_ptr: usize,
    identity_did: *const c_char,
    name: *const c_char,
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::CollectionGet
    ));
    let name_str = try_ffi!(require_c_str(name, "name"));
    let database = try_ffi!(get_node_database(node_ptr));

    ffi_async!(rt, {
        let collection = database
            .get_collection(&name_str)
            .map_err(|e| format!("failed to get collection: {}", e))?
            .ok_or_else(|| format!("collection '{}' not found", name_str))?;

        let json = serde_json::to_string(collection.schema())
            .map_err(|e| format!("failed to serialize collection: {}", e))?;

        Ok(json)
    })
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
pub unsafe extern "C" fn has_collection(
    node_ptr: usize,
    identity_did: *const c_char,
    name: *const c_char,
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::CollectionGet
    ));
    let name_str = try_ffi!(require_c_str(name, "name"));
    let database = try_ffi!(get_node_database(node_ptr));

    ffi_async!(rt, {
        let exists = database
            .has_collection(&name_str)
            .map_err(|e| format!("failed to check collection: {}", e))?;

        Ok(exists.to_string())
    })
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
    identity_did: *const c_char,
    collection_id: *const c_char,
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::CollectionGet
    ));
    let id_str = try_ffi!(require_c_str(collection_id, "collection_id"));
    let database = try_ffi!(get_node_database(node_ptr));

    ffi_async!(rt, {
        let collection = database
            .find_collection_by_id(&id_str)
            .map_err(|e| format!("failed to find collection: {}", e))?;

        let json = match collection {
            Some(c) => serde_json::to_string(c.schema())
                .map_err(|e| format!("failed to serialize collection: {}", e))?,
            None => "null".to_string(),
        };

        Ok(json)
    })
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
    identity_did: *const c_char,
    version_id: *const c_char,
) -> FfiResult {
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::CollectionGet
    ));
    let version_str = try_ffi!(require_c_str(version_id, "version_id"));
    let database = try_ffi!(get_node_database(node_ptr));

    ffi_async!(rt, {
        let collection = database
            .get_collection_by_version_id_full(&version_str)
            .await
            .map_err(|e| format!("failed to get collection: {}", e))?;

        let json = match collection {
            Some(c) => serde_json::to_string(c.schema())
                .map_err(|e| format!("failed to serialize collection: {}", e))?,
            None => "null".to_string(),
        };

        Ok(json)
    })
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
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Get collection by name
        let name = CString::new("User").unwrap();
        let result = unsafe { get_collection_by_name(node, std::ptr::null(), name.as_ptr()) };
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
        let result = unsafe { get_collection_by_name(node, std::ptr::null(), name.as_ptr()) };
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

        let result = unsafe { get_collection_by_name(node, std::ptr::null(), std::ptr::null()) };
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
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        unsafe { crate::types::defra_free_string(result.value) };

        // Check existing collection
        let name = CString::new("Person").unwrap();
        let result = unsafe { has_collection(node, std::ptr::null(), name.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "true");
        unsafe { crate::types::defra_free_string(result.value) };

        // Check non-existing collection
        let name = CString::new("NonExistent").unwrap();
        let result = unsafe { has_collection(node, std::ptr::null(), name.as_ptr()) };
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
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Extract collection ID from add_schema result
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        let collections: Vec<serde_json::Value> = serde_json::from_str(&value).unwrap();
        let collection_id = collections[0]["CollectionID"].as_str().unwrap();

        unsafe { crate::types::defra_free_string(result.value) };

        // Find by collection ID
        let id_cstr = CString::new(collection_id).unwrap();
        let result = unsafe { find_collection_by_id(node, std::ptr::null(), id_cstr.as_ptr()) };
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
        let result = unsafe { find_collection_by_id(node, std::ptr::null(), id.as_ptr()) };
        assert_eq!(result.status, 0, "should succeed with null value");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "null", "should return null for non-existent ID");

        unsafe { crate::types::defra_free_string(result.value) };
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
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Extract version ID
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        let collections: Vec<serde_json::Value> = serde_json::from_str(&value).unwrap();
        let version_id = collections[0]["VersionID"].as_str().unwrap();
        unsafe { crate::types::defra_free_string(result.value) };

        // Get by version ID
        let version_cstr = CString::new(version_id).unwrap();
        let result =
            unsafe { get_collection_by_version_id(node, std::ptr::null(), version_cstr.as_ptr()) };
        assert_eq!(
            result.status, 0,
            "get_collection_by_version_id should succeed"
        );

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("VersionTest"), "should contain VersionTest");
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }
}
