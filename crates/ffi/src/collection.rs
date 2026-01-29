//! Collection management operations for FFI.
//!
//! This module exposes collection lifecycle and management functions
//! that match Go's cbindings collection management behavior.
//! All functions use CollectionOptions + identity_ptr pattern.

use std::ffi::c_char;

use crate::get_runtime;
use crate::state::NODES;
use crate::types::{c_str_to_string, resolve_collection, CollectionOptions, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

/// Delete document(s) from a collection.
///
/// Supports deletion by doc_id or by filter.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `doc_id` - Document ID to delete (null if using filter)
/// * `filter` - JSON filter for bulk delete (null if using doc_id)
/// * `opts` - Collection options identifying which collection
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings or null.
#[export_name = "CollectionDelete"]
pub unsafe extern "C" fn collection_delete(
    node_ptr: usize,
    doc_id: *const c_char,
    _filter: *const c_char,
    opts: CollectionOptions,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let doc_id_opt = c_str_to_string(doc_id).filter(|s| !s.is_empty());

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let collection = match resolve_collection(&database, &opts) {
        Ok(c) => c,
        Err(e) => return FfiResult::error(e),
    };

    let col_name = collection.schema().name.clone();

    let runner = match NODES.get(node_ptr, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        match doc_id_opt {
            Some(id_str) => {
                let delete_gql = format!(
                    "mutation {{ delete_{name}(docID: \"{id}\") {{ _docID }} }}",
                    name = col_name,
                    id = id_str
                );
                let request = query::QueryRequest::new(delete_gql);
                let response = runner.execute(request).await;

                if !response.errors.is_empty() {
                    let error_msg = response
                        .errors
                        .iter()
                        .map(|e| e.message.clone())
                        .collect::<Vec<_>>()
                        .join("; ");
                    return Err(error_msg);
                }

                serde_json::to_string(&response.data)
                    .map_err(|e| format!("failed to serialize response: {}", e))
            }
            None => Err("either doc_id or filter must be provided".to_string()),
        }
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Set the active collection version.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `opts` - Collection options (version field used to identify the version to activate)
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// String pointers in `opts` must be null or valid null-terminated UTF-8 strings.
#[export_name = "SetActiveCollection"]
pub unsafe extern "C" fn set_active_collection(
    node_ptr: usize,
    opts: CollectionOptions,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let version_str = match opts.version_str() {
        Some(s) if !s.is_empty() => s,
        _ => return FfiResult::error("version is required in options"),
    };

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
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
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `patch` - A JSON patch string (RFC 6902 format)
/// * `lens_config` - Optional lens config JSON (null for none)
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings or null.
#[export_name = "CollectionPatch"]
pub unsafe extern "C" fn collection_patch(
    node_ptr: usize,
    patch: *const c_char,
    _lens_config: *const c_char,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let patch_str = match c_str_to_string(patch) {
        Some(s) => s,
        None => return FfiResult::error("patch is null"),
    };

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        // Parse patch to extract collection name from the patch target
        // Go sends patch as a JSON object with collection name as key
        let patch_value: serde_json::Value = serde_json::from_str(&patch_str)
            .map_err(|e| format!("failed to parse patch: {}", e))?;

        // If the patch is an object with collection name as key, extract it
        if let Some(obj) = patch_value.as_object() {
            if let Some((col_name, col_patch)) = obj.iter().next() {
                let patch_str = serde_json::to_string(col_patch)
                    .map_err(|e| format!("failed to serialize patch: {}", e))?;
                let updated_schema = database
                    .patch_collection(col_name, &patch_str)
                    .await
                    .map_err(|e| format!("failed to patch collection: {}", e))?;

                let json = serde_json::to_string(&updated_schema)
                    .map_err(|e| format!("failed to serialize updated schema: {}", e))?;

                return Ok::<String, String>(json);
            }
        }

        // If it's a JSON array, it's a raw patch - need collection name from context
        // For backwards compatibility, try to apply as-is
        Err("patch must be a JSON object with collection name as key".to_string())
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Describe collections matching the given options.
///
/// Returns a JSON array of CollectionVersion objects.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `opts` - Collection options (name to describe specific, empty for all)
///
/// # Safety
///
/// String pointers in `opts` must be null or valid null-terminated UTF-8 strings.
/// * `identity_ptr` - Identity handle (0 for no identity)
#[export_name = "CollectionDescribe"]
pub unsafe extern "C" fn collection_describe(
    node_ptr: usize,
    opts: CollectionOptions,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let name_opt = opts.name_str().filter(|s| !s.is_empty());

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        match name_opt {
            Some(name) => {
                let collection = database
                    .get_collection(&name)
                    .map_err(|e| format!("failed to get collection: {}", e))?
                    .ok_or_else(|| format!("collection '{}' not found", name))?;

                let versions = vec![collection.schema().clone()];
                serde_json::to_string(&versions).map_err(|e| format!("failed to serialize: {}", e))
            }
            None => {
                let names = database
                    .list_collections()
                    .map_err(|e| format!("failed to list collections: {}", e))?;

                let mut versions = Vec::new();
                for name in names {
                    if let Ok(Some(col)) = database.get_collection(&name) {
                        versions.push(col.schema().clone());
                    }
                }

                serde_json::to_string(&versions).map_err(|e| format!("failed to serialize: {}", e))
            }
        }
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

// =============================================================================
// View and Migration APIs
// =============================================================================

/// Add a view to the database.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `gql_query` - The GraphQL query defining the view
/// * `sdl` - The SDL schema for the view output type
/// * `transform` - Optional Lens transform configuration (JSON, null for none)
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings or null.
#[export_name = "ViewAdd"]
pub unsafe extern "C" fn view_add(
    node_ptr: usize,
    gql_query: *const c_char,
    sdl: *const c_char,
    transform: *const c_char,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let query_str = match c_str_to_string(gql_query) {
        Some(s) => s,
        None => return FfiResult::error("gql_query is null"),
    };

    let sdl_str = match c_str_to_string(sdl) {
        Some(s) => s,
        None => return FfiResult::error("sdl is null"),
    };

    let transform_opt = c_str_to_string(transform);

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let collections =
            query::parse_sdl(&sdl_str).map_err(|e| format!("failed to parse view SDL: {}", e))?;

        let mut query_source = schema::QuerySource::new(serde_json::Value::String(query_str));
        if let Some(ref t) = transform_opt {
            query_source = query_source.with_transform(t);
        }

        let mut created_versions = Vec::new();
        for mut col_version in collections {
            col_version.query = Some(query_source.clone());
            let version = col_version.clone();
            database
                .create_collection(col_version)
                .await
                .map_err(|e| format!("failed to create view collection: {}", e))?;
            created_versions.push(version);
        }

        let json = serde_json::to_string(&created_versions)
            .map_err(|e| format!("failed to serialize result: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Refresh view caches.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `opts` - Collection options to filter which views to refresh
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// String pointers in `opts` must be null or valid null-terminated UTF-8 strings.
#[export_name = "ViewRefresh"]
pub unsafe extern "C" fn view_refresh(
    _node_ptr: usize,
    _opts: CollectionOptions,
    _identity_ptr: usize,
) -> FfiResult {
    FfiResult::error("view_refresh is not yet implemented")
}

/// Set migration for collection versions (internal, used by LensSet).
///
/// # Safety
///
/// Caller must ensure `node_ptr` is a valid node handle.
pub unsafe fn set_migration_internal(node_ptr: usize, config: &str) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let lens_config: lens::LensConfig = serde_json::from_str(config)
            .map_err(|e| format!("failed to parse lens config: {}", e))?;

        let transform_id = database
            .set_migration(lens_config)
            .await
            .map_err(|e| format!("failed to set migration: {}", e))?;

        Ok::<String, String>(transform_id.to_string())
    });

    match result {
        Ok(transform_id) => FfiResult::success(&transform_id),
        Err(e) => FfiResult::error(&e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;
    use std::ptr;

    fn make_opts(name: *const c_char) -> CollectionOptions {
        CollectionOptions {
            version: ptr::null(),
            collection_id: ptr::null(),
            name,
            get_inactive: 0,
        }
    }

    #[test]
    fn test_collection_describe_specific() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type Animal { species: String }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr(), 0) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let name = CString::new("Animal").unwrap();
        let result = unsafe { collection_describe(node, make_opts(name.as_ptr()), 0) };
        assert_eq!(result.status, 0, "collection_describe should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Animal"), "should contain collection name");
        assert!(value.contains("species"), "should contain field name");

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    #[test]
    fn test_collection_describe_all() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl1 = CString::new("type Cat { name: String }").unwrap();
        let result = unsafe { add_schema(node, sdl1.as_ptr(), 0) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let sdl2 = CString::new("type Dog { breed: String }").unwrap();
        let result = unsafe { add_schema(node, sdl2.as_ptr(), 0) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let result = unsafe { collection_describe(node, make_opts(ptr::null()), 0) };
        assert_eq!(result.status, 0, "collection_describe all should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Cat"), "should contain Cat");
        assert!(value.contains("Dog"), "should contain Dog");

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    #[test]
    fn test_view_add() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr(), 0) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let gql_query = CString::new("{ User { name } }").unwrap();
        let view_sdl = CString::new("type UserView { name: String }").unwrap();
        let result =
            unsafe { view_add(node, gql_query.as_ptr(), view_sdl.as_ptr(), ptr::null(), 0) };
        assert_eq!(result.status, 0, "view_add should succeed");
        assert!(!result.value.is_null());

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("UserView"), "should contain view name");

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }
}
