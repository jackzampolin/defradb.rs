//! Schema operations for FFI.
//!
//! This module exposes schema management functions that match
//! Go's cbindings/schema.go behavior.

use std::ffi::c_char;

use crate::runtime::RUNTIME;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};

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
pub unsafe extern "C" fn add_schema(node_ptr: usize, schema_sdl: *const c_char) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let schema_str = match c_str_to_string(schema_sdl) {
        Some(s) => s,
        None => return FfiResult::error("schema_sdl is null"),
    };

    let result = rt.block_on(async {
        // Get node state
        let database = NODES
            .get(node_ptr, |state| state.database.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        // Parse the SDL into collection versions
        let collections =
            query::parse_sdl(&schema_str).map_err(|e| format!("failed to parse schema: {}", e))?;

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

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Get all collections from the database.
///
/// Returns a JSON array of collection descriptions.
#[no_mangle]
pub extern "C" fn get_collections(node_ptr: usize) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let result = rt.block_on(async {
        // Get node state
        let database = NODES
            .get(node_ptr, |state| state.database.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        // Get collection names
        let names = database
            .list_collections()
            .map_err(|e| format!("failed to list collections: {}", e))?;

        // Get schemas for each collection, propagating errors
        let mut collections = Vec::new();
        for name in names {
            match database.get_collection(&name) {
                Ok(Some(collection)) => {
                    collections.push(collection.schema().clone());
                }
                Ok(None) => {
                    // Collection was deleted between list and get - skip it
                }
                Err(e) => {
                    return Err(format!("failed to get collection '{}': {}", name, e));
                }
            }
        }

        // Return JSON array
        let json = serde_json::to_string(&collections)
            .map_err(|e| format!("failed to serialize result: {}", e))?;

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
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
        assert_eq!(result.status, 0, "add_schema should succeed");

        // Get collections
        let result = get_collections(node);
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
