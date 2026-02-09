//! Lens FFI functions for DefraDB.
//!
//! This module provides FFI functions for lens transform operations:
//! - Adding lens transforms
//! - Listing lens transforms

use std::ffi::c_char;

use crate::get_runtime;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

use lens::{LensConfig, LensModule};

/// Add a lens transform to the database.
///
/// This registers a lens transform without linking it to schema versions.
/// Use `set_migration` to link a transform between specific versions.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `lens_json` - JSON string containing the lens configuration:
///   - `Path`: Optional path to WASM module file
///   - `Module`: Optional base64-encoded WASM bytes
///   - `Arguments`: Optional JSON arguments for the module
///
/// # Returns
///
/// - Status 0: Success (value contains the lens ID)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `lens_json` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn lens_add(node_ptr: usize, lens_json: *const c_char) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let lens_str = match c_str_to_string(lens_json) {
        Some(s) => s,
        None => return FfiResult::error("lens_json is null"),
    };

    // If the JSON contains version IDs, this is a full migration config — delegate to
    // set_migration on the database so the transform gets linked to schema versions.
    if let Ok(full_config) = serde_json::from_str::<LensConfig>(&lens_str) {
        if !full_config.source_schema_version_id.is_empty()
            && !full_config.destination_schema_version_id.is_empty()
        {
            let database = match NODES.get(node_ptr, |state| state.database.clone()) {
                Some(db) => db,
                None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
            };

            let result = rt.block_on(async {
                let transform_id = database
                    .set_migration(full_config)
                    .await
                    .map_err(|e| format!("failed to set migration: {}", e))?;
                Ok::<String, String>(transform_id.to_string())
            });

            return match result {
                Ok(id) => FfiResult::success(&id),
                Err(e) => FfiResult::error(&e),
            };
        }
    }

    // No version IDs — register as standalone lens module(s)
    let lens_store = match NODES.get(node_ptr, |state| state.database.lens_store().clone()) {
        Some(store) => store,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        // Try parsing as Go's model.Lens format ({"Lenses": [...]}) first,
        // then fall back to single LensModule
        let modules: Vec<LensModule> =
            if let Ok(lens_obj) = serde_json::from_str::<serde_json::Value>(&lens_str) {
                if let Some(lenses_arr) = lens_obj.get("Lenses").and_then(|v| v.as_array()) {
                    lenses_arr
                        .iter()
                        .map(|v| {
                            serde_json::from_value::<LensModule>(v.clone())
                                .map_err(|e| format!("failed to parse lens module: {}", e))
                        })
                        .collect::<std::result::Result<Vec<_>, _>>()?
                } else {
                    vec![serde_json::from_str::<LensModule>(&lens_str)
                        .map_err(|e| format!("failed to parse lens config: {}", e))?]
                }
            } else {
                vec![serde_json::from_str::<LensModule>(&lens_str)
                    .map_err(|e| format!("failed to parse lens config: {}", e))?]
            };

        let mut all_ids = Vec::new();
        for lens_module in modules {
            let config = LensConfig::new("", "", lens_module);
            let lens_id = lens_store
                .add(config)
                .await
                .map_err(|e| format!("failed to add lens: {}", e))?;
            all_ids.push(lens_id.to_string());
        }

        // Return comma-joined IDs so chained transforms are preserved
        Ok::<String, String>(all_ids.join(","))
    });

    match result {
        Ok(lens_id) => FfiResult::success(&lens_id),
        Err(e) => FfiResult::error(&e),
    }
}

/// List all lens transforms.
///
/// Returns a JSON object mapping lens IDs to their configurations.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
///
/// # Returns
///
/// - Status 0: Success (value contains JSON object of lenses)
/// - Status 1: Error (error field contains message)
///
/// # Example Response
///
/// ```json
/// {
///   "lens_0": {"Path": "/path/to/transform.wasm"},
///   "lens_1": {"Module": "base64...", "Arguments": {...}}
/// }
/// ```
///
/// # Safety
///
/// The caller must free the returned string with `defra_free_string`.
#[no_mangle]
pub unsafe extern "C" fn lens_list(node_ptr: usize, identity_did: *const c_char) -> FfiResult {
    let _ = identity_did;
    let rt = get_runtime!(FfiResult);

    // Validate node handle before entering async block
    let lens_store = match NODES.get(node_ptr, |state| state.database.lens_store().clone()) {
        Some(store) => store,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let lenses = lens_store
            .list()
            .await
            .map_err(|e| format!("failed to list lenses: {}", e))?;

        // Convert to JSON
        let json = serde_json::to_string(&lenses)
            .map_err(|e| format!("failed to serialize lenses: {}", e))?;

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
    fn test_lens_add_invalid_node() {
        assert!(crate::runtime::init_runtime());

        let lens_json = CString::new(r#"{"Path": "/path/to/transform.wasm"}"#).unwrap();
        let result = unsafe { lens_add(0, lens_json.as_ptr()) };
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        unsafe { crate::types::defra_free_string(result.error) };
    }

    #[test]
    fn test_lens_add_null_json() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let result = unsafe { lens_add(node, std::ptr::null()) };
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        unsafe { crate::types::defra_free_string(result.error) };

        node_close(node);
    }

    #[test]
    fn test_lens_list_empty() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let result = unsafe { lens_list(node, std::ptr::null()) };
        assert_eq!(result.status, 0);
        assert!(!result.value.is_null());

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        // Should be empty object
        assert!(value == "{}" || value == "[]", "should be empty: {}", value);
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_lens_list_invalid_node() {
        assert!(crate::runtime::init_runtime());

        let result = unsafe { lens_list(0, std::ptr::null()) };
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        unsafe { crate::types::defra_free_string(result.error) };
    }
}
