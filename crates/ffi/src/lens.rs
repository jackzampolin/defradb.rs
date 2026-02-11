//! Lens FFI functions for DefraDB.
//!
//! This module provides FFI functions for lens transform operations:
//! - Adding lens transforms
//! - Listing lens transforms

use std::ffi::c_char;
use std::sync::Arc;

use blockstore::{Blockstore, DefraBlockstore};

use acp::nac::NodePermission;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::FfiResult;
use crate::{ffi_async, try_ffi, ERR_INVALID_NODE_HANDLE};

use lens::{LensConfig, LensModule, TransformId};

/// Read WASM bytes from a LensModule (from path or embedded bytes).
fn read_wasm_bytes(module: &LensModule) -> Result<Vec<u8>, String> {
    if let Some(ref bytes) = module.module {
        return Ok(bytes.clone());
    }
    if let Some(ref path_str) = module.path {
        let clean_path = path_str.strip_prefix("file://").unwrap_or(path_str);
        std::fs::read(clean_path)
            .map_err(|e| format!("failed to read WASM file {}: {}", clean_path, e))
    } else {
        Err("lens module has neither path nor module bytes".to_string())
    }
}

/// Extract arguments from a LensModule as key-value pairs for IPLD blocks.
fn extract_arguments(module: &LensModule) -> Vec<(String, String)> {
    match &module.arguments {
        Some(serde_json::Value::Object(map)) => map
            .iter()
            .map(|(k, v)| {
                let val = match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                (k.clone(), val)
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Add a lens transform to the database.
///
/// Builds Go-compatible IPLD blocks (ConfigBlock -> ModuleBlock -> LensBlock),
/// stores them in the blockstore for P2P Bitswap, and registers the transform
/// under the ConfigBlock CID.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `lens_json` - JSON string containing the lens configuration
///
/// # Returns
///
/// - Status 0: Success (value contains the lens CID)
/// - Status 1: Error (error field contains message)
///
/// # Safety
///
/// `lens_json` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn lens_add(node_ptr: usize, lens_json: *const c_char) -> FfiResult {
    let rt = try_ffi!(get_rt());
    let lens_str = try_ffi!(require_c_str(lens_json, "lens_json"));

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

    // No version IDs — register as standalone lens module(s).
    // Get both the lens store and the database store for blockstore access.
    let (lens_store, db_store) = match NODES.get(node_ptr, |state| {
        (
            state.database.lens_store().clone(),
            state.database.store().clone(),
        )
    }) {
        Some(pair) => pair,
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

        // Create a blockstore for storing IPLD blocks (non-P2P mode, no merge tracking)
        let blockstore = Arc::new(DefraBlockstore::new(db_store, false));

        let mut all_ids = Vec::new();
        for lens_module in &modules {
            // Read WASM bytes from file or embedded bytes
            let wasm_bytes = read_wasm_bytes(lens_module)?;
            let arguments = extract_arguments(lens_module);

            // Build the 3-level IPLD block hierarchy matching Go's format
            let (config_cid, blocks) =
                defra_core::build_lens_ipld_blocks(&wasm_bytes, lens_module.inverse, &arguments)?;

            // Store all blocks in the blockstore for Bitswap availability
            for (cid, data) in &blocks {
                eprintln!(
                    "[FFI-LENS-ADD] Storing block cid={} ({} bytes)",
                    cid,
                    data.len()
                );
                blockstore
                    .put(cid, data)
                    .await
                    .map_err(|e| format!("failed to store lens block: {}", e))?;
                // Verify the block was stored
                let has = blockstore
                    .has(cid)
                    .await
                    .map_err(|e| format!("failed to check block: {}", e))?;
                eprintln!("[FFI-LENS-ADD] Block {} stored: {}", cid, has);
            }

            // Register the transform under the real IPLD CID
            let config = LensConfig::new("", "", lens_module.clone());
            let transform_id = TransformId::new(config_cid.to_string());
            lens_store
                .add_with_id(transform_id, config)
                .await
                .map_err(|e| format!("failed to add lens: {}", e))?;

            all_ids.push(config_cid.to_string());
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
/// # Safety
///
/// The caller must free the returned string with `defra_free_string`.
#[no_mangle]
pub unsafe extern "C" fn lens_list(node_ptr: usize, identity_did: *const c_char) -> FfiResult {
    let rt = try_ffi!(get_rt());
    try_ffi!(check_nac_for_node(
        rt,
        node_ptr,
        identity_did,
        NodePermission::LensList
    ));

    let lens_store = match NODES.get(node_ptr, |state| state.database.lens_store().clone()) {
        Some(store) => store,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    ffi_async!(rt, {
        let lenses = lens_store
            .list()
            .await
            .map_err(|e| format!("failed to list lenses: {}", e))?;

        let json = serde_json::to_string(&lenses)
            .map_err(|e| format!("failed to serialize lenses: {}", e))?;

        Ok(json)
    })
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
