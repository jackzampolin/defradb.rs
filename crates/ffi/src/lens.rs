//! Lens FFI functions for DefraDB.
//!
//! This module provides FFI functions for lens transform operations:
//! - Adding lens transforms
//! - Listing lens transforms
//! - Setting lens migrations between schema versions

use std::ffi::c_char;

use crate::get_runtime;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

use lens::{LensConfig, LensModule};

/// Add a lens transform to the database.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `lens_json` - JSON string containing the lens configuration
///
/// # Safety
///
/// `lens_json` must be a valid null-terminated UTF-8 string.
#[export_name = "LensAdd"]
pub unsafe extern "C" fn lens_add(node_ptr: usize, lens_json: *const c_char) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let lens_str = match c_str_to_string(lens_json) {
        Some(s) => s,
        None => return FfiResult::error("lens_json is null"),
    };

    let lens_store = match NODES.get(node_ptr, |state| state.database.lens_store().clone()) {
        Some(store) => store,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let lens_module: LensModule = serde_json::from_str(&lens_str)
            .map_err(|e| format!("failed to parse lens config: {}", e))?;

        let config = LensConfig::new("", "", lens_module);

        let lens_id = lens_store
            .add(config)
            .await
            .map_err(|e| format!("failed to add lens: {}", e))?;

        Ok::<String, String>(lens_id.to_string())
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
#[export_name = "LensList"]
pub extern "C" fn lens_list(node_ptr: usize) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let lens_store = match NODES.get(node_ptr, |state| state.database.lens_store().clone()) {
        Some(store) => store,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let lenses = lens_store
            .list()
            .await
            .map_err(|e| format!("failed to list lenses: {}", e))?;

        let json = serde_json::to_string(&lenses)
            .map_err(|e| format!("failed to serialize lenses: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Set a lens migration between two schema versions.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `src_version` - Source schema version ID
/// * `dst_version` - Destination schema version ID
/// * `lens_cfg_json` - JSON string containing the lens module configuration
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[export_name = "LensSet"]
pub unsafe extern "C" fn lens_set(
    node_ptr: usize,
    src_version: *const c_char,
    dst_version: *const c_char,
    lens_cfg_json: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let src = match c_str_to_string(src_version) {
        Some(s) => s,
        None => return FfiResult::error("src_version is null"),
    };

    let dst = match c_str_to_string(dst_version) {
        Some(s) => s,
        None => return FfiResult::error("dst_version is null"),
    };

    let cfg_str = match c_str_to_string(lens_cfg_json) {
        Some(s) => s,
        None => return FfiResult::error("lens_cfg_json is null"),
    };

    let lens_store = match NODES.get(node_ptr, |state| state.database.lens_store().clone()) {
        Some(store) => store,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let lens_module: LensModule = serde_json::from_str(&cfg_str)
            .map_err(|e| format!("failed to parse lens config: {}", e))?;

        let config = LensConfig::new(&src, &dst, lens_module);

        lens_store
            .add(config)
            .await
            .map_err(|e| format!("failed to set lens migration: {}", e))?;

        Ok::<(), String>(())
    });

    match result {
        Ok(()) => FfiResult::ok(),
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

        let result = lens_list(node);
        assert_eq!(result.status, 0);
        assert!(!result.value.is_null());

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value == "{}" || value == "[]", "should be empty: {}", value);
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_lens_list_invalid_node() {
        assert!(crate::runtime::init_runtime());

        let result = lens_list(0);
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        unsafe { crate::types::defra_free_string(result.error) };
    }

    #[test]
    fn test_lens_set_invalid_node() {
        assert!(crate::runtime::init_runtime());

        let src = CString::new("v1").unwrap();
        let dst = CString::new("v2").unwrap();
        let cfg = CString::new(r#"{"Path": "/path/to/transform.wasm"}"#).unwrap();
        let result = unsafe { lens_set(0, src.as_ptr(), dst.as_ptr(), cfg.as_ptr()) };
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        unsafe { crate::types::defra_free_string(result.error) };
    }

    #[test]
    fn test_lens_set_null_params() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // null src
        let dst = CString::new("v2").unwrap();
        let cfg = CString::new(r#"{}"#).unwrap();
        let result = unsafe { lens_set(node, std::ptr::null(), dst.as_ptr(), cfg.as_ptr()) };
        assert_eq!(result.status, 1);

        // null dst
        let src = CString::new("v1").unwrap();
        let result = unsafe { lens_set(node, src.as_ptr(), std::ptr::null(), cfg.as_ptr()) };
        assert_eq!(result.status, 1);

        // null cfg
        let result = unsafe { lens_set(node, src.as_ptr(), dst.as_ptr(), std::ptr::null()) };
        assert_eq!(result.status, 1);

        if !result.error.is_null() {
            unsafe { crate::types::defra_free_string(result.error) };
        }

        node_close(node);
    }
}
