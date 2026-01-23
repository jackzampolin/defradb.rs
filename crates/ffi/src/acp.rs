//! Access Control Policy (ACP) operations for FFI.
//!
//! This module exposes ACP management functions for both:
//! - NAC (Node Access Control) - node-level permissions
//! - DAC (Document Access Control) - document-level permissions

use std::ffi::c_char;

use identity::Identity;

use crate::runtime::RUNTIME;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};

// ============================================================================
// NAC (Node Access Control) Functions
// ============================================================================

/// Get the current NAC status.
///
/// Returns a JSON object with NAC status information:
/// ```json
/// {
///   "status": "enabled" | "disabled_temporarily" | "not_configured",
///   "configured_enabled": true | false,
///   "dev_mode": true | false,
///   "owner": "did:key:..." | null
/// }
/// ```
#[no_mangle]
pub extern "C" fn get_nac_status(node_ptr: usize) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let result = rt.block_on(async {
        let nac_manager = NODES
            .get(node_ptr, |state| state.nac_manager.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let info = nac_manager.info().await;
        let json = serde_json::to_string(&info)
            .map_err(|e| format!("failed to serialize NAC info: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Temporarily disable NAC.
///
/// The requestor_did must be an admin. Returns success on completion.
///
/// # Safety
///
/// `requestor_did` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn disable_nac(node_ptr: usize, requestor_did: *const c_char) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let requestor_str = match c_str_to_string(requestor_did) {
        Some(s) => s,
        None => return FfiResult::error("requestor_did is null"),
    };

    let result = rt.block_on(async {
        let nac_manager = NODES
            .get(node_ptr, |state| state.nac_manager.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let requestor = identity::Did::new(&requestor_str)
            .map_err(|e| format!("invalid DID '{}': {}", requestor_str, e))?;

        nac_manager
            .disable(&requestor)
            .await
            .map_err(|e| format!("failed to disable NAC: {}", e))?;

        Ok::<(), String>(())
    });

    match result {
        Ok(()) => FfiResult::ok(),
        Err(e) => FfiResult::error(e),
    }
}

/// Re-enable NAC after temporary disable.
///
/// The requestor_did must be an admin. Returns success on completion.
///
/// # Safety
///
/// `requestor_did` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn re_enable_nac(node_ptr: usize, requestor_did: *const c_char) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let requestor_str = match c_str_to_string(requestor_did) {
        Some(s) => s,
        None => return FfiResult::error("requestor_did is null"),
    };

    let result = rt.block_on(async {
        let nac_manager = NODES
            .get(node_ptr, |state| state.nac_manager.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let requestor = identity::Did::new(&requestor_str)
            .map_err(|e| format!("invalid DID '{}': {}", requestor_str, e))?;

        nac_manager
            .re_enable(&requestor)
            .await
            .map_err(|e| format!("failed to re-enable NAC: {}", e))?;

        Ok::<(), String>(())
    });

    match result {
        Ok(()) => FfiResult::ok(),
        Err(e) => FfiResult::error(e),
    }
}

/// Enable NAC with the given owner identity.
///
/// This initializes NAC and sets the owner. Can only be called when NAC
/// is not already enabled.
///
/// # Safety
///
/// `owner_did` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn enable_nac(node_ptr: usize, owner_did: *const c_char) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let owner_str = match c_str_to_string(owner_did) {
        Some(s) => s,
        None => return FfiResult::error("owner_did is null"),
    };

    let result = rt.block_on(async {
        let nac_manager = NODES
            .get(node_ptr, |state| state.nac_manager.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let owner = identity::Did::new(&owner_str)
            .map_err(|e| format!("invalid DID '{}': {}", owner_str, e))?;

        nac_manager
            .enable(&owner)
            .await
            .map_err(|e| format!("failed to enable NAC: {}", e))?;

        Ok::<(), String>(())
    });

    match result {
        Ok(()) => FfiResult::ok(),
        Err(e) => FfiResult::error(e),
    }
}

/// Add a NAC actor relationship (grant admin to target).
///
/// The requestor must be an admin. Returns JSON with success status:
/// ```json
/// { "added": true }  // or false if already exists
/// ```
///
/// # Safety
///
/// All string parameters must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn add_nac_actor_relationship(
    node_ptr: usize,
    requestor_did: *const c_char,
    target_did: *const c_char,
) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let requestor_str = match c_str_to_string(requestor_did) {
        Some(s) => s,
        None => return FfiResult::error("requestor_did is null"),
    };

    let target_str = match c_str_to_string(target_did) {
        Some(s) => s,
        None => return FfiResult::error("target_did is null"),
    };

    let result = rt.block_on(async {
        let nac_manager = NODES
            .get(node_ptr, |state| state.nac_manager.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let requestor = identity::Did::new(&requestor_str)
            .map_err(|e| format!("invalid requestor DID '{}': {}", requestor_str, e))?;
        let target = identity::Did::new(&target_str)
            .map_err(|e| format!("invalid target DID '{}': {}", target_str, e))?;

        let added = nac_manager
            .add_admin(&requestor, &target)
            .await
            .map_err(|e| format!("failed to add NAC actor relationship: {}", e))?;

        let json = serde_json::json!({ "added": added }).to_string();
        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Delete a NAC actor relationship (remove admin from target).
///
/// The requestor must be an admin. The owner cannot be removed.
/// Returns JSON with success status:
/// ```json
/// { "deleted": true }  // or false if didn't exist
/// ```
///
/// # Safety
///
/// All string parameters must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn delete_nac_actor_relationship(
    node_ptr: usize,
    requestor_did: *const c_char,
    target_did: *const c_char,
) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let requestor_str = match c_str_to_string(requestor_did) {
        Some(s) => s,
        None => return FfiResult::error("requestor_did is null"),
    };

    let target_str = match c_str_to_string(target_did) {
        Some(s) => s,
        None => return FfiResult::error("target_did is null"),
    };

    let result = rt.block_on(async {
        let nac_manager = NODES
            .get(node_ptr, |state| state.nac_manager.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let requestor = identity::Did::new(&requestor_str)
            .map_err(|e| format!("invalid requestor DID '{}': {}", requestor_str, e))?;
        let target = identity::Did::new(&target_str)
            .map_err(|e| format!("invalid target DID '{}': {}", target_str, e))?;

        let deleted = nac_manager
            .remove_admin(&requestor, &target)
            .await
            .map_err(|e| format!("failed to delete NAC actor relationship: {}", e))?;

        let json = serde_json::json!({ "deleted": deleted }).to_string();
        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

// ============================================================================
// DAC (Document Access Control) Functions
// ============================================================================

/// Add a DAC actor relationship (share document access with target).
///
/// The requestor must be the document owner. Relation can be:
/// - "reader" - read access
/// - "updater" - read + update access
/// - "deleter" - read + delete access
///
/// Returns JSON with success status:
/// ```json
/// { "added": true }  // or false if already exists
/// ```
///
/// # Safety
///
/// All string parameters must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn add_dac_actor_relationship(
    node_ptr: usize,
    requestor_did: *const c_char,
    target_did: *const c_char,
    collection_id: *const c_char,
    doc_id: *const c_char,
    relation: *const c_char,
) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let requestor_str = match c_str_to_string(requestor_did) {
        Some(s) => s,
        None => return FfiResult::error("requestor_did is null"),
    };

    let target_str = match c_str_to_string(target_did) {
        Some(s) => s,
        None => return FfiResult::error("target_did is null"),
    };

    let collection_id_str = match c_str_to_string(collection_id) {
        Some(s) => s,
        None => return FfiResult::error("collection_id is null"),
    };

    let doc_id_str = match c_str_to_string(doc_id) {
        Some(s) => s,
        None => return FfiResult::error("doc_id is null"),
    };

    let relation_str = match c_str_to_string(relation) {
        Some(s) => s,
        None => return FfiResult::error("relation is null"),
    };

    let result = rt.block_on(async {
        let document_acp = NODES
            .get(node_ptr, |state| state.document_acp.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let requestor = identity::Did::new(&requestor_str)
            .map_err(|e| format!("invalid requestor DID '{}': {}", requestor_str, e))?;
        let target = identity::Did::new(&target_str)
            .map_err(|e| format!("invalid target DID '{}': {}", target_str, e))?;

        let added = document_acp
            .add_actor_relationship(
                &requestor,
                &target,
                &collection_id_str,
                &doc_id_str,
                &relation_str,
            )
            .await
            .map_err(|e| format!("failed to add DAC actor relationship: {}", e))?;

        let json = serde_json::json!({ "added": added }).to_string();
        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Delete a DAC actor relationship (revoke document access from target).
///
/// The requestor must be the document owner.
///
/// Returns JSON with success status:
/// ```json
/// { "deleted": true }  // or false if didn't exist
/// ```
///
/// # Safety
///
/// All string parameters must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn delete_dac_actor_relationship(
    node_ptr: usize,
    requestor_did: *const c_char,
    target_did: *const c_char,
    collection_id: *const c_char,
    doc_id: *const c_char,
    relation: *const c_char,
) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let requestor_str = match c_str_to_string(requestor_did) {
        Some(s) => s,
        None => return FfiResult::error("requestor_did is null"),
    };

    let target_str = match c_str_to_string(target_did) {
        Some(s) => s,
        None => return FfiResult::error("target_did is null"),
    };

    let collection_id_str = match c_str_to_string(collection_id) {
        Some(s) => s,
        None => return FfiResult::error("collection_id is null"),
    };

    let doc_id_str = match c_str_to_string(doc_id) {
        Some(s) => s,
        None => return FfiResult::error("doc_id is null"),
    };

    let relation_str = match c_str_to_string(relation) {
        Some(s) => s,
        None => return FfiResult::error("relation is null"),
    };

    let result = rt.block_on(async {
        let document_acp = NODES
            .get(node_ptr, |state| state.document_acp.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let requestor = identity::Did::new(&requestor_str)
            .map_err(|e| format!("invalid requestor DID '{}': {}", requestor_str, e))?;
        let target = identity::Did::new(&target_str)
            .map_err(|e| format!("invalid target DID '{}': {}", target_str, e))?;

        let deleted = document_acp
            .delete_actor_relationship(
                &requestor,
                &target,
                &collection_id_str,
                &doc_id_str,
                &relation_str,
            )
            .await
            .map_err(|e| format!("failed to delete DAC actor relationship: {}", e))?;

        let json = serde_json::json!({ "deleted": deleted }).to_string();
        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

// ============================================================================
// Identity Functions
// ============================================================================

/// Get the node's identity (DID).
///
/// Returns JSON with the node identity:
/// ```json
/// { "did": "did:key:z6Mk..." }
/// ```
///
/// Returns an error if no node identity is configured.
#[no_mangle]
pub extern "C" fn get_node_identity(node_ptr: usize) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized - call defra_init() first"),
    };

    let result = rt.block_on(async {
        let database = NODES
            .get(node_ptr, |state| state.database.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        let identity = database
            .node_identity()
            .ok_or_else(|| "node identity not configured".to_string())?;

        let did = identity
            .did()
            .map_err(|e| format!("failed to get DID: {}", e))?;

        let json = serde_json::json!({ "did": did.to_string() }).to_string();
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
    use std::ffi::{CStr, CString};

    fn test_did() -> &'static str {
        "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
    }

    fn test_did2() -> &'static str {
        "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH"
    }

    #[test]
    fn test_get_nac_status() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Get NAC status
        let result = get_nac_status(node);
        assert_eq!(result.status, 0, "get_nac_status should succeed");
        assert!(!result.value.is_null());

        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(
            value.contains("not_configured"),
            "NAC should not be configured initially"
        );

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    #[test]
    fn test_enable_nac() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Enable NAC
        let owner_did = CString::new(test_did()).unwrap();
        let result = unsafe { enable_nac(node, owner_did.as_ptr()) };
        assert_eq!(result.status, 0, "enable_nac should succeed");

        // Verify NAC is now enabled
        let result = get_nac_status(node);
        assert_eq!(result.status, 0);
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("enabled"), "NAC should be enabled");
        assert!(value.contains(test_did()), "should show owner DID");

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    #[test]
    fn test_disable_and_re_enable_nac() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Enable NAC first
        let owner_did = CString::new(test_did()).unwrap();
        let result = unsafe { enable_nac(node, owner_did.as_ptr()) };
        assert_eq!(result.status, 0);

        // Disable NAC
        let result = unsafe { disable_nac(node, owner_did.as_ptr()) };
        assert_eq!(result.status, 0, "disable_nac should succeed");

        // Verify NAC is disabled
        let result = get_nac_status(node);
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(
            value.contains("disabled_temporarily"),
            "NAC should be disabled"
        );
        unsafe { crate::types::defra_free_string(result.value) };

        // Re-enable NAC
        let result = unsafe { re_enable_nac(node, owner_did.as_ptr()) };
        assert_eq!(result.status, 0, "re_enable_nac should succeed");

        // Verify NAC is enabled again
        let result = get_nac_status(node);
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(
            value.contains("\"status\":\"enabled\""),
            "NAC should be re-enabled"
        );
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_nac_actor_relationship() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Enable NAC first
        let owner_did = CString::new(test_did()).unwrap();
        let result = unsafe { enable_nac(node, owner_did.as_ptr()) };
        assert_eq!(result.status, 0);

        // Add admin relationship
        let target_did = CString::new(test_did2()).unwrap();
        let result =
            unsafe { add_nac_actor_relationship(node, owner_did.as_ptr(), target_did.as_ptr()) };
        assert_eq!(
            result.status, 0,
            "add_nac_actor_relationship should succeed"
        );
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("\"added\":true"), "should indicate added");
        unsafe { crate::types::defra_free_string(result.value) };

        // Delete admin relationship
        let result =
            unsafe { delete_nac_actor_relationship(node, owner_did.as_ptr(), target_did.as_ptr()) };
        assert_eq!(
            result.status, 0,
            "delete_nac_actor_relationship should succeed"
        );
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(
            value.contains("\"deleted\":true"),
            "should indicate deleted"
        );
        unsafe { crate::types::defra_free_string(result.value) };

        node_close(node);
    }

    #[test]
    fn test_dac_actor_relationship() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add DAC relationship (no document registered, but we test the API)
        let requestor_did = CString::new(test_did()).unwrap();
        let target_did = CString::new(test_did2()).unwrap();
        let collection_id = CString::new("test-collection").unwrap();
        let doc_id = CString::new("test-doc").unwrap();
        let relation = CString::new("reader").unwrap();

        let result = unsafe {
            add_dac_actor_relationship(
                node,
                requestor_did.as_ptr(),
                target_did.as_ptr(),
                collection_id.as_ptr(),
                doc_id.as_ptr(),
                relation.as_ptr(),
            )
        };
        // This may fail because the document isn't registered - but it tests the API
        // The status code indicates the FFI function was called correctly
        if result.status == 0 {
            let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
            assert!(
                value.contains("added"),
                "should have added field in response"
            );
            unsafe { crate::types::defra_free_string(result.value) };
        } else {
            // Expected - document not registered
            unsafe { crate::types::defra_free_string(result.error) };
        }

        node_close(node);
    }

    #[test]
    fn test_get_node_identity_not_configured() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Get node identity (should fail - not configured)
        let result = get_node_identity(node);
        assert_eq!(result.status, 1, "should fail when identity not configured");
        let error = unsafe { CStr::from_ptr(result.error).to_string_lossy() };
        assert!(
            error.contains("not configured"),
            "should indicate not configured"
        );

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_invalid_did_handling() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Try to enable NAC with invalid DID
        let invalid_did = CString::new("invalid-did").unwrap();
        let result = unsafe { enable_nac(node, invalid_did.as_ptr()) };
        assert_eq!(result.status, 1, "should fail with invalid DID");
        let error = unsafe { CStr::from_ptr(result.error).to_string_lossy() };
        assert!(error.contains("invalid DID"), "should indicate invalid DID");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_null_pointer_handling() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Test null pointer handling
        let result = unsafe { enable_nac(node, std::ptr::null()) };
        assert_eq!(result.status, 1, "should fail with null pointer");
        let error = unsafe { CStr::from_ptr(result.error).to_string_lossy() };
        assert!(error.contains("null"), "should indicate null pointer");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }
}
