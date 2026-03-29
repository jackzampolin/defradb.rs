use std::ffi::c_char;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::{ffi_async, ffi_async_ok, ffi_entry, try_ffi, ERR_INVALID_NODE_HANDLE};
use acp::nac::{is_valid_nac_relation, NodePermission};
use acp::normalize_auth_error;

/// Get the current NAC status.
///
/// Returns a JSON object with NAC status information:
/// ```json
/// {
///   "status": "enabled" | "disabled temporarily" | "not configured",
///   "configured_enabled": true | false,
///   "dev_mode": true | false,
///   "owner": "did:key:..." | null
/// }
/// ```
///
/// This function is NAC-gated with the `NacStatus` permission.
///
/// # Safety
///
/// Caller must ensure all pointer arguments are valid, non-null, and point to valid C strings.
#[no_mangle]
pub unsafe extern "C" fn get_nac_status(node_ptr: usize, identity_did: *const c_char) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::NacStatus
        ));

        let nac_manager = match NODES.get(node_ptr, |state| state.nac_manager.clone()) {
            Some(m) => m,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        ffi_async!(rt, {
            let info = nac_manager.info().await;
            let json = serde_json::to_string(&info)
                .map_err(|e| format!("failed to serialize NAC info: {}", e))?;

            Ok(json)
        })
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
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        let requestor_str = try_ffi!(require_c_str(requestor_did, "requestor_did"));

        let nac_manager = match NODES.get(node_ptr, |state| state.nac_manager.clone()) {
            Some(m) => m,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        ffi_async_ok!(rt, {
            // Check NAC state before validating DID (Go checks state first)
            let status = nac_manager.status().await;
            if status == acp::nac::NacStatus::NotConfigured {
                return Err("node acp is not configured".to_string());
            }
            if status == acp::nac::NacStatus::DisabledTemporarily {
                return Err("node acp is already disabled".to_string());
            }

            // Empty DID means no identity - not authorized
            if requestor_str.is_empty() {
                return Err(format!("not authorized to perform operation. Permission: {}", NodePermission::NacDisable));
            }

            let requestor = identity::Did::new(&requestor_str)
                .map_err(|e| format!("invalid DID '{}': {}", requestor_str, e))?;

            nac_manager
                .disable(&requestor)
                .await
                .map_err(|e| normalize_auth_error(e.to_string(), "disable-nac"))?;

            Ok(())
        })
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
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        let requestor_str = try_ffi!(require_c_str(requestor_did, "requestor_did"));

        let nac_manager = match NODES.get(node_ptr, |state| state.nac_manager.clone()) {
            Some(m) => m,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        ffi_async_ok!(rt, {
            // Check NAC state before validating DID (Go checks state first)
            let status = nac_manager.status().await;
            if status == acp::nac::NacStatus::NotConfigured {
                return Err("node acp is not configured".to_string());
            }
            if status == acp::nac::NacStatus::Enabled {
                return Err("node acp is already enabled".to_string());
            }

            // Empty DID means no identity - not authorized
            if requestor_str.is_empty() {
                return Err(
                    format!("not authorized to perform operation. Permission: {}", NodePermission::NacReEnable),
                );
            }

            let requestor = identity::Did::new(&requestor_str)
                .map_err(|e| format!("invalid DID '{}': {}", requestor_str, e))?;

            // Check admin using persisted relationships (is_admin returns true
            // for everyone when disabled, but re-enable needs real admin check)
            let is_admin = nac_manager
                .is_admin_persisted(&requestor)
                .await
                .unwrap_or(false);
            if !is_admin {
                return Err(
                    format!("not authorized to perform operation. Permission: {}", NodePermission::NacReEnable),
                );
            }

            nac_manager
                .re_enable(&requestor)
                .await
                .map_err(|e| normalize_auth_error(e.to_string(), "re-enable-nac"))?;

            Ok(())
        })
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
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        let owner_str = try_ffi!(require_c_str(owner_did, "owner_did"));

        let nac_manager = match NODES.get(node_ptr, |state| state.nac_manager.clone()) {
            Some(m) => m,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        ffi_async_ok!(rt, {
            let owner = identity::Did::new(&owner_str)
                .map_err(|e| format!("invalid DID '{}': {}", owner_str, e))?;

            nac_manager
                .enable(&owner)
                .await
                .map_err(|e| format!("failed to enable NAC: {}", e))?;

            Ok(())
        })
    }
}

/// Add a NAC actor relationship.
///
/// The requestor must be an admin. The relation can be "admin" or a
/// specific permission name (e.g., "read-document").
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
pub unsafe extern "C" fn add_nac_actor_relationship(
    node_ptr: usize,
    requestor_did: *const c_char,
    relation: *const c_char,
    target_did: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        let requestor_str = try_ffi!(require_c_str(requestor_did, "requestor_did"));

        let relation_str = match c_str_to_string(relation) {
            Some(s) if !s.is_empty() => s,
            _ => "admin".to_string(),
        };

        let target_str = try_ffi!(require_c_str(target_did, "target_did"));

        let nac_manager = match NODES.get(node_ptr, |state| state.nac_manager.clone()) {
            Some(m) => m,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        ffi_async!(rt, {
            // Check NAC state before validating DID (Go checks state first)
            let status = nac_manager.status().await;
            if status == acp::nac::NacStatus::NotConfigured {
                return Err("node acp is not configured".to_string());
            }
            if status == acp::nac::NacStatus::DisabledTemporarily {
                return Err("operation requires ACP, but ACP not available".to_string());
            }

            // Empty or wildcard requestor: check if wildcard admin exists to decide error
            if requestor_str.is_empty() || requestor_str == "*" {
                let wildcard = identity::Did::wildcard();
                let wildcard_is_admin = nac_manager.is_admin(&wildcard).await.unwrap_or(false);
                if wildcard_is_admin {
                    return Err("node acp relationship operation requires identity".to_string());
                } else {
                    return Err(
                        format!("not authorized to perform operation. Permission: {}", NodePermission::NacRelationAdd),
                    );
                }
            }

            let requestor = identity::Did::new(&requestor_str).map_err(|_| {
                format!("not authorized to perform operation. Permission: {}", NodePermission::NacRelationAdd)
            })?;

            // Check admin authorization BEFORE validating target DID
            if !nac_manager.is_admin(&requestor).await.unwrap_or(false) {
                return Err(
                    format!("not authorized to perform operation. Permission: {}", NodePermission::NacRelationAdd),
                );
            }

            // Validate relation name against NAC policy
            if !is_valid_nac_relation(&relation_str) {
                return Err("relation not in resource".to_string());
            }

            // Owner relation cannot be modified
            if relation_str == "owner" {
                return Err("relation not in resource".to_string());
            }

            // Validate target DID
            if target_str.is_empty() {
                return Err("actor must be a valid did".to_string());
            }

            let target = if target_str == "*" {
                identity::Did::wildcard()
            } else {
                identity::Did::new(&target_str)
                    .map_err(|e| format!("invalid target DID '{}': {}", target_str, e))?
            };

            // Route to appropriate operation based on relation
            let added = if relation_str == "admin" {
                nac_manager
                    .add_admin(&requestor, &target)
                    .await
                    .map_err(|e| normalize_auth_error(e.to_string(), "add-nac-relation"))?
            } else if let Some(perm) = NodePermission::parse(&relation_str) {
                // Grant specific permission
                nac_manager
                    .add_permission_grant(&requestor, &target, perm)
                    .await
                    .map_err(|e| normalize_auth_error(e.to_string(), "add-nac-relation"))?
            } else {
                return Err("relation not in resource".to_string());
            };

            let json = serde_json::json!({ "added": added }).to_string();
            Ok(json)
        })
    }
}

/// Delete a NAC actor relationship.
///
/// The requestor must be an admin. The owner cannot be removed.
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
pub unsafe extern "C" fn delete_nac_actor_relationship(
    node_ptr: usize,
    requestor_did: *const c_char,
    relation: *const c_char,
    target_did: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        let requestor_str = try_ffi!(require_c_str(requestor_did, "requestor_did"));

        let relation_str = match c_str_to_string(relation) {
            Some(s) if !s.is_empty() => s,
            _ => "admin".to_string(),
        };

        let target_str = try_ffi!(require_c_str(target_did, "target_did"));

        let nac_manager = match NODES.get(node_ptr, |state| state.nac_manager.clone()) {
            Some(m) => m,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        ffi_async!(rt, {
            // Check NAC state before validating DID (Go checks state first)
            let status = nac_manager.status().await;
            if status == acp::nac::NacStatus::NotConfigured {
                return Err("node acp is not configured".to_string());
            }
            if status == acp::nac::NacStatus::DisabledTemporarily {
                return Err("operation requires ACP, but ACP not available".to_string());
            }

            // Empty or wildcard requestor: check if wildcard admin exists to decide error
            if requestor_str.is_empty() || requestor_str == "*" {
                let wildcard = identity::Did::wildcard();
                let wildcard_is_admin = nac_manager.is_admin(&wildcard).await.unwrap_or(false);
                if wildcard_is_admin {
                    return Err("node acp relationship operation requires identity".to_string());
                } else {
                    return Err(
                        format!("not authorized to perform operation. Permission: {}", NodePermission::NacRelationDelete),
                    );
                }
            }

            let requestor = identity::Did::new(&requestor_str).map_err(|_| {
                format!("not authorized to perform operation. Permission: {}", NodePermission::NacRelationDelete)
            })?;

            // Check admin authorization BEFORE validating target DID
            if !nac_manager.is_admin(&requestor).await.unwrap_or(false) {
                return Err(
                    format!("not authorized to perform operation. Permission: {}", NodePermission::NacRelationDelete),
                );
            }

            // Validate relation name against NAC policy
            if !is_valid_nac_relation(&relation_str) {
                return Err("relation not in resource".to_string());
            }

            // Owner relation cannot be modified
            if relation_str == "owner" {
                return Err("relation not in resource".to_string());
            }

            // Empty target with authorized requestor: Go returns {deleted: false}
            if target_str.is_empty() {
                let json = serde_json::json!({ "deleted": false }).to_string();
                return Ok(json);
            }

            let target = if target_str == "*" {
                identity::Did::wildcard()
            } else {
                identity::Did::new(&target_str)
                    .map_err(|e| format!("invalid target DID '{}': {}", target_str, e))?
            };

            let deleted = if relation_str == "admin" {
                nac_manager
                    .remove_admin(&requestor, &target)
                    .await
                    .map_err(|e| normalize_auth_error(e.to_string(), "delete-nac-relation"))?
            } else if let Some(perm) = NodePermission::parse(&relation_str) {
                nac_manager
                    .remove_permission_grant(&requestor, &target, perm)
                    .await
                    .map_err(|e| normalize_auth_error(e.to_string(), "delete-nac-relation"))?
            } else {
                return Err("relation not in resource".to_string());
            };

            let json = serde_json::json!({ "deleted": deleted }).to_string();
            Ok(json)
        })
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
        let result = unsafe { get_nac_status(node, std::ptr::null()) };
        assert_eq!(result.status, 0, "get_nac_status should succeed");
        assert!(!result.value.is_null());

        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(
            value.contains("not configured"),
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

        // Verify NAC is now enabled (pass owner DID since NAC is active)
        let result = unsafe { get_nac_status(node, owner_did.as_ptr()) };
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

        // Verify NAC is disabled (null identity OK since NAC is disabled)
        let result = unsafe { get_nac_status(node, std::ptr::null()) };
        assert_eq!(
            result.status, 0,
            "get_nac_status should succeed when disabled"
        );
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(
            value.contains("disabled temporarily"),
            "NAC should be disabled, got: {}",
            value
        );
        unsafe { crate::types::defra_free_string(result.value) };

        // Re-enable NAC
        let result = unsafe { re_enable_nac(node, owner_did.as_ptr()) };
        assert_eq!(result.status, 0, "re_enable_nac should succeed");

        // Verify NAC is enabled again (pass owner DID since NAC is active)
        let result = unsafe { get_nac_status(node, owner_did.as_ptr()) };
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
        let relation = CString::new("admin").unwrap();
        let result = unsafe {
            add_nac_actor_relationship(
                node,
                owner_did.as_ptr(),
                relation.as_ptr(),
                target_did.as_ptr(),
            )
        };
        assert_eq!(
            result.status, 0,
            "add_nac_actor_relationship should succeed"
        );
        let value = unsafe { CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("\"added\":true"), "should indicate added");
        unsafe { crate::types::defra_free_string(result.value) };

        // Delete admin relationship
        let result = unsafe {
            delete_nac_actor_relationship(
                node,
                owner_did.as_ptr(),
                relation.as_ptr(),
                target_did.as_ptr(),
            )
        };
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
