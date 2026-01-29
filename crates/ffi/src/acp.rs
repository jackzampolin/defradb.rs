//! Access Control Policy (ACP) operations for FFI.
//!
//! This module exposes ACP management functions for both:
//! - NAC (Node Access Control) - node-level permissions
//! - DAC (Document Access Control) - document-level permissions
//!
//! All functions use identity_ptr handles instead of DID strings.

use std::ffi::c_char;

use identity::Identity;

use crate::get_runtime;
use crate::state::{IDENTITIES, NODES};
use crate::types::{c_str_to_string, resolve_identity_did, FfiResult, NewIdentityResult};
use crate::ERR_INVALID_NODE_HANDLE;

// ============================================================================
// NAC (Node Access Control) Functions
// ============================================================================

/// Get the current NAC status.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `identity_ptr` - Identity handle (0 for no identity)
#[export_name = "ACPGetNACStatus"]
pub extern "C" fn acp_get_nac_status(node_ptr: usize, _identity_ptr: usize) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let nac_manager = match NODES.get(node_ptr, |state| state.nac_manager.clone()) {
        Some(m) => m,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
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
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `identity_ptr` - Identity handle for the admin performing the action
#[export_name = "ACPDisableNAC"]
pub extern "C" fn acp_disable_nac(node_ptr: usize, identity_ptr: usize) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let requestor_did = match resolve_identity_did(identity_ptr) {
        Ok(did) => did,
        Err(e) => return FfiResult::error(e),
    };

    let nac_manager = match NODES.get(node_ptr, |state| state.nac_manager.clone()) {
        Some(m) => m,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let requestor = identity::Did::new(&requestor_did)
            .map_err(|e| format!("invalid DID '{}': {}", requestor_did, e))?;

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
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `identity_ptr` - Identity handle for the admin performing the action
#[export_name = "ACPReEnableNAC"]
pub extern "C" fn acp_re_enable_nac(node_ptr: usize, identity_ptr: usize) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let requestor_did = match resolve_identity_did(identity_ptr) {
        Ok(did) => did,
        Err(e) => return FfiResult::error(e),
    };

    let nac_manager = match NODES.get(node_ptr, |state| state.nac_manager.clone()) {
        Some(m) => m,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let requestor = identity::Did::new(&requestor_did)
            .map_err(|e| format!("invalid DID '{}': {}", requestor_did, e))?;

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

/// Add a NAC actor relationship.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `identity_ptr` - Identity handle for the admin
/// * `relation` - Relation type (e.g., "admin")
/// * `actor` - Target actor DID string
///
/// # Safety
///
/// String pointers must be valid null-terminated UTF-8 strings.
#[export_name = "ACPAddNACActorRelationship"]
pub unsafe extern "C" fn acp_add_nac_actor_relationship(
    node_ptr: usize,
    identity_ptr: usize,
    _relation: *const c_char,
    actor: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let requestor_did = match resolve_identity_did(identity_ptr) {
        Ok(did) => did,
        Err(e) => return FfiResult::error(e),
    };

    let target_str = match c_str_to_string(actor) {
        Some(s) => s,
        None => return FfiResult::error("actor is null"),
    };

    let nac_manager = match NODES.get(node_ptr, |state| state.nac_manager.clone()) {
        Some(m) => m,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let requestor = identity::Did::new(&requestor_did)
            .map_err(|e| format!("invalid requestor DID '{}': {}", requestor_did, e))?;
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

/// Delete a NAC actor relationship.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `identity_ptr` - Identity handle for the admin
/// * `relation` - Relation type (e.g., "admin")
/// * `actor` - Target actor DID string
///
/// # Safety
///
/// String pointers must be valid null-terminated UTF-8 strings.
#[export_name = "ACPDeleteNACActorRelationship"]
pub unsafe extern "C" fn acp_delete_nac_actor_relationship(
    node_ptr: usize,
    identity_ptr: usize,
    _relation: *const c_char,
    actor: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let requestor_did = match resolve_identity_did(identity_ptr) {
        Ok(did) => did,
        Err(e) => return FfiResult::error(e),
    };

    let target_str = match c_str_to_string(actor) {
        Some(s) => s,
        None => return FfiResult::error("actor is null"),
    };

    let nac_manager = match NODES.get(node_ptr, |state| state.nac_manager.clone()) {
        Some(m) => m,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let requestor = identity::Did::new(&requestor_did)
            .map_err(|e| format!("invalid requestor DID '{}': {}", requestor_did, e))?;
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

/// Add a DAC policy.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `identity_ptr` - Identity handle for the requestor
/// * `policy` - Policy definition string (YAML or JSON)
///
/// # Safety
///
/// `policy` must be a valid null-terminated UTF-8 string.
#[export_name = "ACPAddDACPolicy"]
pub unsafe extern "C" fn acp_add_dac_policy(
    node_ptr: usize,
    _identity_ptr: usize,
    policy: *const c_char,
) -> FfiResult {
    let policy_str = match c_str_to_string(policy) {
        Some(s) => s,
        None => return FfiResult::error("policy is null"),
    };

    if policy_str.trim().is_empty() {
        return FfiResult::error("policy cannot be empty");
    }

    let result = NODES
        .get(node_ptr, |state| {
            let policy_id = state.policy_store.add_policy(&policy_str);
            serde_json::json!({
                "PolicyID": policy_id
            })
            .to_string()
        })
        .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string());

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Add a DAC actor relationship.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `identity_ptr` - Identity handle for the requestor (document owner)
/// * `collection_id` - Collection ID string
/// * `doc_id` - Document ID string
/// * `relation` - Relation type (e.g., "reader", "updater", "deleter")
/// * `actor` - Target actor DID string
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[export_name = "ACPAddDACActorRelationship"]
pub unsafe extern "C" fn acp_add_dac_actor_relationship(
    node_ptr: usize,
    identity_ptr: usize,
    collection_id: *const c_char,
    doc_id: *const c_char,
    relation: *const c_char,
    actor: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let requestor_did = match resolve_identity_did(identity_ptr) {
        Ok(did) => did,
        Err(e) => return FfiResult::error(e),
    };

    let target_str = match c_str_to_string(actor) {
        Some(s) => s,
        None => return FfiResult::error("actor is null"),
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

    let document_acp = match NODES.get(node_ptr, |state| state.document_acp.clone()) {
        Some(acp) => acp,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let requestor = identity::Did::new(&requestor_did)
            .map_err(|e| format!("invalid requestor DID '{}': {}", requestor_did, e))?;
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

/// Delete a DAC actor relationship.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `identity_ptr` - Identity handle for the requestor (document owner)
/// * `collection_id` - Collection ID string
/// * `doc_id` - Document ID string
/// * `relation` - Relation type
/// * `actor` - Target actor DID string
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[export_name = "ACPDeleteDACActorRelationship"]
pub unsafe extern "C" fn acp_delete_dac_actor_relationship(
    node_ptr: usize,
    identity_ptr: usize,
    collection_id: *const c_char,
    doc_id: *const c_char,
    relation: *const c_char,
    actor: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let requestor_did = match resolve_identity_did(identity_ptr) {
        Ok(did) => did,
        Err(e) => return FfiResult::error(e),
    };

    let target_str = match c_str_to_string(actor) {
        Some(s) => s,
        None => return FfiResult::error("actor is null"),
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

    let document_acp = match NODES.get(node_ptr, |state| state.document_acp.clone()) {
        Some(acp) => acp,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let requestor = identity::Did::new(&requestor_did)
            .map_err(|e| format!("invalid requestor DID '{}': {}", requestor_did, e))?;
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
#[export_name = "GetNodeIdentity"]
pub extern "C" fn get_node_identity(node_ptr: usize) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
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

/// Create a new identity and return an opaque handle.
///
/// # Safety
///
/// * `key_type` must be null or a valid null-terminated UTF-8 string.
/// * The returned handle must be freed with `identity_free`.
#[export_name = "IdentityNew"]
pub unsafe extern "C" fn identity_new(_key_type: *const c_char) -> NewIdentityResult {
    let result = (|| {
        let private_key = crypto::generate_ed25519()
            .map_err(|e| format!("failed to generate Ed25519 key: {}", e))?;

        let raw_identity = identity::RawIdentity::from_ed25519(private_key)
            .map_err(|e| format!("failed to create identity: {}", e))?;

        let handle = IDENTITIES.insert(raw_identity);
        Ok::<usize, String>(handle)
    })();

    match result {
        Ok(handle) => NewIdentityResult::success(handle),
        Err(e) => NewIdentityResult::error(e),
    }
}

/// Free an identity handle.
#[export_name = "IdentityFree"]
pub extern "C" fn identity_free(identity_ptr: usize) {
    if identity_ptr != 0 {
        IDENTITIES.remove(identity_ptr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::types::NodeInitOptions;
    use std::ffi::CStr;

    #[test]
    fn test_acp_get_nac_status() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let result = acp_get_nac_status(node, 0);
        assert_eq!(result.status, 0, "acp_get_nac_status should succeed");
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
    fn test_get_node_identity_not_configured() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

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
    fn test_identity_new_and_free() {
        let result = unsafe { identity_new(std::ptr::null()) };
        assert_eq!(result.status, 0, "identity_new should succeed");
        assert_ne!(result.identity_ptr, 0, "should return non-zero handle");

        let identity = crate::state::IDENTITIES.get(result.identity_ptr);
        assert!(identity.is_some(), "identity should be in registry");

        identity_free(result.identity_ptr);

        let identity = crate::state::IDENTITIES.get(result.identity_ptr);
        assert!(identity.is_none(), "identity should be removed after free");

        identity_free(0);
    }
}
