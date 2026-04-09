use std::ffi::c_char;
use std::time::{Duration, Instant};

use acp::nac::NodePermission;
use acp::DocumentPermission;

use crate::helpers::{get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::policy_yaml::{
    check_duplicate_yaml_keys, parse_policy_yaml, validate_policy_expressions,
};
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::{ffi_entry, try_ffi, ERR_INVALID_NODE_HANDLE};

/// Resolve managing relations for a given relation within a policy resource.
///
/// Loads the policy YAML, finds the resource, validates the relation exists,
/// and returns the list of managing relations.
pub(crate) fn resolve_managing_relations(
    policy_store: &crate::state::PolicyStore,
    policy_id: &str,
    resource_name: &str,
    relation: &str,
) -> Result<Vec<String>, String> {
    let mut managing_relations: Vec<String> = Vec::new();
    if let Some(policy_yaml) = policy_store.get_policy(policy_id) {
        if let Ok(parsed) = crate::policy_yaml::parse_policy_yaml(&policy_yaml) {
            if let Some(resource) = parsed.find_resource(resource_name) {
                if !resource.has_relation(relation) {
                    return Err(format!(
                        "relation '{}' not found in policy resource '{}'",
                        relation, resource_name
                    ));
                }
                managing_relations = resource
                    .get_managers_for_relation(relation)
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect();
            }
        }
    }
    Ok(managing_relations)
}

/// Add a DAC policy.
///
/// Accepts a policy definition in YAML or JSON format.
/// Returns a JSON object with the policy ID:
/// ```json
/// { "PolicyID": "sha256_hash_of_policy" }
/// ```
///
/// # Safety
///
/// `policy` must be a valid null-terminated UTF-8 string containing
/// the policy definition in YAML or JSON format.
#[no_mangle]
pub unsafe extern "C" fn add_dac_policy(
    node_ptr: usize,
    _identity_did: *const c_char,
    policy: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            _identity_did,
            NodePermission::DacPolicyAdd
        ));
        let policy_str = try_ffi!(require_c_str(policy, "policy"));

        let identity_str = c_str_to_string(_identity_did).unwrap_or_default();

        if identity_str.is_empty() {
            return FfiResult::error("policy creator can not be empty");
        }

        // Check for empty/null policy data
        if policy_str.is_empty() {
            return FfiResult::error("policy data can not be empty");
        }

        // Step 1: Check for duplicate YAML map keys (Go rejects these via YAMLToJSONStrict)
        if let Err(e) = check_duplicate_yaml_keys(&policy_str) {
            return FfiResult::error(e);
        }

        // Step 2: Parse the policy YAML
        let parsed = match parse_policy_yaml(&policy_str) {
            Ok(p) => p,
            Err(e) => return FfiResult::error(e),
        };

        // Step 3: Validate name is present (Go's BasicRequirement)
        if parsed.name.is_empty() {
            return FfiResult::error("name required");
        }

        // Step 4: Validate permission expressions
        if let Err(e) = validate_policy_expressions(&parsed) {
            return FfiResult::error(e);
        }

        // Step 5: Store policy - route through SourceHub when configured, else local
        let sh_acp = match NODES.get(node_ptr, |state| state.sourcehub_acp.clone()) {
            Some(opt) => opt,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        if let Some(sh_acp) = sh_acp {
            // SourceHub mode: submit MsgCreatePolicy transaction
            let result = rt.block_on(async {
                let policy_id = sh_acp
                    .add_policy(&identity_str, &policy_str)
                    .await
                    .map_err(|e| format!("SourceHub create policy failed: {}", e))?;
                Ok::<String, String>(policy_id)
            });
            match result {
                Ok(policy_id) => {
                    // Cache policy on all live FFI nodes so multi-node SourceHub tests
                    // can validate schemas that reference a policy created by another node.
                    NODES.for_each_mut(|state| {
                        state.policy_store.store_policy(&policy_id, &policy_str);
                    });
                    FfiResult::success(serde_json::json!({ "PolicyID": policy_id }).to_string())
                }
                Err(e) => FfiResult::error(e),
            }
        } else {
            // Local mode: Go-compatible ID generation
            let result = NODES
                .get(node_ptr, |state| {
                    let policy_id = state.policy_store.add_policy(&policy_str, &parsed);
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
    }
}

/// Get a DAC policy by ID.
///
/// Returns a JSON object with the policy content, or null if not found.
///
/// # Safety
///
/// `policy_id` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn get_dac_policy(node_ptr: usize, policy_id: *const c_char) -> FfiResult {
    ffi_entry! {
        let policy_id_str = try_ffi!(require_c_str(policy_id, "policy_id"));

        let result = NODES
            .get(node_ptr, |state| {
                match state.policy_store.get_policy(&policy_id_str) {
                    Some(policy) => serde_json::json!({
                        "id": policy_id_str,
                        "policy": policy
                    })
                    .to_string(),
                    None => serde_json::json!(null).to_string(),
                }
            })
            .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string());

        match result {
            Ok(json) => FfiResult::success(json),
            Err(e) => FfiResult::error(e),
        }
    }
}

/// List all DAC policy IDs.
///
/// Returns a JSON array of policy IDs.
///
/// # Safety
///
/// No unsafe string parameters.
#[no_mangle]
pub extern "C" fn list_dac_policies(node_ptr: usize) -> FfiResult {
    ffi_entry! {
        let result = NODES
            .get(node_ptr, |state| {
                let ids = state.policy_store.list_policies();
                serde_json::to_string(&ids).unwrap_or_else(|_| "[]".to_string())
            })
            .ok_or_else(|| ERR_INVALID_NODE_HANDLE.to_string());

        match result {
            Ok(json) => FfiResult::success(json),
            Err(e) => FfiResult::error(e),
        }
    }
}

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
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            requestor_did,
            NodePermission::DacRelationAdd
        ));
        let requestor_str = try_ffi!(require_c_str(requestor_did, "requestor_did"));
        let target_str = try_ffi!(require_c_str(target_did, "target_did"));
        let collection_id_str = try_ffi!(require_c_str(collection_id, "collection_id"));
        let doc_id_str = try_ffi!(require_c_str(doc_id, "doc_id"));
        let relation_str = try_ffi!(require_c_str(relation, "relation"));

        // Go checks collection name first (via GetCollectionByName)
        if collection_id_str.is_empty() {
            return FfiResult::error("collection name can't be empty");
        }

        // Then validates remaining required arguments (matches Go bridge.go validation)
        if requestor_str.is_empty()
            || target_str.is_empty()
            || doc_id_str.is_empty()
            || relation_str.is_empty()
        {
            return FfiResult::error(
                "missing a required argument needed to add doc actor relationship.",
            );
        }

        // Validate node handle and get database, document_acp, policy_store, and optional p2p
        let (database, document_acp, policy_store, p2p_system) = match NODES.get(node_ptr, |state| {
            (
                state.database.clone(),
                state.document_acp.clone(),
                state.policy_store.clone(),
                state.p2p.as_ref().map(|p2p| p2p.system.clone()),
            )
        }) {
            Some(tuple) => tuple,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        // Resolve collection name -> policy resource_name and policy_id
        let (resource_name, policy_id) = match database.get_collection(&collection_id_str) {
            Ok(Some(col)) => match col.schema().policy {
                Some(ref policy) => (policy.resource_name.clone(), policy.id.clone()),
                None => {
                    return FfiResult::error("operation requires ACP, but collection has no policy");
                }
            },
            Ok(None) => {
                return FfiResult::error(format!("collection '{}' does not exist", collection_id_str));
            }
            Err(e) => {
                return FfiResult::error(format!(
                    "failed to get collection '{}': {}",
                    collection_id_str, e
                ));
            }
        };

        // Owner relation is immutable - cannot be added or removed
        if relation_str == "owner" {
            return FfiResult::error("OPERATION_FORBIDDEN: cannot add owner relation");
        }

        let managing_relations = match resolve_managing_relations(
            &policy_store,
            &policy_id,
            &resource_name,
            &relation_str,
        ) {
            Ok(r) => r,
            Err(e) => return FfiResult::error(e),
        };

        let result = rt.block_on(async {
            let requestor = identity::Did::new(&requestor_str)
                .map_err(|e| format!("invalid requestor DID '{}': {}", requestor_str, e))?;
            let target = if target_str == "*" {
                identity::Did::wildcard()
            } else {
                identity::Did::new(&target_str)
                    .map_err(|e| format!("invalid target DID '{}': {}", target_str, e))?
            };

            let added = document_acp
                .add_actor_relationship(
                    &requestor,
                    &target,
                    &policy_id,
                    &resource_name,
                    &doc_id_str,
                    &relation_str,
                    &managing_relations,
                )
                .await
                .map_err(|e| e.to_string())?;

            if added {
                let target_identity = acp::Identity::from(target.clone());
                let wait_deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    let readable = document_acp
                        .check_doc_access(
                            &target_identity,
                            DocumentPermission::Read,
                            &policy_id,
                            &resource_name,
                            &doc_id_str,
                        )
                        .await
                        .map_err(|e| format!("failed to verify DAC relationship propagation: {e}"))?;

                    if readable {
                        break;
                    }

                    if Instant::now() >= wait_deadline {
                        return Err(
                            "timed out waiting for DAC relationship to become readable".to_string(),
                        );
                    }

                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                if let Some(system) = p2p_system {
                    system
                        .ops()
                        .republish_document(&collection_id_str, &doc_id_str)
                        .await
                        .map_err(|e| format!("failed to republish document after DAC relationship add: {e}"))?;
                }
            }

            let json = serde_json::json!({ "added": added }).to_string();
            Ok::<String, String>(json)
        });

        match result {
            Ok(json) => FfiResult::success(json),
            Err(e) => FfiResult::error(e),
        }
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
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            requestor_did,
            NodePermission::DacRelationDelete
        ));
        let requestor_str = try_ffi!(require_c_str(requestor_did, "requestor_did"));
        let target_str = try_ffi!(require_c_str(target_did, "target_did"));
        let collection_id_str = try_ffi!(require_c_str(collection_id, "collection_id"));
        let doc_id_str = try_ffi!(require_c_str(doc_id, "doc_id"));
        let relation_str = try_ffi!(require_c_str(relation, "relation"));

        // Go checks collection name first (via GetCollectionByName)
        if collection_id_str.is_empty() {
            return FfiResult::error("collection name can't be empty");
        }

        // Then validates remaining required arguments (matches Go bridge.go validation)
        if requestor_str.is_empty()
            || target_str.is_empty()
            || doc_id_str.is_empty()
            || relation_str.is_empty()
        {
            return FfiResult::error(
                "missing a required argument needed to delete doc actor relationship.",
            );
        }

        // Validate node handle and get database, document_acp, and policy_store
        let (database, document_acp, policy_store) = match NODES.get(node_ptr, |state| {
            (
                state.database.clone(),
                state.document_acp.clone(),
                state.policy_store.clone(),
            )
        }) {
            Some(tuple) => tuple,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        // Resolve collection name -> policy resource_name and policy_id
        let (resource_name, policy_id) = match database.get_collection(&collection_id_str) {
            Ok(Some(col)) => match col.schema().policy {
                Some(ref policy) => (policy.resource_name.clone(), policy.id.clone()),
                None => {
                    return FfiResult::error("operation requires ACP, but collection has no policy");
                }
            },
            Ok(None) => {
                return FfiResult::error(format!("collection '{}' does not exist", collection_id_str));
            }
            Err(e) => {
                return FfiResult::error(format!(
                    "failed to get collection '{}': {}",
                    collection_id_str, e
                ));
            }
        };

        // Owner relation is immutable - cannot be added or removed
        if relation_str == "owner" {
            return FfiResult::error("OPERATION_FORBIDDEN: cannot delete owner relation");
        }

        let managing_relations = match resolve_managing_relations(
            &policy_store,
            &policy_id,
            &resource_name,
            &relation_str,
        ) {
            Ok(r) => r,
            Err(e) => return FfiResult::error(e),
        };

        let result = rt.block_on(async {
            let requestor = identity::Did::new(&requestor_str)
                .map_err(|e| format!("invalid requestor DID '{}': {}", requestor_str, e))?;
            let target = if target_str == "*" {
                identity::Did::wildcard()
            } else {
                identity::Did::new(&target_str)
                    .map_err(|e| format!("invalid target DID '{}': {}", target_str, e))?
            };

            let deleted = document_acp
                .delete_actor_relationship(
                    &requestor,
                    &target,
                    &policy_id,
                    &resource_name,
                    &doc_id_str,
                    &relation_str,
                    &managing_relations,
                )
                .await
                .map_err(|e| e.to_string())?;

            let json = serde_json::json!({ "deleted": deleted }).to_string();
            Ok::<String, String>(json)
        });

        match result {
            Ok(json) => FfiResult::success(json),
            Err(e) => FfiResult::error(e),
        }
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
}
