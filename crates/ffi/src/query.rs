//! Query execution for FFI.
//!
//! This module exposes GraphQL query execution that matches
//! Go's cbindings/query.go behavior.

use std::ffi::c_char;

use acp::nac::NodePermission;

use crate::get_runtime;
use crate::nac_check::check_nac_for_node;
use crate::state::{GraphQLSubscriptionState, GRAPHQL_SUBSCRIPTIONS, NODES};
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

/// Determine NAC permission based on query content.
/// Mutations require DocumentUpdate, queries require DocumentRead.
pub(crate) fn nac_permission_for_query(query_str: &str) -> NodePermission {
    let trimmed = query_str.trim_start();
    if trimmed.starts_with("mutation") {
        NodePermission::DocumentUpdate
    } else {
        NodePermission::DocumentRead
    }
}

/// Check if the identity has DAC bypass permission (NAC admin/owner).
///
/// Sets the thread-local `dac_bypass` flag to `true` if the identity has
/// the `DacBypass` node permission (meaning they can read all documents
/// regardless of DAC policies).
pub(crate) fn check_and_set_dac_bypass(
    rt: &tokio::runtime::Runtime,
    node_ptr: usize,
    identity_did: *const c_char,
) {
    use acp::nac::NacStatus;

    // Default: no bypass
    defra_core::dac_bypass::set_dac_bypass(false);

    let nac_manager = match NODES.get(node_ptr, |state| state.nac_manager.clone()) {
        Some(m) => m,
        None => return,
    };

    // Only bypass when NAC is enabled
    let status = rt.block_on(nac_manager.status());
    if status != NacStatus::Enabled {
        return;
    }

    let identity_str = unsafe { c_str_to_string(identity_did) };
    let did = match identity_str {
        Some(s) if !s.is_empty() => match identity::Did::new(&s) {
            Ok(d) => d,
            Err(_) => return,
        },
        _ => return,
    };

    let has_bypass = rt
        .block_on(nac_manager.check_permission(&did, NodePermission::DacBypass))
        .unwrap_or(false);

    defra_core::dac_bypass::set_dac_bypass(has_bypass);
}

/// Execute a GraphQL query or mutation.
///
/// Returns a JSON object with the query result in GraphQL format:
/// ```json
/// {
///     "data": { ... },
///     "errors": [ ... ]
/// }
/// ```
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `identity_did` - Optional DID string for ACP permission checks (null for anonymous)
/// * `request_query` - GraphQL query string (required)
/// * `operation_name` - Optional operation name for multi-operation documents (null if not used)
/// * `variables` - Optional JSON string of variables (null if not used)
///
/// # Safety
///
/// All string pointers must be either null or valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn exec_request(
    node_ptr: usize,
    identity_did: *const c_char,
    request_query: *const c_char,
    operation_name: *const c_char,
    variables: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let query_str = match c_str_to_string(request_query) {
        Some(s) => s,
        None => return FfiResult::error("request_query is null"),
    };

    let permission = nac_permission_for_query(&query_str);
    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, permission) {
        return e;
    }

    let identity_str = c_str_to_string(identity_did);
    let op_name = c_str_to_string(operation_name);
    let vars_str = c_str_to_string(variables);

    // Parse identity DID if provided
    let did = match identity_str {
        Some(ref s) if !s.is_empty() => match identity::Did::new(s) {
            Ok(d) => Some(d),
            Err(e) => return FfiResult::error(format!("invalid identity DID: {}", e)),
        },
        _ => None,
    };

    // Set up thread-local signer for block signing during mutations.
    // Matches Go's behavior: if no explicit identity, fall back to node identity.
    if let Some(ref s) = identity_str {
        if !s.is_empty() {
            if let Some(signing_config) = defra_core::signing::get_identity(s) {
                eprintln!(
                    "[SIGN-DEBUG] Using explicit identity signing config for DID: {}",
                    s
                );
                defra_core::signing::set_signing_config(Some(signing_config));
            } else {
                eprintln!(
                    "[SIGN-DEBUG] No signing config found for explicit DID: {}",
                    s
                );
                defra_core::signing::set_signing_config(None);
            }
        } else {
            // Empty string identity — fall back to node identity
            let node_did = NODES
                .get(node_ptr, |state| state.node_identity_did.clone())
                .flatten();
            eprintln!(
                "[SIGN-DEBUG] Empty identity, node_identity_did={:?}",
                node_did
            );
            let node_signing_config = NODES
                .get(node_ptr, |state| {
                    state
                        .node_identity_did
                        .as_ref()
                        .and_then(|did| defra_core::signing::get_identity(did))
                })
                .flatten();
            eprintln!(
                "[SIGN-DEBUG] Node signing config present: {}",
                node_signing_config.is_some()
            );
            defra_core::signing::set_signing_config(node_signing_config);
        }
    } else {
        // Null identity — fall back to node identity
        let node_did = NODES
            .get(node_ptr, |state| state.node_identity_did.clone())
            .flatten();
        eprintln!(
            "[SIGN-DEBUG] Null identity, node_identity_did={:?}",
            node_did
        );
        let node_signing_config = NODES
            .get(node_ptr, |state| {
                state
                    .node_identity_did
                    .as_ref()
                    .and_then(|did| defra_core::signing::get_identity(did))
            })
            .flatten();
        eprintln!(
            "[SIGN-DEBUG] Node signing config present: {}",
            node_signing_config.is_some()
        );
        defra_core::signing::set_signing_config(node_signing_config);
    }

    // Check if identity has DAC bypass (NAC admin/owner can read all documents)
    check_and_set_dac_bypass(rt, node_ptr, identity_did);

    // Check if this is a subscription query - handle it separately from regular queries/mutations.
    // Subscriptions return status=2 with a handle that the Go side polls via poll_graphql_subscription.
    let trimmed_query = query_str.trim_start();
    if trimmed_query.starts_with("subscription") {
        // Create an event bus subscription to receive update events
        let mut subscription = match NODES.get(node_ptr, |state| {
            state.event_bus.subscribe(&[events::EventName::Update])
        }) {
            Some(sub) => sub,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };
        let sub_id = subscription.id();

        let query_runner = match NODES.get(node_ptr, |state| state.query_runner.clone()) {
            Some(r) => r,
            None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
        };

        // Extract any pre-existing docID filter from the subscription query
        let subscription_doc_id = extract_doc_id_from_query(&query_str);
        let sub_query = query_str.clone();
        let sub_identity = did.clone();

        // Create result channel for buffering processed subscription results
        let (result_tx, result_rx) = tokio::sync::mpsc::channel::<String>(256);

        // Detect if this is a _commits subscription (uses CID-based queries)
        let is_commits = is_commits_subscription(&query_str);

        // Spawn background task that processes events and executes queries at event time.
        // This ensures the DB state at query execution matches the event's state.
        let task = rt.spawn(async move {
            while let Some(message) = subscription.recv().await {
                if let Some(update) = message.as_update() {
                    let event_doc_id = update.doc_id.clone();

                    // Check subscription docID filter
                    if let Some(ref sub_doc) = subscription_doc_id {
                        if event_doc_id != *sub_doc {
                            continue;
                        }
                    }

                    // Convert subscription to a query scoped to the changed document.
                    // For _commits subscriptions, use the event's CID to get just the
                    // composite commit. For regular subscriptions, use docID.
                    let query_text = if is_commits {
                        let cid_str = update.cid.to_string();
                        subscription_to_commits_query_with_cid(&sub_query, &cid_str)
                    } else {
                        subscription_to_query_with_doc_id(&sub_query, &event_doc_id)
                    };

                    // Execute the query
                    let mut request = query::QueryRequest::new(query_text);
                    if sub_identity.is_some() {
                        request = request.with_identity(sub_identity.clone());
                    }
                    let response = query_runner.execute(request).await;

                    // Skip empty results (filter excluded the document)
                    if !crate::subscription::response_has_data(&response) {
                        continue;
                    }

                    if let Ok(json) = serde_json::to_string(&response) {
                        if result_tx.send(json).await.is_err() {
                            break; // Receiver dropped
                        }
                    }
                }
            }
        });

        let state = GraphQLSubscriptionState {
            result_receiver: result_rx,
            node_handle: node_ptr,
            event_sub_id: sub_id,
            task_abort: task.abort_handle(),
        };

        let handle = GRAPHQL_SUBSCRIPTIONS.insert(state);
        return FfiResult::subscription(handle.to_string());
    }

    // Check if this is an encrypted query (encrypted_<Collection>) and validate P2P is enabled
    // Go only generates the encrypted_<Collection> GraphQL field when P2P is enabled,
    // so we need to return a schema validation error if P2P is disabled
    if query_str.contains("encrypted_") {
        let has_p2p = NODES
            .get(node_ptr, |state| state.p2p.is_some())
            .unwrap_or(false);
        if !has_p2p {
            // Extract the collection name from the query to generate Go-compatible error
            // e.g., "encrypted_User" -> "Cannot query field \"encrypted_User\" on type \"Query\"."
            if let Some(start) = query_str.find("encrypted_") {
                let rest = &query_str[start..];
                // Find the end of the field name (next non-alphanumeric/underscore)
                let end = rest
                    .find(|c: char| !c.is_alphanumeric() && c != '_')
                    .unwrap_or(rest.len());
                let field_name = &rest[..end];
                return FfiResult::error(format!(
                    "Cannot query field \"{}\" on type \"Query\".",
                    field_name
                ));
            }
            return FfiResult::error("Cannot query encrypted fields when P2P is disabled.");
        }
    }

    // Validate node handle before entering async block
    let runner = match NODES.get(node_ptr, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        // Build request
        let mut request = query::QueryRequest::new(query_str);
        if did.is_some() {
            request = request.with_identity(did);
        }
        if let Some(op) = op_name {
            request = request.with_operation_name(op);
        }
        if let Some(vars) = vars_str {
            let vars_json: serde_json::Value = serde_json::from_str(&vars)
                .map_err(|e| format!("failed to parse variables: {}", e))?;
            request = request.with_variables(vars_json);
        }

        // Execute
        let response = runner.execute(request).await;

        // Serialize response
        let json = serde_json::to_string(&response)
            .map_err(|e| format!("failed to serialize response: {}", e))?;

        Ok::<String, String>(json)
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Extract a docID value from a GraphQL query string.
///
/// Looks for patterns like `docID: "bae-xxx"` or `docID: "bae-xxx"` in the query.
/// Returns None if no docID is found.
pub(crate) fn extract_doc_id_from_query(query: &str) -> Option<String> {
    // Look for docID: "..." or docID:"..."
    let doc_id_marker = "docID:";
    let pos = query.find(doc_id_marker)?;
    let after = &query[pos + doc_id_marker.len()..];
    let after = after.trim_start();

    // Find the quoted value
    if after.starts_with('"') {
        let value_start = 1;
        let value_end = after[value_start..].find('"')?;
        Some(after[value_start..value_start + value_end].to_string())
    } else {
        None
    }
}

/// Convert a subscription query to a regular query scoped to a specific docID.
///
/// Transforms: `subscription { User(filter: ...) { fields } }`
/// Into: `{ User(docID: "bae-xxx", filter: ...) { fields } }`
pub(crate) fn subscription_to_query_with_doc_id(subscription_query: &str, doc_id: &str) -> String {
    // Step 1: Remove "subscription" keyword
    let trimmed = subscription_query.trim_start();
    let query = if let Some(after) = trimmed.strip_prefix("subscription") {
        let after = after.trim_start();
        if after.starts_with('{') {
            after.to_string()
        } else if let Some(brace_pos) = after.find('{') {
            after[brace_pos..].to_string()
        } else {
            after.to_string()
        }
    } else {
        trimmed.to_string()
    };

    // Step 2: Find the root field name and inject docID
    let brace_pos = match query.find('{') {
        Some(p) => p,
        None => return query,
    };

    let after_brace = &query[brace_pos + 1..];
    let ws_len = after_brace.len() - after_brace.trim_start().len();
    let field_start_in_q = brace_pos + 1 + ws_len;

    let rest = &query[field_start_in_q..];
    let field_end = rest
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(rest.len());
    let after_field = field_start_in_q + field_end;

    // Check what follows the field name
    let post_field = query[after_field..].trim_start();
    if post_field.starts_with('(') {
        // Already has arguments
        let paren_offset = query[after_field..].find('(').unwrap();
        let paren_idx = after_field + paren_offset;

        // Check if there's already a docID argument
        if extract_doc_id_from_query(&query).is_some() {
            // Keep the existing docID filter - just convert subscription to query
            query
        } else {
            // Inject docID at start of existing args
            format!(
                "{}docID: \"{}\", {}",
                &query[..paren_idx + 1],
                doc_id,
                &query[paren_idx + 1..]
            )
        }
    } else {
        // No arguments - inject (docID: "...")
        format!(
            "{}(docID: \"{}\"){}",
            &query[..after_field],
            doc_id,
            &query[after_field..]
        )
    }
}

/// Check if a subscription query targets the `_commits` root field.
pub(crate) fn is_commits_subscription(query: &str) -> bool {
    let trimmed = query.trim_start();
    let after_sub = trimmed.strip_prefix("subscription").unwrap_or(trimmed);
    let brace_pos = match after_sub.find('{') {
        Some(p) => p,
        None => return false,
    };
    let after_brace = after_sub[brace_pos + 1..].trim_start();
    after_brace.starts_with("_commits")
}

/// Convert a _commits subscription to a query scoped to a specific CID.
///
/// Transforms: `subscription { _commits(docID: "...") { fields } }`
/// Into: `{ _commits(cid: "bafyrei-xxx") { fields } }`
pub(crate) fn subscription_to_commits_query_with_cid(
    subscription_query: &str,
    cid: &str,
) -> String {
    // Step 1: Remove "subscription" keyword
    let trimmed = subscription_query.trim_start();
    let query = if let Some(after) = trimmed.strip_prefix("subscription") {
        let after = after.trim_start();
        if after.starts_with('{') {
            after.to_string()
        } else if let Some(brace_pos) = after.find('{') {
            after[brace_pos..].to_string()
        } else {
            after.to_string()
        }
    } else {
        trimmed.to_string()
    };

    // Step 2: Find the root field name (_commits)
    let brace_pos = match query.find('{') {
        Some(p) => p,
        None => return query,
    };

    let after_brace = &query[brace_pos + 1..];
    let ws_len = after_brace.len() - after_brace.trim_start().len();
    let field_start_in_q = brace_pos + 1 + ws_len;

    let rest = &query[field_start_in_q..];
    let field_end = rest
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(rest.len());
    let after_field = field_start_in_q + field_end;

    // Step 3: Replace or inject cid argument
    let post_field = query[after_field..].trim_start();
    if post_field.starts_with('(') {
        // Has existing arguments - find the closing paren and replace all args with cid
        let paren_start = query[after_field..].find('(').unwrap();
        let paren_start_abs = after_field + paren_start;
        let mut depth = 0;
        let mut close_paren_abs = paren_start_abs;
        for (i, c) in query[paren_start_abs..].char_indices() {
            if c == '(' {
                depth += 1;
            }
            if c == ')' {
                depth -= 1;
                if depth == 0 {
                    close_paren_abs = paren_start_abs + i;
                    break;
                }
            }
        }
        format!(
            "{}(cid: \"{}\"){}",
            &query[..paren_start_abs],
            cid,
            &query[close_paren_abs + 1..]
        )
    } else {
        // No arguments - inject (cid: "...")
        format!(
            "{}(cid: \"{}\"){}",
            &query[..after_field],
            cid,
            &query[after_field..]
        )
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

    #[test]
    fn test_exec_request() {
        // Initialize runtime
        assert!(crate::runtime::init_runtime());

        // Create node
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Query (should return empty array)
        let query_str = CString::new("{ User { name } }").unwrap();
        let result = unsafe {
            exec_request(
                node,
                ptr::null(),
                query_str.as_ptr(),
                ptr::null(),
                ptr::null(),
            )
        };
        assert_eq!(result.status, 0, "exec_request should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("data"), "response should have data field");

        // Cleanup
        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    #[test]
    fn test_exec_mutation() {
        // Initialize runtime
        assert!(crate::runtime::init_runtime());

        // Create node
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Create a user
        let mutation =
            CString::new(r#"mutation { create_User(input: {name: "Alice"}) { _docID name } }"#)
                .unwrap();
        let result = unsafe {
            exec_request(
                node,
                ptr::null(),
                mutation.as_ptr(),
                ptr::null(),
                ptr::null(),
            )
        };
        assert_eq!(result.status, 0, "mutation should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Alice"), "response should contain Alice");

        // Cleanup
        unsafe { crate::types::defra_free_string(result.value) };

        // Query to verify
        let query_str = CString::new("{ User { name } }").unwrap();
        let result = unsafe {
            exec_request(
                node,
                ptr::null(),
                query_str.as_ptr(),
                ptr::null(),
                ptr::null(),
            )
        };
        assert_eq!(result.status, 0, "query should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Alice"), "query result should contain Alice");

        // Cleanup
        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    // Edge case tests (H2)

    #[test]
    fn test_exec_request_null_query() {
        assert!(crate::runtime::init_runtime());

        // Create node
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Null query should return error
        let result =
            unsafe { exec_request(node, ptr::null(), ptr::null(), ptr::null(), ptr::null()) };
        assert_eq!(result.status, 1, "null query should fail");
        assert!(!result.error.is_null());

        let error = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
        assert!(error.contains("null"), "should indicate null query");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_exec_request_invalid_handle() {
        assert!(crate::runtime::init_runtime());

        // Query with invalid handle should return error
        let query_str = CString::new("{ User { name } }").unwrap();
        let result =
            unsafe { exec_request(0, ptr::null(), query_str.as_ptr(), ptr::null(), ptr::null()) };
        assert_eq!(result.status, 1, "invalid handle should fail");
        assert!(!result.error.is_null());

        let error = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
        assert!(error.contains("invalid"), "should indicate invalid handle");

        unsafe { crate::types::defra_free_string(result.error) };
    }

    #[test]
    fn test_exec_request_invalid_variables_json() {
        assert!(crate::runtime::init_runtime());

        // Create node
        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Query with invalid JSON variables should return error
        let query_str = CString::new("{ User { name } }").unwrap();
        let invalid_json = CString::new("not valid json").unwrap();
        let result = unsafe {
            exec_request(
                node,
                ptr::null(),
                query_str.as_ptr(),
                ptr::null(),
                invalid_json.as_ptr(),
            )
        };
        assert_eq!(result.status, 1, "invalid JSON should fail");
        assert!(!result.error.is_null());

        let error = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
        assert!(
            error.contains("parse") || error.contains("variables"),
            "should indicate parse error"
        );

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }
}
