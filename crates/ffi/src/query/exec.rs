use std::ffi::c_char;

use crate::get_runtime;
use crate::nac_check::check_nac_for_node;
use crate::state::{GraphQLSubscriptionState, GRAPHQL_SUBSCRIPTIONS, NODES};
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

use super::{
    check_and_set_dac_bypass, extract_doc_id_from_query, is_commits_subscription,
    nac_permission_for_query, subscription_to_commits_query_with_cid,
    subscription_to_query_with_doc_id,
};

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
