use std::ffi::c_char;

use crate::ffi_entry;
use crate::helpers::{get_node_runner, get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::{GraphQLSubscriptionState, GRAPHQL_SUBSCRIPTIONS, NODES};
use crate::types::{c_str_to_string, FfiResult};
use crate::{ffi_async, try_ffi, ERR_INVALID_NODE_HANDLE};

use super::{
    check_and_set_dac_bypass, nac_permission_for_query, subscription_accepts_doc_id,
    subscription_doc_ids, subscription_to_scoped_query,
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
/// * `batch_session_id` - Optional batch session ID for CID collection (null if not in batch mode).
///   When provided, CIDs created during this request are collected under this session.
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
    batch_session_id: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        let query_str = try_ffi!(require_c_str(request_query, "request_query"));

        let permission = nac_permission_for_query(&query_str);
        try_ffi!(check_nac_for_node(rt, node_ptr, identity_did, permission));

        let identity_str = c_str_to_string(identity_did);
        let op_name = c_str_to_string(operation_name).filter(|name| !name.is_empty());
        let vars_str = c_str_to_string(variables);
        let batch_session = c_str_to_string(batch_session_id);

        // Parse identity DID if provided
        let did = match identity_str {
            Some(ref s) if !s.is_empty() => match identity::Did::new(s) {
                Ok(d) => Some(d),
                Err(e) => return FfiResult::error(format!("invalid identity DID: {}", e)),
            },
            _ => None,
        };

        // Set up thread-local signer for block signing during mutations.
        // Matches Go's behavior: if signing is enabled and no explicit identity,
        // fall back to node identity for block signing.
        let (node_did, signing_enabled) = NODES
            .get(node_ptr, |state| (state.node_identity_did.clone(), state.signing_enabled))
            .unwrap_or((None, false));
        let signing = defra_core::signing::resolve_signing_config_with_flag(
            identity_str.as_deref(),
            node_did.as_deref(),
            signing_enabled,
        );
        // Use caller-provided session ID if available; otherwise fall back to public key.
        let session_key = batch_session.or_else(|| signing.as_ref().map(|s| s.public_key_hex.clone()));
        defra_core::batch_signing::set_batch_session_key(session_key);
        defra_core::signing::set_signing_config(signing);

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

            let subscription_variables = match vars_str.as_ref() {
                Some(vars) => match serde_json::from_str::<serde_json::Value>(vars) {
                    Ok(value) => Some(value),
                    Err(e) => return FfiResult::error(format!("failed to parse variables: {}", e)),
                },
                None => None,
            };
            let subscription_doc_ids_filter = subscription_doc_ids(
                &query_str,
                subscription_variables.as_ref(),
                op_name.as_deref(),
            );
            let sub_query = query_str.clone();
            let sub_identity = did.clone();
            let sub_operation_name = op_name.clone();
            let sub_variables = subscription_variables.clone();

            // Create result channel for buffering processed subscription results
            let (result_tx, result_rx) = tokio::sync::mpsc::channel::<String>(256);

            // Capture signing config and DAC bypass before spawning.
            // The thread-locals were set on this thread (lines 63-116) but the
            // spawned task runs on a different tokio thread.
            let sub_signing_config = defra_core::signing::get_signing_config();
            let sub_dac_bypass = defra_core::dac_bypass::get_dac_bypass();

            // Spawn background task that processes events and executes queries at event time.
            // This ensures the DB state at query execution matches the event's state.
            let task = rt.spawn(async move {
                while let Some(message) = subscription.recv().await {
                    if let Some(update) = message.as_update() {
                        let event_doc_id = update.doc_id.clone();

                        if !subscription_accepts_doc_id(
                            subscription_doc_ids_filter.as_deref(),
                            &event_doc_id,
                        ) {
                            continue;
                        }

                        let cid_str = update.cid.to_string();
                        let query_text = match subscription_to_scoped_query(
                            &sub_query,
                            &event_doc_id,
                            &cid_str,
                            sub_operation_name.as_deref(),
                        ) {
                            Ok(query) => query,
                            Err(error) => {
                                tracing::warn!(error = %error, "failed to scope subscription query");
                                continue;
                            }
                        };

                        let mut request = query::QueryRequest::new(query_text);
                        if sub_identity.is_some() {
                            request = request.with_identity(sub_identity.clone());
                        }
                        if let Some(ref op_name) = sub_operation_name {
                            request = request.with_operation_name(op_name.clone());
                        }
                        if let Some(ref vars) = sub_variables {
                            request = request.with_variables(vars.clone());
                        }

                        // Execute inside spawn_blocking to pin thread-locals, matching
                        // HTTP's execute_with_resolved_context pattern.
                        let runner = query_runner.clone();
                        let config = sub_signing_config.clone();
                        let bypass = sub_dac_bypass;
                        let handle = tokio::runtime::Handle::current();
                        let response = match tokio::task::spawn_blocking(move || {
                            defra_core::signing::set_signing_config(config);
                            defra_core::dac_bypass::set_dac_bypass(bypass);
                            handle.block_on(async { runner.execute(request).await })
                        })
                        .await
                        {
                            Ok(r) => r,
                            Err(_) => continue,
                        };

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

        let runner = try_ffi!(get_node_runner(node_ptr));

        ffi_async!(rt, {
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

            Ok(json)
        })
    }
}
