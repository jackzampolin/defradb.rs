use std::ffi::c_char;

use acp::nac::NodePermission;
use serde_json::Value as JsonValue;

use crate::helpers::{get_node_runner, get_rt, require_c_str};
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::{ffi_async, ffi_entry, try_ffi};

/// Convert a JSON value to GraphQL input syntax.
///
/// GraphQL uses bare identifiers for object keys (not quoted strings like JSON).
/// This converts: {"name": "Alice", "age": 30} to {name: "Alice", age: 30}
pub(crate) fn json_to_graphql_input(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => {
            let escaped = s
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
                .replace('\t', "\\t");
            format!("\"{}\"", escaped)
        }
        JsonValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_to_graphql_input).collect();
            format!("[{}]", items.join(", "))
        }
        JsonValue::Object(obj) => {
            let fields: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("{}: {}", k, json_to_graphql_input(v)))
                .collect();
            format!("{{{}}}", fields.join(", "))
        }
    }
}

/// Build a GraphQL create mutation for a single document.
fn build_create_mutation(collection: &str, data: &JsonValue) -> String {
    let input = json_to_graphql_input(data);
    format!(
        "mutation {{ add_{}(input: {}) {{ _docID }} }}",
        collection, input
    )
}

/// Build a GraphQL create mutation for multiple documents.
fn build_create_many_mutation(collection: &str, docs: &[JsonValue]) -> String {
    let inputs: Vec<String> = docs.iter().map(json_to_graphql_input).collect();
    format!(
        "mutation {{ add_{}(input: [{}]) {{ _docID }} }}",
        collection,
        inputs.join(", ")
    )
}

/// Create document(s) in a collection.
///
/// This function automatically detects whether the input is a single document
/// (JSON object) or multiple documents (JSON array) and handles both cases.
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn collection_create(
    node_ptr: usize,
    identity_did: *const c_char,
    collection_name: *const c_char,
    json_data: *const c_char,
    batch_session_id: *const c_char,
) -> FfiResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        try_ffi!(check_nac_for_node(
            rt,
            node_ptr,
            identity_did,
            NodePermission::DocumentUpdate
        ));
        let collection = try_ffi!(require_c_str(collection_name, "collection_name"));
        let json_str = try_ffi!(require_c_str(json_data, "json_data"));

        // Set up thread-local signing config and batch session key (same as exec_request)
        let identity_str = c_str_to_string(identity_did);
        let batch_session = c_str_to_string(batch_session_id);
        let (node_did, signing_enabled) = NODES
            .get(node_ptr, |state| (state.node_identity_did.clone(), state.signing_enabled))
            .unwrap_or((None, false));
        let signing = defra_core::signing::resolve_signing_config_with_flag(
            identity_str.as_deref(),
            node_did.as_deref(),
            signing_enabled,
        );
        let session_key = batch_session.or_else(|| signing.as_ref().map(|s| s.public_key_hex.clone()));
        defra_core::batch_signing::set_batch_session_key(session_key);
        defra_core::signing::set_signing_config(signing);

        // Parse JSON to detect array vs object
        let parsed: JsonValue = match serde_json::from_str(&json_str) {
            Ok(v) => v,
            Err(e) => return FfiResult::error(format!("invalid JSON: {}", e)),
        };

        // Build the appropriate mutation based on whether input is array or object
        let mutation = if parsed.is_array() {
            let docs = match parsed.as_array() {
                Some(arr) => arr.clone(),
                None => return FfiResult::error("expected JSON array"),
            };
            if docs.is_empty() {
                return FfiResult::error("cannot create empty array of documents");
            }
            build_create_many_mutation(&collection, &docs)
        } else if parsed.is_object() {
            build_create_mutation(&collection, &parsed)
        } else {
            return FfiResult::error("json_data must be an object or array of objects");
        };

        let runner = try_ffi!(get_node_runner(node_ptr));

        ffi_async!(rt, {
            let request = query::QueryRequest::new(mutation);
            let response = runner.execute(request).await;

            if !response.errors.is_empty() {
                let error_msg = response
                    .errors
                    .iter()
                    .map(|e| e.message.clone())
                    .collect::<Vec<_>>()
                    .join("; ");
                return Err(format!("mutation failed: {}", error_msg));
            }

            serde_json::to_string(&response.data)
                .map_err(|e| format!("failed to serialize response: {}", e))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_to_graphql_input_string() {
        let value = JsonValue::String("hello".to_string());
        assert_eq!(json_to_graphql_input(&value), r#""hello""#);
    }

    #[test]
    fn test_json_to_graphql_input_string_with_escapes() {
        let value = JsonValue::String("hello\nworld".to_string());
        assert_eq!(json_to_graphql_input(&value), r#""hello\nworld""#);
    }

    #[test]
    fn test_json_to_graphql_input_number() {
        let value = serde_json::json!(42);
        assert_eq!(json_to_graphql_input(&value), "42");
    }

    #[test]
    fn test_json_to_graphql_input_bool() {
        assert_eq!(json_to_graphql_input(&JsonValue::Bool(true)), "true");
        assert_eq!(json_to_graphql_input(&JsonValue::Bool(false)), "false");
    }

    #[test]
    fn test_json_to_graphql_input_null() {
        assert_eq!(json_to_graphql_input(&JsonValue::Null), "null");
    }

    #[test]
    fn test_json_to_graphql_input_array() {
        let value = serde_json::json!([1, 2, 3]);
        assert_eq!(json_to_graphql_input(&value), "[1, 2, 3]");
    }

    #[test]
    fn test_json_to_graphql_input_object() {
        let value = serde_json::json!({"name": "Alice", "age": 30});
        let result = json_to_graphql_input(&value);
        // Order may vary, so check both possibilities
        assert!(
            result == r#"{age: 30, name: "Alice"}"# || result == r#"{name: "Alice", age: 30}"#,
            "got: {}",
            result
        );
    }

    #[test]
    fn test_build_create_mutation() {
        let data = serde_json::json!({"name": "Bob"});
        let mutation = build_create_mutation("User", &data);
        assert!(mutation.contains("add_User"));
        assert!(mutation.contains("input:"));
        assert!(mutation.contains("_docID"));
    }

    #[test]
    fn test_build_create_many_mutation() {
        let docs = vec![
            serde_json::json!({"name": "Alice"}),
            serde_json::json!({"name": "Bob"}),
        ];
        let mutation = build_create_many_mutation("User", &docs);
        assert!(mutation.contains("add_User"));
        assert!(mutation.contains("input: ["));
        assert!(mutation.contains("_docID"));
    }
}
