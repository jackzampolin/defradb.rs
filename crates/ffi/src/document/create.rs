use std::ffi::c_char;

use acp::nac::NodePermission;
use serde_json::Value as JsonValue;

use crate::get_runtime;
use crate::nac_check::check_nac_for_node;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

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
        "mutation {{ create_{}(input: {}) {{ _docID }} }}",
        collection, input
    )
}

/// Build a GraphQL create mutation for multiple documents.
fn build_create_many_mutation(collection: &str, docs: &[JsonValue]) -> String {
    let inputs: Vec<String> = docs.iter().map(json_to_graphql_input).collect();
    format!(
        "mutation {{ create_{}(input: [{}]) {{ _docID }} }}",
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
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    if let Err(e) = check_nac_for_node(rt, node_ptr, identity_did, NodePermission::DocumentUpdate) {
        return e;
    }

    let collection = match c_str_to_string(collection_name) {
        Some(s) => s,
        None => return FfiResult::error("collection_name is null"),
    };

    let json_str = match c_str_to_string(json_data) {
        Some(s) => s,
        None => return FfiResult::error("json_data is null"),
    };

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

    // Get the query runner from the node
    let runner = match NODES.get(node_ptr, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    // Execute the mutation
    let result = rt.block_on(async {
        let request = query::QueryRequest::new(mutation);
        let response = runner.execute(request).await;

        // Check for errors in the response
        if !response.errors.is_empty() {
            let error_msg = response
                .errors
                .iter()
                .map(|e| e.message.clone())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(error_msg);
        }

        // Serialize the data
        serde_json::to_string(&response.data)
            .map_err(|e| format!("failed to serialize response: {}", e))
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(format!("mutation failed: {}", e)),
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
        assert!(mutation.contains("create_User"));
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
        assert!(mutation.contains("create_User"));
        assert!(mutation.contains("input: ["));
        assert!(mutation.contains("_docID"));
    }
}
