//! Document operations for FFI.
//!
//! This module handles document creation with automatic JSON array/object detection,
//! moving this logic from Go into Rust.
//! All functions use CollectionOptions + identity_ptr pattern.

use std::ffi::{c_char, c_int};

use serde_json::Value as JsonValue;

use db::{DocID, NormalValue};
use query;

use crate::get_runtime;
use crate::state::NODES;
use crate::types::{c_str_to_string, resolve_collection, CollectionOptions, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

/// Convert a JSON value to GraphQL input syntax.
fn json_to_graphql_input(value: &JsonValue) -> String {
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

fn build_create_mutation(collection: &str, data: &JsonValue) -> String {
    let input = json_to_graphql_input(data);
    format!(
        "mutation {{ create_{}(input: {}) {{ _docID }} }}",
        collection, input
    )
}

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
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `json_data` - JSON string containing either a single object or an array of objects
/// * `is_encrypted` - Whether the document should be encrypted (1=true, 0=false)
/// * `encrypted_fields` - Comma-separated list of field names to encrypt (null for all/none)
/// * `opts` - Collection options identifying which collection
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[export_name = "CollectionCreate"]
pub unsafe extern "C" fn collection_create(
    node_ptr: usize,
    json_data: *const c_char,
    _is_encrypted: c_int,
    _encrypted_fields: *const c_char,
    opts: CollectionOptions,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let collection = match resolve_collection(&database, &opts) {
        Ok(c) => c,
        Err(e) => return FfiResult::error(e),
    };

    let col_name = collection.schema().name.clone();

    let json_str = match c_str_to_string(json_data) {
        Some(s) => s,
        None => return FfiResult::error("json_data is null"),
    };

    let parsed: JsonValue = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => return FfiResult::error(format!("invalid JSON: {}", e)),
    };

    let mutation = if parsed.is_array() {
        let docs = match parsed.as_array() {
            Some(arr) => arr.clone(),
            None => return FfiResult::error("expected JSON array"),
        };
        if docs.is_empty() {
            return FfiResult::error("cannot create empty array of documents");
        }
        build_create_many_mutation(&col_name, &docs)
    } else if parsed.is_object() {
        build_create_mutation(&col_name, &parsed)
    } else {
        return FfiResult::error("json_data must be an object or array of objects");
    };

    let runner = match NODES.get(node_ptr, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let request = query::QueryRequest::new(mutation);
        let response = runner.execute(request).await;

        if !response.errors.is_empty() {
            let error_msg = response
                .errors
                .iter()
                .map(|e| e.message.clone())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(error_msg);
        }

        serde_json::to_string(&response.data)
            .map_err(|e| format!("failed to serialize response: {}", e))
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(format!("mutation failed: {}", e)),
    }
}

/// Check if a JSON string represents an array.
///
/// # Safety
///
/// `json_data` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn is_json_array(json_data: *const c_char) -> FfiResult {
    let json_str = match c_str_to_string(json_data) {
        Some(s) => s,
        None => return FfiResult::error("json_data is null"),
    };

    let parsed: JsonValue = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => return FfiResult::error(format!("invalid JSON: {}", e)),
    };

    FfiResult::success(parsed.is_array().to_string())
}

/// Parse a Go-style duration string into nanoseconds.
///
/// # Safety
///
/// `duration_str` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn parse_duration(duration_str: *const c_char) -> FfiResult {
    let input = match c_str_to_string(duration_str) {
        Some(s) => s,
        None => return FfiResult::error("duration_str is null"),
    };

    let trimmed = input.trim();
    if trimmed.is_empty() {
        return FfiResult::success("0");
    }

    match parse_go_duration(trimmed) {
        Ok(nanos) => FfiResult::success(nanos.to_string()),
        Err(e) => FfiResult::error(e),
    }
}

fn parse_go_duration(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if s.is_empty() || s == "0" {
        return Ok(0);
    }

    let (negative, s) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = s.strip_prefix('+') {
        (false, rest)
    } else {
        (false, s)
    };

    if s.chars().all(|c| c.is_ascii_digit()) {
        let secs: i64 = s.parse().map_err(|_| format!("invalid number: {}", s))?;
        let nanos = secs * 1_000_000_000;
        return Ok(if negative { -nanos } else { nanos });
    }

    let mut total_nanos: i64 = 0;
    let mut remaining = s;

    while !remaining.is_empty() {
        let num_end = remaining
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(remaining.len());

        if num_end == 0 {
            return Err(format!("invalid duration: {}", s));
        }

        let num_str = &remaining[..num_end];
        remaining = &remaining[num_end..];

        let unit_end = remaining
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(remaining.len());

        if unit_end == 0 {
            return Err(format!("missing unit in duration: {}", s));
        }

        let unit = &remaining[..unit_end];
        remaining = &remaining[unit_end..];

        let num: f64 = num_str
            .parse()
            .map_err(|_| format!("invalid number in duration: {}", num_str))?;

        let nanos_per_unit: f64 = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1_000.0,
            "ms" => 1_000_000.0,
            "s" => 1_000_000_000.0,
            "m" => 60.0 * 1_000_000_000.0,
            "h" => 3600.0 * 1_000_000_000.0,
            _ => return Err(format!("unknown unit in duration: {}", unit)),
        };

        total_nanos += (num * nanos_per_unit) as i64;
    }

    if negative {
        total_nanos = -total_nanos;
    }

    Ok(total_nanos)
}

/// Parse a JSON string array into a vector of strings.
///
/// # Safety
///
/// `input` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn parse_string_array(input: *const c_char) -> FfiResult {
    let input_str = match c_str_to_string(input) {
        Some(s) => s,
        None => return FfiResult::error("input is null"),
    };

    let trimmed = input_str.trim();

    if trimmed.is_empty() {
        return FfiResult::success("[]");
    }

    if trimmed.starts_with('[') {
        match serde_json::from_str::<Vec<String>>(trimmed) {
            Ok(arr) => {
                let json = serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string());
                return FfiResult::success(json);
            }
            Err(e) => {
                return FfiResult::error(format!("invalid JSON array: {}", e));
            }
        }
    }

    let parts: Vec<String> = trimmed
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let json = serde_json::to_string(&parts).unwrap_or_else(|_| "[]".to_string());
    FfiResult::success(json)
}

// =============================================================================
// Collection Document Operations
// =============================================================================

/// Get a document by ID from a collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `doc_id_str` - Document ID string (e.g., "bae-...")
/// * `show_deleted` - If non-zero, include soft-deleted documents
/// * `opts` - Collection options identifying which collection
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings.
#[export_name = "CollectionGet"]
pub unsafe extern "C" fn collection_get(
    node_ptr: usize,
    doc_id_str: *const c_char,
    _show_deleted: c_int,
    opts: CollectionOptions,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let doc_id_string = match c_str_to_string(doc_id_str) {
        Some(s) => s,
        None => return FfiResult::error("doc_id_str is null"),
    };

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let collection = match resolve_collection(&database, &opts) {
        Ok(c) => c,
        Err(e) => return FfiResult::error(e),
    };

    let result = rt.block_on(async {
        let doc_id = DocID::from_string(&doc_id_string)
            .map_err(|e| format!("invalid document ID '{}': {}", doc_id_string, e))?;

        let txn = database
            .new_txn(true)
            .await
            .map_err(|e| format!("failed to create transaction: {}", e))?;

        let doc = collection
            .get(&txn, &doc_id)
            .await
            .map_err(|e| format!("failed to get document: {}", e))?
            .ok_or_else(|| format!("document '{}' not found", doc_id_string))?;

        txn.commit()
            .await
            .map_err(|e| format!("failed to commit transaction: {}", e))?;

        let doc_map = doc
            .to_map()
            .map_err(|e| format!("failed to convert document to map: {}", e))?;

        serde_json::to_string(&doc_map).map_err(|e| format!("failed to serialize document: {}", e))
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Update a document in a collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `doc_id_str` - Document ID (can be null if using filter)
/// * `filter_str` - JSON filter (can be null if using doc_id)
/// * `updater_str` - JSON update object
/// * `opts` - Collection options identifying which collection
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// All string pointers must be valid null-terminated UTF-8 strings or null.
#[export_name = "CollectionUpdate"]
pub unsafe extern "C" fn collection_update(
    node_ptr: usize,
    doc_id_str: *const c_char,
    _filter_str: *const c_char,
    updater_str: *const c_char,
    opts: CollectionOptions,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let doc_id_opt = c_str_to_string(doc_id_str).filter(|s| !s.is_empty());

    let updater = match c_str_to_string(updater_str) {
        Some(s) => s,
        None => return FfiResult::error("updater_str is null"),
    };

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let collection = match resolve_collection(&database, &opts) {
        Ok(c) => c,
        Err(e) => return FfiResult::error(e),
    };

    let result = rt.block_on(async {
        match doc_id_opt {
            Some(id_str) => {
                let doc_id = DocID::from_string(&id_str)
                    .map_err(|e| format!("invalid document ID '{}': {}", id_str, e))?;

                let txn = database
                    .new_txn(false)
                    .await
                    .map_err(|e| format!("failed to create transaction: {}", e))?;

                let mut doc = collection
                    .get(&txn, &doc_id)
                    .await
                    .map_err(|e| format!("failed to get document: {}", e))?
                    .ok_or_else(|| format!("document '{}' not found", id_str))?;

                let updates: serde_json::Map<String, JsonValue> = serde_json::from_str(&updater)
                    .map_err(|e| format!("invalid updater JSON: {}", e))?;

                for (key, value) in updates {
                    let normal = json_value_to_normal(&value);
                    doc.set(&key, normal);
                }

                collection
                    .update(&txn, &doc)
                    .await
                    .map_err(|e| format!("failed to update document: {}", e))?;

                txn.commit()
                    .await
                    .map_err(|e| format!("failed to commit transaction: {}", e))?;

                Ok::<String, String>("".to_string())
            }
            None => Err("either doc_id or filter must be provided".to_string()),
        }
    });

    match result {
        Ok(val) => {
            if val.is_empty() {
                FfiResult::ok()
            } else {
                FfiResult::success(val)
            }
        }
        Err(e) => FfiResult::error(e),
    }
}

fn json_value_to_normal(value: &JsonValue) -> NormalValue {
    match value {
        JsonValue::Null => NormalValue::Null,
        JsonValue::Bool(b) => NormalValue::Bool(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                NormalValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                NormalValue::Float64(f)
            } else {
                NormalValue::Null
            }
        }
        JsonValue::String(s) => NormalValue::String(s.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => NormalValue::Json(value.clone()),
    }
}

/// List all document IDs in a collection.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `opts` - Collection options identifying which collection
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// opts must have valid string pointers.
#[export_name = "CollectionListDocIDs"]
pub unsafe extern "C" fn collection_list_doc_ids(
    node_ptr: usize,
    opts: CollectionOptions,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let collection = match resolve_collection(&database, &opts) {
        Ok(c) => c,
        Err(e) => return FfiResult::error(e),
    };

    let col_name = collection.schema().name.clone();

    let runner = match NODES.get(node_ptr, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let gql = format!("{{ {} {{ _docID }} }}", col_name);
        let request = query::QueryRequest::new(gql);
        let response = runner.execute(request).await;

        let response_json = serde_json::to_value(&response)
            .map_err(|e| format!("failed to serialize response: {}", e))?;

        let docs = response_json["data"][&col_name]
            .as_array()
            .cloned()
            .unwrap_or_default();

        let results: Vec<serde_json::Value> = docs
            .iter()
            .filter_map(|doc| {
                doc["_docID"].as_str().map(|id| {
                    serde_json::json!({
                        "docID": id
                    })
                })
            })
            .collect();

        serde_json::to_string(&results).map_err(|e| format!("failed to serialize results: {}", e))
    });

    match result {
        Ok(json) => FfiResult::success(json),
        Err(e) => FfiResult::error(e),
    }
}

/// Delete all documents in a collection (truncate).
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `opts` - Collection options identifying which collection
/// * `identity_ptr` - Identity handle (0 for no identity)
///
/// # Safety
///
/// String pointers in `opts` must be null or valid null-terminated UTF-8 strings.
#[export_name = "CollectionTruncate"]
pub unsafe extern "C" fn collection_truncate(
    node_ptr: usize,
    opts: CollectionOptions,
    _identity_ptr: usize,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let database = match NODES.get(node_ptr, |state| state.database.clone()) {
        Some(db) => db,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let collection = match resolve_collection(&database, &opts) {
        Ok(c) => c,
        Err(e) => return FfiResult::error(e),
    };

    let col_name = collection.schema().name.clone();

    let runner = match NODES.get(node_ptr, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let gql = format!("{{ {} {{ _docID }} }}", col_name);
        let request = query::QueryRequest::new(gql);
        let response = runner.execute(request).await;

        let response_json = serde_json::to_value(&response)
            .map_err(|e| format!("failed to serialize response: {}", e))?;

        let docs = response_json["data"][&col_name]
            .as_array()
            .cloned()
            .unwrap_or_default();

        for doc in &docs {
            if let Some(doc_id) = doc["_docID"].as_str() {
                let delete_gql = format!(
                    "mutation {{ delete_{name}(docID: \"{id}\") {{ _docID }} }}",
                    name = col_name,
                    id = doc_id
                );
                let del_request = query::QueryRequest::new(delete_gql);
                let _del_response = runner.execute(del_request).await;
            }
        }

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
    use std::ptr;

    fn make_opts(name: *const c_char) -> CollectionOptions {
        CollectionOptions {
            version: ptr::null(),
            collection_id: ptr::null(),
            name,
            get_inactive: 0,
        }
    }

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

    #[test]
    fn test_is_json_array() {
        use std::ffi::CString;

        let json = CString::new("[1, 2, 3]").unwrap();
        let result = unsafe { is_json_array(json.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "true");
        unsafe { crate::types::defra_free_string(result.value) };

        let json = CString::new(r#"{"name": "test"}"#).unwrap();
        let result = unsafe { is_json_array(json.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "false");
        unsafe { crate::types::defra_free_string(result.value) };
    }

    #[test]
    fn test_parse_string_array_json() {
        use std::ffi::CString;

        let input = CString::new(r#"["a", "b", "c"]"#).unwrap();
        let result = unsafe { parse_string_array(input.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, r#"["a","b","c"]"#);
        unsafe { crate::types::defra_free_string(result.value) };
    }

    #[test]
    fn test_parse_string_array_comma_separated() {
        use std::ffi::CString;

        let input = CString::new("a, b, c").unwrap();
        let result = unsafe { parse_string_array(input.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, r#"["a","b","c"]"#);
        unsafe { crate::types::defra_free_string(result.value) };
    }

    #[test]
    fn test_parse_go_duration_seconds() {
        assert_eq!(parse_go_duration("30s").unwrap(), 30_000_000_000);
    }

    #[test]
    fn test_parse_go_duration_combined() {
        assert_eq!(parse_go_duration("1h30m").unwrap(), 5400 * 1_000_000_000);
    }

    #[test]
    fn test_parse_go_duration_zero() {
        assert_eq!(parse_go_duration("0").unwrap(), 0);
        assert_eq!(parse_go_duration("").unwrap(), 0);
    }

    #[test]
    fn test_parse_go_duration_plain_integer() {
        assert_eq!(parse_go_duration("30").unwrap(), 30_000_000_000);
    }

    /// Helper: create a node with a Person schema and one document, return (node, doc_id).
    fn setup_node_with_doc() -> (usize, String) {
        use crate::node::new_node;
        use crate::types::NodeInitOptions;
        use std::ffi::CString;

        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type Person { name: String, age: Int }").unwrap();
        let result = unsafe { crate::schema::add_schema(node, sdl.as_ptr(), 0) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let mutation = CString::new(
            r#"mutation { create_Person(input: {name: "Alice", age: 30}) { _docID } }"#,
        )
        .unwrap();
        let result = unsafe {
            crate::query::execute_query(node, mutation.as_ptr(), 0, ptr::null(), ptr::null())
        };
        assert_eq!(result.status, 0, "mutation should succeed");
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };

        let parsed: serde_json::Value = serde_json::from_str(&value).unwrap();
        let doc_id = if let Some(id) = parsed["data"]["create_Person"]["_docID"].as_str() {
            id.to_string()
        } else if let Some(id) = parsed["data"]["create_Person"][0]["_docID"].as_str() {
            id.to_string()
        } else {
            panic!("could not extract doc ID from response: {}", value);
        };
        unsafe { crate::types::defra_free_string(result.value) };

        (node, doc_id)
    }

    #[test]
    fn test_collection_get() {
        use std::ffi::CString;

        let (node, doc_id) = setup_node_with_doc();

        let col_name = CString::new("Person").unwrap();
        let doc_id_cstr = CString::new(doc_id.as_str()).unwrap();

        let result = unsafe {
            collection_get(
                node,
                doc_id_cstr.as_ptr(),
                0,
                make_opts(col_name.as_ptr()),
                0,
            )
        };
        assert_eq!(result.status, 0, "collection_get should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Alice"), "should contain Alice");
        assert!(value.contains(&doc_id), "should contain the doc ID");
        unsafe { crate::types::defra_free_string(result.value) };

        crate::node::node_close(node);
    }

    #[test]
    fn test_collection_list_doc_ids() {
        use std::ffi::CString;

        let (node, doc_id) = setup_node_with_doc();

        let col_name = CString::new("Person").unwrap();

        let result = unsafe { collection_list_doc_ids(node, make_opts(col_name.as_ptr()), 0) };
        if result.status != 0 {
            let err = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
            panic!("collection_list_doc_ids failed: {}", err);
        }
        assert_eq!(result.status, 0, "collection_list_doc_ids should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&value).unwrap();
        assert_eq!(parsed.len(), 1, "should have one document");
        assert_eq!(
            parsed[0]["docID"].as_str().unwrap(),
            doc_id,
            "should match the doc ID"
        );
        unsafe { crate::types::defra_free_string(result.value) };

        crate::node::node_close(node);
    }

    #[test]
    fn test_collection_update() {
        use std::ffi::CString;

        let (node, doc_id) = setup_node_with_doc();

        let col_name = CString::new("Person").unwrap();
        let doc_id_cstr = CString::new(doc_id.as_str()).unwrap();
        let updater = CString::new(r#"{"name": "Bob", "age": 40}"#).unwrap();

        let result = unsafe {
            collection_update(
                node,
                doc_id_cstr.as_ptr(),
                std::ptr::null(),
                updater.as_ptr(),
                make_opts(col_name.as_ptr()),
                0,
            )
        };
        assert_eq!(result.status, 0, "collection_update should succeed");

        let result = unsafe {
            collection_get(
                node,
                doc_id_cstr.as_ptr(),
                0,
                make_opts(col_name.as_ptr()),
                0,
            )
        };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Bob"), "name should be updated to Bob");
        unsafe { crate::types::defra_free_string(result.value) };

        crate::node::node_close(node);
    }

    #[test]
    fn test_collection_truncate() {
        use std::ffi::CString;

        let (node, _doc_id) = setup_node_with_doc();

        let col_name = CString::new("Person").unwrap();

        let result = unsafe { collection_truncate(node, make_opts(col_name.as_ptr()), 0) };
        assert_eq!(result.status, 0, "collection_truncate should succeed");

        let result = unsafe { collection_list_doc_ids(node, make_opts(col_name.as_ptr()), 0) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&value).unwrap();
        assert_eq!(parsed.len(), 0, "should have no documents after truncate");
        unsafe { crate::types::defra_free_string(result.value) };

        crate::node::node_close(node);
    }
}
