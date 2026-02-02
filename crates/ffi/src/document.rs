//! Document operations for FFI.
//!
//! This module handles document creation with automatic JSON array/object detection,
//! moving this logic from Go into Rust.

use std::ffi::c_char;

use acp::nac::NodePermission;
use serde_json::Value as JsonValue;

use query;

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
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `collection_name` - Name of the collection
/// * `json_data` - JSON string containing either a single object or an array of objects
///
/// # Returns
///
/// - Status 0: Success (value contains JSON with created document IDs)
/// - Status 1: Error (error field contains message)
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

/// Check if a JSON string represents an array.
///
/// This is a simple utility function that can be used by Go to determine
/// whether to call single-document or multi-document APIs without
/// re-parsing the entire JSON.
///
/// # Arguments
///
/// * `json_data` - JSON string to check
///
/// # Returns
///
/// - Status 0: Success (value is "true" if array, "false" if not)
/// - Status 1: Error (error field contains message if invalid JSON)
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

    // Try to parse to detect type
    let parsed: JsonValue = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => return FfiResult::error(format!("invalid JSON: {}", e)),
    };

    FfiResult::success(parsed.is_array().to_string())
}

/// Parse a Go-style duration string into nanoseconds.
///
/// Supports Go's duration format: "300ms", "1.5h", "2h45m30s", etc.
/// Valid units: ns, us (or µs), ms, s, m, h
///
/// # Arguments
///
/// * `duration_str` - Duration string in Go format
///
/// # Returns
///
/// - Status 0: Success (value contains nanoseconds as string)
/// - Status 1: Error (error field contains message)
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

/// Parse a Go-style duration string into nanoseconds.
///
/// Go duration format: optional sign, then sequence of decimal numbers with unit suffixes.
/// Examples: "300ms", "1.5h", "2h45m", "-1h30m", "1µs"
///
/// Also accepts plain integers, which are treated as seconds for backwards compatibility.
/// Examples: "30" -> 30 seconds, "60" -> 60 seconds
fn parse_go_duration(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if s.is_empty() || s == "0" {
        return Ok(0);
    }

    let (negative, s) = if s.starts_with('-') {
        (true, &s[1..])
    } else if s.starts_with('+') {
        (false, &s[1..])
    } else {
        (false, s)
    };

    // Check if it's a plain integer (backwards compatibility: treat as seconds)
    if s.chars().all(|c| c.is_ascii_digit()) {
        let secs: i64 = s.parse().map_err(|_| format!("invalid number: {}", s))?;
        let nanos = secs * 1_000_000_000;
        return Ok(if negative { -nanos } else { nanos });
    }

    let mut total_nanos: i64 = 0;
    let mut remaining = s;

    while !remaining.is_empty() {
        // Find the end of the number part (including decimal point)
        let num_end = remaining
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(remaining.len());

        if num_end == 0 {
            return Err(format!("invalid duration: {}", s));
        }

        let num_str = &remaining[..num_end];
        remaining = &remaining[num_end..];

        // Find the unit
        let unit_end = remaining
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(remaining.len());

        if unit_end == 0 {
            return Err(format!("missing unit in duration: {}", s));
        }

        let unit = &remaining[..unit_end];
        remaining = &remaining[unit_end..];

        // Parse number (can be float)
        let num: f64 = num_str
            .parse()
            .map_err(|_| format!("invalid number in duration: {}", num_str))?;

        // Convert to nanoseconds based on unit
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
/// This function handles both JSON arrays (e.g., `["a", "b", "c"]`) and
/// comma-separated strings (e.g., `"a,b,c"`) for backwards compatibility.
///
/// # Arguments
///
/// * `input` - JSON array string or comma-separated string
///
/// # Returns
///
/// - Status 0: Success (value contains JSON array of strings)
/// - Status 1: Error (error field contains message)
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

    // Empty input returns empty array
    if trimmed.is_empty() {
        return FfiResult::success("[]");
    }

    // Try to parse as JSON array first
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

    // Fall back to comma-separated parsing
    let parts: Vec<String> = trimmed
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let json = serde_json::to_string(&parts).unwrap_or_else(|_| "[]".to_string());
    FfiResult::success(json)
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

    #[test]
    fn test_is_json_array() {
        use std::ffi::CString;

        // Array
        let json = CString::new("[1, 2, 3]").unwrap();
        let result = unsafe { is_json_array(json.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "true");
        unsafe { crate::types::defra_free_string(result.value) };

        // Object
        let json = CString::new(r#"{"name": "test"}"#).unwrap();
        let result = unsafe { is_json_array(json.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "false");
        unsafe { crate::types::defra_free_string(result.value) };
    }

    #[test]
    fn test_is_json_array_invalid() {
        use std::ffi::CString;

        let json = CString::new("not valid json").unwrap();
        let result = unsafe { is_json_array(json.as_ptr()) };
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        unsafe { crate::types::defra_free_string(result.error) };
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
    fn test_parse_string_array_empty() {
        use std::ffi::CString;

        let input = CString::new("").unwrap();
        let result = unsafe { parse_string_array(input.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "[]");
        unsafe { crate::types::defra_free_string(result.value) };
    }

    #[test]
    fn test_parse_string_array_single() {
        use std::ffi::CString;

        let input = CString::new("single").unwrap();
        let result = unsafe { parse_string_array(input.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, r#"["single"]"#);
        unsafe { crate::types::defra_free_string(result.value) };
    }

    // Duration parsing tests

    #[test]
    fn test_parse_go_duration_seconds() {
        assert_eq!(parse_go_duration("30s").unwrap(), 30_000_000_000);
        assert_eq!(parse_go_duration("1s").unwrap(), 1_000_000_000);
    }

    #[test]
    fn test_parse_go_duration_minutes() {
        assert_eq!(parse_go_duration("5m").unwrap(), 5 * 60 * 1_000_000_000);
        assert_eq!(parse_go_duration("1m").unwrap(), 60_000_000_000);
    }

    #[test]
    fn test_parse_go_duration_hours() {
        assert_eq!(parse_go_duration("1h").unwrap(), 3600 * 1_000_000_000);
        assert_eq!(parse_go_duration("2h").unwrap(), 2 * 3600 * 1_000_000_000);
    }

    #[test]
    fn test_parse_go_duration_combined() {
        // 1h30m = 90 minutes = 5400 seconds
        assert_eq!(parse_go_duration("1h30m").unwrap(), 5400 * 1_000_000_000);
        // 2h45m30s
        assert_eq!(
            parse_go_duration("2h45m30s").unwrap(),
            (2 * 3600 + 45 * 60 + 30) * 1_000_000_000
        );
    }

    #[test]
    fn test_parse_go_duration_milliseconds() {
        assert_eq!(parse_go_duration("300ms").unwrap(), 300_000_000);
        assert_eq!(parse_go_duration("1500ms").unwrap(), 1_500_000_000);
    }

    #[test]
    fn test_parse_go_duration_microseconds() {
        assert_eq!(parse_go_duration("100us").unwrap(), 100_000);
        assert_eq!(parse_go_duration("100µs").unwrap(), 100_000);
    }

    #[test]
    fn test_parse_go_duration_nanoseconds() {
        assert_eq!(parse_go_duration("1000ns").unwrap(), 1000);
    }

    #[test]
    fn test_parse_go_duration_negative() {
        assert_eq!(parse_go_duration("-30s").unwrap(), -30_000_000_000);
    }

    #[test]
    fn test_parse_go_duration_float() {
        assert_eq!(
            parse_go_duration("1.5h").unwrap(),
            (1.5 * 3600.0 * 1e9) as i64
        );
        assert_eq!(parse_go_duration("0.5s").unwrap(), 500_000_000);
    }

    #[test]
    fn test_parse_go_duration_zero() {
        assert_eq!(parse_go_duration("0").unwrap(), 0);
        assert_eq!(parse_go_duration("").unwrap(), 0);
    }

    #[test]
    fn test_parse_go_duration_plain_integer() {
        // Plain integers are treated as seconds (backwards compatibility)
        assert_eq!(parse_go_duration("30").unwrap(), 30_000_000_000);
        assert_eq!(parse_go_duration("60").unwrap(), 60_000_000_000);
        assert_eq!(parse_go_duration("-30").unwrap(), -30_000_000_000);
    }

    #[test]
    fn test_parse_go_duration_invalid() {
        assert!(parse_go_duration("invalid").is_err());
        assert!(parse_go_duration("30x").is_err());
    }

    #[test]
    fn test_parse_duration_ffi() {
        use std::ffi::CString;

        let input = CString::new("30s").unwrap();
        let result = unsafe { parse_duration(input.as_ptr()) };
        assert_eq!(result.status, 0);
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert_eq!(value, "30000000000");
        unsafe { crate::types::defra_free_string(result.value) };
    }
}
