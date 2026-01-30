//! Query execution for FFI.
//!
//! This module exposes GraphQL query execution that matches
//! Go's cbindings/query.go behavior.

use std::ffi::c_char;

use crate::get_runtime;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

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

    let identity_str = c_str_to_string(identity_did);
    let query_str = match c_str_to_string(request_query) {
        Some(s) => s,
        None => return FfiResult::error("request_query is null"),
    };
    let op_name = c_str_to_string(operation_name);
    let vars_str = c_str_to_string(variables);

    // Parse identity DID if provided
    let did = match identity_str {
        Some(s) if !s.is_empty() => match identity::Did::new(&s) {
            Ok(d) => Some(d),
            Err(e) => return FfiResult::error(format!("invalid identity DID: {}", e)),
        },
        _ => None,
    };

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
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Query (should return empty array)
        let query_str = CString::new("{ User { name } }").unwrap();
        let result = unsafe { exec_request(node, ptr::null(), query_str.as_ptr(), ptr::null(), ptr::null()) };
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
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Create a user
        let mutation =
            CString::new(r#"mutation { create_User(input: {name: "Alice"}) { _docID name } }"#)
                .unwrap();
        let result = unsafe { exec_request(node, ptr::null(), mutation.as_ptr(), ptr::null(), ptr::null()) };
        assert_eq!(result.status, 0, "mutation should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Alice"), "response should contain Alice");

        // Cleanup
        unsafe { crate::types::defra_free_string(result.value) };

        // Query to verify
        let query_str = CString::new("{ User { name } }").unwrap();
        let result = unsafe { exec_request(node, ptr::null(), query_str.as_ptr(), ptr::null(), ptr::null()) };
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
        let result = unsafe { exec_request(node, ptr::null(), ptr::null(), ptr::null(), ptr::null()) };
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
        let result = unsafe { exec_request(0, ptr::null(), query_str.as_ptr(), ptr::null(), ptr::null()) };
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
        let result = unsafe { add_schema(node, sdl.as_ptr()) };
        assert_eq!(result.status, 0);

        // Query with invalid JSON variables should return error
        let query_str = CString::new("{ User { name } }").unwrap();
        let invalid_json = CString::new("not valid json").unwrap();
        let result =
            unsafe { exec_request(node, ptr::null(), query_str.as_ptr(), ptr::null(), invalid_json.as_ptr()) };
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
