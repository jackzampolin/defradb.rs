//! Query execution for FFI.
//!
//! This module exposes GraphQL query execution that matches
//! Go's cbindings/query.go behavior.

use std::ffi::c_char;

use crate::runtime::RUNTIME;
use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};

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
    request_query: *const c_char,
    operation_name: *const c_char,
    variables: *const c_char,
) -> FfiResult {
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return FfiResult::error("runtime not initialized"),
    };

    let query_str = match c_str_to_string(request_query) {
        Some(s) => s,
        None => return FfiResult::error("request_query is null"),
    };
    let op_name = c_str_to_string(operation_name);
    let vars_str = c_str_to_string(variables);

    let result = rt.block_on(async {
        // Get query runner
        let runner = NODES
            .get(node_ptr, |state| state.query_runner.clone())
            .ok_or_else(|| "invalid node handle".to_string())?;

        // Build request
        let mut request = query::QueryRequest::new(query_str);
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
        crate::runtime::init_runtime();

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
        let result = unsafe { exec_request(node, query_str.as_ptr(), ptr::null(), ptr::null()) };
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
        crate::runtime::init_runtime();

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
        let result = unsafe { exec_request(node, mutation.as_ptr(), ptr::null(), ptr::null()) };
        assert_eq!(result.status, 0, "mutation should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Alice"), "response should contain Alice");

        // Cleanup
        unsafe { crate::types::defra_free_string(result.value) };

        // Query to verify
        let query_str = CString::new("{ User { name } }").unwrap();
        let result = unsafe { exec_request(node, query_str.as_ptr(), ptr::null(), ptr::null()) };
        assert_eq!(result.status, 0, "query should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Alice"), "query result should contain Alice");

        // Cleanup
        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }
}
