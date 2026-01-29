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
/// * `request_query` - GraphQL query string (required)
/// * `identity_ptr` - Identity handle (0 for no identity)
/// * `operation_name` - Optional operation name for multi-operation documents (null if not used)
/// * `variables` - Optional JSON string of variables (null if not used)
///
/// # Safety
///
/// All string pointers must be either null or valid null-terminated UTF-8 strings.
#[export_name = "ExecuteQuery"]
pub unsafe extern "C" fn execute_query(
    node_ptr: usize,
    request_query: *const c_char,
    _identity_ptr: usize,
    operation_name: *const c_char,
    variables: *const c_char,
) -> FfiResult {
    let rt = get_runtime!(FfiResult);

    let query_str = match c_str_to_string(request_query) {
        Some(s) => s,
        None => return FfiResult::error("request_query is null"),
    };
    let op_name = c_str_to_string(operation_name);
    let vars_str = c_str_to_string(variables);

    let runner = match NODES.get(node_ptr, |state| state.query_runner.clone()) {
        Some(r) => r,
        None => return FfiResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let result = rt.block_on(async {
        let mut request = query::QueryRequest::new(query_str);
        if let Some(op) = op_name {
            request = request.with_operation_name(op);
        }
        if let Some(vars) = vars_str {
            let vars_json: serde_json::Value = serde_json::from_str(&vars)
                .map_err(|e| format!("failed to parse variables: {}", e))?;
            request = request.with_variables(vars_json);
        }

        let response = runner.execute(request).await;

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
    fn test_execute_query() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr(), 0) };
        assert_eq!(result.status, 0);

        let query_str = CString::new("{ User { name } }").unwrap();
        let result =
            unsafe { execute_query(node, query_str.as_ptr(), 0, ptr::null(), ptr::null()) };
        assert_eq!(result.status, 0, "execute_query should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("data"), "response should have data field");

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    #[test]
    fn test_execute_mutation() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr(), 0) };
        assert_eq!(result.status, 0);

        let mutation =
            CString::new(r#"mutation { create_User(input: {name: "Alice"}) { _docID name } }"#)
                .unwrap();
        let result = unsafe { execute_query(node, mutation.as_ptr(), 0, ptr::null(), ptr::null()) };
        assert_eq!(result.status, 0, "mutation should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Alice"), "response should contain Alice");

        unsafe { crate::types::defra_free_string(result.value) };

        let query_str = CString::new("{ User { name } }").unwrap();
        let result =
            unsafe { execute_query(node, query_str.as_ptr(), 0, ptr::null(), ptr::null()) };
        assert_eq!(result.status, 0, "query should succeed");

        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("Alice"), "query result should contain Alice");

        unsafe { crate::types::defra_free_string(result.value) };
        node_close(node);
    }

    #[test]
    fn test_execute_query_null_query() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let result = unsafe { execute_query(node, ptr::null(), 0, ptr::null(), ptr::null()) };
        assert_eq!(result.status, 1, "null query should fail");
        assert!(!result.error.is_null());

        let error = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
        assert!(error.contains("null"), "should indicate null query");

        unsafe { crate::types::defra_free_string(result.error) };
        node_close(node);
    }

    #[test]
    fn test_execute_query_invalid_handle() {
        assert!(crate::runtime::init_runtime());

        let query_str = CString::new("{ User { name } }").unwrap();
        let result = unsafe { execute_query(0, query_str.as_ptr(), 0, ptr::null(), ptr::null()) };
        assert_eq!(result.status, 1, "invalid handle should fail");
        assert!(!result.error.is_null());

        let error = unsafe { std::ffi::CStr::from_ptr(result.error).to_string_lossy() };
        assert!(error.contains("invalid"), "should indicate invalid handle");

        unsafe { crate::types::defra_free_string(result.error) };
    }

    #[test]
    fn test_execute_query_invalid_variables_json() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        let sdl = CString::new("type User { name: String }").unwrap();
        let result = unsafe { add_schema(node, sdl.as_ptr(), 0) };
        assert_eq!(result.status, 0);

        let query_str = CString::new("{ User { name } }").unwrap();
        let invalid_json = CString::new("not valid json").unwrap();
        let result = unsafe {
            execute_query(
                node,
                query_str.as_ptr(),
                0,
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
