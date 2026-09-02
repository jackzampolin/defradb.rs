use std::ffi::CString;
use std::ptr;

use crate::node::{new_node, node_close};
use crate::schema::add_schema;
use crate::state::NODES;
use crate::types::NodeInitOptions;

use super::{check_and_set_dac_bypass, exec_request};

#[test]
fn node_identity_is_not_promoted_to_nac_dac_bypass() {
    assert!(crate::runtime::init_runtime());

    let options = NodeInitOptions {
        enable_signing: 1,
        ..Default::default()
    };
    let result = new_node(options);
    assert_eq!(result.status, 0);
    let node = result.node_ptr;
    let node_did = NODES
        .get(node, |state| state.node_identity_did.clone())
        .flatten()
        .unwrap();
    let node_did = CString::new(node_did).unwrap();

    check_and_set_dac_bypass(
        crate::runtime::runtime_handle().unwrap(),
        node,
        node_did.as_ptr(),
    );

    assert!(!defra_core::dac_bypass::get_dac_bypass());
    node_close(node);
}

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
        CString::new(r#"mutation { add_User(input: {name: "Alice"}) { _docID name } }"#).unwrap();
    let result = unsafe {
        exec_request(
            node,
            ptr::null(),
            mutation.as_ptr(),
            ptr::null(),
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
    let result = unsafe {
        exec_request(
            node,
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
        )
    };
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
    let result = unsafe {
        exec_request(
            0,
            ptr::null(),
            query_str.as_ptr(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
        )
    };
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
            ptr::null(),
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
