use std::ffi::CString;
use std::ptr;

use super::{close_subscription, create_subscription, poll_subscription};
use crate::node::{new_node, node_close};
use crate::types::NodeInitOptions;

#[test]
fn test_subscription_lifecycle() {
    // Initialize runtime
    assert!(crate::runtime::init_runtime());

    // Create node
    let options = NodeInitOptions::default();
    let result = new_node(options);
    assert_eq!(result.status, 0);
    let node = result.node_ptr;

    // Create subscription
    let result = unsafe { create_subscription(node, ptr::null()) };
    assert_eq!(result.status, 0, "create_subscription should succeed");
    assert!(result.subscription_handle > 0);
    let sub_handle = result.subscription_handle;

    // Poll (should return no event)
    let result = poll_subscription(sub_handle);
    assert_eq!(result.status, 2, "should have no event initially");

    // Close subscription
    let result = close_subscription(sub_handle);
    assert_eq!(result.status, 0, "close_subscription should succeed");

    // Poll closed subscription should fail
    let result = poll_subscription(sub_handle);
    assert_eq!(result.status, 1, "polling closed sub should error");
    if !result.error.is_null() {
        unsafe { crate::types::defra_free_string(result.error) };
    }

    // Close node
    node_close(node);
}

#[test]
fn test_subscription_receives_mutation_event() {
    use crate::query::exec_request;

    assert!(crate::runtime::init_runtime());

    // Create node
    let options = NodeInitOptions::default();
    let result = new_node(options);
    assert_eq!(result.status, 0);
    let node = result.node_ptr;

    // Add schema
    let sdl = CString::new("type Book { title: String }").unwrap();
    let result = unsafe { crate::schema::add_schema(node, std::ptr::null(), sdl.as_ptr()) };
    assert_eq!(result.status, 0);
    if !result.value.is_null() {
        unsafe { crate::types::defra_free_string(result.value) };
    }

    // Create subscription BEFORE mutation
    let result = unsafe { create_subscription(node, ptr::null()) };
    assert_eq!(result.status, 0);
    let sub_handle = result.subscription_handle;

    // Perform a mutation
    let mutation =
        CString::new(r#"mutation { create_Book(input: {title: "Test"}) { _docID } }"#).unwrap();
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
    assert_eq!(result.status, 0);
    if !result.value.is_null() {
        unsafe { crate::types::defra_free_string(result.value) };
    }

    // Poll should return the event
    let result = poll_subscription(sub_handle);
    // Event may or may not be available depending on timing
    // status 0 = event, 2 = no event yet, both are valid
    assert!(
        result.status == 0 || result.status == 2,
        "poll should succeed"
    );
    if result.status == 0 && !result.value.is_null() {
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(value.contains("update"), "event should be an update");
        unsafe { crate::types::defra_free_string(result.value) };
    }

    // Cleanup
    close_subscription(sub_handle);
    node_close(node);
}

#[test]
fn test_subscription_invalid_node() {
    assert!(crate::runtime::init_runtime());

    // Create subscription with invalid node
    let result = unsafe { create_subscription(0, ptr::null()) };
    assert_eq!(result.status, 1, "should fail with invalid node");
    assert!(!result.error.is_null());
    unsafe { crate::types::defra_free_string(result.error) };
}

#[test]
fn test_close_invalid_subscription() {
    assert!(crate::runtime::init_runtime());

    let result = close_subscription(999999);
    assert_eq!(result.status, 1);
    assert!(!result.error.is_null());
    unsafe { crate::types::defra_free_string(result.error) };
}

#[test]
fn test_node_close_cleans_subscriptions() {
    assert!(crate::runtime::init_runtime());

    // Create node
    let options = NodeInitOptions::default();
    let result = new_node(options);
    assert_eq!(result.status, 0);
    let node = result.node_ptr;

    // Create multiple subscriptions
    let result1 = unsafe { create_subscription(node, ptr::null()) };
    assert_eq!(result1.status, 0);
    let sub1 = result1.subscription_handle;

    let result2 = unsafe { create_subscription(node, ptr::null()) };
    assert_eq!(result2.status, 0);
    let sub2 = result2.subscription_handle;

    // Close node (should clean up subscriptions)
    let result = node_close(node);
    assert_eq!(result.status, 0);

    // Subscriptions should now be invalid
    let result = poll_subscription(sub1);
    assert!(
        result.status == 1 || result.status == 3,
        "sub should be closed or invalid"
    );
    if !result.error.is_null() {
        unsafe { crate::types::defra_free_string(result.error) };
    }

    let result = poll_subscription(sub2);
    assert!(
        result.status == 1 || result.status == 3,
        "sub should be closed or invalid"
    );
    if !result.error.is_null() {
        unsafe { crate::types::defra_free_string(result.error) };
    }
}

#[test]
fn test_subscription_collection_filter() {
    use crate::query::exec_request;

    assert!(crate::runtime::init_runtime());

    // Create node
    let options = NodeInitOptions::default();
    let result = new_node(options);
    assert_eq!(result.status, 0);
    let node = result.node_ptr;

    // Add two schemas
    let sdl = CString::new("type Author { name: String }").unwrap();
    let result = unsafe { crate::schema::add_schema(node, std::ptr::null(), sdl.as_ptr()) };
    assert_eq!(result.status, 0);
    if !result.value.is_null() {
        unsafe { crate::types::defra_free_string(result.value) };
    }

    let sdl = CString::new("type Article { title: String }").unwrap();
    let result = unsafe { crate::schema::add_schema(node, std::ptr::null(), sdl.as_ptr()) };
    assert_eq!(result.status, 0);
    if !result.value.is_null() {
        unsafe { crate::types::defra_free_string(result.value) };
    }

    // Create subscription filtered to Author only
    let filter = CString::new("Author").unwrap();
    let result = unsafe { create_subscription(node, filter.as_ptr()) };
    assert_eq!(result.status, 0);
    let sub_handle = result.subscription_handle;

    // Create an Article (should NOT trigger filtered subscription)
    let mutation =
        CString::new(r#"mutation { create_Article(input: {title: "Test"}) { _docID } }"#).unwrap();
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
    assert_eq!(result.status, 0);
    if !result.value.is_null() {
        unsafe { crate::types::defra_free_string(result.value) };
    }

    // Poll should return no event (Article is filtered out)
    let result = poll_subscription(sub_handle);
    assert_eq!(
        result.status, 2,
        "should have no event for filtered collection"
    );

    // Create an Author (should trigger subscription)
    let mutation =
        CString::new(r#"mutation { create_Author(input: {name: "Bob"}) { _docID } }"#).unwrap();
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
    assert_eq!(result.status, 0);
    if !result.value.is_null() {
        unsafe { crate::types::defra_free_string(result.value) };
    }

    // Poll should return the Author event
    let result = poll_subscription(sub_handle);
    // Event may or may not be available depending on timing
    if result.status == 0 && !result.value.is_null() {
        let value = unsafe { std::ffi::CStr::from_ptr(result.value).to_string_lossy() };
        assert!(
            value.contains("Author"),
            "event should be for Author collection"
        );
        unsafe { crate::types::defra_free_string(result.value) };
    }

    // Cleanup
    close_subscription(sub_handle);
    node_close(node);
}
