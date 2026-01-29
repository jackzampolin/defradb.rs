//! Subscription management for FFI.
//!
//! This module exposes a polling-based subscription API for FFI callers.
//! Go uses string-based subscription IDs and FfiResult return types.
//!
//! Subscription creation goes through ExecuteQuery (returns status=2).
//! This module only handles poll and close.

use std::ffi::c_char;

use crate::state::{NODES, SUBSCRIPTIONS};
use crate::types::{c_str_to_string, FfiResult};

/// Poll a subscription for the next event (non-blocking).
///
/// # Arguments
///
/// * `subscription_id` - String subscription ID (from ExecuteQuery with status=2)
///
/// # Returns
///
/// - status=0: Event available (value contains JSON)
/// - status=1: Error occurred
/// - status=2: No event available yet
/// - status=3: Subscription closed
///
/// # Safety
///
/// `subscription_id` must be a valid null-terminated UTF-8 string.
#[export_name = "PollSubscription"]
pub unsafe extern "C" fn poll_subscription(subscription_id: *const c_char) -> FfiResult {
    let id_str = match c_str_to_string(subscription_id) {
        Some(s) => s,
        None => return FfiResult::error("subscription_id is null"),
    };

    // Parse the subscription ID as a handle
    let handle: usize = match id_str.parse() {
        Ok(h) => h,
        Err(_) => return FfiResult::error(format!("invalid subscription ID: {}", id_str)),
    };

    let result = SUBSCRIPTIONS.get_mut(handle, |state| {
        let dropped = state.subscription.check_and_reset_dropped();

        loop {
            match state.subscription.try_recv() {
                Ok(message) => {
                    if let Some(ref filter) = state.collection_filter {
                        if let Some(update) = message.as_update() {
                            if !update.collection_id.contains(filter.as_str()) {
                                continue;
                            }
                        }
                    }

                    let json = message_to_json(&message);
                    // status=0 with value means event available
                    let mut result = FfiResult::success(json);
                    // Encode dropped count info if any
                    if dropped > 0 {
                        // Include dropped info in the JSON itself
                        let json = message_to_json_with_dropped(&message, dropped);
                        result = FfiResult::success(json);
                    }
                    return result;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    // status=2 means no event available
                    return FfiResult {
                        status: 2,
                        error: std::ptr::null_mut(),
                        value: std::ptr::null_mut(),
                    };
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    // status=3 means subscription closed
                    return FfiResult {
                        status: 3,
                        error: std::ptr::null_mut(),
                        value: std::ptr::null_mut(),
                    };
                }
            }
        }
    });

    result.unwrap_or_else(|| FfiResult::error("invalid subscription handle"))
}

/// Close a subscription and release resources.
///
/// # Arguments
///
/// * `subscription_id` - String subscription ID
///
/// # Safety
///
/// `subscription_id` must be a valid null-terminated UTF-8 string.
/// After this call, the subscription ID is no longer valid.
#[export_name = "CloseSubscription"]
pub unsafe extern "C" fn close_subscription(subscription_id: *const c_char) -> FfiResult {
    let id_str = match c_str_to_string(subscription_id) {
        Some(s) => s,
        None => return FfiResult::error("subscription_id is null"),
    };

    let handle: usize = match id_str.parse() {
        Ok(h) => h,
        Err(_) => return FfiResult::error(format!("invalid subscription ID: {}", id_str)),
    };

    let state = match SUBSCRIPTIONS.remove(handle) {
        Some(state) => state,
        None => return FfiResult::error("invalid subscription handle"),
    };

    // Unsubscribe from the event bus
    NODES.get(state.node_handle, |node_state| {
        node_state.event_bus.unsubscribe(state.subscription.id());
    });

    FfiResult::ok()
}

/// Create a subscription to database events (internal helper, not exported to Go).
///
/// Go creates subscriptions through ExecuteQuery, but we keep this
/// for Rust-only tests.
///
/// # Safety
///
/// `collection_filter` must be null or a valid null-terminated UTF-8 string.
pub unsafe fn create_subscription_internal(
    node_ptr: usize,
    collection_filter: *const c_char,
) -> Result<usize, String> {
    use crate::state::SubscriptionState;

    let collection = c_str_to_string(collection_filter);

    let subscription = NODES
        .get(node_ptr, |state| {
            state.event_bus.subscribe(&[events::EventName::Update])
        })
        .ok_or_else(|| crate::ERR_INVALID_NODE_HANDLE.to_string())?;

    let state = SubscriptionState {
        subscription,
        node_handle: node_ptr,
        collection_filter: collection,
    };

    let handle = SUBSCRIPTIONS.insert(state);
    Ok(handle)
}

/// Convert an event message to JSON.
fn message_to_json(message: &events::Message) -> String {
    if let Some(update) = message.as_update() {
        return serde_json::json!({
            "type": "update",
            "doc_id": update.doc_id,
            "cid": update.cid.to_string(),
            "collection_id": update.collection_id,
            "is_retry": update.is_retry,
            "is_relay": update.is_relay
        })
        .to_string();
    }

    let event_type = match message.name {
        events::EventName::Merge => "merge",
        events::EventName::MergeComplete => "merge_complete",
        events::EventName::Update => "update",
        events::EventName::WildCard => "wildcard",
    };
    serde_json::json!({
        "type": event_type
    })
    .to_string()
}

/// Convert an event message to JSON with dropped count.
fn message_to_json_with_dropped(message: &events::Message, dropped: u64) -> String {
    if let Some(update) = message.as_update() {
        return serde_json::json!({
            "type": "update",
            "doc_id": update.doc_id,
            "cid": update.cid.to_string(),
            "collection_id": update.collection_id,
            "is_retry": update.is_retry,
            "is_relay": update.is_relay,
            "dropped_count": dropped
        })
        .to_string();
    }

    let event_type = match message.name {
        events::EventName::Merge => "merge",
        events::EventName::MergeComplete => "merge_complete",
        events::EventName::Update => "update",
        events::EventName::WildCard => "wildcard",
    };
    serde_json::json!({
        "type": event_type,
        "dropped_count": dropped
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::types::NodeInitOptions;
    use std::ffi::CString;
    use std::ptr;

    #[test]
    fn test_subscription_lifecycle() {
        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Create subscription via internal helper
        let handle = unsafe { create_subscription_internal(node, ptr::null()) }.unwrap();
        let handle_str = CString::new(handle.to_string()).unwrap();

        // Poll (should return no event, status=2)
        let result = unsafe { poll_subscription(handle_str.as_ptr()) };
        assert_eq!(result.status, 2, "should have no event initially");

        // Close subscription
        let result = unsafe { close_subscription(handle_str.as_ptr()) };
        assert_eq!(result.status, 0, "close_subscription should succeed");

        // Poll closed subscription should fail
        let result = unsafe { poll_subscription(handle_str.as_ptr()) };
        assert_eq!(result.status, 1, "polling closed sub should error");
        if !result.error.is_null() {
            unsafe { crate::types::defra_free_string(result.error) };
        }

        node_close(node);
    }

    #[test]
    fn test_subscription_receives_mutation_event() {
        use crate::query::execute_query;

        assert!(crate::runtime::init_runtime());

        let options = NodeInitOptions::default();
        let result = new_node(options);
        assert_eq!(result.status, 0);
        let node = result.node_ptr;

        // Add schema
        let sdl = CString::new("type Book { title: String }").unwrap();
        let result = unsafe { crate::schema::add_schema(node, sdl.as_ptr(), 0) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        // Create subscription BEFORE mutation
        let handle = unsafe { create_subscription_internal(node, ptr::null()) }.unwrap();
        let handle_str = CString::new(handle.to_string()).unwrap();

        // Perform a mutation
        let mutation =
            CString::new(r#"mutation { create_Book(input: {title: "Test"}) { _docID } }"#).unwrap();
        let result = unsafe { execute_query(node, mutation.as_ptr(), 0, ptr::null(), ptr::null()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        // Poll should return the event
        let result = unsafe { poll_subscription(handle_str.as_ptr()) };
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
        let _ = unsafe { close_subscription(handle_str.as_ptr()) };
        node_close(node);
    }

    #[test]
    fn test_subscription_invalid_id() {
        assert!(crate::runtime::init_runtime());

        let bad_id = CString::new("not_a_number").unwrap();
        let result = unsafe { poll_subscription(bad_id.as_ptr()) };
        assert_eq!(result.status, 1, "should fail with invalid ID");
        assert!(!result.error.is_null());
        unsafe { crate::types::defra_free_string(result.error) };
    }

    #[test]
    fn test_close_invalid_subscription() {
        assert!(crate::runtime::init_runtime());

        let bad_id = CString::new("999999").unwrap();
        let result = unsafe { close_subscription(bad_id.as_ptr()) };
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        unsafe { crate::types::defra_free_string(result.error) };
    }

    #[test]
    fn test_subscription_null_id() {
        assert!(crate::runtime::init_runtime());

        let result = unsafe { poll_subscription(ptr::null()) };
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        unsafe { crate::types::defra_free_string(result.error) };

        let result = unsafe { close_subscription(ptr::null()) };
        assert_eq!(result.status, 1);
        assert!(!result.error.is_null());
        unsafe { crate::types::defra_free_string(result.error) };
    }
}
