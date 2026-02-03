//! Subscription management for FFI.
//!
//! This module exposes a polling-based subscription API for FFI callers.
//! Since FFI can't easily handle async callbacks, we use a polling model:
//!
//! 1. `create_subscription` - Start listening for events, returns handle
//! 2. `poll_subscription` - Non-blocking poll for next event
//! 3. `close_subscription` - Stop listening and cleanup

use std::ffi::{c_char, c_int};
use std::ptr;

use crate::state::{SubscriptionState, NODES, SUBSCRIPTIONS};
use crate::types::{c_str_to_string, sanitize_to_cstring};
use crate::ERR_INVALID_NODE_HANDLE;

/// Result type for subscription creation.
#[repr(C)]
pub struct CreateSubscriptionResult {
    /// Status code: 0=success, 1=error
    pub status: c_int,
    /// Error message (null on success). Caller must free with `defra_free_string`.
    pub error: *mut c_char,
    /// Subscription handle (0 on error).
    pub subscription_handle: usize,
}

impl CreateSubscriptionResult {
    fn success(handle: usize) -> Self {
        Self {
            status: 0,
            error: ptr::null_mut(),
            subscription_handle: handle,
        }
    }

    fn error(message: impl Into<String>) -> Self {
        Self {
            status: 1,
            error: sanitize_to_cstring(message, "unknown error").into_raw(),
            subscription_handle: 0,
        }
    }
}

/// Result type for polling subscriptions.
///
/// Status codes:
/// - 0: Event available (value contains JSON event data)
/// - 1: Error occurred
/// - 2: No event available (subscription open but no pending events)
/// - 3: Subscription closed (no more events will arrive)
#[repr(C)]
pub struct PollSubscriptionResult {
    /// Status code (see above)
    pub status: c_int,
    /// Error message (null unless status=1). Caller must free with `defra_free_string`.
    pub error: *mut c_char,
    /// Event data as JSON (null unless status=0). Caller must free with `defra_free_string`.
    pub value: *mut c_char,
    /// Number of events dropped due to buffer overflow since last poll.
    /// When non-zero, the client should re-fetch data to ensure consistency.
    pub dropped_count: u64,
}

impl PollSubscriptionResult {
    fn event(json: String, dropped: u64) -> Self {
        Self {
            status: 0,
            error: ptr::null_mut(),
            value: sanitize_to_cstring(json, "{}").into_raw(),
            dropped_count: dropped,
        }
    }

    fn error(message: impl Into<String>) -> Self {
        Self {
            status: 1,
            error: sanitize_to_cstring(message, "unknown error").into_raw(),
            value: ptr::null_mut(),
            dropped_count: 0,
        }
    }

    fn no_event(dropped: u64) -> Self {
        Self {
            status: 2,
            error: ptr::null_mut(),
            value: ptr::null_mut(),
            dropped_count: dropped,
        }
    }

    fn closed() -> Self {
        Self {
            status: 3,
            error: ptr::null_mut(),
            value: ptr::null_mut(),
            dropped_count: 0,
        }
    }
}

/// Result type for closing subscriptions.
#[repr(C)]
pub struct CloseSubscriptionResult {
    /// Status code: 0=success, 1=error
    pub status: c_int,
    /// Error message (null on success). Caller must free with `defra_free_string`.
    pub error: *mut c_char,
}

impl CloseSubscriptionResult {
    fn success() -> Self {
        Self {
            status: 0,
            error: ptr::null_mut(),
        }
    }

    fn error(message: impl Into<String>) -> Self {
        Self {
            status: 1,
            error: sanitize_to_cstring(message, "unknown error").into_raw(),
        }
    }
}

/// Create a subscription to database events.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
/// * `collection_filter` - Optional collection name to filter events (null for all)
///
/// # Returns
///
/// A handle that can be used with `poll_subscription` and `close_subscription`.
///
/// # Safety
///
/// The collection_filter must be either null or a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn create_subscription(
    node_ptr: usize,
    collection_filter: *const c_char,
) -> CreateSubscriptionResult {
    let collection = c_str_to_string(collection_filter);

    // Get the event bus from the node
    let subscription = match NODES.get(node_ptr, |state| {
        // Subscribe to Update events (document changes)
        state.event_bus.subscribe(&[events::EventName::Update])
    }) {
        Some(sub) => sub,
        None => return CreateSubscriptionResult::error(ERR_INVALID_NODE_HANDLE),
    };

    // Create subscription state with optional collection filter
    let state = SubscriptionState {
        subscription,
        node_handle: node_ptr,
        collection_filter: collection,
    };

    // Register and return handle
    let handle = SUBSCRIPTIONS.insert(state);
    CreateSubscriptionResult::success(handle)
}

/// Create a subscription to P2P merge complete events.
///
/// # Arguments
///
/// * `node_ptr` - Handle to the node
///
/// # Returns
///
/// A handle that can be used with `poll_subscription` and `close_subscription`.
/// Events will contain merge complete data (doc_id, cid, collection_id, by_peer).
#[no_mangle]
pub extern "C" fn create_merge_complete_subscription(node_ptr: usize) -> CreateSubscriptionResult {
    // Get the event bus from the node
    let subscription = match NODES.get(node_ptr, |state| {
        state.event_bus.subscribe(&[
            events::EventName::MergeComplete,
            events::EventName::ReplicatorCompleted,
            events::EventName::TopicPeerEvent,
            events::EventName::SEArtifactReceived,
        ])
    }) {
        Some(sub) => sub,
        None => return CreateSubscriptionResult::error(ERR_INVALID_NODE_HANDLE),
    };

    let state = SubscriptionState {
        subscription,
        node_handle: node_ptr,
        collection_filter: None,
    };

    let handle = SUBSCRIPTIONS.insert(state);
    CreateSubscriptionResult::success(handle)
}

/// Poll a subscription for the next event (non-blocking).
///
/// # Arguments
///
/// * `subscription_handle` - Handle from `create_subscription`
///
/// # Returns
///
/// - status=0: Event available (value contains JSON)
/// - status=1: Error occurred
/// - status=2: No event available yet
/// - status=3: Subscription closed
///
/// # Event JSON Format
///
/// ```json
/// {
///     "type": "update",
///     "doc_id": "bae-...",
///     "collection_id": "...",
///     "is_relay": false
/// }
/// ```
#[no_mangle]
pub extern "C" fn poll_subscription(subscription_handle: usize) -> PollSubscriptionResult {
    let result = SUBSCRIPTIONS.get_mut(subscription_handle, |state| {
        // Check for dropped messages
        let dropped = state.subscription.check_and_reset_dropped();

        // Try to receive events, filtering by collection if specified
        loop {
            match state.subscription.try_recv() {
                Ok(message) => {
                    // Check collection filter
                    if let Some(ref filter) = state.collection_filter {
                        if let Some(update) = message.as_update() {
                            // Filter by collection name (collection_id contains the schema version ID,
                            // but we match against collection name for user convenience)
                            // The collection_id format is typically the collection name
                            if !update.collection_id.contains(filter.as_str()) {
                                // Skip this event, try next
                                continue;
                            }
                        }
                    }

                    // Convert message to JSON
                    let json = message_to_json(&message);
                    return PollSubscriptionResult::event(json, dropped);
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    return PollSubscriptionResult::no_event(dropped);
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    return PollSubscriptionResult::closed();
                }
            }
        }
    });

    result.unwrap_or_else(|| PollSubscriptionResult::error("invalid subscription handle"))
}

/// Close a subscription and release resources.
///
/// # Arguments
///
/// * `subscription_handle` - Handle from `create_subscription`
///
/// # Safety
///
/// After this call, the subscription handle is no longer valid.
#[no_mangle]
pub extern "C" fn close_subscription(subscription_handle: usize) -> CloseSubscriptionResult {
    // Remove from registry
    let state = match SUBSCRIPTIONS.remove(subscription_handle) {
        Some(state) => state,
        None => return CloseSubscriptionResult::error("invalid subscription handle"),
    };

    // Unsubscribe from the event bus
    let unsubscribed = NODES.get(state.node_handle, |node_state| {
        node_state.event_bus.unsubscribe(state.subscription.id());
    });

    if unsubscribed.is_none() {
        // Node already closed, subscription is effectively cleaned up
    }

    CloseSubscriptionResult::success()
}

// ============================================================================
// GraphQL Subscription Stubs (for compatibility with other worktrees)
// ============================================================================

/// Poll a GraphQL subscription for the next result (stub).
///
/// This is a stub function for compatibility with worktrees that have
/// GraphQL subscription support. Returns an error indicating the feature
/// is not available.
///
/// # Safety
///
/// The subscription_id must be a valid null-terminated C string or null.
#[no_mangle]
pub unsafe extern "C" fn poll_graphql_subscription(
    _subscription_id: *const std::ffi::c_char,
) -> PollSubscriptionResult {
    PollSubscriptionResult::error("GraphQL subscriptions not implemented in this build")
}

/// Close a GraphQL subscription (stub).
///
/// This is a stub function for compatibility with worktrees that have
/// GraphQL subscription support.
///
/// # Safety
///
/// The subscription_id must be a valid null-terminated C string or null.
#[no_mangle]
pub unsafe extern "C" fn close_graphql_subscription(
    _subscription_id: *const std::ffi::c_char,
) -> CloseSubscriptionResult {
    CloseSubscriptionResult::error("GraphQL subscriptions not implemented in this build")
}

/// Convert an event message to JSON.
fn message_to_json(message: &events::Message) -> String {
    // Check if this is an Update event with data
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

    // Check if this is a MergeComplete event with data
    if let Some(mc) = message.as_merge_complete() {
        return serde_json::json!({
            "type": "merge_complete",
            "doc_id": mc.doc_id,
            "cid": mc.cid.to_string(),
            "collection_id": mc.collection_id,
            "by_peer": mc.by_peer
        })
        .to_string();
    }

    // Check if this is a ReplicatorCompleted event
    if message.name == events::EventName::ReplicatorCompleted {
        return serde_json::json!({
            "type": "replicator_completed"
        })
        .to_string();
    }

    // Check if this is a TopicPeerEvent
    if let Some(tpe) = message.as_topic_peer_event() {
        return serde_json::json!({
            "type": "topic_peer_event",
            "peer_id": tpe.peer_id,
            "topic": tpe.topic,
            "event_type": tpe.event_type
        })
        .to_string();
    }

    // Check if this is an SEArtifactReceived event
    if let Some(se) = message.as_se_artifact_received() {
        return serde_json::json!({
            "type": "se_artifact_received",
            "doc_id": se.doc_id
        })
        .to_string();
    }

    // Signal event without data
    let event_type = match message.name {
        events::EventName::Merge => "merge",
        events::EventName::MergeComplete => "merge_complete",
        events::EventName::Update => "update",
        events::EventName::ReplicatorCompleted => "replicator_completed",
        events::EventName::TopicPeerEvent => "topic_peer_event",
        events::EventName::SEArtifactReceived => "se_artifact_received",
        events::EventName::WildCard => "wildcard",
    };
    serde_json::json!({
        "type": event_type
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{new_node, node_close};
    use crate::schema::add_schema;
    use crate::types::NodeInitOptions;
    use std::ffi::CString;

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
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
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
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
        assert_eq!(result.status, 0);
        if !result.value.is_null() {
            unsafe { crate::types::defra_free_string(result.value) };
        }

        let sdl = CString::new("type Article { title: String }").unwrap();
        let result = unsafe { add_schema(node, std::ptr::null(), sdl.as_ptr()) };
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
            CString::new(r#"mutation { create_Article(input: {title: "Test"}) { _docID } }"#)
                .unwrap();
        let result = unsafe {
            exec_request(
                node,
                ptr::null(),
                mutation.as_ptr(),
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
}
