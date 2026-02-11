use std::ffi::c_char;

use crate::state::{GRAPHQL_SUBSCRIPTIONS, NODES, SUBSCRIPTIONS};
use crate::types::c_str_to_string;

use super::create::message_to_json;
use super::{CloseSubscriptionResult, PollSubscriptionResult};

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

/// Poll a GraphQL subscription for new results.
///
/// Results have already been processed by the background task at event time,
/// so this function simply checks the result buffer.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn poll_graphql_subscription(
    subscription_id: *const c_char,
) -> PollSubscriptionResult {
    let id_str = match unsafe { c_str_to_string(subscription_id) } {
        Some(s) => s,
        None => {
            return PollSubscriptionResult::error("invalid subscription id: null or invalid UTF-8")
        }
    };
    let handle = match id_str.parse::<usize>() {
        Ok(h) => h,
        Err(_) => return PollSubscriptionResult::error("invalid subscription id: not a number"),
    };

    let result =
        GRAPHQL_SUBSCRIPTIONS.get_mut(handle, |state| match state.result_receiver.try_recv() {
            Ok(json) => PollSubscriptionResult::event(json, 0),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                PollSubscriptionResult::no_event(0)
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                PollSubscriptionResult::closed()
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

/// Close a GraphQL subscription and release resources.
///
/// Accepts a string subscription ID and parses it as a numeric handle.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[no_mangle]
pub extern "C" fn close_graphql_subscription(
    subscription_id: *const c_char,
) -> CloseSubscriptionResult {
    let id_str = match unsafe { c_str_to_string(subscription_id) } {
        Some(s) => s,
        None => {
            return CloseSubscriptionResult::error("invalid subscription id: null or invalid UTF-8")
        }
    };
    let handle = match id_str.parse::<usize>() {
        Ok(h) => h,
        Err(_) => return CloseSubscriptionResult::error("invalid subscription id: not a number"),
    };

    // Remove from GraphQL subscription registry
    let state = match GRAPHQL_SUBSCRIPTIONS.remove(handle) {
        Some(state) => state,
        None => return CloseSubscriptionResult::error("invalid subscription handle"),
    };

    // Abort the background event processing task
    state.task_abort.abort();

    // Unsubscribe from the event bus
    NODES.get(state.node_handle, |node_state| {
        node_state.event_bus.unsubscribe(state.event_sub_id);
    });

    CloseSubscriptionResult::success()
}
