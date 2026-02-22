use std::ffi::c_char;

use crate::ffi_entry;
use crate::state::{SubscriptionState, NODES, SUBSCRIPTIONS};
use crate::types::c_str_to_string;
use crate::ERR_INVALID_NODE_HANDLE;

use super::CreateSubscriptionResult;

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
    ffi_entry! {
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
    ffi_entry! {
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
}

/// Convert an event message to JSON.
pub(crate) fn message_to_json(message: &events::Message) -> String {
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

pub(crate) use query::subscription::response_has_data;
