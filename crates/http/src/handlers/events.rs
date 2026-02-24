//! SSE endpoint for streaming event bus events.

use std::convert::Infallible;

use axum::{
    extract::{Query, State},
    response::sse::{Event, Sse},
};
use serde::Deserialize;

use crate::error::HttpError;
use crate::router::AppState;

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    pub event: Option<String>,
}

/// GET /api/v0/events — stream event bus events as SSE.
///
/// Optional query param `?event=topic-peer-event` filters by event name.
pub async fn events_sse(
    State(state): State<AppState>,
    Query(params): Query<EventsQuery>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, HttpError> {
    let event_bus = state
        .event_bus
        .as_ref()
        .ok_or_else(|| HttpError::ServiceUnavailable("event bus is not available".to_string()))?;

    let filter = match params.event.as_deref() {
        Some("topic-peer-event") => vec![events::EventName::TopicPeerEvent],
        Some("update") => vec![events::EventName::Update],
        Some("merge-complete") => vec![events::EventName::MergeComplete],
        _ => vec![events::EventName::WildCard],
    };

    let mut subscription = event_bus.subscribe(&filter);

    let stream = async_stream::stream! {
        while let Some(message) = subscription.recv().await {
            let json = if let Some(data) = message.as_topic_peer_event() {
                serde_json::json!({
                    "name": "topic-peer-event",
                    "data": {
                        "peer_id": data.peer_id,
                        "topic": data.topic,
                        "event_type": data.event_type,
                    }
                })
            } else if let Some(update) = message.as_update() {
                serde_json::json!({
                    "name": "update",
                    "data": {
                        "doc_id": update.doc_id,
                        "cid": update.cid.to_string(),
                        "collection_id": update.collection_id,
                    }
                })
            } else if let Some(data) = message.as_merge_complete() {
                serde_json::json!({
                    "name": "merge-complete",
                    "data": {
                        "doc_id": data.doc_id,
                        "cid": data.cid.to_string(),
                        "collection_id": data.collection_id,
                        "by_peer": data.by_peer,
                    }
                })
            } else if message.name == events::EventName::ReplicatorCompleted {
                serde_json::json!({
                    "name": "replicator-completed",
                    "data": {}
                })
            } else {
                continue;
            };

            if let Ok(json_str) = serde_json::to_string(&json) {
                yield Ok(Event::default().event("next").data(json_str));
            }
        }

        yield Ok(Event::default().event("complete").data("{}"));
    };

    Ok(Sse::new(stream))
}
