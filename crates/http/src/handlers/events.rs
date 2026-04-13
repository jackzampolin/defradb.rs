//! SSE endpoint for streaming event bus events.

use std::convert::Infallible;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    response::sse::{Event, Sse},
};
use query::executor::QueryRequest;
use query::subscription::response_has_data;
use serde::Deserialize;

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::query_context::{
    execute_with_resolved_context, resolve_dac_bypass, resolve_signing_config,
};
use crate::router::AppState;
use crate::router::NodePermission;

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    pub event: Option<String>,
}

fn required_permission_for_event_filter(event: Option<&str>) -> NodePermission {
    match event {
        Some("topic-peer-event") => NodePermission::P2pPeerInfo,
        Some("acp-cache-invalidated") | Some("acp-height-advanced") => NodePermission::DacStatus,
        Some("update") | Some("merge-complete") | None | Some(_) => NodePermission::DocumentRead,
    }
}

#[derive(Clone)]
struct EventAccessContext {
    executor: Arc<dyn query::executor::QueryExecutor>,
    collection_mgmt: Option<Arc<dyn crate::router::CollectionManagementOperations>>,
    signing_config: Option<defra_core::signing::SigningConfig>,
    dac_bypass: bool,
    did: Option<identity::Did>,
    acp_enabled: bool,
}

impl EventAccessContext {
    async fn can_observe_document_event(
        &self,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<bool, HttpError> {
        if !self.acp_enabled {
            return Ok(true);
        }

        let Some(collection_mgmt) = &self.collection_mgmt else {
            // Without collection metadata we cannot safely prove visibility.
            return Ok(false);
        };

        let collection = collection_mgmt
            .find_collection_by_id(collection_id)
            .await
            .map_err(HttpError::Internal)?
            .or(collection_mgmt
                .get_collection_by_version_id(collection_id)
                .await
                .map_err(HttpError::Internal)?);

        let Some(collection) = collection else {
            return Ok(false);
        };

        if collection.policy.is_none() {
            return Ok(true);
        }

        if doc_id.is_empty() {
            return Ok(false);
        }

        let query = format!(
            r#"query {{ {}(docID: "{}") {{ _docID }} }}"#,
            collection.name, doc_id
        );
        let response = execute_with_resolved_context(
            self.executor.clone(),
            QueryRequest::new(query).with_identity(self.did.clone()),
            self.signing_config.clone(),
            self.dac_bypass,
        )
        .await;

        Ok(response_has_data(&response))
    }
}

/// GET /api/v0/events — stream event bus events as SSE.
///
/// Optional query param `?event=topic-peer-event` filters by event name.
pub async fn events_sse(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Query(params): Query<EventsQuery>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, HttpError> {
    require_permission(
        &state,
        &identity,
        required_permission_for_event_filter(params.event.as_deref()),
    )
    .await?;

    let event_bus = state
        .event_bus
        .as_ref()
        .ok_or_else(|| HttpError::ServiceUnavailable("event bus is not available".to_string()))?;

    let filter = match params.event.as_deref() {
        Some("acp-cache-invalidated") => vec![events::EventName::AcpCacheInvalidated],
        Some("acp-height-advanced") => vec![events::EventName::AcpHeightAdvanced],
        Some("topic-peer-event") => vec![events::EventName::TopicPeerEvent],
        Some("update") => vec![events::EventName::Update],
        Some("merge-complete") => vec![events::EventName::MergeComplete],
        _ => vec![events::EventName::WildCard],
    };

    let mut subscription = event_bus.subscribe(&filter);
    let access = EventAccessContext {
        executor: state.executor.clone(),
        collection_mgmt: state.collection_mgmt.clone(),
        signing_config: resolve_signing_config(&state, &identity),
        dac_bypass: resolve_dac_bypass(&state, &identity).await,
        did: identity.did().cloned(),
        acp_enabled: state.doc_acp.is_some() || state.acp.is_some(),
    };

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
                let Ok(can_observe) = access.can_observe_document_event(&update.collection_id, &update.doc_id).await else {
                    continue;
                };
                if !can_observe {
                    continue;
                }

                serde_json::json!({
                    "name": "update",
                    "data": {
                        "doc_id": update.doc_id,
                        "cid": update.cid.to_string(),
                        "collection_id": update.collection_id,
                    }
                })
            } else if let Some(data) = message.as_merge_complete() {
                let Ok(can_observe) = access.can_observe_document_event(&data.collection_id, &data.doc_id).await else {
                    continue;
                };
                if !can_observe {
                    continue;
                }

                serde_json::json!({
                    "name": "merge-complete",
                    "data": {
                        "doc_id": data.doc_id,
                        "cid": data.cid.to_string(),
                        "collection_id": data.collection_id,
                        "by_peer": data.by_peer,
                    }
                })
            } else if let Some(data) = message.as_acp_height_advanced() {
                serde_json::json!({
                    "name": "acp-height-advanced",
                    "data": {
                        "height": data.height,
                        "module_state_root": data.module_state_root,
                    }
                })
            } else if let Some(data) = message.as_acp_cache_invalidated() {
                serde_json::json!({
                    "name": "acp-cache-invalidated",
                    "data": {
                        "height": data.height,
                        "module_state_root": data.module_state_root,
                        "previous_root": data.previous_root,
                        "entries_invalidated": data.entries_invalidated,
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
