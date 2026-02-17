//! View endpoint handlers.

use axum::{extract::State, Json};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};
use schema::CollectionVersion;

/// Request body for adding a view (Go-compatible format).
#[derive(Debug, serde::Deserialize)]
pub struct AddViewRequest {
    #[serde(rename = "Query")]
    pub query: String,
    #[serde(rename = "SDL")]
    pub sdl: String,
    #[serde(rename = "Transform", default)]
    pub transform: Option<String>,
}

/// Request body for refreshing views (Go-compatible format).
#[derive(Debug, serde::Deserialize)]
pub struct RefreshViewsRequest {
    #[serde(rename = "Names", default)]
    pub names: Option<Vec<String>>,
}

/// Add a view.
///
/// POST /api/v0/views
///
/// Creates a new Defra View from a GQL query and SDL schema.
///
/// Requires `ViewAdd` permission when NAC is enabled.
pub async fn add_view(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<AddViewRequest>,
) -> Result<Json<Vec<CollectionVersion>>, HttpError> {
    require_permission(&state, &identity, NodePermission::ViewAdd).await?;

    let view_ops = state.require_view()?;

    let result = view_ops
        .add_view(&body.query, &body.sdl, body.transform.as_deref())
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(result))
}

/// Refresh materialized view caches.
///
/// POST /api/v0/views/refresh
///
/// Refreshes all or specific materialized views.
pub async fn refresh_views(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(body): Json<RefreshViewsRequest>,
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::ViewRefresh).await?;

    let view_ops = state.require_view()?;

    view_ops
        .refresh_views(body.names)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(serde_json::json!({})))
}
