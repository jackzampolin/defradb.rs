//! View endpoint handlers.

use axum::{
    body::Bytes,
    extract::{Query, State},
    Json,
};

use crate::error::{http_error_from_backend_message, HttpError};
use crate::handlers::collection_selector::CollectionSelectorQuery;
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
    /// `Transform` is Rust's original name for Go's `TransformCID`.
    #[serde(rename = "TransformCID", alias = "Transform", default)]
    pub transform: Option<String>,
}

/// Request body for refreshing views (Go-compatible format).
#[derive(Debug, serde::Deserialize)]
pub struct ViewNamesRequest {
    #[serde(rename = "Names", default)]
    pub names: Option<Vec<String>>,
}

/// Add a view.
///
/// POST /api/v1/view (Go-compatible), POST /api/v1/views
///
/// Creates a new Defra View from a GQL query and SDL schema.
///
/// Requires `ViewAdd` permission when NAC is enabled.
pub async fn add_view(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: Bytes,
) -> Result<Json<Vec<CollectionVersion>>, HttpError> {
    require_permission(&state, &identity, NodePermission::ViewAdd).await?;

    let body: AddViewRequest = required_body(&body)?;

    let view_ops = state.require_view()?;

    let result = view_ops
        .add_view(&body.query, &body.sdl, body.transform.as_deref())
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(Json(result))
}

/// Parse a required JSON body.
///
/// Not `Json`, which answers 422 where Go answers 400 for the same body
/// (`http/handler_store.go:236`), and which discards a body sent without a JSON
/// content type.
fn required_body<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<T, HttpError> {
    serde_json::from_slice(body)
        .map_err(|e| HttpError::BadRequest(format!("invalid request body: {e}")))
}

/// Read an optional `Names` body, absent when empty.
///
/// Dropping a caller's selection here silently widens the work from one view to
/// every view, so a body that is present but unparseable is refused.
fn view_names_from_body(body: &Bytes) -> Result<Option<Vec<String>>, HttpError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    Ok(required_body::<ViewNamesRequest>(body)?.names)
}

/// Refresh materialized view caches.
///
/// POST /api/v1/view/refresh (Go-compatible), POST /api/v1/views/refresh
///
/// Go's client sends no body and selects with query parameters, so the body is
/// optional here. Its `Names` and the query's `name` are both "restrict to
/// these views" and are unioned; neither one refreshes everything.
pub async fn refresh_views(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Query(query): Query<CollectionSelectorQuery>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::ViewRefresh).await?;

    let view_ops = state.require_view()?;

    view_ops
        .refresh_views(query.into_selector_with(view_names_from_body(&body)?)?)
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(Json(serde_json::json!({})))
}

/// Run explicit downsample history GC.
///
/// POST /api/v0/views/gc
///
/// Applies retention-based history cleanup for all or specific downsample views.
pub async fn gc_downsample_histories(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: Bytes,
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::ViewGc).await?;

    let names = view_names_from_body(&body)?;

    let view_ops = state.require_view()?;

    view_ops
        .gc_downsample_histories(names)
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(Json(serde_json::json!({})))
}
