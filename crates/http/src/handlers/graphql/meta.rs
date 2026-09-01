//! Meta endpoint handlers (health check, version, schema).

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

/// Health check response.
pub async fn health_check() -> impl IntoResponse {
    Json("Healthy")
}

/// Version endpoint handler.
///
/// Returns full version info including Go compatibility metadata.
pub async fn version() -> Json<defra_version::VersionInfo> {
    Json(defra_version::VersionInfo::new())
}

/// Schema endpoint handler.
///
/// Returns the GraphQL schema as plain text.
///
/// Requires `CollectionGet` permission when NAC is enabled.
pub async fn schema(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<impl IntoResponse, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionGet).await?;

    match state.executor.schema().await {
        Ok(sdl) => Ok((StatusCode::OK, sdl).into_response()),
        Err(e) => {
            tracing::error!(error = %e, "Schema retrieval failed");
            Ok((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(crate::error::ErrorResponse {
                    error: "Failed to retrieve schema".to_string(),
                }),
            )
                .into_response())
        }
    }
}
