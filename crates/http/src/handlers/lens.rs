//! Lens migration endpoint handlers.
//!
//! These handlers provide HTTP access to lens migration management:
//! - Set migration between schema versions
//! - Reload lens modules
//!
//! All endpoints enforce NAC permissions when NAC is enabled.

use axum::extract::State;
use axum::Json;
use serde::Serialize;

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

/// Response for setting a migration.
#[derive(Debug, Clone, Serialize)]
pub struct SetMigrationResponse {
    #[serde(rename = "transformId")]
    pub transform_id: String,
}

/// Set a lens migration between schema versions.
///
/// POST /api/v0/lens/set
///
/// Accepts lens configuration in JSON format in the request body.
/// The configuration should include source and destination schema version IDs
/// and the path to the WASM transform module.
///
/// Example body:
/// ```json
/// {
///   "SourceSchemaVersionID": "bafyreiciz2hrrmt7ritk5gf5fyruw46v2tfhq5dc7qto4wgpzluben2smu",
///   "DestinationSchemaVersionID": "bafyreigqfjat435ghyt66tdaucp7oi2mke5jafx3jw3rozanopihr2vf44",
///   "Lens": {
///     "Path": "/path/to/transform.wasm"
///   }
/// }
/// ```
///
/// Requires `CollectionPatch` permission when NAC is enabled.
pub async fn set_migration(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: String,
) -> Result<Json<SetMigrationResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionPatch).await?;

    let lens = state.require_lens()?;

    if body.trim().is_empty() {
        return Err(HttpError::BadRequest(
            "lens configuration cannot be empty".into(),
        ));
    }

    let transform_id = lens
        .set_migration(&body)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(SetMigrationResponse { transform_id }))
}

/// Reload all lens modules.
///
/// POST /api/v0/lens/reload
///
/// Reloads all registered lens WASM modules from disk.
/// This is useful after updating WASM files to pick up changes.
///
/// Requires `CollectionPatch` permission when NAC is enabled.
pub async fn reload(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<(), HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionPatch).await?;

    let lens = state.require_lens()?;

    lens.reload().await.map_err(HttpError::Internal)?;

    Ok(())
}

/// Add a lens migration.
///
/// POST /api/v0/lens
///
/// Accepts lens configuration in JSON format in the request body.
///
/// Requires `CollectionPatch` permission when NAC is enabled.
pub async fn add_lens(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: String,
) -> Result<Json<SetMigrationResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionPatch).await?;

    let lens = state.require_lens()?;

    if body.trim().is_empty() {
        return Err(HttpError::BadRequest(
            "lens configuration cannot be empty".into(),
        ));
    }

    let transform_id = lens.add(&body).await.map_err(HttpError::BadRequest)?;

    Ok(Json(SetMigrationResponse { transform_id }))
}

/// List lens migrations.
///
/// GET /api/v0/lens
///
/// Requires `CollectionGet` permission when NAC is enabled.
pub async fn list_lenses(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionGet).await?;

    let lens = state.require_lens()?;

    let modules = lens.list().await.map_err(HttpError::Internal)?;

    Ok(Json(modules))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_migration_response_serialize() {
        let response = SetMigrationResponse {
            transform_id: "transform-123".to_string(),
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("transformId"));
        assert!(json.contains("transform-123"));
    }
}
