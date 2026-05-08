//! Lens migration endpoint handlers.
//!
//! These handlers provide HTTP access to lens migration management:
//! - Set migration between schema versions
//! - Reload lens modules
//!
//! All endpoints enforce NAC permissions when NAC is enabled.
//!
//! Security: File-path-based WASM loading is rejected via HTTP unless
//! dev_mode is enabled. Only inline module bytes are allowed.

use axum::extract::State;
use axum::Json;
use serde::Serialize;

use crate::error::{http_error_from_backend_message, HttpError};
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

/// Response for setting a migration.
#[derive(Debug, Clone, Serialize)]
pub struct SetMigrationResponse {
    #[serde(rename = "lensId")]
    pub lens_id: String,
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
/// Requires `MigrationSet` permission when NAC is enabled.
pub async fn set_migration(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: String,
) -> Result<Json<SetMigrationResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::MigrationSet).await?;

    let lens = state.require_lens()?;

    if body.trim().is_empty() {
        return Err(HttpError::BadRequest(
            "lens configuration cannot be empty".into(),
        ));
    }

    if !state.dev_mode {
        let config: lens::LensConfig = serde_json::from_str(&body)
            .map_err(|e| HttpError::BadRequest(format!("invalid lens config JSON: {}", e)))?;
        config
            .validate_for_http()
            .map_err(|e| HttpError::BadRequest(e.to_string()))?;
    }

    let lens_id = lens
        .set_migration(&body)
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(Json(SetMigrationResponse { lens_id }))
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
/// Accepts lens configuration wrapped as `{"lens": <config>}`.
///
/// Requires `LensCreate` permission when NAC is enabled.
pub async fn add_lens(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: String,
) -> Result<Json<SetMigrationResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::LensCreate).await?;

    let lens = state.require_lens()?;

    if body.trim().is_empty() {
        return Err(HttpError::BadRequest(
            "lens configuration cannot be empty".into(),
        ));
    }

    let parsed: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| HttpError::BadRequest(format!("invalid JSON: {}", e)))?;
    let inner = parsed
        .get("lens")
        .ok_or_else(|| HttpError::BadRequest("missing \"lens\" wrapper".into()))?;
    let config_str = serde_json::to_string(inner)
        .map_err(|e| HttpError::BadRequest(format!("failed to serialize lens config: {}", e)))?;

    if !state.dev_mode {
        let config: lens::LensConfig = serde_json::from_str(&config_str)
            .map_err(|e| HttpError::BadRequest(format!("invalid lens config JSON: {}", e)))?;
        config
            .validate_for_http()
            .map_err(|e| HttpError::BadRequest(e.to_string()))?;
    }

    let lens_id = lens
        .add(&config_str)
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(Json(SetMigrationResponse { lens_id }))
}

/// List lens migrations.
///
/// GET /api/v0/lens
///
/// Requires `LensList` permission when NAC is enabled.
pub async fn list_lenses(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::LensList).await?;

    let lens = state.require_lens()?;

    let modules = lens.list().await.map_err(HttpError::Internal)?;

    Ok(Json(serde_json::json!({"lenses": modules})))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_migration_response_serialize() {
        let response = SetMigrationResponse {
            lens_id: "lens-123".to_string(),
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("lensId"));
        assert!(json.contains("lens-123"));
    }
}
