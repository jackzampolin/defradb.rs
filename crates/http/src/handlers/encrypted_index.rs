//! Go-compatible encrypted index endpoint handlers.
//!
//! Route pattern: /api/v0/collections/{name}/encrypted-indexes

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, EncryptedIndexInfo, NodePermission};
use crate::validation::validate_identifier;

/// Go-compatible request to create an encrypted index.
#[derive(Debug, Deserialize)]
pub struct GoCreateEncryptedIndexRequest {
    /// Field name to create the encrypted index on.
    #[serde(rename = "FieldName")]
    pub field_name: Option<String>,
}

/// Create an encrypted index (Go-compatible route).
///
/// POST /api/v0/collections/{name}/encrypted-indexes
pub async fn go_create_encrypted_index(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(collection): Path<String>,
    body: Option<Json<GoCreateEncryptedIndexRequest>>,
) -> Result<Json<EncryptedIndexInfo>, HttpError> {
    require_permission(&state, &identity, NodePermission::EncryptedIndexCreate).await?;

    let ops = state.require_encrypted_index()?;

    validate_identifier(&collection).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            collection
        ))
    })?;

    // Field name can come from body or path segment
    let field_name = body
        .and_then(|b| b.field_name.clone())
        .ok_or_else(|| HttpError::BadRequest("field_name is required".into()))?;

    let info = ops
        .create_encrypted_index(&collection, &field_name)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(info))
}

/// List encrypted indexes for a collection (Go-compatible route).
///
/// GET /api/v0/collections/{name}/encrypted-indexes
pub async fn go_list_encrypted_indexes(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(collection): Path<String>,
) -> Result<Json<Vec<EncryptedIndexInfo>>, HttpError> {
    require_permission(&state, &identity, NodePermission::EncryptedIndexList).await?;

    let ops = state.require_encrypted_index()?;

    validate_identifier(&collection).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            collection
        ))
    })?;

    let indexes = ops
        .list_encrypted_indexes(Some(&collection))
        .await
        .map_err(HttpError::Internal)?;

    Ok(Json(indexes))
}

/// List all encrypted indexes across all collections (Go-compatible route).
///
/// GET /api/v0/encrypted-indexes
pub async fn go_list_all_encrypted_indexes(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<Vec<EncryptedIndexInfo>>, HttpError> {
    require_permission(&state, &identity, NodePermission::EncryptedIndexListAll).await?;

    let ops = state.require_encrypted_index()?;

    let indexes = ops
        .list_encrypted_indexes(None)
        .await
        .map_err(HttpError::Internal)?;

    Ok(Json(indexes))
}

/// Delete an encrypted index (Go-compatible route).
///
/// DELETE /api/v0/collections/{name}/encrypted-indexes/{field}
pub async fn go_delete_encrypted_index(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path((collection, field)): Path<(String, String)>,
) -> Result<StatusCode, HttpError> {
    require_permission(&state, &identity, NodePermission::EncryptedIndexDelete).await?;

    let ops = state.require_encrypted_index()?;

    validate_identifier(&collection).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            collection
        ))
    })?;

    ops.delete_encrypted_index(&collection, &field)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(StatusCode::OK)
}
