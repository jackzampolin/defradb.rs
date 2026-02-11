//! Go-compatible encrypted index endpoint handlers.
//!
//! Route pattern: /api/v0/collections/{name}/encrypted-indexes

use std::collections::HashMap;

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
#[derive(Debug, Clone, Deserialize)]
pub struct GoCreateEncryptedIndexRequest {
    #[serde(rename = "FieldName")]
    pub field_name: String,
    #[serde(rename = "Type", default = "default_index_type")]
    pub index_type: String,
}

fn default_index_type() -> String {
    "equality".to_string()
}

/// Create an encrypted index (Go-compatible route).
///
/// POST /api/v0/collections/{name}/encrypted-indexes
pub async fn go_create_encrypted_index(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(collection): Path<String>,
    Json(request): Json<GoCreateEncryptedIndexRequest>,
) -> Result<Json<EncryptedIndexInfo>, HttpError> {
    require_permission(&state, &identity, NodePermission::IndexCreate).await?;

    let ops = state.require_encrypted_index()?;

    validate_identifier(&collection).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            collection
        ))
    })?;

    validate_identifier(&request.field_name).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid field name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            request.field_name
        ))
    })?;

    let info = ops
        .create_encrypted_index(&collection, &request.field_name)
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
    require_permission(&state, &identity, NodePermission::IndexList).await?;

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

/// Delete an encrypted index (Go-compatible route).
///
/// DELETE /api/v0/collections/{name}/encrypted-indexes/{field}
///
/// Returns HTTP 200 with empty body to match Go DefraDB behavior.
pub async fn go_delete_encrypted_index(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path((collection, field)): Path<(String, String)>,
) -> Result<StatusCode, HttpError> {
    require_permission(&state, &identity, NodePermission::IndexDrop).await?;

    let ops = state.require_encrypted_index()?;

    validate_identifier(&collection).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            collection
        ))
    })?;

    validate_identifier(&field).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid field name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            field
        ))
    })?;

    ops.delete_encrypted_index(&collection, &field)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(StatusCode::OK)
}

/// List all encrypted indexes across all collections (Go-compatible route).
///
/// GET /api/v0/encrypted-indexes
///
/// Returns a map grouped by collection name to match Go DefraDB format.
pub async fn go_list_all_encrypted_indexes(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<HashMap<String, Vec<EncryptedIndexInfo>>>, HttpError> {
    require_permission(&state, &identity, NodePermission::IndexList).await?;

    let ops = state.require_encrypted_index()?;

    let indexes = ops
        .list_encrypted_indexes(None)
        .await
        .map_err(HttpError::Internal)?;

    let mut grouped: HashMap<String, Vec<EncryptedIndexInfo>> = HashMap::new();
    for idx in indexes {
        grouped.entry(idx.collection.clone()).or_default().push(idx);
    }

    Ok(Json(grouped))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_go_create_encrypted_index_request_deserialize() {
        let json = r#"{"FieldName": "ssn", "Type": "equality"}"#;
        let request: GoCreateEncryptedIndexRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.field_name, "ssn");
        assert_eq!(request.index_type, "equality");
    }

    #[test]
    fn test_go_create_encrypted_index_request_default_type() {
        let json = r#"{"FieldName": "email"}"#;
        let request: GoCreateEncryptedIndexRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.field_name, "email");
        assert_eq!(request.index_type, "equality");
    }
}
