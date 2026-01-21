//! Backup endpoint handlers.
//!
//! These handlers provide HTTP access to database backup operations:
//! - Export database to JSON
//! - Import database from JSON

use axum::{
    body::Body,
    extract::{Query, State},
    http::header,
    response::Response,
    Json,
};
use serde::Deserialize;

use crate::error::HttpError;
use crate::router::AppState;
use crate::validation::validate_collection_name;

/// Query parameters for export.
#[derive(Debug, Clone, Deserialize)]
pub struct ExportQuery {
    /// Collections to export (if empty, exports all).
    #[serde(default)]
    pub collections: Vec<String>,
    /// Whether to pretty-print the JSON output.
    #[serde(default)]
    pub pretty: bool,
}

/// Export the database.
///
/// GET /api/v0/backup/export
pub async fn export(
    State(state): State<AppState>,
    Query(query): Query<ExportQuery>,
) -> Result<Response, HttpError> {
    let backup = state
        .backup
        .as_ref()
        .ok_or_else(|| HttpError::Internal("Backup operations not configured".into()))?;

    // Validate collection names if provided
    for col in &query.collections {
        validate_collection_name(col)?;
    }

    let collections = if query.collections.is_empty() {
        None
    } else {
        Some(query.collections)
    };

    let data = backup
        .export(collections, query.pretty)
        .await
        .map_err(HttpError::Internal)?;

    // Return as JSON with appropriate content type
    let response = Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(data))
        .map_err(|e| HttpError::Internal(e.to_string()))?;

    Ok(response)
}

/// Import the database.
///
/// POST /api/v0/backup/import
pub async fn import(
    State(state): State<AppState>,
    body: String,
) -> Result<Json<ImportResponse>, HttpError> {
    let backup = state
        .backup
        .as_ref()
        .ok_or_else(|| HttpError::Internal("Backup operations not configured".into()))?;

    if body.trim().is_empty() {
        return Err(HttpError::BadRequest("import data cannot be empty".into()));
    }

    // Validate that the body is valid JSON with expected structure
    let parsed: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| HttpError::BadRequest(format!("invalid JSON: {}", e)))?;

    // Backup data should be an object or array, not a primitive
    if !parsed.is_object() && !parsed.is_array() {
        return Err(HttpError::BadRequest(
            "backup data must be a JSON object or array".into(),
        ));
    }

    backup
        .import(&body)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(ImportResponse { success: true }))
}

/// Response for import operation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ImportResponse {
    pub success: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export_query_empty() {
        let query: ExportQuery = serde_json::from_str("{}").unwrap();
        assert!(query.collections.is_empty());
        assert!(!query.pretty);
    }

    #[test]
    fn test_export_query_with_collections() {
        let json = r#"{"collections": ["Users", "Posts"], "pretty": true}"#;
        let query: ExportQuery = serde_json::from_str(json).unwrap();
        assert_eq!(query.collections.len(), 2);
        assert!(query.pretty);
    }

    #[test]
    fn test_import_response_serialize() {
        let response = ImportResponse { success: true };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("success"));
        assert!(json.contains("true"));
    }
}
