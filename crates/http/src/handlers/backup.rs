//! Backup endpoint handlers.
//!
//! These handlers provide HTTP access to database backup operations:
//! - Export database to JSON
//! - Import database from JSON
//!
//! All endpoints enforce NAC permissions when NAC is enabled.
//! Export requires `DocumentRead` permission.
//! Import requires `DocumentUpdate` permission.

use axum::{
    body::{Body, Bytes},
    extract::{Query, State},
    http::header,
    response::Response,
    Json,
};
use serde::Deserialize;

use crate::error::HttpError;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, ImportResult, NodePermission};
use crate::validation::validate_collection_name;

/// Maximum size for backup import data (100 MB).
const MAX_IMPORT_SIZE: usize = 100 * 1024 * 1024;

/// Maximum number of collections that can be specified in export query.
const MAX_EXPORT_COLLECTIONS: usize = 100;

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
///
/// Requires `DocumentRead` permission when NAC is enabled.
pub async fn export(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Query(query): Query<ExportQuery>,
) -> Result<Response, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentRead).await?;

    let backup = state.require_backup()?;

    // Validate collection count limit
    if query.collections.len() > MAX_EXPORT_COLLECTIONS {
        return Err(HttpError::BadRequest(format!(
            "too many collections specified (max: {})",
            MAX_EXPORT_COLLECTIONS
        )));
    }

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
///
/// Requires `DocumentUpdate` permission when NAC is enabled.
pub async fn import(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: Bytes,
) -> Result<Json<ImportResponse>, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentUpdate).await?;

    let backup = state.require_backup()?;

    // Check body size limit
    if body.len() > MAX_IMPORT_SIZE {
        return Err(HttpError::BadRequest(format!(
            "import data exceeds maximum size of {} bytes",
            MAX_IMPORT_SIZE
        )));
    }

    // Convert bytes to UTF-8 string
    let body = String::from_utf8(body.to_vec())
        .map_err(|_| HttpError::BadRequest("import data must be valid UTF-8".into()))?;

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

    // Reject empty objects or arrays
    let is_empty = match &parsed {
        serde_json::Value::Object(obj) => obj.is_empty(),
        serde_json::Value::Array(arr) => arr.is_empty(),
        _ => false,
    };
    if is_empty {
        return Err(HttpError::BadRequest(
            "backup data is empty - nothing to import".into(),
        ));
    }

    let result = backup.import(&body).await.map_err(HttpError::BadRequest)?;

    Ok(Json(ImportResponse::from(result)))
}

/// Response for import operation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ImportResponse {
    /// Whether the import completed successfully.
    pub success: bool,
    /// Number of documents imported.
    pub documents_imported: u64,
    /// Number of documents skipped.
    pub documents_skipped: u64,
    /// Collections affected by the import.
    pub collections_affected: Vec<String>,
    /// Non-fatal errors encountered during import.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

impl From<ImportResult> for ImportResponse {
    fn from(result: ImportResult) -> Self {
        Self {
            success: result.errors.is_empty(),
            documents_imported: result.documents_imported,
            documents_skipped: result.documents_skipped,
            collections_affected: result.collections_affected,
            errors: result.errors,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::ImportResult;

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
        let response = ImportResponse {
            success: true,
            documents_imported: 10,
            documents_skipped: 2,
            collections_affected: vec!["Users".to_string(), "Posts".to_string()],
            errors: vec![],
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("success"));
        assert!(json.contains("true"));
        assert!(json.contains("documents_imported"));
        assert!(json.contains("10"));
        assert!(json.contains("collections_affected"));
        // errors should be omitted when empty
        assert!(!json.contains("errors"));
    }

    #[test]
    fn test_import_response_with_errors() {
        let response = ImportResponse {
            success: false,
            documents_imported: 5,
            documents_skipped: 0,
            collections_affected: vec!["Users".to_string()],
            errors: vec!["Failed to import document bae-123".to_string()],
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"success\":false"));
        assert!(json.contains("errors"));
        assert!(json.contains("Failed to import"));
    }

    #[test]
    fn test_import_response_from_import_result() {
        let result = ImportResult {
            documents_imported: 15,
            documents_skipped: 3,
            collections_affected: vec!["Users".to_string()],
            errors: vec![],
        };
        let response = ImportResponse::from(result);
        assert!(response.success);
        assert_eq!(response.documents_imported, 15);
        assert_eq!(response.documents_skipped, 3);
    }
}
