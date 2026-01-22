//! Backup endpoint handlers.
//!
//! These handlers provide HTTP access to database backup operations:
//! - Export database to JSON
//! - Import database from JSON
//!
//! All endpoints enforce NAC permissions when NAC is enabled.
//! Export requires `DocumentRead` permission.
//! Import requires `DocumentUpdate` permission.
//!
//! Note: Export uses POST method with JSON body to match Go DefraDB behavior.
//! Go DefraDB writes to a file path specified in the request; this implementation
//! returns the data in the response body for HTTP-native usage.

use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{header, StatusCode},
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

/// Maximum number of collections that can be specified in export request.
const MAX_EXPORT_COLLECTIONS: usize = 100;

/// Request body for export (Go-compatible format).
#[derive(Debug, Clone, Deserialize)]
pub struct ExportRequest {
    /// Collections to export (if empty, exports all).
    #[serde(default)]
    pub collections: Vec<String>,
    /// Whether to pretty-print the JSON output.
    #[serde(default)]
    pub pretty: bool,
    /// Format for export (only "json" is supported).
    #[serde(default)]
    pub format: Option<String>,
    /// Filepath (Go-compatible, but not supported - see note below).
    /// Go DefraDB writes to this file path. This implementation returns
    /// data in the response body instead.
    #[serde(default)]
    pub filepath: Option<String>,
}

/// Request body for Go-compatible import.
/// Go DefraDB expects: `{"filepath": "/path/to/backup.json"}`
#[derive(Debug, Clone, Deserialize)]
pub struct GoImportRequest {
    /// File path to import from (Go format).
    pub filepath: Option<String>,
}

/// Export the database.
///
/// POST /api/v0/backup/export
///
/// Accepts JSON body with export configuration (Go-compatible format):
/// ```json
/// {
///   "collections": ["Users", "Posts"],
///   "pretty": true,
///   "format": "json"
/// }
/// ```
///
/// Note: Go DefraDB also accepts a "filepath" field and writes to disk.
/// This implementation ignores filepath and returns the data in the response body instead.
///
/// Requires `DocumentRead` permission when NAC is enabled.
pub async fn export(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(request): Json<ExportRequest>,
) -> Result<Response, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentRead).await?;

    let backup = state.require_backup()?;

    // Log warning if filepath was provided (Go-compatibility note)
    if let Some(ref filepath) = request.filepath {
        tracing::warn!(
            filepath = %filepath,
            "filepath parameter ignored - export data returned in response body instead of file"
        );
    }

    // Validate format if specified (only "json" is supported)
    if let Some(ref format) = request.format {
        if !format.eq_ignore_ascii_case("json") {
            return Err(HttpError::BadRequest(format!(
                "unsupported export format '{}': only 'json' is supported",
                format
            )));
        }
    }

    // Validate collection count limit
    if request.collections.len() > MAX_EXPORT_COLLECTIONS {
        return Err(HttpError::BadRequest(format!(
            "too many collections specified (max: {})",
            MAX_EXPORT_COLLECTIONS
        )));
    }

    // Validate collection names if provided
    for col in &request.collections {
        validate_collection_name(col)?;
    }

    let collections = if request.collections.is_empty() {
        None
    } else {
        Some(request.collections)
    };

    let data = backup
        .export(collections, request.pretty)
        .await
        .map_err(HttpError::Internal)?;

    // Return as JSON with appropriate content type
    // Go returns empty body (writes to file), but we return data in response
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(data))
        .map_err(|e| HttpError::Internal(e.to_string()))?;

    Ok(response)
}

/// Import the database.
///
/// POST /api/v0/backup/import
///
/// This endpoint supports two formats:
///
/// 1. **Go DefraDB format**: JSON body with "filepath" field
///    `{"filepath": "/path/to/backup.json"}`
///    Note: File-based import is NOT supported in this HTTP implementation.
///    Use the direct data format instead.
///
/// 2. **Direct data format**: Raw backup JSON in request body
///    `{"CollectionName": [{"_docID": "...", ...}]}`
///
/// Requires `DocumentUpdate` permission when NAC is enabled.
///
/// Returns HTTP 200 with empty body to match Go DefraDB behavior.
pub async fn import(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: Bytes,
) -> Result<StatusCode, HttpError> {
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
    let body_str = String::from_utf8(body.to_vec())
        .map_err(|_| HttpError::BadRequest("import data must be valid UTF-8".into()))?;

    if body_str.trim().is_empty() {
        return Err(HttpError::BadRequest("import data cannot be empty".into()));
    }

    // Try to parse as Go format first (filepath-based)
    if let Ok(go_request) = serde_json::from_str::<GoImportRequest>(&body_str) {
        if let Some(filepath) = go_request.filepath {
            // Go DefraDB expects file-based import, which we don't support
            return Err(HttpError::BadRequest(format!(
                "file-based import is not supported in HTTP mode. \
                 Go DefraDB requested filepath '{}'. \
                 Please send the backup data directly in the request body instead.",
                filepath
            )));
        }
    }

    // Validate that the body is valid JSON with expected structure
    let parsed: serde_json::Value = serde_json::from_str(&body_str)
        .map_err(|e| HttpError::BadRequest(format!("invalid JSON: {}", e)))?;

    // Backup data should be an object or array, not a primitive
    if !parsed.is_object() && !parsed.is_array() {
        return Err(HttpError::BadRequest(
            "backup data must be a JSON object or array".into(),
        ));
    }

    // Reject empty objects or arrays (but allow Go filepath format detection first)
    let is_empty = match &parsed {
        serde_json::Value::Object(obj) => {
            // Check if this looks like Go filepath format (has only filepath key)
            if obj.len() == 1 && obj.contains_key("filepath") {
                // Already handled above
                false
            } else {
                obj.is_empty()
            }
        }
        serde_json::Value::Array(arr) => arr.is_empty(),
        _ => false,
    };
    if is_empty {
        return Err(HttpError::BadRequest(
            "backup data is empty - nothing to import".into(),
        ));
    }

    let _result = backup
        .import(&body_str)
        .await
        .map_err(HttpError::BadRequest)?;

    // Return empty body to match Go DefraDB behavior
    Ok(StatusCode::OK)
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
    fn test_export_request_empty() {
        let request: ExportRequest = serde_json::from_str("{}").unwrap();
        assert!(request.collections.is_empty());
        assert!(!request.pretty);
        assert!(request.format.is_none());
    }

    #[test]
    fn test_export_request_with_collections() {
        let json = r#"{"collections": ["Users", "Posts"], "pretty": true}"#;
        let request: ExportRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.collections.len(), 2);
        assert!(request.pretty);
    }

    #[test]
    fn test_export_request_with_format() {
        let json = r#"{"collections": ["Users"], "format": "json"}"#;
        let request: ExportRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.format, Some("json".to_string()));
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
