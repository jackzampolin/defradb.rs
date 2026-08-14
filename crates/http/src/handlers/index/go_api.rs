//! Go-compatible index endpoint handlers.
//!
//! Route pattern: /api/v0/collections/{name}/indexes (collection in path).

use std::collections::HashMap;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use defra_core::{ActionExecution, ActionStatus};
use serde::{Deserialize, Serialize};

use crate::error::{http_error_from_backend_message, HttpError};
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};
use crate::validation::validate_identifier;

/// Go-compatible request to create an index.
/// Collection is provided in the URL path, not the body.
#[derive(Debug, Clone, Deserialize)]
pub struct GoCreateIndexRequest {
    /// Index name (optional, auto-generated if not provided).
    #[serde(rename = "Name", default)]
    pub name: Option<String>,
    /// Fields to index.
    #[serde(rename = "Fields")]
    pub fields: Vec<GoIndexedFieldDescription>,
    /// Whether to create a unique index.
    #[serde(rename = "Unique", default)]
    pub unique: bool,
    /// Vector index config. Present iff this is a vector index request.
    #[serde(rename = "Vector", default)]
    pub vector: Option<schema::VectorIndexDescription>,
}

/// Go-compatible indexed field description.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GoIndexedFieldDescription {
    /// Field name.
    #[serde(rename = "Name")]
    pub name: String,
    /// Sort order (true = descending, false = ascending).
    #[serde(rename = "Descending", default)]
    pub descending: bool,
}

/// Go-compatible index description response.
#[derive(Debug, Clone, Serialize)]
pub struct GoIndexDescription {
    /// Index name.
    #[serde(rename = "Name")]
    pub name: String,
    /// Index ID (local identifier).
    #[serde(rename = "ID")]
    pub id: u32,
    /// Indexed fields.
    #[serde(rename = "Fields")]
    pub fields: Vec<GoIndexedFieldDescription>,
    /// Whether the index enforces uniqueness.
    #[serde(rename = "Unique")]
    pub unique: bool,
    /// Kind-specific config, omitted for an ordinary index.
    #[serde(rename = "Kind", skip_serializing_if = "Option::is_none")]
    pub kind: Option<schema::IndexKind>,
}

/// Go v1 index description paired with its lifecycle state.
#[derive(Debug, Clone, Serialize)]
pub struct GoListIndexesResult {
    #[serde(rename = "Description")]
    pub description: GoIndexDescription,
    #[serde(rename = "Execution")]
    pub execution: ActionExecution,
}

fn ready_index(index: crate::router::IndexInfo) -> GoListIndexesResult {
    let execution = ActionExecution {
        collection_id: index.collection_id.clone(),
        subject: index.id.to_string(),
        status: ActionStatus::COMPLETED,
        ..Default::default()
    };
    let description = GoIndexDescription {
        kind: index.kind,
        name: index.name,
        id: index.id,
        fields: index
            .fields
            .into_iter()
            .map(|field| GoIndexedFieldDescription {
                name: field.name,
                descending: field.direction.as_deref() == Some("DESC"),
            })
            .collect(),
        unique: index.unique,
    };
    GoListIndexesResult {
        description,
        execution,
    }
}

/// Create an index (Go-compatible route).
///
/// POST /api/v0/collections/{name}/indexes
///
/// Requires `IndexCreate` permission when NAC is enabled.
pub async fn go_create_index(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(collection): Path<String>,
    Json(request): Json<GoCreateIndexRequest>,
) -> Result<Json<GoIndexDescription>, HttpError> {
    require_permission(&state, &identity, NodePermission::IndexCreate).await?;

    let index_ops = state.require_index()?;

    // Validate collection name
    validate_identifier(&collection).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            collection
        ))
    })?;

    if request.fields.is_empty() {
        return Err(HttpError::BadRequest(
            "at least one field is required".into(),
        ));
    }

    // Extract field names and validate
    let field_names: Vec<String> = request.fields.iter().map(|f| f.name.clone()).collect();
    for field in &field_names {
        validate_identifier(field).map_err(|_| {
            HttpError::BadRequest(format!(
                "invalid field name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
                field
            ))
        })?;
    }

    // Validate index name if provided
    if let Some(ref name) = request.name {
        validate_identifier(name).map_err(|_| {
            HttpError::BadRequest(format!(
                "invalid index name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
                name
            ))
        })?;
    }

    let index = index_ops
        .create_index(
            &collection,
            field_names,
            request.name.as_deref(),
            request.unique,
            request.vector,
        )
        .await
        .map_err(http_error_from_backend_message)?;

    // Convert to Go-compatible response format
    let response = GoIndexDescription {
        name: index.name,
        id: index.id,
        fields: request.fields,
        unique: index.unique,
        kind: index.kind,
    };

    Ok(Json(response))
}

/// List indexes for a collection (Go-compatible route).
///
/// GET /api/v0/collections/{name}/indexes
///
/// Requires `IndexList` permission when NAC is enabled.
pub async fn go_list_indexes(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(collection): Path<String>,
) -> Result<Json<Vec<GoListIndexesResult>>, HttpError> {
    require_permission(&state, &identity, NodePermission::IndexList).await?;

    let index_ops = state.require_index()?;

    // Validate collection name
    validate_identifier(&collection).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            collection
        ))
    })?;

    let indexes = index_ops
        .list_indexes(Some(&collection))
        .await
        .map_err(HttpError::Internal)?;

    // Convert to Go-compatible response format
    let response = indexes.into_iter().map(ready_index).collect();

    Ok(Json(response))
}

/// Delete an index (Go-compatible route).
///
/// DELETE /api/v0/collections/{name}/indexes/{index}
///
/// Requires `IndexDelete` permission when NAC is enabled.
/// Returns HTTP 200 with empty body to match Go DefraDB behavior.
pub async fn go_delete_index(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path((collection, index_name)): Path<(String, String)>,
) -> Result<StatusCode, HttpError> {
    require_permission(&state, &identity, NodePermission::IndexDelete).await?;

    let index_ops = state.require_index()?;

    // Validate collection name
    validate_identifier(&collection).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            collection
        ))
    })?;

    // Validate index name
    validate_identifier(&index_name).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid index name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            index_name
        ))
    })?;

    index_ops
        .delete_index(&collection, &index_name)
        .await
        .map_err(http_error_from_backend_message)?;

    // Return empty body to match Go DefraDB behavior
    Ok(StatusCode::OK)
}

/// List all indexes across all collections (Go-compatible route).
///
/// GET /api/v0/collections/indexes
///
/// Returns a map grouped by collection name to match Go DefraDB format.
pub async fn go_list_all_indexes(
    State(state): State<AppState>,
    identity: ExtractIdentity,
) -> Result<Json<HashMap<String, Vec<GoListIndexesResult>>>, HttpError> {
    require_permission(&state, &identity, NodePermission::IndexList).await?;

    let index_ops = state.require_index()?;

    let indexes = index_ops
        .list_indexes(None)
        .await
        .map_err(HttpError::Internal)?;

    let mut grouped: HashMap<String, Vec<GoListIndexesResult>> = HashMap::new();
    for idx in indexes {
        let collection = idx.collection.clone();
        grouped
            .entry(collection)
            .or_default()
            .push(ready_index(idx));
    }

    Ok(Json(grouped))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_go_create_index_request_deserialize() {
        let json = r#"{"Name": "idx_email", "Fields": [{"Name": "email", "Descending": false}], "Unique": true}"#;
        let request: GoCreateIndexRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.name, Some("idx_email".to_string()));
        assert_eq!(request.fields.len(), 1);
        assert_eq!(request.fields[0].name, "email");
        assert!(!request.fields[0].descending);
        assert!(request.unique);
    }

    #[test]
    fn test_go_create_index_request_minimal() {
        let json = r#"{"Fields": [{"Name": "name"}]}"#;
        let request: GoCreateIndexRequest = serde_json::from_str(json).unwrap();
        assert!(request.name.is_none());
        assert_eq!(request.fields.len(), 1);
        assert!(!request.unique);
    }

    #[test]
    fn test_go_index_description_serialize() {
        let desc = GoIndexDescription {
            kind: None,
            name: "idx_email".to_string(),
            id: 1,
            fields: vec![GoIndexedFieldDescription {
                name: "email".to_string(),
                descending: false,
            }],
            unique: true,
        };
        let json = serde_json::to_string(&desc).unwrap();
        assert!(json.contains("\"Name\":\"idx_email\""));
        assert!(json.contains("\"ID\":1"));
        assert!(json.contains("\"Fields\""));
        assert!(json.contains("\"Unique\":true"));
    }
}
