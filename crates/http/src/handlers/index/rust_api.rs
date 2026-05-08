//! Rust-native index endpoint handlers.
//!
//! Flat route pattern: /api/v0/index (collection in request body/query).

use axum::{
    extract::{Query, State},
    Json,
};
use serde::Deserialize;

use crate::error::{http_error_from_backend_message, HttpError};
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, IndexInfo, NodePermission};
use crate::validation::validate_identifier;

/// Request to create a new index.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateIndexRequest {
    pub collection: String,
    pub fields: Vec<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub unique: bool,
}

/// Query parameters for listing indexes.
#[derive(Debug, Clone, Deserialize)]
pub struct ListIndexesQuery {
    #[serde(default)]
    pub collection: Option<String>,
}

/// Query parameters for deleting an index.
#[derive(Debug, Clone, Deserialize)]
pub struct DeleteIndexQuery {
    pub collection: String,
    pub name: String,
}

/// Create a new index.
///
/// POST /api/v0/index
///
/// Requires `IndexCreate` permission when NAC is enabled.
pub async fn create_index(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Json(request): Json<CreateIndexRequest>,
) -> Result<Json<IndexInfo>, HttpError> {
    require_permission(&state, &identity, NodePermission::IndexCreate).await?;

    let index_ops = state.require_index()?;

    // Validate collection name
    validate_identifier(&request.collection).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            request.collection
        ))
    })?;

    if request.fields.is_empty() {
        return Err(HttpError::BadRequest(
            "at least one field is required".into(),
        ));
    }

    // Validate field names
    for field in &request.fields {
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
            &request.collection,
            request.fields,
            request.name.as_deref(),
            request.unique,
        )
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(Json(index))
}

/// List indexes, optionally filtered by collection.
///
/// GET /api/v0/index
///
/// Requires `IndexList` permission when NAC is enabled.
pub async fn list_indexes(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Query(query): Query<ListIndexesQuery>,
) -> Result<Json<Vec<IndexInfo>>, HttpError> {
    require_permission(&state, &identity, NodePermission::IndexList).await?;

    let index_ops = state.require_index()?;

    // Validate collection name if provided
    if let Some(ref col) = query.collection {
        validate_identifier(col).map_err(|_| {
            HttpError::BadRequest(format!(
                "invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
                col
            ))
        })?;
    }

    let indexes = index_ops
        .list_indexes(query.collection.as_deref())
        .await
        .map_err(HttpError::Internal)?;

    Ok(Json(indexes))
}

/// Delete an index.
///
/// DELETE /api/v0/index
///
/// Requires `IndexDelete` permission when NAC is enabled.
pub async fn delete_index(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Query(query): Query<DeleteIndexQuery>,
) -> Result<Json<()>, HttpError> {
    require_permission(&state, &identity, NodePermission::IndexDelete).await?;

    let index_ops = state.require_index()?;

    // Validate collection name
    validate_identifier(&query.collection).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            query.collection
        ))
    })?;

    // Validate index name
    validate_identifier(&query.name).map_err(|_| {
        HttpError::BadRequest(format!(
            "invalid index name '{}': must match [A-Za-z_][A-Za-z0-9_]*",
            query.name
        ))
    })?;

    index_ops
        .delete_index(&query.collection, &query.name)
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(Json(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::IndexFieldInfo;

    #[test]
    fn test_create_index_request_deserialize() {
        let json = r#"{"collection": "Users", "fields": ["name", "email"], "name": "idx_name_email", "unique": true}"#;
        let request: CreateIndexRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.collection, "Users");
        assert_eq!(request.fields.len(), 2);
        assert_eq!(request.name, Some("idx_name_email".to_string()));
        assert!(request.unique);
    }

    #[test]
    fn test_create_index_request_minimal() {
        let json = r#"{"collection": "Users", "fields": ["name"]}"#;
        let request: CreateIndexRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.collection, "Users");
        assert_eq!(request.fields.len(), 1);
        assert!(request.name.is_none());
        assert!(!request.unique);
    }

    #[test]
    fn test_list_indexes_query_empty() {
        let query: ListIndexesQuery = serde_json::from_str("{}").unwrap();
        assert!(query.collection.is_none());
    }

    #[test]
    fn test_list_indexes_query_with_collection() {
        let json = r#"{"collection": "Users"}"#;
        let query: ListIndexesQuery = serde_json::from_str(json).unwrap();
        assert_eq!(query.collection, Some("Users".to_string()));
    }

    #[test]
    fn test_delete_index_query() {
        let json = r#"{"collection": "Users", "name": "idx_name"}"#;
        let query: DeleteIndexQuery = serde_json::from_str(json).unwrap();
        assert_eq!(query.collection, "Users");
        assert_eq!(query.name, "idx_name");
    }

    #[test]
    fn test_index_info_serialize() {
        let info = IndexInfo {
            name: "idx_name".to_string(),
            collection: "Users".to_string(),
            fields: vec![
                IndexFieldInfo {
                    name: "name".to_string(),
                    direction: Some("ASC".to_string()),
                },
                IndexFieldInfo {
                    name: "email".to_string(),
                    direction: None,
                },
            ],
            unique: true,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("idx_name"));
        assert!(json.contains("Users"));
        assert!(json.contains("name"));
        assert!(json.contains("email"));
        assert!(json.contains("unique"));
    }
}
