//! Schema endpoint handlers.
//!
//! These handlers provide HTTP access to schema management:
//! - Add schema (POST /schema)

use axum::{extract::State, Json};

use crate::error::HttpError;
use crate::router::AppState;
use schema::CollectionVersion;

/// Add a schema (Go-compatible format).
///
/// POST /api/v0/schema
///
/// Body: SDL text (text/plain)
/// Returns: Array of created CollectionVersions
pub async fn add_schema(
    State(state): State<AppState>,
    body: String,
) -> Result<Json<Vec<CollectionVersion>>, HttpError> {
    let schema_ops = state.require_schema()?;

    let collections = schema_ops
        .add_schema(&body)
        .await
        .map_err(HttpError::BadRequest)?;

    Ok(Json(collections))
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_schema_handler_exists() {
        // Handler compiles correctly
    }
}
