//! Schema endpoint handlers.
//!
//! These handlers provide HTTP access to schema management:
//! - Add schema (POST /schema)
//!
//! All endpoints enforce NAC permissions when NAC is enabled.

use axum::{extract::State, http::HeaderMap, Json};

use crate::error::HttpError;
use crate::handlers::txn_header::txn_id_from_headers;
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};
use schema::CollectionVersion;

/// Add a schema (Go-compatible format).
///
/// POST /api/v0/schema
///
/// Body: SDL text (text/plain)
/// Returns: Array of created CollectionVersions
///
/// Requires `CollectionPatch` permission when NAC is enabled.
pub async fn add_schema(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    headers: HeaderMap,
    body: String,
) -> Result<Json<Vec<CollectionVersion>>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionPatch).await?;

    if let Some(txn_id) = txn_id_from_headers(&headers)? {
        let txn_ops = state.require_txn_ops()?;
        let collections = txn_ops
            .add_schema_in_txn(txn_id, &body)
            .await
            .map_err(HttpError::BadRequest)?;

        return Ok(Json(collections));
    }

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
