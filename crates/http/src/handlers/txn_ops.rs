//! Transaction-scoped operation handlers.
//!
//! These handlers provide HTTP access to operations that execute within
//! an existing transaction (e.g., setting migrations, reading collections
//! including uncommitted writes).
//!
//! All endpoints enforce NAC permissions when NAC is enabled.

use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;

use crate::error::{http_error_from_backend_message, HttpError};
use crate::identity_extractor::ExtractIdentity;
use crate::nac_guard::require_permission;
use crate::router::{AppState, NodePermission};

/// Path parameter for transaction-scoped routes.
#[derive(Debug, Deserialize)]
pub struct TxnPathParam {
    pub id: String,
}

/// Request body for setting a migration in a transaction.
#[derive(Debug, Deserialize)]
pub struct SetMigrationInTxnRequest {
    #[serde(rename = "Config", alias = "config")]
    pub config: String,
}

/// Set a lens migration within an existing transaction.
///
/// POST /api/v0/tx/{id}/lens
///
/// The transaction must have been started via `POST /api/v0/tx`.
/// The migration is registered within the transaction and only becomes
/// visible after the transaction is committed.
///
/// Requires `CollectionPatch` permission when NAC is enabled.
pub async fn set_migration_in_txn(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(params): Path<TxnPathParam>,
    Json(body): Json<SetMigrationInTxnRequest>,
) -> Result<Json<serde_json::Value>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionPatch).await?;

    let txn_ops = state.require_txn_ops()?;

    let txn_id = params.id.clone();

    let transform_id = txn_ops
        .set_migration_in_txn(&txn_id, &body.config)
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(Json(serde_json::json!({ "lensId": transform_id })))
}

/// Get all collection versions visible within a transaction.
///
/// GET /api/v0/tx/{id}/collections
///
/// Returns collection versions including any uncommitted writes made
/// within the specified transaction.
///
/// Requires `CollectionGet` permission when NAC is enabled.
pub async fn get_collections_in_txn(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(params): Path<TxnPathParam>,
) -> Result<Json<Vec<schema::CollectionVersion>>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionGet).await?;

    let txn_ops = state.require_txn_ops()?;

    let txn_id = params.id.clone();

    let collections = txn_ops
        .get_collections_in_txn(&txn_id)
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(Json(collections))
}

/// Add a schema within an existing transaction.
///
/// POST /api/v0/tx/{id}/schema
///
/// Body: SDL text (text/plain)
/// Returns: Array of created CollectionVersions
///
/// The collections are created within the transaction and only become
/// visible globally after the transaction is committed. Queries within
/// the same transaction can use the new collections immediately.
///
/// Requires `CollectionPatch` permission when NAC is enabled.
pub async fn add_schema_in_txn(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    Path(params): Path<TxnPathParam>,
    body: String,
) -> Result<Json<Vec<schema::CollectionVersion>>, HttpError> {
    require_permission(&state, &identity, NodePermission::CollectionPatch).await?;

    let txn_ops = state.require_txn_ops()?;

    let txn_id = params.id.clone();

    let collections = txn_ops
        .add_schema_in_txn(&txn_id, &body)
        .await
        .map_err(http_error_from_backend_message)?;

    Ok(Json(collections))
}
