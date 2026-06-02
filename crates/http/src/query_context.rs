//! Query execution with thread-local signing and DAC bypass context.
//!
//! HTTP handlers must set `signing_config` and `dac_bypass` thread-locals
//! before executing queries to match FFI behavior. Because tokio's
//! work-stealing scheduler can move tasks between OS threads at `.await`
//! points, we pin execution to a single thread using `spawn_blocking` +
//! `Handle::block_on` — the same pattern the FFI uses with `rt.block_on()`.

use std::sync::Arc;

use query::executor::{QueryExecutor, QueryRequest, QueryResponse};
use query::txn::TransactionHandle;

use crate::identity_extractor::ExtractIdentity;
use crate::router::AppState;

/// Execute a GraphQL query with signing config and DAC bypass context.
pub async fn execute_with_context(
    state: &AppState,
    identity: &ExtractIdentity,
    request: QueryRequest,
) -> QueryResponse {
    let signing_config = resolve_signing_config(state, identity);

    // Fast path: when no signing and no NAC, skip spawn_blocking entirely.
    // Thread-local defaults are None/false, matching what we'd set.
    if signing_config.is_none() && state.nac.is_none() {
        return state.executor.execute(request).await;
    }

    let dac_bypass = resolve_dac_bypass(state, identity).await;
    let executor = state.executor.clone();
    let handle = tokio::runtime::Handle::current();

    let batch_session_key = signing_config.as_ref().map(|s| s.public_key_hex.clone());
    let acting_did = identity.did().map(|d| d.as_str().to_string());

    match tokio::task::spawn_blocking(move || {
        let _identity_guard = defra_core::current_identity::scoped_current_identity(acting_did);
        defra_core::signing::set_signing_config(signing_config);
        defra_core::batch_signing::set_batch_session_key(batch_session_key);
        defra_core::dac_bypass::set_dac_bypass(dac_bypass);
        handle.block_on(async { executor.execute(request).await })
    })
    .await
    {
        Ok(response) => response,
        Err(join_err) => QueryResponse::error(format!("query execution task failed: {join_err}")),
    }
}

/// Execute a GraphQL query within a transaction with signing config and DAC bypass context.
pub async fn execute_in_txn_with_context(
    state: &AppState,
    identity: &ExtractIdentity,
    request: QueryRequest,
    txn_handle: TransactionHandle,
) -> QueryResponse {
    let signing_config = resolve_signing_config(state, identity);

    // Fast path: when no signing and no NAC, skip spawn_blocking entirely.
    if signing_config.is_none() && state.nac.is_none() {
        return state.executor.execute_in_txn(request, &txn_handle).await;
    }

    let dac_bypass = resolve_dac_bypass(state, identity).await;

    let executor = state.executor.clone();
    let handle = tokio::runtime::Handle::current();

    let batch_session_key = signing_config.as_ref().map(|s| s.public_key_hex.clone());
    let acting_did = identity.did().map(|d| d.as_str().to_string());

    match tokio::task::spawn_blocking(move || {
        let _identity_guard = defra_core::current_identity::scoped_current_identity(acting_did);
        defra_core::signing::set_signing_config(signing_config);
        defra_core::batch_signing::set_batch_session_key(batch_session_key);
        defra_core::dac_bypass::set_dac_bypass(dac_bypass);
        handle.block_on(async { executor.execute_in_txn(request, &txn_handle).await })
    })
    .await
    {
        Ok(response) => response,
        Err(join_err) => QueryResponse::error(format!("query execution task failed: {join_err}")),
    }
}

/// Execute a query inside `spawn_blocking` with pre-resolved context values.
///
/// Used by SSE streams where signing config and DAC bypass are resolved once
/// at subscription setup time, then reused for each per-event query.
pub async fn execute_with_resolved_context(
    executor: Arc<dyn QueryExecutor>,
    request: QueryRequest,
    signing_config: Option<defra_core::signing::SigningConfig>,
    dac_bypass: bool,
    acting_did: Option<String>,
) -> QueryResponse {
    let handle = tokio::runtime::Handle::current();

    let batch_session_key = signing_config.as_ref().map(|s| s.public_key_hex.clone());

    match tokio::task::spawn_blocking(move || {
        let _identity_guard = defra_core::current_identity::scoped_current_identity(acting_did);
        defra_core::signing::set_signing_config(signing_config);
        defra_core::batch_signing::set_batch_session_key(batch_session_key);
        defra_core::dac_bypass::set_dac_bypass(dac_bypass);
        handle.block_on(async { executor.execute(request).await })
    })
    .await
    {
        Ok(response) => response,
        Err(join_err) => QueryResponse::error(format!("query execution task failed: {join_err}")),
    }
}

/// Resolve the signing config for a request identity.
///
/// - If identity has a DID, look up its signing config.
/// - If no DID (anonymous), fall back to the node identity DID.
pub(crate) fn resolve_signing_config(
    state: &AppState,
    identity: &ExtractIdentity,
) -> Option<defra_core::signing::SigningConfig> {
    defra_core::signing::resolve_signing_config_with_flag(
        identity.did().map(|d| d.as_str()),
        state.node_identity_did.as_deref(),
        state.signing_enabled,
    )
}

/// Resolve whether DAC bypass should be enabled for this request.
///
/// - If the request identity equals the configured node identity, bypass.
///   (Matches Go's `internal/db/collection_acp.go:60-62` — the process owner
///   gets full access to all documents regardless of DAC.)
/// - If NAC is configured and the identity has `DacBypass` permission, bypass.
/// - Otherwise, no bypass.
pub(crate) async fn resolve_dac_bypass(state: &AppState, identity: &ExtractIdentity) -> bool {
    // Node-identity full-access shortcut.
    if let (Some(node_did), Some(req_did)) = (&state.node_identity_did, identity.did()) {
        if node_did.as_str() == req_did.as_str() {
            return true;
        }
    }

    let Some(nac) = &state.nac else {
        return false;
    };

    let status = nac.get_status().await;
    acp::nac::should_bypass_dac(status, identity.did(), |d, p| async move {
        nac.check_permission(d, p).await.unwrap_or(false)
    })
    .await
}
