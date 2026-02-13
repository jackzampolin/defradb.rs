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
use crate::router::{AppState, NodePermission};

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

    tokio::task::spawn_blocking(move || {
        defra_core::signing::set_signing_config(signing_config);
        defra_core::dac_bypass::set_dac_bypass(dac_bypass);
        handle.block_on(async { executor.execute(request).await })
    })
    .await
    .expect("query execution task panicked")
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

    tokio::task::spawn_blocking(move || {
        defra_core::signing::set_signing_config(signing_config);
        defra_core::dac_bypass::set_dac_bypass(dac_bypass);
        handle.block_on(async { executor.execute_in_txn(request, &txn_handle).await })
    })
    .await
    .expect("query execution task panicked")
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
) -> QueryResponse {
    let handle = tokio::runtime::Handle::current();

    tokio::task::spawn_blocking(move || {
        defra_core::signing::set_signing_config(signing_config);
        defra_core::dac_bypass::set_dac_bypass(dac_bypass);
        handle.block_on(async { executor.execute(request).await })
    })
    .await
    .expect("query execution task panicked")
}

/// Resolve the signing config for a request identity.
///
/// Matches FFI logic in `exec.rs:63-113`:
/// - If identity has a DID, look up its signing config
/// - If no DID (anonymous), fall back to the node identity DID
pub(crate) fn resolve_signing_config(
    state: &AppState,
    identity: &ExtractIdentity,
) -> Option<defra_core::signing::SigningConfig> {
    match identity.did() {
        Some(did) => {
            let did_str = did.as_str();
            if let Some(config) = defra_core::signing::get_identity(did_str) {
                tracing::debug!(did = %did_str, "using explicit identity signing config");
                Some(config)
            } else {
                tracing::debug!(did = %did_str, "no signing config found for explicit DID");
                None
            }
        }
        None => {
            // Fall back to node identity (matches FFI null/empty identity path)
            if let Some(ref node_did) = state.node_identity_did {
                let config = defra_core::signing::get_identity(node_did);
                tracing::debug!(
                    node_did = %node_did,
                    present = config.is_some(),
                    "anonymous request, falling back to node identity signing config"
                );
                config
            } else {
                None
            }
        }
    }
}

/// Resolve whether DAC bypass should be enabled for this request.
///
/// Matches FFI logic in `query/mod.rs:49-84`:
/// - If NAC is not configured or not enabled, no bypass
/// - If identity has `DacBypass` permission, enable bypass
pub(crate) async fn resolve_dac_bypass(state: &AppState, identity: &ExtractIdentity) -> bool {
    let Some(nac) = &state.nac else {
        return false;
    };

    // Only bypass when NAC is enabled
    let status = nac.get_status().await;
    if status != acp::nac::NacStatus::Enabled {
        return false;
    }

    let Some(did) = identity.did() else {
        return false;
    };

    nac.check_permission(did, NodePermission::DacBypass)
        .await
        .unwrap_or(false)
}
