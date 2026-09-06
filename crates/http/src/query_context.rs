//! Query execution with thread-local signing and DAC bypass context.
//!
//! HTTP handlers must set `signing_config` and `dac_bypass` thread-locals
//! before executing queries to match FFI behavior. Because tokio's
//! work-stealing scheduler can move tasks between OS threads at `.await`
//! points, we pin execution to a single thread using `spawn_blocking` +
//! `Handle::block_on` — the same pattern the FFI uses with `rt.block_on()`.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use query::executor::{QueryExecutor, QueryRequest, QueryResponse};
use query::txn::TransactionHandle;
use rand::Rng;

use crate::identity_extractor::ExtractIdentity;
use crate::router::AppState;

const INITIAL_RETRY_BACKOFF: Duration = Duration::from_millis(10);
const MAX_RETRY_BACKOFF: Duration = Duration::from_millis(250);

/// Execute a GraphQL query with signing config and DAC bypass context.
pub async fn execute_with_context(
    state: &AppState,
    identity: &ExtractIdentity,
    request: QueryRequest,
) -> QueryResponse {
    execute_request_with_retry_loop(
        request,
        state.max_txn_retries,
        INITIAL_RETRY_BACKOFF,
        MAX_RETRY_BACKOFF,
        |request| execute_once_with_context(state, identity, request),
    )
    .await
}

async fn execute_once_with_context(
    state: &AppState,
    identity: &ExtractIdentity,
    request: QueryRequest,
) -> QueryResponse {
    let signing_config = resolve_signing_config(state, identity);

    let dac_bypass = resolve_dac_bypass(state, identity).await;

    // Fast path: when there is nothing to put on the thread-locals, skip
    // spawn_blocking entirely.
    if signing_config.is_none() && state.nac.is_none() && !dac_bypass {
        return state.executor.execute(request).await;
    }
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

async fn execute_request_with_retry_loop<F, Fut>(
    request: QueryRequest,
    max_retries: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    mut execute_once: F,
) -> QueryResponse
where
    F: FnMut(QueryRequest) -> Fut,
    Fut: Future<Output = QueryResponse>,
{
    let mut retry_count = 0;
    let mut backoff = initial_backoff.min(max_backoff);

    loop {
        let response = execute_once(request.clone()).await;
        if !response.is_transaction_conflict() {
            if retry_count > 0 {
                telemetry::record_retry_success(telemetry::RetryLayer::HttpAutoCommit);
            }
            return response;
        }
        if retry_count >= max_retries {
            telemetry::record_retry_exhaustion(telemetry::RetryLayer::HttpAutoCommit);
            return response;
        }

        telemetry::record_retry_attempt(telemetry::RetryLayer::HttpAutoCommit);
        retry_count += 1;
        let delay = jittered_backoff(backoff);
        tracing::warn!(
            retry = retry_count,
            max_retries,
            backoff_ms = delay.as_millis(),
            "retrying auto-commit GraphQL request after transaction conflict"
        );
        tokio::time::sleep(delay).await;
        backoff = next_retry_backoff(backoff, max_backoff);
    }
}

fn jittered_backoff(max_backoff: Duration) -> Duration {
    if max_backoff.is_zero() {
        return Duration::ZERO;
    }

    let max_nanos = max_backoff.as_nanos().min(u64::MAX as u128) as u64;
    Duration::from_nanos(rand::thread_rng().gen_range(0..=max_nanos))
}

fn next_retry_backoff(current: Duration, max_backoff: Duration) -> Duration {
    current.saturating_mul(2).min(max_backoff)
}

/// Execute a GraphQL query within a transaction with signing config and DAC bypass context.
pub async fn execute_in_txn_with_context(
    state: &AppState,
    identity: &ExtractIdentity,
    request: QueryRequest,
    txn_handle: TransactionHandle,
) -> QueryResponse {
    let signing_config = resolve_signing_config(state, identity);

    let dac_bypass = resolve_dac_bypass(state, identity).await;

    // Fast path: see the note in `execute_once_with_context`.
    if signing_config.is_none() && state.nac.is_none() && !dac_bypass {
        return state.executor.execute_in_txn(request, &txn_handle).await;
    }

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
/// - If NAC is configured and the identity has `DacBypass` permission, bypass.
/// - Otherwise, no bypass.
pub(crate) async fn resolve_dac_bypass(state: &AppState, identity: &ExtractIdentity) -> bool {
    let Some(nac) = &state.nac else {
        return false;
    };

    let status = nac.get_status().await;
    acp::nac::should_bypass_dac(status, identity.did(), |d, p| async move {
        nac.check_permission(d, p).await.unwrap_or(false)
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use query::error::{Result, TransactionError};

    use super::*;
    use crate::router::AppStateBuilder;

    #[derive(Default)]
    struct ConflictExecutor {
        auto_commit_attempts: AtomicUsize,
        explicit_txn_attempts: AtomicUsize,
    }

    #[async_trait]
    impl QueryExecutor for ConflictExecutor {
        fn abandon_txn(&self, _handle: &TransactionHandle) {}

        async fn execute(&self, _request: QueryRequest) -> QueryResponse {
            if self.auto_commit_attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                QueryResponse::transaction_conflict("transaction conflict")
            } else {
                QueryResponse::success(serde_json::json!({"_docID": "doc"}))
            }
        }

        async fn execute_in_txn(
            &self,
            _request: QueryRequest,
            _handle: &TransactionHandle,
        ) -> QueryResponse {
            self.explicit_txn_attempts.fetch_add(1, Ordering::SeqCst);
            QueryResponse::transaction_conflict("transaction conflict")
        }

        async fn begin_txn(
            &self,
            _readonly: bool,
        ) -> std::result::Result<TransactionHandle, TransactionError> {
            Err(TransactionError::not_supported("test executor"))
        }

        async fn commit_txn(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            Err(TransactionError::not_supported("test executor"))
        }

        async fn rollback_txn(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            Err(TransactionError::not_supported("test executor"))
        }

        async fn schema(&self) -> Result<String> {
            Ok(String::new())
        }
    }

    #[tokio::test]
    async fn auto_commit_execution_retries_typed_conflicts() {
        let metrics_before = telemetry::conflict_metrics_snapshot().http_auto_commit;
        let executor = Arc::new(ConflictExecutor::default());
        let state = AppStateBuilder::new(executor.clone())
            .with_max_txn_retries(3)
            .build();

        let response = execute_with_context(
            &state,
            &ExtractIdentity::anonymous(),
            QueryRequest::new("mutation { create_User(input: {}) { _docID } }"),
        )
        .await;

        assert_eq!(executor.auto_commit_attempts.load(Ordering::SeqCst), 3);
        assert!(!response.has_errors());
        let metrics_after = telemetry::conflict_metrics_snapshot().http_auto_commit;
        assert_eq!(metrics_after.attempts - metrics_before.attempts, 2);
        assert_eq!(metrics_after.successes - metrics_before.successes, 1);
    }

    #[tokio::test]
    async fn explicit_transaction_execution_is_not_retried() {
        let executor = Arc::new(ConflictExecutor::default());
        let state = AppStateBuilder::new(executor.clone())
            .with_max_txn_retries(3)
            .build();

        let response = execute_in_txn_with_context(
            &state,
            &ExtractIdentity::anonymous(),
            QueryRequest::new("mutation { update_User(input: {}) { _docID } }"),
            TransactionHandle::new("txn".to_string()),
        )
        .await;

        assert_eq!(executor.explicit_txn_attempts.load(Ordering::SeqCst), 1);
        assert!(response.is_transaction_conflict());
    }

    const OWNER_DID: &str = "did:key:z6MkTestNodeOwner";

    /// Records the `dac_bypass` thread-local as observed by the executor, which
    /// is the only place the fast path's effect is visible.
    #[derive(Default)]
    struct DacBypassRecorder {
        observed: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl QueryExecutor for DacBypassRecorder {
        fn abandon_txn(&self, _handle: &TransactionHandle) {}

        async fn execute(&self, _request: QueryRequest) -> QueryResponse {
            self.observed
                .store(defra_core::dac_bypass::get_dac_bypass(), Ordering::SeqCst);
            QueryResponse::success(serde_json::json!({}))
        }

        async fn execute_in_txn(
            &self,
            _request: QueryRequest,
            _handle: &TransactionHandle,
        ) -> QueryResponse {
            self.observed
                .store(defra_core::dac_bypass::get_dac_bypass(), Ordering::SeqCst);
            QueryResponse::success(serde_json::json!({}))
        }

        async fn begin_txn(
            &self,
            _readonly: bool,
        ) -> std::result::Result<TransactionHandle, TransactionError> {
            Err(TransactionError::not_supported("test executor"))
        }

        async fn commit_txn(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            Err(TransactionError::not_supported("test executor"))
        }

        async fn rollback_txn(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            Err(TransactionError::not_supported("test executor"))
        }

        async fn schema(&self) -> Result<String> {
            Ok(String::new())
        }
    }

    fn owner_identity() -> ExtractIdentity {
        ExtractIdentity::from_did(Some(identity::Did::new_unchecked(OWNER_DID.to_string())))
    }

    #[tokio::test]
    async fn node_owner_is_not_promoted_to_nac_dac_bypass() {
        let executor = Arc::new(DacBypassRecorder::default());
        let state = AppStateBuilder::new(executor.clone())
            .with_node_identity_did(OWNER_DID.to_string())
            .with_signing_enabled(false)
            .build();

        execute_once_with_context(
            &state,
            &owner_identity(),
            QueryRequest::new("{ __typename }"),
        )
        .await;

        assert!(!executor.observed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_stranger_gets_no_dac_bypass() {
        let executor = Arc::new(DacBypassRecorder::default());
        let state = AppStateBuilder::new(executor.clone())
            .with_node_identity_did(OWNER_DID.to_string())
            .with_signing_enabled(false)
            .build();

        execute_once_with_context(
            &state,
            &ExtractIdentity::from_did(Some(identity::Did::new_unchecked(
                "did:key:z6MkSomeoneElse".to_string(),
            ))),
            QueryRequest::new("{ __typename }"),
        )
        .await;

        assert!(!executor.observed.load(Ordering::SeqCst));
    }
}
