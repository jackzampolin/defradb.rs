//! Benchmarks that separate HTTP transport overhead from query execution.
//!
//! Every benchmark drives the *same* trivial workload - a stub executor that
//! returns a canned response and does no database work - so whatever they
//! measure is the API layer itself rather than the query engine.
//!
//! Two questions are answered here:
//!
//! 1. **What does the transport cost?** `executor_direct` calls the executor
//!    straight through; `router_bare` goes through the axum router, the
//!    GraphQL handler and JSON codec; `router_with_middleware` adds the full
//!    production middleware stack (CORS, trace, timeout, concurrency limit,
//!    body limit, auth). The deltas attribute cost to each layer.
//!
//! 2. **What does `spawn_blocking` cost?** `crates/http/src/query_context.rs`
//!    takes a fast path that awaits the executor directly when no signing
//!    config and no NAC are configured, and otherwise hops the request onto a
//!    blocking thread (`spawn_blocking` + `Handle::block_on`) so that
//!    thread-locals stay pinned. Default deployments take the fast path, so
//!    the cost of the slow path has never been measured. `query_context_*`
//!    runs both branches on an identical workload.

use std::hint::black_box;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use serde_json::json;
use tower::ServiceExt;

use crypto::{Key, PrivateKey};
use defra_core::signing::{SigningConfig, SigningKeyType};
use defra_http::mock::MockNodeAcpOperations;
use defra_http::{create_router_with_state, AppStateBuilder, Server};
use query::executor::{QueryExecutor, QueryRequest, QueryResponse};
use query::txn::TransactionHandle;
use query::TransactionError;

/// The GraphQL endpoint under the v1 API prefix.
const GRAPHQL_URI: &str = "/api/v1/graphql";

/// A syntactically valid request. The handler parses it (twice: once for the
/// encrypted-field check and once to derive the required NAC permission)
/// before it ever reaches the executor, so it must be real GraphQL.
const QUERY: &str = "query { Users { name age } }";

/// Response body limit when draining the router's response.
const BODY_LIMIT: usize = 1024 * 1024;

/// A query executor that does no work at all.
///
/// The point of these benchmarks is the cost of everything *around* execution,
/// so the executor must be as close to free as possible. The crate's
/// `MockQueryExecutor` lowercases and substring-matches the query, which is
/// real work that would show up in the transport delta.
#[derive(Debug, Default)]
struct NoopExecutor;

#[async_trait]
impl QueryExecutor for NoopExecutor {
    async fn execute(&self, _request: QueryRequest) -> QueryResponse {
        QueryResponse::success(json!({ "Users": [] }))
    }

    async fn execute_in_txn(
        &self,
        _request: QueryRequest,
        _handle: &TransactionHandle,
    ) -> QueryResponse {
        QueryResponse::success(json!({ "Users": [] }))
    }

    async fn begin_txn(
        &self,
        _readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        Ok(TransactionHandle::new("bench-txn".to_string()))
    }

    async fn commit_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Ok(())
    }

    async fn rollback_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Ok(())
    }

    async fn schema(&self) -> query::Result<String> {
        Ok(String::new())
    }
}

/// A multi-threaded runtime is required, not merely preferred: the slow path
/// calls `Handle::block_on` inside `spawn_blocking`, which deadlocks on a
/// current-thread runtime.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime should build")
}

fn executor() -> Arc<dyn QueryExecutor> {
    Arc::new(NoopExecutor)
}

/// Builds the POST request the router is driven with. A fresh request is built
/// per iteration because the body is consumed; that is what a real client pays
/// too.
fn graphql_request() -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(GRAPHQL_URI)
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({ "query": QUERY })).expect("body should serialize"),
        ))
        .expect("request should build")
}

/// Drives one request through the router and drains the response.
///
/// The router is cloned per iteration because `oneshot` consumes the service.
/// Cloning an axum `Router` is a refcount bump, not a rebuild.
async fn drive(router: &Router) -> StatusCode {
    let response = router
        .clone()
        .oneshot(graphql_request())
        .await
        .expect("router should respond");
    let status = response.status();
    let body = to_bytes(response.into_body(), BODY_LIMIT)
        .await
        .expect("body should read");
    black_box(body);
    status
}

/// Fails loudly if a router does not actually serve the request, so a
/// benchmark can never report an error path as a fast path.
fn assert_serves(rt: &tokio::runtime::Runtime, router: &Router, label: &str) {
    let status = rt.block_on(drive(router));
    assert_eq!(status, StatusCode::OK, "{label} did not return 200");
}

/// Transport A/B: executor alone, versus the router, versus the router with
/// the production middleware stack.
fn bench_transport(c: &mut Criterion) {
    let rt = runtime();

    let direct = executor();
    let bare = create_router_with_state(AppStateBuilder::new(executor()).build());
    let layered = Server::from_arc(executor())
        .router()
        .expect("server router should build");

    assert_serves(&rt, &bare, "bare router");
    assert_serves(&rt, &layered, "layered router");

    let mut group = c.benchmark_group("transport");

    group.bench_function("executor_direct", |b| {
        b.to_async(&rt).iter(|| async {
            let response = direct.execute(QueryRequest::new(black_box(QUERY))).await;
            black_box(response)
        });
    });

    group.bench_function("router_bare", |b| {
        b.to_async(&rt).iter(|| drive(&bare));
    });

    group.bench_function("router_with_middleware", |b| {
        b.to_async(&rt).iter(|| drive(&layered));
    });

    group.finish();
}

/// Builds a signing config for an ephemeral Ed25519 key.
fn signing_config() -> SigningConfig {
    let private_key = crypto::generate_ed25519().expect("key generation should succeed");
    let public_key = private_key.public_key();
    SigningConfig {
        key_type: SigningKeyType::Ed25519,
        private_key_bytes: SigningConfig::private_key_bytes_from_vec(private_key.raw_owned()),
        public_key_bytes: public_key.raw_owned(),
        public_key_hex: public_key.to_hex_string(),
        remote_signer: None,
        signing_authorization: None,
    }
}

/// The `spawn_blocking` delta.
///
/// All three routers below serve the identical request against the identical
/// no-op executor. `fast_path` satisfies `signing_config.is_none() &&
/// state.nac.is_none()` and awaits the executor inline. The other two fail
/// that condition and therefore pay `spawn_blocking` + `Handle::block_on` plus
/// the thread-local setup:
///
/// * `signing_enabled` trips it via the signing config alone, which isolates
///   the hop itself.
/// * `nac_enabled` trips it via NAC, which additionally pays a NAC status
///   lookup and a DAC-bypass resolution - closer to what an access-controlled
///   deployment actually costs.
fn bench_query_context(c: &mut Criterion) {
    let rt = runtime();

    let fast_path = create_router_with_state(AppStateBuilder::new(executor()).build());

    // The signing registry is process-global and keyed by DID, so this bench
    // uses a DID of its own rather than sharing one with any other test.
    let signing_did = "did:key:defra-http-transport-bench";
    defra_core::signing::store_identity(signing_did, signing_config());
    let signing_state = AppStateBuilder::new(executor())
        .with_node_identity_did(signing_did.to_string())
        .with_signing_enabled(true)
        .build();
    let signing_path = create_router_with_state(signing_state);

    let nac_state = AppStateBuilder::new(executor())
        .with_nac(Arc::new(MockNodeAcpOperations::new()))
        .build();
    let nac_path = create_router_with_state(nac_state);

    assert_serves(&rt, &fast_path, "fast path router");
    assert_serves(&rt, &signing_path, "signing path router");
    assert_serves(&rt, &nac_path, "nac path router");

    let mut group = c.benchmark_group("query_context");

    for (name, router) in [
        ("fast_path", &fast_path),
        ("spawn_blocking_signing", &signing_path),
        ("spawn_blocking_nac", &nac_path),
    ] {
        group.bench_with_input(BenchmarkId::from_parameter(name), router, |b, router| {
            b.to_async(&rt).iter(|| drive(router));
        });
    }

    group.finish();
}

criterion_group!(benches, bench_transport, bench_query_context);
criterion_main!(benches);
