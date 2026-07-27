use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use query::executor::QueryExecutor;
use serde_json::json;
use tower::ServiceExt;

use defra_core::browser_sync::{
    BrowserSyncRequest, BrowserSyncResponse, MAX_SYNC_DOCUMENTS_PER_REQUEST, MAX_SYNC_PULL_DOC_IDS,
};

use crate::mock::{MockNodeAcpOperations, MockQueryExecutor};
use crate::router::{
    create_router_with_state, create_router_with_state_and_sync_body_limit, AppStateBuilder,
    BrowserSyncOperations, BrowserSyncResult, NodeAcpOperations,
};

#[derive(Default)]
struct RecordingSync {
    calls: AtomicUsize,
}

#[async_trait]
impl BrowserSyncOperations for RecordingSync {
    async fn sync(
        &self,
        request: BrowserSyncRequest,
        _caller_did: Option<&str>,
        _bypass_dac: bool,
    ) -> BrowserSyncResult<BrowserSyncResponse> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(BrowserSyncResponse {
            documents: request.documents,
            next_cursor: None,
        })
    }
}

fn router(sync: Arc<RecordingSync>) -> axum::Router {
    let executor = Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>;
    let state = AppStateBuilder::new(executor)
        .with_browser_sync(sync)
        .build();
    create_router_with_state(state)
}

fn router_with_nac(sync: Arc<RecordingSync>, nac: Arc<MockNodeAcpOperations>) -> axum::Router {
    let executor = Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>;
    let state = AppStateBuilder::new(executor)
        .with_browser_sync(sync)
        .with_nac(nac as Arc<dyn NodeAcpOperations>)
        .build();
    create_router_with_state(state)
}

/// A request that neither pulls nor pushes still reaches the sync service,
/// whose "not enabled" error would otherwise tell an unauthorized caller
/// whether browser sync is turned on. It must be permission-checked like any
/// other sync call.
#[tokio::test]
async fn empty_sync_request_is_permission_checked() {
    let sync = Arc::new(RecordingSync::default());
    let owner = identity::Did::new("did:key:owner").unwrap();
    let nac = Arc::new(MockNodeAcpOperations::enabled_with_owner(owner));
    let router = router_with_nac(sync.clone(), nac);

    let response = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v0/sync")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"documents":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(sync.calls.load(Ordering::Relaxed), 0);
}

/// The same request from a caller that does hold document-read still works,
/// so gating the probe does not turn a no-op sync into a hard failure.
#[tokio::test]
async fn empty_sync_request_is_allowed_for_permitted_caller() {
    let sync = Arc::new(RecordingSync::default());
    let owner = identity::Did::new("did:key:owner").unwrap();
    let nac = Arc::new(MockNodeAcpOperations::enabled_with_owner(owner).with_grant(
        identity::Did::wildcard(),
        crate::router::NodePermission::DocumentRead,
    ));
    let router = router_with_nac(sync.clone(), nac);

    let response = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v0/sync")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"documents":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(sync.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn sync_is_available_under_v0_and_v1() {
    let sync = Arc::new(RecordingSync::default());
    let router = router(sync.clone());

    for path in ["/api/v0/sync", "/api/v1/sync"] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"documents":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            json!({"documents": []})
        );
    }

    assert_eq!(sync.calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn sync_rejects_document_count_before_calling_adapter() {
    let sync = Arc::new(RecordingSync::default());
    let documents = vec![
        json!({
            "doc_id": "bae-test",
            "collection_id": "collection",
            "roots": [],
            "blocks": []
        });
        MAX_SYNC_DOCUMENTS_PER_REQUEST + 1
    ];
    let response = router(sync.clone())
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sync")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"documents": documents})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(sync.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn sync_rejects_pull_document_count_before_calling_adapter() {
    let sync = Arc::new(RecordingSync::default());
    let doc_ids = vec!["bae-test"; MAX_SYNC_PULL_DOC_IDS + 1];
    let response = router(sync.clone())
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sync")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "documents": [],
                        "pull": { "doc_ids": doc_ids }
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(sync.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn sync_honors_stricter_body_limit() {
    let sync = Arc::new(RecordingSync::default());
    let executor = Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>;
    let state = AppStateBuilder::new(executor)
        .with_browser_sync(sync.clone())
        .build();
    let router = create_router_with_state_and_sync_body_limit(state, 15);
    let response = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sync")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"documents":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(sync.calls.load(Ordering::Relaxed), 0);
}

// =============================================================================
// Contract coverage for the #1188 authorization gate (issue #1181).
//
// The tests above build the router without the global auth middleware and only
// exercise anonymous callers. These add the three properties that pin the
// deployed contract: the gate works for a real authenticated identity, it is
// inert when NAC is disabled, and pushes that create documents are gated by
// `DocumentUpdate` because no create permission exists in the model.
// =============================================================================

/// A bearer token that passes JWT verification against a `Host: localhost`
/// audience, so tests can exercise the production middleware path.
fn authenticated_caller() -> (identity::Did, String) {
    let private_key = crypto::generate_ed25519().unwrap();
    let raw = identity::RawIdentity::from_private_key(private_key).unwrap();
    let did = identity::Identity::did(&raw).unwrap();
    let token = identity::new_token(
        &raw,
        std::time::Duration::from_secs(3600),
        Some("localhost".to_string()),
        None,
    )
    .unwrap();
    (did, format!("Bearer {}", String::from_utf8(token).unwrap()))
}

/// Mirrors `server.rs`: the auth middleware is applied via `route_layer`, so
/// these tests prove what the middleware does and does not enforce for the
/// Dynamic `/sync` route.
fn router_with_middleware(
    sync: Option<Arc<RecordingSync>>,
    nac: Option<Arc<MockNodeAcpOperations>>,
) -> axum::Router {
    let executor = Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>;
    let mut builder = AppStateBuilder::new(executor);
    if let Some(sync) = sync {
        builder = builder.with_browser_sync(sync);
    }
    if let Some(nac) = nac {
        builder = builder.with_nac(nac as Arc<dyn NodeAcpOperations>);
    }
    let state = builder.build();
    create_router_with_state(state.clone()).route_layer(axum::middleware::from_fn_with_state(
        state,
        crate::auth_middleware::auth_middleware,
    ))
}

fn empty_sync_request(bearer: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/sync")
        .header("content-type", "application/json")
        .header("host", "localhost");
    if let Some(bearer) = bearer {
        builder = builder.header("authorization", bearer);
    }
    builder.body(Body::from(r#"{"documents":[]}"#)).unwrap()
}

/// The gate holds for a real authenticated identity on the production
/// middleware path, not just for the anonymous wildcard fallback.
#[tokio::test]
async fn empty_sync_request_is_rejected_for_authenticated_caller_without_permission() {
    let (owner, _) = authenticated_caller();
    let (_, stranger_bearer) = authenticated_caller();
    let nac = Arc::new(MockNodeAcpOperations::enabled_with_owner(owner));
    let sync = Arc::new(RecordingSync::default());
    let router = router_with_middleware(Some(sync.clone()), Some(nac));

    let response = router
        .oneshot(empty_sync_request(Some(&stranger_bearer)))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(sync.calls.load(Ordering::Relaxed), 0);
}

/// Known limit of the gate: `require_permission` is a no-op when NAC is not
/// configured, so an empty request still distinguishes a node with browser
/// sync enabled (200) from one without it (503). Acceptable because a node
/// without NAC exposes its whole API unauthenticated anyway; this test exists
/// so that reasoning stays explicit rather than assumed.
#[tokio::test]
async fn empty_sync_request_still_reveals_availability_without_nac() {
    let sync = Arc::new(RecordingSync::default());
    let enabled = router_with_middleware(Some(sync.clone()), None);
    let response = enabled.oneshot(empty_sync_request(None)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(sync.calls.load(Ordering::Relaxed), 1);

    let disabled = router_with_middleware(None, None);
    let response = disabled.oneshot(empty_sync_request(None)).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// Pushing a document the server has never seen is a create, and it is
/// authorized by the same `DocumentUpdate` permission as an update: the NAC
/// model has no create variant, matching Go. Pinned so that adding one becomes
/// a deliberate, Go-diverging decision rather than an accident.
#[tokio::test]
async fn push_creating_a_new_document_is_gated_by_document_update() {
    let (owner, _) = authenticated_caller();
    let (writer, writer_bearer) = authenticated_caller();
    let nac = Arc::new(
        MockNodeAcpOperations::enabled_with_owner(owner)
            .with_grant(writer, crate::router::NodePermission::DocumentUpdate),
    );
    let sync = Arc::new(RecordingSync::default());
    let router = router_with_middleware(Some(sync.clone()), Some(nac));

    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/sync")
        .header("content-type", "application/json")
        .header("host", "localhost")
        .header("authorization", &writer_bearer)
        .body(Body::from(
            r#"{"documents":[{"doc_id":"bae-new","collection_id":"c","roots":[],"blocks":[]}]}"#,
        ))
        .unwrap();
    let response = router.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(sync.calls.load(Ordering::Relaxed), 1);
    assert!(crate::router::NodePermission::parse("create-document").is_none());
}
