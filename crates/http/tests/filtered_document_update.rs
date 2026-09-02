//! `PATCH /collections/{name}` is Go's filtered document update.
//!
//! Go registers `UpdateDocumentsWithFilter` here
//! (`http/handler_collection.go:510`), back to back with the filtered delete,
//! and its client sends `{"filter": ..., "updater": "..."}` to this path
//! (`http/client_document.go:263-296`). Rust registered `GET`, `POST` and
//! `DELETE` on this path but no `PATCH`, so the request answered 405 and
//! filtered update was reachable only through a GraphQL mutation.
//!
//! Unlike the filtered delete this failed loudly, so it was a capability gap
//! rather than a correctness hazard.

use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{header::HOST, Method, Request, StatusCode},
    Router,
};
use defra_http::route_permissions::{route_permission, RoutePermission};
use defra_http::router::{AppStateBuilder, NodePermission};
use defra_http::{MockQueryExecutor, MockRestOperations};
use query::rest::RestOperations;
use serde_json::{json, Value};
use tower::ServiceExt;

fn router() -> Router {
    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()) as Arc<dyn RestOperations>)
        .build();
    defra_http::create_router_with_state(state)
}

async fn send(router: Router, method: Method, uri: &str, body: &str) -> (StatusCode, String) {
    let response = router
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(HOST, "localhost:9181")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("router should respond");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

async fn patch_with(router: Router, body: &str) -> (StatusCode, String) {
    send(router, Method::PATCH, "/api/v0/collections/Users", body).await
}

async fn document(router: Router, doc_id: &str) -> Value {
    let (status, body) = send(
        router,
        Method::GET,
        &format!("/api/v0/collections/Users/document/{doc_id}"),
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    serde_json::from_str(&body).unwrap()
}

/// The gap itself: this used to be a 405.
#[tokio::test]
async fn the_route_is_no_longer_a_405() {
    let (status, body) = patch_with(
        router(),
        r#"{"filter":{"name":{"_eq":"Alice"}},"updater":"{\"age\":31}"}"#,
    )
    .await;
    assert_ne!(status, StatusCode::METHOD_NOT_ALLOWED, "{body}");
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// Go types `updater` as a string holding JSON, which is what its client
/// sends, so that encoding has to work.
#[tokio::test]
async fn an_updater_sent_as_a_json_string_is_applied() {
    let router = router();
    let (status, body) = patch_with(
        router.clone(),
        r#"{"filter":{"name":{"_eq":"Alice"}},"updater":"{\"age\":31}"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let result: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(result["Count"], json!(1));
    assert_eq!(result["DocIDs"], json!(["bae-123"]));

    assert_eq!(document(router.clone(), "bae-123").await["age"], json!(31));
    assert_eq!(
        document(router, "bae-456").await["age"],
        json!(25),
        "a non-matching document must not be touched"
    );
}

/// A hand-written client is likelier to send an object than to double-encode.
#[tokio::test]
async fn an_updater_sent_as_an_object_is_applied() {
    let router = router();
    let (status, body) = patch_with(
        router.clone(),
        r#"{"filter":{"name":{"_eq":"Alice"}},"updater":{"age":31}}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(document(router, "bae-123").await["age"], json!(31));
}

/// Go's `UpdateResult` fields carry no json tags, so they marshal capitalised.
#[tokio::test]
async fn the_response_uses_gos_field_names() {
    let (_, body) = patch_with(
        router(),
        r#"{"filter":{"name":{"_eq":"Alice"}},"updater":"{\"age\":31}"}"#,
    )
    .await;
    let result: Value = serde_json::from_str(&body).unwrap();
    assert!(result.get("Count").is_some(), "{body}");
    assert!(result.get("DocIDs").is_some(), "{body}");
}

#[tokio::test]
async fn a_filter_matching_nothing_updates_nothing() {
    let router = router();
    let (status, body) = patch_with(
        router.clone(),
        r#"{"filter":{"name":{"_eq":"Nobody"}},"updater":"{\"age\":31}"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let result: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(result["Count"], json!(0));
    assert_eq!(document(router, "bae-123").await["age"], json!(30));
}

/// Neither half may be guessed. A missing filter would rewrite the whole
/// collection; a missing updater would claim an update that never happened.
#[tokio::test]
async fn an_incomplete_request_is_refused() {
    let bodies = [
        "",
        "{}",
        "not json",
        r#"{"updater":"{\"age\":31}"}"#,
        r#"{"filter":null,"updater":"{\"age\":31}"}"#,
        r#"{"filter":{"name":{"_eq":"Alice"}}}"#,
        r#"{"filter":{"name":{"_eq":"Alice"}},"updater":null}"#,
        r#"{"filter":{"name":{"_eq":"Alice"}},"updater":"not json"}"#,
    ];
    for body in bodies {
        let router = router();
        let (status, response) = patch_with(router.clone(), body).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "body {body:?} should be refused, got {response}"
        );
        assert_eq!(
            document(router, "bae-123").await["age"],
            json!(30),
            "body {body:?} must not have changed anything"
        );
    }
}

#[tokio::test]
async fn an_unknown_collection_is_not_a_success() {
    let (status, _) = send(
        router(),
        Method::PATCH,
        "/api/v0/collections/Nope",
        r#"{"filter":{"name":{"_eq":"Alice"}},"updater":"{\"age\":31}"}"#,
    )
    .await;
    assert_ne!(status, StatusCode::OK);
}

/// A new verb on an existing path needs its own permission entry, or it falls
/// to the safe default and is gated as a read.
#[tokio::test]
async fn the_route_is_gated_as_a_document_update() {
    assert_eq!(
        route_permission("/api/v0/collections/{name}", &Method::PATCH),
        RoutePermission::Required(NodePermission::DocumentUpdate)
    );
}

/// A JSON Patch updater is refused, deliberately.
///
/// Go accepts the array and answers 200 with a non-zero `Count`, but changes
/// nothing: the patch branch is an empty `// todo` and the loop still calls
/// `c.update` and increments the count (`internal/db/document_update.go:135-152`,
/// and its own `TestUpdateWithPatch_DoesNothing`). Reporting documents as
/// updated while leaving them untouched is a lie to the caller, so this route
/// says it cannot do it instead of copying the behaviour. That is a known
/// divergence from Go, in the direction of refusing a no-op.
#[tokio::test]
async fn a_json_patch_updater_is_refused_rather_than_silently_doing_nothing() {
    let router = router();
    let (status, body) = patch_with(
        router.clone(),
        r#"{"filter":{"name":{"_eq":"Alice"}},"updater":"[{\"name\":\"Eric\"}]"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        document(router, "bae-123").await["name"],
        json!("Alice"),
        "and nothing may have changed"
    );
}

/// Go's filter is `any`, and a string is first-class; the JS client always
/// sends one (`internal/db/document_update.go:168-176`).
#[tokio::test]
async fn a_filter_sent_as_graphql_source_is_applied() {
    let router = router();
    let (status, body) = patch_with(
        router.clone(),
        r#"{"filter":"{name: {_eq: \"Alice\"}}","updater":"{\"age\":31}"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(document(router, "bae-123").await["age"], json!(31));
}

/// A bare scalar condition is equality, as Go's connor reads it.
#[tokio::test]
async fn a_bare_scalar_condition_means_equality() {
    let router = router();
    let (status, body) = patch_with(
        router.clone(),
        r#"{"filter":{"name":"Alice"},"updater":"{\"age\":31}"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(document(router, "bae-123").await["age"], json!(31));
}
