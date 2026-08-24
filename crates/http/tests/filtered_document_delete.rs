//! `DELETE /collections/{name}` is Go's filtered document delete.
//!
//! Go serves `DeleteDocumentsWithFilter` here (`http/handler_collection.go:511`)
//! and its own client posts `{"filter": ...}` to this path
//! (`http/client_document.go:299-321`). Rust dropped the collection and every
//! one of its versions instead, and answered success, so a Go-compatible
//! client asking to delete two documents destroyed the collection and could
//! not tell. That is worse than a 404: it succeeds, and does something far
//! more destructive than what was asked.
//!
//! Dropping a collection keeps its own route, `DELETE /collections?name=...`,
//! which is what the Rust CLI already calls.

use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{header::HOST, Method, Request, StatusCode},
    Router,
};
use defra_http::route_permissions::{route_permission, RoutePermission};
use defra_http::router::{AppStateBuilder, NodePermission};
use defra_http::{MockCollectionManagementOperations, MockQueryExecutor, MockRestOperations};
use query::rest::RestOperations;
use serde_json::{json, Value};
use tower::ServiceExt;

fn router() -> Router {
    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()) as Arc<dyn RestOperations>)
        .with_collection_mgmt(Arc::new(MockCollectionManagementOperations::new()))
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

async fn delete_with(router: Router, body: &str) -> (StatusCode, String) {
    send(router, Method::DELETE, "/api/v0/collections/Users", body).await
}

async fn collection_names(router: Router) -> Vec<String> {
    let (status, body) = send(router, Method::GET, "/api/v0/collections", "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    serde_json::from_str::<Value>(&body).unwrap()["collections"]
        .as_array()
        .unwrap()
        .iter()
        .map(|name| name.as_str().unwrap().to_string())
        .collect()
}

async fn doc_ids(router: Router) -> Vec<String> {
    let (status, body) = send(router, Method::GET, "/api/v0/collections/Users", "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    serde_json::from_str::<Value>(&body).unwrap()["doc_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect()
}

/// The filter selects, and only the matching documents go.
#[tokio::test]
async fn a_filter_deletes_only_the_matching_documents() {
    let router = router();
    let (status, body) = delete_with(router.clone(), r#"{"filter":{"name":"Alice"}}"#).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let result: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(result["Count"], json!(1));
    assert_eq!(result["DocIDs"], json!(["bae-123"]));

    assert_eq!(doc_ids(router).await, vec!["bae-456"], "Bob must survive");
}

/// The bug itself: this route used to destroy the collection.
#[tokio::test]
async fn a_filtered_delete_leaves_the_collection_standing() {
    let router = router();
    let (status, body) = delete_with(router.clone(), r#"{"filter":{"name":"Alice"}}"#).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    assert!(
        collection_names(router)
            .await
            .contains(&"Users".to_string()),
        "the collection must still exist after deleting a document from it"
    );
}

/// Go's `DeleteResult` fields carry no json tags, so they marshal capitalised.
/// A client reading `Count` off a lowercase `count` sees nothing.
#[tokio::test]
async fn the_response_uses_gos_field_names() {
    let (_, body) = delete_with(router(), r#"{"filter":{"name":"Alice"}}"#).await;
    let result: Value = serde_json::from_str(&body).unwrap();
    assert!(result.get("Count").is_some(), "{body}");
    assert!(result.get("DocIDs").is_some(), "{body}");
}

/// A filter matching nothing is a successful no-op, not an error.
#[tokio::test]
async fn a_filter_matching_nothing_deletes_nothing() {
    let router = router();
    let (status, body) = delete_with(router.clone(), r#"{"filter":{"name":"Nobody"}}"#).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let result: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(result["Count"], json!(0));
    assert_eq!(result["DocIDs"], json!([]));
    assert_eq!(doc_ids(router).await.len(), 2);
}

/// A filter that cannot be read must not fall back to acting on everything.
/// Go's behaviour for a null filter could not be confirmed from source here,
/// and guessing "match all" would delete the whole collection's contents.
#[tokio::test]
async fn a_request_without_a_usable_filter_is_refused() {
    for body in ["", "{}", r#"{"filter":null}"#, "not json"] {
        let router = router();
        let (status, response) = delete_with(router.clone(), body).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "body {body:?} should be refused, got {response}"
        );
        assert_eq!(
            doc_ids(router).await.len(),
            2,
            "body {body:?} must not have deleted anything"
        );
    }
}

#[tokio::test]
async fn an_unknown_collection_is_not_a_success() {
    let (status, _) = send(
        router(),
        Method::DELETE,
        "/api/v0/collections/Nope",
        r#"{"filter":{"name":"Alice"}}"#,
    )
    .await;
    assert_ne!(status, StatusCode::OK);
}

/// Dropping a collection did not go away, it just stopped sharing a route with
/// document deletion.
#[tokio::test]
async fn dropping_a_collection_still_has_its_own_route() {
    let (status, body) = send(
        router(),
        Method::DELETE,
        "/api/v0/collections?name=Users",
        "",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// The route changed meaning, so its permission had to change with it. Leaving
/// it on `CollectionPatch` would gate document deletion behind a schema
/// permission and let a `DocumentDelete` holder be refused.
#[tokio::test]
async fn the_route_is_gated_as_a_document_delete() {
    assert_eq!(
        route_permission("/api/v0/collections/:name", &Method::DELETE),
        RoutePermission::Required(NodePermission::DocumentDelete)
    );
    assert_eq!(
        route_permission("/api/v0/collections", &Method::DELETE),
        RoutePermission::Required(NodePermission::CollectionPatch),
        "dropping a collection stays a collection permission"
    );
}
