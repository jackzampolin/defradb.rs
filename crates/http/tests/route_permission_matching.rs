//! Parameterized permission keys must match the templates Axum puts in `MatchedPath`.

mod common;

use axum::body::{to_bytes, Body};
use axum::extract::{MatchedPath, Request};
use axum::http::Method;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use defra_http::route_permissions::{route_permission, RoutePermission};
use defra_http::router::NodePermission;
use tower::ServiceExt;

async fn echo_matched_path(request: Request, _next: Next) -> Response {
    request
        .extensions()
        .get::<MatchedPath>()
        .expect("matched route should expose its template")
        .as_str()
        .to_owned()
        .into_response()
}

#[tokio::test]
async fn parameterized_routes_use_their_registered_permissions() {
    let router = common::router().route_layer(middleware::from_fn(echo_matched_path));
    let cases = [
        (
            "/api/v0/tx/7",
            Method::POST,
            "/api/v0/tx/{id}",
            RoutePermission::IdentityOnly,
        ),
        (
            "/api/v0/tx/7",
            Method::DELETE,
            "/api/v0/tx/{id}",
            RoutePermission::IdentityOnly,
        ),
        (
            "/api/v0/tx/7/lens",
            Method::POST,
            "/api/v0/tx/{id}/lens",
            RoutePermission::Required(NodePermission::CollectionPatch),
        ),
        (
            "/api/v0/tx/7/collections",
            Method::GET,
            "/api/v0/tx/{id}/collections",
            RoutePermission::Required(NodePermission::CollectionGet),
        ),
        (
            "/api/v0/tx/7/schema",
            Method::POST,
            "/api/v0/tx/{id}/schema",
            RoutePermission::Required(NodePermission::CollectionPatch),
        ),
        (
            "/api/v0/collections/by-id/abc",
            Method::GET,
            "/api/v0/collections/by-id/{id}",
            RoutePermission::Required(NodePermission::CollectionGet),
        ),
        (
            "/api/v0/collections/by-version/v1",
            Method::GET,
            "/api/v0/collections/by-version/{id}",
            RoutePermission::Required(NodePermission::CollectionGet),
        ),
        (
            "/api/v0/collections/Users",
            Method::GET,
            "/api/v0/collections/{name}",
            RoutePermission::Required(NodePermission::CollectionGet),
        ),
        (
            "/api/v0/collections/Users",
            Method::POST,
            "/api/v0/collections/{name}",
            RoutePermission::Required(NodePermission::DocumentUpdate),
        ),
        (
            "/api/v0/collections/Users",
            Method::PATCH,
            "/api/v0/collections/{name}",
            RoutePermission::Required(NodePermission::DocumentUpdate),
        ),
        (
            "/api/v0/collections/Users",
            Method::DELETE,
            "/api/v0/collections/{name}",
            RoutePermission::Required(NodePermission::DocumentDelete),
        ),
        (
            "/api/v0/collections/Users/describe",
            Method::GET,
            "/api/v0/collections/{name}/describe",
            RoutePermission::Required(NodePermission::CollectionGet),
        ),
        (
            "/api/v0/collections/Users/exists",
            Method::GET,
            "/api/v0/collections/{name}/exists",
            RoutePermission::Required(NodePermission::CollectionGet),
        ),
        (
            "/api/v0/collections/Users/truncate",
            Method::DELETE,
            "/api/v0/collections/{name}/truncate",
            RoutePermission::Required(NodePermission::CollectionTruncate),
        ),
        (
            "/api/v0/collections/Users/document/doc",
            Method::GET,
            "/api/v0/collections/{name}/document/{docID}",
            RoutePermission::Required(NodePermission::DocumentRead),
        ),
        (
            "/api/v0/collections/Users/document/doc",
            Method::PATCH,
            "/api/v0/collections/{name}/document/{docID}",
            RoutePermission::Required(NodePermission::DocumentUpdate),
        ),
        (
            "/api/v0/collections/Users/document/doc",
            Method::DELETE,
            "/api/v0/collections/{name}/document/{docID}",
            RoutePermission::Required(NodePermission::DocumentDelete),
        ),
        (
            "/api/v0/collections/Users/indexes",
            Method::GET,
            "/api/v0/collections/{name}/indexes",
            RoutePermission::Required(NodePermission::IndexList),
        ),
        (
            "/api/v0/collections/Users/indexes",
            Method::POST,
            "/api/v0/collections/{name}/indexes",
            RoutePermission::Required(NodePermission::IndexCreate),
        ),
        (
            "/api/v0/collections/Users/indexes/by_name",
            Method::DELETE,
            "/api/v0/collections/{name}/indexes/{index}",
            RoutePermission::Required(NodePermission::IndexDelete),
        ),
        (
            "/api/v0/collections/Users/encrypted-indexes",
            Method::GET,
            "/api/v0/collections/{name}/encrypted-indexes",
            RoutePermission::Required(NodePermission::EncryptedIndexList),
        ),
        (
            "/api/v0/collections/Users/encrypted-indexes",
            Method::POST,
            "/api/v0/collections/{name}/encrypted-indexes",
            RoutePermission::Required(NodePermission::EncryptedIndexAdd),
        ),
        (
            "/api/v0/collections/Users/encrypted-indexes/email",
            Method::DELETE,
            "/api/v0/collections/{name}/encrypted-indexes/{field}",
            RoutePermission::Required(NodePermission::EncryptedIndexDelete),
        ),
        (
            "/api/v0/acp/policy/test",
            Method::GET,
            "/api/v0/acp/policy/{id}",
            RoutePermission::Required(NodePermission::DacStatus),
        ),
    ];

    for (path, method, expected_path, expected_permission) in cases {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method.clone())
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("router should respond");
        let matched_path = String::from_utf8(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();

        assert_eq!(matched_path, expected_path, "{method} {path}");
        assert_eq!(
            route_permission(&matched_path, &method),
            expected_permission,
            "{method} {path}"
        );
    }
}
