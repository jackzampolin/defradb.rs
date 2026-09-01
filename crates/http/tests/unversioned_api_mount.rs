//! Go mounts its whole router at `/api` with no version segment
//! (`r.Handle("/*", router)`, `http/handler.go:126`), where Rust mounted only
//! `/api/v0` and `/api/v1`, so every unversioned request 404'd.
//!
//! The mount is driven by `go_paths::API_PREFIXES`, and the permission table
//! folds every prefix in that same list onto the v0 key: a form that resolves
//! but does not fold is enforced as `DocumentRead`, a privilege downgrade
//! rather than a 404.
//!
//! Parameterized templates are exercised through the real router in
//! `route_permission_matching.rs`; this file focuses on mount equivalence.

mod common;
use common::*;

use axum::{body::Body, http::Request, Router};
use defra_http::router::{AppStateBuilder, NodeAcpOperations};
use defra_http::{MockNodeAcpOperations, MockQueryExecutor};
use std::sync::Arc;
use tower::ServiceExt;

/// One route per nesting style the router uses, all unparameterized.
const PATHS: &[(&str, Method)] = &[
    ("/schema", Method::GET),
    ("/version", Method::GET),
    ("/collections", Method::GET),
    ("/p2p/replicators", Method::GET),
    ("/view/refresh", Method::POST),
    ("/node/identity", Method::GET),
];

/// Every prefix must answer a given request the same way, not merely resolve.
/// `!= NOT_FOUND` would pass with the unversioned mount answering 405 while
/// the versioned ones answer 400.
#[tokio::test]
async fn every_mount_answers_a_request_identically() {
    // Pinned literally: a test that only iterates the constant stops covering
    // the unversioned mount the moment the constant loses it.
    assert!(
        defra_http::go_paths::API_PREFIXES.contains(&"/api"),
        "the unversioned mount must stay in API_PREFIXES"
    );

    for (path, method) in PATHS {
        let mut answers = Vec::new();
        for prefix in defra_http::go_paths::API_PREFIXES {
            let answer = Call::post(&format!("{prefix}{path}"))
                .method(method.clone())
                .json("{}")
                .authenticated()
                .send()
                .await;
            assert_ne!(
                answer.0,
                StatusCode::NOT_FOUND,
                "{prefix}{path} did not resolve: {}",
                answer.1
            );
            answers.push((prefix, answer));
        }
        let (first_prefix, first) = &answers[0];
        for (prefix, answer) in &answers[1..] {
            assert_eq!(
                answer, first,
                "{prefix}{path} disagrees with {first_prefix}{path}"
            );
        }
    }
}

/// The call this change exists to serve. Go advertises `POST /api/graphql`
/// with a query body (`node/node_api.go`), and a GET without `query` is a 400
/// on both, so asserting the GET proves nothing about the mount.
#[tokio::test]
async fn an_unversioned_graphql_post_is_served() {
    let (status, body) = Call::post("/api/graphql")
        .json(r#"{"query":"{ __typename }"}"#)
        .authenticated()
        .send()
        .await;
    assert_eq!(status, StatusCode::OK, "/api/graphql: {body}");

    let mut answers = Vec::new();
    for prefix in defra_http::go_paths::API_PREFIXES {
        let (status, body) = Call::post(&format!("{prefix}/graphql"))
            .json(r#"{"query":"{ __typename }"}"#)
            .authenticated()
            .send()
            .await;
        assert_eq!(status, StatusCode::OK, "{prefix}/graphql: {body}");
        answers.push((prefix, status, body));
    }
    let (first_prefix, _, first_body) = &answers[0];
    for (prefix, _, body) in &answers[1..] {
        assert_eq!(
            body, first_body,
            "{prefix}/graphql disagrees with {first_prefix}/graphql"
        );
    }
}

/// The regression: this was a 404.
#[tokio::test]
async fn an_unversioned_request_is_not_a_404() {
    let (status, body) = Call::post("/api/collections")
        .method(Method::GET)
        .authenticated()
        .send()
        .await;
    assert_ne!(status, StatusCode::NOT_FOUND, "{body}");
}

/// `MatchedPath` for the unversioned mount is `/api/collections`, not
/// `/api/v0/collections`. Without the fold these drop to the `DocumentRead`
/// default.
#[test]
fn the_unversioned_form_keeps_its_real_permission() {
    let cases = [
        (
            "/collections",
            Method::POST,
            NodePermission::CollectionPatch,
        ),
        ("/purge", Method::POST, NodePermission::DocumentUpdate),
        (
            "/acp/document/policy",
            Method::POST,
            NodePermission::DacPolicyAdd,
        ),
        (
            "/backup/import",
            Method::POST,
            NodePermission::DocumentUpdate,
        ),
    ];

    for (path, method, expected) in cases {
        assert_eq!(
            route_permission(&format!("/api{path}"), &method),
            RoutePermission::Required(expected),
            "/api{path} lost its permission"
        );
    }
}

/// A prefix added to `API_PREFIXES` must fold without a second edit here, or
/// it mounts the route set unenforced.
#[test]
fn every_mounted_prefix_folds() {
    for prefix in defra_http::go_paths::API_PREFIXES {
        assert_eq!(
            route_permission(&format!("{prefix}/purge"), &Method::POST),
            RoutePermission::Required(NodePermission::DocumentUpdate),
            "{prefix}/purge does not fold onto the v0 key"
        );
    }
}

/// The fold rewrites rather than rejects, so a near-miss has to land somewhere
/// harmless: `/api/v2/collections` becomes `/api/v0/v2/collections`, which is
/// no table key.
#[test]
fn near_miss_version_segments_land_on_the_safe_default() {
    for path in ["/api/v10/collections", "/api/v2/collections", "/apifoo"] {
        assert_eq!(
            route_permission(path, &Method::GET),
            RoutePermission::Required(NodePermission::DocumentRead),
            "{path} should fall to the safe default"
        );
    }
}

/// Through the real middleware stack, which is what `server.rs:526-530`
/// applies.
///
/// The same caller hits the same unversioned route twice, holding a different
/// grant each time. That isolates which permission the route is enforced as:
/// `/api/purge` folds to `DocumentUpdate`, so a `DocumentUpdate` holder gets
/// through and a `DocumentRead` holder does not. An unfolded route would be
/// enforced as `DocumentRead` and flip the first case.
#[tokio::test]
async fn the_unversioned_mount_is_enforced_as_its_real_permission() {
    async fn purge_as(granted: NodePermission) -> StatusCode {
        let (owner, _) = nac_owner();
        let (caller, bearer) = nac_owner();
        let nac =
            Arc::new(MockNodeAcpOperations::enabled_with_owner(owner).with_grant(caller, granted));
        let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
            .with_nac(nac as Arc<dyn NodeAcpOperations>)
            .build();
        let router: Router = defra_http::create_router_with_state(state.clone()).route_layer(
            axum::middleware::from_fn_with_state(
                state,
                defra_http::auth_middleware::auth_middleware,
            ),
        );

        router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/purge")
                    .header(axum::http::header::HOST, TEST_HOST)
                    .header(axum::http::header::AUTHORIZATION, bearer)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("router should respond")
            .status()
    }

    let with_read = purge_as(NodePermission::DocumentRead).await;
    let with_update = purge_as(NodePermission::DocumentUpdate).await;

    // `require_permission` answers 401 on a denial (`nac_guard.rs:73-77`), so
    // 401 means the middleware refused and anything else means it let the
    // request reach the handler.
    assert_ne!(
        with_update,
        StatusCode::NOT_FOUND,
        "the route must be mounted unversioned"
    );
    assert_eq!(
        with_read,
        StatusCode::UNAUTHORIZED,
        "a DocumentRead holder passed the unversioned purge gate, so it did \
         not fold onto its real permission"
    );
    assert_ne!(
        with_update,
        StatusCode::UNAUTHORIZED,
        "a DocumentUpdate holder must clear the purge gate, got {with_update}"
    );
}
