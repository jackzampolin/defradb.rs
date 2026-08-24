//! Go mounts its whole router at `/api` with no version segment
//! (`r.Handle("/*", router)`, `http/handler.go:127`), where Rust mounted only
//! `/api/v0` and `/api/v1`, so every unversioned request 404'd.
//!
//! The mount is driven by `go_paths::API_PREFIXES`, and the permission table
//! folds all three forms onto the v0 key: a form that resolves but does not
//! fold is enforced as `DocumentRead`, a privilege downgrade rather than a 404.

mod common;
use common::*;

/// The routes a hand-written client or a curl script is most likely to reach
/// for, one per nesting style the router uses.
const PATHS: &[(&str, Method)] = &[
    ("/graphql", Method::GET),
    ("/schema", Method::GET),
    ("/version", Method::GET),
    ("/collections", Method::GET),
    ("/collections/Users", Method::GET),
    ("/p2p/replicators", Method::GET),
    ("/view/refresh", Method::POST),
];

#[tokio::test]
async fn the_unversioned_mount_serves_the_same_routes_as_the_versioned_ones() {
    for (path, method) in PATHS {
        for prefix in defra_http::go_paths::API_PREFIXES {
            let (status, body) = Call::post(&format!("{prefix}{path}"))
                .method(method.clone())
                .json("{}")
                .authenticated()
                .send()
                .await;
            assert_ne!(
                status,
                StatusCode::NOT_FOUND,
                "{prefix}{path} did not resolve: {body}"
            );
        }
    }
}

/// The regression itself: before this change these were 404s.
#[tokio::test]
async fn an_unversioned_request_is_not_a_404() {
    let (status, body) = Call::post("/api/graphql")
        .method(Method::GET)
        .authenticated()
        .send()
        .await;
    assert_ne!(status, StatusCode::NOT_FOUND, "{body}");
}

/// `MatchedPath` for the unversioned mount is `/api/collections`, not
/// `/api/v0/collections`. Without the fold in `normalize_api_version` every
/// unversioned route silently drops to the `DocumentRead` default.
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
            "/collections/:name/truncate",
            Method::DELETE,
            NodePermission::CollectionTruncate,
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

/// The exemptions have to survive the fold too, or an unversioned health or
/// version probe starts demanding a token.
#[test]
fn the_unversioned_form_keeps_its_exemption() {
    assert_eq!(
        route_permission("/api/version", &Method::GET),
        RoutePermission::Exempt
    );
    assert_eq!(
        route_permission("/api/graphql/ws", &Method::GET),
        RoutePermission::Exempt
    );
}

/// The router mounts one route set three times; axum panics at construction on
/// overlapping routes, so this is the proof the third mount is legal.
#[tokio::test]
async fn the_router_builds_with_every_mount() {
    let _ = router();
}
