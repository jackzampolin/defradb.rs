//! Go's wire paths for policy registration and views must resolve on Rust.
//!
//! Go's client posts policies to `/acp/document/policy` and views to `/view`,
//! where Rust served `/acp/policy` and `/views`, so every Go-compatible client
//! 404'd. An aliased route also gets its own `MatchedPath`, so it needs its own
//! permission entry or it silently falls to the safe default.

mod common;
use common::*;

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

/// The constants the router, the CLI and Go's client all have to agree on must
/// actually resolve, under every API version the server mounts. Without this
/// the shared constant is just a fourth copy of the string.
#[tokio::test]
async fn every_declared_go_path_resolves_under_every_api_version() {
    for prefix in defra_http::go_paths::API_PREFIXES {
        for path in defra_http::go_paths::ALL {
            let (status, body) = Call::post(&format!("{prefix}{path}"))
                .json("{}")
                .send()
                .await;
            assert_ne!(
                status,
                StatusCode::NOT_FOUND,
                "{prefix}{path} is declared but not served: {body}"
            );
        }
    }
}

/// Go's client posts to `acp/document/policy`. Rust 404'd there.
#[tokio::test]
async fn gos_policy_path_resolves() {
    for version in ["v0", "v1"] {
        let (status, body) = Call::post(&format!("/api/{version}/acp/document/policy"))
            .body(POLICY)
            .authenticated()
            .send()
            .await;
        assert_ne!(status, StatusCode::NOT_FOUND, "{version}: {body}");
    }
}

/// Go's client posts views to `view`, not `views`.
#[tokio::test]
async fn gos_view_paths_resolve() {
    for version in ["v0", "v1"] {
        for (path, body) in [
            ("view", r#"{"Query":"q","SDL":"s"}"#),
            ("view/refresh", "{}"),
        ] {
            let (status, response) = Call::post(&format!("/api/{version}/{path}"))
                .json(body)
                .send()
                .await;
            assert_ne!(
                status,
                StatusCode::NOT_FOUND,
                "{version}/{path}: {response}"
            );
        }
    }
}

/// The change is additive: every path Rust already served still resolves.
#[tokio::test]
async fn the_rust_paths_still_resolve() {
    let calls = [
        Call::post("/api/v0/acp/policy")
            .body(POLICY)
            .authenticated(),
        Call::post("/api/v0/views").json(r#"{"Query":"q","SDL":"s"}"#),
        Call::post("/api/v0/views/refresh").json("{}"),
        Call::post("/api/v0/views/gc").json("{}"),
    ];
    for call in calls {
        let (status, body) = call.send().await;
        assert_ne!(status, StatusCode::NOT_FOUND, "{}: {body}", call.path);
    }
}

/// `/gc` is Rust's own route. Go has no `/view/gc`, and the Go mount must not
/// acquire Rust extras just because it shares a handler set.
#[tokio::test]
async fn the_go_view_mount_has_no_gc_route() {
    let (status, _) = Call::post("/api/v0/view/gc").json("{}").send().await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// axum panics at construction on overlapping routes, and this change mounts
/// one router at two prefixes and one handler at two paths. Every other test
/// builds a router, but only this one says that is the thing being checked.
#[tokio::test]
async fn the_router_builds() {
    let _ = router();
}

/// A method Go does not register answers 405 on the alias exactly as on the
/// canonical path. The auth middleware runs on a method mismatch, so this also
/// pins that the merged permission arm does not turn a 405 into something else.
#[tokio::test]
async fn a_method_mismatch_answers_the_same_on_both_paths() {
    let pairs = [
        ("/api/v0/acp/document/policy", "/api/v0/acp/policy"),
        ("/api/v0/view/refresh", "/api/v0/views/refresh"),
    ];
    for (go_path, rust_path) in pairs {
        let (go_status, _) = Call::post(go_path).method(Method::PUT).send().await;
        let (rust_status, _) = Call::post(rust_path).method(Method::PUT).send().await;
        assert_eq!(go_status, StatusCode::METHOD_NOT_ALLOWED, "{go_path}");
        assert_eq!(go_status, rust_status, "{go_path} vs {rust_path}");
    }
}

// ---------------------------------------------------------------------------
// Permissions
// ---------------------------------------------------------------------------

/// Every Go path added here, with the canonical path it must agree with.
const ALIASED_PATHS: &[(&str, &str, NodePermission)] = &[
    (
        "/api/v0/acp/document/policy",
        "/api/v0/acp/policy",
        NodePermission::DacPolicyAdd,
    ),
    ("/api/v0/view", "/api/v0/views", NodePermission::ViewAdd),
    (
        "/api/v0/view/refresh",
        "/api/v0/views/refresh",
        NodePermission::ViewRefresh,
    ),
];

#[tokio::test]
async fn every_new_path_carries_its_own_permission() {
    for (go_path, rust_path, expected) in ALIASED_PATHS {
        assert_eq!(
            route_permission(go_path, &Method::POST),
            RoutePermission::Required(*expected),
            "{go_path}"
        );
        assert_eq!(
            route_permission(go_path, &Method::POST),
            route_permission(rust_path, &Method::POST),
            "{go_path} must agree with {rust_path}"
        );
    }
}

/// The guard for the trap this change could have shipped: a route registered
/// without a permission entry falls to `Required(DocumentRead)`, which would
/// let a read-only actor register a DAC policy.
#[tokio::test]
async fn no_new_path_falls_to_the_safe_default() {
    for (go_path, _, _) in ALIASED_PATHS {
        assert_ne!(
            route_permission(go_path, &Method::POST),
            RoutePermission::Required(NodePermission::DocumentRead),
            "{go_path} is not in the permission table"
        );
    }
}

/// Go's client talks to `/api/v1`, so the v1 form has to resolve to the same
/// permission as the v0 form it is normalized onto.
#[tokio::test]
async fn both_api_versions_resolve_the_same_permission() {
    for (go_path, _, _) in ALIASED_PATHS {
        let v1_path = go_path.replacen("/api/v0/", "/api/v1/", 1);
        assert_eq!(
            route_permission(&v1_path, &Method::POST),
            route_permission(go_path, &Method::POST),
            "{v1_path}"
        );
    }
}

// ---------------------------------------------------------------------------
// Behaviour equivalence
// ---------------------------------------------------------------------------

/// The two policy paths share a handler but are registered separately, so the
/// guard against them drifting is that they answer identically.
#[tokio::test]
async fn the_policy_paths_answer_identically() {
    for (body, authenticated) in [(POLICY, true), (POLICY, false), ("", true)] {
        let go = Call::post("/api/v0/acp/document/policy").body(body);
        let rust = Call::post("/api/v0/acp/policy").body(body);
        let (go, rust) = if authenticated {
            (go.authenticated(), rust.authenticated())
        } else {
            (go, rust)
        };
        assert_eq!(go.send().await, rust.send().await, "body {body:?}");
    }
}

#[tokio::test]
async fn the_view_paths_answer_identically() {
    let pairs = [
        (
            Call::post("/api/v0/view").json(r#"{"Query":"q","SDL":"s"}"#),
            Call::post("/api/v0/views").json(r#"{"Query":"q","SDL":"s"}"#),
        ),
        (
            Call::post("/api/v0/view/refresh").json(r#"{"Names":["A"]}"#),
            Call::post("/api/v0/views/refresh").json(r#"{"Names":["A"]}"#),
        ),
    ];
    for (go, rust) in pairs {
        assert_eq!(go.send().await, rust.send().await, "{}", go.path);
    }
}
