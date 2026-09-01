//! The route permission table, which the auth middleware consults for every
//! request.
//!
//! The table is keyed on axum's `MatchedPath`, so a route registered at a
//! second path does not inherit the first path's entry. A missing entry is not
//! a compile error and not a 404: it falls to the safe default and the route is
//! enforced as `DocumentRead`, which on a privileged route is a downgrade.
//! `all_registered_routes_return_expected_permission` is the exhaustive list
//! that catches that, and it is deliberately the only one, so the two copies
//! cannot drift apart.
//!
//! Note the parameterized entries here use colon syntax (`:name`). axum 0.8's
//! `MatchedPath` yields brace syntax (`{name}`), so those entries never fire in
//! production and these assertions do not speak for them. That is #1497, not
//! something this file can fix by itself: the table's keys have to change with
//! it.

use axum::http::Method;

use defra_http::route_permissions::{route_permission, RoutePermission};
use defra_http::router::NodePermission;

#[test]
fn exempt_routes() {
    assert_eq!(
        route_permission("/health-check", &Method::GET),
        RoutePermission::Exempt
    );
    assert_eq!(
        route_permission("/openapi.json", &Method::GET),
        RoutePermission::Exempt
    );
    assert_eq!(
        route_permission("/api/v0/version", &Method::GET),
        RoutePermission::Exempt
    );
    assert_eq!(
        route_permission("/api/v0/graphql/ws", &Method::GET),
        RoutePermission::Exempt
    );
    assert_eq!(
        route_permission("/api/v0/batch/verify", &Method::POST),
        RoutePermission::Exempt
    );
}

#[test]
fn identity_only_routes() {
    assert_eq!(
        route_permission("/api/v0/tx", &Method::POST),
        RoutePermission::IdentityOnly
    );
    assert_eq!(
        route_permission("/api/v0/tx/:id", &Method::POST),
        RoutePermission::IdentityOnly
    );
    assert_eq!(
        route_permission("/api/v0/tx/:id", &Method::DELETE),
        RoutePermission::IdentityOnly
    );
}

#[test]
fn dynamic_routes() {
    assert_eq!(
        route_permission("/api/v0/ccip", &Method::POST),
        RoutePermission::Dynamic
    );
    assert_eq!(
        route_permission("/api/v0/ccip/{sender}/{data}", &Method::GET),
        RoutePermission::Dynamic
    );
    assert_eq!(
        route_permission("/api/v0/graphql", &Method::POST),
        RoutePermission::Dynamic
    );
    assert_eq!(
        route_permission("/api/v0/acp/node/enable", &Method::POST),
        RoutePermission::Dynamic
    );
    assert_eq!(
        route_permission("/api/v0/acp/node/disable", &Method::POST),
        RoutePermission::Dynamic
    );
    assert_eq!(
        route_permission("/api/v0/acp/node/re-enable", &Method::POST),
        RoutePermission::Dynamic
    );
    assert_eq!(
        route_permission("/api/v0/batch/start", &Method::POST),
        RoutePermission::Dynamic
    );
    assert_eq!(
        route_permission("/api/v0/batch/sign", &Method::POST),
        RoutePermission::Dynamic
    );
    assert_eq!(
        route_permission("/api/v1/sync", &Method::POST),
        RoutePermission::Dynamic
    );
}

#[test]
fn collection_routes() {
    assert_eq!(
        route_permission("/api/v0/collections", &Method::GET),
        RoutePermission::Required(NodePermission::CollectionGet)
    );
    assert_eq!(
        route_permission("/api/v0/collections", &Method::PATCH),
        RoutePermission::Required(NodePermission::CollectionPatch)
    );
    assert_eq!(
        route_permission("/api/v0/collections/:name", &Method::POST),
        RoutePermission::Required(NodePermission::DocumentUpdate)
    );
    assert_eq!(
        route_permission("/api/v0/collections/:name", &Method::DELETE),
        RoutePermission::Required(NodePermission::DocumentDelete)
    );
}

#[test]
fn document_routes() {
    assert_eq!(
        route_permission("/api/v0/collections/:name/document/:docID", &Method::GET),
        RoutePermission::Required(NodePermission::DocumentRead)
    );
    assert_eq!(
        route_permission("/api/v0/collections/:name/document/:docID", &Method::PATCH),
        RoutePermission::Required(NodePermission::DocumentUpdate)
    );
    assert_eq!(
        route_permission("/api/v0/collections/:name/document/:docID", &Method::DELETE),
        RoutePermission::Required(NodePermission::DocumentDelete)
    );
}

#[test]
fn p2p_routes() {
    assert_eq!(
        route_permission("/api/v0/p2p/info", &Method::GET),
        RoutePermission::Required(NodePermission::P2pPeerInfo)
    );
    assert_eq!(
        route_permission("/api/v0/p2p/shareable-address", &Method::GET),
        RoutePermission::Required(NodePermission::P2pPeerInfo)
    );
    assert_eq!(
        route_permission("/api/v0/p2p/disconnect", &Method::POST),
        RoutePermission::Required(NodePermission::P2pPeerDisconnect)
    );
    assert_eq!(
        route_permission("/api/v0/p2p/replicators", &Method::GET),
        RoutePermission::Required(NodePermission::P2pReplicatorList)
    );
    assert_eq!(
        route_permission("/api/v0/p2p/replicators", &Method::POST),
        RoutePermission::Required(NodePermission::P2pReplicatorAdd)
    );
    assert_eq!(
        route_permission("/api/v0/p2p/replicators", &Method::DELETE),
        RoutePermission::Required(NodePermission::P2pReplicatorDelete)
    );
    assert_eq!(
        route_permission("/api/v0/p2p/documents/sync", &Method::POST),
        RoutePermission::Required(NodePermission::P2pSyncDocuments)
    );
    assert_eq!(
        route_permission("/api/v0/p2p/manage", &Method::POST),
        RoutePermission::Required(NodePermission::P2pPeerConnect)
    );
    assert_eq!(
        route_permission("/api/v0/p2p/manage/query", &Method::POST),
        RoutePermission::Required(NodePermission::P2pPeerConnect)
    );
}

#[test]
fn nac_routes() {
    assert_eq!(
        route_permission("/api/v0/nac/status", &Method::GET),
        RoutePermission::Required(NodePermission::NacStatus)
    );
    assert_eq!(
        route_permission("/api/v0/nac/admin", &Method::POST),
        RoutePermission::Required(NodePermission::NacRelationAdd)
    );
    assert_eq!(
        route_permission("/api/v0/nac/admin", &Method::DELETE),
        RoutePermission::Required(NodePermission::NacRelationDelete)
    );
}

#[test]
fn acp_status_route_uses_dac_status_permission() {
    // Regression test for #758: the route was missing from the
    // permission table and falling through to DocumentRead, which
    // over-restricted callers that only had DacStatus.
    assert_eq!(
        route_permission("/api/v0/acp/status", &Method::GET),
        RoutePermission::Required(NodePermission::DacStatus)
    );
}

#[test]
fn acp_document_decide_route_uses_dac_status_permission() {
    assert_eq!(
        route_permission("/api/v0/acp/document/decide", &Method::POST),
        RoutePermission::Required(NodePermission::DacStatus)
    );
}

#[test]
fn unknown_route_gets_safe_default() {
    assert_eq!(
        route_permission("/api/v0/unknown", &Method::GET),
        RoutePermission::Required(NodePermission::DocumentRead)
    );
}

#[test]
fn all_registered_routes_return_expected_permission() {
    // Exhaustive list of all (path, method, expected) from routes.rs.
    // If a route is added without updating route_permission(), the safe
    // default fires and the warn log catches it at runtime.
    let routes: Vec<(&str, Method, RoutePermission)> = vec![
        // Exempt
        ("/health-check", Method::GET, RoutePermission::Exempt),
        ("/openapi.json", Method::GET, RoutePermission::Exempt),
        ("/api/v0/version", Method::GET, RoutePermission::Exempt),
        ("/api/v0/graphql/ws", Method::GET, RoutePermission::Exempt),
        (
            "/api/v0/batch/verify",
            Method::POST,
            RoutePermission::Exempt,
        ),
        // GraphQL
        ("/api/v0/ccip", Method::POST, RoutePermission::Dynamic),
        (
            "/api/v0/ccip/:sender/:data",
            Method::GET,
            RoutePermission::Dynamic,
        ),
        (
            "/api/v0/graphql",
            Method::GET,
            RoutePermission::Required(NodePermission::DocumentRead),
        ),
        ("/api/v0/graphql", Method::POST, RoutePermission::Dynamic),
        ("/api/v0/sync", Method::POST, RoutePermission::Dynamic),
        (
            "/api/v0/actions",
            Method::GET,
            RoutePermission::Required(NodePermission::ActionList),
        ),
        // Schema
        (
            "/api/v0/schema",
            Method::GET,
            RoutePermission::Required(NodePermission::CollectionGet),
        ),
        (
            "/api/v0/schema",
            Method::POST,
            RoutePermission::Required(NodePermission::CollectionPatch),
        ),
        // Transactions
        ("/api/v0/tx", Method::POST, RoutePermission::IdentityOnly),
        (
            "/api/v0/tx/:id",
            Method::POST,
            RoutePermission::IdentityOnly,
        ),
        (
            "/api/v0/tx/:id",
            Method::DELETE,
            RoutePermission::IdentityOnly,
        ),
        (
            "/api/v0/tx/:id/lens",
            Method::POST,
            RoutePermission::Required(NodePermission::CollectionPatch),
        ),
        (
            "/api/v0/tx/:id/collections",
            Method::GET,
            RoutePermission::Required(NodePermission::CollectionGet),
        ),
        (
            "/api/v0/tx/:id/schema",
            Method::POST,
            RoutePermission::Required(NodePermission::CollectionPatch),
        ),
        // Collections
        (
            "/api/v0/collections",
            Method::GET,
            RoutePermission::Required(NodePermission::CollectionGet),
        ),
        (
            "/api/v0/collections",
            Method::PATCH,
            RoutePermission::Required(NodePermission::CollectionPatch),
        ),
        (
            "/api/v0/collections",
            Method::DELETE,
            RoutePermission::Required(NodePermission::CollectionPatch),
        ),
        (
            "/api/v0/collections/default",
            Method::POST,
            RoutePermission::Required(NodePermission::CollectionPatch),
        ),
        (
            "/api/v0/collections/versions",
            Method::GET,
            RoutePermission::Required(NodePermission::CollectionGet),
        ),
        (
            "/api/v0/collections/versions",
            Method::DELETE,
            RoutePermission::Required(NodePermission::CollectionPatch),
        ),
        (
            "/api/v0/collections/migrations",
            Method::POST,
            RoutePermission::Required(NodePermission::MigrationSet),
        ),
        (
            "/api/v0/collections/:name",
            Method::POST,
            RoutePermission::Required(NodePermission::DocumentUpdate),
        ),
        // Go's filtered document operations, not collection ones.
        (
            "/api/v0/collections/:name",
            Method::PATCH,
            RoutePermission::Required(NodePermission::DocumentUpdate),
        ),
        (
            "/api/v0/collections/:name",
            Method::DELETE,
            RoutePermission::Required(NodePermission::DocumentDelete),
        ),
        (
            "/api/v0/collections/:name/truncate",
            Method::DELETE,
            RoutePermission::Required(NodePermission::CollectionTruncate),
        ),
        (
            "/api/v0/collections/:name/document/:docID",
            Method::GET,
            RoutePermission::Required(NodePermission::DocumentRead),
        ),
        (
            "/api/v0/collections/:name/document/:docID",
            Method::PATCH,
            RoutePermission::Required(NodePermission::DocumentUpdate),
        ),
        (
            "/api/v0/collections/:name/document/:docID",
            Method::DELETE,
            RoutePermission::Required(NodePermission::DocumentDelete),
        ),
        // P2P
        (
            "/api/v0/p2p/info",
            Method::GET,
            RoutePermission::Required(NodePermission::P2pPeerInfo),
        ),
        (
            "/api/v0/p2p/sync/status",
            Method::GET,
            RoutePermission::Required(NodePermission::P2pPeerInfo),
        ),
        (
            "/api/v0/p2p/active-peers",
            Method::GET,
            RoutePermission::Required(NodePermission::P2pPeerActive),
        ),
        (
            "/api/v0/p2p/connect",
            Method::POST,
            RoutePermission::Required(NodePermission::P2pPeerConnect),
        ),
        (
            "/api/v0/p2p/disconnect",
            Method::POST,
            RoutePermission::Required(NodePermission::P2pPeerDisconnect),
        ),
        (
            "/api/v0/p2p/replicators",
            Method::GET,
            RoutePermission::Required(NodePermission::P2pReplicatorList),
        ),
        (
            "/api/v0/p2p/replicators",
            Method::POST,
            RoutePermission::Required(NodePermission::P2pReplicatorAdd),
        ),
        (
            "/api/v0/p2p/replicators",
            Method::DELETE,
            RoutePermission::Required(NodePermission::P2pReplicatorDelete),
        ),
        (
            "/api/v0/p2p/collections",
            Method::GET,
            RoutePermission::Required(NodePermission::P2pCollectionList),
        ),
        (
            "/api/v0/p2p/collections",
            Method::POST,
            RoutePermission::Required(NodePermission::P2pCollectionAdd),
        ),
        (
            "/api/v0/p2p/collections",
            Method::DELETE,
            RoutePermission::Required(NodePermission::P2pCollectionDelete),
        ),
        (
            "/api/v0/p2p/documents/sync",
            Method::POST,
            RoutePermission::Required(NodePermission::P2pSyncDocuments),
        ),
        (
            "/api/v0/p2p/manage",
            Method::POST,
            RoutePermission::Required(NodePermission::P2pPeerConnect),
        ),
        (
            "/api/v0/p2p/manage/query",
            Method::POST,
            RoutePermission::Required(NodePermission::P2pPeerConnect),
        ),
        // ACP
        (
            "/api/v0/acp/status",
            Method::GET,
            RoutePermission::Required(NodePermission::DacStatus),
        ),
        (
            "/api/v0/acp/policy",
            Method::POST,
            RoutePermission::Required(NodePermission::DacPolicyAdd),
        ),
        (
            "/api/v0/acp/document/policy",
            Method::POST,
            RoutePermission::Required(NodePermission::DacPolicyAdd),
        ),
        (
            "/api/v0/acp/policy",
            Method::GET,
            RoutePermission::Required(NodePermission::DacStatus),
        ),
        (
            "/api/v0/acp/document/decide",
            Method::POST,
            RoutePermission::Required(NodePermission::DacStatus),
        ),
        (
            "/api/v0/acp/document/relationship",
            Method::POST,
            RoutePermission::Required(NodePermission::DacRelationAdd),
        ),
        (
            "/api/v0/acp/document/relationship",
            Method::DELETE,
            RoutePermission::Required(NodePermission::DacRelationDelete),
        ),
        (
            "/api/v0/acp/document/relationships",
            Method::POST,
            RoutePermission::Required(NodePermission::DacRelationAdd),
        ),
        // ACP Node
        (
            "/api/v0/acp/node/status",
            Method::GET,
            RoutePermission::Required(NodePermission::NacStatus),
        ),
        (
            "/api/v0/acp/node/enable",
            Method::POST,
            RoutePermission::Dynamic,
        ),
        (
            "/api/v0/acp/node/relationships",
            Method::POST,
            RoutePermission::Required(NodePermission::NacRelationAdd),
        ),
        (
            "/api/v0/acp/node/disable",
            Method::POST,
            RoutePermission::Dynamic,
        ),
        (
            "/api/v0/acp/node/re-enable",
            Method::POST,
            RoutePermission::Dynamic,
        ),
        // Index
        (
            "/api/v0/index",
            Method::POST,
            RoutePermission::Required(NodePermission::IndexCreate),
        ),
        (
            "/api/v0/index",
            Method::GET,
            RoutePermission::Required(NodePermission::IndexList),
        ),
        (
            "/api/v0/index",
            Method::DELETE,
            RoutePermission::Required(NodePermission::IndexDelete),
        ),
        // Backup
        (
            "/api/v0/backup/export",
            Method::POST,
            RoutePermission::Required(NodePermission::DocumentRead),
        ),
        (
            "/api/v0/backup/import",
            Method::POST,
            RoutePermission::Required(NodePermission::DocumentUpdate),
        ),
        // Block
        (
            "/api/v0/block/verify-signature",
            Method::GET,
            RoutePermission::Required(NodePermission::SignatureVerify),
        ),
        (
            "/api/v0/block/signed",
            Method::GET,
            RoutePermission::Required(NodePermission::SignatureVerify),
        ),
        // Views
        (
            "/api/v0/views",
            Method::POST,
            RoutePermission::Required(NodePermission::ViewAdd),
        ),
        (
            "/api/v0/views/refresh",
            Method::POST,
            RoutePermission::Required(NodePermission::ViewRefresh),
        ),
        (
            "/api/v0/views/gc",
            Method::POST,
            RoutePermission::Required(NodePermission::ViewGc),
        ),
        // Go-compatible view mount
        (
            "/api/v0/view",
            Method::POST,
            RoutePermission::Required(NodePermission::ViewAdd),
        ),
        (
            "/api/v0/view/refresh",
            Method::POST,
            RoutePermission::Required(NodePermission::ViewRefresh),
        ),
        // Utility
        (
            "/api/v0/purge",
            Method::POST,
            RoutePermission::Required(NodePermission::DocumentUpdate),
        ),
        (
            "/api/v0/node/options",
            Method::GET,
            RoutePermission::Required(NodePermission::P2pPeerInfo),
        ),
        (
            "/api/v0/node/identity",
            Method::GET,
            RoutePermission::Required(NodePermission::P2pPeerConnect),
        ),
    ];

    // Every mount in `API_PREFIXES` serves this same route set, so each form
    // has to resolve to the same permission. Checking them here rather than
    // hand-copying the list keeps the mounts from drifting apart.
    for (path, method, expected) in &routes {
        for candidate in mount_forms(path) {
            let actual = route_permission(&candidate, method);
            assert_eq!(
                actual, *expected,
                "Mismatch for {} {}: expected {:?}, got {:?}",
                method, candidate, expected, actual
            );
        }
    }
}

/// The same route as every prefix in `go_paths::API_PREFIXES` mounts it.
fn mount_forms(v0_path: &str) -> Vec<String> {
    let Some(suffix) = v0_path.strip_prefix("/api/v0") else {
        return vec![v0_path.to_string()];
    };
    defra_http::go_paths::API_PREFIXES
        .iter()
        .map(|prefix| format!("{prefix}{suffix}"))
        .collect()
}

#[test]
fn every_mount_form_uses_the_same_permission_as_v0() {
    for (v0_path, method) in [
        ("/api/v0/version", Method::GET),
        ("/api/v0/graphql", Method::POST),
        ("/api/v0/collections/:name", Method::POST),
        ("/api/v0/p2p/replicators", Method::DELETE),
        ("/api/v0/acp/node/disable", Method::POST),
        ("/api/v0/backup/export", Method::POST),
        ("/api/v0/block/signed", Method::GET),
    ] {
        let expected = route_permission(v0_path, &method);
        for candidate in mount_forms(v0_path) {
            assert_eq!(
                route_permission(&candidate, &method),
                expected,
                "{candidate} disagrees with {v0_path} for {method}"
            );
        }
    }
}

/// A path that only looks versioned must not be rewritten into the table.
#[test]
fn near_miss_version_segments_are_not_folded() {
    for path in ["/api/v10/collections", "/api/v2/collections"] {
        assert_eq!(
            route_permission(path, &Method::GET),
            RoutePermission::Required(NodePermission::DocumentRead),
            "{path} should fall to the safe default"
        );
    }
}
