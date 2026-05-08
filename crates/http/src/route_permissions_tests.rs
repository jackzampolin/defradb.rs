//! Exhaustive permission table tests for route_permissions.

#[cfg(test)]
mod tests {
    use axum::http::Method;

    use crate::route_permissions::{route_permission, RoutePermission};
    use crate::router::NodePermission;

    #[test]
    fn exempt_routes() {
        assert_eq!(
            route_permission("/health-check", &Method::GET),
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
            route_permission("/api/v0/tx/concurrent", &Method::POST),
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
            RoutePermission::Required(NodePermission::CollectionPatch)
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
            ("/api/v0/version", Method::GET, RoutePermission::Exempt),
            ("/api/v0/graphql/ws", Method::GET, RoutePermission::Exempt),
            (
                "/api/v0/batch/verify",
                Method::POST,
                RoutePermission::Exempt,
            ),
            // GraphQL
            (
                "/api/v0/graphql",
                Method::GET,
                RoutePermission::Required(NodePermission::DocumentRead),
            ),
            ("/api/v0/graphql", Method::POST, RoutePermission::Dynamic),
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
                "/api/v0/tx/concurrent",
                Method::POST,
                RoutePermission::IdentityOnly,
            ),
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
            (
                "/api/v0/collections/:name",
                Method::DELETE,
                RoutePermission::Required(NodePermission::CollectionPatch),
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
                "/api/v0/acp/policy",
                Method::GET,
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
            // Utility
            (
                "/api/v0/purge",
                Method::POST,
                RoutePermission::Required(NodePermission::DocumentUpdate),
            ),
            (
                "/api/v0/node/identity",
                Method::GET,
                RoutePermission::Required(NodePermission::P2pPeerConnect),
            ),
        ];

        for (path, method, expected) in &routes {
            let actual = route_permission(path, method);
            assert_eq!(
                actual, *expected,
                "Mismatch for {} {} -- expected {:?}, got {:?}",
                method, path, expected, actual
            );
        }
    }
}
