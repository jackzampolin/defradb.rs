//! Route permission table for global auth middleware.
//!
//! Maps every registered HTTP route to its required permission level,
//! ensuring consistent access control enforcement across all endpoints.

use axum::http::Method;

use crate::router::NodePermission;

/// Permission requirement for a route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutePermission {
    /// Route requires this specific permission.
    Required(NodePermission),
    /// Route has custom auth logic in the handler (e.g., NAC enable/disable, batch start/sign).
    Dynamic,
    /// Route is intentionally public (health, version, batch verify).
    Exempt,
    /// Route needs identity extracted but no specific permission check
    /// (e.g., tx handlers where permissions are per-operation).
    IdentityOnly,
}

/// Look up the permission requirement for a route.
///
/// The `path` parameter is the Axum `MatchedPath` template string
/// (e.g., `/api/v0/collections/:name/:docID`). The `method` is the HTTP method.
///
/// Unknown routes return `Required(DocumentRead)` as a safe default:
/// when NAC is enabled, this blocks unauthenticated access.
pub fn route_permission(path: &str, method: &Method) -> RoutePermission {
    match path {
        // =====================================================================
        // Exempt routes (no auth needed)
        // =====================================================================
        "/health-check" => RoutePermission::Exempt,
        "/api/v0/version" => RoutePermission::Exempt,
        "/api/v0/graphql/ws" => RoutePermission::Exempt,
        "/api/v0/batch/verify" => RoutePermission::Exempt,

        // =====================================================================
        // GraphQL
        // =====================================================================
        "/api/v0/graphql" => match *method {
            // GET is always a read operation
            Method::GET => RoutePermission::Required(NodePermission::DocumentRead),
            // POST determines permission by parsing the query in the handler
            Method::POST => RoutePermission::Dynamic,
            _ => RoutePermission::Required(NodePermission::DocumentRead),
        },
        "/api/v0/schema" => match *method {
            Method::GET => RoutePermission::Required(NodePermission::CollectionGet),
            Method::POST => RoutePermission::Required(NodePermission::CollectionPatch),
            _ => RoutePermission::Required(NodePermission::CollectionGet),
        },

        // =====================================================================
        // Transactions (permissions enforced per-operation within tx)
        // =====================================================================
        "/api/v0/tx" => RoutePermission::IdentityOnly,
        "/api/v0/tx/concurrent" => RoutePermission::IdentityOnly,
        "/api/v0/tx/:id" => RoutePermission::IdentityOnly,
        "/api/v0/tx/:id/lens" => RoutePermission::Required(NodePermission::CollectionPatch),
        "/api/v0/tx/:id/collections" => RoutePermission::Required(NodePermission::CollectionGet),

        // =====================================================================
        // Collections
        // =====================================================================
        "/api/v0/collections" => match *method {
            Method::GET => RoutePermission::Required(NodePermission::CollectionGet),
            Method::POST => RoutePermission::Required(NodePermission::CollectionPatch),
            Method::PATCH => RoutePermission::Required(NodePermission::CollectionPatch),
            _ => RoutePermission::Required(NodePermission::CollectionGet),
        },
        "/api/v0/collections/default" => RoutePermission::Required(NodePermission::CollectionPatch),
        "/api/v0/collections/versions" => match *method {
            Method::GET => RoutePermission::Required(NodePermission::CollectionGet),
            Method::DELETE => RoutePermission::Required(NodePermission::CollectionPatch),
            _ => RoutePermission::Required(NodePermission::CollectionGet),
        },
        "/api/v0/collections/migrations" => RoutePermission::Required(NodePermission::MigrationSet),
        "/api/v0/collections/by-id/:id" => RoutePermission::Required(NodePermission::CollectionGet),
        "/api/v0/collections/by-version/:id" => {
            RoutePermission::Required(NodePermission::CollectionGet)
        }
        "/api/v0/collections/indexes" => RoutePermission::Required(NodePermission::IndexList),
        "/api/v0/collections/:name" => match *method {
            Method::GET => RoutePermission::Required(NodePermission::CollectionGet),
            Method::POST => RoutePermission::Required(NodePermission::DocumentUpdate),
            Method::DELETE => RoutePermission::Required(NodePermission::CollectionPatch),
            _ => RoutePermission::Required(NodePermission::CollectionGet),
        },
        "/api/v0/collections/:name/describe" => {
            RoutePermission::Required(NodePermission::CollectionGet)
        }
        "/api/v0/collections/:name/exists" => {
            RoutePermission::Required(NodePermission::CollectionGet)
        }
        "/api/v0/collections/:name/truncate" => {
            RoutePermission::Required(NodePermission::CollectionTruncate)
        }
        "/api/v0/collections/:name/:docID" => match *method {
            Method::GET => RoutePermission::Required(NodePermission::DocumentRead),
            Method::PATCH => RoutePermission::Required(NodePermission::DocumentUpdate),
            Method::DELETE => RoutePermission::Required(NodePermission::DocumentDelete),
            _ => RoutePermission::Required(NodePermission::DocumentRead),
        },
        "/api/v0/collections/:name/indexes" => match *method {
            Method::GET => RoutePermission::Required(NodePermission::IndexList),
            Method::POST => RoutePermission::Required(NodePermission::IndexCreate),
            _ => RoutePermission::Required(NodePermission::IndexList),
        },
        "/api/v0/collections/:name/indexes/:index" => {
            RoutePermission::Required(NodePermission::IndexDelete)
        }
        "/api/v0/collections/:name/encrypted-indexes" => match *method {
            Method::GET => RoutePermission::Required(NodePermission::EncryptedIndexList),
            Method::POST => RoutePermission::Required(NodePermission::EncryptedIndexAdd),
            _ => RoutePermission::Required(NodePermission::EncryptedIndexList),
        },
        "/api/v0/collections/:name/encrypted-indexes/:field" => {
            RoutePermission::Required(NodePermission::EncryptedIndexDelete)
        }

        // =====================================================================
        // P2P
        // =====================================================================
        "/api/v0/p2p/info" => RoutePermission::Required(NodePermission::P2pPeerInfo),
        "/api/v0/p2p/active-peers" => RoutePermission::Required(NodePermission::P2pPeerActive),
        "/api/v0/p2p/connect" => RoutePermission::Required(NodePermission::P2pPeerConnect),
        "/api/v0/p2p/peers" => match *method {
            Method::GET => RoutePermission::Required(NodePermission::P2pPeerConnect),
            Method::POST => RoutePermission::Required(NodePermission::P2pPeerConnect),
            _ => RoutePermission::Required(NodePermission::P2pPeerConnect),
        },
        "/api/v0/p2p/replicators" => match *method {
            Method::GET => RoutePermission::Required(NodePermission::P2pReplicatorList),
            Method::POST => RoutePermission::Required(NodePermission::P2pReplicatorAdd),
            Method::DELETE => RoutePermission::Required(NodePermission::P2pReplicatorDelete),
            _ => RoutePermission::Required(NodePermission::P2pReplicatorList),
        },
        "/api/v0/p2p/replicator" => match *method {
            Method::GET => RoutePermission::Required(NodePermission::P2pReplicatorList),
            Method::POST => RoutePermission::Required(NodePermission::P2pReplicatorAdd),
            Method::DELETE => RoutePermission::Required(NodePermission::P2pReplicatorDelete),
            _ => RoutePermission::Required(NodePermission::P2pReplicatorList),
        },
        "/api/v0/p2p/collections" => match *method {
            Method::GET => RoutePermission::Required(NodePermission::P2pCollectionList),
            Method::POST => RoutePermission::Required(NodePermission::P2pCollectionAdd),
            Method::DELETE => RoutePermission::Required(NodePermission::P2pCollectionDelete),
            _ => RoutePermission::Required(NodePermission::P2pCollectionList),
        },
        "/api/v0/p2p/collections/sync-branchable" => {
            RoutePermission::Required(NodePermission::P2pSyncBranchableCollection)
        }
        "/api/v0/p2p/collections/sync-versions" => {
            RoutePermission::Required(NodePermission::P2pSyncCollectionVersions)
        }
        "/api/v0/p2p/documents" => match *method {
            Method::GET => RoutePermission::Required(NodePermission::P2pDocumentList),
            Method::POST => RoutePermission::Required(NodePermission::P2pDocumentAdd),
            Method::DELETE => RoutePermission::Required(NodePermission::P2pDocumentDelete),
            _ => RoutePermission::Required(NodePermission::P2pDocumentList),
        },
        "/api/v0/p2p/documents/sync" => RoutePermission::Required(NodePermission::P2pSyncDocuments),

        // =====================================================================
        // ACP (Document Access Control)
        // =====================================================================
        "/api/v0/acp/policy" => match *method {
            Method::POST => RoutePermission::Required(NodePermission::DacPolicyAdd),
            Method::GET => RoutePermission::Required(NodePermission::DacStatus),
            _ => RoutePermission::Required(NodePermission::DacStatus),
        },
        "/api/v0/acp/policy/:id" => RoutePermission::Required(NodePermission::DacStatus),
        "/api/v0/acp/document/relationship" => match *method {
            Method::POST => RoutePermission::Required(NodePermission::DacRelationAdd),
            Method::DELETE => RoutePermission::Required(NodePermission::DacRelationDelete),
            _ => RoutePermission::Required(NodePermission::DacRelationAdd),
        },

        // =====================================================================
        // ACP Node (NAC via Go-compatible /acp/node/* routes)
        // =====================================================================
        "/api/v0/acp/node/status" => RoutePermission::Required(NodePermission::NacStatus),
        "/api/v0/acp/node/enable" => RoutePermission::Dynamic,
        "/api/v0/acp/node/relationship" => match *method {
            Method::POST => RoutePermission::Required(NodePermission::NacRelationAdd),
            Method::DELETE => RoutePermission::Required(NodePermission::NacRelationDelete),
            _ => RoutePermission::Required(NodePermission::NacRelationAdd),
        },
        "/api/v0/acp/node/disable" => RoutePermission::Dynamic,
        "/api/v0/acp/node/re-enable" => RoutePermission::Dynamic,

        // =====================================================================
        // NAC (Rust-native routes)
        // =====================================================================
        "/api/v0/nac/status" => RoutePermission::Required(NodePermission::NacStatus),
        "/api/v0/nac/admin" => match *method {
            Method::POST => RoutePermission::Required(NodePermission::NacRelationAdd),
            Method::DELETE => RoutePermission::Required(NodePermission::NacRelationDelete),
            _ => RoutePermission::Required(NodePermission::NacRelationAdd),
        },

        // =====================================================================
        // Index (Rust-native routes)
        // =====================================================================
        "/api/v0/index" => match *method {
            Method::POST => RoutePermission::Required(NodePermission::IndexCreate),
            Method::GET => RoutePermission::Required(NodePermission::IndexList),
            Method::DELETE => RoutePermission::Required(NodePermission::IndexDelete),
            _ => RoutePermission::Required(NodePermission::IndexList),
        },

        // =====================================================================
        // Backup
        // =====================================================================
        "/api/v0/backup/export" => RoutePermission::Required(NodePermission::DocumentRead),
        "/api/v0/backup/import" => RoutePermission::Required(NodePermission::DocumentUpdate),

        // =====================================================================
        // Block
        // =====================================================================
        "/api/v0/block/verify-signature" => {
            RoutePermission::Required(NodePermission::SignatureVerify)
        }

        // =====================================================================
        // Lens
        // =====================================================================
        "/api/v0/lens" => match *method {
            Method::POST => RoutePermission::Required(NodePermission::LensCreate),
            Method::GET => RoutePermission::Required(NodePermission::LensList),
            _ => RoutePermission::Required(NodePermission::LensList),
        },
        "/api/v0/lens/set" => RoutePermission::Required(NodePermission::MigrationSet),
        "/api/v0/lens/reload" => RoutePermission::Required(NodePermission::CollectionPatch),

        // =====================================================================
        // Batch signing
        // =====================================================================
        "/api/v0/batch/start" => RoutePermission::Dynamic,
        "/api/v0/batch/sign" => RoutePermission::Dynamic,

        // =====================================================================
        // Views
        // =====================================================================
        "/api/v0/views" => RoutePermission::Required(NodePermission::ViewAdd),
        "/api/v0/views/refresh" => RoutePermission::Required(NodePermission::ViewRefresh),

        // =====================================================================
        // Encrypted indexes (global list)
        // =====================================================================
        "/api/v0/encrypted-indexes" => {
            RoutePermission::Required(NodePermission::EncryptedIndexListAll)
        }

        // =====================================================================
        // Utility
        // =====================================================================
        "/api/v0/debug/dump" => RoutePermission::Required(NodePermission::DocumentRead),
        "/api/v0/purge" => RoutePermission::Required(NodePermission::DocumentUpdate),
        "/api/v0/node/identity" => RoutePermission::Required(NodePermission::P2pPeerConnect),

        // =====================================================================
        // Safe default for unknown routes
        // =====================================================================
        _ => {
            tracing::warn!(
                path = path,
                method = %method,
                "Route not in permission table — applying safe default (DocumentRead)"
            );
            RoutePermission::Required(NodePermission::DocumentRead)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            route_permission("/api/v0/collections/:name", &Method::GET),
            RoutePermission::Required(NodePermission::CollectionGet)
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
            route_permission("/api/v0/collections/:name/:docID", &Method::GET),
            RoutePermission::Required(NodePermission::DocumentRead)
        );
        assert_eq!(
            route_permission("/api/v0/collections/:name/:docID", &Method::PATCH),
            RoutePermission::Required(NodePermission::DocumentUpdate)
        );
        assert_eq!(
            route_permission("/api/v0/collections/:name/:docID", &Method::DELETE),
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
                Method::GET,
                RoutePermission::Required(NodePermission::CollectionGet),
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
                "/api/v0/collections/:name/:docID",
                Method::GET,
                RoutePermission::Required(NodePermission::DocumentRead),
            ),
            (
                "/api/v0/collections/:name/:docID",
                Method::PATCH,
                RoutePermission::Required(NodePermission::DocumentUpdate),
            ),
            (
                "/api/v0/collections/:name/:docID",
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
                "Mismatch for {} {} — expected {:?}, got {:?}",
                method, path, expected, actual
            );
        }
    }
}
