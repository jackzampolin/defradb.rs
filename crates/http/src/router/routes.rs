//! Router creation and route definitions.

use std::sync::Arc;

use axum::{
    extract::DefaultBodyLimit,
    routing::{delete, get, patch, post, MethodRouter},
    Router,
};

use query::executor::QueryExecutor;
use query::rest::RestOperations;

use super::{AppState, AppStateBuilder};
use crate::handlers;

/// Create the main router with all routes.
///
/// This creates a router with GraphQL endpoints only (no REST).
/// Use `create_router_with_rest` to include REST endpoints.
pub fn create_router(executor: Arc<dyn QueryExecutor>) -> Router {
    let state = AppStateBuilder::new(executor).build();
    create_router_with_state(state)
}

/// Create the main router with all routes including REST endpoints.
pub fn create_router_with_rest(
    executor: Arc<dyn QueryExecutor>,
    rest: Arc<dyn RestOperations>,
) -> Router {
    let state = AppStateBuilder::new(executor).with_rest(rest).build();
    create_router_with_state(state)
}

/// Create the main router with full AppState.
///
/// This allows configuring all optional components (REST, P2P, ACP, Index, Backup).
pub fn create_router_with_state(state: AppState) -> Router {
    create_router_with_state_and_body_limits(state, BodyLimits::unlimited())
}

/// Per-route request body caps, in bytes.
///
/// `None` leaves a route bound only by the global limit `server.rs` applies to
/// the whole router. Callers are responsible for clamping these against that
/// global limit -- a route-level cap overrides the global one rather than
/// intersecting with it, so a looser value here would raise the effective cap.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BodyLimits {
    pub sync: usize,
    pub schema: Option<usize>,
    pub backup_import: Option<usize>,
}

impl BodyLimits {
    pub(crate) fn unlimited() -> Self {
        Self {
            sync: defra_core::browser_sync::MAX_SYNC_BODY_BYTES,
            schema: None,
            backup_import: None,
        }
    }
}

/// Apply a body cap to a single method handler, or leave it uncapped.
fn capped(route: MethodRouter<AppState>, limit: Option<usize>) -> MethodRouter<AppState> {
    match limit {
        Some(bytes) => route.layer(DefaultBodyLimit::max(bytes)),
        None => route,
    }
}

pub(crate) fn create_router_with_state_and_body_limits(
    state: AppState,
    limits: BodyLimits,
) -> Router {
    // Health check at root level (matches Go DefraDB)
    let root_routes = Router::new().route("/health-check", get(handlers::health_check));

    // Transaction routes (Go-compatible)
    // Go DefraDB:
    //   POST /tx - begin transaction (query param: ?read_only=true)
    //   POST /tx/{id} - commit transaction
    //   DELETE /tx/{id} - discard transaction
    let tx_routes = Router::new()
        .route("/", post(handlers::tx_begin))
        .route("/{id}", post(handlers::tx_commit))
        .route("/{id}", delete(handlers::tx_discard))
        .route("/{id}/lens", post(handlers::txn_ops::set_migration_in_txn))
        .route(
            "/{id}/collections",
            get(handlers::txn_ops::get_collections_in_txn),
        )
        .route(
            "/{id}/schema",
            capped(post(handlers::txn_ops::add_schema_in_txn), limits.schema),
        );

    // Collection routes (REST API)
    // Static routes must come before parametric `:name` routes
    let collection_routes = Router::new()
        .route(
            "/",
            get(handlers::list_collections)
                .patch(handlers::patch_collection)
                .delete(handlers::delete_collections_by_names)
                // Merged rather than chained so the cap binds only the schema
                // POST, not the sibling patch/delete methods on this path.
                .merge(capped(post(handlers::schema::add_schema), limits.schema)),
        )
        .route("/default", post(handlers::set_active))
        .route(
            "/versions",
            get(handlers::get_all_collections).delete(handlers::delete_collection_versions),
        )
        .route("/migrations", post(handlers::lens::set_migration))
        .route("/by-id/{id}", get(handlers::find_collection_by_id))
        .route(
            "/by-version/{id}",
            get(handlers::get_collection_by_version_id),
        )
        // Go-compatible list-all-indexes route (no path param)
        .route("/indexes", get(handlers::index::go_list_all_indexes))
        .route(
            "/{name}",
            get(handlers::get_collection_doc_ids)
                .post(handlers::create_document)
                .delete(handlers::delete_collection),
        )
        .route("/{name}/describe", get(handlers::describe_collection))
        .route("/{name}/exists", get(handlers::collection_exists))
        .route("/{name}/truncate", delete(handlers::truncate_collection))
        .route("/{name}/document/{docID}", get(handlers::get_document))
        .route("/{name}/document/{docID}", patch(handlers::update_document))
        .route(
            "/{name}/document/{docID}",
            delete(handlers::delete_document),
        )
        // Go-compatible index routes (collection in path)
        .route("/{name}/indexes", get(handlers::index::go_list_indexes))
        .route("/{name}/indexes", post(handlers::index::go_create_index))
        .route(
            "/{name}/indexes/{index}",
            delete(handlers::index::go_delete_index),
        )
        // Go-compatible encrypted index routes
        .route(
            "/{name}/encrypted-indexes",
            get(handlers::encrypted_index::go_list_encrypted_indexes),
        )
        .route(
            "/{name}/encrypted-indexes",
            post(handlers::encrypted_index::go_add_encrypted_index),
        )
        .route(
            "/{name}/encrypted-indexes/{field}",
            delete(handlers::encrypted_index::go_delete_encrypted_index),
        );

    // P2P routes
    let p2p_routes = Router::new()
        .route("/info", get(handlers::p2p::get_info))
        .route("/sync/status", get(handlers::p2p::sync_status))
        .route(
            "/shareable-address",
            get(handlers::p2p::get_shareable_address),
        )
        .route("/active-peers", get(handlers::p2p::active_peers)) // Go-compatible
        .route("/connect", post(handlers::p2p::connect)) // Go-compatible
        .route("/peers", get(handlers::p2p::list_peers))
        .route("/peers", post(handlers::p2p::connect_peer)) // Legacy
        .route("/replicators", get(handlers::p2p::list_replicators)) // Go uses /replicators
        .route("/replicators", post(handlers::p2p::add_replicator))
        .route("/replicators", delete(handlers::p2p::remove_replicator))
        .route("/replicator", get(handlers::p2p::list_replicators)) // Legacy
        .route("/replicator", post(handlers::p2p::add_replicator))
        .route("/replicator", delete(handlers::p2p::remove_replicator))
        .route("/collections", get(handlers::p2p::list_collections))
        .route("/collections", post(handlers::p2p::add_collections))
        .route("/collections", delete(handlers::p2p::remove_collections))
        .route(
            "/collections/sync-branchable",
            post(handlers::p2p::sync_branchable),
        ) // Go-compatible
        .route(
            "/collections/sync-versions",
            post(handlers::p2p::sync_versions),
        ) // Go-compatible
        .route("/documents", get(handlers::p2p::list_documents)) // Go-compatible
        .route("/documents", post(handlers::p2p::add_documents))
        .route("/documents", delete(handlers::p2p::remove_documents))
        .route("/documents/sync", post(handlers::p2p::sync_documents)) // Go-compatible
        // Management relay: this node forwards a signed request to a P2P-only peer
        .route("/manage", post(handlers::p2p::manage))
        .route("/manage/query", post(handlers::p2p::manage_query));

    // ACP routes
    let acp_routes = Router::new()
        .route("/status", get(handlers::acp::get_status))
        // /acp/policy is Rust's original path, kept as an alias.
        .route("/document/policy", post(handlers::acp::add_policy))
        .route("/policy", post(handlers::acp::add_policy))
        .route("/policy", get(handlers::acp::list_policies))
        .route("/policy/{id}", get(handlers::acp::get_policy))
        .route("/document/decide", post(handlers::acp::decide_doc_access))
        .route(
            "/document/relationship",
            post(handlers::acp::add_doc_relationship),
        )
        .route(
            "/document/relationships",
            post(handlers::acp::add_doc_relationships),
        )
        .route(
            "/document/relationship",
            delete(handlers::acp::remove_doc_relationship),
        );

    // Index routes
    let index_routes = Router::new()
        .route("/", post(handlers::index::create_index))
        .route("/", get(handlers::index::list_indexes))
        .route("/", delete(handlers::index::delete_index));

    // Backup routes (POST for both to match Go DefraDB)
    let backup_routes = Router::new()
        .route("/export", post(handlers::backup::export))
        .route(
            "/import",
            capped(post(handlers::backup::import), limits.backup_import),
        );

    // Block routes
    let block_routes = Router::new()
        .route("/verify-signature", get(handlers::block::verify_signature))
        .route("/signed", get(handlers::block::signed_block));

    // Lens migration routes
    let lens_routes = Router::new()
        .route("/", post(handlers::lens::add_lens))
        .route("/", get(handlers::lens::list_lenses))
        .route("/set", post(handlers::lens::set_migration))
        .route("/reload", post(handlers::lens::reload));

    // Batch signing routes
    let batch_routes = Router::new()
        .route("/start", post(handlers::batch::batch_start))
        .route("/sign", post(handlers::batch::batch_sign))
        .route("/verify", post(handlers::batch::batch_verify));

    // NAC (Node Access Control) routes
    let nac_routes = Router::new()
        .route("/status", get(handlers::nac::get_status))
        .route("/admin", post(handlers::nac::add_admin))
        .route("/admin", delete(handlers::nac::remove_admin));

    // Go-compatible ACP node routes (aliased from /acp/node/*)
    // Go DefraDB uses:
    //   GET /acp/node/status
    //   POST /acp/node/relationship
    //   DELETE /acp/node/relationship
    //   POST /acp/node/disable
    //   POST /acp/node/re-enable
    let acp_node_routes = Router::new()
        .route("/status", get(handlers::nac::get_status))
        .route("/enable", post(handlers::nac::enable))
        .route("/relationship", post(handlers::nac::go_add_relationship))
        .route("/relationships", post(handlers::nac::go_add_relationships))
        .route(
            "/relationship",
            delete(handlers::nac::go_remove_relationship),
        )
        .route("/disable", post(handlers::nac::disable))
        .route("/re-enable", post(handlers::nac::re_enable));

    // /views is Rust's original mount and carries the Rust-only /gc route.
    // Deriving it from the Go set keeps the two mounts from drifting.
    let go_view_routes = Router::new()
        // A view is defined by an SDL block, which Go parses with the same
        // `ParseSDL` as a schema add (`internal/db/view.go:47` vs
        // `internal/db/collection.go:276`), so it is a schema request body.
        .route("/", capped(post(handlers::views::add_view), limits.schema))
        .route("/refresh", post(handlers::views::refresh_views));
    let view_routes = go_view_routes
        .clone()
        .route("/gc", post(handlers::views::gc_downsample_histories));

    // Versioned API routes
    let api_routes = Router::new()
        // GraphQL endpoints
        .route("/graphql", post(handlers::graphql_transactional))
        .route("/graphql", get(handlers::graphql_get))
        .route(
            "/graphql/ws",
            axum::routing::any(handlers::graphql_ws_handler),
        )
        .route("/schema", get(handlers::schema))
        .route(
            "/schema",
            capped(post(handlers::schema::add_schema), limits.schema),
        )
        .route("/version", get(handlers::version))
        .route("/actions", get(handlers::actions::list_actions))
        .route(
            "/sync",
            post(handlers::browser_sync::sync).layer(DefaultBodyLimit::max(limits.sync)),
        )
        // Transaction endpoints
        .nest("/tx", tx_routes)
        // REST collection endpoints
        .nest("/collections", collection_routes)
        // P2P endpoints
        .nest("/p2p", p2p_routes)
        // ACP endpoints (document-level access control)
        .nest("/acp", acp_routes)
        // Go-compatible ACP node routes (NAC via /acp/node/*)
        .nest("/acp/node", acp_node_routes)
        // Index endpoints
        .nest("/index", index_routes)
        // Backup endpoints
        .nest("/backup", backup_routes)
        // View endpoints
        .nest("/views", view_routes)
        .nest("/view", go_view_routes)
        // Block endpoints
        .nest("/block", block_routes)
        // Lens migration endpoints
        .nest("/lens", lens_routes)
        // Batch signing endpoints
        .nest("/batch", batch_routes)
        // NAC endpoints (Rust-native routes)
        .nest("/nac", nac_routes)
        // Go-compatible list-all encrypted indexes
        .route(
            "/encrypted-indexes",
            get(handlers::encrypted_index::go_list_all_encrypted_indexes),
        )
        // Event bus SSE endpoint
        .route("/events", get(handlers::events::events_sse))
        // Debug endpoints
        .route("/debug/dump", get(handlers::utility::dump))
        // Utility endpoints (Go-compatible)
        .route("/purge", post(handlers::utility::purge))
        .route("/node/identity", get(handlers::utility::get_node_identity))
        .with_state(state);

    crate::go_paths::API_PREFIXES
        .iter()
        .fold(root_routes, |router, prefix| {
            router.nest(prefix, api_routes.clone())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::{
        FailingMockP2POperations, MockDocumentAcpOperations, MockQueryExecutor, MockRestOperations,
    };
    use crate::router::{
        DocumentAcpOperations, ManageRequester, P2POperations, P2PResult, P2pDocumentInfo,
        P2pDocumentRequest, RemoteManageOp, RemoteManageQueryOp, RemoteManageQueryResult,
        ReplicatorInfo,
    };
    use axum::{
        body::{to_bytes, Body},
        http::{Method, Request, StatusCode},
    };
    use std::time::Duration;
    use tokio::sync::Notify;
    use tower::ServiceExt;

    struct BlockingAddCollectionsP2P {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait::async_trait]
    impl P2POperations for BlockingAddCollectionsP2P {
        async fn local_peer_id(&self) -> P2PResult<String> {
            Ok("blocking-peer".into())
        }

        async fn listen_addresses(&self) -> P2PResult<Vec<String>> {
            Ok(vec![])
        }

        async fn connected_peers(&self) -> P2PResult<Vec<String>> {
            Ok(vec![])
        }

        async fn connect_peer(&self, _addr: &str) -> P2PResult<()> {
            Ok(())
        }

        async fn disconnect_peer(&self, _addr: &str) -> P2PResult<()> {
            Ok(())
        }

        async fn get_replicators(&self) -> P2PResult<Vec<ReplicatorInfo>> {
            Ok(vec![])
        }

        async fn add_replicator(
            &self,
            _collections: Vec<String>,
            _addr: Option<&str>,
            _filters: crate::router::ReplicationFilters,
            _explicit_replay_capabilities: Vec<crate::router::ExplicitReplayCapabilityInput>,
            _expected_authorizer_did: Option<&str>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn remove_replicator(
            &self,
            _collections: Vec<String>,
            _addr: Option<&str>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn get_collections(&self) -> P2PResult<Vec<String>> {
            Ok(vec![])
        }

        async fn add_collections(&self, _collections: Vec<String>) -> P2PResult<()> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(())
        }

        async fn remove_collections(&self, _collections: Vec<String>) -> P2PResult<()> {
            Ok(())
        }

        async fn get_documents(&self) -> P2PResult<Vec<P2pDocumentInfo>> {
            Ok(vec![])
        }

        async fn add_documents(&self, _docs: Vec<P2pDocumentRequest>) -> P2PResult<()> {
            Ok(())
        }

        async fn remove_documents(&self, _docs: Vec<P2pDocumentRequest>) -> P2PResult<()> {
            Ok(())
        }

        async fn sync_documents(
            &self,
            _collection_name: &str,
            _doc_ids: Vec<String>,
            _timeout: Option<std::time::Duration>,
        ) -> P2PResult<()> {
            Ok(())
        }

        async fn sync_branchable_collection(&self, _collection_id: &str) -> P2PResult<()> {
            Ok(())
        }

        async fn sync_collection_versions(&self, _version_ids: Vec<String>) -> P2PResult<()> {
            Ok(())
        }
    }

    async fn status_for(path: &str) -> axum::http::StatusCode {
        let router = create_router(Arc::new(MockQueryExecutor::new()));
        router
            .oneshot(
                Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond")
            .status()
    }

    async fn status_for_p2p_request(method: Method, path: &str, body: &'static str) -> StatusCode {
        let executor = Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>;
        let p2p = Arc::new(FailingMockP2POperations::new("injected p2p failure"))
            as Arc<dyn P2POperations>;
        let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
        let router = create_router_with_state(state);
        router
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond")
            .status()
    }

    #[tokio::test]
    async fn version_route_resolves_under_v0_and_v1() {
        assert_eq!(
            status_for("/api/v0/version").await,
            axum::http::StatusCode::OK
        );
        assert_eq!(
            status_for("/api/v1/version").await,
            axum::http::StatusCode::OK
        );
    }

    #[tokio::test]
    async fn v0_and_v1_resolve_same_route_set() {
        for path in [
            "/graphql",
            "/schema",
            "/collections",
            "/p2p/info",
            "/acp/status",
            "/index",
            "/encrypted-indexes",
            "/node/identity",
        ] {
            let v0_status = status_for(&format!("/api/v0{path}")).await;
            let v1_status = status_for(&format!("/api/v1{path}")).await;
            assert_ne!(v0_status, axum::http::StatusCode::NOT_FOUND);
            assert_eq!(v1_status, v0_status, "route mismatch for {path}");
        }
    }

    #[tokio::test]
    async fn collection_doc_ids_route_is_available_with_rest() {
        let executor = Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>;
        let rest = Arc::new(MockRestOperations::new()) as Arc<dyn RestOperations>;
        let router = create_router_with_rest(executor, rest);

        for path in ["/api/v0/collections/Users", "/api/v1/collections/Users"] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .body(Body::empty())
                        .expect("request should build"),
                )
                .await
                .expect("router should respond");

            assert_eq!(response.status(), axum::http::StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn p2p_collection_add_does_not_ack_when_operation_fails() {
        let status =
            status_for_p2p_request(Method::POST, "/api/v0/p2p/collections", r#"["Users"]"#).await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn document_acp_decide_route_returns_decision() {
        let executor = Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>;
        let doc_acp = Arc::new(MockDocumentAcpOperations::with_allowed(false))
            as Arc<dyn DocumentAcpOperations>;
        let state = AppStateBuilder::new(executor).with_doc_acp(doc_acp).build();
        let router = create_router_with_state(state);

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v0/acp/document/decide")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{
                            "actor":"did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
                            "permission":"read",
                            "policy_id":"policy",
                            "resource_name":"resource",
                            "doc_id":"doc"
                        }"#,
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable");
        assert_eq!(&body[..], br#"{"allowed":false}"#);
    }

    #[tokio::test]
    async fn p2p_collection_add_waits_for_operation_before_ack() {
        let executor = Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>;
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let p2p = Arc::new(BlockingAddCollectionsP2P {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        }) as Arc<dyn P2POperations>;
        let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
        let router = create_router_with_state(state);

        let response_task = tokio::spawn(
            router.oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v0/p2p/collections")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"["Users"]"#))
                    .expect("request should build"),
            ),
        );

        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("P2P operation should start");
        assert!(
            !response_task.is_finished(),
            "handler responded before P2P operation completed"
        );

        release.notify_one();
        let response = response_task
            .await
            .expect("router task should join")
            .expect("router should respond");
        assert_eq!(response.status(), StatusCode::OK);
    }

    struct DenyingManageRequester;

    #[async_trait::async_trait]
    impl ManageRequester for DenyingManageRequester {
        async fn manage(
            &self,
            _target_addr: &str,
            _auth_token: Vec<u8>,
            _op: RemoteManageOp,
        ) -> Result<(), String> {
            Err("unauthorized".to_string())
        }

        async fn manage_query(
            &self,
            _target_addr: &str,
            _auth_token: Vec<u8>,
            _op: RemoteManageQueryOp,
        ) -> Result<RemoteManageQueryResult, String> {
            Err("unauthorized".to_string())
        }
    }

    async fn manage_status_with_state(state: AppState, body: &'static str) -> StatusCode {
        let router = create_router_with_state(state);
        router
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v0/p2p/manage")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond")
            .status()
    }

    #[tokio::test]
    async fn p2p_manage_returns_service_unavailable_when_unset() {
        let executor = Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>;
        let state = AppStateBuilder::new(executor).build();
        let body = r#"{"Target":"addr","AuthToken":"tok","Op":{"Kind":"CollectionAdd","collection_ids":["Users"]}}"#;
        assert_eq!(
            manage_status_with_state(state, body).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn p2p_manage_maps_unauthorized_to_forbidden() {
        let executor = Arc::new(MockQueryExecutor::new()) as Arc<dyn QueryExecutor>;
        let manage = Arc::new(DenyingManageRequester) as Arc<dyn ManageRequester>;
        let state = AppStateBuilder::new(executor).with_manage(manage).build();
        let body = r#"{"Target":"addr","AuthToken":"tok","Op":{"Kind":"CollectionAdd","collection_ids":["Users"]}}"#;
        assert_eq!(
            manage_status_with_state(state, body).await,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn p2p_replicator_add_does_not_ack_when_operation_fails() {
        let status = status_for_p2p_request(
            Method::POST,
            "/api/v0/p2p/replicators",
            r#"{"Collections":["Users"]}"#,
        )
        .await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn p2p_replicator_forget_reaches_backend_without_collections() {
        let status = status_for_p2p_request(
            Method::DELETE,
            "/api/v0/p2p/replicators",
            r#"{"ID":"peer","Forget":true}"#,
        )
        .await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn p2p_replicator_forget_rejects_collections() {
        let status = status_for_p2p_request(
            Method::DELETE,
            "/api/v0/p2p/replicators",
            r#"{"ID":"peer","Collections":["Users"],"Forget":true}"#,
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn p2p_replicator_forget_requires_nonempty_id() {
        for body in [r#"{"Forget":true}"#, r#"{"ID":"  ","Forget":true}"#] {
            let status =
                status_for_p2p_request(Method::DELETE, "/api/v0/p2p/replicators", body).await;

            assert_eq!(status, StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn p2p_replicator_delete_without_collections_is_not_forget() {
        let status = status_for_p2p_request(
            Method::DELETE,
            "/api/v0/p2p/replicators",
            r#"{"ID":"peer"}"#,
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
