//! Router creation and route definitions.

use std::sync::Arc;

use axum::{
    routing::{delete, get, patch, post},
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
    // Health check at root level (matches Go DefraDB)
    let root_routes = Router::new().route("/health-check", get(handlers::health_check));

    // Transaction routes (Go-compatible)
    // Go DefraDB:
    //   POST /tx - begin transaction (query param: ?read_only=true)
    //   POST /tx/concurrent - begin concurrent transaction
    //   POST /tx/{id} - commit transaction
    //   DELETE /tx/{id} - discard transaction
    let tx_routes = Router::new()
        .route("/", post(handlers::tx_begin))
        .route("/concurrent", post(handlers::tx_begin_concurrent))
        .route("/:id", post(handlers::tx_commit))
        .route("/:id", delete(handlers::tx_discard))
        .route("/:id/lens", post(handlers::txn_ops::set_migration_in_txn))
        .route(
            "/:id/collections",
            get(handlers::txn_ops::get_collections_in_txn),
        );

    // Collection routes (REST API)
    // Static routes must come before parametric `:name` routes
    let collection_routes = Router::new()
        .route(
            "/",
            get(handlers::list_collections)
                .post(handlers::schema::add_schema)
                .patch(handlers::patch_collection),
        )
        .route("/default", post(handlers::set_active))
        .route(
            "/versions",
            get(handlers::get_all_collections).delete(handlers::delete_collection_versions),
        )
        .route("/migrations", post(handlers::lens::set_migration))
        .route("/by-id/:id", get(handlers::find_collection_by_id))
        .route(
            "/by-version/:id",
            get(handlers::get_collection_by_version_id),
        )
        // Go-compatible list-all-indexes route (no path param)
        .route("/indexes", get(handlers::index::go_list_all_indexes))
        .route(
            "/:name",
            post(handlers::create_document).delete(handlers::delete_collection),
        )
        .route("/:name/describe", get(handlers::describe_collection))
        .route("/:name/exists", get(handlers::collection_exists))
        .route("/:name/truncate", delete(handlers::truncate_collection))
        .route("/:name/document/:docID", get(handlers::get_document))
        .route("/:name/document/:docID", patch(handlers::update_document))
        .route("/:name/document/:docID", delete(handlers::delete_document))
        // Go-compatible index routes (collection in path)
        .route("/:name/indexes", get(handlers::index::go_list_indexes))
        .route("/:name/indexes", post(handlers::index::go_create_index))
        .route(
            "/:name/indexes/:index",
            delete(handlers::index::go_delete_index),
        )
        // Go-compatible encrypted index routes
        .route(
            "/:name/encrypted-indexes",
            get(handlers::encrypted_index::go_list_encrypted_indexes),
        )
        .route(
            "/:name/encrypted-indexes",
            post(handlers::encrypted_index::go_add_encrypted_index),
        )
        .route(
            "/:name/encrypted-indexes/:field",
            delete(handlers::encrypted_index::go_delete_encrypted_index),
        );

    // P2P routes
    let p2p_routes = Router::new()
        .route("/info", get(handlers::p2p::get_info))
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
        .route("/documents/sync", post(handlers::p2p::sync_documents)); // Go-compatible

    // ACP routes
    let acp_routes = Router::new()
        .route("/status", get(handlers::acp::get_status))
        .route("/policy", post(handlers::acp::add_policy))
        .route("/policy", get(handlers::acp::list_policies))
        .route("/policy/:id", get(handlers::acp::get_policy))
        .route(
            "/document/relationship",
            post(handlers::acp::add_doc_relationship),
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
        .route("/import", post(handlers::backup::import));

    // Block routes
    let block_routes =
        Router::new().route("/verify-signature", get(handlers::block::verify_signature));

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
        .route(
            "/relationship",
            delete(handlers::nac::go_remove_relationship),
        )
        .route("/disable", post(handlers::nac::disable))
        .route("/re-enable", post(handlers::nac::re_enable));

    // View routes
    let view_routes = Router::new()
        .route("/", post(handlers::views::add_view))
        .route("/refresh", post(handlers::views::refresh_views));

    // API v0 routes
    let api_routes = Router::new()
        // GraphQL endpoints
        .route("/graphql", post(handlers::graphql_transactional))
        .route("/graphql", get(handlers::graphql_get))
        .route(
            "/graphql/ws",
            axum::routing::any(handlers::graphql_ws_handler),
        )
        .route("/schema", get(handlers::schema))
        .route("/schema", post(handlers::schema::add_schema))
        .route("/version", get(handlers::version))
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

    root_routes.nest("/api/v0", api_routes)
}
