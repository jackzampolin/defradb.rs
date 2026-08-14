use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use tower::ServiceExt;

use crate::mock::{MockBackupOperations, MockQueryExecutor};
use crate::server::{Server, ServerConfig};

const OVERSIZED: usize = 64;
const LIMIT: u64 = 8;

fn server(config: ServerConfig) -> Server {
    Server::with_config(MockQueryExecutor::new(), config)
        .with_backup_arc(std::sync::Arc::new(MockBackupOperations::default()))
}

async fn post(config: ServerConfig, uri: &str, body_len: usize) -> StatusCode {
    let router = server(config).router().expect("router builds");
    router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from("x".repeat(body_len)))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

fn schema_limited() -> ServerConfig {
    ServerConfig {
        max_schema_size: LIMIT,
        ..Default::default()
    }
}

fn backup_limited() -> ServerConfig {
    ServerConfig {
        max_backup_size: LIMIT,
        ..Default::default()
    }
}

#[tokio::test]
async fn schema_add_rejects_oversized_body() {
    assert_eq!(
        post(schema_limited(), "/api/v1/schema", OVERSIZED).await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn schema_add_accepts_body_within_limit() {
    let status = post(schema_limited(), "/api/v1/schema", LIMIT as usize).await;
    assert_ne!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "a body at the limit must not be rejected as too large"
    );
}

#[tokio::test]
async fn collection_schema_add_rejects_oversized_body() {
    assert_eq!(
        post(schema_limited(), "/api/v1/collections", OVERSIZED).await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

/// `/collections` carries get/patch/delete alongside the schema POST. The cap
/// is merged onto the POST alone, so patching a collection stays uncapped.
#[tokio::test]
async fn collection_patch_is_not_bound_by_the_schema_limit() {
    let router = server(schema_limited()).router().expect("router builds");
    let status = router
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri("/api/v1/collections")
                .header("content-type", "application/json")
                .body(Body::from("x".repeat(OVERSIZED)))
                .unwrap(),
        )
        .await
        .unwrap()
        .status();

    assert_ne!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

/// A view is defined by an SDL block that Go runs through the same
/// `ParseSDL` as a schema add (`internal/db/view.go:47` vs
/// `internal/db/collection.go:276`), so it is a schema request body.
#[tokio::test]
async fn view_add_rejects_oversized_body() {
    assert_eq!(
        post(schema_limited(), "/api/v1/views", OVERSIZED).await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn txn_scoped_schema_add_rejects_oversized_body() {
    assert_eq!(
        post(schema_limited(), "/api/v1/tx/1/schema", OVERSIZED).await,
        StatusCode::PAYLOAD_TOO_LARGE,
        "the transaction-scoped route must not be a bypass"
    );
}

#[tokio::test]
async fn backup_import_rejects_oversized_body() {
    assert_eq!(
        post(backup_limited(), "/api/v1/backup/import", OVERSIZED).await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn backup_export_is_not_bound_by_the_import_limit() {
    let status = post(backup_limited(), "/api/v1/backup/export", OVERSIZED).await;
    assert_ne!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "max_backup_size is documented as an import limit"
    );
}

/// Must exceed axum's 2 MiB default extractor cap, or the assertion holds
/// whether or not `0` actually means unlimited.
const ABOVE_AXUM_DEFAULT: usize = 3 * 1024 * 1024;

#[tokio::test]
async fn zero_means_unlimited() {
    let config = ServerConfig {
        max_schema_size: 0,
        max_body_size: 0,
        ..Default::default()
    };
    let status = post(config, "/api/v1/schema", ABOVE_AXUM_DEFAULT).await;
    assert_ne!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "0 must continue to mean unlimited, not fall back to axum's 2 MiB default"
    );
}

/// `--max-backup-size` used to be unable to raise the effective cap: the
/// handler carried its own hardcoded 100 MiB check that the route layer never
/// saw, so `0` did not mean unlimited and a larger flag value did nothing.
/// The bound now lives entirely in the flag, whose default preserves the
/// backstop the hardcoded check used to provide.
#[test]
fn backup_import_is_capped_by_default() {
    let config = ServerConfig::default();
    assert_eq!(
        config.max_backup_size,
        100 * 1024 * 1024,
        "a default node must keep the 100 MiB import backstop"
    );
    assert_eq!(
        server(config.clone()).route_body_limit(config.max_backup_size),
        Some(100 * 1024 * 1024)
    );
}

#[test]
fn explicit_zero_disables_the_backup_cap() {
    let config = ServerConfig {
        max_backup_size: 0,
        ..Default::default()
    };
    assert_eq!(
        server(config.clone()).route_body_limit(config.max_backup_size),
        None,
        "0 must mean unlimited, as the flag documents"
    );
}

/// The flag must be able to raise the cap, not only lower it.
#[tokio::test]
async fn backup_import_accepts_a_body_above_the_old_hardcoded_check() {
    let config = ServerConfig {
        max_backup_size: 0,
        ..Default::default()
    };
    let status = post(config, "/api/v1/backup/import", ABOVE_AXUM_DEFAULT).await;
    assert_ne!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

/// The interaction that could have made this whole fix inert: when
/// `max_body_size` is 0, `server.rs` applies `DefaultBodyLimit::disable()` to
/// the entire router, outside the per-route layers.
#[tokio::test]
async fn route_limit_survives_the_disabled_global_limit() {
    let config = ServerConfig {
        max_schema_size: LIMIT,
        max_body_size: 0,
        ..Default::default()
    };
    assert_eq!(
        post(config, "/api/v1/schema", OVERSIZED).await,
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

/// A per-route limit must never raise the effective cap above the global one.
#[tokio::test]
async fn route_limit_never_exceeds_the_global_limit() {
    let config = ServerConfig {
        max_schema_size: 1024,
        max_body_size: LIMIT,
        ..Default::default()
    };
    assert_eq!(
        post(config, "/api/v1/schema", OVERSIZED).await,
        StatusCode::PAYLOAD_TOO_LARGE,
        "global body limit must still bound a looser per-route limit"
    );
}

const TEST_DID: &str = "did:key:z6MkTestNodeIdentity";

fn signing_server(config: ServerConfig) -> Server {
    Server::with_config(MockQueryExecutor::new(), config).with_node_identity_did(TEST_DID.into())
}

/// Asserts through `app_state()` rather than the private predicate, so that
/// reverting `with_signing_enabled(self.signing_enabled())` back to a literal
/// `true` fails the test. Testing the predicate alone would reproduce the exact
/// defect this change fixes: a value computed correctly and never consumed.
#[test]
fn a_node_identity_enables_signing_by_default() {
    assert!(
        signing_server(ServerConfig::default())
            .app_state()
            .signing_enabled
    );
}

#[test]
fn no_signing_disables_signing_even_with_a_node_identity() {
    let config = ServerConfig {
        no_signing: true,
        ..Default::default()
    };
    assert!(
        !signing_server(config).app_state().signing_enabled,
        "--no-signing must be honoured; commits were signed regardless"
    );
}

#[test]
fn signing_stays_off_without_a_node_identity() {
    let server = Server::with_config(MockQueryExecutor::new(), ServerConfig::default());
    assert!(!server.app_state().signing_enabled);
}
