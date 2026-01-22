//! NAC (Node Access Control) integration tests.
//!
//! Tests that verify NAC permission checks work correctly at the HTTP layer.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    http::{header::AUTHORIZATION, header::HOST, Request, StatusCode},
};
use identity::{new_token, Did, Identity, RawIdentity};
use tower::ServiceExt;

use defra_http::{
    create_router_with_state, AppStateBuilder, MockNodeAcpOperations, MockQueryExecutor,
    MockRestOperations, NodePermission,
};

const TEST_HOST: &str = "localhost:9181";

/// Create a test token for the given identity.
fn create_test_token(identity: &RawIdentity) -> String {
    let token = new_token(
        identity,
        Duration::from_secs(3600),
        Some(TEST_HOST.to_lowercase()),
        None,
    )
    .unwrap();
    String::from_utf8(token).unwrap()
}

/// Create a test identity with DID.
fn create_test_identity() -> (RawIdentity, Did) {
    let private_key = crypto::generate_ed25519().unwrap();
    let identity = RawIdentity::from_private_key(private_key).unwrap();
    let did = identity.did().unwrap();
    (identity, did)
}

// ============================================================================
// NAC Not Configured Tests (Permissive Default)
// ============================================================================

#[tokio::test]
async fn test_nac_not_configured_allows_anonymous() {
    // NAC not configured = permissive default, anonymous allowed
    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        // No NAC configured
        .build();

    let app = create_router_with_state(state);

    // Anonymous request should succeed
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_nac_not_configured_allows_authenticated() {
    let (identity, _did) = create_test_identity();
    let token = create_test_token(&identity);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        // No NAC configured
        .build();

    let app = create_router_with_state(state);

    // Authenticated request should also succeed
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ============================================================================
// NAC Enabled Tests
// ============================================================================

#[tokio::test]
async fn test_nac_enabled_rejects_anonymous() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // Anonymous request should be rejected
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_nac_enabled_allows_owner() {
    let (owner_identity, owner_did) = create_test_identity();
    let token = create_test_token(&owner_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // Owner request should succeed
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_nac_enabled_rejects_non_owner() {
    let (owner_identity, owner_did) = create_test_identity();
    let (other_identity, _other_did) = create_test_identity();
    let _ = create_test_token(&owner_identity); // owner's token (unused)
    let other_token = create_test_token(&other_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // Non-owner request should be rejected
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", other_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_nac_enabled_allows_admin() {
    let (_, owner_did) = create_test_identity();
    let (admin_identity, admin_did) = create_test_identity();
    let admin_token = create_test_token(&admin_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did).with_admin(admin_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // Admin request should succeed
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", admin_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_nac_enabled_allows_specific_permission_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant only CollectionGet permission to the user
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::CollectionGet);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // User with CollectionGet permission can list collections
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ============================================================================
// NAC Disabled Tests
// ============================================================================

#[tokio::test]
async fn test_nac_disabled_allows_any_authenticated_user() {
    // Create a random user (not owner, not admin)
    let (user_identity, _user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // NAC is disabled temporarily
    let nac = MockNodeAcpOperations::disabled();

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // Any authenticated user should succeed when NAC is disabled
    // (even though they would normally be denied if NAC was enabled)
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ============================================================================
// Error Message Security Tests
// ============================================================================

#[tokio::test]
async fn test_error_message_does_not_leak_permission_name() {
    let (_, owner_did) = create_test_identity();
    let (other_identity, _) = create_test_identity();
    let other_token = create_test_token(&other_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", other_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // Read the body to check error message
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&body);

    // Error message should NOT contain permission names
    assert!(
        !body_str.contains("CollectionGet"),
        "Error message should not leak permission name"
    );
    assert!(
        !body_str.contains("collection_get"),
        "Error message should not leak permission name"
    );
}

#[tokio::test]
async fn test_error_message_does_not_leak_nac_status() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // Anonymous request
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&body);

    // Error message should NOT reveal NAC is enabled
    assert!(
        !body_str.contains("NAC is enabled"),
        "Error message should not leak NAC status"
    );
    assert!(
        !body_str.contains("when NAC is enabled"),
        "Error message should not leak NAC status"
    );
}

// ============================================================================
// NAC Status Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_nac_status_endpoint_requires_permission() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // Anonymous request to status endpoint should be rejected
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/nac/status")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_nac_status_endpoint_allowed_for_owner() {
    let (owner_identity, owner_did) = create_test_identity();
    let token = create_test_token(&owner_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // Owner request to status endpoint should succeed
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/nac/status")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ============================================================================
// Schema Endpoint NAC Tests
// ============================================================================

#[tokio::test]
async fn test_schema_endpoint_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // Anonymous request to schema endpoint should be rejected when NAC is enabled
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/schema")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_schema_endpoint_allows_owner() {
    let (owner_identity, owner_did) = create_test_identity();
    let token = create_test_token(&owner_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // Owner request to schema endpoint should succeed
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/schema")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_schema_endpoint_rejects_non_owner() {
    let (_, owner_did) = create_test_identity();
    let (other_identity, _) = create_test_identity();
    let other_token = create_test_token(&other_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // Non-owner request to schema endpoint should be rejected
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/schema")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", other_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_schema_endpoint_allows_user_with_collection_get_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant only CollectionGet permission to the user
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::CollectionGet);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // User with CollectionGet permission can access schema
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/schema")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_schema_endpoint_allows_anonymous_when_nac_not_configured() {
    // NAC not configured = permissive default
    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new())).build();

    let app = create_router_with_state(state);

    // Anonymous request should succeed when NAC is not configured
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/schema")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}
