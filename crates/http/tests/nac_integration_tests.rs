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
    create_router_with_state, AppStateBuilder, FailingMockNodeAcpOperations,
    MockAcpOperations, MockBackupOperations, MockIndexOperations, MockNodeAcpOperations,
    MockP2POperations, MockQueryExecutor, MockRestOperations, NodePermission,
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

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

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

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

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

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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

// ============================================================================
// Document CRUD Endpoint NAC Tests
// ============================================================================

#[tokio::test]
async fn test_get_document_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections/users/doc123")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_get_document_allows_owner() {
    let (owner_identity, owner_did) = create_test_identity();
    let token = create_test_token(&owner_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections/users/doc123")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // May be 200 or 404 depending on mock, but NOT 403
    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_get_document_allows_user_with_document_read_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DocumentRead);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/collections/users/doc123")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should not be forbidden - may be 200 or 404
    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_create_document_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/collections/users")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name": "test"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_create_document_allows_owner() {
    let (owner_identity, owner_did) = create_test_identity();
    let token = create_test_token(&owner_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/collections/users")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name": "test"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Should not be forbidden
    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_create_document_requires_document_update_permission() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant only DocumentRead - NOT DocumentUpdate
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DocumentRead);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // User with only DocumentRead should be denied for create
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/collections/users")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name": "test"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_create_document_allows_user_with_document_update_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DocumentUpdate);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/collections/users")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name": "test"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Should not be forbidden
    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_update_document_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v0/collections/users/doc123")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name": "updated"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_update_document_allows_user_with_document_update_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DocumentUpdate);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v0/collections/users/doc123")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name": "updated"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Should not be forbidden
    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_delete_document_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v0/collections/users/doc123")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_delete_document_allows_user_with_document_delete_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DocumentDelete);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_rest(Arc::new(MockRestOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v0/collections/users/doc123")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should not be forbidden
    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

// ============================================================================
// Backup Endpoint NAC Tests
// ============================================================================

#[tokio::test]
async fn test_backup_export_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_backup(Arc::new(MockBackupOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/backup/export")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_backup_export_allows_owner() {
    let (owner_identity, owner_did) = create_test_identity();
    let token = create_test_token(&owner_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_backup(Arc::new(MockBackupOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/backup/export")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_backup_export_allows_user_with_document_read_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DocumentRead);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_backup(Arc::new(MockBackupOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/backup/export")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_backup_import_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_backup(Arc::new(MockBackupOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/backup/import")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"collections": {}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_backup_import_allows_user_with_document_update_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DocumentUpdate);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_backup(Arc::new(MockBackupOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/backup/import")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"collections": {}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

// ============================================================================
// NAC Admin Management Tests
// ============================================================================

#[tokio::test]
async fn test_nac_add_admin_rejects_anonymous() {
    let (_, owner_did) = create_test_identity();
    let (_, target_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({ "target": target_did.to_string() });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/nac/admin")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_nac_add_admin_allows_owner() {
    let (owner_identity, owner_did) = create_test_identity();
    let (_, target_did) = create_test_identity();
    let token = create_test_token(&owner_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({ "target": target_did.to_string() });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/nac/admin")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    // Should not be forbidden (may be 200 or other status)
    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_nac_remove_admin_rejects_anonymous() {
    let (_, owner_did) = create_test_identity();
    let (_, target_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({ "target": target_did.to_string() });
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v0/nac/admin")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_nac_remove_admin_allows_owner() {
    let (owner_identity, owner_did) = create_test_identity();
    let (_, target_did) = create_test_identity();
    let token = create_test_token(&owner_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did.clone())
        .with_admin(target_did.clone());

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({ "target": target_did.to_string() });
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v0/nac/admin")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

// ============================================================================
// Index Endpoint NAC Tests
// ============================================================================

#[tokio::test]
async fn test_create_index_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_index(Arc::new(MockIndexOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "collection": "users",
        "fields": ["name"]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/index")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_create_index_allows_user_with_index_create_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::IndexCreate);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_index(Arc::new(MockIndexOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "collection": "users",
        "fields": ["name"]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/index")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_list_indexes_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_index(Arc::new(MockIndexOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/index")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_list_indexes_allows_user_with_index_list_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::IndexList);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_index(Arc::new(MockIndexOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/index")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_drop_index_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_index(Arc::new(MockIndexOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // drop_index uses query params, not JSON body
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v0/index?collection=users&name=users_name_idx")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_drop_index_allows_user_with_index_drop_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::IndexDrop);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_index(Arc::new(MockIndexOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // drop_index uses query params, not JSON body
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v0/index?collection=users&name=users_name_idx")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

// ============================================================================
// P2P Endpoint NAC Tests
// ============================================================================

#[tokio::test]
async fn test_p2p_info_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_p2p(Arc::new(MockP2POperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/p2p/info")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_p2p_info_allows_user_with_peer_connect_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::P2pPeerConnect);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_p2p(Arc::new(MockP2POperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/p2p/info")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_p2p_list_peers_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_p2p(Arc::new(MockP2POperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/p2p/peers")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_p2p_connect_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_p2p(Arc::new(MockP2POperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // connect expects array of multiaddr strings
    let body = serde_json::json!(["/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWTest"]);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/p2p/connect")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_p2p_replicators_list_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_p2p(Arc::new(MockP2POperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/p2p/replicators")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_p2p_replicators_list_allows_user_with_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::P2pReplicatorList);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_p2p(Arc::new(MockP2POperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/p2p/replicators")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_p2p_add_replicator_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_p2p(Arc::new(MockP2POperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // ReplicatorRequest uses Go-compatible capitalized keys
    let body = serde_json::json!({
        "Collections": ["users"],
        "Addresses": ["/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWTest"]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/p2p/replicators")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_p2p_remove_replicator_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_p2p(Arc::new(MockP2POperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "collections": ["users"]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v0/p2p/replicator")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_p2p_collections_list_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_p2p(Arc::new(MockP2POperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/p2p/collections")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_p2p_add_collections_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_p2p(Arc::new(MockP2POperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "collections": ["users"]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/p2p/collections")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_p2p_remove_collections_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_p2p(Arc::new(MockP2POperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "collections": ["users"]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v0/p2p/collections")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

// ============================================================================
// NAC Error Path Tests (Internal Errors)
// ============================================================================

#[tokio::test]
async fn test_nac_internal_error_returns_500() {
    let (user_identity, _) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Use failing mock that returns errors for all permission checks
    let nac = FailingMockNodeAcpOperations::new("internal database error");

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
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Internal NAC errors should return 500, not 403
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_nac_internal_error_does_not_leak_error_details() {
    let (user_identity, _) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    let nac = FailingMockNodeAcpOperations::new("secret internal error details");

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
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&body);

    // Error response should NOT contain the internal error details
    assert!(
        !body_str.contains("secret internal error"),
        "Error response should not leak internal error details"
    );
}

// ============================================================================
// ACP Policy Endpoint NAC Tests
// ============================================================================

#[tokio::test]
async fn test_acp_add_policy_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_acp(Arc::new(MockAcpOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "policy": "name: test\nresources: {}"
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/acp/policy")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_acp_list_policies_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_acp(Arc::new(MockAcpOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_acp_get_policy_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_acp(Arc::new(MockAcpOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy/policy123")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_acp_allows_owner() {
    let (owner_identity, owner_did) = create_test_identity();
    let token = create_test_token(&owner_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_acp(Arc::new(MockAcpOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::UNAUTHORIZED);
}

// ============================================================================
// ACP Policy Cross-Permission Tests
// ============================================================================
// These tests verify that granting one permission does NOT grant another,
// ensuring proper permission isolation.

#[tokio::test]
async fn test_acp_add_policy_allows_user_with_dac_policy_add_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant ONLY DacPolicyAdd - user should be able to add policies
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DacPolicyAdd);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_acp(Arc::new(MockAcpOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "policy": "name: test\nresources: {}"
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/acp/policy")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    // Should succeed (not forbidden)
    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "User with DacPolicyAdd grant should be able to add policies"
    );
}

#[tokio::test]
async fn test_acp_list_policies_allows_user_with_dac_status_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant ONLY DacStatus - user should be able to list policies
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DacStatus);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_acp(Arc::new(MockAcpOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should succeed (not forbidden)
    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "User with DacStatus grant should be able to list policies"
    );
}

#[tokio::test]
async fn test_acp_get_policy_allows_user_with_dac_status_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant ONLY DacStatus - user should be able to get policies
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DacStatus);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_acp(Arc::new(MockAcpOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy/policy123")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should succeed (not forbidden) - may return 404 if policy doesn't exist, but not 403
    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "User with DacStatus grant should be able to get policies"
    );
}

#[tokio::test]
async fn test_acp_add_policy_requires_dac_policy_add_not_dac_status() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant ONLY DacStatus - user should NOT be able to add policies
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DacStatus);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_acp(Arc::new(MockAcpOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "policy": "name: test\nresources: {}"
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/acp/policy")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    // Should be forbidden - DacStatus doesn't grant DacPolicyAdd
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "User with only DacStatus should NOT be able to add policies"
    );
}

#[tokio::test]
async fn test_acp_list_policies_requires_dac_status_not_dac_policy_add() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant ONLY DacPolicyAdd - user should NOT be able to list policies
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DacPolicyAdd);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_acp(Arc::new(MockAcpOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should be forbidden - DacPolicyAdd doesn't grant DacStatus
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "User with only DacPolicyAdd should NOT be able to list policies"
    );
}

#[tokio::test]
async fn test_acp_get_policy_requires_dac_status_not_dac_policy_add() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant ONLY DacPolicyAdd - user should NOT be able to get policies
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DacPolicyAdd);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_acp(Arc::new(MockAcpOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy/policy123")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should be forbidden - DacPolicyAdd doesn't grant DacStatus
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "User with only DacPolicyAdd should NOT be able to get policies"
    );
}

#[tokio::test]
async fn test_acp_admin_can_perform_all_operations() {
    let (_, owner_did) = create_test_identity();
    let (admin_identity, admin_did) = create_test_identity();
    let admin_token = create_test_token(&admin_identity);

    // Admin should be able to perform all ACP operations without explicit grants
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did).with_admin(admin_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_acp(Arc::new(MockAcpOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // Test list policies
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", admin_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "Admin should be able to list policies"
    );

    // Test add policy
    let body = serde_json::json!({
        "policy": "name: test\nresources: {}"
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/acp/policy")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .header(AUTHORIZATION, format!("Bearer {}", admin_token))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "Admin should be able to add policies"
    );
}

#[tokio::test]
async fn test_acp_user_with_both_grants_can_do_all_operations() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // User with BOTH DacPolicyAdd AND DacStatus should be able to do all ACP operations
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did.clone(), NodePermission::DacPolicyAdd)
        .with_grant(user_did, NodePermission::DacStatus);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_acp(Arc::new(MockAcpOperations::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    // Test list policies (requires DacStatus)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "User with both grants should be able to list policies"
    );

    // Test add policy (requires DacPolicyAdd)
    let body = serde_json::json!({
        "policy": "name: test\nresources: {}"
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/acp/policy")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "User with both grants should be able to add policies"
    );
}

// ============================================================================
// GraphQL Endpoint NAC Tests
// ============================================================================
// These tests verify that GraphQL endpoints respect NAC permissions.

#[tokio::test]
async fn test_graphql_post_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "query": "{ __typename }"
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/graphql")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "GraphQL POST should reject anonymous requests when NAC is enabled"
    );
}

#[tokio::test]
async fn test_graphql_get_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/graphql?query=%7B%20__typename%20%7D")
                .header(HOST, TEST_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "GraphQL GET should reject anonymous requests when NAC is enabled"
    );
}

#[tokio::test]
async fn test_graphql_post_allows_owner() {
    let (owner_identity, owner_did) = create_test_identity();
    let token = create_test_token(&owner_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "query": "{ __typename }"
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/graphql")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "GraphQL POST should allow owner"
    );
}

#[tokio::test]
async fn test_graphql_get_allows_user_with_document_read_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant DocumentRead - should be able to use GraphQL GET
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DocumentRead);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/graphql?query=%7B%20__typename%20%7D")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "GraphQL GET should allow user with DocumentRead grant"
    );
}

#[tokio::test]
async fn test_graphql_post_query_allows_user_with_document_read_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant DocumentRead - should be able to use GraphQL POST for read-only queries
    // (Go DefraDB compatibility: permission is determined by operation type)
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DocumentRead);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "query": "{ __typename }"
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/graphql")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "GraphQL POST with read-only query should allow user with DocumentRead grant"
    );
}

#[tokio::test]
async fn test_graphql_get_requires_document_read_not_document_update() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant ONLY DocumentUpdate - should NOT be able to use GraphQL GET (requires DocumentRead)
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DocumentUpdate);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v0/graphql?query=%7B%20__typename%20%7D")
                .header(HOST, TEST_HOST)
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "GraphQL GET should require DocumentRead, not DocumentUpdate"
    );
}

#[tokio::test]
async fn test_graphql_post_mutation_requires_document_update_not_document_read() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant ONLY DocumentRead - should NOT be able to use GraphQL POST for mutations
    // (Go DefraDB compatibility: mutations require DocumentUpdate)
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DocumentRead);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "query": "mutation { create_Users(input: [{name: \"Test\"}]) { _docID } }"
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/graphql")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "GraphQL POST with mutation should require DocumentUpdate, not DocumentRead"
    );
}

// ============================================================================
// Transaction Endpoint NAC Tests
// ============================================================================
// These tests verify that transaction endpoints respect NAC permissions.

#[tokio::test]
async fn test_tx_begin_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "readonly": false
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/tx/begin")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "tx_begin should reject anonymous requests when NAC is enabled"
    );
}

#[tokio::test]
async fn test_tx_commit_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "txn_id": "fake-txn-id"
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/tx/commit")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "tx_commit should reject anonymous requests when NAC is enabled"
    );
}

#[tokio::test]
async fn test_tx_rollback_rejects_anonymous_when_nac_enabled() {
    let (_, owner_did) = create_test_identity();

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "txn_id": "fake-txn-id"
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/tx/rollback")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "tx_rollback should reject anonymous requests when NAC is enabled"
    );
}

#[tokio::test]
async fn test_tx_begin_allows_owner() {
    let (owner_identity, owner_did) = create_test_identity();
    let token = create_test_token(&owner_identity);

    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "readonly": false
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/tx/begin")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "tx_begin should allow owner"
    );
}

#[tokio::test]
async fn test_tx_begin_allows_user_with_document_update_grant() {
    let (_, owner_did) = create_test_identity();
    let (user_identity, user_did) = create_test_identity();
    let user_token = create_test_token(&user_identity);

    // Grant DocumentUpdate - should be able to begin transactions
    let nac = MockNodeAcpOperations::enabled_with_owner(owner_did)
        .with_grant(user_did, NodePermission::DocumentUpdate);

    let state = AppStateBuilder::new(Arc::new(MockQueryExecutor::new()))
        .with_nac(Arc::new(nac))
        .build();

    let app = create_router_with_state(state);

    let body = serde_json::json!({
        "readonly": false
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/tx/begin")
                .header(HOST, TEST_HOST)
                .header("content-type", "application/json")
                .header(AUTHORIZATION, format!("Bearer {}", user_token))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "tx_begin should allow user with DocumentUpdate grant"
    );
}
