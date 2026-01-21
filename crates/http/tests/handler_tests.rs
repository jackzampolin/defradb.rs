//! Integration tests for ACP, Index, Backup, and P2P handlers.

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;

use defra_http::mock::{
    FailingMockAcpOperations, FailingMockBackupOperations, FailingMockIndexOperations,
    FailingMockP2POperations, MockAcpOperations, MockBackupOperations, MockIndexOperations,
    MockP2POperations, MockQueryExecutor,
};
use defra_http::{create_router_with_state, AppStateBuilder};

// ============================================================================
// P2P Handler Tests
// ============================================================================

#[tokio::test]
async fn test_p2p_info_without_p2p_enabled() {
    let executor = Arc::new(MockQueryExecutor::new());
    let state = AppStateBuilder::new(executor).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/p2p/info")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 503 Service Unavailable when P2P not configured
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_p2p_info_with_p2p_enabled() {
    let executor = Arc::new(MockQueryExecutor::new());
    let p2p = Arc::new(MockP2POperations::new());
    let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/p2p/info")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let info: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(info.get("id").is_some());
    assert!(info.get("addresses").is_some());
}

#[tokio::test]
async fn test_p2p_list_peers() {
    let executor = Arc::new(MockQueryExecutor::new());
    let p2p = Arc::new(MockP2POperations::new().with_peer("12D3KooWTestPeer"));
    let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/p2p/peers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let peers: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(peers.len(), 1);
}

#[tokio::test]
async fn test_p2p_connect_peer() {
    let executor = Arc::new(MockQueryExecutor::new());
    let p2p = Arc::new(MockP2POperations::new());
    let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/p2p/peers")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"address": "/ip4/127.0.0.1/tcp/9000/p2p/12D3KooWTestPeer"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_p2p_connect_peer_invalid_address() {
    let executor = Arc::new(MockQueryExecutor::new());
    let p2p = Arc::new(MockP2POperations::new());
    let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/p2p/peers")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"address": "invalid-address"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_p2p_add_replicator() {
    let executor = Arc::new(MockQueryExecutor::new());
    let p2p = Arc::new(MockP2POperations::new());
    let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/p2p/replicator")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"collections": ["Users"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_p2p_add_replicator_empty_collections() {
    let executor = Arc::new(MockQueryExecutor::new());
    let p2p = Arc::new(MockP2POperations::new());
    let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/p2p/replicator")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"collections": []}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_p2p_add_collections() {
    let executor = Arc::new(MockQueryExecutor::new());
    let p2p = Arc::new(MockP2POperations::new());
    let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/p2p/collections")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"collections": ["Users", "Posts"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ============================================================================
// ACP Handler Tests
// ============================================================================

#[tokio::test]
async fn test_acp_without_acp_enabled() {
    let executor = Arc::new(MockQueryExecutor::new());
    let state = AppStateBuilder::new(executor).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 503 Service Unavailable when ACP not configured
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_acp_add_policy() {
    let executor = Arc::new(MockQueryExecutor::new());
    let acp = Arc::new(MockAcpOperations::new());
    let state = AppStateBuilder::new(executor).with_acp(acp).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/acp/policy")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"policy": "name: test\nresources: []"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(result.get("PolicyID").is_some());
}

#[tokio::test]
async fn test_acp_list_policies() {
    let executor = Arc::new(MockQueryExecutor::new());
    let acp = Arc::new(MockAcpOperations::new().with_policy("policy-1", Some("Test Policy")));
    let state = AppStateBuilder::new(executor).with_acp(acp).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let policies: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(policies.len(), 1);
    assert_eq!(policies[0].get("id").unwrap(), "policy-1");
}

#[tokio::test]
async fn test_acp_get_policy() {
    let executor = Arc::new(MockQueryExecutor::new());
    let acp = Arc::new(MockAcpOperations::new().with_policy("policy-1", Some("Test Policy")));
    let state = AppStateBuilder::new(executor).with_acp(acp).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy/policy-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let policy: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(policy.get("id").unwrap(), "policy-1");
}

#[tokio::test]
async fn test_acp_get_policy_not_found() {
    let executor = Arc::new(MockQueryExecutor::new());
    let acp = Arc::new(MockAcpOperations::new());
    let state = AppStateBuilder::new(executor).with_acp(acp).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// ============================================================================
// Index Handler Tests
// ============================================================================

#[tokio::test]
async fn test_index_without_index_enabled() {
    let executor = Arc::new(MockQueryExecutor::new());
    let state = AppStateBuilder::new(executor).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/index")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 503 Service Unavailable when Index not configured
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_index_create() {
    let executor = Arc::new(MockQueryExecutor::new());
    let index = Arc::new(MockIndexOperations::new());
    let state = AppStateBuilder::new(executor).with_index(index).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/index")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"collection": "Users", "fields": ["name", "email"], "unique": true}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let index_info: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(index_info.get("name").is_some());
    assert_eq!(index_info.get("collection").unwrap(), "Users");
    assert_eq!(index_info.get("unique").unwrap(), true);
}

#[tokio::test]
async fn test_index_create_empty_fields() {
    let executor = Arc::new(MockQueryExecutor::new());
    let index = Arc::new(MockIndexOperations::new());
    let state = AppStateBuilder::new(executor).with_index(index).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/index")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"collection": "Users", "fields": []}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_index_create_invalid_collection_name() {
    let executor = Arc::new(MockQueryExecutor::new());
    let index = Arc::new(MockIndexOperations::new());
    let state = AppStateBuilder::new(executor).with_index(index).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/index")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"collection": "123Invalid", "fields": ["name"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_index_list() {
    let executor = Arc::new(MockQueryExecutor::new());
    let index =
        Arc::new(MockIndexOperations::new().with_index("Users", "idx_name", vec!["name"], false));
    let state = AppStateBuilder::new(executor).with_index(index).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/index")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let indexes: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(indexes.len(), 1);
}

#[tokio::test]
async fn test_index_list_filtered_by_collection() {
    let executor = Arc::new(MockQueryExecutor::new());
    let index = Arc::new(
        MockIndexOperations::new()
            .with_index("Users", "idx_name", vec!["name"], false)
            .with_index("Posts", "idx_title", vec!["title"], false),
    );
    let state = AppStateBuilder::new(executor).with_index(index).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/index?collection=Users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let indexes: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].get("collection").unwrap(), "Users");
}

#[tokio::test]
async fn test_index_list_invalid_collection_name() {
    let executor = Arc::new(MockQueryExecutor::new());
    let index = Arc::new(MockIndexOperations::new());
    let state = AppStateBuilder::new(executor).with_index(index).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/index?collection=123Invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_index_drop() {
    let executor = Arc::new(MockQueryExecutor::new());
    let index =
        Arc::new(MockIndexOperations::new().with_index("Users", "idx_name", vec!["name"], false));
    let state = AppStateBuilder::new(executor).with_index(index).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v0/index?collection=Users&name=idx_name")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ============================================================================
// Backup Handler Tests
// ============================================================================

#[tokio::test]
async fn test_backup_without_backup_enabled() {
    let executor = Arc::new(MockQueryExecutor::new());
    let state = AppStateBuilder::new(executor).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/backup/export")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 503 Service Unavailable when Backup not configured
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_backup_export() {
    let executor = Arc::new(MockQueryExecutor::new());
    let backup = Arc::new(MockBackupOperations::new());
    let state = AppStateBuilder::new(executor).with_backup(backup).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/backup/export")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let data: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(data.get("Users").is_some());
}

#[tokio::test]
async fn test_backup_export_pretty() {
    let executor = Arc::new(MockQueryExecutor::new());
    let backup = Arc::new(MockBackupOperations::new());
    let state = AppStateBuilder::new(executor).with_backup(backup).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/backup/export?pretty=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&body);
    // Pretty printed JSON should have newlines
    assert!(body_str.contains('\n'));
}

#[tokio::test]
async fn test_backup_import() {
    let executor = Arc::new(MockQueryExecutor::new());
    let backup = Arc::new(MockBackupOperations::new());
    let state = AppStateBuilder::new(executor).with_backup(backup).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/backup/import")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"Users": [{"_docID": "bae-456", "name": "Bob"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result.get("success").unwrap(), true);
    assert_eq!(result.get("documents_imported").unwrap(), 1);
}

#[tokio::test]
async fn test_backup_import_empty_body() {
    let executor = Arc::new(MockQueryExecutor::new());
    let backup = Arc::new(MockBackupOperations::new());
    let state = AppStateBuilder::new(executor).with_backup(backup).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/backup/import")
                .header("content-type", "application/json")
                .body(Body::from(""))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_backup_import_empty_json() {
    let executor = Arc::new(MockQueryExecutor::new());
    let backup = Arc::new(MockBackupOperations::new());
    let state = AppStateBuilder::new(executor).with_backup(backup).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/backup/import")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_backup_import_invalid_json() {
    let executor = Arc::new(MockQueryExecutor::new());
    let backup = Arc::new(MockBackupOperations::new());
    let state = AppStateBuilder::new(executor).with_backup(backup).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/backup/import")
                .header("content-type", "application/json")
                .body(Body::from("{invalid json}"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_backup_import_primitive_json() {
    let executor = Arc::new(MockQueryExecutor::new());
    let backup = Arc::new(MockBackupOperations::new());
    let state = AppStateBuilder::new(executor).with_backup(backup).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/backup/import")
                .header("content-type", "application/json")
                .body(Body::from("\"just a string\""))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ============================================================================
// Error Path Tests with Failing Mocks
// ============================================================================

#[tokio::test]
async fn test_p2p_info_internal_error() {
    let executor = Arc::new(MockQueryExecutor::new());
    let p2p = Arc::new(FailingMockP2POperations::new("P2P service unavailable"));
    let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/p2p/info")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_p2p_connect_peer_internal_error() {
    let executor = Arc::new(MockQueryExecutor::new());
    let p2p = Arc::new(FailingMockP2POperations::new("Connection refused"));
    let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/p2p/peers")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"address": "/ip4/127.0.0.1/tcp/9000/p2p/12D3KooWTestPeer"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_acp_add_policy_internal_error() {
    let executor = Arc::new(MockQueryExecutor::new());
    let acp = Arc::new(FailingMockAcpOperations::new("Policy validation failed"));
    let state = AppStateBuilder::new(executor).with_acp(acp).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/acp/policy")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"policy": "name: test\nresources: []"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_acp_list_policies_internal_error() {
    let executor = Arc::new(MockQueryExecutor::new());
    let acp = Arc::new(FailingMockAcpOperations::new("Database connection failed"));
    let state = AppStateBuilder::new(executor).with_acp(acp).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/acp/policy")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_index_create_internal_error() {
    let executor = Arc::new(MockQueryExecutor::new());
    let index = Arc::new(FailingMockIndexOperations::new("Collection not found"));
    let state = AppStateBuilder::new(executor).with_index(index).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/index")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"collection": "Users", "fields": ["name"], "unique": false}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_index_list_internal_error() {
    let executor = Arc::new(MockQueryExecutor::new());
    let index = Arc::new(FailingMockIndexOperations::new("Database unavailable"));
    let state = AppStateBuilder::new(executor).with_index(index).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/index")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_backup_export_internal_error() {
    let executor = Arc::new(MockQueryExecutor::new());
    let backup = Arc::new(FailingMockBackupOperations::new("Export failed"));
    let state = AppStateBuilder::new(executor).with_backup(backup).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/backup/export")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_backup_import_internal_error() {
    let executor = Arc::new(MockQueryExecutor::new());
    let backup = Arc::new(FailingMockBackupOperations::new("Import failed"));
    let state = AppStateBuilder::new(executor).with_backup(backup).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/backup/import")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"Users": [{"_docID": "bae-456", "name": "Bob"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ============================================================================
// Validation Edge Case Tests
// ============================================================================

#[tokio::test]
async fn test_index_create_unicode_collection_name() {
    let executor = Arc::new(MockQueryExecutor::new());
    let index = Arc::new(MockIndexOperations::new());
    let state = AppStateBuilder::new(executor).with_index(index).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/index")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"collection": "Usuários", "fields": ["name"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // Unicode characters should be rejected
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_index_create_field_with_special_chars() {
    let executor = Arc::new(MockQueryExecutor::new());
    let index = Arc::new(MockIndexOperations::new());
    let state = AppStateBuilder::new(executor).with_index(index).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/index")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"collection": "Users", "fields": ["name-field"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // Hyphens should be rejected
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_index_create_field_with_spaces() {
    let executor = Arc::new(MockQueryExecutor::new());
    let index = Arc::new(MockIndexOperations::new());
    let state = AppStateBuilder::new(executor).with_index(index).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/index")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"collection": "Users", "fields": ["field name"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // Spaces should be rejected
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_p2p_add_replicator_invalid_collection_name() {
    let executor = Arc::new(MockQueryExecutor::new());
    let p2p = Arc::new(MockP2POperations::new());
    let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/p2p/replicator")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"collections": ["123Invalid"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_p2p_add_collections_invalid_collection_name() {
    let executor = Arc::new(MockQueryExecutor::new());
    let p2p = Arc::new(MockP2POperations::new());
    let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/p2p/collections")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"collections": ["Users", "Invalid-Name"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_p2p_multiaddr_empty_string() {
    let executor = Arc::new(MockQueryExecutor::new());
    let p2p = Arc::new(MockP2POperations::new());
    let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/p2p/peers")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"address": ""}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_p2p_multiaddr_whitespace_only() {
    let executor = Arc::new(MockQueryExecutor::new());
    let p2p = Arc::new(MockP2POperations::new());
    let state = AppStateBuilder::new(executor).with_p2p(p2p).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/p2p/peers")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"address": "   "}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ============================================================================
// ACP Policy Validation Tests
// ============================================================================

#[tokio::test]
async fn test_acp_add_policy_empty_string() {
    let executor = Arc::new(MockQueryExecutor::new());
    let acp = Arc::new(MockAcpOperations::new());
    let state = AppStateBuilder::new(executor).with_acp(acp).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/acp/policy")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"policy": ""}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_acp_add_policy_whitespace_only() {
    let executor = Arc::new(MockQueryExecutor::new());
    let acp = Arc::new(MockAcpOperations::new());
    let state = AppStateBuilder::new(executor).with_acp(acp).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/acp/policy")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"policy": "   \n\t  "}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ============================================================================
// Backup Edge Case Tests
// ============================================================================

#[tokio::test]
async fn test_backup_import_empty_array() {
    let executor = Arc::new(MockQueryExecutor::new());
    let backup = Arc::new(MockBackupOperations::new());
    let state = AppStateBuilder::new(executor).with_backup(backup).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/backup/import")
                .header("content-type", "application/json")
                .body(Body::from("[]"))
                .unwrap(),
        )
        .await
        .unwrap();

    // Empty array should be rejected
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_backup_import_number_json() {
    let executor = Arc::new(MockQueryExecutor::new());
    let backup = Arc::new(MockBackupOperations::new());
    let state = AppStateBuilder::new(executor).with_backup(backup).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v0/backup/import")
                .header("content-type", "application/json")
                .body(Body::from("123"))
                .unwrap(),
        )
        .await
        .unwrap();

    // Number primitive should be rejected
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_backup_export_invalid_collection_name() {
    let executor = Arc::new(MockQueryExecutor::new());
    let backup = Arc::new(MockBackupOperations::new());
    let state = AppStateBuilder::new(executor).with_backup(backup).build();
    let router = create_router_with_state(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/v0/backup/export?collections=123Invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
