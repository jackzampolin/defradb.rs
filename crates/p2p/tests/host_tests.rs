//! Tests for P2P host functionality.

use p2p::testutil::MockBitswapStore;
use p2p::{P2PHost, PeerId};

#[tokio::test]
async fn test_host_creation() {
    let store = MockBitswapStore::new();
    let result = P2PHost::new(store).await;
    assert!(result.is_ok());

    let (host, handle, _events, _replicators) = result.unwrap();
    let peer_id = host.local_peer_id();
    assert_ne!(peer_id.to_string(), "");

    // Shutdown
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_replicator_management() {
    let store = MockBitswapStore::new();
    let (host, handle, _events, replicators) = P2PHost::new(store).await.unwrap();

    // Spawn the host
    tokio::spawn(host.run());

    let peer_id = PeerId::random();
    let collections = vec!["users".to_string(), "posts".to_string()];

    // Set replicator
    handle
        .create_replicator(peer_id, collections.clone())
        .await
        .unwrap();

    // Get replicator
    let info = handle.get_replicator(peer_id).await.unwrap();
    assert!(info.is_some());
    let info = info.unwrap();
    assert_eq!(info.peer_id(), Some(peer_id));
    assert_eq!(info.collections.len(), 2);

    // Verify in registry
    let peer_str = peer_id.to_string();
    assert!(replicators.is_replicator("users", &peer_str));
    assert!(replicators.is_replicator("posts", &peer_str));

    // Get all replicators
    let all = handle.list_replicators().await.unwrap();
    assert_eq!(all.len(), 1);

    // Delete replicator
    handle.delete_replicator(peer_id).await.unwrap();

    // Verify deleted
    let info = handle.get_replicator(peer_id).await.unwrap();
    assert!(info.is_none());

    assert!(!replicators.is_replicator("users", &peer_str));

    // Shutdown
    handle.shutdown().await.unwrap();
}
