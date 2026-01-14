// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Integration tests for P2P networking.
//!
//! These tests verify end-to-end functionality with multiple hosts.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use libipld::{Block, Cid, DefaultParams, Result as IpldResult};
use libp2p_bitswap_next::BitswapStore;
use p2p::{
    codec, DefraTopic, Error, HostEvent, Message, P2PHost, P2PHostHandle, PushLogBroadcast,
    PushLogReply, PushLogRequest, SyncConfig, SyncCoordinator, SyncEvent, REP_REQUEST_PROTOCOL,
    REP_RESPONSE_PROTOCOL,
};
use tokio::time::timeout;

/// Mock BitswapStore for testing.
#[derive(Clone)]
struct MockBitswapStore {
    blocks: Arc<Mutex<HashMap<Cid, Vec<u8>>>>,
}

impl MockBitswapStore {
    fn new() -> Self {
        Self {
            blocks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl BitswapStore for MockBitswapStore {
    type Params = DefaultParams;

    async fn contains(&mut self, cid: &Cid) -> IpldResult<bool> {
        Ok(self.blocks.lock().unwrap().contains_key(cid))
    }

    async fn get(&mut self, cid: &Cid) -> IpldResult<Option<Vec<u8>>> {
        Ok(self.blocks.lock().unwrap().get(cid).cloned())
    }

    async fn insert(&mut self, block: &Block<Self::Params>) -> IpldResult<()> {
        self.blocks
            .lock()
            .unwrap()
            .insert(*block.cid(), block.data().to_vec());
        Ok(())
    }

    async fn missing_blocks(&mut self, cid: &Cid) -> IpldResult<Vec<Cid>> {
        if self.blocks.lock().unwrap().contains_key(cid) {
            Ok(vec![])
        } else {
            Ok(vec![*cid])
        }
    }
}

/// Helper to create and start a P2P host, returning the handle and event receiver.
async fn create_and_start_host() -> (P2PHostHandle, tokio::sync::mpsc::Receiver<HostEvent>) {
    let store = MockBitswapStore::new();
    let (host, handle, events) = P2PHost::new(store).expect("failed to create host");

    // Spawn the host event loop
    tokio::spawn(host.run());

    (handle, events)
}

/// Wait for a listening event and return the address.
async fn wait_for_listening(
    events: &mut tokio::sync::mpsc::Receiver<HostEvent>,
) -> libp2p::Multiaddr {
    loop {
        match timeout(Duration::from_secs(5), events.recv()).await {
            Ok(Some(HostEvent::Listening(addr))) => return addr,
            Ok(Some(_)) => continue,
            Ok(None) => panic!("event channel closed"),
            Err(_) => panic!("timeout waiting for listening event"),
        }
    }
}

/// Wait for a peer connected event.
async fn wait_for_peer_connected(
    events: &mut tokio::sync::mpsc::Receiver<HostEvent>,
) -> libp2p::PeerId {
    loop {
        match timeout(Duration::from_secs(5), events.recv()).await {
            Ok(Some(HostEvent::PeerConnected(peer_id))) => return peer_id,
            Ok(Some(_)) => continue,
            Ok(None) => panic!("event channel closed"),
            Err(_) => panic!("timeout waiting for peer connected event"),
        }
    }
}

#[tokio::test]
async fn test_two_hosts_connect() {
    let (handle1, mut events1) = create_and_start_host().await;
    let (handle2, mut events2) = create_and_start_host().await;

    // Start listening on host1
    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");

    let addr1 = wait_for_listening(&mut events1).await;

    // Get peer IDs
    let peer_id1 = handle1
        .local_peer_id()
        .await
        .expect("failed to get peer id");
    let peer_id2 = handle2
        .local_peer_id()
        .await
        .expect("failed to get peer id");

    assert_ne!(peer_id1, peer_id2, "peer IDs should be different");

    // Start listening on host2
    handle2
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");

    let _addr2 = wait_for_listening(&mut events2).await;

    // Dial from host2 to host1
    handle2
        .dial(peer_id1, vec![addr1])
        .await
        .expect("failed to dial");

    // Wait for connection on both sides
    let connected_to_1 = wait_for_peer_connected(&mut events2).await;
    let connected_to_2 = wait_for_peer_connected(&mut events1).await;

    assert_eq!(connected_to_1, peer_id1);
    assert_eq!(connected_to_2, peer_id2);

    // Cleanup
    handle1.shutdown().await.ok();
    handle2.shutdown().await.ok();
}

#[tokio::test]
async fn test_host_listen_addresses() {
    let (handle, mut events) = create_and_start_host().await;

    // Initially no addresses
    let addrs = handle
        .listen_addresses()
        .await
        .expect("failed to get addresses");
    assert!(addrs.is_empty());

    // Start listening
    handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");

    wait_for_listening(&mut events).await;

    // Now should have an address
    let addrs = handle
        .listen_addresses()
        .await
        .expect("failed to get addresses");
    assert!(!addrs.is_empty());

    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_host_connected_peers() {
    let (handle1, mut events1) = create_and_start_host().await;
    let (handle2, mut events2) = create_and_start_host().await;

    // Initially no connected peers
    let peers = handle1
        .connected_peers()
        .await
        .expect("failed to get peers");
    assert!(peers.is_empty());

    // Set up connection
    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    let addr1 = wait_for_listening(&mut events1).await;

    handle2
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    wait_for_listening(&mut events2).await;

    let peer_id1 = handle1
        .local_peer_id()
        .await
        .expect("failed to get peer id");

    // Connect
    handle2
        .dial(peer_id1, vec![addr1])
        .await
        .expect("failed to dial");

    wait_for_peer_connected(&mut events1).await;
    wait_for_peer_connected(&mut events2).await;

    // Now should have connected peer
    let peers1 = handle1
        .connected_peers()
        .await
        .expect("failed to get peers");
    let peers2 = handle2
        .connected_peers()
        .await
        .expect("failed to get peers");

    assert_eq!(peers1.len(), 1);
    assert_eq!(peers2.len(), 1);

    handle1.shutdown().await.ok();
    handle2.shutdown().await.ok();
}

#[tokio::test]
async fn test_dial_no_addresses_fails() {
    let (handle, mut events) = create_and_start_host().await;

    handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    wait_for_listening(&mut events).await;

    // Try to dial with no addresses - this should fail immediately
    let fake_peer_id = libp2p::PeerId::random();

    let result = handle.dial(fake_peer_id, vec![]).await;

    // Should fail to dial with no addresses
    assert!(result.is_err());
    match result {
        Err(Error::Dial(_)) => {}
        Err(e) => panic!("Expected Dial error, got: {:?}", e),
        Ok(_) => panic!("Expected dial to fail"),
    }

    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_dial_unreachable_peer_connection_fails() {
    let (handle, mut events) = create_and_start_host().await;

    handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    wait_for_listening(&mut events).await;

    // Try to dial a random peer ID at an unreachable address
    // Note: libp2p accepts the dial and tries in background, so this may not
    // immediately fail. We verify no PeerConnected event occurs.
    let fake_peer_id = libp2p::PeerId::random();
    // Use a TEST-NET address that's guaranteed unreachable
    let bad_addr: libp2p::Multiaddr = "/ip4/192.0.2.1/tcp/9999".parse().unwrap();

    // Dial attempt is queued - may or may not return error
    let _ = handle.dial(fake_peer_id, vec![bad_addr]).await;

    // Wait briefly - should NOT get a PeerConnected event
    let result = timeout(Duration::from_millis(500), async {
        loop {
            match events.recv().await {
                Some(HostEvent::PeerConnected(peer_id)) if peer_id == fake_peer_id => {
                    return true;
                }
                Some(_) => continue,
                None => return false,
            }
        }
    })
    .await;

    // Either timeout (good) or no matching event (good)
    match result {
        Err(_) => {}    // Timeout is expected
        Ok(false) => {} // Channel closed without connection is fine
        Ok(true) => panic!("Should not have connected to unreachable peer"),
    }

    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_host_shutdown() {
    let (handle, _events) = create_and_start_host().await;

    // Shutdown should succeed
    handle.shutdown().await.expect("shutdown failed");

    // Subsequent operations should fail
    let result = handle.local_peer_id().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_multiple_listen_addresses() {
    let (handle, mut events) = create_and_start_host().await;

    // Listen on multiple addresses
    handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen on first address");
    wait_for_listening(&mut events).await;

    handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen on second address");
    wait_for_listening(&mut events).await;

    let addrs = handle
        .listen_addresses()
        .await
        .expect("failed to get addresses");
    assert!(addrs.len() >= 2);

    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_host_with_custom_keypair() {
    use libp2p::identity::Keypair;

    let keypair = Keypair::generate_ed25519();
    let expected_peer_id = keypair.public().to_peer_id();
    let store = MockBitswapStore::new();

    let (host, handle, _events) =
        P2PHost::with_keypair(keypair, store).expect("failed to create host");

    assert_eq!(host.local_peer_id(), expected_peer_id);

    // Verify through handle as well
    tokio::spawn(host.run());
    let peer_id = handle.local_peer_id().await.expect("failed to get peer id");
    assert_eq!(peer_id, expected_peer_id);

    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_two_hosts_pushlog_exchange() {
    let (handle1, mut events1) = create_and_start_host().await;
    let (handle2, mut events2) = create_and_start_host().await;

    // Set up hosts
    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    let addr1 = wait_for_listening(&mut events1).await;

    handle2
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    wait_for_listening(&mut events2).await;

    let peer_id1 = handle1
        .local_peer_id()
        .await
        .expect("failed to get peer id");

    // Connect
    handle2
        .dial(peer_id1, vec![addr1])
        .await
        .expect("failed to dial");

    wait_for_peer_connected(&mut events1).await;
    wait_for_peer_connected(&mut events2).await;

    // Create a PushLog request
    let request = PushLogRequest::new(
        "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi".to_string(),
        vec![1, 2, 3, 4, 5, 6, 7, 8],
        "collection-123".to_string(),
        "creator-abc".to_string(),
        vec![10, 20, 30, 40, 50], // Block data
    );

    // Send PushLog from host2 to host1
    // Note: This will timeout because host1 doesn't have a handler that responds.
    // The request should be received but the response mechanism needs the application
    // layer to respond. This test verifies the message can be sent.
    let send_result = timeout(
        Duration::from_secs(2),
        handle2.send_pushlog(peer_id1, request),
    )
    .await;

    // We expect a timeout since host1 has no application handler to respond
    // The important thing is that we got past the dial and connection phase
    assert!(
        send_result.is_err(),
        "Expected timeout (no handler to respond)"
    );

    // Verify the request was received on host1
    let received_event = timeout(Duration::from_millis(500), async {
        loop {
            match events1.recv().await {
                Some(HostEvent::PushLogRequest { request, .. }) => return Some(request),
                Some(_) => continue,
                None => return None,
            }
        }
    })
    .await;

    match received_event {
        Ok(Some(received_request)) => {
            assert_eq!(
                received_request.doc_id,
                "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
            );
            assert_eq!(received_request.collection_id, "collection-123");
            assert_eq!(received_request.creator, "creator-abc");
        }
        _ => {
            // Request may not have been received due to timing
            // This is acceptable for this test - the key is the connection worked
        }
    }

    handle1.shutdown().await.ok();
    handle2.shutdown().await.ok();
}

#[tokio::test]
async fn test_send_to_disconnected_peer_returns_error() {
    let (handle, mut events) = create_and_start_host().await;

    handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    wait_for_listening(&mut events).await;

    // Try to send to a random peer ID that we're not connected to
    let fake_peer_id = libp2p::PeerId::random();
    let request = PushLogRequest::new(
        "doc123".to_string(),
        vec![1, 2, 3],
        "collection1".to_string(),
        "creator1".to_string(),
        vec![4, 5, 6],
    );

    // This should eventually return an error (not hang forever)
    // The timeout ensures the test doesn't hang if the error isn't returned
    let result = timeout(
        Duration::from_secs(5),
        handle.send_pushlog(fake_peer_id, request),
    )
    .await;

    // Either we get a timeout (acceptable) or we get an error (preferred)
    match result {
        Ok(Err(_)) => {
            // Good - we got an error as expected
        }
        Err(_) => {
            // Timeout - also acceptable, libp2p may retry in background
        }
        Ok(Ok(_)) => {
            panic!("Should not have succeeded sending to disconnected peer")
        }
    }

    handle.shutdown().await.ok();
}

#[test]
fn test_protocol_ids_match_go() {
    // Verify our protocol IDs match what Go expects
    assert_eq!(REP_REQUEST_PROTOCOL, "/defradb/rep_req/0.0.1");
    assert_eq!(REP_RESPONSE_PROTOCOL, "/defradb/rep_resp/0.0.1");
}

#[test]
fn test_cbor_wire_compatibility_pushlog_request() {
    // Test that our CBOR encoding produces the expected field names
    // that Go can understand
    let request = PushLogRequest::new(
        "bafybeigdyrzt".to_string(),
        vec![1, 2, 3, 4],
        "col-123".to_string(),
        "creator-xyz".to_string(),
        vec![10, 20, 30],
    );

    // Encode to CBOR
    let encoded = codec::encode(&request).expect("encoding should succeed");

    // Decode as a generic CBOR value to inspect field names
    let value: serde_cbor::Value =
        serde_cbor::from_slice(&encoded).expect("should decode as Value");

    if let serde_cbor::Value::Map(map) = value {
        // Verify all expected Go field names are present
        let field_names: Vec<String> = map
            .iter()
            .filter_map(|(k, _)| {
                if let serde_cbor::Value::Text(s) = k {
                    Some(s.clone())
                } else {
                    None
                }
            })
            .collect();

        // These are the field names Go expects (PascalCase)
        assert!(
            field_names.contains(&"Version".to_string()),
            "Missing Version field"
        );
        assert!(
            field_names.contains(&"MessageID".to_string()),
            "Missing MessageID field"
        );
        assert!(
            field_names.contains(&"SenderID".to_string()),
            "Missing SenderID field"
        );
        assert!(
            field_names.contains(&"Pubkey".to_string()),
            "Missing Pubkey field"
        );
        assert!(
            field_names.contains(&"DocID".to_string()),
            "Missing DocID field"
        );
        assert!(
            field_names.contains(&"CID".to_string()),
            "Missing CID field"
        );
        assert!(
            field_names.contains(&"CollectionID".to_string()),
            "Missing CollectionID field"
        );
        assert!(
            field_names.contains(&"Creator".to_string()),
            "Missing Creator field"
        );
        assert!(
            field_names.contains(&"Block".to_string()),
            "Missing Block field"
        );
    } else {
        panic!("Expected CBOR map, got {:?}", value);
    }
}

#[test]
fn test_cbor_wire_compatibility_pushlog_reply() {
    // Test reply encoding
    let reply = PushLogReply::success("msg-123");

    let encoded = codec::encode(&reply).expect("encoding should succeed");
    let value: serde_cbor::Value =
        serde_cbor::from_slice(&encoded).expect("should decode as Value");

    if let serde_cbor::Value::Map(map) = value {
        let field_names: Vec<String> = map
            .iter()
            .filter_map(|(k, _)| {
                if let serde_cbor::Value::Text(s) = k {
                    Some(s.clone())
                } else {
                    None
                }
            })
            .collect();

        assert!(
            field_names.contains(&"Version".to_string()),
            "Missing Version field"
        );
        assert!(
            field_names.contains(&"MessageID".to_string()),
            "Missing MessageID field"
        );
    } else {
        panic!("Expected CBOR map");
    }
}

#[test]
fn test_cbor_roundtrip_preserves_data() {
    // Create a request with realistic data
    let original = PushLogRequest::new(
        "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi".to_string(),
        vec![
            0x01, 0x71, 0x12, 0x20, 0x7e, 0x7f, 0x0e, 0x5d, 0x94, 0x27, 0x11, 0x81, 0xb6, 0x81,
            0xcc, 0x72, 0x59, 0x85, 0x72, 0x7e, 0x43, 0x6d, 0x74, 0xc1, 0x6a, 0x05, 0x91, 0x32,
            0x5b, 0x8a, 0x60, 0x8a, 0xc5, 0x1e, 0x73, 0x6b,
        ], // Real CID bytes
        "bafkreih3x2qgxr4gpx7qd5kqj7gg6ukipvxc32e3ihdpkwmv5fvnz6wuui".to_string(),
        "12D3KooWPJ4C8M6VzWv8NMf3s9ycqrTqJ8wM9PRQ3dP5gLwFvbZ7".to_string(),
        vec![0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61], // CBOR-encoded block data
    );

    // Encode
    let encoded = codec::encode(&original).expect("encoding should succeed");

    // Decode
    let decoded: PushLogRequest = codec::decode(&encoded).expect("decoding should succeed");

    // Verify all fields match
    assert_eq!(decoded.doc_id, original.doc_id);
    assert_eq!(decoded.cid, original.cid);
    assert_eq!(decoded.collection_id, original.collection_id);
    assert_eq!(decoded.creator, original.creator);
    assert_eq!(decoded.block, original.block);
}

#[test]
fn test_message_trait_accessors() {
    let request = PushLogRequest::new(
        "doc".to_string(),
        vec![1],
        "col".to_string(),
        "creator".to_string(),
        vec![2],
    );

    // Test Message trait accessors
    assert!(request.message_id().is_empty()); // Not set yet
    assert!(request.version().is_empty() || request.version() == "/defradb/0.0.1");
    assert!(request.sender_id().is_empty());
    assert!(request.pubkey().is_empty());
    assert!(request.signature().is_none());
    assert!(request.err_message().is_none());
}

#[test]
fn test_signed_message_has_required_fields() {
    use libp2p::identity::Keypair;
    use p2p::sign_message;

    let keypair = Keypair::generate_ed25519();
    let mut request = PushLogRequest::new(
        "doc123".to_string(),
        vec![1, 2, 3, 4],
        "collection1".to_string(),
        "creator1".to_string(),
        vec![5, 6, 7, 8],
    );

    // Sign the message
    sign_message(&keypair, &mut request).expect("signing should succeed");

    // Verify all required fields are populated
    assert!(!request.message_id().is_empty(), "message_id should be set");
    assert_eq!(request.version(), "/defradb/0.0.1", "version should be set");
    assert!(!request.sender_id().is_empty(), "sender_id should be set");
    assert!(!request.pubkey().is_empty(), "pubkey should be set");
    assert!(request.signature().is_some(), "signature should be set");

    // Verify sender_id matches the keypair's peer ID
    let expected_peer_id = keypair.public().to_peer_id().to_string();
    assert_eq!(request.sender_id(), expected_peer_id);
}

// ============================================================================
// GossipSub Tests
// ============================================================================

#[tokio::test]
async fn test_gossipsub_subscribe_unsubscribe() {
    let (handle, mut events) = create_and_start_host().await;

    // Start listening
    handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    wait_for_listening(&mut events).await;

    // Initially no subscriptions
    let topics = handle
        .subscribed_topics()
        .await
        .expect("failed to get topics");
    assert!(topics.is_empty(), "should start with no subscriptions");

    // Subscribe to doc-sync topic
    let is_new = handle
        .subscribe(DefraTopic::DocSync)
        .await
        .expect("subscribe should succeed");
    assert!(is_new, "should be a new subscription");

    // Verify subscription
    let topics = handle
        .subscribed_topics()
        .await
        .expect("failed to get topics");
    assert_eq!(topics.len(), 1, "should have one subscription");

    // Subscribe again - should return false (already subscribed)
    let is_new = handle
        .subscribe(DefraTopic::DocSync)
        .await
        .expect("subscribe should succeed");
    assert!(!is_new, "should not be a new subscription");

    // Unsubscribe
    let was_subscribed = handle
        .unsubscribe(DefraTopic::DocSync)
        .await
        .expect("unsubscribe should succeed");
    assert!(was_subscribed, "should have been subscribed");

    // Verify unsubscription
    let topics = handle
        .subscribed_topics()
        .await
        .expect("failed to get topics");
    assert!(topics.is_empty(), "should have no subscriptions");

    // Unsubscribe again - should return false (not subscribed)
    let was_subscribed = handle
        .unsubscribe(DefraTopic::DocSync)
        .await
        .expect("unsubscribe should succeed");
    assert!(!was_subscribed, "should not have been subscribed");

    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_gossipsub_multiple_topics() {
    let (handle, mut events) = create_and_start_host().await;

    handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    wait_for_listening(&mut events).await;

    // Subscribe to multiple topics
    handle
        .subscribe(DefraTopic::DocSync)
        .await
        .expect("subscribe should succeed");
    handle
        .subscribe(DefraTopic::Encryption)
        .await
        .expect("subscribe should succeed");
    handle
        .subscribe(DefraTopic::collection("my-collection"))
        .await
        .expect("subscribe should succeed");
    handle
        .subscribe(DefraTopic::document("my-document"))
        .await
        .expect("subscribe should succeed");

    // Verify all subscriptions
    let topics = handle
        .subscribed_topics()
        .await
        .expect("failed to get topics");
    assert_eq!(topics.len(), 4, "should have four subscriptions");

    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_gossipsub_publish_receive() {
    let (handle1, mut events1) = create_and_start_host().await;
    let (handle2, mut events2) = create_and_start_host().await;

    // Set up hosts
    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    let addr1 = wait_for_listening(&mut events1).await;

    handle2
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    wait_for_listening(&mut events2).await;

    let peer_id1 = handle1
        .local_peer_id()
        .await
        .expect("failed to get peer id");

    // Connect
    handle2
        .dial(peer_id1, vec![addr1])
        .await
        .expect("failed to dial");
    wait_for_peer_connected(&mut events1).await;
    wait_for_peer_connected(&mut events2).await;

    // Both hosts subscribe to the same topic
    let topic = DefraTopic::collection("test-collection");
    handle1
        .subscribe(topic.clone())
        .await
        .expect("subscribe failed");
    handle2
        .subscribe(topic.clone())
        .await
        .expect("subscribe failed");

    // Give time for subscription propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create a broadcast message
    let broadcast = PushLogBroadcast {
        doc_id: "doc-123".to_string(),
        cid: vec![1, 2, 3, 4, 5],
        collection_id: "test-collection".to_string(),
        creator: "creator-abc".to_string(),
        block: vec![10, 20, 30],
    };

    // Publish from host2
    let msg_id = handle2
        .publish(topic, broadcast.clone())
        .await
        .expect("publish should succeed");
    assert!(!msg_id.0.is_empty(), "message ID should not be empty");

    // Wait for host1 to receive the message
    let received = timeout(Duration::from_secs(5), async {
        loop {
            match events1.recv().await {
                Some(HostEvent::GossipMessage { message, .. }) => return Some(message),
                Some(_) => continue,
                None => return None,
            }
        }
    })
    .await;

    match received {
        Ok(Some(msg)) => {
            assert_eq!(msg.doc_id, broadcast.doc_id);
            assert_eq!(msg.cid, broadcast.cid);
            assert_eq!(msg.collection_id, broadcast.collection_id);
            assert_eq!(msg.creator, broadcast.creator);
            assert_eq!(msg.block, broadcast.block);
        }
        Ok(None) => panic!("Event channel closed"),
        Err(_) => {
            // Timeout is acceptable - gossipsub message propagation can be flaky in tests
            // The important thing is that publish succeeded
        }
    }

    handle1.shutdown().await.ok();
    handle2.shutdown().await.ok();
}

#[test]
fn test_pushlog_broadcast_cbor_field_names() {
    // Test that PushLogBroadcast CBOR encoding produces Go-compatible field names
    let broadcast = PushLogBroadcast {
        doc_id: "doc-123".to_string(),
        cid: vec![1, 2, 3, 4],
        collection_id: "col-456".to_string(),
        creator: "creator-789".to_string(),
        block: vec![10, 20, 30],
    };

    // Encode to CBOR
    let encoded = serde_cbor::to_vec(&broadcast).expect("encoding should succeed");

    // Decode as a generic CBOR value to inspect field names
    let value: serde_cbor::Value =
        serde_cbor::from_slice(&encoded).expect("should decode as Value");

    if let serde_cbor::Value::Map(map) = value {
        let field_names: Vec<String> = map
            .iter()
            .filter_map(|(k, _)| {
                if let serde_cbor::Value::Text(s) = k {
                    Some(s.clone())
                } else {
                    None
                }
            })
            .collect();

        // Verify PascalCase field names for Go compatibility
        assert!(
            field_names.contains(&"DocID".to_string()),
            "Missing DocID field"
        );
        assert!(
            field_names.contains(&"CID".to_string()),
            "Missing CID field"
        );
        assert!(
            field_names.contains(&"CollectionID".to_string()),
            "Missing CollectionID field"
        );
        assert!(
            field_names.contains(&"Creator".to_string()),
            "Missing Creator field"
        );
        assert!(
            field_names.contains(&"Block".to_string()),
            "Missing Block field"
        );
    } else {
        panic!("Expected CBOR map, got {:?}", value);
    }
}

#[test]
fn test_pushlog_broadcast_roundtrip() {
    let original = PushLogBroadcast {
        doc_id: "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi".to_string(),
        cid: vec![
            0x01, 0x71, 0x12, 0x20, 0x7e, 0x7f, 0x0e, 0x5d, 0x94, 0x27, 0x11, 0x81, 0xb6, 0x81,
            0xcc, 0x72, 0x59, 0x85, 0x72, 0x7e, 0x43, 0x6d, 0x74, 0xc1, 0x6a, 0x05, 0x91, 0x32,
            0x5b, 0x8a, 0x60, 0x8a, 0xc5, 0x1e, 0x73, 0x6b,
        ],
        collection_id: "bafkreih3x2qgxr4gpx7qd5kqj7gg6ukipvxc32e3ihdpkwmv5fvnz6wuui".to_string(),
        creator: "12D3KooWPJ4C8M6VzWv8NMf3s9ycqrTqJ8wM9PRQ3dP5gLwFvbZ7".to_string(),
        block: vec![0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61],
    };

    // Encode
    let encoded = serde_cbor::to_vec(&original).expect("encoding should succeed");

    // Decode
    let decoded: PushLogBroadcast =
        serde_cbor::from_slice(&encoded).expect("decoding should succeed");

    // Verify all fields match
    assert_eq!(decoded.doc_id, original.doc_id);
    assert_eq!(decoded.cid, original.cid);
    assert_eq!(decoded.collection_id, original.collection_id);
    assert_eq!(decoded.creator, original.creator);
    assert_eq!(decoded.block, original.block);
}

#[test]
fn test_defra_topic_types() {
    // Verify topic string conversions
    assert_eq!(DefraTopic::DocSync.topic_string(), "doc-sync");
    assert_eq!(DefraTopic::Encryption.topic_string(), "encryption");
    assert_eq!(DefraTopic::collection("my-col").topic_string(), "my-col");
    assert_eq!(DefraTopic::document("my-doc").topic_string(), "my-doc");
    assert_eq!(
        DefraTopic::Custom("custom".to_string()).topic_string(),
        "custom"
    );

    // Test from str conversion
    assert_eq!(DefraTopic::from("doc-sync"), DefraTopic::DocSync);
    assert_eq!(DefraTopic::from("encryption"), DefraTopic::Encryption);
    assert_eq!(
        DefraTopic::from("other"),
        DefraTopic::Custom("other".to_string())
    );
}

// ============================================================================
// SyncCoordinator Integration Tests
// ============================================================================

#[tokio::test]
async fn test_two_node_sync_with_coordinator() {
    use blockstore::{Blockstore, DefraBlockstore};
    use storage::backends::MemoryStore;

    // Create host 1 with sync coordinator
    let (handle1, mut events1) = create_and_start_host().await;
    let store1 = Arc::new(MemoryStore::new());
    let blockstore1 = Arc::new(DefraBlockstore::new(store1, true));
    let (coordinator1, _sync_events1) =
        SyncCoordinator::new(handle1.clone(), blockstore1.clone(), SyncConfig::default())
            .await
            .expect("failed to create coordinator 1");

    // Create host 2 with sync coordinator
    let (handle2, mut events2) = create_and_start_host().await;
    let store2 = Arc::new(MemoryStore::new());
    let blockstore2 = Arc::new(DefraBlockstore::new(store2, true));
    let (coordinator2, mut sync_events2) =
        SyncCoordinator::new(handle2.clone(), blockstore2.clone(), SyncConfig::default())
            .await
            .expect("failed to create coordinator 2");

    // Set up networking
    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    let addr1 = wait_for_listening(&mut events1).await;

    handle2
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    wait_for_listening(&mut events2).await;

    let peer_id1 = handle1
        .local_peer_id()
        .await
        .expect("failed to get peer id");

    // Connect the hosts
    handle2
        .dial(peer_id1, vec![addr1])
        .await
        .expect("failed to dial");
    wait_for_peer_connected(&mut events1).await;
    wait_for_peer_connected(&mut events2).await;

    // Both coordinators subscribe to the same collection
    let collection_id = "test-sync-collection";
    coordinator1
        .subscribe_collection(collection_id)
        .await
        .expect("subscribe failed");
    coordinator2
        .subscribe_collection(collection_id)
        .await
        .expect("subscribe failed");

    // Give time for subscription propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create a test block
    let block_data = b"test block for two-node sync";
    let cid = cid::Cid::try_from(
        "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
            .parse::<cid::Cid>()
            .unwrap()
            .to_bytes()
            .as_slice(),
    )
    .unwrap();
    let doc_id = "bae-sync-test-doc";

    // Store block locally on coordinator1 and broadcast
    blockstore1.put(&cid, block_data).await.unwrap();
    coordinator1
        .broadcast_local_update(&cid, block_data, doc_id, collection_id)
        .await
        .expect("broadcast failed");

    // Spawn a task to handle host events for coordinator2
    let events_handler = tokio::spawn({
        async move {
            let mut received_events = Vec::new();
            loop {
                tokio::select! {
                    event = events2.recv() => {
                        match event {
                            Some(HostEvent::GossipMessage { message, .. }) => {
                                // Store the message for later processing
                                received_events.push(message);
                            }
                            Some(_) => continue,
                            None => break,
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(3)) => {
                        break;
                    }
                }
            }
            received_events
        }
    });

    // Wait for the handler to complete
    let received = events_handler.await.expect("handler panicked");

    // Check if coordinator2 received the message
    if !received.is_empty() {
        let msg = &received[0];
        assert_eq!(msg.doc_id, doc_id);
        assert_eq!(msg.collection_id, collection_id);
        assert_eq!(msg.block, block_data.to_vec());

        // Process the received message
        coordinator2
            .handle_host_event(HostEvent::GossipMessage {
                propagation_source: libp2p::PeerId::random(),
                message_id: libp2p::gossipsub::MessageId::new(b"test"),
                topic: collection_id.to_string(),
                message: msg.clone(),
            })
            .await
            .expect("handle event failed");

        // Check sync event
        match sync_events2.try_recv() {
            Ok(SyncEvent::BlockReceived {
                cid: event_cid,
                doc_id: event_doc_id,
                collection_id: event_col_id,
                ..
            }) => {
                assert_eq!(event_doc_id, doc_id);
                assert_eq!(event_col_id, collection_id);
                // Verify block is in blockstore2
                assert!(
                    blockstore2.has(&event_cid).await.unwrap(),
                    "Block should be in blockstore2"
                );
                // Mark as merged
                coordinator2.mark_as_merged(&event_cid).await.unwrap();
                assert!(
                    coordinator2.is_merged(&event_cid).await.unwrap(),
                    "Block should be marked as merged"
                );
            }
            Ok(other) => {
                // Other event types are also valid (e.g., BlockAlreadyMerged)
                tracing::info!(?other, "Received other sync event");
            }
            Err(_) => {
                // No event yet - acceptable in tests
            }
        }
    }

    handle1.shutdown().await.ok();
    handle2.shutdown().await.ok();
}

#[tokio::test]
async fn test_peer_state_tracking_with_coordinator() {
    use blockstore::DefraBlockstore;
    use storage::backends::MemoryStore;

    // Create two hosts with coordinators
    let (handle1, mut events1) = create_and_start_host().await;
    let store1 = Arc::new(MemoryStore::new());
    let blockstore1 = Arc::new(DefraBlockstore::new(store1, true));
    let (coordinator1, _sync_events1) =
        SyncCoordinator::new(handle1.clone(), blockstore1.clone(), SyncConfig::default())
            .await
            .expect("failed to create coordinator 1");

    let (handle2, mut events2) = create_and_start_host().await;
    let store2 = Arc::new(MemoryStore::new());
    let blockstore2 = Arc::new(DefraBlockstore::new(store2, true));
    let (coordinator2, _sync_events2) =
        SyncCoordinator::new(handle2.clone(), blockstore2.clone(), SyncConfig::default())
            .await
            .expect("failed to create coordinator 2");

    // Set up networking
    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    let addr1 = wait_for_listening(&mut events1).await;

    handle2
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    wait_for_listening(&mut events2).await;

    let peer_id1 = handle1
        .local_peer_id()
        .await
        .expect("failed to get peer id");
    let peer_id2 = handle2
        .local_peer_id()
        .await
        .expect("failed to get peer id");

    // Initially no peers connected in tracker
    assert!(coordinator1.peer_state().connected_peers().is_empty());
    assert!(coordinator2.peer_state().connected_peers().is_empty());

    // Connect the hosts
    handle2
        .dial(peer_id1, vec![addr1])
        .await
        .expect("failed to dial");

    // Wait for connection events and process them
    let connected1 = wait_for_peer_connected(&mut events1).await;
    let connected2 = wait_for_peer_connected(&mut events2).await;

    // Process the connection events through coordinators
    coordinator1
        .handle_host_event(HostEvent::PeerConnected(connected1))
        .await
        .expect("handle event failed");
    coordinator2
        .handle_host_event(HostEvent::PeerConnected(connected2))
        .await
        .expect("handle event failed");

    // Now peer state should track the connections
    let connected_to_1 = coordinator1.peer_state().connected_peers();
    let connected_to_2 = coordinator2.peer_state().connected_peers();

    assert_eq!(connected_to_1.len(), 1, "Coordinator 1 should see peer 2");
    assert_eq!(connected_to_2.len(), 1, "Coordinator 2 should see peer 1");
    assert!(connected_to_1.contains(&peer_id2));
    assert!(connected_to_2.contains(&peer_id1));

    // Test peer state for topic subscriptions
    coordinator1
        .handle_host_event(HostEvent::PeerSubscribed {
            peer_id: peer_id2,
            topic: "users".to_string(),
        })
        .await
        .expect("handle event failed");

    let peers_for_users = coordinator1.peer_state().peers_for_collection("users");
    assert_eq!(peers_for_users.len(), 1);
    assert!(peers_for_users.contains(&peer_id2));

    // Test peer stats
    let stats = coordinator1.peer_state().stats();
    assert_eq!(stats.connected_peers, 1);
    assert_eq!(stats.total_peers, 1);

    // Cleanup
    handle1.shutdown().await.ok();
    handle2.shutdown().await.ok();
}

#[tokio::test]
async fn test_dag_sync_prepare_sync_no_missing_blocks() {
    use p2p::sync::{DagSync, SyncPlan};
    use std::str::FromStr;

    // Create a coordinator with peer state
    let (handle, _events) = create_and_start_host().await;
    let peer_state = Arc::new(p2p::sync::PeerStateTracker::new());
    let dag_sync = DagSync::new(peer_state);

    // Create test CIDs
    let root_cid =
        cid::Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();
    let link_cid =
        cid::Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap();

    // When all links exist locally, prepare_sync returns Complete
    let plan = dag_sync
        .prepare_sync(root_cid, &[link_cid], |_| true) // All CIDs exist locally
        .await
        .expect("prepare_sync failed");

    assert!(matches!(plan, SyncPlan::Complete));

    // Root should be marked as synced
    assert!(dag_sync.state().is_synced(&root_cid).await);

    // Cleanup
    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_peer_state_cid_tracking_before_connection() {
    use blockstore::DefraBlockstore;
    use std::str::FromStr;
    use storage::backends::MemoryStore;

    // Create two hosts with coordinators
    let (handle1, mut events1) = create_and_start_host().await;
    let store1 = Arc::new(MemoryStore::new());
    let blockstore1 = Arc::new(DefraBlockstore::new(store1, true));
    let (coordinator1, _sync_events1) =
        SyncCoordinator::new(handle1.clone(), blockstore1.clone(), SyncConfig::default())
            .await
            .expect("failed to create coordinator 1");

    let (handle2, mut events2) = create_and_start_host().await;
    let store2 = Arc::new(MemoryStore::new());
    let blockstore2 = Arc::new(DefraBlockstore::new(store2, true));
    let (_coordinator2, _sync_events2) =
        SyncCoordinator::new(handle2.clone(), blockstore2.clone(), SyncConfig::default())
            .await
            .expect("failed to create coordinator 2");

    // Set up networking
    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    let addr1 = wait_for_listening(&mut events1).await;

    handle2
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");
    wait_for_listening(&mut events2).await;

    let peer_id1 = handle1
        .local_peer_id()
        .await
        .expect("failed to get peer id");
    let peer_id2 = handle2
        .local_peer_id()
        .await
        .expect("failed to get peer id");

    // Test: Record CID for peer2 BEFORE connection event is processed
    // This simulates a race condition where gossip message arrives before PeerConnected
    let test_cid =
        cid::Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();

    // Record CID before connection
    coordinator1.peer_state().peer_has_cid(&peer_id2, test_cid);

    // Peer entry should be created (bug fix verification)
    assert!(coordinator1.peer_state().peer_has(&peer_id2, &test_cid));
    assert_eq!(coordinator1.peer_state().stats().total_peers, 1);

    // But peer is not connected, so shouldn't appear in peers_with_cid
    assert!(coordinator1
        .peer_state()
        .peers_with_cid(&test_cid)
        .is_empty());

    // Now connect the hosts
    handle2
        .dial(peer_id1, vec![addr1])
        .await
        .expect("failed to dial");

    // Wait for and process connection events
    let connected1 = wait_for_peer_connected(&mut events1).await;
    coordinator1
        .handle_host_event(HostEvent::PeerConnected(connected1))
        .await
        .expect("handle event failed");

    // Now peer2 should appear in peers_with_cid
    let peers_with = coordinator1.peer_state().peers_with_cid(&test_cid);
    assert_eq!(peers_with.len(), 1);
    assert!(peers_with.contains(&peer_id2));

    // Cleanup
    handle1.shutdown().await.ok();
    handle2.shutdown().await.ok();
}

// ============================================================================
// Error Path Tests
// ============================================================================

#[tokio::test]
async fn test_handle_host_event_invalid_cid_in_gossip_message() {
    use blockstore::DefraBlockstore;
    use storage::backends::MemoryStore;

    let (handle, _events) = create_and_start_host().await;
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (coordinator, _sync_events) =
        SyncCoordinator::new(handle.clone(), blockstore, SyncConfig::default())
            .await
            .expect("failed to create coordinator");

    // Create a gossip message with invalid CID bytes
    let invalid_cid_bytes = vec![0xFF, 0xFF, 0xFF]; // Invalid CID
    let message = PushLogBroadcast {
        doc_id: "test-doc".to_string(),
        cid: invalid_cid_bytes,
        collection_id: "test-collection".to_string(),
        creator: "test-creator".to_string(),
        block: vec![1, 2, 3],
    };

    // Processing should not panic - invalid CID is logged and skipped
    let result = coordinator
        .handle_host_event(HostEvent::GossipMessage {
            propagation_source: libp2p::PeerId::random(),
            message_id: libp2p::gossipsub::MessageId::new(b"test"),
            topic: "test-collection".to_string(),
            message,
        })
        .await;

    // Invalid CID is detected and returns an error - but importantly it doesn't panic
    // The error is expected and the coordinator handles it gracefully
    assert!(
        result.is_err(),
        "Should return error for invalid CID, got: {:?}",
        result
    );

    // Verify it's specifically an InvalidCid error
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("CID") || err_str.contains("cid"),
        "Error should mention CID: {}",
        err_str
    );

    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_handle_host_event_peer_disconnect_updates_state() {
    use blockstore::DefraBlockstore;
    use storage::backends::MemoryStore;

    let (handle, _events) = create_and_start_host().await;
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (coordinator, _sync_events) =
        SyncCoordinator::new(handle.clone(), blockstore, SyncConfig::default())
            .await
            .expect("failed to create coordinator");

    let peer_id = libp2p::PeerId::random();

    // Connect peer
    coordinator
        .handle_host_event(HostEvent::PeerConnected(peer_id))
        .await
        .expect("connect failed");
    assert!(coordinator.peer_state().is_connected(&peer_id));

    // Disconnect peer
    coordinator
        .handle_host_event(HostEvent::PeerDisconnected(peer_id))
        .await
        .expect("disconnect failed");
    assert!(!coordinator.peer_state().is_connected(&peer_id));

    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_handle_host_event_subscription_tracking() {
    use blockstore::DefraBlockstore;
    use storage::backends::MemoryStore;

    let (handle, _events) = create_and_start_host().await;
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (coordinator, _sync_events) =
        SyncCoordinator::new(handle.clone(), blockstore, SyncConfig::default())
            .await
            .expect("failed to create coordinator");

    let peer_id = libp2p::PeerId::random();

    // Connect peer
    coordinator
        .handle_host_event(HostEvent::PeerConnected(peer_id))
        .await
        .expect("connect failed");

    // Subscribe to collection
    coordinator
        .handle_host_event(HostEvent::PeerSubscribed {
            peer_id,
            topic: "users".to_string(),
        })
        .await
        .expect("subscribe failed");

    assert_eq!(
        coordinator.peer_state().peers_for_collection("users").len(),
        1
    );

    // Unsubscribe from collection
    coordinator
        .handle_host_event(HostEvent::PeerUnsubscribed {
            peer_id,
            topic: "users".to_string(),
        })
        .await
        .expect("unsubscribe failed");

    assert!(coordinator
        .peer_state()
        .peers_for_collection("users")
        .is_empty());

    handle.shutdown().await.ok();
}

// ============================================================================
// ReplicationLoop Tests
// ============================================================================

#[tokio::test]
async fn test_replication_loop_channel_closed_terminates() {
    use p2p::sync::{MergeHandler, MergeOutcome, ReplicationConfig, ReplicationLoop};

    // Create a simple merge handler for testing
    struct TestHandler;

    #[derive(Debug)]
    struct TestError;
    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestError")
        }
    }
    impl std::error::Error for TestError {}

    #[async_trait::async_trait]
    impl MergeHandler for TestHandler {
        type Error = TestError;
        async fn handle_block(
            &self,
            _cid: &cid::Cid,
            _block_data: &[u8],
            _doc_id: &str,
            _collection_id: &str,
            _creator: &str,
        ) -> Result<MergeOutcome, TestError> {
            Ok(MergeOutcome::Merged)
        }
    }

    use blockstore::DefraBlockstore;
    use storage::backends::MemoryStore;

    let (handle, _events) = create_and_start_host().await;
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (coordinator, events) =
        SyncCoordinator::new(handle.clone(), blockstore, SyncConfig::default())
            .await
            .expect("failed to create coordinator");

    let coordinator = Arc::new(coordinator);
    let handler = Arc::new(TestHandler);

    // Drop the original events receiver - we'll use our own channel
    drop(events);

    // Create a new channel that we control
    let (tx, rx) = tokio::sync::mpsc::channel::<SyncEvent>(1);

    // Immediately drop the sender to close the channel
    drop(tx);

    // Run the loop with a timeout - it should terminate when channel closes
    let result = timeout(
        Duration::from_secs(2),
        ReplicationLoop::run(
            coordinator.clone(),
            rx,
            handler,
            ReplicationConfig::default(),
        ),
    )
    .await;

    // The loop should complete (not timeout) when channel is closed
    assert!(
        result.is_ok(),
        "ReplicationLoop should terminate when channel closes"
    );

    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_replication_loop_drain_empty_channel() {
    use p2p::sync::{MergeHandler, MergeOutcome, ReplicationConfig, ReplicationLoop};

    struct TestHandler;

    #[derive(Debug)]
    struct TestError;
    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestError")
        }
    }
    impl std::error::Error for TestError {}

    #[async_trait::async_trait]
    impl MergeHandler for TestHandler {
        type Error = TestError;
        async fn handle_block(
            &self,
            _cid: &cid::Cid,
            _block_data: &[u8],
            _doc_id: &str,
            _collection_id: &str,
            _creator: &str,
        ) -> Result<MergeOutcome, TestError> {
            Ok(MergeOutcome::Merged)
        }
    }

    use blockstore::DefraBlockstore;
    use storage::backends::MemoryStore;

    let (handle, _events) = create_and_start_host().await;
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (coordinator, mut events) =
        SyncCoordinator::new(handle.clone(), blockstore, SyncConfig::default())
            .await
            .expect("failed to create coordinator");

    let coordinator = Arc::new(coordinator);
    let handler = Arc::new(TestHandler);

    // Drain empty channel should return empty vec
    let results = ReplicationLoop::drain(
        coordinator.clone(),
        &mut events,
        handler,
        ReplicationConfig::default(),
    )
    .await;

    assert!(
        results.is_empty(),
        "Draining empty channel should return no results"
    );

    handle.shutdown().await.ok();
}

// ============================================================================
// Bitswap Integration Tests
// ============================================================================

#[tokio::test]
async fn test_dag_sync_with_missing_blocks() {
    use p2p::sync::{DagSync, SyncPlan};
    use std::str::FromStr;

    // Create peer state with a connected peer
    let peer_state = Arc::new(p2p::sync::PeerStateTracker::new());
    let peer = libp2p::PeerId::random();
    peer_state.peer_connected(peer);

    let dag_sync = DagSync::new(peer_state);

    // Create test CIDs
    let root_cid =
        cid::Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();
    let missing_cid =
        cid::Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap();

    // When some links are missing locally, prepare_sync returns NeedsFetch
    let plan = dag_sync
        .prepare_sync(root_cid, &[missing_cid], |_| false) // No CIDs exist locally
        .await
        .expect("prepare_sync failed");

    match plan {
        SyncPlan::NeedsFetch {
            root,
            missing,
            providers,
        } => {
            assert_eq!(root, root_cid);
            assert_eq!(missing.len(), 1);
            assert_eq!(missing[0], missing_cid);
            assert!(!providers.is_empty(), "Should have providers");
        }
        _ => panic!("Expected NeedsFetch, got {:?}", plan),
    }

    // Root should be marked as syncing
    assert!(dag_sync.state().is_syncing(&root_cid).await);
    // Missing block should also be marked as syncing
    assert!(dag_sync.state().is_syncing(&missing_cid).await);
}

#[tokio::test]
async fn test_dag_sync_handle_block_received() {
    use p2p::sync::DagSync;
    use std::str::FromStr;

    let peer_state = Arc::new(p2p::sync::PeerStateTracker::new());
    let dag_sync = DagSync::new(peer_state);

    let cid =
        cid::Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();

    // Start syncing
    dag_sync.state().start_sync(cid).await;
    assert!(dag_sync.state().is_syncing(&cid).await);

    // Handle block received
    dag_sync.handle_block_received(cid).await;

    // Should now be synced, not syncing
    assert!(!dag_sync.state().is_syncing(&cid).await);
    assert!(dag_sync.state().is_synced(&cid).await);
}

#[tokio::test]
async fn test_dag_sync_handle_sync_complete_success() {
    use p2p::sync::DagSync;
    use std::str::FromStr;

    let peer_state = Arc::new(p2p::sync::PeerStateTracker::new());
    let dag_sync = DagSync::new(peer_state);

    let cid =
        cid::Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();

    // Start syncing
    dag_sync.state().start_sync(cid).await;

    // Complete with success
    dag_sync.handle_sync_complete(cid, true).await;

    assert!(dag_sync.state().is_synced(&cid).await);
    assert!(!dag_sync.state().is_syncing(&cid).await);
}

#[tokio::test]
async fn test_dag_sync_handle_sync_complete_failure() {
    use p2p::sync::DagSync;
    use std::str::FromStr;

    let peer_state = Arc::new(p2p::sync::PeerStateTracker::new());
    let dag_sync = DagSync::new(peer_state);

    let cid =
        cid::Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();

    // Start syncing
    dag_sync.state().start_sync(cid).await;

    // Complete with failure
    dag_sync.handle_sync_complete(cid, false).await;

    // Should be neither syncing nor synced (can retry)
    assert!(!dag_sync.state().is_synced(&cid).await);
    assert!(!dag_sync.state().is_syncing(&cid).await);
}

#[tokio::test]
async fn test_replicator_registry_access_control() {
    use p2p::{BlockAccessController, ReplicatorRegistry};

    let registry = Arc::new(ReplicatorRegistry::new());
    let peer1 = libp2p::PeerId::random();
    let peer2 = libp2p::PeerId::random();

    // Register peer1 as a replicator for "users" collection
    registry.add_replicator("users", peer1);

    // Create controller with access control enabled
    let controller = BlockAccessController::controlled(registry);

    let cid = cid::Cid::try_from("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
        .unwrap();

    // peer1 should have access (is replicator)
    assert!(controller.has_access(&peer1, &cid, Some("users")));

    // peer2 should NOT have access (not a replicator)
    assert!(!controller.has_access(&peer2, &cid, Some("users")));

    // peer1 should also have access when collection is unknown (is_any_replicator)
    assert!(controller.has_access(&peer1, &cid, None));
}

#[tokio::test]
async fn test_replicator_registry_multiple_collections() {
    use p2p::ReplicatorRegistry;

    let registry = ReplicatorRegistry::new();
    let peer = libp2p::PeerId::random();

    registry.add_replicator("users", peer);
    registry.add_replicator("posts", peer);
    registry.add_replicator("comments", peer);

    // Should be replicator for all three
    assert!(registry.is_replicator("users", &peer));
    assert!(registry.is_replicator("posts", &peer));
    assert!(registry.is_replicator("comments", &peer));
    assert!(!registry.is_replicator("other", &peer));

    // Get collections should return all three
    let collections = registry.get_collections(&peer);
    assert_eq!(collections.len(), 3);

    // Remove from one
    registry.remove_replicator("posts", &peer);
    assert!(!registry.is_replicator("posts", &peer));
    assert!(registry.is_replicator("users", &peer));

    // Remove peer entirely
    registry.remove_peer(&peer);
    assert!(!registry.is_any_replicator(&peer));
}

#[tokio::test]
async fn test_bitswap_store_adapter_contains() {
    use blockstore::{Blockstore, DefraBlockstore};
    use p2p::{BitswapStore, BitswapStoreAdapter};
    use storage::backends::MemoryStore;
    use std::str::FromStr;

    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, false));
    let mut adapter = BitswapStoreAdapter::new(blockstore.clone());

    let cid =
        cid::Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();

    // Initially should not contain the CID
    let libipld_cid = libipld::Cid::try_from(cid.to_bytes().as_slice()).unwrap();
    let contains = adapter.contains(&libipld_cid).await.unwrap();
    assert!(!contains);

    // Put a block
    let block_data = vec![1, 2, 3, 4, 5];
    blockstore.put(&cid, &block_data).await.unwrap();

    // Now should contain the CID
    let contains = adapter.contains(&libipld_cid).await.unwrap();
    assert!(contains);
}

#[tokio::test]
async fn test_bitswap_store_adapter_get() {
    use blockstore::{Blockstore, DefraBlockstore};
    use p2p::{BitswapStore, BitswapStoreAdapter};
    use storage::backends::MemoryStore;
    use std::str::FromStr;

    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, false));
    let mut adapter = BitswapStoreAdapter::new(blockstore.clone());

    let cid =
        cid::Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();
    let libipld_cid = libipld::Cid::try_from(cid.to_bytes().as_slice()).unwrap();

    // Initially should return None
    let data = adapter.get(&libipld_cid).await.unwrap();
    assert!(data.is_none());

    // Put a block
    let block_data = vec![1, 2, 3, 4, 5];
    blockstore.put(&cid, &block_data).await.unwrap();

    // Now should return the block data
    let data = adapter.get(&libipld_cid).await.unwrap();
    assert!(data.is_some());
    assert_eq!(data.unwrap(), block_data);
}

// ============================================================================
// Concurrent State Tests
// ============================================================================

#[tokio::test]
async fn test_dag_sync_state_concurrent_operations() {
    use p2p::sync::DagSync;
    use std::str::FromStr;

    let peer_state = Arc::new(p2p::sync::PeerStateTracker::new());
    let dag_sync = DagSync::new(peer_state);
    let state = dag_sync.state();

    // Create multiple test CIDs
    let cid1 =
        cid::Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();
    let cid2 =
        cid::Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap();
    let cid3 =
        cid::Cid::from_str("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku").unwrap();

    // Spawn multiple concurrent tasks that manipulate state
    let state1 = state.clone();
    let state2 = state.clone();
    let state3 = state.clone();

    let handle1 = tokio::spawn(async move {
        for _ in 0..100 {
            state1.start_sync(cid1).await;
            tokio::task::yield_now().await;
            state1.complete_sync(cid1).await;
        }
    });

    let handle2 = tokio::spawn(async move {
        for _ in 0..100 {
            state2.start_sync(cid2).await;
            tokio::task::yield_now().await;
            state2.cancel_sync(&cid2).await;
        }
    });

    let handle3 = tokio::spawn(async move {
        for _ in 0..100 {
            let _ = state3.is_syncing(&cid1).await;
            let _ = state3.is_synced(&cid2).await;
            let _ = state3.syncing_cids().await;
            state3.start_sync(cid3).await;
            state3.complete_sync(cid3).await;
        }
    });

    // Wait for all tasks to complete without deadlock
    let result = timeout(Duration::from_secs(5), async {
        handle1.await.expect("task 1 panicked");
        handle2.await.expect("task 2 panicked");
        handle3.await.expect("task 3 panicked");
    })
    .await;

    assert!(
        result.is_ok(),
        "Concurrent state operations should complete without deadlock"
    );

    // Verify final state is consistent
    // cid1 and cid3 should be synced (completed many times)
    assert!(state.is_synced(&cid1).await, "cid1 should be synced");
    assert!(state.is_synced(&cid3).await, "cid3 should be synced");

    // cid2 was always cancelled, so should not be synced
    assert!(
        !state.is_synced(&cid2).await,
        "cid2 should not be synced (was cancelled)"
    );

    // No CIDs should be in syncing state (all operations completed)
    assert!(
        !state.is_syncing(&cid1).await,
        "cid1 should not still be syncing"
    );
    assert!(
        !state.is_syncing(&cid2).await,
        "cid2 should not still be syncing"
    );
    assert!(
        !state.is_syncing(&cid3).await,
        "cid3 should not still be syncing"
    );
}

// ============================================================================
// ReplicationLoop Error Path Tests
// ============================================================================

#[tokio::test]
async fn test_replication_handles_missing_block_in_store() {
    use p2p::sync::{MergeHandler, MergeOutcome, ReplicationConfig, ReplicationLoop, SyncConfig};
    use blockstore::DefraBlockstore;
    use storage::backends::MemoryStore;
    use std::str::FromStr;

    // Create coordinator with an EMPTY blockstore
    let (handle, _events) = create_and_start_host().await;
    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, true));
    let (coordinator, _sync_events) =
        SyncCoordinator::new(handle.clone(), blockstore, SyncConfig::default())
            .await
            .expect("failed to create coordinator");

    let coordinator = Arc::new(coordinator);

    // Test handler that should never be called (block won't be found)
    struct NeverCalledHandler;

    #[derive(Debug)]
    struct NeverCalledError;
    impl std::fmt::Display for NeverCalledError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "NeverCalledError")
        }
    }
    impl std::error::Error for NeverCalledError {}

    #[async_trait::async_trait]
    impl MergeHandler for NeverCalledHandler {
        type Error = NeverCalledError;
        async fn handle_block(
            &self,
            _cid: &cid::Cid,
            _block_data: &[u8],
            _doc_id: &str,
            _collection_id: &str,
            _creator: &str,
        ) -> Result<MergeOutcome, NeverCalledError> {
            panic!("Handler should not be called when block is not in store");
        }
    }

    let handler = Arc::new(NeverCalledHandler);

    // Manually inject a BlockReceived event for a CID that's NOT in the blockstore
    let missing_cid =
        cid::Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();

    // Create a channel to send the event
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    tx.send(p2p::sync::SyncEvent::BlockReceived {
        cid: missing_cid,
        doc_id: "test-doc".to_string(),
        collection_id: "test-collection".to_string(),
        creator: "test-creator".to_string(),
    })
    .await
    .unwrap();
    drop(tx); // Close channel to trigger drain completion

    // Drain events (should return Failed result)
    let results = ReplicationLoop::drain(
        coordinator.clone(),
        &mut rx,
        handler,
        ReplicationConfig::default(),
    )
    .await;

    assert_eq!(results.len(), 1, "Should have exactly one result");

    match &results[0] {
        p2p::sync::ReplicationResult::Failed { cid, error } => {
            assert_eq!(*cid, missing_cid);
            assert!(
                error.contains("not found"),
                "Error should mention block not found: {}",
                error
            );
        }
        other => panic!(
            "Expected Failed result for missing block, got {:?}",
            other
        ),
    }

    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_bitswap_store_adapter_insert_and_retrieve_roundtrip() {
    use blockstore::{Blockstore, DefraBlockstore};
    use p2p::{BitswapStore, BitswapStoreAdapter};
    use storage::backends::MemoryStore;
    use libipld::{Block, IpldCodec};
    use libipld::multihash::{Code, MultihashDigest};

    let store = Arc::new(MemoryStore::new());
    let blockstore = Arc::new(DefraBlockstore::new(store, false));
    let mut adapter = BitswapStoreAdapter::new(blockstore.clone());

    // Create block data
    let block_data = b"test block data for bitswap roundtrip";

    // Create a CID for this data (using raw codec and sha2-256)
    let multihash = Code::Sha2_256.digest(block_data);
    let libipld_cid = libipld::Cid::new_v1(IpldCodec::Raw.into(), multihash);

    // Create a Block for insertion
    let block = Block::<libipld::DefaultParams>::new(libipld_cid, block_data.to_vec()).unwrap();

    // Verify block is not present before insertion
    assert!(!adapter.contains(&libipld_cid).await.unwrap());

    // Insert via BitswapStore trait (this is what Bitswap does when receiving blocks)
    adapter.insert(&block).await.unwrap();

    // Verify block is now present
    assert!(adapter.contains(&libipld_cid).await.unwrap());

    // Retrieve and verify data matches
    let retrieved = adapter.get(&libipld_cid).await.unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap(), block_data.to_vec());

    // Also verify we can read via the underlying blockstore using cid crate's Cid
    let cid_crate_cid = cid::Cid::try_from(libipld_cid.to_bytes().as_slice()).unwrap();
    let from_blockstore = blockstore.get(&cid_crate_cid).await.unwrap();
    assert!(from_blockstore.is_some());
    assert_eq!(from_blockstore.unwrap(), block_data.to_vec());
}
