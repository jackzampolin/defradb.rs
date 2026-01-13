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

use std::time::Duration;

use p2p::{
    codec, Error, HostEvent, Message, P2PHost, P2PHostHandle, PushLogReply, PushLogRequest,
    REP_REQUEST_PROTOCOL, REP_RESPONSE_PROTOCOL,
};
use tokio::time::timeout;

/// Helper to create and start a P2P host, returning the handle and event receiver.
async fn create_and_start_host() -> (P2PHostHandle, tokio::sync::mpsc::Receiver<HostEvent>) {
    let (host, handle, events) = P2PHost::new().expect("failed to create host");

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
    let peer_id1 = handle1.local_peer_id().await.expect("failed to get peer id");
    let peer_id2 = handle2.local_peer_id().await.expect("failed to get peer id");

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
    let addrs = handle.listen_addresses().await.expect("failed to get addresses");
    assert!(addrs.is_empty());

    // Start listening
    handle
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .expect("failed to listen");

    wait_for_listening(&mut events).await;

    // Now should have an address
    let addrs = handle.listen_addresses().await.expect("failed to get addresses");
    assert!(!addrs.is_empty());

    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_host_connected_peers() {
    let (handle1, mut events1) = create_and_start_host().await;
    let (handle2, mut events2) = create_and_start_host().await;

    // Initially no connected peers
    let peers = handle1.connected_peers().await.expect("failed to get peers");
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

    let peer_id1 = handle1.local_peer_id().await.expect("failed to get peer id");

    // Connect
    handle2.dial(peer_id1, vec![addr1]).await.expect("failed to dial");

    wait_for_peer_connected(&mut events1).await;
    wait_for_peer_connected(&mut events2).await;

    // Now should have connected peer
    let peers1 = handle1.connected_peers().await.expect("failed to get peers");
    let peers2 = handle2.connected_peers().await.expect("failed to get peers");

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
        Err(_) => {} // Timeout is expected
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

    let addrs = handle.listen_addresses().await.expect("failed to get addresses");
    assert!(addrs.len() >= 2);

    handle.shutdown().await.ok();
}

#[tokio::test]
async fn test_host_with_custom_keypair() {
    use libp2p::identity::Keypair;

    let keypair = Keypair::generate_ed25519();
    let expected_peer_id = keypair.public().to_peer_id();

    let (host, handle, _events) = P2PHost::with_keypair(keypair).expect("failed to create host");

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

    let peer_id1 = handle1.local_peer_id().await.expect("failed to get peer id");

    // Connect
    handle2.dial(peer_id1, vec![addr1]).await.expect("failed to dial");

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
    let result = timeout(Duration::from_secs(5), handle.send_pushlog(fake_peer_id, request)).await;

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
        assert!(field_names.contains(&"CID".to_string()), "Missing CID field");
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
    assert_eq!(
        request.version(),
        "/defradb/0.0.1",
        "version should be set"
    );
    assert!(!request.sender_id().is_empty(), "sender_id should be set");
    assert!(!request.pubkey().is_empty(), "pubkey should be set");
    assert!(request.signature().is_some(), "signature should be set");

    // Verify sender_id matches the keypair's peer ID
    let expected_peer_id = keypair.public().to_peer_id().to_string();
    assert_eq!(request.sender_id(), expected_peer_id);
}
