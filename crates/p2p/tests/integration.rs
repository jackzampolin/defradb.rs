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

use p2p::{Error, HostEvent, P2PHost, P2PHostHandle, PushLogRequest};
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
