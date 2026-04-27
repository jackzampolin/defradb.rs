//! Tests for P2P host functionality.

use bytes::Bytes;
use crypto::generate_ed25519;
use identity::{Identity, RawIdentity};
use std::time::Duration;

use p2p::testutil::MockBitswapStore;
use p2p::{
    message::{PushLogBroadcast, PushLogReply},
    signing::sign_message,
    DefraTopic, P2PHost, PeerId, PushLogRequest,
};
use tokio::time::timeout;

fn authorizer_identity() -> RawIdentity {
    RawIdentity::from_private_key(generate_ed25519().unwrap()).unwrap()
}

async fn wait_until_connected(handle: &p2p::P2PHostHandle, peer_id: PeerId) {
    let start = std::time::Instant::now();
    loop {
        if handle
            .connected_peers()
            .await
            .unwrap_or_default()
            .contains(&peer_id)
        {
            return;
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "timed out waiting for connection to {peer_id}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn send_two_stream_request_and_capture_flag(
    sender: &p2p::P2PHostHandle,
    receiver: &p2p::P2PHostHandle,
    events: &mut tokio::sync::mpsc::Receiver<p2p::HostEvent>,
    target_peer_id: PeerId,
    request: PushLogRequest,
) -> bool {
    let sender_handle = sender.clone();
    let receiver_handle = receiver.clone();
    let send_task = tokio::spawn(async move {
        sender_handle
            .send_two_stream_request(target_peer_id, request)
            .await
            .unwrap();
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let is_explicit_replicator = loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let event = timeout(remaining, events.recv())
            .await
            .expect("timed out waiting for two-stream request")
            .expect("host event channel closed");

        match event {
            p2p::HostEvent::TwoStreamRequest {
                peer_id,
                request,
                is_explicit_replicator,
                ..
            } => {
                let mut reply = PushLogReply::success(&request.message_id);
                sign_message(receiver_handle.keypair(), &mut reply).unwrap();
                receiver_handle
                    .send_two_stream_response(peer_id, reply)
                    .await
                    .unwrap();
                break is_explicit_replicator;
            }
            _ => continue,
        }
    };

    send_task.await.unwrap();
    is_explicit_replicator
}

fn explicit_replay_capability(
    sender: &p2p::P2PHostHandle,
    target_peer_id: PeerId,
    collection_id: &str,
) -> (String, String) {
    let identity = authorizer_identity();
    let did = identity.did().unwrap().to_string();
    let capability = p2p::generate_explicit_replay_capability(
        &identity,
        &sender.local_peer_id_cached().to_string(),
        &target_peer_id.to_string(),
        collection_id,
        Duration::from_secs(60 * 60),
    )
    .unwrap();
    (did, capability)
}

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
async fn test_two_stream_non_replicator_is_not_marked_explicit() {
    let store0 = MockBitswapStore::new();
    let store1 = MockBitswapStore::new();
    let (host0, handle0, _events0, _replicators0) = P2PHost::new(store0).await.unwrap();
    let (host1, handle1, mut events1, _replicators1) = P2PHost::new(store1).await.unwrap();

    tokio::spawn(host0.run());
    tokio::spawn(host1.run());

    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let addr1 = handle1.listen_addresses().await.unwrap().remove(0);
    let peer1 = handle1.local_peer_id_cached();
    let peer0 = handle0.local_peer_id_cached();

    handle0.dial(peer1, vec![addr1]).await.unwrap();
    wait_until_connected(&handle0, peer1).await;
    wait_until_connected(&handle1, peer0).await;

    let mut request = PushLogRequest::new(
        "doc1".to_string(),
        Bytes::from(vec![1, 2, 3]),
        "collection1".to_string(),
        "creator1".to_string(),
        Bytes::from(b"block-data".to_vec()),
    );
    sign_message(handle0.keypair(), &mut request).unwrap();

    let is_explicit =
        send_two_stream_request_and_capture_flag(&handle0, &handle1, &mut events1, peer1, request)
            .await;
    assert!(
        !is_explicit,
        "ordinary two-stream push must not get explicit trust"
    );

    handle0.shutdown().await.unwrap();
    handle1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_identity_protocol_roundtrip_returns_defra_identity() {
    let store0 = MockBitswapStore::new();
    let store1 = MockBitswapStore::new();
    let node_identity0 = std::sync::Arc::new(authorizer_identity());
    let node_identity1 = std::sync::Arc::new(authorizer_identity());
    let (host0, handle0, _events0, _replicators0) = P2PHost::with_keypair_and_config_and_identity(
        libp2p::identity::Keypair::generate_ed25519(),
        store0,
        p2p::P2PHostConfig::default(),
        Some(node_identity0),
    )
    .await
    .unwrap();
    let (host1, handle1, _events1, _replicators1) = P2PHost::with_keypair_and_config_and_identity(
        libp2p::identity::Keypair::generate_ed25519(),
        store1,
        p2p::P2PHostConfig::default(),
        Some(node_identity1.clone()),
    )
    .await
    .unwrap();

    tokio::spawn(host0.run());
    tokio::spawn(host1.run());

    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let addr1 = handle1.listen_addresses().await.unwrap().remove(0);
    let peer1 = handle1.local_peer_id_cached();
    let peer0 = handle0.local_peer_id_cached();

    handle0.dial(peer1, vec![addr1]).await.unwrap();
    wait_until_connected(&handle0, peer1).await;
    wait_until_connected(&handle1, peer0).await;

    let peer_identity = timeout(Duration::from_secs(5), handle0.get_peer_identity(peer1))
        .await
        .expect("timed out waiting for identity reply")
        .unwrap()
        .unwrap();
    assert_eq!(
        peer_identity.to_string(),
        node_identity1.did().unwrap().to_string()
    );

    handle0.shutdown().await.unwrap();
    handle1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_two_stream_registered_replicator_without_capability_is_not_marked_explicit() {
    let store0 = MockBitswapStore::new();
    let store1 = MockBitswapStore::new();
    let (host0, handle0, _events0, _replicators0) = P2PHost::new(store0).await.unwrap();
    let (host1, handle1, mut events1, _replicators1) = P2PHost::new(store1).await.unwrap();

    tokio::spawn(host0.run());
    tokio::spawn(host1.run());

    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let addr1 = handle1.listen_addresses().await.unwrap().remove(0);
    let peer1 = handle1.local_peer_id_cached();
    let peer0 = handle0.local_peer_id_cached();

    handle0.dial(peer1, vec![addr1]).await.unwrap();
    wait_until_connected(&handle0, peer1).await;
    wait_until_connected(&handle1, peer0).await;

    handle0
        .create_replicator(peer1, vec!["collection1".to_string()])
        .await
        .unwrap();

    let mut request = PushLogRequest::new(
        "doc1".to_string(),
        Bytes::from(vec![1, 2, 3]),
        "collection1".to_string(),
        "creator1".to_string(),
        Bytes::from(b"block-data".to_vec()),
    );
    sign_message(handle0.keypair(), &mut request).unwrap();

    let is_explicit =
        send_two_stream_request_and_capture_flag(&handle0, &handle1, &mut events1, peer1, request)
            .await;
    assert!(
        !is_explicit,
        "registered replicator without capability must not get explicit trust"
    );

    handle0.shutdown().await.unwrap();
    handle1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_two_stream_registered_replicator_with_capability_is_marked_explicit() {
    let store0 = MockBitswapStore::new();
    let store1 = MockBitswapStore::new();
    let (host0, handle0, _events0, _replicators0) = P2PHost::new(store0).await.unwrap();
    let (host1, handle1, mut events1, _replicators1) = P2PHost::new(store1).await.unwrap();

    tokio::spawn(host0.run());
    tokio::spawn(host1.run());

    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let addr1 = handle1.listen_addresses().await.unwrap().remove(0);
    let peer1 = handle1.local_peer_id_cached();
    let peer0 = handle0.local_peer_id_cached();

    handle0.dial(peer1, vec![addr1]).await.unwrap();
    wait_until_connected(&handle0, peer1).await;
    wait_until_connected(&handle1, peer0).await;

    handle0
        .create_replicator(peer1, vec!["collection1".to_string()])
        .await
        .unwrap();

    let (creator, capability) = explicit_replay_capability(&handle0, peer1, "collection1");
    handle0.set_explicit_replay_capability(peer1, &["collection1".to_string()], &capability);

    let mut request = PushLogRequest::new(
        "doc1".to_string(),
        Bytes::from(vec![1, 2, 3]),
        "collection1".to_string(),
        creator,
        Bytes::from(b"block-data".to_vec()),
    );
    sign_message(handle0.keypair(), &mut request).unwrap();

    let is_explicit =
        send_two_stream_request_and_capture_flag(&handle0, &handle1, &mut events1, peer1, request)
            .await;
    assert!(
        is_explicit,
        "registered replicator with capability must get explicit trust"
    );

    handle0.shutdown().await.unwrap();
    handle1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_two_stream_capability_for_other_collection_is_rejected() {
    let store0 = MockBitswapStore::new();
    let store1 = MockBitswapStore::new();
    let (host0, handle0, _events0, _replicators0) = P2PHost::new(store0).await.unwrap();
    let (host1, handle1, mut events1, _replicators1) = P2PHost::new(store1).await.unwrap();

    tokio::spawn(host0.run());
    tokio::spawn(host1.run());

    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let addr1 = handle1.listen_addresses().await.unwrap().remove(0);
    let peer1 = handle1.local_peer_id_cached();
    let peer0 = handle0.local_peer_id_cached();

    handle0.dial(peer1, vec![addr1]).await.unwrap();
    wait_until_connected(&handle0, peer1).await;
    wait_until_connected(&handle1, peer0).await;

    let (creator, capability) = explicit_replay_capability(&handle0, peer1, "collection-a");
    let mut request = PushLogRequest::new(
        "doc1".to_string(),
        Bytes::from(vec![1, 2, 3]),
        "collection-b".to_string(),
        creator,
        Bytes::from(b"block-data".to_vec()),
    );
    request.explicit_replay_capability = Some(capability);
    sign_message(handle0.keypair(), &mut request).unwrap();

    let is_explicit =
        send_two_stream_request_and_capture_flag(&handle0, &handle1, &mut events1, peer1, request)
            .await;
    assert!(
        !is_explicit,
        "capability for collection-a must not authorize collection-b"
    );

    handle0.shutdown().await.unwrap();
    handle1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_two_stream_expired_capability_is_rejected() {
    let store0 = MockBitswapStore::new();
    let store1 = MockBitswapStore::new();
    let (host0, handle0, _events0, _replicators0) = P2PHost::new(store0).await.unwrap();
    let (host1, handle1, mut events1, _replicators1) = P2PHost::new(store1).await.unwrap();

    tokio::spawn(host0.run());
    tokio::spawn(host1.run());

    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let addr1 = handle1.listen_addresses().await.unwrap().remove(0);
    let peer1 = handle1.local_peer_id_cached();
    let peer0 = handle0.local_peer_id_cached();

    handle0.dial(peer1, vec![addr1]).await.unwrap();
    wait_until_connected(&handle0, peer1).await;
    wait_until_connected(&handle1, peer0).await;

    let authorizer = authorizer_identity();
    let claims = p2p::ExplicitReplayCapabilityClaims {
        version: 1,
        purpose: "explicit-replay".to_string(),
        source_peer_id: handle0.local_peer_id_cached().to_string(),
        target_peer_id: peer1.to_string(),
        collection_id: "collection1".to_string(),
        authorizer_did: authorizer.did().unwrap().to_string(),
        expires_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(1),
    };
    let capability =
        p2p::generate_explicit_replay_capability_from_claims(&authorizer, claims).unwrap();

    let mut request = PushLogRequest::new(
        "doc1".to_string(),
        Bytes::from(vec![1, 2, 3]),
        "collection1".to_string(),
        authorizer.did().unwrap().to_string(),
        Bytes::from(b"block-data".to_vec()),
    );
    request.explicit_replay_capability = Some(capability);
    sign_message(handle0.keypair(), &mut request).unwrap();

    let is_explicit =
        send_two_stream_request_and_capture_flag(&handle0, &handle1, &mut events1, peer1, request)
            .await;
    assert!(!is_explicit, "expired capability must be rejected");

    handle0.shutdown().await.unwrap();
    handle1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_two_stream_invalid_authorizer_signature_is_rejected() {
    let store0 = MockBitswapStore::new();
    let store1 = MockBitswapStore::new();
    let (host0, handle0, _events0, _replicators0) = P2PHost::new(store0).await.unwrap();
    let (host1, handle1, mut events1, _replicators1) = P2PHost::new(store1).await.unwrap();

    tokio::spawn(host0.run());
    tokio::spawn(host1.run());

    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let addr1 = handle1.listen_addresses().await.unwrap().remove(0);
    let peer1 = handle1.local_peer_id_cached();
    let peer0 = handle0.local_peer_id_cached();

    handle0.dial(peer1, vec![addr1]).await.unwrap();
    wait_until_connected(&handle0, peer1).await;
    wait_until_connected(&handle1, peer0).await;

    let claimed_authorizer = authorizer_identity();
    let wrong_signer = authorizer_identity();
    let claims = p2p::ExplicitReplayCapabilityClaims {
        version: 1,
        purpose: "explicit-replay".to_string(),
        source_peer_id: handle0.local_peer_id_cached().to_string(),
        target_peer_id: peer1.to_string(),
        collection_id: "collection1".to_string(),
        authorizer_did: claimed_authorizer.did().unwrap().to_string(),
        expires_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_add(60 * 60),
    };
    let capability =
        p2p::generate_explicit_replay_capability_from_claims(&wrong_signer, claims).unwrap();

    let mut request = PushLogRequest::new(
        "doc1".to_string(),
        Bytes::from(vec![1, 2, 3]),
        "collection1".to_string(),
        claimed_authorizer.did().unwrap().to_string(),
        Bytes::from(b"block-data".to_vec()),
    );
    request.explicit_replay_capability = Some(capability);
    sign_message(handle0.keypair(), &mut request).unwrap();

    let is_explicit =
        send_two_stream_request_and_capture_flag(&handle0, &handle1, &mut events1, peer1, request)
            .await;
    assert!(
        !is_explicit,
        "capability with an invalid authorizer signature must be rejected"
    );

    handle0.shutdown().await.unwrap();
    handle1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_two_stream_capability_is_edge_scoped() {
    let store0 = MockBitswapStore::new();
    let store1 = MockBitswapStore::new();
    let store2 = MockBitswapStore::new();
    let (host0, handle0, _events0, _replicators0) = P2PHost::new(store0).await.unwrap();
    let (host1, handle1, _events1, _replicators1) = P2PHost::new(store1).await.unwrap();
    let (host2, handle2, mut events2, _replicators2) = P2PHost::new(store2).await.unwrap();

    tokio::spawn(host0.run());
    tokio::spawn(host1.run());
    tokio::spawn(host2.run());

    handle2
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let addr2 = handle2.listen_addresses().await.unwrap().remove(0);
    let peer2 = handle2.local_peer_id_cached();
    let peer1 = handle1.local_peer_id_cached();

    handle1.dial(peer2, vec![addr2]).await.unwrap();
    wait_until_connected(&handle1, peer2).await;
    wait_until_connected(&handle2, peer1).await;

    let (creator, capability_for_a_to_b) =
        explicit_replay_capability(&handle0, handle1.local_peer_id_cached(), "collection1");
    let mut request = PushLogRequest::new(
        "doc1".to_string(),
        Bytes::from(vec![1, 2, 3]),
        "collection1".to_string(),
        creator,
        Bytes::from(b"block-data".to_vec()),
    );
    request.explicit_replay_capability = Some(capability_for_a_to_b);
    sign_message(handle1.keypair(), &mut request).unwrap();

    let is_explicit =
        send_two_stream_request_and_capture_flag(&handle1, &handle2, &mut events2, peer2, request)
            .await;
    assert!(
        !is_explicit,
        "capability for A->B must not authorize replay on B->C"
    );

    handle0.shutdown().await.unwrap();
    handle1.shutdown().await.unwrap();
    handle2.shutdown().await.unwrap();
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

// Regression guard for issue #834.
//
// Go DefraDB (via go-libp2p-pubsub) uses the default message-id function,
// which hashes `(source peer, sequence number)`. Two peers publishing the
// same payload on the same topic therefore produce DIFFERENT message IDs.
//
// The Rust implementation previously overrode `message_id_fn` in
// `crates/p2p/src/behaviour.rs` to compute `sha256(message.data)`, which
// collides across senders with identical payloads. Removing that override
// restores Go parity.
//
// This test fails on the pre-fix code: both hosts publish identical
// `PushLogBroadcast` content, and the current custom `sha256(data)`
// `message_id_fn` returns the same `MessageId` from each. Post-fix, the
// IDs differ because they incorporate the sender's peer ID and sequence.
#[tokio::test]
async fn regression_834_gossipsub_message_id_distinguishes_senders() {
    let store0 = MockBitswapStore::new();
    let store1 = MockBitswapStore::new();
    let (host0, handle0, _events0, _replicators0) = P2PHost::new(store0).await.unwrap();
    let (host1, handle1, _events1, _replicators1) = P2PHost::new(store1).await.unwrap();

    tokio::spawn(host0.run());
    tokio::spawn(host1.run());

    handle0
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    handle1
        .listen("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await
        .unwrap();
    let addr1 = handle1.listen_addresses().await.unwrap().remove(0);
    let peer0 = handle0.local_peer_id_cached();
    let peer1 = handle1.local_peer_id_cached();

    handle0.dial(peer1, vec![addr1]).await.unwrap();
    wait_until_connected(&handle0, peer1).await;
    wait_until_connected(&handle1, peer0).await;

    // Both peers subscribe to the same topic.
    let topic = DefraTopic::Custom("regression-834".to_string());
    handle0.subscribe(topic.clone()).await.unwrap();
    handle1.subscribe(topic.clone()).await.unwrap();

    // Wait for gossipsub mesh formation — each peer must see the other
    // on the topic, AND the mesh GRAFT exchange must complete. Heartbeat
    // interval is 1s; we poll until `publish` actually succeeds rather
    // than relying on `topic_peers` alone (which reflects subscription
    // knowledge, not mesh membership).
    let payload = PushLogBroadcast::new(
        "doc-834".to_string(),
        Bytes::from(vec![0xCA, 0xFE]),
        "col-834".to_string(),
        "creator-834".to_string(),
        Bytes::from(vec![0x01, 0x02, 0x03]),
        None,
    );

    async fn publish_when_ready(
        handle: &p2p::P2PHostHandle,
        topic: DefraTopic,
        payload: PushLogBroadcast,
    ) -> libp2p::gossipsub::MessageId {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            match handle.publish(topic.clone(), payload.clone()).await {
                Ok(id) => return id,
                Err(e) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "publish never succeeded within 15s: {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    let id_from_peer0 = publish_when_ready(&handle0, topic.clone(), payload.clone()).await;
    let id_from_peer1 = publish_when_ready(&handle1, topic, payload).await;

    assert_ne!(
        id_from_peer0, id_from_peer1,
        "bug #834: two peers publishing identical payload produced the same \
         gossipsub MessageId. The Go-parity default (source + seqno) must \
         distinguish senders; a content-hash `message_id_fn` has been \
         reintroduced."
    );

    handle0.shutdown().await.unwrap();
    handle1.shutdown().await.unwrap();
}
