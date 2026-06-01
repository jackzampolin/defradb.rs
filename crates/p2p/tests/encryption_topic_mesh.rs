//! Step-1 discriminator probe for issue #976: does the `"encryption"`
//! gossipsub topic form a usable publish mesh between two Rust nodes?
//!
//! Mirrors what the KMS does at startup: both hosts subscribe to
//! `DefraTopic::Encryption` and register it as a `pubsub_rpc` topic (so
//! payloads arrive as `GossipRawMessage`), connect, then one publishes.

use std::time::Duration;

use p2p::testutil::MockBitswapStore;
use p2p::{DefraTopic, HostEvent, P2PHost, PeerId, ENCRYPTION_TOPIC};
use tokio::time::timeout;

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

async fn setup_pair() -> (
    p2p::P2PHostHandle,
    p2p::P2PHostHandle,
    tokio::sync::mpsc::Receiver<HostEvent>,
) {
    let store0 = MockBitswapStore::new();
    let store1 = MockBitswapStore::new();
    let (host0, handle0, _events0, _r0) = P2PHost::new(store0).await.unwrap();
    let (host1, handle1, events1, _r1) = P2PHost::new(store1).await.unwrap();

    tokio::spawn(host0.run());
    tokio::spawn(host1.run());

    // Both subscribe + register the encryption topic, exactly like
    // PubsubKeyTransport::new does at KMS startup.
    for h in [&handle0, &handle1] {
        h.subscribe(DefraTopic::Encryption).await.unwrap();
        h.register_pubsub_rpc_topic(ENCRYPTION_TOPIC.to_string())
            .await
            .unwrap();
    }

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

    (handle0, handle1, events1)
}

/// Reproduces the bug shape: subscribe to the encryption topic AFTER the
/// peers are already connected (the "late subscribe" case the KMS hits when
/// a key-miss happens), then publish immediately. Mirrors the KMS
/// `send_request` path that publishes once with no wait. Captures whether
/// the very first publish targets a peer.
#[tokio::test]
async fn encryption_topic_late_subscribe_immediate_publish() {
    let store0 = MockBitswapStore::new();
    let store1 = MockBitswapStore::new();
    let (host0, handle0, _events0, _r0) = P2PHost::new(store0).await.unwrap();
    let (host1, handle1, _events1, _r1) = P2PHost::new(store1).await.unwrap();

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

    // Subscribe AFTER connection (late subscribe).
    for h in [&handle0, &handle1] {
        h.subscribe(DefraTopic::Encryption).await.unwrap();
        h.register_pubsub_rpc_topic(ENCRYPTION_TOPIC.to_string())
            .await
            .unwrap();
    }

    // Publish immediately. This is the exact KMS code path (no wait).
    let res = handle0
        .publish_raw(ENCRYPTION_TOPIC.to_string(), b"req".to_vec())
        .await;
    println!("late-subscribe immediate encryption publish result: {res:?}");

    handle0.shutdown().await.unwrap();
    handle1.shutdown().await.unwrap();
}

/// The fix shape: poll until the topic has a known subscriber, then publish,
/// and assert the peer actually receives it as a GossipRawMessage.
#[tokio::test]
async fn encryption_topic_publish_after_subscriber_known_is_received() {
    let (handle0, handle1, mut events1) = setup_pair().await;
    let peer1 = handle1.local_peer_id_cached();

    // Wait (bounded) until handle0 knows peer1 is subscribed to encryption.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let peers = handle0
            .topic_peers(DefraTopic::Encryption)
            .await
            .unwrap_or_default();
        if peers.contains(&peer1) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "handle0 never learned peer1's encryption subscription"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let payload = b"fetch-encryption-key-request".to_vec();
    // Even after topic_peers shows the subscriber, the very first publish can
    // still race the mesh graft; retry briefly like host tests do.
    let pub_deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match handle0
            .publish_raw(ENCRYPTION_TOPIC.to_string(), payload.clone())
            .await
        {
            Ok(_) => break,
            Err(e) => {
                assert!(
                    std::time::Instant::now() < pub_deadline,
                    "publish never succeeded after subscriber known: {e}"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }

    // Assert the peer receives it as a raw message on the encryption topic.
    let got = timeout(Duration::from_secs(10), async {
        loop {
            match events1.recv().await.expect("event channel closed") {
                HostEvent::GossipRawMessage { topic, data, .. } if topic == ENCRYPTION_TOPIC => {
                    break data;
                }
                _ => continue,
            }
        }
    })
    .await
    .expect("peer never received encryption-topic message");

    assert_eq!(got, payload);

    handle0.shutdown().await.unwrap();
    handle1.shutdown().await.unwrap();
}
