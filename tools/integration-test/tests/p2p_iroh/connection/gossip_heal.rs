//! Gossip send-path healing tests (#1092).
//!
//! In-process approximation of the acceptance scenario: kill one peer's
//! transport, bring it back, and assert the survivor re-establishes the
//! gossip mesh (PeerSubscribed again) and message delivery. Also verifies
//! that the periodic gossip path refresh (the heal sweep) does not disrupt
//! a healthy mesh.
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh -- connection::gossip_heal

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use p2p::iroh::{
    load_or_generate_secret_key, spawn_endpoint, GossipHealConfig, IrohDiscoveryConfig,
    IrohEndpointConfig, IrohRelayModeConfig, IrohTransport,
};
use p2p::topics::{DefraTopic, ENCRYPTION_TOPIC};
use p2p::{P2PTransport, TransportEvent};
use tokio::sync::mpsc::Receiver;
use tokio::time::timeout;

async fn test_config(gossip_heal: GossipHealConfig) -> IrohEndpointConfig {
    IrohEndpointConfig {
        secret_key: load_or_generate_secret_key(None)
            .await
            .expect("generate iroh key"),
        relay_mode: IrohRelayModeConfig::Disabled,
        discovery: IrohDiscoveryConfig::Disabled,
        bind_port: None,
        bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        max_concurrent_multipath_paths: None,
        gossip_heal,
    }
}

async fn wait_peer_subscribed<T>(
    events: &mut Receiver<TransportEvent<T>>,
    want_topic: &str,
    context: &str,
) {
    loop {
        let event = timeout(Duration::from_secs(15), events.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for PeerSubscribed ({context})"))
            .expect("iroh event channel closed");
        if let TransportEvent::PeerSubscribed { topic, .. } = &event {
            if topic == want_topic {
                return;
            }
        }
    }
}

async fn wait_peer_unsubscribed<T>(
    events: &mut Receiver<TransportEvent<T>>,
    want_topic: &str,
    context: &str,
) {
    loop {
        let event = timeout(Duration::from_secs(30), events.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for PeerUnsubscribed ({context})"))
            .expect("iroh event channel closed");
        if let TransportEvent::PeerUnsubscribed { topic, .. } = &event {
            if topic == want_topic {
                return;
            }
        }
    }
}

async fn assert_raw_delivery<T>(
    from: &IrohTransport,
    to_events: &mut Receiver<TransportEvent<T>>,
    payload: Vec<u8>,
    context: &str,
) {
    from.publish_raw(ENCRYPTION_TOPIC.to_string(), payload.clone())
        .await
        .expect("publish_raw");
    loop {
        let event = timeout(Duration::from_secs(10), to_events.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for gossip delivery ({context})"))
            .expect("iroh event channel closed");
        if let TransportEvent::GossipRawMessage { topic, data, .. } = event {
            if topic == ENCRYPTION_TOPIC && data == payload {
                return;
            }
        }
    }
}

/// Acceptance-shaped regression (#1092): kill one peer's transport, restart
/// it under the same identity, and assert the survivor's gossip mesh heals —
/// PeerSubscribed fires again and messages flow — via the 0→1 reconnect heal.
#[tokio::test]
async fn remesh_after_peer_restart() {
    let config0 = test_config(GossipHealConfig::default()).await;
    let config1 = test_config(GossipHealConfig::default()).await;
    let key0 = config0.secret_key.clone();
    let key1 = config1.secret_key.clone();

    let (tx0, mut events0, _r0, task0) = spawn_endpoint(config0).await.expect("spawn endpoint 0");
    let (tx1, mut events1, _r1, task1) = spawn_endpoint(config1).await.expect("spawn endpoint 1");
    let transport0 = IrohTransport::new(tx0, key0);
    let transport1 = IrohTransport::new(tx1, key1.clone());

    transport0
        .dial(
            transport1.local_peer_id(),
            transport1.listen_addresses().await.expect("addrs 1"),
        )
        .await
        .expect("dial 0→1");
    transport0
        .poll_until_connected(transport1.local_peer_id(), Duration::from_secs(5))
        .await
        .expect("0 sees 1");
    transport1
        .poll_until_connected(transport0.local_peer_id(), Duration::from_secs(5))
        .await
        .expect("1 sees 0");

    transport0
        .subscribe(DefraTopic::Encryption)
        .await
        .expect("subscribe 0");
    transport1
        .subscribe(DefraTopic::Encryption)
        .await
        .expect("subscribe 1");
    transport0
        .register_pubsub_rpc_topic(ENCRYPTION_TOPIC.to_string())
        .await
        .expect("raw topic 0");
    wait_peer_subscribed(&mut events0, ENCRYPTION_TOPIC, "initial mesh, node 0").await;
    wait_peer_subscribed(&mut events1, ENCRYPTION_TOPIC, "initial mesh, node 1").await;

    // Kill node 1's transport entirely.
    transport1.shutdown().await.expect("shutdown 1");
    task1.await.expect("endpoint task 1");
    drop(transport1);
    wait_peer_unsubscribed(&mut events0, ENCRYPTION_TOPIC, "after node 1 death").await;

    // Bring node 1 back under the same identity (new ephemeral port).
    let mut config1b = test_config(GossipHealConfig::default()).await;
    config1b.secret_key = key1.clone();
    let (tx1b, mut events1b, _r1b, task1b) =
        spawn_endpoint(config1b).await.expect("respawn endpoint 1");
    let transport1b = IrohTransport::new(tx1b, key1);
    transport1b
        .subscribe(DefraTopic::Encryption)
        .await
        .expect("resubscribe 1");

    transport0
        .dial(
            transport1b.local_peer_id(),
            transport1b.listen_addresses().await.expect("addrs 1b"),
        )
        .await
        .expect("redial 0→1");
    transport0
        .poll_until_connected(transport1b.local_peer_id(), Duration::from_secs(5))
        .await
        .expect("0 sees 1 again");

    wait_peer_subscribed(&mut events0, ENCRYPTION_TOPIC, "healed mesh, node 0").await;
    wait_peer_subscribed(&mut events1b, ENCRYPTION_TOPIC, "healed mesh, node 1").await;

    assert_raw_delivery(
        &transport1b,
        &mut events0,
        vec![0xCB, 0x01, 0x02],
        "1→0 after heal",
    )
    .await;

    transport0.shutdown().await.expect("shutdown 0");
    transport1b.shutdown().await.expect("shutdown 1b");
    task0.await.expect("endpoint task 0");
    task1b.await.expect("endpoint task 1b");
}

/// The periodic heal sweep unconditionally re-dials the gossip path and swaps
/// the active gossip connection; each superseded connection is closed after a
/// grace period. Run several sweep cycles on a healthy mesh — long enough
/// that multiple superseded-connection closes fire while the mesh is live —
/// and assert the closes cause no neighbor churn (no PeerUnsubscribed) and
/// gossip delivery still works in both directions afterwards.
#[tokio::test]
async fn sweep_refresh_preserves_gossip_delivery() {
    let fast_heal = GossipHealConfig {
        refresh_interval: Duration::from_millis(500),
        backoff_base: Duration::from_millis(200),
        backoff_cap: Duration::from_secs(2),
        max_attempts: 5,
        superseded_close_grace: Duration::from_millis(750),
        refresh_probe: None,
    };
    let config0 = test_config(fast_heal.clone()).await;
    let config1 = test_config(fast_heal).await;
    let key0 = config0.secret_key.clone();
    let key1 = config1.secret_key.clone();

    let (tx0, mut events0, _r0, task0) = spawn_endpoint(config0).await.expect("spawn endpoint 0");
    let (tx1, mut events1, _r1, task1) = spawn_endpoint(config1).await.expect("spawn endpoint 1");
    let transport0 = IrohTransport::new(tx0, key0);
    let transport1 = IrohTransport::new(tx1, key1);

    transport0
        .dial(
            transport1.local_peer_id(),
            transport1.listen_addresses().await.expect("addrs 1"),
        )
        .await
        .expect("dial 0→1");
    transport0
        .poll_until_connected(transport1.local_peer_id(), Duration::from_secs(5))
        .await
        .expect("0 sees 1");
    transport1
        .poll_until_connected(transport0.local_peer_id(), Duration::from_secs(5))
        .await
        .expect("1 sees 0");

    transport0
        .subscribe(DefraTopic::Encryption)
        .await
        .expect("subscribe 0");
    transport1
        .subscribe(DefraTopic::Encryption)
        .await
        .expect("subscribe 1");
    transport0
        .register_pubsub_rpc_topic(ENCRYPTION_TOPIC.to_string())
        .await
        .expect("raw topic 0");
    transport1
        .register_pubsub_rpc_topic(ENCRYPTION_TOPIC.to_string())
        .await
        .expect("raw topic 1");
    wait_peer_subscribed(&mut events0, ENCRYPTION_TOPIC, "initial mesh, node 0").await;
    wait_peer_subscribed(&mut events1, ENCRYPTION_TOPIC, "initial mesh, node 1").await;

    // Let several sweep cycles refresh the gossip path on both sides, long
    // enough (4× the 750ms grace) that superseded-connection closes fire
    // while the mesh is live — the delayed close must not read as a peer
    // disconnect.
    tokio::time::sleep(Duration::from_secs(3)).await;

    for (events, node) in [(&mut events0, "node 0"), (&mut events1, "node 1")] {
        while let Ok(event) = events.try_recv() {
            if let TransportEvent::PeerUnsubscribed { topic, .. } = &event {
                assert_ne!(
                    topic, ENCRYPTION_TOPIC,
                    "superseded-connection close churned the gossip mesh on {node}"
                );
            }
        }
    }

    assert_raw_delivery(
        &transport0,
        &mut events1,
        vec![0xAA, 0x01],
        "0→1 after sweeps",
    )
    .await;
    assert_raw_delivery(
        &transport1,
        &mut events0,
        vec![0xBB, 0x02],
        "1→0 after sweeps",
    )
    .await;

    transport0.shutdown().await.expect("shutdown 0");
    transport1.shutdown().await.expect("shutdown 1");
    task0.await.expect("endpoint task 0");
    task1.await.expect("endpoint task 1");
}

/// A publish-only node (publish_raw without any persistent subscription — the
/// KMS `_response` responder shape) rides the same per-peer gossip send path,
/// so the heal sweep must refresh it too, and the refresh must not disrupt
/// ephemeral publishing.
#[tokio::test]
async fn sweep_refresh_covers_publish_only_node() {
    let fast_heal = GossipHealConfig {
        refresh_interval: Duration::from_millis(500),
        backoff_base: Duration::from_millis(200),
        backoff_cap: Duration::from_secs(2),
        max_attempts: 5,
        superseded_close_grace: Duration::from_millis(750),
        refresh_probe: None,
    };
    // A healthy mesh delivers even if node 1 never refreshes (node 0's
    // healing and gossip's own dialing both suffice), so delivery alone
    // cannot regress on the empty-subscription guard. The probe asserts that
    // the publish-only node itself performed refreshes.
    let publish_only_refreshes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let config0 = test_config(fast_heal.clone()).await;
    let config1 = test_config(GossipHealConfig {
        refresh_probe: Some(publish_only_refreshes.clone()),
        ..fast_heal
    })
    .await;
    let key0 = config0.secret_key.clone();
    let key1 = config1.secret_key.clone();

    let (tx0, mut events0, _r0, task0) = spawn_endpoint(config0).await.expect("spawn endpoint 0");
    let (tx1, _events1, _r1, task1) = spawn_endpoint(config1).await.expect("spawn endpoint 1");
    let transport0 = IrohTransport::new(tx0, key0);
    let transport1 = IrohTransport::new(tx1, key1);

    transport0
        .dial(
            transport1.local_peer_id(),
            transport1.listen_addresses().await.expect("addrs 1"),
        )
        .await
        .expect("dial 0→1");
    transport0
        .poll_until_connected(transport1.local_peer_id(), Duration::from_secs(5))
        .await
        .expect("0 sees 1");
    transport1
        .poll_until_connected(transport0.local_peer_id(), Duration::from_secs(5))
        .await
        .expect("1 sees 0");

    // Only node 0 subscribes; node 1 never does (publish-only).
    transport0
        .subscribe(DefraTopic::Encryption)
        .await
        .expect("subscribe 0");
    transport0
        .register_pubsub_rpc_topic(ENCRYPTION_TOPIC.to_string())
        .await
        .expect("raw topic 0");

    // No PeerSubscribed to wait for: node 1 only joins the topic ephemerally
    // at publish time. Several sweep cycles and superseded-connection closes
    // pass on both sides, with node 1's schedule driven purely by the
    // empty-subscription path.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // >= 2 requires at least one sweep-driven refresh on top of the single
    // connect-time (0->1) refresh, so a regression in either empty-
    // subscription path fails here.
    let refreshes = publish_only_refreshes.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        refreshes >= 2,
        "publish-only node performed {refreshes} gossip path refresh(es); \
         expected the connect-time refresh plus sweep-driven refreshes"
    );

    assert_raw_delivery(
        &transport1,
        &mut events0,
        vec![0xEE, 0x03],
        "publish-only 1→0 after sweeps",
    )
    .await;

    transport0.shutdown().await.expect("shutdown 0");
    transport1.shutdown().await.expect("shutdown 1");
    task0.await.expect("endpoint task 0");
    task1.await.expect("endpoint task 1");
}
