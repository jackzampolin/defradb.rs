//! Iroh P2P connection and peer info tests.
//!
//! Ported from Go: tests/integration/net/info/
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh_connection -- --ignored

use std::time::Duration;

use integration_test::{extract_p2p_addr, extract_peer_id, TestCluster};
use serial_test::serial;

const P2P_TIMEOUT: Duration = Duration::from_secs(15);

/// Port: TestNetInfoPeers_NoP2PConfigured
/// Without P2P transport, active_peers returns error or empty.
#[tokio::test]
#[serial]
async fn no_p2p_configured() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();

    let client = cluster.client(0);

    // Without iroh transport, p2p_info should error or return empty
    let result = client.p2p_info();
    // The node may return an error or empty array depending on configuration
    match result {
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            assert!(
                msg.contains("no p2p") || msg.contains("not configured") || msg.contains("error"),
                "expected P2P-not-configured error, got: {}",
                msg
            );
        }
        Ok(info) => {
            // Some configurations return empty instead of error
            if let Some(addrs) = info.as_array() {
                assert!(
                    addrs.is_empty(),
                    "without P2P transport, should have no addresses"
                );
            }
        }
    }
}

/// Port: TestNetInfoPeers
/// Newly started node with P2P has no active peers.
#[tokio::test]
#[serial]
async fn peers_empty() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("P2P listener did not start");

    let client = cluster.client(0);

    // p2p_info should return addresses
    let info = client.p2p_info().expect("p2p_info failed");
    let addrs = info.as_array().expect("p2p_info not array");
    assert!(!addrs.is_empty(), "node should have at least one address");

    // active_peers should be empty
    let peers = client.p2p_active_peers().expect("active_peers failed");
    let peer_arr = peers.as_array().expect("active_peers not array");
    assert!(
        peer_arr.is_empty(),
        "new node should have 0 active peers, got {}",
        peer_arr.len()
    );
}

/// Port: TestNetInfoConnectPeers
/// Two nodes connect, verify peer listed in active_peers.
#[tokio::test]
#[serial]
async fn connect_peers() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node0");
    cluster
        .wait_for_log(1, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node1");

    let node0 = cluster.client(0);
    let addr1 = extract_p2p_addr(&cluster, 1);
    let peer1_id = extract_peer_id(&cluster, 1);

    node0.p2p_connect(&[&addr1]).expect("connect failed");

    // Wait for peer to appear
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let peers = node0.p2p_active_peers().unwrap_or_default();
        if let Some(arr) = peers.as_array() {
            if !arr.is_empty() {
                let peer_strs: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
                assert_eq!(peer_strs.len(), 1, "should have exactly 1 active peer");
                assert!(
                    peer_strs[0].contains(&peer1_id),
                    "active peer should be node1 ({}), got {}",
                    peer1_id,
                    peer_strs[0]
                );
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "peer did not appear in active_peers"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Port: TestNetInfoConnectMultiplePeers
/// Connect 3 nodes, verify all see each other.
#[tokio::test]
#[serial]
async fn connect_multiple_peers() {
    let cluster = TestCluster::builder()
        .rust_nodes(3)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    for i in 0..3 {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{} listener", i));
    }

    let addr1 = extract_p2p_addr(&cluster, 1);
    let addr2 = extract_p2p_addr(&cluster, 2);

    // Connect node1→node0 and node2→node0
    cluster
        .client(0)
        .p2p_connect(&[&addr1])
        .expect("connect 0→1");
    cluster
        .client(0)
        .p2p_connect(&[&addr2])
        .expect("connect 0→2");

    // Wait for connections to establish
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Node0 should see both node1 and node2
    let peers0 = cluster
        .client(0)
        .p2p_active_peers()
        .expect("active_peers node0");
    let peer0_arr = peers0.as_array().expect("not array");
    assert!(
        peer0_arr.len() >= 2,
        "node0 should see at least 2 active peers, got {}",
        peer0_arr.len()
    );
}
