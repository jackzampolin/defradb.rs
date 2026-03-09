//! Smoke tests for iroh P2P transport.
//!
//! These tests verify basic P2P functionality using the iroh QUIC transport
//! instead of libp2p. Run with:
//!   cargo test -p integration-test --test p2p_iroh_smoke -- --ignored

use std::time::{Duration, Instant};

use integration_test::TestCluster;
use serial_test::serial;

/// Get the iroh peer ID from a node's p2p_info response.
///
/// For iroh, p2p_info returns `["{listen_addr}/p2p/{endpoint_id}"]`.
/// We extract the endpoint ID after the last `/p2p/` segment.
fn extract_iroh_peer_id(cluster: &TestCluster, node_index: usize) -> String {
    let client = cluster.client(node_index);
    let info = client.p2p_info().expect("failed to get p2p info");
    let addr = info
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node has no P2P address");

    // Extract endpoint ID from compound address format
    if let Some(pos) = addr.rfind("/p2p/") {
        addr[pos + 5..].to_string()
    } else {
        addr.to_string()
    }
}

/// Verify that an iroh node starts and reports P2P info.
#[tokio::test]
#[serial]
async fn iroh_node_info() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("iroh P2P listener did not start");

    let client = cluster.client(0);
    let info = client.p2p_info().expect("failed to get p2p info");
    let addrs = info.as_array().expect("p2p_info should be an array");
    assert!(
        !addrs.is_empty(),
        "iroh node should report at least one address"
    );

    let peer_id = extract_iroh_peer_id(&cluster, 0);
    assert!(!peer_id.is_empty(), "iroh peer ID should not be empty");
    assert!(
        peer_id.len() > 10,
        "iroh peer ID should be a substantial identifier, got: {}",
        peer_id
    );

    // Verify the address contains /p2p/ format
    let addr = addrs[0].as_str().expect("address should be a string");
    assert!(
        addr.contains("/p2p/"),
        "iroh address should contain /p2p/ segment, got: {}",
        addr
    );
}

/// Verify that two iroh nodes can connect to each other.
#[tokio::test]
#[serial]
async fn iroh_peer_connect() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Get node1's peer info (full address from p2p_info)
    let info1 = node1.p2p_info().expect("failed to get node1 p2p info");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");

    // Connect node0 to node1
    node0
        .p2p_connect(&[addr1])
        .expect("failed to connect peers");

    // Get node1's peer ID for verification
    let node1_peer_id = extract_iroh_peer_id(&cluster, 1);

    // Verify connection via active peers
    let deadline = Instant::now() + Duration::from_secs(10);
    let found_peers;
    loop {
        let peers = node0.p2p_active_peers().unwrap_or_default();
        if let Some(arr) = peers.as_array() {
            if !arr.is_empty() {
                found_peers = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<_>>();
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "peers did not connect within timeout"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Verify the connected peer is actually node1
    assert_eq!(
        found_peers.len(),
        1,
        "expected exactly 1 active peer, got {:?}",
        found_peers
    );
    assert!(
        found_peers[0].contains(&node1_peer_id),
        "active peer should be node1 ({}), got {}",
        node1_peer_id,
        found_peers[0]
    );
}

/// Verify P2P collection management works with iroh transport.
#[tokio::test]
#[serial]
async fn iroh_collection_management() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("P2P listener did not start");

    let node = cluster.client(0);

    // Deploy schema
    node.schema_add("type Note { text: String }")
        .expect("schema add");

    // P2P collections should be empty initially
    let cols = node.p2p_collection_list().expect("p2p_collection_list");
    let cols_arr = cols.as_array().expect("collection_list not array");
    assert!(cols_arr.is_empty(), "expected 0 P2P collections initially");

    // Add collection
    node.p2p_collection_add(&["Note"])
        .expect("p2p_collection_add");

    // Verify collection was added
    let cols_after = node
        .p2p_collection_list()
        .expect("p2p_collection_list after add");
    let cols_after_arr = cols_after.as_array().expect("collection_list not array");
    assert_eq!(
        cols_after_arr.len(),
        1,
        "expected 1 P2P collection after add"
    );

    // Remove collection
    node.p2p_collection_delete(&["Note"])
        .expect("p2p_collection_delete");

    // Verify collection was removed
    let cols_final = node
        .p2p_collection_list()
        .expect("p2p_collection_list after delete");
    let cols_final_arr = cols_final.as_array().expect("collection_list not array");
    assert!(
        cols_final_arr.is_empty(),
        "expected 0 P2P collections after delete"
    );
}

/// Verify document replication between two iroh nodes.
#[tokio::test]
#[serial]
async fn iroh_replication() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Get node1 address
    let info1 = node1.p2p_info().expect("failed to get node1 p2p info");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");

    // Deploy schema on both nodes
    node0
        .schema_add("type User { name: String  age: Int }")
        .expect("schema add node0");
    node1
        .schema_add("type User { name: String  age: Int }")
        .expect("schema add node1");

    // Connect peers
    node0
        .p2p_connect(&[addr1])
        .expect("failed to connect peers");

    // Enable collection sync and set replicator
    node0
        .p2p_collection_add(&["User"])
        .expect("p2p_collection_add node0");
    node1
        .p2p_collection_add(&["User"])
        .expect("p2p_collection_add node1");
    node0
        .p2p_replicator_set(&["User"], addr1)
        .expect("p2p_replicator_set");

    // Create document on node0
    let data = node0
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID name age } }"#)
        .expect("create user");
    let doc_id = data["add_User"][0]["_docID"]
        .as_str()
        .expect("missing _docID");

    // Poll node1 until document appears
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let result = node1.query("query { User { _docID name age } }").unwrap();
        if let Some(users) = result["User"].as_array() {
            if !users.is_empty() {
                assert_eq!(users[0]["_docID"].as_str().unwrap(), doc_id);
                assert_eq!(users[0]["name"], "Alice");
                assert_eq!(users[0]["age"], 30);
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "document did not replicate via iroh within timeout"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
