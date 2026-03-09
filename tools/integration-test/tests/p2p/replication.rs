use std::time::{Duration, Instant};

use integration_test::{for_each_p2p_topology, TestCluster};

async fn replication_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Wait for P2P listeners to be ready on both nodes before calling p2p info
    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    // Get full multiaddrs (with peer ID) from p2p info
    // Response is a flat JSON array: ["/ip4/127.0.0.1/tcp/PORT/p2p/PEERID"]
    let info1 = node1.p2p_info().expect("failed to get node1 p2p info");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");

    // Deploy schema on both nodes
    node0
        .schema_add("type User { name: String  age: Int }")
        .unwrap();
    node1
        .schema_add("type User { name: String  age: Int }")
        .unwrap();

    // Connect peers (HTTP call is synchronous — returns after connection is established)
    node0.p2p_connect(&[addr1]).unwrap();

    // Enable collection sync + set replicator
    node0.p2p_collection_add(&["User"]).unwrap();
    node1.p2p_collection_add(&["User"]).unwrap();
    node0.p2p_replicator_set(&["User"], addr1).unwrap();

    // Create doc on node 0
    let data = node0
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID name age } }"#)
        .unwrap();
    let doc_id = data["add_User"][0]["_docID"]
        .as_str()
        .expect("missing _docID");

    // Poll-based: query node 1 until doc appears
    let deadline = Instant::now() + Duration::from_secs(15);
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
            "doc did not replicate within timeout"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

for_each_p2p_topology!(replication, replication_test, .with_p2p());
