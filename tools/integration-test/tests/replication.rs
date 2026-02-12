use std::time::{Duration, Instant};

use integration_test::TestCluster;

async fn replication_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Deploy schema on both nodes
    node0
        .schema_add("type User { name: String  age: Int }")
        .unwrap();
    node1
        .schema_add("type User { name: String  age: Int }")
        .unwrap();

    // Get P2P info, extract addresses
    let info1 = node1.p2p_info().unwrap();
    let addr1 = info1["addresses"][0]
        .as_str()
        .expect("node1 has no P2P address");

    // Connect peers
    node0.p2p_connect(&[addr1]).unwrap();

    // Event-based: wait for "Peer connected" on both nodes
    let timeout = Duration::from_secs(10);
    cluster
        .wait_for_log(0, "peer_connected", timeout)
        .await
        .unwrap();
    cluster
        .wait_for_log(1, "peer_connected", timeout)
        .await
        .unwrap();

    // Enable collection sync + set replicator
    node0.p2p_collection_add(&["User"]).unwrap();
    node1.p2p_collection_add(&["User"]).unwrap();
    node0.p2p_replicator_set(&["User"], addr1).unwrap();

    // Create doc on node 0
    let data = node0
        .query(r#"mutation { create_User(input: {name: "Alice", age: 30}) { _docID name age } }"#)
        .unwrap();
    let doc_id = data["create_User"][0]["_docID"]
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

#[tokio::test]
#[ignore]
async fn rust_rust_replication() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    replication_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_rust_replication() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    replication_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_go_replication() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    replication_test(cluster).await;
}
