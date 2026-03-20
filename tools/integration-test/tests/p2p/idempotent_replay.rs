use std::time::Duration;

use integration_test::{for_each_p2p_topology, poll_until, TestCluster};

/// Calling add_replicator twice with the same collections should be idempotent:
/// the second call must succeed without error and live replication must continue
/// working after the duplicate call.
async fn idempotent_reconnect_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0
        .schema_add("type User { name: String  age: Int }")
        .expect("schema add node0");
    node1
        .schema_add("type User { name: String  age: Int }")
        .expect("schema add node1");

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    let info1 = node1.p2p_info().expect("p2p_info node1");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address")
        .to_string();

    // Set up replicator (first time)
    node0.p2p_connect(&[&addr1]).expect("p2p_connect");
    node0
        .p2p_collection_add(&["User"])
        .expect("collection add node0");
    node1
        .p2p_collection_add(&["User"])
        .expect("collection add node1");
    node0
        .p2p_replicator_set(&["User"], &addr1)
        .expect("first replicator_set");

    // Create doc and verify live replication works
    node0
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create Alice on node0");

    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref.query("query { User { name } }").unwrap();
            result["User"]
                .as_array()
                .map(|arr| arr.len() == 1)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "Alice did not replicate to node1",
    )
    .await;

    // Call add_replicator AGAIN with the same collections (idempotent reconnect).
    // This must not error, must not trigger a full replay storm, and must not
    // break the existing replicator.
    node0
        .p2p_replicator_set(&["User"], &addr1)
        .expect("second replicator_set (idempotent)");

    // Create another document after the duplicate call
    node0
        .query(r#"mutation { add_User(input: {name: "Bob", age: 25}) { _docID } }"#)
        .expect("create Bob on node0");

    // Verify Bob arrives via live replication (proves the replicator still works)
    poll_until(
        || {
            let result = node1_ref.query("query { User { name } }").unwrap();
            result["User"]
                .as_array()
                .map(|arr| arr.len() == 2)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "Bob did not replicate to node1 after idempotent replicator_set",
    )
    .await;

    // Verify both documents have correct data
    let final_result = node1.query("query { User { name age } }").unwrap();
    let users = final_result["User"].as_array().expect("User not array");
    assert_eq!(users.len(), 2, "expected 2 users on node1");

    let names: Vec<&str> = users.iter().map(|u| u["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"Alice"), "Alice missing from node1");
    assert!(names.contains(&"Bob"), "Bob missing from node1");
}

for_each_p2p_topology!(idempotent_reconnect, idempotent_reconnect_test, .with_p2p());
