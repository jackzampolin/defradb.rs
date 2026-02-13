use std::time::Duration;

use integration_test::{poll_until, TestCluster};

async fn replication_crud_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Wait for P2P listeners
    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    // Get node1 multiaddr
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

    // Connect and set up replication
    node0.p2p_connect(&[addr1]).unwrap();
    node0.p2p_collection_add(&["User"]).unwrap();
    node1.p2p_collection_add(&["User"]).unwrap();
    node0.p2p_replicator_set(&["User"], addr1).unwrap();

    // Step 1: Create 3 documents on node 0
    let alice = node0
        .query(r#"mutation { create_User(input: {name: "Alice", age: 30}) { _docID name age } }"#)
        .unwrap();
    let alice_id = alice["create_User"][0]["_docID"]
        .as_str()
        .expect("missing Alice _docID")
        .to_string();

    let bob = node0
        .query(r#"mutation { create_User(input: {name: "Bob", age: 25}) { _docID name age } }"#)
        .unwrap();
    let bob_id = bob["create_User"][0]["_docID"]
        .as_str()
        .expect("missing Bob _docID")
        .to_string();

    node0
        .query(r#"mutation { create_User(input: {name: "Carol", age: 35}) { _docID name age } }"#)
        .unwrap();

    // Step 2: Wait for all 3 to replicate to node 1
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { User { _docID name age } }")
                .unwrap();
            result["User"]
                .as_array()
                .map(|arr| arr.len() == 3)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "3 docs did not replicate",
    )
    .await;

    // Step 3: Update Alice's age on node 0
    node0
        .query(&format!(
            r#"mutation {{ update_User(docID: "{}", input: {{age: 31}}) {{ _docID name age }} }}"#,
            alice_id
        ))
        .unwrap();

    // Step 4: Wait for update to replicate, verify on node 1
    let alice_id_ref = &alice_id;
    poll_until(
        || {
            let result = node1_ref
                .query("query { User { _docID name age } }")
                .unwrap();
            if let Some(users) = result["User"].as_array() {
                users
                    .iter()
                    .any(|u| u["_docID"].as_str() == Some(alice_id_ref.as_str()) && u["age"] == 31)
            } else {
                false
            }
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "Alice update did not replicate",
    )
    .await;

    // Step 5: Query with filter on node 1
    let filtered = node1
        .query(r#"query { User(filter: {age: {_gt: 25}}) { _docID name age } }"#)
        .unwrap();
    let filtered_users = filtered["User"]
        .as_array()
        .expect("filtered result not array");
    assert_eq!(
        filtered_users.len(),
        2,
        "expected Alice and Carol, got {:?}",
        filtered_users
    );
    let names: Vec<&str> = filtered_users
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert!(names.contains(&"Alice"), "Alice missing from filter result");
    assert!(names.contains(&"Carol"), "Carol missing from filter result");

    // Step 6: Delete Bob on node 0
    node0.collection_delete("User", &bob_id).unwrap();

    // Step 7: Wait for deletion to replicate
    poll_until(
        || {
            let result = node1_ref.query("query { User { _docID name } }").unwrap();
            result["User"]
                .as_array()
                .map(|arr| arr.len() == 2)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "Bob deletion did not replicate",
    )
    .await;

    // Verify remaining docs are Alice and Carol
    let remaining = node1.query("query { User { name } }").unwrap();
    let remaining_names: Vec<&str> = remaining["User"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert!(remaining_names.contains(&"Alice"));
    assert!(remaining_names.contains(&"Carol"));
    assert!(!remaining_names.contains(&"Bob"));
}

#[tokio::test]
#[ignore]
async fn rust_rust_replication_crud() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    replication_crud_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_go_replication_crud() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();
    replication_crud_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_rust_replication_crud() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    replication_crud_test(cluster).await;
}
