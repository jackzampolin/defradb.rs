//! Iroh P2P document update and sync tests.
//!
//! Ported from Go: tests/integration/net/simple/peer/ (update tests)
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh_peer_update -- --ignored

use std::time::Duration;

use integration_test::{extract_p2p_addr, poll_until, TestCluster};
use serial_test::serial;

const SCHEMA: &str = "type Users { name: String  age: Int }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);

/// Set up a 2-node iroh cluster with Users schema, connected with replicator 0→1.
/// Creates a doc on node0 and waits for replication to node1.
/// Returns (cluster, doc_id, addr1).
async fn setup_with_replicated_doc() -> (TestCluster, String, String) {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node1 P2P listener did not start");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(SCHEMA).expect("schema add node0");
    node1.schema_add(SCHEMA).expect("schema add node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0→1");
    node0
        .p2p_collection_add(&["Users"])
        .expect("collection add node0");
    node1
        .p2p_collection_add(&["Users"])
        .expect("collection add node1");
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator 0→1");

    // Create initial doc on node0
    let result = node0
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create user");
    let doc_id = result["add_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Wait for replication to node1
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { Users { _docID } }")
                .unwrap_or_default();
            result["Users"]
                .as_array()
                .map(|arr| !arr.is_empty())
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "initial doc did not replicate",
    )
    .await;

    (cluster, doc_id, addr1)
}

/// Port: TestP2PWithSingleDocumentSingleUpdateFromChild
/// Update on source node syncs to target.
#[tokio::test]
#[serial]
async fn single_update_from_child() {
    let (cluster, doc_id, _) = setup_with_replicated_doc().await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Update age on node0
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 60}}) {{ _docID age }} }}"#,
            doc_id
        ))
        .expect("update age on node0");

    // Verify update replicates to node1
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { Users { age } }")
                .unwrap_or_default();
            result["Users"]
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(|u| u["age"].as_i64())
                .map(|age| age == 60)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "update did not replicate to node1",
    )
    .await;
}

/// Port: TestP2PWithSingleDocumentSingleUpdateFromParent
/// Update on target node syncs back to source (with bidirectional replicator).
#[tokio::test]
#[serial]
async fn single_update_from_parent() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node1 P2P listener did not start");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(SCHEMA).expect("schema add node0");
    node1.schema_add(SCHEMA).expect("schema add node1");

    let addr0 = extract_p2p_addr(&cluster, 0);
    let addr1 = extract_p2p_addr(&cluster, 1);

    node0.p2p_connect(&[&addr1]).expect("connect 0→1");
    node0
        .p2p_collection_add(&["Users"])
        .expect("collection add node0");
    node1
        .p2p_collection_add(&["Users"])
        .expect("collection add node1");

    // Bidirectional replicators
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator 0→1");
    node1
        .p2p_replicator_set(&["Users"], &addr0)
        .expect("replicator 1→0");

    // Create doc on node0
    let result = node0
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create user");
    let doc_id = result["add_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Wait for replication to node1
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { Users { _docID } }")
                .unwrap_or_default();
            result["Users"]
                .as_array()
                .map(|arr| !arr.is_empty())
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "initial doc did not replicate",
    )
    .await;

    // Update on node1 (the "parent"/target)
    node1
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 60}}) {{ _docID age }} }}"#,
            doc_id
        ))
        .expect("update age on node1");

    // Verify update syncs back to node0
    let node0_ref = &node0;
    poll_until(
        || {
            let result = node0_ref
                .query("query { Users { age } }")
                .unwrap_or_default();
            result["Users"]
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(|u| u["age"].as_i64())
                .map(|age| age == 60)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "update from node1 did not sync back to node0",
    )
    .await;
}

/// Port: TestP2PWithSingleDocumentUpdatePerNode
/// Both nodes update the same LWW field; they converge to one value.
#[tokio::test]
#[serial]
async fn update_per_node() {
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
    let node1 = cluster.client(1);

    node0.schema_add(SCHEMA).expect("schema node0");
    node1.schema_add(SCHEMA).expect("schema node1");

    let addr0 = extract_p2p_addr(&cluster, 0);
    let addr1 = extract_p2p_addr(&cluster, 1);

    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("rep 0→1");
    node1
        .p2p_replicator_set(&["Users"], &addr0)
        .expect("rep 1→0");

    let result = node0
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create");
    let doc_id = result["add_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Wait for replication
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { _docID } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|a| !a.is_empty())
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "doc did not replicate",
    )
    .await;

    // Both nodes update the same field (LWW — one will win)
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 60}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update node0");
    node1
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 45}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update node1");

    // Wait for convergence — both nodes should agree on either 45 or 60
    tokio::time::sleep(Duration::from_secs(5)).await;

    poll_until(
        || {
            let r0 = node0.query("query { Users { age } }").unwrap_or_default();
            let r1 = node1_ref
                .query("query { Users { age } }")
                .unwrap_or_default();
            let age0 = r0["Users"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|u| u["age"].as_i64())
                .unwrap_or(-1);
            let age1 = r1["Users"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|u| u["age"].as_i64())
                .unwrap_or(-1);
            // LWW convergence: both must agree on the same value
            age0 == age1 && (age0 == 45 || age0 == 60)
        },
        Duration::from_secs(30),
        Duration::from_millis(500),
        "LWW fields did not converge (both nodes should agree on 45 or 60)",
    )
    .await;
}

/// Port: TestP2PWithSingleDocumentSingleUpdateDoesNotSyncToNonPeerNode
/// Update doesn't sync to a node that isn't connected.
#[tokio::test]
#[serial]
async fn update_does_not_sync_to_non_peer() {
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
        cluster
            .client(i)
            .schema_add(SCHEMA)
            .unwrap_or_else(|_| panic!("schema node{}", i));
    }

    let addr1 = extract_p2p_addr(&cluster, 1);

    // Only connect 0→1, node2 is isolated
    cluster
        .client(0)
        .p2p_connect(&[&addr1])
        .expect("connect 0→1");
    cluster
        .client(0)
        .p2p_collection_add(&["Users"])
        .expect("col node0");
    cluster
        .client(1)
        .p2p_collection_add(&["Users"])
        .expect("col node1");
    cluster
        .client(0)
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("rep 0→1");

    // Create doc on node0
    let result = cluster
        .client(0)
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create");
    let doc_id = result["add_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Wait for replication to node1
    let node1 = cluster.client(1);
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { _docID } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|a| !a.is_empty())
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "doc did not replicate to node1",
    )
    .await;

    // Update on node0
    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 60}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update on node0");

    // Verify update reaches node1
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { age } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|u| u["age"].as_i64())
                .map(|age| age == 60)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "update did not reach node1",
    )
    .await;

    // Verify node2 (isolated) has NO docs
    let node2_result = cluster
        .client(2)
        .query("query { Users { _docID age } }")
        .expect("query node2");
    let node2_users = node2_result["Users"].as_array().expect("not array");
    assert!(
        node2_users.is_empty(),
        "isolated node2 should have 0 docs, got {}",
        node2_users.len()
    );
}

/// Port: TestP2PWithSingleDocumentSingleUpdateDoesNotSyncFromUnmappedNode
/// Update on isolated node doesn't propagate to connected nodes.
#[tokio::test]
#[serial]
async fn update_does_not_sync_from_unmapped_node() {
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
        cluster
            .client(i)
            .schema_add(SCHEMA)
            .unwrap_or_else(|_| panic!("schema node{}", i));
    }

    let addr1 = extract_p2p_addr(&cluster, 1);

    // Only 0→1 connected, node2 is isolated
    cluster.client(0).p2p_connect(&[&addr1]).expect("connect");
    cluster
        .client(0)
        .p2p_collection_add(&["Users"])
        .expect("col");
    cluster
        .client(1)
        .p2p_collection_add(&["Users"])
        .expect("col");
    cluster
        .client(0)
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("rep");

    // Create doc on isolated node2
    cluster
        .client(2)
        .query(r#"mutation { add_Users(input: {name: "John", age: 60}) { _docID } }"#)
        .expect("create on node2");

    // Wait and verify nodes 0 and 1 do NOT have the doc
    tokio::time::sleep(Duration::from_secs(5)).await;

    let r0 = cluster
        .client(0)
        .query("query { Users { _docID } }")
        .expect("query node0");
    let r1 = cluster
        .client(1)
        .query("query { Users { _docID } }")
        .expect("query node1");

    assert!(
        r0["Users"].as_array().map(|a| a.is_empty()).unwrap_or(true),
        "node0 should have 0 docs from isolated node2"
    );
    assert!(
        r1["Users"].as_array().map(|a| a.is_empty()).unwrap_or(true),
        "node1 should have 0 docs from isolated node2"
    );

    // Verify node2 still has its doc
    let r2 = cluster
        .client(2)
        .query("query { Users { name age } }")
        .expect("query node2");
    let node2_users = r2["Users"].as_array().expect("not array");
    assert_eq!(node2_users.len(), 1, "node2 should have its local doc");
    assert_eq!(node2_users[0]["age"], 60);
}

/// Port: TestP2PWithMultipleDocumentUpdatesPerNode
/// Multiple sequential updates from both nodes, final values converge.
#[tokio::test]
#[serial]
async fn multiple_updates_per_node() {
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
    let node1 = cluster.client(1);

    node0.schema_add(SCHEMA).expect("schema node0");
    node1.schema_add(SCHEMA).expect("schema node1");

    let addr0 = extract_p2p_addr(&cluster, 0);
    let addr1 = extract_p2p_addr(&cluster, 1);

    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("rep 0→1");
    node1
        .p2p_replicator_set(&["Users"], &addr0)
        .expect("rep 1→0");

    let result = node0
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create");
    let doc_id = result["add_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Wait for replication
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { _docID } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|a| !a.is_empty())
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "doc did not replicate",
    )
    .await;

    // Multiple updates from node0: 21→60→61→62
    for age in [60, 61, 62] {
        node0
            .query(&format!(
                r#"mutation {{ update_Users(docID: "{}", input: {{age: {}}}) {{ _docID }} }}"#,
                doc_id, age
            ))
            .expect("update node0");
    }

    // Multiple updates from node1: 21→45→46→47
    for age in [45, 46, 47] {
        node1
            .query(&format!(
                r#"mutation {{ update_Users(docID: "{}", input: {{age: {}}}) {{ _docID }} }}"#,
                doc_id, age
            ))
            .expect("update node1");
    }

    // Wait for convergence — both should agree (LWW: either 47 or 62)
    poll_until(
        || {
            let r0 = node0.query("query { Users { age } }").unwrap_or_default();
            let r1 = node1_ref
                .query("query { Users { age } }")
                .unwrap_or_default();
            let age0 = r0["Users"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|u| u["age"].as_i64())
                .unwrap_or(-1);
            let age1 = r1["Users"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|u| u["age"].as_i64())
                .unwrap_or(-1);
            age0 == age1 && (age0 == 47 || age0 == 62)
        },
        Duration::from_secs(30),
        Duration::from_millis(500),
        "multiple LWW updates did not converge",
    )
    .await;
}

/// Port: TestP2PWithSingleDocumentSingleUpdateFromChildWithP2PCollection
/// Update replicates via collection-level subscription (create + update in one flow).
#[tokio::test]
#[serial]
async fn single_update_from_child_with_p2p_collection() {
    let (cluster, _, _) = setup_with_replicated_doc().await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Create a second doc and update it
    let result = node0
        .query(r#"mutation { add_Users(input: {name: "Fred", age: 31}) { _docID } }"#)
        .expect("create Fred");
    let fred_id = result["add_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Update Fred's age
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 60}}) {{ _docID }} }}"#,
            fred_id
        ))
        .expect("update Fred");

    // Verify node1 receives Fred with updated age
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            let arr = match result["Users"].as_array() {
                Some(a) => a,
                None => return false,
            };
            // Should have 2 docs: John(21) and Fred(60)
            if arr.len() < 2 {
                return false;
            }
            arr.iter()
                .any(|u| u["name"].as_str() == Some("Fred") && u["age"].as_i64() == Some(60))
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "Fred with updated age did not replicate",
    )
    .await;
}

/// Port: TestP2PWithMultipleDocumentUpdatesPerNodeWithP2PCollection
/// Multiple updates on multiple docs with collection subscription.
#[tokio::test]
#[serial]
async fn multiple_updates_per_node_with_p2p_collection() {
    let (cluster, doc_id, _) = setup_with_replicated_doc().await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Create Fred on node0
    let result = node0
        .query(r#"mutation { add_Users(input: {name: "Fred", age: 31}) { _docID } }"#)
        .expect("create Fred");
    let fred_id = result["add_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Multiple updates on John (doc_id) from node0
    for age in [60, 61, 62] {
        node0
            .query(&format!(
                r#"mutation {{ update_Users(docID: "{}", input: {{age: {}}}) {{ _docID }} }}"#,
                doc_id, age
            ))
            .expect("update John");
    }

    // Update Fred
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 60}}) {{ _docID }} }}"#,
            fred_id
        ))
        .expect("update Fred");

    // Verify node1 receives all updates
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            let arr = match result["Users"].as_array() {
                Some(a) => a,
                None => return false,
            };
            if arr.len() < 2 {
                return false;
            }
            let john_ok = arr
                .iter()
                .any(|u| u["name"].as_str() == Some("John") && u["age"].as_i64() == Some(62));
            let fred_ok = arr
                .iter()
                .any(|u| u["name"].as_str() == Some("Fred") && u["age"].as_i64() == Some(60));
            john_ok && fred_ok
        },
        Duration::from_secs(30),
        Duration::from_millis(300),
        "multiple doc updates did not replicate correctly",
    )
    .await;
}

/// Port: TestP2PWithSingleDocumentSingleUpdateFromChildAndRestart
/// Update survives node restart.
#[tokio::test]
#[serial]
async fn single_update_from_child_and_restart() {
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .with_store("redb")
        .with_keyring()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node0 P2P listener");
    cluster
        .wait_for_log(1, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node1 P2P listener");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(SCHEMA).expect("schema add node0");
    node1.schema_add(SCHEMA).expect("schema add node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0→1");
    node0
        .p2p_collection_add(&["Users"])
        .expect("collection add node0");
    node1
        .p2p_collection_add(&["Users"])
        .expect("collection add node1");
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator 0→1");

    // Create initial doc on node0
    let result = node0
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create user");
    let doc_id = result["add_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Wait for initial replication
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { _docID } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|a| !a.is_empty())
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "initial doc did not replicate before restart",
    )
    .await;

    // Restart both nodes
    cluster
        .restart_node(0, Duration::from_secs(30))
        .await
        .expect("restart node0");
    cluster
        .restart_node(1, Duration::from_secs(30))
        .await
        .expect("restart node1");

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node0 P2P after restart");
    cluster
        .wait_for_log(1, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node1 P2P after restart");

    // Re-establish QUIC connection (ephemeral transport state lost on restart)
    let addr1_after = extract_p2p_addr(&cluster, 1);
    cluster
        .client(0)
        .p2p_connect(&[&addr1_after])
        .expect("reconnect after restart");

    // Update doc on node0 after restart
    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 60}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update after restart");

    // Verify update replicates to node1
    let node1_after = cluster.client(1);
    let node1_after_ref = &node1_after;
    poll_until(
        || {
            let r = node1_after_ref
                .query("query { Users { age } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|u| u["age"].as_i64())
                .map(|age| age == 60)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "update did not replicate after restart (expected age=60)",
    )
    .await;
}
