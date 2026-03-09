//! Iroh P2P replicator tests.
//!
//! Ported from Go: tests/integration/net/simple/peer_replicator/
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh_replicator -- --ignored

use std::time::Duration;

use integration_test::{extract_p2p_addr, poll_until, TestCluster};
use serial_test::serial;

const SCHEMA: &str = "type Users { name: String  age: Int }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);

/// Set up 2 iroh nodes with schema, connected, with replicator 0→1.
async fn setup_replicator_cluster() -> (TestCluster, String) {
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

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator set");

    (cluster, addr1)
}

/// Port: TestP2PPeerReplicatorWithCreate
/// Replicator pushes created documents to target.
#[tokio::test]
#[serial]
async fn replicator_with_create() {
    let (cluster, _) = setup_replicator_cluster().await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Create doc on node0
    let result = node0
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID name age } }"#)
        .expect("create user");
    let doc_id = result["add_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID");

    // Verify replication
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { _docID name age } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|arr| {
                    arr.iter().any(|u| {
                        u["_docID"].as_str() == Some(doc_id)
                            && u["name"].as_str() == Some("John")
                            && u["age"].as_i64() == Some(21)
                    })
                })
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "created doc did not replicate via replicator",
    )
    .await;
}

/// Port: TestP2PPeerReplicatorWithUpdate
/// Replicator pushes updates to target.
#[tokio::test]
#[serial]
async fn replicator_with_update() {
    let (cluster, _) = setup_replicator_cluster().await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let result = node0
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create");
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
        "initial doc did not replicate",
    )
    .await;

    // Update doc
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 60}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update");

    // Verify update replicates
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name age } }")
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
        "update did not replicate via replicator",
    )
    .await;
}

/// Port: TestP2PPeerReplicatorWithDeleteShowDeleted
/// Replicator pushes deletions; tombstone visible with showDeleted.
#[tokio::test]
#[serial]
async fn replicator_with_delete_show_deleted() {
    let (cluster, _) = setup_replicator_cluster().await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

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

    // Delete on node0
    node0
        .collection_delete("Users", &doc_id)
        .expect("delete John");

    // Verify deletion replicates (normal query returns empty)
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { _docID } }")
                .unwrap_or_default();
            r["Users"].as_array().map(|a| a.is_empty()).unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "deletion did not replicate",
    )
    .await;

    // showDeleted reveals tombstone
    let r = node1
        .query("query { Users(showDeleted: true) { name _deleted } }")
        .expect("showDeleted query");
    let users = r["Users"].as_array().expect("not array");
    assert_eq!(users.len(), 1, "showDeleted should show 1 tombstone");
    assert_eq!(users[0]["name"], "John");
    assert_eq!(users[0]["_deleted"], true);
}

/// Port: TestP2PPeerReplicatorWithCreate_PCounter_NoError
/// Replicator handles PCounter CRDT creates.
#[tokio::test]
#[serial]
async fn replicator_create_pcounter() {
    let pcounter_schema = "type Users { name: String  points: Int @crdt(type: pcounter) }";

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

    node0.schema_add(pcounter_schema).expect("schema node0");
    node1.schema_add(pcounter_schema).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0.p2p_replicator_set(&["Users"], &addr1).expect("rep");

    // Create with PCounter initial value
    node0
        .query(r#"mutation { add_Users(input: {name: "Shahzad", points: 10}) { _docID } }"#)
        .expect("create");

    // Verify replication preserves PCounter value
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name points } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .and_then(|a| a.first())
                .map(|u| u["name"].as_str() == Some("Shahzad") && u["points"].as_i64() == Some(10))
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "PCounter doc did not replicate",
    )
    .await;
}

/// Port: TestP2PPeerReplicatorWithUpdate_PCounter_NoError
/// Replicator handles PCounter CRDT updates (accumulation).
#[tokio::test]
#[serial]
async fn replicator_update_pcounter() {
    let pcounter_schema = "type Users { name: String  points: Int @crdt(type: pcounter) }";

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

    node0.schema_add(pcounter_schema).expect("schema node0");
    node1.schema_add(pcounter_schema).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0.p2p_replicator_set(&["Users"], &addr1).expect("rep");

    let result = node0
        .query(r#"mutation { add_Users(input: {name: "Shahzad", points: 10}) { _docID } }"#)
        .expect("create");
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
        "initial doc did not replicate",
    )
    .await;

    // Update PCounter (+10, total should be 20)
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{points: 10}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update points");

    // Verify accumulated value replicates
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { points } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|u| u["points"].as_i64())
                .map(|p| p == 20)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "PCounter update did not replicate (expected 20)",
    )
    .await;
}

/// Port: TestP2PPeerReplicatorWithCreate_PNCounter_NoError
/// Replicator handles PNCounter CRDT creates.
#[tokio::test]
#[serial]
async fn replicator_create_pncounter() {
    let pncounter_schema = "type Users { name: String  points: Int @crdt(type: pncounter) }";

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

    node0.schema_add(pncounter_schema).expect("schema node0");
    node1.schema_add(pncounter_schema).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0.p2p_replicator_set(&["Users"], &addr1).expect("rep");

    node0
        .query(r#"mutation { add_Users(input: {name: "Shahzad", points: 10}) { _docID } }"#)
        .expect("create");

    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name points } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .and_then(|a| a.first())
                .map(|u| u["name"].as_str() == Some("Shahzad") && u["points"].as_i64() == Some(10))
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "PNCounter doc did not replicate",
    )
    .await;
}

/// Port: TestP2PPeerReplicatorWithUpdate_PNCounter_NoError
/// Replicator handles PNCounter CRDT updates (accumulation).
#[tokio::test]
#[serial]
async fn replicator_update_pncounter() {
    let pncounter_schema = "type Users { name: String  points: Int @crdt(type: pncounter) }";

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

    node0.schema_add(pncounter_schema).expect("schema node0");
    node1.schema_add(pncounter_schema).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0.p2p_replicator_set(&["Users"], &addr1).expect("rep");

    let result = node0
        .query(r#"mutation { add_Users(input: {name: "Shahzad", points: 10}) { _docID } }"#)
        .expect("create");
    let doc_id = result["add_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

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
        "initial doc did not replicate",
    )
    .await;

    // Update PNCounter
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{points: 10}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update points");

    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { points } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|u| u["points"].as_i64())
                .map(|p| p == 20)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "PNCounter update did not replicate (expected 20)",
    )
    .await;
}

/// Port: TestP2PPeerReplicatorWithUpdateAndRestart
/// Replicator survives node restart and continues replicating.
#[tokio::test]
#[serial]
async fn replicator_with_update_and_restart() {
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
        .expect("node0");
    cluster
        .wait_for_log(1, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node1");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(SCHEMA).expect("schema node0");
    node1.schema_add(SCHEMA).expect("schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator set");

    // Create doc and verify initial replication
    let result = node0
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create");
    let doc_id = result["add_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

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

    // Restart both nodes — replicator config should persist in peerstore
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

    // Re-establish QUIC connection (ephemeral transport state is lost on restart)
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
