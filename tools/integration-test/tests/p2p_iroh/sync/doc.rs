//! Iroh P2P document sync tests.
//!
//! Ported from Go: tests/integration/net/sync/ (document sync tests)
//!
//! Document sync is a pull-based, explicit, one-time operation.
//! Unlike replicator (continuous push), doc sync explicitly requests
//! specific documents from connected peers and does NOT establish
//! ongoing subscriptions.
//!
//! Run with:
//!   cargo test --test p2p_iroh -- sync::doc::

use std::time::Duration;

use integration_test::{extract_p2p_addr, poll_until, TestCluster};
use serial_test::serial;

const SCHEMA: &str = "type Users { name: String  age: Int }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);

/// Port: TestDocSync_WithDocsAvailableOnSingleNode_ShouldSync
/// Single node has docs, sync pulls them to peer.
#[tokio::test]
#[serial]
async fn docs_on_single_node() {
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

    // Create 2 docs on node0
    let r1 = node0
        .query(r#"mutation { create_Users(input: {name: "John", age: 30}) { _docID } }"#)
        .expect("create John");
    let doc1 = r1["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    let r2 = node0
        .query(r#"mutation { create_Users(input: {name: "Islam", age: 25}) { _docID } }"#)
        .expect("create Islam");
    let doc2 = r2["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Connect peers
    let addr1 = extract_p2p_addr(&cluster, 1);
    let addr0 = extract_p2p_addr(&cluster, 0);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node1.p2p_connect(&[&addr0]).expect("connect 1->0");

    // Node1 explicitly syncs docs
    node1
        .p2p_document_sync("Users", &[&doc1, &doc2])
        .expect("p2p_document_sync");

    // Wait for sync
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|arr| arr.len() >= 2)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "doc sync did not pull both docs",
    )
    .await;

    let result = node1
        .query("query { Users { name age } }")
        .expect("query node1");
    let users = result["Users"].as_array().expect("not array");
    let names: Vec<&str> = users.iter().filter_map(|u| u["name"].as_str()).collect();
    assert!(names.contains(&"John"), "node1 should have John");
    assert!(names.contains(&"Islam"), "node1 should have Islam");
}

/// Port: TestDocSync_WithDocsAvailableOnMultipleNode_ShouldSync
/// Multiple nodes have different docs, sync merges them on a third node.
#[tokio::test]
#[serial]
async fn docs_on_multiple_nodes() {
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
            .unwrap_or_else(|_| panic!("node{} P2P listener", i));
        cluster
            .client(i)
            .schema_add(SCHEMA)
            .unwrap_or_else(|_| panic!("schema node{}", i));
    }

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let node2 = cluster.client(2);

    // Doc on node0
    let r1 = node0
        .query(r#"mutation { create_Users(input: {name: "John", age: 30}) { _docID } }"#)
        .expect("create John");
    let doc1 = r1["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Doc on node1
    let r2 = node1
        .query(r#"mutation { create_Users(input: {name: "Islam", age: 25}) { _docID } }"#)
        .expect("create Islam");
    let doc2 = r2["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Connect node2 to both
    let addr0 = extract_p2p_addr(&cluster, 0);
    let addr1 = extract_p2p_addr(&cluster, 1);
    let addr2 = extract_p2p_addr(&cluster, 2);
    node2.p2p_connect(&[&addr0]).expect("connect 2->0");
    node2.p2p_connect(&[&addr1]).expect("connect 2->1");
    node0.p2p_connect(&[&addr2]).expect("connect 0->2");
    node1.p2p_connect(&[&addr2]).expect("connect 1->2");

    // Node2 syncs both docs
    node2
        .p2p_document_sync("Users", &[&doc1, &doc2])
        .expect("p2p_document_sync");

    let node2_ref = &node2;
    poll_until(
        || {
            let r = node2_ref
                .query("query { Users { name } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|arr| arr.len() >= 2)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "doc sync did not merge docs from both nodes",
    )
    .await;

    let result = node2
        .query("query { Users { name } }")
        .expect("query node2");
    let names: Vec<&str> = result["Users"]
        .as_array()
        .expect("not array")
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert!(names.contains(&"John"), "node2 should have John from node0");
    assert!(
        names.contains(&"Islam"),
        "node2 should have Islam from node1"
    );
}

/// Port: TestDocSync_WithSingleDocAvailableOnMultipleNode_ShouldSync
/// Same doc on multiple nodes, sync converges to single document.
#[tokio::test]
#[serial]
async fn single_doc_on_multiple_nodes() {
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

    // Create doc on node0
    let r1 = node0
        .query(r#"mutation { create_Users(input: {name: "John", age: 30}) { _docID } }"#)
        .expect("create John");
    let doc_id = r1["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Connect
    let addr0 = extract_p2p_addr(&cluster, 0);
    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node1.p2p_connect(&[&addr0]).expect("connect 1->0");

    // Node1 syncs the doc
    node1
        .p2p_document_sync("Users", &[&doc_id])
        .expect("p2p_document_sync");

    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|arr| arr.len() == 1)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "doc sync did not pull single doc",
    )
    .await;

    // Should have exactly 1 doc
    let result = node1
        .query("query { Users { name age } }")
        .expect("query node1");
    let users = result["Users"].as_array().expect("not array");
    assert_eq!(users.len(), 1, "should have exactly 1 doc");
    assert_eq!(users[0]["name"].as_str(), Some("John"));
    assert_eq!(users[0]["age"].as_i64(), Some(30));
}

/// Port: TestDocSync_WithDifferentVersionsOnPeers_ShouldSyncLatest
/// Different versions on peers -- sync resolves to latest via CRDT merge.
#[tokio::test]
#[serial]
async fn different_versions_sync_latest() {
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

    // Create doc on node0
    let r1 = node0
        .query(r#"mutation { create_Users(input: {name: "John", age: 20}) { _docID } }"#)
        .expect("create John");
    let doc_id = r1["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Connect and do initial sync so both nodes have the doc
    let addr0 = extract_p2p_addr(&cluster, 0);
    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node1.p2p_connect(&[&addr0]).expect("connect 1->0");

    node1
        .p2p_document_sync("Users", &[&doc_id])
        .expect("p2p_document_sync");

    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { _docID } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|arr| !arr.is_empty())
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "initial sync failed",
    )
    .await;

    // Update age on node0 multiple times
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 25}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update age to 25");
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 30}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update age to 30");

    // Node1 syncs again to get latest
    node1
        .p2p_document_sync("Users", &[&doc_id])
        .expect("sync latest");

    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(|u| u["age"].as_i64())
                .map(|a| a == 30)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "sync did not converge to latest age (30)",
    )
    .await;
}

/// Port: TestDocSync_AfterSync_ShouldNotSubscribeToDocUpdates
/// After explicit sync, node should NOT auto-subscribe to doc updates.
#[tokio::test]
#[serial]
async fn after_sync_no_auto_subscribe() {
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

    // Create doc on node0
    let r1 = node0
        .query(r#"mutation { create_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create John");
    let doc_id = r1["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Connect
    let addr0 = extract_p2p_addr(&cluster, 0);
    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0->1");
    node1.p2p_connect(&[&addr0]).expect("connect 1->0");

    // Node1 syncs the doc
    node1
        .p2p_document_sync("Users", &[&doc_id])
        .expect("p2p_document_sync");

    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(|u| u["age"].as_i64())
                .map(|a| a == 21)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "initial sync failed",
    )
    .await;

    // Update doc on node0 (age 21 -> 22)
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 22}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update age to 22");

    // Wait a bit -- node1 should NOT receive the update automatically
    tokio::time::sleep(Duration::from_secs(5)).await;

    let result = node1
        .query("query { Users { name age } }")
        .expect("query node1");
    let age = result["Users"]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|u| u["age"].as_i64())
        .expect("missing age");
    assert_eq!(
        age, 21,
        "after doc sync, node1 should NOT auto-subscribe; age should still be 21, got {}",
        age
    );
}
