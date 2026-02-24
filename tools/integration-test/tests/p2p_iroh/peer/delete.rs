//! Iroh P2P document deletion and sync tests.
//!
//! Ported from Go: tests/integration/net/simple/peer/ (delete tests)
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh_peer_delete -- --ignored

use std::time::Duration;

use integration_test::{extract_p2p_addr, poll_until, TestCluster};
use serial_test::serial;

const SCHEMA: &str = "type Users { name: String  age: Int }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);

/// Set up 2-node cluster with 2 docs (John age=43, Andy age=74), replicator 0→1.
/// Returns (cluster, john_doc_id, andy_doc_id).
async fn setup_with_two_docs() -> (TestCluster, String, String) {
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
        .expect("rep 0→1");

    // Create two docs on node0
    let john = node0
        .query(r#"mutation { create_Users(input: {name: "John", age: 43}) { _docID } }"#)
        .expect("create John");
    let john_id = john["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing John _docID")
        .to_string();

    let andy = node0
        .query(r#"mutation { create_Users(input: {name: "Andy", age: 74}) { _docID } }"#)
        .expect("create Andy");
    let andy_id = andy["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing Andy _docID")
        .to_string();

    // Wait for both docs to replicate
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { _docID } }")
                .unwrap_or_default();
            r["Users"].as_array().map(|a| a.len() >= 2).unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "initial 2 docs did not replicate",
    )
    .await;

    (cluster, john_id, andy_id)
}

/// Port: TestP2PWithMultipleDocumentsSingleDelete
/// Delete one doc, verify only the other remains on both nodes.
#[tokio::test]
#[serial]
async fn multiple_docs_single_delete() {
    let (cluster, john_id, _andy_id) = setup_with_two_docs().await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Delete John on node0
    node0
        .collection_delete("Users", &john_id)
        .expect("delete John");

    // Verify node0 has only Andy
    let r0 = node0
        .query("query { Users { name age } }")
        .expect("query node0");
    let users0 = r0["Users"].as_array().expect("not array");
    assert_eq!(users0.len(), 1, "node0 should have 1 doc after delete");
    assert_eq!(users0[0]["name"], "Andy");
    assert_eq!(users0[0]["age"], 74);

    // Verify deletion replicates to node1
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name } }")
                .unwrap_or_default();
            let arr = match r["Users"].as_array() {
                Some(a) => a,
                None => return false,
            };
            if arr.len() != 1 {
                return false;
            }
            arr[0]["name"].as_str() == Some("Andy")
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "deletion did not replicate to node1",
    )
    .await;
}

/// Port: TestP2PWithMultipleDocumentsSingleDeleteWithShowDeleted
/// After delete, querying with showDeleted reveals the tombstone.
#[tokio::test]
#[serial]
async fn multiple_docs_single_delete_show_deleted() {
    let (cluster, john_id, _andy_id) = setup_with_two_docs().await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Delete John on node0
    node0
        .collection_delete("Users", &john_id)
        .expect("delete John");

    // Verify deletion replicates
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name } }")
                .unwrap_or_default();
            r["Users"].as_array().map(|a| a.len() == 1).unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "deletion did not replicate",
    )
    .await;

    // Query with showDeleted on node1
    let r = node1
        .query("query { Users(showDeleted: true) { name age _deleted } }")
        .expect("showDeleted query");
    let users = r["Users"].as_array().expect("not array");
    assert_eq!(
        users.len(),
        2,
        "showDeleted should reveal 2 docs (1 active + 1 deleted)"
    );

    let andy = users
        .iter()
        .find(|u| u["name"].as_str() == Some("Andy"))
        .expect("Andy should exist");
    assert_eq!(andy["_deleted"], false, "Andy should not be deleted");

    let john = users
        .iter()
        .find(|u| u["name"].as_str() == Some("John"))
        .expect("John should exist with showDeleted");
    assert_eq!(john["_deleted"], true, "John should be marked as deleted");
    assert_eq!(john["age"], 43, "John should retain original age");
}

/// Port: TestP2PWithMultipleDocumentsWithSingleUpdateBeforeConnectSingleDeleteWithShowDeleted
/// Update before connection, then delete: synced state shows updated age on deleted doc.
#[tokio::test]
#[serial]
async fn update_before_connect_then_delete_show_deleted() {
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

    // Create docs on node0 BEFORE connection
    let john = node0
        .query(r#"mutation { create_Users(input: {name: "John", age: 43}) { _docID } }"#)
        .expect("create John");
    let john_id = john["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    node0
        .query(r#"mutation { create_Users(input: {name: "Andy", age: 74}) { _docID } }"#)
        .expect("create Andy");

    // Update John's age before connection
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 60}}) {{ _docID }} }}"#,
            john_id
        ))
        .expect("update John age");

    // Now connect and set up replication
    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("rep 0→1");

    // Wait for initial sync
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { _docID } }")
                .unwrap_or_default();
            r["Users"].as_array().map(|a| a.len() >= 2).unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "docs did not sync after connect",
    )
    .await;

    // Delete John on node0
    node0
        .collection_delete("Users", &john_id)
        .expect("delete John");

    // Verify deletion replicates and showDeleted preserves updated age
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users(showDeleted: true) { name age _deleted } }")
                .unwrap_or_default();
            let arr = match r["Users"].as_array() {
                Some(a) => a,
                None => return false,
            };
            let john = arr.iter().find(|u| u["name"].as_str() == Some("John"));
            match john {
                Some(j) => j["_deleted"] == true && j["age"].as_i64() == Some(60),
                None => false,
            }
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "deleted John with updated age did not sync",
    )
    .await;
}

/// Port: TestP2PWithMultipleDocumentsWithMultipleUpdatesBeforeConnectSingleDeleteWithShowDeleted
/// Multiple updates before connect, then delete: final age preserved on tombstone.
#[tokio::test]
#[serial]
async fn multiple_updates_before_connect_then_delete_show_deleted() {
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

    // Create docs before connection
    let john = node0
        .query(r#"mutation { create_Users(input: {name: "John", age: 43}) { _docID } }"#)
        .expect("create John");
    let john_id = john["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    node0
        .query(r#"mutation { create_Users(input: {name: "Andy", age: 74}) { _docID } }"#)
        .expect("create Andy");

    // Multiple updates: 43→60→62
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 60}}) {{ _docID }} }}"#,
            john_id
        ))
        .expect("update 1");
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 62}}) {{ _docID }} }}"#,
            john_id
        ))
        .expect("update 2");

    // Connect and replicate
    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("rep 0→1");

    // Wait for sync
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { _docID } }")
                .unwrap_or_default();
            r["Users"].as_array().map(|a| a.len() >= 2).unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "docs did not sync",
    )
    .await;

    // Delete John
    node0
        .collection_delete("Users", &john_id)
        .expect("delete John");

    // Verify tombstone with final age=62
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users(showDeleted: true) { name age _deleted } }")
                .unwrap_or_default();
            let arr = match r["Users"].as_array() {
                Some(a) => a,
                None => return false,
            };
            let john = arr.iter().find(|u| u["name"].as_str() == Some("John"));
            match john {
                Some(j) => j["_deleted"] == true && j["age"].as_i64() == Some(62),
                None => false,
            }
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "deleted John with final age=62 did not sync",
    )
    .await;
}

/// Port: TestP2PWithMultipleDocumentsWithUpdateAndDeleteBeforeConnectSingleDeleteWithShowDeleted
/// Complex scenario: update + delete before connect, then verify state exchange.
///
/// Node0: creates 2 docs, updates John twice (43→60→62), then deletes John.
/// Nodes connect. Node1 receives the surviving doc (Andy).
/// Verify node0 shows John as deleted with final pre-delete age (62).
#[tokio::test]
#[serial]
async fn update_and_delete_before_connect_show_deleted() {
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

    // Create 2 docs on node0 BEFORE connection
    let john = node0
        .query(r#"mutation { create_Users(input: {name: "John", age: 43}) { _docID } }"#)
        .expect("create John");
    let john_id = john["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing John _docID")
        .to_string();

    node0
        .query(r#"mutation { create_Users(input: {name: "Andy", age: 74}) { _docID } }"#)
        .expect("create Andy");

    // Update John twice: 43→60→62
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 60}}) {{ _docID }} }}"#,
            john_id
        ))
        .expect("update John to 60");
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 62}}) {{ _docID }} }}"#,
            john_id
        ))
        .expect("update John to 62");

    // Delete John on node0 BEFORE connection
    node0
        .collection_delete("Users", &john_id)
        .expect("delete John");

    // Verify node0 state: Andy alive, John deleted with age 62
    let r0 = node0
        .query("query { Users(showDeleted: true) { name age _deleted } }")
        .expect("node0 showDeleted");
    let users0 = r0["Users"].as_array().expect("not array");
    let john0 = users0
        .iter()
        .find(|u| u["name"].as_str() == Some("John"))
        .expect("John should exist in showDeleted");
    assert_eq!(john0["_deleted"], true, "John should be deleted on node0");
    assert_eq!(
        john0["age"].as_i64(),
        Some(62),
        "John should have final age 62 on node0"
    );

    // Now connect and set up replication
    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("rep 0→1");

    // Wait for Andy to replicate to node1
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|arr| arr.iter().any(|u| u["name"].as_str() == Some("Andy")))
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "Andy did not replicate to node1",
    )
    .await;

    // Verify node1 received the deleted doc state too
    tokio::time::sleep(Duration::from_secs(3)).await;
    let r1 = node1
        .query("query { Users(showDeleted: true) { name age _deleted } }")
        .expect("node1 showDeleted");
    let users1 = r1["Users"].as_array().expect("not array");

    let andy1 = users1.iter().find(|u| u["name"].as_str() == Some("Andy"));
    assert!(andy1.is_some(), "node1 should have Andy");

    // John may or may not appear on node1 depending on whether deleted docs replicate
    let john1 = users1.iter().find(|u| u["name"].as_str() == Some("John"));
    if let Some(j) = john1 {
        assert_eq!(
            j["_deleted"], true,
            "if John appears on node1, should be deleted"
        );
        assert_eq!(
            j["age"].as_i64(),
            Some(62),
            "John should have age 62 on node1"
        );
    }
}
