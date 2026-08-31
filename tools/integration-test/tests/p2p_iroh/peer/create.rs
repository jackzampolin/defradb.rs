//! Iroh P2P document creation and sync tests.
//!
//! Ported from Go: tests/integration/net/simple/peer/ (create tests)
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh_peer_create -- --ignored

use std::time::Duration;

use integration_test::{poll_until, TestCluster};
use serial_test::serial;

const SCHEMA: &str = "type Users { name: String  age: Int }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);

/// Port: TestP2PCreateDoesNotSync
/// Without collection subscription or replicator, new docs do NOT sync.
#[tokio::test]
#[serial]
async fn create_does_not_sync() {
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

    // Connect peers but do NOT set up collection subscription or replicator
    let info1 = node1.p2p_info().expect("p2p_info node1");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");
    node0.p2p_connect(&[addr1]).expect("connect peers");

    // Create doc on node0 only
    node0
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create user on node0");

    // Wait a reasonable time, then verify node1 does NOT have the doc
    tokio::time::sleep(Duration::from_secs(5)).await;

    let result = node1
        .query("query { Users { _docID } }")
        .expect("query node1");
    let users = result["Users"]
        .as_array()
        .expect("Users should be an array");
    assert!(
        users.is_empty(),
        "without collection subscription, docs should NOT sync to peers, got {} docs",
        users.len()
    );
}

/// Port: TestP2PCreateWithP2PCollection
/// When both nodes subscribe, docs gossip symmetrically even if the explicit
/// replicator configuration has only one outbound leg.
#[tokio::test]
#[serial]
async fn create_with_p2p_collection() {
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

    let info1 = node1.p2p_info().expect("p2p_info node1");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address")
        .to_string();

    node0.p2p_connect(&[&addr1]).expect("connect peers");
    node0
        .p2p_collection_add(&["Users"])
        .expect("collection add node0");
    node1
        .p2p_collection_add(&["Users"])
        .expect("collection add node1");
    // Replicator: node0 explicitly pushes to node1. The shared subscription
    // still expresses receive intent in both directions.
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator set 0→1");

    // Create docs on node0
    node0
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create John on node0");
    node0
        .query(r#"mutation { add_Users(input: {name: "Addo", age: 28}) { _docID } }"#)
        .expect("create Addo on node0");

    // Create doc on node1; the subscription should carry it back to node0.
    node1
        .query(r#"mutation { add_Users(input: {name: "Fred", age: 31}) { _docID } }"#)
        .expect("create Fred on node1");

    // Verify node1 receives node0's docs
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            result["Users"]
                .as_array()
                .map(|arr| arr.len() >= 3)
                .unwrap_or(false)
        },
        Duration::from_secs(30),
        Duration::from_millis(300),
        "node1 should have 3 docs (2 from node0 + 1 local)",
    )
    .await;

    let node1_result = node1
        .query("query { Users { name age } }")
        .expect("query node1");
    let node1_users = node1_result["Users"].as_array().expect("not array");
    let node1_names: Vec<&str> = node1_users
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert!(
        node1_names.contains(&"John"),
        "node1 should have John from node0"
    );
    assert!(
        node1_names.contains(&"Addo"),
        "node1 should have Addo from node0"
    );
    assert!(
        node1_names.contains(&"Fred"),
        "node1 should have locally-created Fred"
    );

    // Verify node0 accepts Fred from its outbound replicator target because it
    // is locally subscribed to the collection.
    let node0_ref = &node0;
    poll_until(
        || {
            let result = node0_ref
                .query("query { Users { name } }")
                .unwrap_or_default();
            result["Users"]
                .as_array()
                .is_some_and(|users| users.iter().any(|user| user["name"] == "Fred"))
        },
        Duration::from_secs(30),
        Duration::from_millis(300),
        "node0 should accept Fred over its subscribed collection topic",
    )
    .await;

    let node0_result = node0
        .query("query { Users { name } }")
        .expect("query node0");
    let node0_names: Vec<&str> = node0_result["Users"]
        .as_array()
        .expect("not array")
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert!(
        node0_names.contains(&"Fred"),
        "node0 should have Fred from its subscribed collection, got {:?}",
        node0_names
    );
}

/// A three-node Iroh chain must converge through a provider that owns the
/// complete linked DAG. A relayed root-only hint may not become C's durable
/// recovery source; after B merges, its normal B→C head hint is serviceable.
#[tokio::test]
#[serial]
async fn create_propagates_to_last_node_in_chain() {
    // 3-node chain: 0→1→2
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
            .unwrap_or_else(|_| panic!("node{} P2P listener did not start", i));
        cluster
            .client(i)
            .schema_add(SCHEMA)
            .unwrap_or_else(|_| panic!("add schema node{}", i));
    }

    // All nodes subscribe to the collection (needed for relay)
    for i in 0..3 {
        cluster
            .client(i)
            .p2p_collection_add(&["Users"])
            .unwrap_or_else(|_| panic!("collection add node{}", i));
    }

    let addr1 = integration_test::extract_p2p_addr(&cluster, 1);
    let addr2 = integration_test::extract_p2p_addr(&cluster, 2);

    // Connect chain: 0→1→2
    cluster
        .client(0)
        .p2p_connect(&[&addr1])
        .expect("connect 0→1");
    cluster
        .client(1)
        .p2p_connect(&[&addr2])
        .expect("connect 1→2");

    // Set up replicators along the chain
    cluster
        .client(0)
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator 0→1");
    cluster
        .client(1)
        .p2p_replicator_set(&["Users"], &addr2)
        .expect("replicator 1→2");

    // Create doc on node0
    cluster
        .client(0)
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create user on node0");

    // Verify doc reaches node1.
    let node1 = cluster.client(1);
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            result["Users"]
                .as_array()
                .map(|arr| {
                    arr.iter().any(|u| {
                        u["name"].as_str() == Some("John") && u["age"].as_i64() == Some(21)
                    })
                })
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "doc did not reach node1 (first hop)",
    )
    .await;

    // B owns the full DAG only after its merge. Its B→C announcement must
    // complete C without promoting A's root-only relayed hint into ownership.
    let node2 = cluster.client(2);
    poll_until(
        || {
            let result = node2
                .query("query { Users { name age } }")
                .unwrap_or_default();
            result["Users"]
                .as_array()
                .map(|arr| {
                    arr.iter().any(|u| {
                        u["name"].as_str() == Some("John") && u["age"].as_i64() == Some(21)
                    })
                })
                .unwrap_or(false)
        },
        Duration::from_secs(30),
        Duration::from_millis(300),
        "doc did not reach node2 through the complete B provider",
    )
    .await;

    let status: serde_json::Value =
        reqwest::get(format!("{}/api/v0/p2p/sync/status", cluster.api_url(2)))
            .await
            .expect("node2 sync status request")
            .json()
            .await
            .expect("node2 sync status json");
    assert_eq!(status["pending_dags"].as_u64(), Some(0));
    assert_eq!(status["persisted_pending_dags"].as_u64(), Some(0));
    assert_eq!(status["pending_dag_fetch_exhausted"].as_u64(), Some(0));
    assert_eq!(status["provider_rotations"].as_u64(), Some(0));
}

/// One serviceable origin must drain a head-hint fan-out larger than the
/// provider's fixed CAR worker reserve. Excess first-wave requests may be
/// nacked, but every receiver keeps its durable obligation and re-enters the
/// same paced recovery path until merge and terminal cleanup complete.
#[tokio::test]
#[serial]
async fn head_hint_fanout_above_car_worker_bound_quiesces() {
    const NODE_COUNT: usize = 10;

    let cluster = TestCluster::builder()
        .rust_nodes(NODE_COUNT)
        .with_iroh_transport()
        .build()
        .await
        .expect("fan-out cluster");

    for node in 0..NODE_COUNT {
        cluster
            .wait_for_log(node, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{node} P2P listener did not start"));
        cluster
            .client(node)
            .schema_add(SCHEMA)
            .unwrap_or_else(|_| panic!("add schema node{node}"));
    }

    let source = cluster.client(0);
    for receiver in 1..NODE_COUNT {
        let address = integration_test::extract_p2p_addr(&cluster, receiver);
        source
            .p2p_connect(&[&address])
            .unwrap_or_else(|_| panic!("connect source to node{receiver}"));
        source
            .p2p_replicator_set(&["Users"], &address)
            .unwrap_or_else(|_| panic!("set node{receiver} as replicator"));
    }

    source
        .query(r#"mutation { add_Users(input: {name: "fanout", age: 21}) { _docID } }"#)
        .expect("create fan-out document");

    poll_until(
        || {
            (1..NODE_COUNT).all(|node| {
                cluster
                    .client(node)
                    .query("query { Users { name age } }")
                    .ok()
                    .and_then(|result| result["Users"].as_array().cloned())
                    .is_some_and(|users| {
                        users.iter().any(|user| {
                            user["name"].as_str() == Some("fanout")
                                && user["age"].as_i64() == Some(21)
                        })
                    })
            })
        },
        Duration::from_secs(45),
        Duration::from_millis(300),
        "head-hint fan-out did not converge",
    )
    .await;

    for receiver in 1..NODE_COUNT {
        let status: serde_json::Value = reqwest::get(format!(
            "{}/api/v0/p2p/sync/status",
            cluster.api_url(receiver)
        ))
        .await
        .unwrap_or_else(|_| panic!("node{receiver} sync status request"))
        .json()
        .await
        .unwrap_or_else(|_| panic!("node{receiver} sync status json"));
        assert_eq!(
            status["pending_dags"].as_u64(),
            Some(0),
            "node{receiver} retained a live receiver obligation: {status}"
        );
        assert_eq!(
            status["persisted_pending_dags"].as_u64(),
            Some(0),
            "node{receiver} retained a durable receiver obligation: {status}"
        );
        assert_eq!(
            status["pending_dag_fetch_exhausted"].as_u64(),
            Some(0),
            "node{receiver} exhausted recovery: {status}"
        );
    }
}

/// Port: TestP2PCreate_WithP2PCollectionAndSubscription_ShouldSucceed
/// Both collection subscription and document subscription work together.
/// Node0 creates a doc, and node1 (with collection subscription + GraphQL subscription)
/// receives the update via SSE.
#[tokio::test]
#[serial]
async fn create_with_collection_and_subscription() {
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

    // Connect peers
    let info1 = node1.p2p_info().expect("p2p_info node1");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address")
        .to_string();
    node0.p2p_connect(&[&addr1]).expect("connect peers");

    // Both nodes subscribe to collection
    node0
        .p2p_collection_add(&["Users"])
        .expect("collection add node0");
    node1
        .p2p_collection_add(&["Users"])
        .expect("collection add node1");

    // Set up replicator: node0 pushes to node1
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator set 0→1");

    // Open GraphQL subscription on node1 for Users updates
    let api_url1 = cluster.api_url(1).to_string();
    let sub_url = format!("{}/api/v0/graphql", api_url1);
    let sub_body = serde_json::json!({ "query": "subscription { Users { _docID name age } }" });
    let sub_events: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sub_events_clone = sub_events.clone();

    let sub_handle = tokio::spawn(async move {
        let client = reqwest::Client::new();
        let resp = client
            .post(&sub_url)
            .header("Accept", "text/event-stream")
            .json(&sub_body)
            .send()
            .await
            .expect("SSE subscription request failed");

        let mut buf = String::new();
        let mut stream = resp.bytes_stream();
        use futures::StreamExt;
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                Err(_) => break,
            };
            buf.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(pos) = buf.find("\n\n") {
                let block = buf[..pos].to_string();
                buf = buf[pos + 2..].to_string();

                let mut event_type = String::new();
                let mut data = String::new();
                for line in block.lines() {
                    if let Some(rest) = line.strip_prefix("event:") {
                        event_type = rest.trim().to_string();
                    } else if let Some(rest) = line.strip_prefix("data:") {
                        data = rest.trim().to_string();
                    }
                }
                if event_type == "next" {
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(&data) {
                        sub_events_clone.lock().unwrap().push(val);
                    }
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create doc on node0
    let result = node0
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create user on node0");
    let doc_id = result["add_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Wait for doc to replicate to node1
    let node1_ref = &node1;
    poll_until(
        || {
            let result = node1_ref
                .query("query { Users { _docID name } }")
                .unwrap_or_default();
            result["Users"]
                .as_array()
                .map(|arr| arr.iter().any(|u| u["_docID"].as_str() == Some(&doc_id)))
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "doc did not replicate to node1",
    )
    .await;

    // Wait for subscription event to arrive on node1
    tokio::time::sleep(Duration::from_secs(2)).await;
    sub_handle.abort();

    let collected = sub_events.lock().unwrap();
    assert!(
        !collected.is_empty(),
        "expected at least 1 subscription event for replicated doc on node1, got 0"
    );

    // Verify the subscription event contains the replicated doc
    let has_john = collected.iter().any(|e| {
        e.pointer("/data/Users/0/name")
            .or_else(|| e.pointer("/Users/0/name"))
            .and_then(|v| v.as_str())
            == Some("John")
    });
    assert!(
        has_john,
        "subscription event should contain John, got: {:?}",
        *collected
    );
}
