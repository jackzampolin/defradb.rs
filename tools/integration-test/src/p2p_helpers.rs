//! Shared P2P test utilities for iroh integration tests.
//!
//! Common setup patterns, extraction helpers, and wait utilities
//! used across the iroh P2P test suite.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::Value;

use crate::{poll_until, DefraClient, TestCluster};

/// Standard timeout for P2P operations.
pub const P2P_TIMEOUT: Duration = Duration::from_secs(15);

/// Standard poll interval for P2P replication checks.
pub const P2P_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Extract the iroh peer ID from a node's p2p_info response.
///
/// For iroh, p2p_info returns `["{listen_addr}/p2p/{endpoint_id}"]`.
pub fn extract_peer_id(cluster: &TestCluster, node_index: usize) -> String {
    let client = cluster.client(node_index);
    let info = client.p2p_info().expect("failed to get p2p info");
    let addr = info
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node has no P2P address");
    if let Some(pos) = addr.rfind("/p2p/") {
        addr[pos + 5..].to_string()
    } else {
        addr.to_string()
    }
}

/// Extract the full P2P address string from a node.
pub fn extract_p2p_addr(cluster: &TestCluster, node_index: usize) -> String {
    let client = cluster.client(node_index);
    let info = client.p2p_info().expect("failed to get p2p info");
    info.as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node has no P2P address")
        .to_string()
}

/// Extract the full P2P address string from a node using an identity key.
///
/// Use this for NAC-enabled nodes where p2p_info requires authorization.
pub fn extract_p2p_addr_with_identity(
    cluster: &TestCluster,
    node_index: usize,
    hex_key: &str,
) -> String {
    let client = cluster.client(node_index);
    let info = client
        .p2p_info_with_identity(hex_key)
        .expect("failed to get p2p info");
    info.as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node has no P2P address")
        .to_string()
}

/// Extract a document ID from a GraphQL mutation result.
///
/// Handles both array and object response formats:
/// - `{"create_Foo": [{"_docID": "..."}]}`
/// - `{"create_Foo": {"_docID": "..."}}`
pub fn extract_doc_id(result: &Value, mutation_name: &str) -> String {
    result[mutation_name]
        .as_array()
        .and_then(|arr| arr.first())
        .or_else(|| {
            result[mutation_name]
                .as_object()
                .map(|_| &result[mutation_name])
        })
        .and_then(|v| v.get("_docID"))
        .and_then(|v| v.as_str())
        .expect("could not extract _docID")
        .to_string()
}

/// Set up a 2-node iroh cluster with a schema deployed on both nodes.
///
/// Returns (cluster, node1_address).
pub async fn setup_two_node_iroh(schema: &str) -> (TestCluster, String) {
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

    node0.schema_add(schema).expect("add schema node0");
    node1.schema_add(schema).expect("add schema node1");

    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("p2p connect");

    (cluster, addr1)
}

/// Set up a 2-node iroh cluster with replication configured.
///
/// Both nodes have the schema, are connected, and collections are subscribed
/// with a replicator set from node0 → node1.
pub async fn setup_two_node_replicated(
    schema: &str,
    collections: &[&str],
) -> (TestCluster, String) {
    let (cluster, addr1) = setup_two_node_iroh(schema).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0
        .p2p_collection_add(collections)
        .expect("collection add node0");
    node1
        .p2p_collection_add(collections)
        .expect("collection add node1");
    node0
        .p2p_replicator_set(collections, &addr1)
        .expect("replicator set");

    (cluster, addr1)
}

/// Set up a 3-node iroh chain: node0 → node1 → node2.
pub async fn setup_three_node_chain(schema: &str, collections: &[&str]) -> TestCluster {
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
        cluster.client(i).schema_add(schema).unwrap_or_else(|_| {
            panic!("add schema node{}", i);
        });
        cluster
            .client(i)
            .p2p_collection_add(collections)
            .unwrap_or_else(|_| {
                panic!("collection add node{}", i);
            });
    }

    let addr1 = extract_p2p_addr(&cluster, 1);
    let addr2 = extract_p2p_addr(&cluster, 2);

    cluster
        .client(0)
        .p2p_connect(&[&addr1])
        .expect("connect 0→1");
    cluster
        .client(1)
        .p2p_connect(&[&addr2])
        .expect("connect 1→2");
    cluster
        .client(0)
        .p2p_replicator_set(collections, &addr1)
        .expect("replicator 0→1");
    cluster
        .client(1)
        .p2p_replicator_set(collections, &addr2)
        .expect("replicator 1→2");

    cluster
}

/// Wait until the given collection has at least `count` documents on a node.
pub async fn wait_for_doc_count(client: &DefraClient, collection: &str, count: usize) {
    let query = format!("query {{ {} {{ _docID }} }}", collection);
    let client_ref = client;
    poll_until(
        || {
            let result = client_ref.query(&query).unwrap_or_default();
            result[collection]
                .as_array()
                .map(|arr| arr.len() >= count)
                .unwrap_or(false)
        },
        P2P_TIMEOUT,
        P2P_POLL_INTERVAL,
        &format!(
            "{} docs did not appear in {} within timeout",
            count, collection
        ),
    )
    .await;
}

/// Wait until replication produces docs with specific field values.
pub async fn wait_for_field_values(
    client: &DefraClient,
    collection: &str,
    expected: &HashMap<String, i64>,
) {
    let query = format!("query {{ {} {{ name age }} }}", collection);
    let client_ref = client;
    let expected_clone = expected.clone();
    poll_until(
        || {
            let result = client_ref.query(&query).unwrap_or_default();
            let arr = match result[collection].as_array() {
                Some(a) => a,
                None => return false,
            };
            let mut found: HashMap<String, i64> = HashMap::new();
            for u in arr {
                if let (Some(name), Some(age)) = (u["name"].as_str(), u["age"].as_i64()) {
                    found.insert(name.to_string(), age);
                }
            }
            expected_clone
                .iter()
                .all(|(name, age)| found.get(name) == Some(age))
        },
        Duration::from_secs(30),
        P2P_POLL_INTERVAL,
        &format!(
            "expected field values did not replicate to {} within timeout",
            collection
        ),
    )
    .await;
}
