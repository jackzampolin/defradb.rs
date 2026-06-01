//! Searchable-encryption (SE) query/serve over the **iroh** transport, #976.
//!
//! Mirrors the libp2p `rust_rust_se_cross_node` test
//! (`tests/encryption/se_cross_runtime.rs`) but runs both nodes on the iroh
//! transport (`with_iroh_transport`, which auto-builds the iroh-featured Rust
//! binary). It proves the SE query fan-out / serve loop is wired over iroh:
//! the document OWNER (node 0) runs `encrypted_User`, generates the search tag,
//! fans it out to its replicator (node 1) over the iroh SE query protocol, and
//! the replicator byte-matches the pushed artifact and returns the docID.
//!
//! Topology: the QUERIER is the document OWNER (node 0). Owner fans out to its
//! replicator (node 1).

use std::time::{Duration, Instant};

use integration_test::{extract_p2p_addr, TestCluster};
use serial_test::serial;

/// A fixed 32-byte AES-256 searchable-encryption key shared across nodes.
const SHARED_SE_KEY: [u8; 32] = [
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
    0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
];

const USER_SCHEMA: &str = "type User { name: String  age: Int  city: String }";

/// Poll `encrypted_<Collection>(filter: {field: {_eq: value}}) { docIDs }` on
/// `node` until it returns `expected_doc_id`, or panic past `deadline`.
async fn wait_for_se_query(
    node: &integration_test::DefraClient,
    collection: &str,
    filter: &str,
    expected_doc_id: &str,
    deadline: Instant,
) {
    let query = format!("query {{ encrypted_{collection}(filter: {filter}) {{ docIDs }} }}");
    loop {
        let result = node
            .query(&query)
            .unwrap_or_else(|e| panic!("encrypted query failed on owner: {e}"));
        let key = format!("encrypted_{collection}");
        if let Some(rows) = result[&key].as_array() {
            for row in rows {
                if let Some(ids) = row["docIDs"].as_array() {
                    if ids.iter().any(|v| v.as_str() == Some(expected_doc_id)) {
                        return;
                    }
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "SE query did not return doc {expected_doc_id} within timeout; last result: {result}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Two Rust `defra` processes on the **iroh** transport share one SE key.
/// Node 0 (the OWNER) writes a User with an encrypted index on `name`, pushes
/// the SE artifact to its replicator node 1, then runs `encrypted_User` ON
/// node 0. Node 0 generates the search tag, fans it out to node 1 over the iroh
/// SE query protocol, and node 1 byte-matches the artifact and returns the
/// docID. End-to-end exercise of the iroh SE query/serve loop (#976).
#[tokio::test]
#[serial]
async fn rust_rust_se_iroh() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .with_encryption()
        .with_shared_searchable_encryption_key(SHARED_SE_KEY)
        .build()
        .await
        .expect("build rust 2-node iroh cluster with shared SE key");

    let p2p_timeout = Duration::from_secs(30);
    for i in 0..2 {
        cluster
            .wait_for_log(i, "p2p_listening", p2p_timeout)
            .await
            .unwrap_or_else(|_| panic!("node{i} P2P listener did not start"));
    }

    let node_a = cluster.client(0);
    let node_b = cluster.client(1);

    node_a.schema_add(USER_SCHEMA).expect("add schema node A");
    node_b.schema_add(USER_SCHEMA).expect("add schema node B");
    node_a
        .encrypted_index_add("User", "name")
        .expect("encrypted index node A");
    node_b
        .encrypted_index_add("User", "name")
        .expect("encrypted index node B");

    let addr_b = extract_p2p_addr(&cluster, 1);
    node_a.p2p_connect(&[&addr_b]).expect("connect peers");
    node_a
        .p2p_collection_add(&["User"])
        .expect("collection add owner");
    node_b
        .p2p_collection_add(&["User"])
        .expect("collection add replicator");
    node_a
        .p2p_replicator_set(&["User"], &addr_b)
        .expect("replicator owner->replicator");

    let created = node_a
        .query(r#"mutation { add_User(input: {name: "John", age: 21, city: "NYC"}) { _docID } }"#)
        .expect("create User on node A");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .or_else(|| created["add_User"]["_docID"].as_str())
        .expect("missing _docID")
        .to_string();

    // Query runs on the OWNER (node A), which fans out to replicator B over iroh.
    let deadline = Instant::now() + Duration::from_secs(45);
    wait_for_se_query(
        &node_a,
        "User",
        r#"{name: {_eq: "John"}}"#,
        &doc_id,
        deadline,
    )
    .await;
}
