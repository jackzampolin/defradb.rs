//! Searchable-encryption (SE) write-compatibility over P2P, issue #976.
//!
//! SE is a cluster-shared secret: an `encrypted_<Collection>` query only
//! resolves a replicated document when the querying node holds the same
//! `searchable-encryption-key` the writer used to produce its SE artifacts.
//! Go provisions that key per node via the keyring
//! (`cli/start.go:getOrCreateSearchableEncryptionKey`); the standalone Rust
//! `defra start` now mirrors that getOrCreate (#976, Part 1).
//!
//! These tests seed the SAME 32-byte SE key into every node's keyring before
//! start (via `TestClusterBuilder::with_shared_searchable_encryption_key`),
//! so both nodes' getOrCreate find it instead of minting a fresh one. A doc
//! is written + replicated, then the encrypted-index query runs on the peer.
//!
//! The standalone `defra` CLI now wires the SE query fan-out / serve loop
//! (#976), mirroring Go's owner-queries-replicator model
//! (`internal/se/coordinator.go::QueryDocIDsByValues`): the document *owner*
//! runs the `encrypted_<Collection>` query, which generates search tags and
//! fans them out to its replicators over the `/defradb/se_query_req/0.0.1`
//! two-stream protocol; the replicator byte-matches the tags against the
//! artifacts the owner pushed and returns the matching docIDs. The owner never
//! resolves locally; zero replicators yields an empty result.
//!
//! Because the search tag binds the writer's identity, the WRITE-side and
//! QUERY-side identities must agree. The CLI write side uses an anonymous
//! identity (`identity_pubkey: None`), so the query side does too; this matches
//! Go (which byte-matches on the serving node regardless of identity, as long
//! as the owner that wrote and the owner that queries are the same node).
//!
//! Topology for every test below: the QUERIER is the document OWNER (the node
//! that wrote the doc and set the replicator). The owner fans out to its
//! replicator, which serves docIDs from the pushed artifacts.

use std::time::{Duration, Instant};

use integration_test::TestCluster;

/// A fixed 32-byte AES-256 searchable-encryption key shared across nodes.
/// Distinct value per byte so a wrong key (e.g. a freshly generated one)
/// would not coincidentally match.
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
            .unwrap_or_else(|e| panic!("encrypted query failed on peer: {e}"));
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

/// Connect node A -> B, register A's collection + replicator, and return B's
/// P2P address. Both nodes must already have the schema + encrypted index.
async fn connect_and_replicate(
    cluster: &TestCluster,
    writer_idx: usize,
    reader_idx: usize,
    collection: &str,
) {
    let timeout = Duration::from_secs(30);
    cluster
        .wait_for_log(writer_idx, "p2p_listening", timeout)
        .await
        .expect("writer P2P listener did not start");
    cluster
        .wait_for_log(reader_idx, "p2p_listening", timeout)
        .await
        .expect("reader P2P listener did not start");

    let writer = cluster.client(writer_idx);
    let reader = cluster.client(reader_idx);

    let reader_info = reader.p2p_info().expect("failed to get reader p2p info");
    let reader_addr = reader_info
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("reader has no P2P address");

    writer.p2p_connect(&[reader_addr]).expect("connect peers");
    writer
        .p2p_collection_add(&[collection])
        .expect("collection add writer");
    reader
        .p2p_collection_add(&[collection])
        .expect("collection add reader");
    writer
        .p2p_replicator_set(&[collection], reader_addr)
        .expect("replicator writer->reader");
}

/// Two Rust `defra` processes share one SE key (seeded into both keyrings).
/// Node A (the OWNER) writes a User with an encrypted index on `name`, pushes
/// the SE artifact to its replicator B, then runs `encrypted_User` ON A. A
/// generates the search tag, fans it out to B over the SE query protocol, and B
/// byte-matches the artifact and returns the docID. End-to-end exercise of the
/// CLI SE query/serve loop (#976).
#[tokio::test]
async fn rust_rust_se_cross_node() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_encryption()
        .with_shared_searchable_encryption_key(SHARED_SE_KEY)
        .build()
        .await
        .expect("build rust 2-node cluster with shared SE key");

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

    connect_and_replicate(&cluster, 0, 1, "User").await;

    let created = node_a
        .query(r#"mutation { add_User(input: {name: "John", age: 21, city: "NYC"}) { _docID } }"#)
        .expect("create User on node A");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .or_else(|| created["add_User"]["_docID"].as_str())
        .expect("missing _docID")
        .to_string();

    // Query runs on the OWNER (node A), which fans out to replicator B.
    let deadline = Instant::now() + Duration::from_secs(30);
    wait_for_se_query(
        &node_a,
        "User",
        r#"{name: {_eq: "John"}}"#,
        &doc_id,
        deadline,
    )
    .await;
}

/// Rust is the OWNER: Rust (node 0) writes a User, pushes the SE artifact to
/// its replicator Go (node 1), then runs `encrypted_User` ON RUST. Rust
/// generates the search tag and fans it out to the Go replicator, which
/// byte-matches and returns the docID. Proves the Rust requester +
/// tag-generation interoperate with a Go serve side. Node 0 = Rust (owner),
/// node 1 = Go (replicator).
#[tokio::test]
async fn go_rust_se_cross_node_rust_owner() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_encryption()
        .with_shared_searchable_encryption_key(SHARED_SE_KEY)
        .build()
        .await
        .expect("build mixed go/rust cluster with shared SE key");

    let rust = cluster.client(0);
    let go = cluster.client(1);

    rust.schema_add(USER_SCHEMA).expect("add schema rust");
    go.schema_add(USER_SCHEMA).expect("add schema go");
    rust.encrypted_index_add("User", "name")
        .expect("encrypted index rust");
    go.encrypted_index_add("User", "name")
        .expect("encrypted index go");

    connect_and_replicate(&cluster, 0, 1, "User").await;

    let created = rust
        .query(r#"mutation { add_User(input: {name: "John", age: 21, city: "NYC"}) { _docID } }"#)
        .expect("create User on rust");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .or_else(|| created["add_User"]["_docID"].as_str())
        .expect("missing _docID")
        .to_string();

    // Query runs on the OWNER (Rust, node 0), which fans out to Go replicator.
    let deadline = Instant::now() + Duration::from_secs(45);
    wait_for_se_query(&rust, "User", r#"{name: {_eq: "John"}}"#, &doc_id, deadline).await;
}

/// Go is the OWNER: Go (node 1) writes a User, pushes the SE artifact to its
/// replicator Rust (node 0), then runs `encrypted_User` ON GO. Go fans the
/// search tag out to the Rust replicator, which exercises the Rust SERVE loop
/// (byte-match + signed reply). Node 0 = Rust (replicator), node 1 = Go (owner).
#[tokio::test]
async fn go_to_rust_se_cross_node() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_encryption()
        .with_shared_searchable_encryption_key(SHARED_SE_KEY)
        .build()
        .await
        .expect("build mixed go/rust cluster with shared SE key");

    let rust = cluster.client(0);
    let go = cluster.client(1);

    rust.schema_add(USER_SCHEMA).expect("add schema rust");
    go.schema_add(USER_SCHEMA).expect("add schema go");
    rust.encrypted_index_add("User", "name")
        .expect("encrypted index rust");
    go.encrypted_index_add("User", "name")
        .expect("encrypted index go");

    // Go (node 1) writes and replicates to Rust (node 0).
    connect_and_replicate(&cluster, 1, 0, "User").await;

    let created = go
        .query(r#"mutation { add_User(input: {name: "John", age: 21, city: "NYC"}) { _docID } }"#)
        .expect("create User on go");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .or_else(|| created["add_User"]["_docID"].as_str())
        .expect("missing _docID")
        .to_string();

    // Query runs on the OWNER (Go, node 1), which fans out to Rust replicator
    // (node 0) -- this exercises the Rust SE serve loop.
    let deadline = Instant::now() + Duration::from_secs(45);
    wait_for_se_query(&go, "User", r#"{name: {_eq: "John"}}"#, &doc_id, deadline).await;
}
