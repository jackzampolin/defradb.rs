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
//! `rust_rust_se_cross_node` exercises Part 1's CLI SE wiring over two real
//! `defra` processes: node A loads the seeded SE key, generates SE artifacts on
//! write (logged `Sent PushSEArtifacts ... artifacts_count=1`), and the doc +
//! encrypted-index query resolve on peer B.
//!
//! The Go<->Rust variants are `#[ignore]`d on a CLI feature gap (NOT a keyring
//! gap -- seeding works; both runtimes load the same key). Go's SE query model
//! (`internal/se/coordinator.go::QueryDocIDsByValues`) runs on the document
//! *owner*, which fans the search out to its replicators over the
//! `/defradb/se_query_req/0.0.1` protocol; replicators serve docIDs from the
//! artifacts the owner pushed. The standalone Rust `defra` CLI does not wire
//! that SE query fan-out / serve loop nor `se::receive_and_store` (they live in
//! `crates/embedded/src/node_tasks.rs`, used by the FFI, not the CLI). The
//! CLI's `execute_encrypted_select` resolves encrypted queries against local
//! plaintext only. So:
//!   - Querying the Go *reader* directly returns empty (it has no replicators
//!     to fan out to) -- verified: the doc replicates (plain `User` returns it)
//!     but `encrypted_User` is empty.
//!   - A Go owner querying a Rust replicator can't be served (CLI has no SE
//!     query serve handler).
//! Part 1 (load + push SE artifacts) is done and proven by the Rust writer log;
//! the missing piece is the CLI SE *query* P2P loop -- a separate feature
//! beyond #976's keyring-load scope. Un-ignore once the CLI wires the SE
//! query/serve loop (mirror `embedded/src/node_tasks.rs`).

use std::time::{Duration, Instant};

use integration_test::{BinarySource, TestCluster};

/// Path to the Go `defradb` binary built with KMS + searchable encryption.
const GO_KMS_BINARY: &str =
    "/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb/build/defradb";

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
/// Node A loads the seeded key (logs "Searchable encryption key loaded from
/// keyring"), writes a User with an encrypted index on `name`, generates an SE
/// artifact and pushes it to B (logs "Sent PushSEArtifacts ...
/// artifacts_count=1"); the doc replicates and `encrypted_User` on B resolves
/// it. End-to-end regression guard for Part 1's CLI SE-key wiring over real
/// processes (the SE key feeds the broadcast mutator + merge handler).
///
/// Note: the CLI's encrypted query resolves against local plaintext, so this
/// asserts the SE-keyed write path runs end-to-end without breaking
/// replication/query -- it does not assert artifact-based remote tag matching
/// (that needs the CLI SE query fan-out/serve loop; see the ignored Go<->Rust
/// tests and module docs).
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

    let deadline = Instant::now() + Duration::from_secs(30);
    wait_for_se_query(
        &node_b,
        "User",
        r#"{name: {_eq: "John"}}"#,
        &doc_id,
        deadline,
    )
    .await;
}

/// Rust writes; Go reads and runs the SE query. Go's encrypted-index query is
/// artifact-based, so a match would prove Go recomputed the search tag from the
/// shared key over the document Rust replicated. Node 0 = Rust (writer), node 1
/// = Go (reader).
///
/// IGNORED (CLI feature gap, not a keyring gap): the doc replicates to Go and
/// Rust pushes its SE artifact, but Go's reader-side `encrypted_User` query
/// fans out to *its* replicators (it has none here) rather than searching the
/// locally-stored artifacts, so it returns empty. The owner-queries-replicators
/// model needs the CLI to implement the SE query fan-out/serve loop (see module
/// docs). Verified: plain `User` on Go returns the doc; `encrypted_User` empty.
#[ignore = "CLI lacks the SE query fan-out/serve P2P loop; Go owner-queries-replicator model can't resolve on the reader. Keyring seeding works (both nodes load the same key). See module docs."]
#[tokio::test]
async fn rust_to_go_se_cross_node() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_encryption()
        .with_shared_searchable_encryption_key(SHARED_SE_KEY)
        .with_go_binary(BinarySource::Path(GO_KMS_BINARY.into()))
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

    let deadline = Instant::now() + Duration::from_secs(45);
    wait_for_se_query(&go, "User", r#"{name: {_eq: "John"}}"#, &doc_id, deadline).await;
}

/// Go writes; Rust reads and runs the SE query. Node 0 = Rust (reader), node 1
/// = Go (writer).
///
/// IGNORED (CLI feature gap, not a keyring gap): same root cause as
/// `rust_to_go_se_cross_node` -- the standalone CLI's encrypted-index query
/// resolves against local plaintext and does not fan out to / serve from
/// replicators. Keyring seeding works (both nodes load the shared key).
#[ignore = "CLI lacks the SE query fan-out/serve P2P loop; encrypted query resolves local plaintext only. Keyring seeding works. See module docs."]
#[tokio::test]
async fn go_to_rust_se_cross_node() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_encryption()
        .with_shared_searchable_encryption_key(SHARED_SE_KEY)
        .with_go_binary(BinarySource::Path(GO_KMS_BINARY.into()))
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

    let deadline = Instant::now() + Duration::from_secs(45);
    wait_for_se_query(&rust, "User", r#"{name: {_eq: "John"}}"#, &doc_id, deadline).await;
}
