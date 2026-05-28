//! P2P replication parity test for issue #651.
//!
//! Validates that after switching Rust to random per-write encryption keys
//! (matching Go's `crypto/rand` model), an encrypted document written on one
//! node replicates and decrypts cleanly on a peer. Before #651 was fixed,
//! Rust used deterministic `SHA-256(field_name || doc_id || master_key)` key
//! derivation — wire-incompatible with Go and cryptographically weaker.
//!
//! `rust_rust_encrypted_p2p_replication` covers the pure-Rust path.
//!
//! `go_to_rust_encrypted_p2p_replication` / `rust_to_go_encrypted_p2p_replication`
//! exercise Go↔Rust KMS wire-compat over libp2p: the reader must fetch the DEK
//! from the writer's KMS over the `encryption` gossip topic (bare CBOR
//! `FetchEncryptionKeyRequest`/`Reply`, ECIES-wrapped key blocks) and decrypt.
//! These require the Go KMS binary at `GO_KMS_BINARY`.

use std::time::{Duration, Instant};

use integration_test::{BinarySource, TestCluster};

/// Path to the Go `defradb` binary built with KMS + the #4778 fix.
const GO_KMS_BINARY: &str =
    "/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb/build/defradb";

#[tokio::test]
async fn rust_rust_encrypted_p2p_replication() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_encryption()
        .build()
        .await
        .expect("build rust 2-node cluster with p2p + encryption");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    let info1 = node1.p2p_info().expect("failed to get node1 p2p info");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");

    let schema = "type Vault { secret: String  pin: String  notes: String }";
    node0.schema_add(schema).expect("add Vault schema on node0");
    node1.schema_add(schema).expect("add Vault schema on node1");

    node0.p2p_connect(&[addr1]).expect("connect peers");
    node0
        .p2p_collection_add(&["Vault"])
        .expect("collection add node0");
    node1
        .p2p_collection_add(&["Vault"])
        .expect("collection add node1");
    node0
        .p2p_replicator_set(&["Vault"], addr1)
        .expect("replicator 0->1");

    let created = node0
        .query(
            r#"mutation { add_Vault(input: {secret: "topsecret", pin: "4242", notes: "hello"}, encryptFields: [secret, pin]) { _docID } }"#,
        )
        .expect("create encrypted Vault on node0");
    let doc_id = created["add_Vault"][0]["_docID"]
        .as_str()
        .or_else(|| created["add_Vault"]["_docID"].as_str())
        .expect("missing _docID")
        .to_string();

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let result = node1
            .query("query { Vault { _docID secret pin notes } }")
            .expect("query Vault on node1");
        if let Some(rows) = result["Vault"].as_array() {
            if let Some(row) = rows.iter().find(|r| r["_docID"].as_str() == Some(&doc_id)) {
                assert_eq!(
                    row["secret"], "topsecret",
                    "secret must decrypt after P2P replication with random per-write keys"
                );
                assert_eq!(
                    row["pin"], "4242",
                    "pin must decrypt after P2P replication with random per-write keys"
                );
                assert_eq!(row["notes"], "hello", "plaintext notes must replicate");
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "encrypted doc did not replicate within timeout"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Go↔Rust KMS interop over libp2p.
///
/// One node is a real Go `defradb` (with KMS), the other is Rust. An encrypted
/// document is written on `writer`, replicated to `reader`, and `reader` must
/// fetch the DEK from `writer`'s KMS over the `encryption` gossip topic to
/// decrypt. Asserts `reader` reads back the exact plaintext.
///
/// With `.rust_nodes(1).go_nodes(1)` the harness assigns index 0 = Rust,
/// index 1 = Go (Rust nodes are spawned before Go nodes in the builder).
async fn go_rust_kms_interop(writer_idx: usize, reader_idx: usize) {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_encryption()
        .with_go_binary(BinarySource::Path(GO_KMS_BINARY.into()))
        .build()
        .await
        .expect("build mixed go/rust cluster with p2p + encryption");

    let writer = cluster.client(writer_idx);
    let reader = cluster.client(reader_idx);

    let timeout = Duration::from_secs(30);
    cluster
        .wait_for_log(writer_idx, "p2p_listening", timeout)
        .await
        .expect("writer P2P listener did not start");
    cluster
        .wait_for_log(reader_idx, "p2p_listening", timeout)
        .await
        .expect("reader P2P listener did not start");

    let reader_info = reader.p2p_info().expect("failed to get reader p2p info");
    let reader_addr = reader_info
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("reader has no P2P address");

    let schema = "type Vault { secret: String  pin: String  notes: String }";
    writer
        .schema_add(schema)
        .expect("add Vault schema on writer");
    reader
        .schema_add(schema)
        .expect("add Vault schema on reader");

    writer.p2p_connect(&[reader_addr]).expect("connect peers");
    writer
        .p2p_collection_add(&["Vault"])
        .expect("collection add writer");
    reader
        .p2p_collection_add(&["Vault"])
        .expect("collection add reader");
    writer
        .p2p_replicator_set(&["Vault"], reader_addr)
        .expect("replicator writer->reader");

    let created = writer
        .query(
            r#"mutation { add_Vault(input: {secret: "topsecret", pin: "4242", notes: "hello"}, encryptFields: [secret, pin]) { _docID } }"#,
        )
        .expect("create encrypted Vault on writer");
    let doc_id = created["add_Vault"][0]["_docID"]
        .as_str()
        .or_else(|| created["add_Vault"]["_docID"].as_str())
        .expect("missing _docID")
        .to_string();

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let result = reader
            .query("query { Vault { _docID secret pin notes } }")
            .expect("query Vault on reader");
        if let Some(rows) = result["Vault"].as_array() {
            if let Some(row) = rows.iter().find(|r| r["_docID"].as_str() == Some(&doc_id)) {
                // notes is plaintext: replicates immediately. secret/pin are
                // encrypted and require a successful cross-runtime KMS DEK fetch.
                assert_eq!(
                    row["secret"], "topsecret",
                    "secret must decrypt after cross-runtime KMS DEK fetch over the encryption topic"
                );
                assert_eq!(
                    row["pin"], "4242",
                    "pin must decrypt after cross-runtime KMS DEK fetch over the encryption topic"
                );
                assert_eq!(row["notes"], "hello", "plaintext notes must replicate");
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "encrypted doc did not replicate+decrypt within timeout (doc_id={doc_id})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Go writes an encrypted doc; Rust replicates it and must fetch the DEK from
/// the Go node's KMS over the `encryption` topic. Exercises the Rust KMS
/// requester against a Go KMS server.
///
/// IGNORED: blocked upstream of KMS by Go↔Rust encrypted-doc block
/// replication. For encrypted docs the Go PushLog does not carry the blocks
/// inline (unlike plaintext docs, which replicate fine — see
/// `replication::go_rust_replication`), so the Rust node falls back to a
/// Bitswap DAG walk. Bitswap fetches the per-field encrypted delta blocks but
/// the root composite block is never served by the Go provider, so the DAG
/// fetch times out (`DAG fetch incomplete ... remaining_count=2`) and the doc
/// never materializes. The Rust KMS `encryption`-topic exchange is never
/// reached. This is a P2P block-transport interop gap, NOT a KMS wire-compat
/// bug; the KMS requester path cannot be validated until it clears.
#[tokio::test]
#[ignore = "blocked upstream of KMS: Go->Rust encrypted-doc Bitswap DAG fetch never retrieves the root composite block; KMS topic never reached"]
async fn go_to_rust_encrypted_p2p_replication() {
    // index 0 = Rust (reader), index 1 = Go (writer)
    go_rust_kms_interop(1, 0).await;
}

/// Rust writes an encrypted doc; Go replicates it and must fetch the DEK from
/// the Rust node's KMS over the `encryption` topic. Exercises the Rust KMS
/// server against a Go KMS requester.
///
/// IGNORED: blocked upstream of KMS by the same encrypted-doc block
/// replication gap as `go_to_rust_encrypted_p2p_replication`, in reverse —
/// the Go reader never receives/merges the encrypted doc's blocks from the
/// Rust writer, so it never issues a KMS `FetchEncryptionKeyRequest`. The Rust
/// KMS server path cannot be validated until block replication clears.
#[tokio::test]
#[ignore = "blocked upstream of KMS: Rust->Go encrypted-doc blocks never fully merge on the Go reader; KMS topic never reached"]
async fn rust_to_go_encrypted_p2p_replication() {
    // index 0 = Rust (writer), index 1 = Go (reader)
    go_rust_kms_interop(0, 1).await;
}
