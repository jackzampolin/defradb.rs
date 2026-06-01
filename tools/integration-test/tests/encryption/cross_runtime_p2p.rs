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
//! `go_to_rust_encrypted_p2p_replication` / `go_rust_encrypted_p2p_replication_rust_writer`
//! exercise Go↔Rust KMS wire-compat over libp2p: the reader must fetch the DEK
//! from the writer's KMS over the `encryption` gossip topic (bare CBOR
//! `FetchEncryptionKeyRequest`/`Reply`, ECIES-wrapped key blocks) and decrypt.
//! These require the Go KMS binary at `GO_KMS_BINARY`.

use std::path::Path;
use std::time::{Duration, Instant};

use integration_test::identity::generate_identity;
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
    // Go's KMS pubsub service — which subscribes to the `encryption` topic —
    // is only created when the node has a *node identity*
    // (internal/db/p2p/p2p.go: `if nodeIdentity.HasValue()`). The `--identity`
    // flag sets only the *request* identity, not the node identity; the node
    // identity comes from the keyring (`getOrCreateIdentity`) or, when the
    // keyring is disabled, from dev mode's ephemeral identity (cli/start.go).
    // The harness always launches the Go node with `--no-keyring`, so without
    // `--development` Go never gets a node identity, never subscribes to
    // `encryption`, and the Rust requester's publish has zero targets. Dev
    // mode gives both nodes a node identity. We also mint explicit request
    // identities so the fetch carries a real DID. Node index 0 = Rust, 1 = Go.
    let go_binary = Path::new(GO_KMS_BINARY);
    let rust_identity =
        generate_identity(go_binary).expect("generate request identity for rust node");
    let go_identity = generate_identity(go_binary).expect("generate request identity for go node");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_encryption()
        .with_development()
        .with_node_identity(0, rust_identity.private_key_hex)
        .with_node_identity(1, go_identity.private_key_hex)
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
/// The original P2P block-transport gap is FIXED: Rust's DAG-completion walk
/// (`find_all_missing_links`) used libipld `references()`, which includes the
/// `encryption` link. Go stores encryption-metadata blocks in a separate
/// `Encstore` and serves them ONLY over the KMS `encryption` pubsub topic, not
/// Bitswap (Go's `loadBlockLinks` walks `block.AllLinks()`, which excludes the
/// encryption link, and fetches it via `kms.GetKeys`). Rust now excludes the
/// encryption link from the Bitswap walk, so the encrypted DAG fully
/// replicates and the merge is reached.
///
/// The `encryption`-topic mesh symptom (#976) is FIXED. Two prior gaps are
/// resolved:
///   1. Rust-side race: `PubsubKeyTransport::send_request` published the fetch
///      immediately on a key-miss, before the peer's `encryption` SUBSCRIBE had
///      propagated, so `flood_publish` had zero targets → InsufficientPeers. It
///      now waits (bounded) for a known subscriber before publishing.
///   2. Test config: Go only subscribes to `encryption` when it has a *node
///      identity* (internal/db/p2p/p2p.go: `if nodeIdentity.HasValue()`), which
///      under `--no-keyring` requires `--development`. The cluster now runs
///      both nodes in dev mode with explicit request identities. Go logs
///      `Adding pubsub topic Topic=encryption` and Rust records the Go peer as
///      an `encryption` subscriber (`PeerSubscribed`), so the publish now has a
///      target.
///
/// STILL IGNORED — a separate, deeper gap remains downstream of the mesh: the
/// reader's merge reaches `decrypt_block_data` → `kms.get_keys().wait_all()`,
/// which blocks for the full test window with no reply ever arriving from the
/// Go KMS server (no `Document stored`, no `Encryption block not found`). Either
/// the request does not reach Go's KMS serve handler or Go never publishes a
/// usable reply on `encryption`. Diagnosing this requires Go-side KMS trace
/// logging (out of scope: RUST-ONLY) and is beyond the encryption-topic-mesh
/// scope of #976. See the design/976-kms work and `crates/p2p/src/kms/`.
#[tokio::test]
async fn go_to_rust_encrypted_p2p_replication() {
    // index 0 = Rust (reader), index 1 = Go (writer)
    go_rust_kms_interop(1, 0).await;
}

/// Rust writes an encrypted doc; Go replicates it and must fetch the DEK from
/// the Rust node's KMS over the `encryption` topic. Exercises the Rust KMS
/// server against a Go KMS requester. The Rust KMS serves the DEK from its
/// blockstore-backed KeyStore (#976), so the Go reader's
/// `FetchEncryptionKeyRequest` is answered for any encrypted write.
#[tokio::test]
async fn go_rust_encrypted_p2p_replication_rust_writer() {
    // index 0 = Rust (writer), index 1 = Go (reader)
    go_rust_kms_interop(0, 1).await;
}
