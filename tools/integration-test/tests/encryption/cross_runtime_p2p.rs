//! P2P replication parity test for issue #651.
//!
//! Validates that after switching Rust to random per-write encryption keys
//! (matching Go's `crypto/rand` model), an encrypted document written on one
//! node replicates and decrypts cleanly on a peer. Before #651 was fixed,
//! Rust used deterministic `SHA-256(field_name || doc_id || master_key)` key
//! derivation — wire-incompatible with Go and cryptographically weaker.
//!
//! Only `rust_rust` is covered here because Go-side P2P encrypted replication
//! currently hangs in this harness for reasons unrelated to #651 (observed
//! with `go_go_` as well, which exercises no Rust code).

use std::time::{Duration, Instant};

use integration_test::TestCluster;

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
