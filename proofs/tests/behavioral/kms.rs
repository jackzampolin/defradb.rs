//! KMS key distribution — an unauthorized node's request for a document's
//! data-encryption key is DENIED by the holder's KMS policy. The denial is made
//! observable by a `tracing::warn!` at the serve-side policy check in
//! `crates/kms/src/defra_kms.rs` (otherwise it is a silent reply omission that
//! manifests only as a fetch timeout — see the registry note that previously
//! marked this family Boundary).
//! Model: `MC_Kms_Green` (proofs/tla); ECIES secrecy remains a crypto boundary.
//!
//! Anti-tautology: the owner is asserted to read the decrypted, ACP-protected
//! field on node0 FIRST (proving the document is genuinely encrypted and
//! protected), and the deny line only appears when node1 actually replicates the
//! encrypted blocks and requests the DEK — so neither leg can pass on a no-op.
//!
//! The denial is read from node0's `stdout.log` directly: the harness'
//! `wait_for_log` only matches pre-registered named patterns, not arbitrary
//! lines, and the deny is a one-shot event.

use crate::support;
use defra_harness::fixtures::{users_schema_with_policy, USER_ACP_POLICY};
use defra_harness::{generate_identity, TestCluster};
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn node_log_path(cluster: &TestCluster, index: usize) -> PathBuf {
    cluster.nodes[index]
        .rootdir
        .parent()
        .expect("node data dir has a parent")
        .join("logs/stdout.log")
}

async fn poll_log_contains(path: &PathBuf, needle: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(contents) = std::fs::read_to_string(path) {
            if contents.contains(needle) {
                return true;
            }
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn kms_unauthorized_dek_release_denied() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_acp_local()
        .with_encryption()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build 2-node encrypted + ACP cluster");
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let owner = generate_identity(node0.binary_path()).expect("owner identity");

    // ACP policy + protected schema on both nodes (node1 must accept the
    // protected collection to replicate its blocks).
    let policy = node0
        .acp_policy_add(USER_ACP_POLICY, &owner.private_key_hex)
        .expect("add ACP policy on node0");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("PolicyID");
    let schema = users_schema_with_policy(policy_id);
    node0
        .schema_add_with_identity(&schema, &owner.private_key_hex)
        .expect("schema node0");
    node1
        .acp_policy_add(USER_ACP_POLICY, &owner.private_key_hex)
        .expect("add ACP policy on node1");
    node1
        .schema_add_with_identity(&schema, &owner.private_key_hex)
        .expect("schema node1");

    // Owner creates an encrypted, ACP-protected document on node0.
    node0
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Secret", age: 42}, encryptFields: [age]) { _docID } }"#,
            &owner.private_key_hex,
        )
        .expect("create encrypted protected document");

    // Positive (anti-tautology): the owner reads the decrypted protected field.
    let owner_read = node0
        .query_with_identity("query { User { _docID age } }", &owner.private_key_hex)
        .expect("owner read");
    assert_eq!(
        owner_read["User"].as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "owner must see its protected document"
    );
    assert_eq!(
        owner_read["User"][0]["age"], 42,
        "owner must read the decrypted age"
    );

    // Wire replication node0 -> node1. node1 is NOT granted access, so when its
    // merge replicates the encrypted blocks and requests the DEK from node0's
    // KMS, node0's policy must DENY the release.
    let info1 = node1.p2p_info().expect("p2p info node1");
    let addr1 = info1
        .as_array()
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .expect("node1 p2p address");
    node0.p2p_connect(&[addr1]).expect("connect node0 -> node1");
    node0
        .p2p_collection_add(&["User"])
        .expect("subscribe node0");
    node1
        .p2p_collection_add(&["User"])
        .expect("subscribe node1");
    node0
        .p2p_replicator_set(&["User"], addr1)
        .expect("set replicator node0 -> node1");

    // node0's KMS must deny the DEK to node1's unauthorized fetch — observable
    // as a serve-side denial in node0's log.
    let log0 = node_log_path(&cluster, 0);
    assert!(
        poll_log_contains(&log0, "DEK release DENIED", Duration::from_secs(25)).await,
        "node0 KMS must deny the DEK to the unauthorized node1 \
         (no 'DEK release DENIED' appeared in node0's log)"
    );
}
