//! Iroh P2P signature verification tests.
//!
//! Ported from Go: tests/integration/signature/ (P2P-related)
//!
//! These tests verify that signed documents sync correctly between peers
//! with different key types, and that signature verification works.
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh -- connection::signature

use std::time::Duration;

use integration_test::{
    extract_p2p_addr, generate_ed25519_identity, generate_identity, poll_until, DefraClient,
    TestCluster,
};
use serial_test::serial;

const SCHEMA: &str = "type Users { name: String  age: Int }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);

/// Helper: extract doc_id from create mutation response.
fn extract_doc_id(data: &serde_json::Value, mutation_name: &str) -> String {
    data[mutation_name]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v["_docID"].as_str())
        .or_else(|| data[mutation_name]["_docID"].as_str())
        .expect("missing _docID")
        .to_string()
}

/// Helper: get first commit CID for a document.
fn first_commit_cid(node: &DefraClient, doc_id: &str) -> String {
    let commits_query = format!(r#"query {{ _commits(docID: "{}") {{ cid }} }}"#, doc_id);
    let commits = node.query(&commits_query).expect("_commits query");
    let commits_arr = commits["_commits"]
        .as_array()
        .expect("_commits should be array");
    assert!(
        !commits_arr.is_empty(),
        "should have commits after creating doc"
    );
    commits_arr[0]["cid"]
        .as_str()
        .expect("commit should have cid")
        .to_string()
}

/// Helper: extract public key from TestIdentity (must have been generated with JSON output).
fn require_public_key(identity: &integration_test::TestIdentity) -> String {
    identity
        .public_key_hex
        .clone()
        .expect("identity should include public_key_hex")
}

/// Port: TestDocSignature_WithPeersAndSecp256k1KeyType_ShouldSync
/// Signed docs sync between peers (secp256k1).
#[tokio::test]
#[serial]
async fn peers_secp256k1_sync() {
    let identity =
        generate_identity(&integration_test::rust_binary()).expect("generate secp256k1 identity");

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .with_signing()
        .with_identity(&identity.private_key_hex)
        .build()
        .await
        .unwrap();

    for i in 0..2 {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{} listener", i));
        cluster
            .client(i)
            .schema_add(SCHEMA)
            .unwrap_or_else(|_| panic!("schema node{}", i));
    }

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let addr1 = extract_p2p_addr(&cluster, 1);

    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator");

    node0
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create signed John");

    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            r["Users"]
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
        "secp256k1-signed doc did not replicate",
    )
    .await;
}

/// Port: TestDocSignature_WithPeersAndEd25519KeyType_ShouldSync
/// Signed docs sync between peers (ed25519).
#[tokio::test]
#[serial]
async fn peers_ed25519_sync() {
    let identity = generate_ed25519_identity(&integration_test::rust_binary())
        .expect("generate ed25519 identity");

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .with_signing()
        .with_identity(&identity.private_key_hex)
        .build()
        .await
        .unwrap();

    for i in 0..2 {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{} listener", i));
        cluster
            .client(i)
            .schema_add(SCHEMA)
            .unwrap_or_else(|_| panic!("schema node{}", i));
    }

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let addr1 = extract_p2p_addr(&cluster, 1);

    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator");

    node0
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create ed25519-signed John");

    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            r["Users"]
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
        "ed25519-signed doc did not replicate",
    )
    .await;
}

/// Port: TestDocSignature_WithPeersAnDifferentKeyTypes_ShouldSync
/// Signed docs sync between peers with different key types.
#[tokio::test]
#[serial]
async fn peers_different_key_types_sync() {
    let binary = &integration_test::rust_binary();
    let id0 = generate_identity(binary).expect("generate secp256k1 identity");
    let id1 = generate_ed25519_identity(binary).expect("generate ed25519 identity");

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .with_signing()
        .with_node_identity(0, &id0.private_key_hex)
        .with_node_identity(1, &id1.private_key_hex)
        .build()
        .await
        .unwrap();

    for i in 0..2 {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{} listener", i));
        cluster
            .client(i)
            .schema_add(SCHEMA)
            .unwrap_or_else(|_| panic!("schema node{}", i));
    }

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let addr1 = extract_p2p_addr(&cluster, 1);

    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator 0→1");

    // Node0 creates a doc (secp256k1-signed)
    node0
        .query(r#"mutation { add_Users(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create Alice on node0");

    // Wait for replication to node1
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|arr| arr.iter().any(|u| u["name"].as_str() == Some("Alice")))
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "Alice did not replicate to node1",
    )
    .await;

    // Query _commits on node1 to verify signature types
    let commits = node1
        .query(
            r#"query {
                _commits(filter: {fieldName: {_eq: "_C"}}) {
                    signature { type identity }
                }
            }"#,
        )
        .expect("_commits query on node1");

    let arr = commits["_commits"]
        .as_array()
        .expect("_commits should be array");
    assert!(!arr.is_empty(), "should have collection-level commits");

    // At least one commit should have ES256K signature (from node0's secp256k1 key)
    let has_es256k = arr
        .iter()
        .any(|c| c["signature"]["type"].as_str() == Some("ES256K"));
    assert!(has_es256k, "should have ES256K-signed commit from node0");
}

/// Port: TestDocSignature_WithPeersAnDifferentKeyTypesUpdatingSameDoc_ShouldSync
/// Different key types updating same doc sync correctly.
#[tokio::test]
#[serial]
async fn peers_different_key_types_same_doc_sync() {
    let binary = &integration_test::rust_binary();
    let id0 = generate_identity(binary).expect("generate secp256k1 identity");
    let id1 = generate_ed25519_identity(binary).expect("generate ed25519 identity");

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .with_signing()
        .with_node_identity(0, &id0.private_key_hex)
        .with_node_identity(1, &id1.private_key_hex)
        .build()
        .await
        .unwrap();

    for i in 0..2 {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{} listener", i));
        cluster
            .client(i)
            .schema_add(SCHEMA)
            .unwrap_or_else(|_| panic!("schema node{}", i));
    }

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let addr0 = extract_p2p_addr(&cluster, 0);
    let addr1 = extract_p2p_addr(&cluster, 1);

    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");

    // Bidirectional replication
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator 0→1");
    node1
        .p2p_replicator_set(&["Users"], &addr0)
        .expect("replicator 1→0");

    // Node0 creates a doc (secp256k1)
    let data = node0
        .query(r#"mutation { add_Users(input: {name: "Bob", age: 25}) { _docID } }"#)
        .expect("create Bob on node0");
    let doc_id = extract_doc_id(&data, "add_Users");

    // Wait for replication to node1
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|arr| arr.iter().any(|u| u["name"].as_str() == Some("Bob")))
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "Bob did not replicate to node1",
    )
    .await;

    // Node1 updates the doc (ed25519)
    node1
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 26}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update Bob on node1");

    // Wait for update to replicate back to node0
    let node0_ref = &node0;
    poll_until(
        || {
            let r = node0_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|arr| arr.iter().any(|u| u["age"].as_i64() == Some(26)))
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "node1 update did not replicate to node0",
    )
    .await;

    // Node0 updates the doc again (secp256k1)
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 27}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update Bob on node0");

    // Wait for final update to replicate to node1
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|arr| arr.iter().any(|u| u["age"].as_i64() == Some(27)))
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "node0 update did not replicate to node1",
    )
    .await;

    // Query _commits on node0 to verify both signature types present
    let commits = node0
        .query(
            r#"query {
                _commits(filter: {fieldName: {_eq: "_C"}}, order: {height: DESC}) {
                    signature { type identity }
                }
            }"#,
        )
        .expect("_commits query on node0");

    let arr = commits["_commits"]
        .as_array()
        .expect("_commits should be array");
    assert!(
        arr.len() >= 3,
        "should have at least 3 collection commits (create + 2 updates), got {}",
        arr.len()
    );

    let has_es256k = arr
        .iter()
        .any(|c| c["signature"]["type"].as_str() == Some("ES256K"));
    let has_eddsa = arr
        .iter()
        .any(|c| c["signature"]["type"].as_str() == Some("EdDSA"));
    assert!(has_es256k, "should have ES256K-signed commits from node0");
    assert!(has_eddsa, "should have EdDSA-signed commits from node1");
}

/// Port: TestSignature_WithBranchableCollection_ShouldSignCollectionBlocks
/// Branchable collection blocks get signed.
#[tokio::test]
#[serial]
async fn branchable_collection_signed() {
    let identity = generate_identity(&integration_test::rust_binary()).expect("generate identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_signing()
        .with_identity(&identity.private_key_hex)
        .build()
        .await
        .unwrap();

    let node = cluster.client(0);
    node.schema_add("type Users @branchable { name: String }")
        .expect("add branchable schema");

    node.query(r#"mutation { add_Users(input: {name: "John"}) { _docID } }"#)
        .expect("create doc in branchable collection");

    // Query all commits — branchable collections produce collection, composite, and field blocks
    let commits = node
        .query(
            r#"query {
                _commits {
                    fieldName
                    signature { type identity value }
                }
            }"#,
        )
        .expect("_commits query");

    let arr = commits["_commits"]
        .as_array()
        .expect("_commits should be array");

    // Verify all commits are signed
    for commit in arr {
        let sig = &commit["signature"];
        assert!(
            !sig.is_null(),
            "commit with fieldName={:?} should have signature",
            commit["fieldName"]
        );
        assert!(
            sig["type"].as_str().is_some_and(|t| !t.is_empty()),
            "signature type should not be empty for fieldName={:?}",
            commit["fieldName"]
        );
        assert!(
            sig["identity"].as_str().is_some_and(|i| !i.is_empty()),
            "signature identity should not be empty for fieldName={:?}",
            commit["fieldName"]
        );
        assert!(
            sig["value"].as_str().is_some_and(|v| !v.is_empty()),
            "signature value should not be empty for fieldName={:?}",
            commit["fieldName"]
        );
    }

    // For secp256k1, all signatures should be ES256K
    let all_es256k = arr
        .iter()
        .all(|c| c["signature"]["type"].as_str() == Some("ES256K"));
    assert!(
        all_es256k,
        "all commits in branchable collection should be ES256K-signed"
    );
}

/// Port: TestSignatureVerify_WithValidData_ShouldVerify
/// Signature verification with valid data succeeds (secp256k1).
#[tokio::test]
#[serial]
async fn verify_valid_data() {
    let identity = generate_identity(&integration_test::rust_binary()).expect("generate identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_signing()
        .with_identity(&identity.private_key_hex)
        .build()
        .await
        .unwrap();

    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("add schema");

    let data = node
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create signed doc");

    let doc_id = extract_doc_id(&data, "add_Users");
    let cid = first_commit_cid(&node, &doc_id);
    let pk = require_public_key(&identity);

    let result = node
        .block_verify_signature(&pk, &cid, None)
        .expect("block verify-signature should succeed for secp256k1");
    assert!(
        !result.is_empty(),
        "verify-signature should return a response"
    );
}

/// Port: TestSignatureVerify_WithDifferentKeyType_ShouldVerify
/// Signature verification with ed25519 key type succeeds.
#[tokio::test]
#[serial]
async fn verify_different_key_type() {
    let identity = generate_ed25519_identity(&integration_test::rust_binary())
        .expect("generate ed25519 identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_signing()
        .with_identity(&identity.private_key_hex)
        .build()
        .await
        .unwrap();

    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("add schema");

    let data = node
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create ed25519-signed doc");

    let doc_id = extract_doc_id(&data, "add_Users");
    let cid = first_commit_cid(&node, &doc_id);
    let pk = require_public_key(&identity);

    let result = node
        .block_verify_signature(&pk, &cid, Some("ed25519"))
        .expect("ed25519 block verify-signature should succeed");
    assert!(
        !result.is_empty(),
        "verify-signature should return a response"
    );
}

/// Port: TestSignatureVerify_WithWrongIdentity_ShouldError
/// Signature verification with wrong identity fails.
#[tokio::test]
#[serial]
async fn verify_wrong_identity_error() {
    let identity = generate_identity(&integration_test::rust_binary()).expect("generate identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_signing()
        .with_identity(&identity.private_key_hex)
        .build()
        .await
        .unwrap();

    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("add schema");

    let data = node
        .query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create signed doc");

    let doc_id = extract_doc_id(&data, "add_Users");
    let cid = first_commit_cid(&node, &doc_id);

    let wrong = generate_identity(node.binary_path()).expect("generate wrong identity");
    let wrong_pk = wrong
        .public_key_hex
        .expect("identity should have public_key_hex");

    let result = node.block_verify_signature(&wrong_pk, &cid, None);
    assert!(
        result.is_err(),
        "verify with wrong identity should fail, got: {:?}",
        result
    );
}

/// Port: TestSignatureVerify_WithWrongCid_ShouldError
/// Signature verification with wrong CID fails.
#[tokio::test]
#[serial]
async fn verify_wrong_cid_error() {
    let identity = generate_identity(&integration_test::rust_binary()).expect("generate identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_signing()
        .with_identity(&identity.private_key_hex)
        .build()
        .await
        .unwrap();

    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("add schema");

    let pk = require_public_key(&identity);

    node.query(r#"mutation { add_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create signed doc");

    let bogus_cid = "bafyreihymej6gbxq7qauy4tgt37di25uap2ahzq7z5d3ln3og5syo7rwxx";
    let result = node.block_verify_signature(&pk, bogus_cid, None);
    assert!(
        result.is_err(),
        "verify with wrong CID should fail, got: {:?}",
        result
    );
}
