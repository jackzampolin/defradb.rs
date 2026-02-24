//! Iroh P2P encryption tests.
//!
//! Ported from Go: tests/integration/encryption/ (P2P-related)
//!
//! These tests verify that encrypted documents sync correctly between peers,
//! including key exchange, field-level encryption, and ACP integration.
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh -- schema::encryption

use std::time::Duration;

use integration_test::{
    extract_doc_id, extract_p2p_addr, open_merge_events_sse, wait_for_merge_events, TestCluster,
};
use serial_test::serial;

const SCHEMA: &str = "type Users { name: String  age: Int }";
const PCOUNTER_SCHEMA: &str = "type Users { name: String  age: Int @crdt(type: pcounter) }";
const INDEXED_SCHEMA: &str = "type Users { name: String  age: Int @index }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);
const MERGE_TIMEOUT: Duration = Duration::from_secs(15);

/// Set up 2-node replicated cluster for encryption tests.
async fn setup_encrypted_cluster(schema: &str) -> (TestCluster, String) {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .with_encryption()
        .build()
        .await
        .unwrap();

    for i in 0..2 {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{} P2P listener", i));
        cluster
            .client(i)
            .schema_add(schema)
            .unwrap_or_else(|_| panic!("schema node{}", i));
    }

    let addr1 = extract_p2p_addr(&cluster, 1);
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.p2p_connect(&[&addr1]).expect("connect");
    node0.p2p_collection_add(&["Users"]).expect("col add node0");
    node1.p2p_collection_add(&["Users"]).expect("col add node1");
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator");

    (cluster, addr1)
}

/// Assert a single user's name and age on a node.
fn assert_user(cluster: &TestCluster, node_index: usize, expected_name: &str, expected_age: i64) {
    let result = cluster
        .client(node_index)
        .query("query { Users { name age } }")
        .expect("query");
    let users = result["Users"].as_array().expect("Users not array");
    assert!(!users.is_empty(), "no users found on node {}", node_index);
    let user = &users[0];
    assert_eq!(user["name"].as_str(), Some(expected_name));
    assert_eq!(user["age"].as_i64(), Some(expected_age));
}

// ---------------------------------------------------------------------------
// Basic encryption replication tests
// ---------------------------------------------------------------------------

/// Port: TestDocEncryptionPeer_IfDocIsPublic_ShouldFetchKeyAndDecrypt
#[tokio::test]
#[serial]
async fn public_doc_fetch_key_decrypt() {
    let (cluster, _) = setup_encrypted_cluster(SCHEMA).await;
    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;

    cluster
        .client(0)
        .query(r#"mutation { create_Users(input: {name: "John", age: 21}, encrypt: true) { _docID } }"#)
        .expect("create encrypted doc");

    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();
    assert_user(&cluster, 1, "John", 21);
}

/// Port: TestDocEncryptionPeer_IfPublicDocHasEncryptedField_ShouldFetchKeyAndDecrypt
#[tokio::test]
#[serial]
async fn public_doc_encrypted_field_fetch_key_decrypt() {
    let (cluster, _) = setup_encrypted_cluster(SCHEMA).await;
    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;

    cluster
        .client(0)
        .query(
            r#"mutation { create_Users(input: {name: "John", age: 21}, encryptFields: ["age"]) { _docID } }"#,
        )
        .expect("create doc with encrypted field");

    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();
    assert_user(&cluster, 1, "John", 21);
}

/// Port: TestDocEncryptionPeer_IfEncryptedPublicDocHasEncryptedField_ShouldFetchKeysAndDecrypt
#[tokio::test]
#[serial]
#[ignore = "requires Orbis KMS for encryption key distribution across peers"]
async fn encrypted_public_doc_encrypted_field() {
    let (cluster, _) = setup_encrypted_cluster(SCHEMA).await;
    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;

    cluster
        .client(0)
        .query(
            r#"mutation { create_Users(input: {name: "John", age: 21}, encrypt: true, encryptFields: ["age"]) { _docID } }"#,
        )
        .expect("create encrypted doc with encrypted field");

    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();
    assert_user(&cluster, 1, "John", 21);
}

/// Port: TestDocEncryptionPeer_IfAllFieldsOfEncryptedPublicDocAreIndividuallyEncrypted_ShouldFetchKeysAndDecrypt
#[tokio::test]
#[serial]
async fn all_fields_individually_encrypted() {
    let (cluster, _) = setup_encrypted_cluster(SCHEMA).await;
    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;

    cluster
        .client(0)
        .query(
            r#"mutation { create_Users(input: {name: "John", age: 21}, encrypt: true, encryptFields: ["name","age"]) { _docID } }"#,
        )
        .expect("create encrypted doc with all fields encrypted");

    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();
    assert_user(&cluster, 1, "John", 21);
}

/// Port: TestDocEncryptionPeer_IfAllFieldsOfPublicDocAreIndividuallyEncrypted_ShouldFetchKeysAndDecrypt
#[tokio::test]
#[serial]
#[ignore = "requires Orbis KMS for encryption key distribution across peers"]
async fn all_fields_of_public_doc_individually_encrypted() {
    let (cluster, _) = setup_encrypted_cluster(SCHEMA).await;
    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;

    cluster
        .client(0)
        .query(
            r#"mutation { create_Users(input: {name: "John", age: 21}, encryptFields: ["name","age"]) { _docID } }"#,
        )
        .expect("create public doc with all fields encrypted");

    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();
    assert_user(&cluster, 1, "John", 21);
}

// ---------------------------------------------------------------------------
// PCounter CRDT encryption tests
// ---------------------------------------------------------------------------

/// Port: TestDocEncryptionPeer_WithUpdatesOnEncryptedDeltaBasedCRDTField_ShouldDecryptAndCorrectlyMerge
#[tokio::test]
#[serial]
async fn updates_encrypted_delta_crdt_field() {
    let (cluster, _) = setup_encrypted_cluster(PCOUNTER_SCHEMA).await;
    let node0 = cluster.client(0);
    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;

    // Create doc with encrypted age field (pcounter initial: 21)
    let result = node0
        .query(
            r#"mutation { create_Users(input: {name: "John", age: 21}, encryptFields: ["age"]) { _docID } }"#,
        )
        .expect("create");
    let doc_id = extract_doc_id(&result, "create_Users");

    // Update age by 3 (pcounter: 21 + 3 = 24)
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 3}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update +3");

    // Update age by 2 (pcounter: 24 + 2 = 26)
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 2}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update +2");

    // Wait for all 3 merges (create + 2 updates)
    wait_for_merge_events(&merges, 3, MERGE_TIMEOUT).await;
    sse.abort();
    assert_user(&cluster, 1, "John", 26);
}

/// Port: TestDocEncryptionPeer_WithUpdatesOnDeltaBasedCRDTFieldOfEncryptedDoc_ShouldDecryptAndCorrectlyMerge
#[tokio::test]
#[serial]
async fn updates_delta_crdt_field_of_encrypted_doc() {
    let (cluster, _) = setup_encrypted_cluster(PCOUNTER_SCHEMA).await;
    let node0 = cluster.client(0);
    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;

    // Create fully encrypted doc (pcounter initial: 21)
    let result = node0
        .query(
            r#"mutation { create_Users(input: {name: "John", age: 21}, encrypt: true) { _docID } }"#,
        )
        .expect("create");
    let doc_id = extract_doc_id(&result, "create_Users");

    // Update age by 3 (pcounter: 21 + 3 = 24)
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 3}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update +3");

    // Update age by 2 (pcounter: 24 + 2 = 26)
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{age: 2}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update +2");

    // Wait for all 3 merges (create + 2 updates)
    wait_for_merge_events(&merges, 3, MERGE_TIMEOUT).await;
    sse.abort();
    assert_user(&cluster, 1, "John", 26);
}

// ---------------------------------------------------------------------------
// Update edge cases
// ---------------------------------------------------------------------------

/// Port: TestDocEncryptionPeer_WithUpdatesThatSetsEmptyString_ShouldDecryptAndCorrectlyMerge
#[tokio::test]
#[serial]
async fn updates_set_empty_string() {
    let (cluster, _) = setup_encrypted_cluster(SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;

    // Create encrypted doc
    let result = node0
        .query(
            r#"mutation { create_Users(input: {name: "John", age: 21}, encrypt: true) { _docID } }"#,
        )
        .expect("create");
    let doc_id = extract_doc_id(&result, "create_Users");

    // Wait for initial replication
    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;

    // Update name to empty string
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{name: ""}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update to empty");

    // Wait for empty string to replicate
    wait_for_merge_events(&merges, 2, MERGE_TIMEOUT).await;

    let r = node1
        .query("query { Users { name age } }")
        .expect("query after empty");
    assert_eq!(r["Users"][0]["name"].as_str(), Some(""));

    // Update name back to "John"
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{name: "John"}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update back to John");

    // Wait for "John" to replicate
    wait_for_merge_events(&merges, 3, MERGE_TIMEOUT).await;
    sse.abort();
    assert_user(&cluster, 1, "John", 21);
}

/// Port: TestDocEncryptionPeer_WithUpdatesThatSetsStringToNull_ShouldDecryptAndCorrectlyMerge
#[tokio::test]
#[serial]
async fn updates_set_string_to_null() {
    let (cluster, _) = setup_encrypted_cluster(SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;

    // Create encrypted doc
    let result = node0
        .query(
            r#"mutation { create_Users(input: {name: "John", age: 21}, encrypt: true) { _docID } }"#,
        )
        .expect("create");
    let doc_id = extract_doc_id(&result, "create_Users");

    // Wait for initial replication
    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;

    // Update name to null
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{name: null}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update to null");

    // Wait for null to replicate
    wait_for_merge_events(&merges, 2, MERGE_TIMEOUT).await;

    let r = node1
        .query("query { Users { name age } }")
        .expect("query after null");
    assert!(r["Users"][0]["name"].is_null());

    // Update name back to "John"
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{name: "John"}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update back to John");

    // Wait for "John" to replicate
    wait_for_merge_events(&merges, 3, MERGE_TIMEOUT).await;
    sse.abort();
    assert_user(&cluster, 1, "John", 21);
}

// ---------------------------------------------------------------------------
// Encrypted DAG sync
// ---------------------------------------------------------------------------

/// Port: TestDocEncryptionPeer_UponSync_ShouldSyncEncryptedDAG
#[tokio::test]
#[serial]
async fn sync_encrypted_dag() {
    let (cluster, _) = setup_encrypted_cluster(SCHEMA).await;
    let node0 = cluster.client(0);
    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;

    // Create encrypted doc
    let result = node0
        .query(
            r#"mutation { create_Users(input: {name: "John", age: 21}, encrypt: true) { _docID } }"#,
        )
        .expect("create");
    let doc_id = extract_doc_id(&result, "create_Users");

    // Wait for replication
    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();

    // Query _commits on node1 to verify encrypted DAG synced
    let commits_query = format!(
        r#"query {{ _commits(docID: "{}") {{ cid delta fieldName }} }}"#,
        doc_id
    );
    let commits = cluster
        .client(1)
        .query(&commits_query)
        .expect("commits query");
    let commit_arr = commits["_commits"].as_array().expect("_commits not array");

    // Should have at least 3 commits (composite, name field, age field)
    assert!(
        commit_arr.len() >= 3,
        "expected at least 3 commits, got {}",
        commit_arr.len()
    );

    // Verify each commit has a CID; field commits have deltas, composite (_C) may not
    for commit in commit_arr {
        assert!(
            commit["cid"].as_str().is_some(),
            "commit missing cid: {:?}",
            commit
        );
    }

    // Verify field-level commits have non-null deltas
    let field_commits: Vec<_> = commit_arr
        .iter()
        .filter(|c| c["fieldName"].as_str() != Some("_C"))
        .collect();
    assert!(!field_commits.is_empty(), "expected field-level commits");
    for commit in &field_commits {
        assert!(
            !commit["delta"].is_null(),
            "field commit missing delta: {:?}",
            commit
        );
    }
}

// ---------------------------------------------------------------------------
// Encrypted index tests
// ---------------------------------------------------------------------------

/// Port: TestDocEncryptionPeer_IfEncryptedDocHasIndexedField_ShouldIndexAfterDecryption
#[tokio::test]
#[serial]
async fn encrypted_doc_indexed_field() {
    let (cluster, _) = setup_encrypted_cluster(INDEXED_SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;

    // Create mix of encrypted and non-encrypted docs
    node0
        .query(r#"mutation { create_Users(input: {name: "Shahzad", age: 25}) { _docID } }"#)
        .expect("create Shahzad");
    node0
        .query(
            r#"mutation { create_Users(input: {name: "Islam", age: 33}, encrypt: true) { _docID } }"#,
        )
        .expect("create Islam encrypted");
    node0
        .query(r#"mutation { create_Users(input: {name: "Andy", age: 21}) { _docID } }"#)
        .expect("create Andy");
    node0
        .query(
            r#"mutation { create_Users(input: {name: "John", age: 21}, encrypt: true) { _docID } }"#,
        )
        .expect("create John encrypted");

    // Wait for all 4 docs to replicate
    wait_for_merge_events(&merges, 4, MERGE_TIMEOUT).await;
    sse.abort();

    // Query with filter on indexed field
    let result = node1
        .query(r#"query { Users(filter: {age: {_eq: 21}}) { name } }"#)
        .expect("filtered query");
    let users = result["Users"].as_array().expect("not array");
    assert_eq!(
        users.len(),
        2,
        "expected 2 users with age=21, got {:?}",
        users
    );

    let names: Vec<&str> = users.iter().filter_map(|u| u["name"].as_str()).collect();
    assert!(names.contains(&"Andy"), "missing Andy in {:?}", names);
    assert!(names.contains(&"John"), "missing John in {:?}", names);
}

/// Port: TestDocEncryptionPeer_IfDocDocHasEncryptedIndexedField_ShouldIndexAfterDecryption
#[tokio::test]
#[serial]
async fn doc_encrypted_indexed_field() {
    let (cluster, _) = setup_encrypted_cluster(INDEXED_SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;

    // Create docs with encrypted indexed fields
    node0
        .query(r#"mutation { create_Users(input: {name: "Shahzad", age: 25}) { _docID } }"#)
        .expect("create Shahzad");
    node0
        .query(
            r#"mutation { create_Users(input: {name: "Islam", age: 33}, encryptFields: ["age"]) { _docID } }"#,
        )
        .expect("create Islam with encrypted age");
    node0
        .query(r#"mutation { create_Users(input: {name: "Andy", age: 21}) { _docID } }"#)
        .expect("create Andy");
    node0
        .query(
            r#"mutation { create_Users(input: {name: "John", age: 21}, encryptFields: ["age"]) { _docID } }"#,
        )
        .expect("create John with encrypted age");

    // Wait for all 4 docs to replicate
    wait_for_merge_events(&merges, 4, MERGE_TIMEOUT).await;
    sse.abort();

    // Query with filter on encrypted indexed field
    let result = node1
        .query(r#"query { Users(filter: {age: {_eq: 21}}) { name } }"#)
        .expect("filtered query");
    let users = result["Users"].as_array().expect("not array");
    assert_eq!(
        users.len(),
        2,
        "expected 2 users with age=21, got {:?}",
        users
    );

    let names: Vec<&str> = users.iter().filter_map(|u| u["name"].as_str()).collect();
    assert!(names.contains(&"Andy"), "missing Andy in {:?}", names);
    assert!(names.contains(&"John"), "missing John in {:?}", names);
}

// ---------------------------------------------------------------------------
// KMS-specific test (kept ignored — Rust uses shared hardcoded key)
// ---------------------------------------------------------------------------

/// Port: TestDocEncryptionPeer_IfPeerDidNotReceiveKey_ShouldNotFetch
#[tokio::test]
#[serial]
#[ignore]
async fn peer_no_key_should_not_fetch() {
    // Rust uses a shared hardcoded encryption key, so the key-not-received
    // scenario cannot be reproduced without KMS infrastructure.
}

// ---------------------------------------------------------------------------
// ACP encryption tests (kept ignored — require SourceHub DAC + encryption)
// ---------------------------------------------------------------------------

/// Port: TestDocEncryptionACP_IfUserAndNodeHaveAccess_ShouldFetch
#[tokio::test]
#[serial]
#[ignore]
async fn encryption_acp_user_and_node_access() {
    // Requires SourceHub DAC policy + KMS integration
}

/// Port: TestDocEncryptionACP_IfUserHasAccessButNotNode_ShouldNotFetch
#[tokio::test]
#[serial]
#[ignore]
async fn encryption_acp_user_access_not_node() {
    // Requires SourceHub DAC policy + KMS integration
}

/// Port: TestDocEncryptionACP_IfNodeHasAccessToSomeDocs_ShouldFetchOnlyThem
#[tokio::test]
#[serial]
#[ignore]
async fn encryption_acp_node_partial_access() {
    // Requires SourceHub DAC policy + KMS integration
}

/// Port: TestDocEncryptionACP_IfClientNodeHasDocPermissionButServerNodeIsNotAvailable_ShouldNotFetch
#[tokio::test]
#[serial]
#[ignore]
async fn encryption_acp_server_not_available() {
    // Requires SourceHub DAC policy + KMS + server availability testing
}
