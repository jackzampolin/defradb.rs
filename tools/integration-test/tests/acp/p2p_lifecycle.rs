//! ACP P2P lifecycle tests ported from Go DefraDB `dac/p2p`.
//!
//! Source families:
//! - `add_test.go`
//! - `update_test.go`
//! - `delete_test.go`
//! - `replicator_test.go`
//! - `subscribe_test.go`
//!
//! The remaining Go relationship-propagation P2P tests are intentionally not ported here:
//! Rust local ACP explicitly does not replicate document-actor relationships across nodes,
//! and that non-replication is already covered in `negative_p2p.rs`.

use std::time::Duration;

use integration_test::{
    extract_p2p_addr, generate_identity, poll_until, users_schema_with_policy, TestCluster,
    TestIdentity, USER_ACP_POLICY,
};

const P2P_TIMEOUT: Duration = Duration::from_secs(15);

fn extract_policy_id(value: &serde_json::Value) -> String {
    value["PolicyID"]
        .as_str()
        .or_else(|| value["policyID"].as_str())
        .expect("missing PolicyID")
        .to_string()
}

async fn wait_for_p2p(cluster: &TestCluster) {
    for index in 0..2 {
        cluster
            .wait_for_log(index, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{index} P2P listener did not start"));
    }
}

async fn setup_acp_p2p(cluster: &TestCluster) -> TestIdentity {
    wait_for_p2p(cluster).await;

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let owner = generate_identity(node0.binary_path()).expect("owner identity");

    let policy = node0
        .acp_policy_add(USER_ACP_POLICY, &owner.private_key_hex)
        .expect("add ACP policy on node0");
    let policy_id = extract_policy_id(&policy);

    node1
        .acp_policy_add(USER_ACP_POLICY, &owner.private_key_hex)
        .expect("add ACP policy on node1");

    let schema = users_schema_with_policy(&policy_id);
    node0
        .schema_add_with_identity(&schema, &owner.private_key_hex)
        .expect("add schema on node0");
    node1
        .schema_add_with_identity(&schema, &owner.private_key_hex)
        .expect("add schema on node1");

    owner
}

fn create_user(
    node: &integration_test::DefraClient,
    owner_key: &str,
    name: &str,
    age: i64,
) -> String {
    let result = node
        .query_with_identity(
            &format!(
                r#"mutation {{ add_User(input: {{name: "{name}", age: {age}}}) {{ _docID name age }} }}"#
            ),
            owner_key,
        )
        .expect("create User");

    result["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string()
}

fn update_user(node: &integration_test::DefraClient, owner_key: &str, doc_id: &str, name: &str) {
    node.query_with_identity(
        &format!(r#"mutation {{ update_User(docID: "{doc_id}", input: {{name: "{name}"}}) {{ _docID name }} }}"#),
        owner_key,
    )
    .expect("update User");
}

fn delete_user(node: &integration_test::DefraClient, owner_key: &str, doc_id: &str) {
    node.query_with_identity(
        &format!(r#"mutation {{ delete_User(docID: "{doc_id}") {{ _docID }} }}"#),
        owner_key,
    )
    .expect("delete User");
}

fn query_user_names(node: &integration_test::DefraClient, key: Option<&str>) -> Vec<String> {
    let result = match key {
        Some(key) => node
            .query_with_identity(r#"query { User { name } }"#, key)
            .expect("query User with identity"),
        None => node
            .query(r#"query { User { name } }"#)
            .expect("query User anonymously"),
    };

    result["User"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| value["name"].as_str().map(ToString::to_string))
        .collect()
}

fn configure_subscription(cluster: &TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let addr1 = extract_p2p_addr(cluster, 1);

    node0
        .p2p_connect(&[&addr1])
        .expect("connect node0 -> node1");
    node0
        .p2p_collection_add(&["User"])
        .expect("add User collection on node0");
    node1
        .p2p_collection_add(&["User"])
        .expect("add User collection on node1");
}

fn configure_replicator(cluster: &TestCluster, owner_key: &str) {
    let node0 = cluster.client(0);
    let addr1 = extract_p2p_addr(cluster, 1);

    configure_subscription(cluster);
    node0
        .p2p_replicator_set_with_identity(&["User"], &addr1, owner_key)
        .expect("configure explicit replicator");
}

async fn acp_p2p_add_private_documents_on_different_nodes_test(cluster: TestCluster) {
    let owner = setup_acp_p2p(&cluster).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    create_user(&node0, &owner.private_key_hex, "Shahzad", 27);
    create_user(&node1, &owner.private_key_hex, "Shahzad Lone", 28);

    assert_eq!(
        query_user_names(&node0, Some(&owner.private_key_hex)),
        vec!["Shahzad".to_string()]
    );
    assert_eq!(
        query_user_names(&node1, Some(&owner.private_key_hex)),
        vec!["Shahzad Lone".to_string()]
    );
    assert!(query_user_names(&node0, None).is_empty());
    assert!(query_user_names(&node1, None).is_empty());
}

async fn acp_p2p_subscribe_add_get_single_with_permissioned_collection_test(cluster: TestCluster) {
    let owner = setup_acp_p2p(&cluster).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    configure_subscription(&cluster);

    let collections = node1
        .p2p_collection_list()
        .expect("list collections on node1");
    assert_eq!(
        collections.as_array().map(|arr| arr.len()).unwrap_or(0),
        1,
        "node1 should have exactly one subscribed P2P collection"
    );

    create_user(&node0, &owner.private_key_hex, "John", 21);

    assert_eq!(
        query_user_names(&node0, Some(&owner.private_key_hex)),
        vec!["John".to_string()]
    );

    // Go parity (acp/dac/p2p subscribe_test.go): a permissioned document does
    // NOT sync to a NON-replicator subscriber. Subscription delivers the head
    // via pubsub, but node1 must PULL the doc's blocks back from node0, and
    // node0's serve-side ACP gate denies that pull — node1 is a subscriber (not
    // a replicator, so the replicator bypass does not fire) and holds no reader
    // grant, so it resolves to an unauthorized/anonymous actor for a registered
    // private doc. Allow a settle window well beyond the replicator variant's
    // normal sync latency, then assert node1 never received the doc.
    tokio::time::sleep(Duration::from_secs(5)).await;

    assert!(
        query_user_names(&node1, Some(&owner.private_key_hex)).is_empty(),
        "permissioned doc must NOT reach a non-replicator subscriber, even under the owner identity on node1"
    );
    assert!(
        query_user_names(&node1, None).is_empty(),
        "permissioned doc must NOT be visible to anonymous on the subscriber node"
    );

    // node0 is the creating node: the doc's owner is registered there, so Local
    // ACP gates it and anonymous sees nothing.
    assert!(query_user_names(&node0, None).is_empty());
}

async fn acp_p2p_one_to_one_replicator_with_permissioned_collection_test(cluster: TestCluster) {
    let owner = setup_acp_p2p(&cluster).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    configure_replicator(&cluster, &owner.private_key_hex);
    create_user(&node0, &owner.private_key_hex, "John", 21);

    let owner_key = owner.private_key_hex.clone();
    poll_until(
        || {
            query_user_names(&node1, Some(&owner_key))
                .iter()
                .any(|name| name == "John")
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "permissioned document did not replicate to node1",
    )
    .await;

    // node0 is the creating node: Local ACP gates the doc, anonymous sees nothing.
    assert!(query_user_names(&node0, None).is_empty());
    // Local ACP is node-local; the replicated doc is public on the peer (matches Go).
    assert!(query_user_names(&node1, None)
        .iter()
        .any(|name| name == "John"));
}

async fn acp_p2p_update_private_documents_on_different_nodes_test(cluster: TestCluster) {
    let owner = setup_acp_p2p(&cluster).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    configure_replicator(&cluster, &owner.private_key_hex);

    let node0_doc = create_user(&node0, &owner.private_key_hex, "Shahzad", 27);
    let node1_doc = create_user(&node1, &owner.private_key_hex, "Shahzad Lone", 28);

    let owner_key = owner.private_key_hex.clone();
    poll_until(
        || {
            query_user_names(&node1, Some(&owner_key))
                .iter()
                .any(|name| name == "Shahzad")
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "node0 document did not replicate before update",
    )
    .await;

    update_user(&node0, &owner.private_key_hex, &node0_doc, "ShahzadUpdated");
    update_user(
        &node1,
        &owner.private_key_hex,
        &node1_doc,
        "ShahzadLoneUpdated",
    );

    assert!(
        query_user_names(&node0, Some(&owner.private_key_hex))
            .iter()
            .any(|name| name == "ShahzadUpdated"),
        "node0 should expose its updated local document"
    );
    assert!(
        query_user_names(&node1, Some(&owner.private_key_hex))
            .iter()
            .any(|name| name == "ShahzadLoneUpdated"),
        "node1 should expose its updated local document"
    );
    // node0 created "Shahzad" (gated locally) and node1's doc never replicates back
    // (replicator is node0 -> node1 only), so anonymous on node0 sees nothing.
    assert!(query_user_names(&node0, None).is_empty());
    // Local ACP is node-local; node0's doc replicated to the peer is public there
    // (matches Go), so anonymous on node1 sees it. Poll for the updated value to
    // land (the update replicates after the create).
    poll_until(
        || {
            query_user_names(&node1, None)
                .iter()
                .any(|name| name == "ShahzadUpdated")
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "node0's updated doc did not replicate to the peer for anonymous to see",
    )
    .await;
    // node1's own doc ("ShahzadLoneUpdated") was created here and remains gated, so
    // anonymous does not see it.
    assert!(!query_user_names(&node1, None)
        .iter()
        .any(|name| name == "ShahzadLoneUpdated"));
}

async fn acp_p2p_delete_private_documents_on_different_nodes_test(cluster: TestCluster) {
    let owner = setup_acp_p2p(&cluster).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    configure_replicator(&cluster, &owner.private_key_hex);

    let node0_doc = create_user(&node0, &owner.private_key_hex, "Shahzad", 27);
    let node1_doc = create_user(&node1, &owner.private_key_hex, "Shahzad Lone", 28);

    let owner_key = owner.private_key_hex.clone();
    poll_until(
        || {
            query_user_names(&node1, Some(&owner_key))
                .iter()
                .any(|name| name == "Shahzad")
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "node0 document did not replicate before delete",
    )
    .await;

    delete_user(&node0, &owner.private_key_hex, &node0_doc);
    delete_user(&node1, &owner.private_key_hex, &node1_doc);

    poll_until(
        || {
            !query_user_names(&node0, Some(&owner_key))
                .iter()
                .any(|name| name == "Shahzad")
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "node0 document delete did not apply",
    )
    .await;

    poll_until(
        || {
            !query_user_names(&node1, Some(&owner_key))
                .iter()
                .any(|name| name == "Shahzad Lone")
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "node1 document delete did not apply",
    )
    .await;

    poll_until(
        || query_user_names(&node0, None).is_empty(),
        Duration::from_secs(15),
        Duration::from_millis(200),
        "node0 still exposed a deleted document anonymously",
    )
    .await;
    poll_until(
        || query_user_names(&node1, None).is_empty(),
        Duration::from_secs(15),
        Duration::from_millis(200),
        "node1 still exposed a deleted document anonymously",
    )
    .await;
}

#[tokio::test]
async fn rust_rust_acp_p2p_add_private_documents_on_different_nodes() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_add_private_documents_on_different_nodes_test(cluster).await;
}

/// Go does not carry owner DID in PushLog Creator field, so ACP
/// enforcement on the receiving node cannot work for Go-originated documents.
#[tokio::test]
#[ignore]
async fn go_go_acp_p2p_add_private_documents_on_different_nodes() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_add_private_documents_on_different_nodes_test(cluster).await;
}

/// Go does not carry owner DID in PushLog Creator field, so ACP
/// enforcement on the receiving node cannot work for Go-originated documents.
#[tokio::test]
#[ignore]
async fn go_rust_acp_p2p_add_private_documents_on_different_nodes() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_add_private_documents_on_different_nodes_test(cluster).await;
}

#[tokio::test]
async fn rust_rust_acp_p2p_subscribe_add_get_single_with_permissioned_collection() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_subscribe_add_get_single_with_permissioned_collection_test(cluster).await;
}

/// Go does not carry owner DID in PushLog Creator field, so ACP
/// enforcement on the receiving node cannot work for Go-originated documents.
#[tokio::test]
#[ignore]
async fn go_go_acp_p2p_subscribe_add_get_single_with_permissioned_collection() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_subscribe_add_get_single_with_permissioned_collection_test(cluster).await;
}

/// Go does not carry owner DID in PushLog Creator field, so ACP
/// enforcement on the receiving node cannot work for Go-originated documents.
#[tokio::test]
#[ignore]
async fn go_rust_acp_p2p_subscribe_add_get_single_with_permissioned_collection() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_subscribe_add_get_single_with_permissioned_collection_test(cluster).await;
}

#[tokio::test]
async fn rust_rust_acp_p2p_one_to_one_replicator_with_permissioned_collection() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_one_to_one_replicator_with_permissioned_collection_test(cluster).await;
}

/// Go does not carry owner DID in PushLog Creator field, so ACP
/// enforcement on the receiving node cannot work for Go-originated documents.
#[tokio::test]
#[ignore]
async fn go_go_acp_p2p_one_to_one_replicator_with_permissioned_collection() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_one_to_one_replicator_with_permissioned_collection_test(cluster).await;
}

/// Go does not carry owner DID in PushLog Creator field, so ACP
/// enforcement on the receiving node cannot work for Go-originated documents.
#[tokio::test]
#[ignore]
async fn go_rust_acp_p2p_one_to_one_replicator_with_permissioned_collection() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_one_to_one_replicator_with_permissioned_collection_test(cluster).await;
}

#[tokio::test]
async fn rust_rust_acp_p2p_update_private_documents_on_different_nodes() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_update_private_documents_on_different_nodes_test(cluster).await;
}

/// Go does not carry owner DID in PushLog Creator field, so ACP
/// enforcement on the receiving node cannot work for Go-originated documents.
#[tokio::test]
#[ignore]
async fn go_go_acp_p2p_update_private_documents_on_different_nodes() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_update_private_documents_on_different_nodes_test(cluster).await;
}

/// Go does not carry owner DID in PushLog Creator field, so ACP
/// enforcement on the receiving node cannot work for Go-originated documents.
#[tokio::test]
#[ignore]
async fn go_rust_acp_p2p_update_private_documents_on_different_nodes() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_update_private_documents_on_different_nodes_test(cluster).await;
}

#[tokio::test]
async fn rust_rust_acp_p2p_delete_private_documents_on_different_nodes() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_delete_private_documents_on_different_nodes_test(cluster).await;
}

/// Go does not carry owner DID in PushLog Creator field, so ACP
/// enforcement on the receiving node cannot work for Go-originated documents.
#[tokio::test]
#[ignore]
async fn go_go_acp_p2p_delete_private_documents_on_different_nodes() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_delete_private_documents_on_different_nodes_test(cluster).await;
}

/// Go does not carry owner DID in PushLog Creator field, so ACP
/// enforcement on the receiving node cannot work for Go-originated documents.
#[tokio::test]
#[ignore]
async fn go_rust_acp_p2p_delete_private_documents_on_different_nodes() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    acp_p2p_delete_private_documents_on_different_nodes_test(cluster).await;
}
