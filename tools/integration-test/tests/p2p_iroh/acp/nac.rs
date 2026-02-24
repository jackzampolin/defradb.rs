//! Iroh P2P NAC (Node Access Control) gate tests.
//!
//! Ported from Go: tests/integration/acp/nac/ (P2P-related)
//!
//! Each P2P operation is tested with 3 variants:
//! - Authorized identity (admin) allows access
//! - No identity returns not-authorized error
//! - Wrong identity returns not-authorized error
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh_acp_nac -- --ignored

use std::time::Duration;

use integration_test::{extract_p2p_addr_with_identity, generate_identity, TestCluster};
use serial_test::serial;

const SCHEMA: &str = "type Users { name: String  age: Int }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);

/// Set up a single-node iroh cluster with NAC enabled.
/// Returns (cluster, admin_key, outsider_key).
async fn setup_nac_node() -> (TestCluster, String, String) {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .with_nac()
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("P2P listener did not start");

    let admin_key = cluster
        .startup_identity()
        .expect("NAC cluster must have startup identity")
        .to_string();

    let node = cluster.client(0);
    node.schema_add_with_identity(SCHEMA, &admin_key)
        .expect("deploy schema");

    let binary = node.binary_path().to_path_buf();
    let outsider = generate_identity(&binary).expect("generate outsider identity");
    let outsider_key = outsider.private_key_hex;

    (cluster, admin_key, outsider_key)
}

/// Set up a single NAC node and also return collection_id + version_id.
/// The schema_add return value has these fields for the Rust node.
/// Returns (cluster, admin_key, outsider_key, collection_id, version_id).
async fn setup_nac_node_with_schema_info() -> (TestCluster, String, String, String, String) {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .with_nac()
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("P2P listener did not start");

    let admin_key = cluster
        .startup_identity()
        .expect("NAC cluster must have startup identity")
        .to_string();

    let node = cluster.client(0);
    let schema_result = node
        .schema_add_with_identity(SCHEMA, &admin_key)
        .expect("deploy schema");

    // schema_add returns [{CollectionID, VersionID, ...}] for Rust nodes
    let col_id = schema_result
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.get("CollectionID"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let ver_id = schema_result
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.get("VersionID"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let binary = node.binary_path().to_path_buf();
    let outsider = generate_identity(&binary).expect("generate outsider identity");
    let outsider_key = outsider.private_key_hex;

    (cluster, admin_key, outsider_key, col_id, ver_id)
}

/// Set up a 2-node iroh cluster with NAC enabled.
/// Returns (cluster, admin_key, outsider_key).
async fn setup_nac_two_nodes() -> (TestCluster, String, String) {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_acp_local()
        .with_nac()
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    for i in 0..2 {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{} P2P listener did not start", i));
    }

    let admin_key = cluster
        .startup_identity()
        .expect("NAC cluster must have startup identity")
        .to_string();

    for i in 0..2 {
        cluster
            .client(i)
            .schema_add_with_identity(SCHEMA, &admin_key)
            .unwrap_or_else(|_| panic!("deploy schema node{}", i));
    }

    let binary = cluster.client(0).binary_path().to_path_buf();
    let outsider = generate_identity(&binary).expect("generate outsider identity");
    let outsider_key = outsider.private_key_hex;

    (cluster, admin_key, outsider_key)
}

// --- P2P Peer Info ---

/// Port: TestNAC_GatesP2PPeerInfo_AuthorizedIdentity_AllowAccess
#[tokio::test]
#[serial]
async fn nac_peer_info_authorized() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_info_with_identity(&admin_key);
    assert!(result.is_ok(), "admin should pass NAC gate for p2p_info");
}

/// Port: TestNAC_GatesP2PPeerInfo_NoIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_peer_info_no_identity() {
    let (cluster, _, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_info();
    assert!(
        result.is_err(),
        "anonymous should be rejected by NAC for p2p_info"
    );
}

/// Port: TestNAC_GatesP2PPeerInfo_WrongIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_peer_info_wrong_identity() {
    let (cluster, _, outsider_key) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_info_with_identity(&outsider_key);
    assert!(
        result.is_err(),
        "outsider should be rejected by NAC for p2p_info"
    );
}

// --- P2P Peer Connect ---

/// Port: TestNAC_GatesP2PPeerConnect_AuthorizedIdentity_AllowAccess
#[tokio::test]
#[serial]
async fn nac_peer_connect_authorized() {
    let (cluster, admin_key, _) = setup_nac_two_nodes().await;
    let addr1 = extract_p2p_addr_with_identity(&cluster, 1, &admin_key);
    let node = cluster.client(0);
    let result = node.p2p_connect_with_identity(&[&addr1], &admin_key);
    assert!(result.is_ok(), "admin should pass NAC gate for p2p_connect");
}

/// Port: TestNAC_GatesP2PPeerConnect_NoIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_peer_connect_no_identity() {
    let (cluster, admin_key, _) = setup_nac_two_nodes().await;
    let addr1 = extract_p2p_addr_with_identity(&cluster, 1, &admin_key);
    let node = cluster.client(0);
    let result = node.p2p_connect(&[&addr1]);
    assert!(
        result.is_err(),
        "anonymous should be rejected by NAC for p2p_connect"
    );
}

/// Port: TestNAC_GatesP2PPeerConnect_WrongIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_peer_connect_wrong_identity() {
    let (cluster, admin_key, outsider_key) = setup_nac_two_nodes().await;
    let addr1 = extract_p2p_addr_with_identity(&cluster, 1, &admin_key);
    let node = cluster.client(0);
    let result = node.p2p_connect_with_identity(&[&addr1], &outsider_key);
    assert!(
        result.is_err(),
        "outsider should be rejected by NAC for p2p_connect"
    );
}

// --- Active Peers ---

/// Port: TestNAC_GatesActivePeers_AuthorizedIdentity_AllowAccess
#[tokio::test]
#[serial]
async fn nac_active_peers_authorized() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_active_peers_with_identity(&admin_key);
    assert!(
        result.is_ok(),
        "admin should pass NAC gate for active_peers"
    );
}

/// Port: TestNAC_GatesActivePeers_NoIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_active_peers_no_identity() {
    let (cluster, _, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_active_peers();
    assert!(
        result.is_err(),
        "anonymous should be rejected by NAC for active_peers"
    );
}

/// Port: TestNAC_GatesActivePeers_WrongIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_active_peers_wrong_identity() {
    let (cluster, _, outsider_key) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_active_peers_with_identity(&outsider_key);
    assert!(
        result.is_err(),
        "outsider should be rejected by NAC for active_peers"
    );
}

// --- Collection Add ---

/// Port: TestNAC_GatesP2PCollectionAdd_AuthorizedIdentity_AllowAccess
#[tokio::test]
#[serial]
async fn nac_collection_add_authorized() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_collection_add_with_identity(&["Users"], &admin_key);
    assert!(
        result.is_ok(),
        "admin should pass NAC gate for collection_add"
    );
}

/// Port: TestNAC_GatesP2PCollectionAdd_NoIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_collection_add_no_identity() {
    let (cluster, _, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_collection_add(&["Users"]);
    assert!(
        result.is_err(),
        "anonymous should be rejected by NAC for collection_add"
    );
}

/// Port: TestNAC_GatesP2PCollectionAdd_WrongIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_collection_add_wrong_identity() {
    let (cluster, _, outsider_key) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_collection_add_with_identity(&["Users"], &outsider_key);
    assert!(
        result.is_err(),
        "outsider should be rejected by NAC for collection_add"
    );
}

// --- Collection List ---

/// Port: TestNAC_GatesP2PCollectionList_AuthorizedIdentity_AllowAccess
#[tokio::test]
#[serial]
async fn nac_collection_list_authorized() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_collection_list_with_identity(&admin_key);
    assert!(
        result.is_ok(),
        "admin should pass NAC gate for collection_list"
    );
}

/// Port: TestNAC_GatesP2PCollectionList_NoIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_collection_list_no_identity() {
    let (cluster, _, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_collection_list();
    assert!(
        result.is_err(),
        "anonymous should be rejected by NAC for collection_list"
    );
}

/// Port: TestNAC_GatesP2PCollectionList_WrongIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_collection_list_wrong_identity() {
    let (cluster, _, outsider_key) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_collection_list_with_identity(&outsider_key);
    assert!(
        result.is_err(),
        "outsider should be rejected by NAC for collection_list"
    );
}

// --- Collection Delete ---

/// Port: TestNAC_GatesP2PCollectionDelete_AuthorizedIdentity_AllowAccess
#[tokio::test]
#[serial]
async fn nac_collection_delete_authorized() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    // Add first so we can delete
    node.p2p_collection_add_with_identity(&["Users"], &admin_key)
        .expect("add");
    let result = node.p2p_collection_delete_with_identity(&["Users"], &admin_key);
    assert!(
        result.is_ok(),
        "admin should pass NAC gate for collection_delete"
    );
}

/// Port: TestNAC_GatesP2PCollectionDelete_NoIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_collection_delete_no_identity() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    node.p2p_collection_add_with_identity(&["Users"], &admin_key)
        .expect("add");
    let result = node.p2p_collection_delete(&["Users"]);
    assert!(
        result.is_err(),
        "anonymous should be rejected by NAC for collection_delete"
    );
}

/// Port: TestNAC_GatesP2PCollectionDelete_WrongIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_collection_delete_wrong_identity() {
    let (cluster, admin_key, outsider_key) = setup_nac_node().await;
    let node = cluster.client(0);
    node.p2p_collection_add_with_identity(&["Users"], &admin_key)
        .expect("add");
    let result = node.p2p_collection_delete_with_identity(&["Users"], &outsider_key);
    assert!(
        result.is_err(),
        "outsider should be rejected by NAC for collection_delete"
    );
}

// --- Document Add ---

/// Port: TestNAC_GatesP2PDocumentAdd_AuthorizedIdentity_AllowAccess
#[tokio::test]
#[serial]
async fn nac_document_add_authorized() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let r = node
        .query_with_identity(
            r#"mutation { create_Users(input: {name: "Alice", age: 30}) { _docID } }"#,
            &admin_key,
        )
        .expect("create doc");
    let doc_id = r["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID");
    let result = node.p2p_document_add_with_identity(&[doc_id], &admin_key);
    assert!(
        result.is_ok(),
        "admin should pass NAC gate for document_add"
    );
}

/// Port: TestNAC_GatesP2PDocumentAdd_NoIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_document_add_no_identity() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let r = node
        .query_with_identity(
            r#"mutation { create_Users(input: {name: "Alice", age: 30}) { _docID } }"#,
            &admin_key,
        )
        .expect("create doc");
    let doc_id = r["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID");
    let result = node.p2p_document_add(&[doc_id]);
    assert!(
        result.is_err(),
        "anonymous should be rejected by NAC for document_add"
    );
}

/// Port: TestNAC_GatesP2PDocumentAdd_WrongIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_document_add_wrong_identity() {
    let (cluster, admin_key, outsider_key) = setup_nac_node().await;
    let node = cluster.client(0);
    let r = node
        .query_with_identity(
            r#"mutation { create_Users(input: {name: "Alice", age: 30}) { _docID } }"#,
            &admin_key,
        )
        .expect("create doc");
    let doc_id = r["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID");
    let result = node.p2p_document_add_with_identity(&[doc_id], &outsider_key);
    assert!(
        result.is_err(),
        "outsider should be rejected by NAC for document_add"
    );
}

// --- Document List ---

/// Port: TestNAC_GatesP2PDocumentList_AuthorizedIdentity_AllowAccess
#[tokio::test]
#[serial]
async fn nac_document_list_authorized() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_document_list_with_identity(&admin_key);
    assert!(
        result.is_ok(),
        "admin should pass NAC gate for document_list"
    );
}

/// Port: TestNAC_GatesP2PDocumentList_NoIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_document_list_no_identity() {
    let (cluster, _, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_document_list();
    assert!(
        result.is_err(),
        "anonymous should be rejected by NAC for document_list"
    );
}

/// Port: TestNAC_GatesP2PDocumentList_WrongIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_document_list_wrong_identity() {
    let (cluster, _, outsider_key) = setup_nac_node().await;
    let node = cluster.client(0);
    let result = node.p2p_document_list_with_identity(&outsider_key);
    assert!(
        result.is_err(),
        "outsider should be rejected by NAC for document_list"
    );
}

// --- Document Delete ---

/// Port: TestNAC_GatesP2PDocumentDelete_AuthorizedIdentity_AllowAccess
#[tokio::test]
#[serial]
async fn nac_document_delete_authorized() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let r = node
        .query_with_identity(
            r#"mutation { create_Users(input: {name: "Alice", age: 30}) { _docID } }"#,
            &admin_key,
        )
        .expect("create doc");
    let doc_id = r["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID");
    node.p2p_document_add_with_identity(&[doc_id], &admin_key)
        .expect("add doc");
    let result = node.p2p_document_delete_with_identity(&[doc_id], &admin_key);
    assert!(
        result.is_ok(),
        "admin should pass NAC gate for document_delete"
    );
}

/// Port: TestNAC_GatesP2PDocumentDelete_NoIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_document_delete_no_identity() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let r = node
        .query_with_identity(
            r#"mutation { create_Users(input: {name: "Alice", age: 30}) { _docID } }"#,
            &admin_key,
        )
        .expect("create doc");
    let doc_id = r["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID");
    node.p2p_document_add_with_identity(&[doc_id], &admin_key)
        .expect("add doc");
    let result = node.p2p_document_delete(&[doc_id]);
    assert!(
        result.is_err(),
        "anonymous should be rejected by NAC for document_delete"
    );
}

/// Port: TestNAC_GatesP2PDocumentDelete_WrongIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_document_delete_wrong_identity() {
    let (cluster, admin_key, outsider_key) = setup_nac_node().await;
    let node = cluster.client(0);
    let r = node
        .query_with_identity(
            r#"mutation { create_Users(input: {name: "Alice", age: 30}) { _docID } }"#,
            &admin_key,
        )
        .expect("create doc");
    let doc_id = r["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID");
    node.p2p_document_add_with_identity(&[doc_id], &admin_key)
        .expect("add doc");
    let result = node.p2p_document_delete_with_identity(&[doc_id], &outsider_key);
    assert!(
        result.is_err(),
        "outsider should be rejected by NAC for document_delete"
    );
}

// --- Replicator Add ---

/// Port: TestNAC_GatesP2PReplicatorAdd_AuthorizedIdentity_AllowAccess
#[tokio::test]
#[serial]
async fn nac_replicator_add_authorized() {
    let (cluster, admin_key, _) = setup_nac_two_nodes().await;
    let addr1 = extract_p2p_addr_with_identity(&cluster, 1, &admin_key);
    let node = cluster.client(0);
    node.p2p_collection_add_with_identity(&["Users"], &admin_key)
        .expect("add collection");
    let result = node.p2p_replicator_set_with_identity(&["Users"], &addr1, &admin_key);
    assert!(
        result.is_ok(),
        "admin should pass NAC gate for replicator_add"
    );
}

/// Port: TestNAC_GatesP2PReplicatorAdd_NoIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_replicator_add_no_identity() {
    let (cluster, admin_key, _) = setup_nac_two_nodes().await;
    let addr1 = extract_p2p_addr_with_identity(&cluster, 1, &admin_key);
    let node = cluster.client(0);
    node.p2p_collection_add_with_identity(&["Users"], &admin_key)
        .expect("add collection");
    let result = node.p2p_replicator_set(&["Users"], &addr1);
    assert!(
        result.is_err(),
        "anonymous should be rejected by NAC for replicator_add"
    );
}

/// Port: TestNAC_GatesP2PReplicatorAdd_WrongIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_replicator_add_wrong_identity() {
    let (cluster, admin_key, outsider_key) = setup_nac_two_nodes().await;
    let addr1 = extract_p2p_addr_with_identity(&cluster, 1, &admin_key);
    let node = cluster.client(0);
    node.p2p_collection_add_with_identity(&["Users"], &admin_key)
        .expect("add collection");
    let result = node.p2p_replicator_set_with_identity(&["Users"], &addr1, &outsider_key);
    assert!(
        result.is_err(),
        "outsider should be rejected by NAC for replicator_add"
    );
}

// --- Replicator Delete ---

/// Port: TestNAC_GatesP2PReplicatorDelete_AuthorizedIdentity_AllowAccess
#[tokio::test]
#[serial]
async fn nac_replicator_delete_authorized() {
    let (cluster, admin_key, _) = setup_nac_two_nodes().await;
    let addr1 = extract_p2p_addr_with_identity(&cluster, 1, &admin_key);
    let node = cluster.client(0);
    node.p2p_collection_add_with_identity(&["Users"], &admin_key)
        .expect("add collection");
    node.p2p_replicator_set_with_identity(&["Users"], &addr1, &admin_key)
        .expect("add replicator");
    let result = node.p2p_replicator_delete_with_identity(&["Users"], Some(&addr1), &admin_key);
    assert!(
        result.is_ok(),
        "admin should pass NAC gate for replicator_delete"
    );
}

/// Port: TestNAC_GatesP2PReplicatorDelete_NoIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_replicator_delete_no_identity() {
    let (cluster, admin_key, _) = setup_nac_two_nodes().await;
    let addr1 = extract_p2p_addr_with_identity(&cluster, 1, &admin_key);
    let node = cluster.client(0);
    node.p2p_collection_add_with_identity(&["Users"], &admin_key)
        .expect("add collection");
    node.p2p_replicator_set_with_identity(&["Users"], &addr1, &admin_key)
        .expect("add replicator");
    let result = node.p2p_replicator_delete(&["Users"], Some(&addr1));
    assert!(
        result.is_err(),
        "anonymous should be rejected by NAC for replicator_delete"
    );
}

/// Port: TestNAC_GatesP2PReplicatorDelete_WrongIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_replicator_delete_wrong_identity() {
    let (cluster, admin_key, outsider_key) = setup_nac_two_nodes().await;
    let addr1 = extract_p2p_addr_with_identity(&cluster, 1, &admin_key);
    let node = cluster.client(0);
    node.p2p_collection_add_with_identity(&["Users"], &admin_key)
        .expect("add collection");
    node.p2p_replicator_set_with_identity(&["Users"], &addr1, &admin_key)
        .expect("add replicator");
    let result = node.p2p_replicator_delete_with_identity(&["Users"], Some(&addr1), &outsider_key);
    assert!(
        result.is_err(),
        "outsider should be rejected by NAC for replicator_delete"
    );
}

// --- Sync Branchable Collection ---

/// Port: TestNAC_GatesSyncBranchableCollection_AuthorizedIdentity_AllowAccess
#[tokio::test]
#[serial]
async fn nac_sync_branchable_authorized() {
    let (cluster, admin_key, _, col_id, _) = setup_nac_node_with_schema_info().await;
    let node = cluster.client(0);
    // May fail due to no peers, but should pass NAC gate
    let result = node.p2p_collection_sync_branchable_with_identity(&col_id, &admin_key);
    // Success or non-NAC error both mean NAC gate passed
    if let Err(ref e) = result {
        let msg = e.to_string().to_lowercase();
        assert!(
            !msg.contains("unauthorized") && !msg.contains("forbidden") && !msg.contains("nac"),
            "admin should pass NAC gate, got: {}",
            msg
        );
    }
}

/// Port: TestNAC_GatesSyncBranchableCollection_NoIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_sync_branchable_no_identity() {
    let (cluster, _, _, col_id, _) = setup_nac_node_with_schema_info().await;
    let node = cluster.client(0);
    let result = node.p2p_collection_sync_branchable(&col_id);
    assert!(
        result.is_err(),
        "anonymous should be rejected by NAC for sync_branchable"
    );
}

/// Port: TestNAC_GatesSyncBranchableCollection_WrongIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_sync_branchable_wrong_identity() {
    let (cluster, _, outsider_key, col_id, _) = setup_nac_node_with_schema_info().await;
    let node = cluster.client(0);
    let result = node.p2p_collection_sync_branchable_with_identity(&col_id, &outsider_key);
    assert!(
        result.is_err(),
        "outsider should be rejected by NAC for sync_branchable"
    );
}

// --- Sync Collection Versions ---

/// Port: TestNAC_GatesSyncCollectionVersions_AuthorizedIdentity_AllowAccess
#[tokio::test]
#[serial]
async fn nac_sync_versions_authorized() {
    let (cluster, admin_key, _, _, version_id) = setup_nac_node_with_schema_info().await;
    let node = cluster.client(0);
    let result = node.p2p_collection_sync_versions_with_identity(&[&version_id], &admin_key);
    // Success or non-NAC error means gate passed
    if let Err(ref e) = result {
        let msg = e.to_string().to_lowercase();
        assert!(
            !msg.contains("unauthorized") && !msg.contains("forbidden") && !msg.contains("nac"),
            "admin should pass NAC gate, got: {}",
            msg
        );
    }
}

/// Port: TestNAC_GatesSyncCollectionVersions_NoIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_sync_versions_no_identity() {
    let (cluster, _, _, _, version_id) = setup_nac_node_with_schema_info().await;
    let node = cluster.client(0);
    let result = node.p2p_collection_sync_versions(&[&version_id]);
    assert!(
        result.is_err(),
        "anonymous should be rejected by NAC for sync_versions"
    );
}

/// Port: TestNAC_GatesSyncCollectionVersions_WrongIdentity_NotAuthorizedError
#[tokio::test]
#[serial]
async fn nac_sync_versions_wrong_identity() {
    let (cluster, _, outsider_key, _, version_id) = setup_nac_node_with_schema_info().await;
    let node = cluster.client(0);
    let result = node.p2p_collection_sync_versions_with_identity(&[&version_id], &outsider_key);
    assert!(
        result.is_err(),
        "outsider should be rejected by NAC for sync_versions"
    );
}

// --- Admin Relation Tests ---
// These test that the admin relation (automatically granted to startup identity) works.

/// Port: TestNAC_AdminRelation_CanP2PPeerInfo
#[tokio::test]
#[serial]
async fn nac_admin_peer_info() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    cluster
        .client(0)
        .p2p_info_with_identity(&admin_key)
        .expect("admin should access p2p_info");
}

/// Port: TestNAC_AdminRelation_CanP2PPeerConnect
#[tokio::test]
#[serial]
async fn nac_admin_peer_connect() {
    let (cluster, admin_key, _) = setup_nac_two_nodes().await;
    let addr1 = extract_p2p_addr_with_identity(&cluster, 1, &admin_key);
    cluster
        .client(0)
        .p2p_connect_with_identity(&[&addr1], &admin_key)
        .expect("admin should connect peers");
}

/// Port: TestNAC_AdminRelation_CanActivePeers
#[tokio::test]
#[serial]
async fn nac_admin_active_peers() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    cluster
        .client(0)
        .p2p_active_peers_with_identity(&admin_key)
        .expect("admin should list active peers");
}

/// Port: TestNAC_AdminRelation_CanP2PCollectionAdd
#[tokio::test]
#[serial]
async fn nac_admin_collection_add() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    cluster
        .client(0)
        .p2p_collection_add_with_identity(&["Users"], &admin_key)
        .expect("admin should add collection");
}

/// Port: TestNAC_AdminRelation_CanP2PCollectionList
#[tokio::test]
#[serial]
async fn nac_admin_collection_list() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    cluster
        .client(0)
        .p2p_collection_list_with_identity(&admin_key)
        .expect("admin should list collections");
}

/// Port: TestNAC_AdminRelation_CanP2PCollectionDelete
#[tokio::test]
#[serial]
async fn nac_admin_collection_delete() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    node.p2p_collection_add_with_identity(&["Users"], &admin_key)
        .expect("add");
    node.p2p_collection_delete_with_identity(&["Users"], &admin_key)
        .expect("admin should delete collection");
}

/// Port: TestNAC_AdminRelation_CanP2PDocumentAdd
#[tokio::test]
#[serial]
async fn nac_admin_document_add() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let r = node
        .query_with_identity(
            r#"mutation { create_Users(input: {name: "Alice", age: 30}) { _docID } }"#,
            &admin_key,
        )
        .expect("create");
    let doc_id = r["create_Users"][0]["_docID"].as_str().expect("_docID");
    node.p2p_document_add_with_identity(&[doc_id], &admin_key)
        .expect("admin should add document");
}

/// Port: TestNAC_AdminRelation_CanP2PDocumentList
#[tokio::test]
#[serial]
async fn nac_admin_document_list() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    cluster
        .client(0)
        .p2p_document_list_with_identity(&admin_key)
        .expect("admin should list documents");
}

/// Port: TestNAC_AdminRelation_CanP2PDocumentDelete
#[tokio::test]
#[serial]
async fn nac_admin_document_delete() {
    let (cluster, admin_key, _) = setup_nac_node().await;
    let node = cluster.client(0);
    let r = node
        .query_with_identity(
            r#"mutation { create_Users(input: {name: "Alice", age: 30}) { _docID } }"#,
            &admin_key,
        )
        .expect("create");
    let doc_id = r["create_Users"][0]["_docID"].as_str().expect("_docID");
    node.p2p_document_add_with_identity(&[doc_id], &admin_key)
        .expect("add");
    node.p2p_document_delete_with_identity(&[doc_id], &admin_key)
        .expect("admin should delete document");
}

/// Port: TestNAC_AdminRelation_CanP2PReplicatorAdd
#[tokio::test]
#[serial]
async fn nac_admin_replicator_add() {
    let (cluster, admin_key, _) = setup_nac_two_nodes().await;
    let addr1 = extract_p2p_addr_with_identity(&cluster, 1, &admin_key);
    let node = cluster.client(0);
    node.p2p_collection_add_with_identity(&["Users"], &admin_key)
        .expect("add collection");
    node.p2p_replicator_set_with_identity(&["Users"], &addr1, &admin_key)
        .expect("admin should add replicator");
}

/// Port: TestNAC_AdminRelation_CanP2PReplicatorDelete
#[tokio::test]
#[serial]
async fn nac_admin_replicator_delete() {
    let (cluster, admin_key, _) = setup_nac_two_nodes().await;
    let addr1 = extract_p2p_addr_with_identity(&cluster, 1, &admin_key);
    let node = cluster.client(0);
    node.p2p_collection_add_with_identity(&["Users"], &admin_key)
        .expect("add collection");
    node.p2p_replicator_set_with_identity(&["Users"], &addr1, &admin_key)
        .expect("add replicator");
    node.p2p_replicator_delete_with_identity(&["Users"], Some(&addr1), &admin_key)
        .expect("admin should delete replicator");
}

/// Port: TestNAC_AdminRelation_CanSyncBranchableCollection
#[tokio::test]
#[serial]
async fn nac_admin_sync_branchable() {
    let (cluster, admin_key, _, col_id, _) = setup_nac_node_with_schema_info().await;
    let node = cluster.client(0);
    // May fail due to non-branchable or no peers, but should pass NAC gate
    let result = node.p2p_collection_sync_branchable_with_identity(&col_id, &admin_key);
    if let Err(ref e) = result {
        let msg = e.to_string().to_lowercase();
        assert!(
            !msg.contains("unauthorized") && !msg.contains("forbidden") && !msg.contains("nac"),
            "admin should pass NAC gate for sync_branchable, got: {}",
            msg
        );
    }
}

/// Port: TestNAC_AdminRelation_CanSyncCollectionVersions
#[tokio::test]
#[serial]
async fn nac_admin_sync_versions() {
    let (cluster, admin_key, _, _, version_id) = setup_nac_node_with_schema_info().await;
    let node = cluster.client(0);
    let result = node.p2p_collection_sync_versions_with_identity(&[&version_id], &admin_key);
    if let Err(ref e) = result {
        let msg = e.to_string().to_lowercase();
        assert!(
            !msg.contains("unauthorized") && !msg.contains("forbidden") && !msg.contains("nac"),
            "admin should pass NAC gate for sync_versions, got: {}",
            msg
        );
    }
}
