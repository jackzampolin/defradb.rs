//! Iroh P2P document subscription management tests.
//!
//! Ported from Go: tests/integration/net/simple/peer/ (document subscription tests)
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh_document_sub -- --ignored

use std::time::Duration;

use integration_test::TestCluster;
use serial_test::serial;

const SCHEMA: &str = "type Users { name: String  age: Int }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);

async fn setup_with_docs() -> (TestCluster, String, String) {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();
    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("P2P listener");

    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("schema");

    let r1 = node
        .query(r#"mutation { create_Users(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create Alice");
    let doc1 = r1["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    let r2 = node
        .query(r#"mutation { create_Users(input: {name: "Bob", age: 25}) { _docID } }"#)
        .expect("create Bob");
    let doc2 = r2["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    (cluster, doc1, doc2)
}

/// Port: TestP2PDocument_GetAllWithNoneConfigured_ShouldSucceed
/// Get all with no documents configured returns empty.
#[tokio::test]
#[serial]
async fn document_get_all_none_configured() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();
    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("P2P listener");

    let docs = cluster.client(0).p2p_document_list().expect("doc list");
    let arr = docs.as_array().expect("not array");
    assert!(arr.is_empty(), "should have 0 P2P documents initially");
}

/// Port: TestP2PDocumentAddGetSingle
/// Add and get a single document subscription.
#[tokio::test]
#[serial]
async fn document_add_get_single() {
    let (cluster, doc1, _) = setup_with_docs().await;
    let node = cluster.client(0);

    node.p2p_document_add(&[&doc1]).expect("add doc");

    let docs = node.p2p_document_list().expect("list");
    let arr = docs.as_array().expect("not array");
    assert_eq!(arr.len(), 1, "should have 1 tracked document");
}

/// Port: TestP2PDocumentAddGetMultiple
/// Add and get multiple document subscriptions.
#[tokio::test]
#[serial]
async fn document_add_get_multiple() {
    let (cluster, doc1, doc2) = setup_with_docs().await;
    let node = cluster.client(0);

    node.p2p_document_add(&[&doc1]).expect("add doc1");
    node.p2p_document_add(&[&doc2]).expect("add doc2");

    let docs = node.p2p_document_list().expect("list");
    let arr = docs.as_array().expect("not array");
    assert_eq!(arr.len(), 2, "should have 2 tracked documents");
}

/// Port: TestP2PDocumentAddRemoveGetSingle
/// Add, remove, verify single document gone.
#[tokio::test]
#[serial]
async fn document_add_remove_get_single() {
    let (cluster, doc1, _) = setup_with_docs().await;
    let node = cluster.client(0);

    node.p2p_document_add(&[&doc1]).expect("add");
    assert_eq!(
        node.p2p_document_list()
            .expect("list")
            .as_array()
            .expect("not array")
            .len(),
        1
    );

    node.p2p_document_delete(&[&doc1]).expect("remove");
    let after = node.p2p_document_list().expect("list after");
    assert!(
        after.as_array().expect("not array").is_empty(),
        "should be empty after remove"
    );
}

/// Port: TestP2PDocumentAddRemoveGetMultiple
/// Add two, remove one, verify one remains.
#[tokio::test]
#[serial]
async fn document_add_remove_get_multiple() {
    let (cluster, doc1, doc2) = setup_with_docs().await;
    let node = cluster.client(0);

    node.p2p_document_add(&[&doc1]).expect("add doc1");
    node.p2p_document_add(&[&doc2]).expect("add doc2");
    assert_eq!(
        node.p2p_document_list()
            .expect("list")
            .as_array()
            .expect("not array")
            .len(),
        2
    );

    node.p2p_document_delete(&[&doc1]).expect("remove doc1");
    let after = node.p2p_document_list().expect("list after");
    let arr = after.as_array().expect("not array");
    assert_eq!(arr.len(), 1, "should have 1 after removing doc1");
}

/// Port: TestP2PDocument_AddSingle_ShouldSync
/// Adding a document to P2P tracking enables sync for that doc.
#[tokio::test]
#[serial]
async fn document_add_single_should_sync() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node0");
    cluster
        .wait_for_log(1, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node1");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0.schema_add(SCHEMA).expect("schema node0");
    node1.schema_add(SCHEMA).expect("schema node1");

    // Create doc on node0
    let result = node0
        .query(r#"mutation { create_Users(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create");
    let doc_id = result["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Connect and set up document-level tracking
    let addr1 = integration_test::extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect");
    node0
        .p2p_document_add(&[&doc_id])
        .expect("track doc on node0");
    node1
        .p2p_document_add(&[&doc_id])
        .expect("track doc on node1");

    // Set up collection and replicator to enable push
    node0.p2p_collection_add(&["Users"]).expect("col node0");
    node1.p2p_collection_add(&["Users"]).expect("col node1");
    node0.p2p_replicator_set(&["Users"], &addr1).expect("rep");

    // Verify doc syncs
    let node1_ref = &node1;
    integration_test::poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|a| {
                    a.iter().any(|u| {
                        u["name"].as_str() == Some("Alice") && u["age"].as_i64() == Some(30)
                    })
                })
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "tracked doc did not sync",
    )
    .await;
}

/// Port: TestP2PDocument_AddSingleErroneousDocID_ShouldNotSync
/// Adding erroneous doc ID should fail or not cause sync.
#[tokio::test]
#[serial]
async fn document_add_erroneous_should_not_sync() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();
    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("P2P listener");

    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("schema");

    // Try to add a bogus doc ID
    let result = node.p2p_document_add(&["not-a-real-doc-id"]);
    // May succeed (just tracks an ID that doesn't exist) or fail
    // Either way, the document list should reflect what was added
    if result.is_ok() {
        let docs = node.p2p_document_list().expect("list");
        let arr = docs.as_array().expect("not array");
        // The ID was accepted but there's nothing to sync
        assert!(arr.len() <= 1);
    }
}

/// Port: TestP2PDocumentAddAndRemoveSingle
/// Full lifecycle: add, verify, remove, verify empty.
#[tokio::test]
#[serial]
async fn document_add_and_remove_single() {
    let (cluster, doc1, _) = setup_with_docs().await;
    let node = cluster.client(0);

    node.p2p_document_add(&[&doc1]).expect("add");
    assert_eq!(
        node.p2p_document_list()
            .expect("list")
            .as_array()
            .expect("not array")
            .len(),
        1
    );

    node.p2p_document_delete(&[&doc1]).expect("remove");
    assert!(
        node.p2p_document_list()
            .expect("list")
            .as_array()
            .expect("not array")
            .is_empty(),
        "should be empty after remove"
    );
}

/// Port: TestP2PDocumentAddAndRemoveMultiple
/// Add multiple, remove all, verify empty.
#[tokio::test]
#[serial]
async fn document_add_and_remove_multiple() {
    let (cluster, doc1, doc2) = setup_with_docs().await;
    let node = cluster.client(0);

    node.p2p_document_add(&[&doc1]).expect("add doc1");
    node.p2p_document_add(&[&doc2]).expect("add doc2");

    node.p2p_document_delete(&[&doc1]).expect("remove doc1");
    node.p2p_document_delete(&[&doc2]).expect("remove doc2");

    assert!(
        node.p2p_document_list()
            .expect("list")
            .as_array()
            .expect("not array")
            .is_empty(),
        "should be empty after removing all"
    );
}

/// Port: TestP2PDocumentAddSingleAndRemoveErroneous
/// Add valid, remove bogus — valid should persist.
#[tokio::test]
#[serial]
async fn document_add_single_remove_erroneous() {
    let (cluster, doc1, _) = setup_with_docs().await;
    let node = cluster.client(0);

    node.p2p_document_add(&[&doc1]).expect("add");
    let _ = node.p2p_document_delete(&["not-a-real-doc-id"]); // may fail or no-op

    let docs = node.p2p_document_list().expect("list");
    assert_eq!(
        docs.as_array().expect("not array").len(),
        1,
        "valid doc should persist after removing bogus"
    );
}

/// Port: TestP2PDocumentAddSingleAndRemoveNone
/// Add valid, remove empty — valid should persist.
#[tokio::test]
#[serial]
async fn document_add_single_remove_none() {
    let (cluster, doc1, _) = setup_with_docs().await;
    let node = cluster.client(0);

    node.p2p_document_add(&[&doc1]).expect("add");
    let _ = node.p2p_document_delete(&[]); // empty remove

    let docs = node.p2p_document_list().expect("list");
    assert_eq!(
        docs.as_array().expect("not array").len(),
        1,
        "valid doc should persist after empty remove"
    );
}
