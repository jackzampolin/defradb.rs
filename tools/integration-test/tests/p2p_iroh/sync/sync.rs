//! Iroh P2P sync operation tests.
//!
//! Tests explicit sync operations over iroh transport:
//! - Invalid CID rejection for sync-versions
//! - Explicit document sync between two nodes
//! - Collection version sync between two nodes
//! - Sync-branchable with invalid collection ID
//!
//! Run with:
//!   cargo test --test p2p_iroh -- sync::sync::

use std::time::Duration;

use integration_test::TestCluster;
use serial_test::serial;

/// Invalid CID should be rejected by sync-versions.
#[tokio::test]
#[serial]
async fn iroh_sync_invalid_cid() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", Duration::from_secs(15))
        .await
        .expect("P2P listener did not start");

    let node = cluster.client(0);
    let err = node
        .p2p_collection_sync_versions(&["bafyreiblahblah123"])
        .unwrap_err()
        .to_string();
    assert!(
        err.to_lowercase().contains("invalid cid") || err.to_lowercase().contains("illegal base32"),
        "sync-versions should reject invalid CID, got: {}",
        err
    );
}

/// Sync-branchable with invalid collection ID should fail.
#[tokio::test]
#[serial]
async fn iroh_sync_branchable_invalid() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", Duration::from_secs(15))
        .await
        .expect("P2P listener did not start");

    let node = cluster.client(0);
    let err = node
        .p2p_collection_sync_branchable("1")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("not found") || err.contains("collection"),
        "sync-branchable should fail at collection lookup, got: {}",
        err
    );
}

/// DocSync is a pull protocol: the requesting node asks connected peers for
/// specific documents, peers reply with their head CIDs, and the requester
/// fetches and merges the blocks.
///
/// Setup: doc lives on node0, node1 has no replicator — node1 must pull it
/// explicitly via p2p_document_sync.
#[tokio::test]
#[serial]
async fn iroh_document_sync() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 p2p_listening");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 p2p_listening");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // node1 dials node0 so node1 knows node0's address for the DocSync response.
    let info0 = node0.p2p_info().expect("p2p_info node0");
    let addr0 = info0
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node0 has no P2P address");

    node0
        .schema_add("type Task { title: String }")
        .expect("schema node0");
    node1
        .schema_add("type Task { title: String }")
        .expect("schema node1");

    // Connect: node1 dials node0.  DocSync flows node1→(request)→node0→(reply)→node1.
    node1.p2p_connect(&[addr0]).expect("p2p connect");
    node0
        .p2p_collection_add(&["Task"])
        .expect("collection add node0");
    node1
        .p2p_collection_add(&["Task"])
        .expect("collection add node1");

    // Create the doc on node0 only — no replicator, so node1 won't get it automatically.
    let result = node0
        .query(r#"mutation { add_Task(input: {title: "sync me"}) { _docID } }"#)
        .expect("create task on node0");
    let doc_id = result["add_Task"]
        .as_array()
        .and_then(|arr| arr.first())
        .or_else(|| result["add_Task"].as_object().map(|_| &result["add_Task"]))
        .and_then(|v| v.get("_docID"))
        .and_then(|v| v.as_str())
        .expect("could not extract _docID");

    // node1 pulls the doc from node0 via DocSync.
    node1
        .p2p_document_sync("Task", &[doc_id])
        .expect("p2p_document_sync from node1");

    // Doc should now be on node1.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let result = node1
            .query("query { Task { _docID title } }")
            .unwrap_or_default();
        if let Some(tasks) = result["Task"].as_array() {
            if !tasks.is_empty() {
                assert_eq!(tasks[0]["_docID"].as_str().unwrap(), doc_id);
                assert_eq!(tasks[0]["title"], "sync me");
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "doc did not sync within timeout"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Sync collection versions between two iroh nodes.
#[tokio::test]
#[serial]
async fn iroh_collection_sync_versions() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let info1 = node1.p2p_info().expect("p2p_info node1");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");

    // Deploy same schema on both nodes
    node0
        .schema_add("type Item { name: String }")
        .expect("add schema node0");
    node1
        .schema_add("type Item { name: String }")
        .expect("add schema node1");

    // Connect peers
    node0.p2p_connect(&[addr1]).expect("p2p connect");

    // Get the collection version CID from node0
    let desc = node0
        .collection_describe_version("Item")
        .expect("describe version");
    let version_id = desc
        .as_array()
        .and_then(|arr| arr.first())
        .or(Some(&desc))
        .and_then(|v| {
            v.get("VersionID")
                .or_else(|| v.get("versionID"))
                .or_else(|| v.get("version_id"))
                .or_else(|| v.get("SchemaVersionID"))
                .or_else(|| v.get("schemaVersionId"))
        })
        .and_then(|v| v.as_str())
        .expect("could not extract version ID");

    // Sync the version — both nodes have it already, verify it succeeds
    node0
        .p2p_collection_sync_versions(&[version_id])
        .expect("p2p collection sync-versions should succeed");

    // Verify node1 also knows this version after sync
    let desc1 = node1
        .collection_describe_version("Item")
        .expect("describe version on node1");
    let version_id_1 = desc1
        .as_array()
        .and_then(|arr| arr.first())
        .or(Some(&desc1))
        .and_then(|v| {
            v.get("VersionID")
                .or_else(|| v.get("versionID"))
                .or_else(|| v.get("version_id"))
                .or_else(|| v.get("SchemaVersionID"))
                .or_else(|| v.get("schemaVersionId"))
        })
        .and_then(|v| v.as_str())
        .expect("could not extract version ID from node1");
    assert_eq!(
        version_id, version_id_1,
        "both nodes should have the same schema version after sync"
    );
}
