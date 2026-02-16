use integration_test::TestCluster;
use std::time::Duration;

async fn p2p_document_sync_test(cluster: TestCluster) {
    let node = cluster.client(0);

    cluster
        .wait_for_log(0, "p2p_listening", Duration::from_secs(15))
        .await
        .expect("P2P listener did not start");

    node.schema_add("type Task { title: String }")
        .expect("add Task schema");

    let result = node
        .query(r#"mutation { create_Task(input: {title: "sync me"}) { _docID } }"#)
        .expect("create task");
    let doc_id = result["create_Task"]
        .as_array()
        .and_then(|arr| arr.first())
        .or_else(|| {
            result["create_Task"]
                .as_object()
                .map(|_| &result["create_Task"])
        })
        .and_then(|v| v.get("_docID"))
        .and_then(|v| v.as_str())
        .expect("could not extract _docID");

    node.p2p_document_sync("Task", &[doc_id])
        .expect("p2p_document_sync should succeed");
}

async fn p2p_collection_sync_versions_test(cluster: TestCluster) {
    let node = cluster.client(0);

    cluster
        .wait_for_log(0, "p2p_listening", Duration::from_secs(15))
        .await
        .expect("P2P listener did not start");

    // sync-versions with a dummy version ID — should not error on the CLI/HTTP layer
    node.p2p_collection_sync_versions(&["bafyreiblahblah123"])
        .expect("p2p_collection_sync_versions should succeed");
}

async fn p2p_collection_sync_branchable_test(cluster: TestCluster) {
    let node = cluster.client(0);

    cluster
        .wait_for_log(0, "p2p_listening", Duration::from_secs(15))
        .await
        .expect("P2P listener did not start");

    // sync-branchable with a dummy collection ID — should not error on the CLI/HTTP layer
    node.p2p_collection_sync_branchable("1")
        .expect("p2p_collection_sync_branchable should succeed");
}

#[tokio::test]
#[ignore]
async fn rust_p2p_sync_document() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    p2p_document_sync_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_p2p_sync_document() {
    let cluster = TestCluster::builder()
        .go_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    p2p_document_sync_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn rust_p2p_sync_versions() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    p2p_collection_sync_versions_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_p2p_sync_versions() {
    let cluster = TestCluster::builder()
        .go_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    p2p_collection_sync_versions_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn rust_p2p_sync_branchable() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    p2p_collection_sync_branchable_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_p2p_sync_branchable() {
    let cluster = TestCluster::builder()
        .go_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    p2p_collection_sync_branchable_test(cluster).await;
}
