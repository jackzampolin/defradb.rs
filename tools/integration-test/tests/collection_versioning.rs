use integration_test::TestCluster;

async fn collection_versioning_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // 1. Deploy schema
    client
        .schema_add("type Item { name: String }")
        .expect("failed to add schema");

    // 2. Get version ID from collection describe
    let desc = client.collection_describe("Item").expect("describe failed");

    // Extract version ID - Go uses "VersionID", check both
    let version_id = desc
        .get("VersionID")
        .or_else(|| desc.get("version_id"))
        .and_then(|v| v.as_str())
        .expect("missing VersionID in collection describe");

    // 3. Set-active with the same version ID (no-op, should succeed)
    client
        .collection_set_active(version_id)
        .expect("set-active failed");

    // 4. Verify collection still works
    client
        .query(r#"mutation { create_Item(input: {name: "widget"}) { _docID } }"#)
        .expect("create after set-active failed");

    let data = client
        .query("query { Item { name } }")
        .expect("query after set-active failed");

    let items = data["Item"].as_array().expect("expected Item array");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "widget");
}

#[tokio::test]
#[ignore]
async fn rust_collection_versioning() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    collection_versioning_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_collection_versioning() {
    let cluster = TestCluster::builder().go_nodes(1).build().await.unwrap();
    collection_versioning_test(cluster).await;
}
