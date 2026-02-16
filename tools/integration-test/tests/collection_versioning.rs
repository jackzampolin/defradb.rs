use integration_test::TestCluster;

async fn collection_versioning_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // 1. Deploy schema
    client
        .schema_add("type Item { name: String }")
        .expect("failed to add schema");

    // 2. Get version ID from REST API describe endpoint
    let desc = client
        .collection_describe_version("Item")
        .expect("describe version failed");

    // Extract version ID from describe output.
    // Go CLI returns an array of collection versions; Rust REST returns a single object.
    let version_obj = if desc.is_array() {
        desc.as_array()
            .and_then(|arr| arr.first())
            .expect("empty collection describe array")
    } else {
        &desc
    };
    let version_id = version_obj
        .get("VersionID")
        .or_else(|| version_obj.get("version_id"))
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
