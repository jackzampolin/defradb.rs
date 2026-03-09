use integration_test::{for_each_runtime, TestCluster};

async fn collection_patch_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // 1. Deploy schema with title field
    client
        .schema_add("type Book { title: String }")
        .expect("failed to add schema");

    // 2. Create a document
    client
        .query(r#"mutation { add_Book(input: {title: "Rust Programming"}) { _docID } }"#)
        .expect("create doc failed");

    // 3. Patch schema to add summary field
    let patch = r#"[{"op": "add", "path": "/Book/Fields/-", "value": {"Name": "summary", "Kind": "String"}}]"#;
    client
        .collection_patch(patch)
        .expect("collection patch failed");

    // 4. Query: verify new field exists (returns null for existing docs)
    let data = client
        .query("query { Book { title summary } }")
        .expect("query after patch failed");

    let books = data["Book"].as_array().expect("expected Book array");
    assert_eq!(books.len(), 1, "should have 1 book");
    assert_eq!(books[0]["title"], "Rust Programming");
    // New field should be null for existing docs
    assert!(
        books[0]["summary"].is_null(),
        "summary should be null for pre-existing doc"
    );
}

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
        .query(r#"mutation { add_Item(input: {name: "widget"}) { _docID } }"#)
        .expect("create after set-active failed");

    let data = client
        .query("query { Item { name } }")
        .expect("query after set-active failed");

    let items = data["Item"].as_array().expect("expected Item array");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "widget");
}

for_each_runtime!(collection_patch, collection_patch_test);
for_each_runtime!(collection_versioning, collection_versioning_test);
