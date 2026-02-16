use integration_test::TestCluster;

async fn collection_patch_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // 1. Deploy schema with title field
    client
        .schema_add("type Book { title: String }")
        .expect("failed to add schema");

    // 2. Create a document
    client
        .query(r#"mutation { create_Book(input: {title: "Rust Programming"}) { _docID } }"#)
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

#[tokio::test]
#[ignore]
async fn rust_collection_patch() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    collection_patch_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_collection_patch() {
    let cluster = TestCluster::builder().go_nodes(1).build().await.unwrap();
    collection_patch_test(cluster).await;
}
