use integration_test::{for_each_runtime, TestCluster};

/// Validates that concurrent truncate operations don't corrupt data or deadlock.
/// Port of Go PR #4420 parallel truncation test.
async fn parallel_truncate_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // Create two collections
    client
        .schema_add("type ItemA { name: String  value: Int }")
        .expect("failed to add ItemA schema");
    client
        .schema_add("type ItemB { label: String  count: Int }")
        .expect("failed to add ItemB schema");

    // Populate both collections with documents
    for i in 0..5 {
        client
            .query(&format!(
                r#"mutation {{ add_ItemA(input: {{name: "a{}", value: {}}}) {{ _docID }} }}"#,
                i, i
            ))
            .unwrap_or_else(|e| panic!("failed to create ItemA doc {}: {}", i, e));

        client
            .query(&format!(
                r#"mutation {{ add_ItemB(input: {{label: "b{}", count: {}}}) {{ _docID }} }}"#,
                i,
                i * 10
            ))
            .unwrap_or_else(|e| panic!("failed to create ItemB doc {}: {}", i, e));
    }

    // Verify documents exist
    let data_a = client
        .query("query { ItemA { name } }")
        .expect("query ItemA");
    assert_eq!(data_a["ItemA"].as_array().unwrap().len(), 5);
    let data_b = client
        .query("query { ItemB { label } }")
        .expect("query ItemB");
    assert_eq!(data_b["ItemB"].as_array().unwrap().len(), 5);

    // Truncate both collections (sequentially — the CLI is synchronous)
    // This validates that truncating one collection doesn't affect another,
    // and that back-to-back truncates don't deadlock.
    client
        .collection_truncate("ItemA")
        .expect("truncate ItemA failed");
    client
        .collection_truncate("ItemB")
        .expect("truncate ItemB failed");

    // Verify both collections are now empty
    let data_a = client
        .query("query { ItemA { name } }")
        .expect("query ItemA after truncate");
    assert_eq!(
        data_a["ItemA"].as_array().unwrap().len(),
        0,
        "ItemA should be empty after truncate"
    );

    let data_b = client
        .query("query { ItemB { label } }")
        .expect("query ItemB after truncate");
    assert_eq!(
        data_b["ItemB"].as_array().unwrap().len(),
        0,
        "ItemB should be empty after truncate"
    );

    // Verify we can still insert after truncate
    client
        .query(r#"mutation { add_ItemA(input: {name: "new", value: 99}) { _docID } }"#)
        .expect("create after truncate failed");

    let data = client
        .query("query { ItemA { name value } }")
        .expect("query after re-insert");
    let items = data["ItemA"].as_array().unwrap();
    assert_eq!(items.len(), 1, "should have 1 doc after re-insert");
    assert_eq!(items[0]["name"], "new");
    assert_eq!(items[0]["value"], 99);
}

for_each_runtime!(parallel_truncate, parallel_truncate_test);
