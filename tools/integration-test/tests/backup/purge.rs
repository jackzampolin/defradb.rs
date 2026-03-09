use integration_test::{for_each_runtime, TestCluster};

async fn purge_dev_mode_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // 1. Deploy schema and create documents
    client
        .schema_add("type Note { text: String }")
        .expect("failed to add schema");

    client
        .query(r#"mutation { add_Note(input: {text: "hello"}) { _docID } }"#)
        .expect("create doc failed");

    // 2. Verify document exists
    let data = client
        .query("query { Note { text } }")
        .expect("query failed");
    let notes = data["Note"].as_array().expect("expected Note array");
    assert_eq!(notes.len(), 1);

    // 3. Purge in dev mode should succeed
    client.purge().expect("purge should succeed in dev mode");
}

async fn purge_non_dev_mode_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // 1. Deploy schema
    client
        .schema_add("type Note { text: String }")
        .expect("failed to add schema");

    // 2. Purge without dev mode should fail
    let result = client.purge();
    assert!(result.is_err(), "purge should fail when not in dev mode");

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("cannot purge database when development mode is disabled")
            || err_msg.contains("development mode"),
        "error should mention development mode, got: {}",
        err_msg
    );
}

for_each_runtime!(purge_dev_mode, purge_dev_mode_test, .with_development());
for_each_runtime!(purge_non_dev_mode, purge_non_dev_mode_test);
