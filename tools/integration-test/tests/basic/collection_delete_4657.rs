use integration_test::TestCluster;

async fn stale_transaction_cannot_write_after_collection_delete_test(cluster: TestCluster) {
    let client = cluster.client(0);

    client
        .schema_add("type Users { name: String }")
        .expect("add schema");

    let tx_id = client.tx_create().expect("create transaction");

    client
        .collection_patch(r#"[{"op": "remove", "path": "/Users"}]"#)
        .expect("delete collection");

    assert!(
        client.query("query { Users { name } }").is_err(),
        "deleted collection should not be queryable outside the stale transaction"
    );

    let stale_write = client.query_with_tx(
        r#"mutation { add_Users(input: {name: "stale"}) { _docID } }"#,
        &tx_id,
    );
    let stale_err = stale_write.expect_err("stale transaction write should fail");
    assert!(
        stale_err.to_string().contains("collection not found")
            || stale_err.to_string().contains("Cannot query field")
            || stale_err.to_string().contains("Users"),
        "unexpected stale write error: {stale_err}"
    );

    client
        .tx_discard(&tx_id)
        .expect("discard stale transaction");

    let commits = client
        .query("query { _commits { cid } }")
        .expect("query commits");
    assert_eq!(
        commits["_commits"],
        serde_json::json!([]),
        "stale write must not leave document commits behind"
    );
}

#[tokio::test]
async fn rust_stale_transaction_cannot_write_after_collection_delete() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    stale_transaction_cannot_write_after_collection_delete_test(cluster).await;
}
