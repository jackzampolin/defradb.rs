use integration_test::TestCluster;

async fn lens_migration_test(cluster: TestCluster) {
    let node = cluster.client(0);

    // Deploy schema
    node.schema_add("type Article { title: String }")
        .expect("add Article schema");

    // Create articles
    node.query(r#"mutation { create_Article(input: {title: "First"}) { _docID } }"#)
        .expect("create article 1");
    node.query(r#"mutation { create_Article(input: {title: "Second"}) { _docID } }"#)
        .expect("create article 2");

    // Verify schema describe works
    let schema_desc = node.schema_describe().expect("schema describe");
    assert!(
        schema_desc.contains("Article"),
        "schema describe should mention Article"
    );

    // lens_list should succeed on fresh node (empty result)
    let list_result = node.lens_list().expect("lens_list should succeed");
    let is_empty = list_result.is_null()
        || list_result
            .as_object()
            .map(|o| o.is_empty())
            .unwrap_or(false)
        || list_result
            .as_array()
            .map(|a| a.is_empty())
            .unwrap_or(false);
    assert!(
        is_empty,
        "lens_list on fresh node should be empty, got: {}",
        list_result
    );

    // lens_reload should succeed
    node.lens_reload().expect("lens_reload should succeed");

    // Verify articles still queryable
    let articles = node
        .query("query { Article { title } }")
        .expect("query articles");
    let arr = articles["Article"].as_array().expect("articles array");
    assert_eq!(arr.len(), 2, "should have 2 articles");
}

#[tokio::test]
#[ignore]
async fn rust_lens_migration() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    lens_migration_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_lens_migration() {
    let cluster = TestCluster::builder().go_nodes(1).build().await.unwrap();
    lens_migration_test(cluster).await;
}
