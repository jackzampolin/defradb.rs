use integration_test::{for_each_runtime, TestCluster};

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

async fn lens_workflow_test(cluster: TestCluster) {
    let node = cluster.client(0);

    // Deploy schema
    node.schema_add("type Widget { name: String }")
        .expect("add Widget schema");

    // 1. lens list on fresh node — should be empty map
    let list_result = node.lens_list().expect("lens_list");
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

    // 2. lens add — verify wire format accepted (fails at lens validation, not JSON parsing)
    let add_config = r#"{
        "SourceCollectionVersionID": "bafyreiciz2hrrmt7ritk5gf5fyruw46v2tfhq5dc7qto4wgpzluben2smu",
        "DestinationCollectionVersionID": "bafyreigqfjat435ghyt66tdaucp7oi2mke5jafx3jw3rozanopihr2vf44",
        "Lenses": []
    }"#;
    let add_err = node.lens_add(add_config).unwrap_err().to_string();
    // Should fail at lens validation, not at JSON parsing/wrapping
    assert!(
        !add_err.contains("invalid JSON") && !add_err.contains("missing field"),
        "lens_add should not fail at JSON parsing, got: {}",
        add_err
    );

    // 3. lens set — verify wire format accepted (fails at lens validation, not JSON parsing)
    let set_err = node
        .lens_set(
            "bafyreiciz2hrrmt7ritk5gf5fyruw46v2tfhq5dc7qto4wgpzluben2smu",
            "bafyreigqfjat435ghyt66tdaucp7oi2mke5jafx3jw3rozanopihr2vf44",
            r#"{"Lenses": []}"#,
        )
        .unwrap_err()
        .to_string();
    assert!(
        !set_err.contains("invalid JSON") && !set_err.contains("missing field"),
        "lens_set should not fail at JSON parsing, got: {}",
        set_err
    );
}

for_each_runtime!(lens_migration, lens_migration_test);
for_each_runtime!(lens_workflow, lens_workflow_test);
