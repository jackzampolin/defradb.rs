use integration_test::TestCluster;

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

#[tokio::test]
#[ignore]
async fn rust_lens_workflow() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    lens_workflow_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_lens_workflow() {
    let cluster = TestCluster::builder().go_nodes(1).build().await.unwrap();
    lens_workflow_test(cluster).await;
}
