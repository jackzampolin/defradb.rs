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

    // 2. lens add — verify response contains lensId
    let add_config = r#"{"Path": "/tmp/test.wasm"}"#;
    let add_result = node.lens_add(add_config).expect("lens_add");
    assert!(
        add_result.get("lensId").is_some(),
        "lens_add response should contain lensId, got: {}",
        add_result
    );

    // 3. lens set with src/dst — verify response contains lensId
    let set_result = node
        .lens_set(
            "bafyreiciz2hrrmt7ritk5gf5fyruw46v2tfhq5dc7qto4wgpzluben2smu",
            "bafyreigqfjat435ghyt66tdaucp7oi2mke5jafx3jw3rozanopihr2vf44",
            r#"{"Lenses": []}"#,
        )
        .expect("lens_set");
    assert!(
        set_result.get("lensId").is_some(),
        "lens_set response should contain lensId, got: {}",
        set_result
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
