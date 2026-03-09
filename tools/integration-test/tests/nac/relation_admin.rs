use integration_test::{for_each_runtime, generate_identity, TestCluster, PRODUCT_SCHEMA};

for_each_runtime!(
    nac_relation_admin,
    nac_relation_admin_test,
    .with_acp_local().with_nac()
);

async fn nac_relation_admin_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let admin_key = cluster
        .startup_identity()
        .expect("NAC cluster must have startup identity")
        .to_string();

    let outsider = generate_identity(&binary).expect("outsider identity");
    let outsider_key = &outsider.private_key_hex;

    // Admin deploys schema and creates a document
    node.schema_add_with_identity(PRODUCT_SCHEMA, &admin_key)
        .expect("deploy schema");
    node.query_with_identity(
        r#"mutation { add_Product(input: {name: "Widget", sku: "W001", price: 100}) { _docID } }"#,
        &admin_key,
    )
    .expect("create product");

    // =========================================================================
    // Phase 1: Verify outsider is DENIED
    // =========================================================================

    // schema_add → error
    assert!(
        node.schema_add_with_identity("type Gadget { label: String }", outsider_key,)
            .is_err(),
        "outsider should be denied schema_add before grant"
    );

    // query → error or empty results
    let outsider_query = node.query_with_identity("query { Product { name } }", outsider_key);
    let outsider_sees_nothing = match &outsider_query {
        Err(_) => true,
        Ok(val) => val["Product"]
            .as_array()
            .map(|a| a.is_empty())
            .unwrap_or(true),
    };
    assert!(
        outsider_sees_nothing,
        "outsider should see no data before grant"
    );

    // collection_list → error (soft: Go doesn't NAC-gate introspection)
    if node.collection_list_with_identity(outsider_key).is_ok() {
        eprintln!("Warning: outsider collection_list succeeded before grant (Go introspection)");
    }

    // index_create → error
    assert!(
        node.index_create_with_identity(
            "Product",
            &["name"],
            Some("idx_outsider"),
            false,
            outsider_key,
        )
        .is_err(),
        "outsider should be denied index_create before grant"
    );

    // =========================================================================
    // Phase 2: Admin grants NAC admin to outsider
    // =========================================================================
    node.acp_node_relationship_add("admin", &outsider.did, &admin_key)
        .expect("grant outsider NAC admin");

    // =========================================================================
    // Phase 3: Verify outsider is now ALLOWED
    // =========================================================================

    // schema_add → succeeds
    node.schema_add_with_identity("type Gadget { label: String }", outsider_key)
        .expect("outsider should add schema after grant");

    // query → returns data
    let outsider_query = node
        .query_with_identity("query { Product { name } }", outsider_key)
        .expect("outsider should query after grant");
    let count = outsider_query["Product"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert!(count > 0, "outsider should see data after grant");

    // collection_list → returns list
    let cols = node
        .collection_list_with_identity(outsider_key)
        .expect("outsider should list collections after grant");
    assert!(
        !cols.is_empty(),
        "outsider should see collections after grant"
    );

    // index_create → succeeds
    node.index_create_with_identity(
        "Product",
        &["name"],
        Some("idx_outsider"),
        false,
        outsider_key,
    )
    .expect("outsider should create index after grant");

    // =========================================================================
    // Phase 4: Admin revokes NAC admin from outsider
    // =========================================================================
    node.acp_node_relationship_delete("admin", &outsider.did, &admin_key)
        .expect("revoke outsider NAC admin");

    // =========================================================================
    // Phase 5: Verify outsider is DENIED again
    // =========================================================================

    // schema_add → error
    assert!(
        node.schema_add_with_identity("type Widget { code: String }", outsider_key,)
            .is_err(),
        "outsider should be denied schema_add after revoke"
    );

    // query → error or empty
    let outsider_query = node.query_with_identity("query { Product { name } }", outsider_key);
    let outsider_sees_nothing = match &outsider_query {
        Err(_) => true,
        Ok(val) => val["Product"]
            .as_array()
            .map(|a| a.is_empty())
            .unwrap_or(true),
    };
    assert!(
        outsider_sees_nothing,
        "outsider should see no data after revoke"
    );

    // collection_list → error (soft: Go doesn't NAC-gate introspection)
    if node.collection_list_with_identity(outsider_key).is_ok() {
        eprintln!("Warning: outsider collection_list succeeded after revoke (Go introspection)");
    }

    // index_create → error
    assert!(
        node.index_create_with_identity(
            "Product",
            &["sku"],
            Some("idx_revoked"),
            false,
            outsider_key,
        )
        .is_err(),
        "outsider should be denied index_create after revoke"
    );
}
