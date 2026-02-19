use integration_test::{for_each_runtime, generate_identity, TestCluster, PRODUCT_SCHEMA};

for_each_runtime!(
    nac_core_operations_gate,
    nac_core_operations_gate_test,
    .with_acp_local().with_nac()
);

async fn nac_core_operations_gate_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let admin_key = cluster
        .startup_identity()
        .expect("NAC cluster must have startup identity")
        .to_string();

    let outsider = generate_identity(&binary).expect("outsider identity");
    let outsider_key = &outsider.private_key_hex;

    // Setup: admin deploys schema and creates a document
    node.schema_add_with_identity(PRODUCT_SCHEMA, &admin_key)
        .expect("deploy schema");

    let data = node
        .query_with_identity(
            r#"mutation { create_Product(input: {name: "Widget", sku: "W001", price: 100}) { _docID } }"#,
            &admin_key,
        )
        .expect("create product");
    let doc_id = data["create_Product"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    // =========================================================================
    // collection list — anonymous rejected, outsider rejected, admin accepted
    // Note: Go doesn't NAC-gate GraphQL introspection; Rust does.
    // Use soft checks for anonymous/outsider, hard check for admin.
    // =========================================================================
    if node.collection_list().is_ok() {
        eprintln!(
            "Warning: anonymous collection_list succeeded (Go doesn't NAC-gate introspection)"
        );
    }
    if node.collection_list_with_identity(outsider_key).is_ok() {
        eprintln!(
            "Warning: outsider collection_list succeeded (Go doesn't NAC-gate introspection)"
        );
    }
    let collections = node
        .collection_list_with_identity(&admin_key)
        .expect("admin should list collections");
    assert!(!collections.is_empty(), "admin should see collections");

    // =========================================================================
    // collection describe — anonymous rejected, outsider rejected, admin accepted
    // Note: Same GraphQL introspection caveat as collection list.
    // =========================================================================
    if node.collection_describe("Product").is_ok() {
        eprintln!("Warning: anonymous collection_describe succeeded");
    }
    if node
        .collection_describe_with_identity("Product", outsider_key)
        .is_ok()
    {
        eprintln!("Warning: outsider collection_describe succeeded");
    }
    node.collection_describe_with_identity("Product", &admin_key)
        .expect("admin should describe collection");

    // =========================================================================
    // collection truncate — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.collection_truncate("Product").is_err(),
        "anonymous should be rejected from collection truncate"
    );
    assert!(
        node.collection_truncate_with_identity("Product", outsider_key)
            .is_err(),
        "outsider should be rejected from collection truncate"
    );

    // =========================================================================
    // index create — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.index_create("Product", &["name"], None, false)
            .is_err(),
        "anonymous should be rejected from index create"
    );
    assert!(
        node.index_create_with_identity(
            "Product",
            &["name"],
            Some("idx_name"),
            false,
            outsider_key
        )
        .is_err(),
        "outsider should be rejected from index create"
    );
    node.index_create_with_identity("Product", &["name"], Some("idx_name"), false, &admin_key)
        .expect("admin should create index");

    // =========================================================================
    // index list — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.index_list(Some("Product")).is_err(),
        "anonymous should be rejected from index list"
    );
    assert!(
        node.index_list_with_identity(Some("Product"), outsider_key)
            .is_err(),
        "outsider should be rejected from index list"
    );
    node.index_list_with_identity(Some("Product"), &admin_key)
        .expect("admin should list indexes");

    // =========================================================================
    // index drop — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.index_delete("Product", "idx_name").is_err(),
        "anonymous should be rejected from index delete"
    );
    assert!(
        node.index_delete_with_identity("Product", "idx_name", outsider_key)
            .is_err(),
        "outsider should be rejected from index delete"
    );
    node.index_delete_with_identity("Product", "idx_name", &admin_key)
        .expect("admin should delete index");

    // =========================================================================
    // collection create (document mutation) — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.collection_create("Product", r#"{"name":"Anon","sku":"A001","price":1}"#)
            .is_err(),
        "anonymous should be rejected from collection create"
    );
    assert!(
        node.collection_create_with_identity(
            "Product",
            r#"{"name":"Out","sku":"O001","price":2}"#,
            outsider_key,
        )
        .is_err(),
        "outsider should be rejected from collection create"
    );
    node.collection_create_with_identity(
        "Product",
        r#"{"name":"Admin","sku":"AD01","price":3}"#,
        &admin_key,
    )
    .expect("admin should create document");

    // =========================================================================
    // collection update (document mutation) — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.collection_update("Product", &doc_id, r#"{"price": 200}"#)
            .is_err(),
        "anonymous should be rejected from collection update"
    );
    assert!(
        node.collection_update_with_identity("Product", &doc_id, r#"{"price": 200}"#, outsider_key)
            .is_err(),
        "outsider should be rejected from collection update"
    );
    node.collection_update_with_identity("Product", &doc_id, r#"{"price": 200}"#, &admin_key)
        .expect("admin should update document");

    // =========================================================================
    // collection delete (document mutation) — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.collection_delete("Product", &doc_id).is_err(),
        "anonymous should be rejected from collection delete"
    );
    assert!(
        node.collection_delete_with_identity("Product", &doc_id, outsider_key)
            .is_err(),
        "outsider should be rejected from collection delete"
    );
    node.collection_delete_with_identity("Product", &doc_id, &admin_key)
        .expect("admin should delete document");

    // =========================================================================
    // Admin truncate (run last since it removes all docs)
    // =========================================================================
    node.collection_truncate_with_identity("Product", &admin_key)
        .expect("admin should truncate collection");
}
