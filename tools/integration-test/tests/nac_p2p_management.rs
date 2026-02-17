use integration_test::{for_each_runtime, generate_identity, TestCluster, PRODUCT_SCHEMA};

for_each_runtime!(
    nac_p2p_management_gate,
    nac_p2p_management_gate_test,
    .with_acp_local().with_nac().with_p2p()
);

async fn nac_p2p_management_gate_test(cluster: TestCluster) {
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
    // P2P collection add — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.p2p_collection_add(&["Product"]).is_err(),
        "anonymous should be rejected from p2p collection add"
    );
    assert!(
        node.p2p_collection_add_with_identity(&["Product"], outsider_key)
            .is_err(),
        "outsider should be rejected from p2p collection add"
    );
    node.p2p_collection_add_with_identity(&["Product"], &admin_key)
        .expect("admin should add p2p collection");

    // =========================================================================
    // P2P collection list — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.p2p_collection_list().is_err(),
        "anonymous should be rejected from p2p collection list"
    );
    assert!(
        node.p2p_collection_list_with_identity(outsider_key)
            .is_err(),
        "outsider should be rejected from p2p collection list"
    );
    node.p2p_collection_list_with_identity(&admin_key)
        .expect("admin should list p2p collections");

    // =========================================================================
    // P2P collection delete — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.p2p_collection_delete(&["Product"]).is_err(),
        "anonymous should be rejected from p2p collection delete"
    );
    assert!(
        node.p2p_collection_delete_with_identity(&["Product"], outsider_key)
            .is_err(),
        "outsider should be rejected from p2p collection delete"
    );
    node.p2p_collection_delete_with_identity(&["Product"], &admin_key)
        .expect("admin should delete p2p collection");

    // =========================================================================
    // P2P document create — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.p2p_document_create(&[&doc_id]).is_err(),
        "anonymous should be rejected from p2p document create"
    );
    assert!(
        node.p2p_document_create_with_identity(&[&doc_id], outsider_key)
            .is_err(),
        "outsider should be rejected from p2p document create"
    );
    node.p2p_document_create_with_identity(&[&doc_id], &admin_key)
        .expect("admin should add p2p document");

    // =========================================================================
    // P2P document list — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.p2p_document_list().is_err(),
        "anonymous should be rejected from p2p document list"
    );
    assert!(
        node.p2p_document_list_with_identity(outsider_key).is_err(),
        "outsider should be rejected from p2p document list"
    );
    node.p2p_document_list_with_identity(&admin_key)
        .expect("admin should list p2p documents");

    // =========================================================================
    // P2P document delete — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.p2p_document_delete(&[&doc_id]).is_err(),
        "anonymous should be rejected from p2p document delete"
    );
    assert!(
        node.p2p_document_delete_with_identity(&[&doc_id], outsider_key)
            .is_err(),
        "outsider should be rejected from p2p document delete"
    );
    node.p2p_document_delete_with_identity(&[&doc_id], &admin_key)
        .expect("admin should delete p2p document");

    // =========================================================================
    // P2P replicator list — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.p2p_replicator_list().is_err(),
        "anonymous should be rejected from p2p replicator list"
    );
    assert!(
        node.p2p_replicator_list_with_identity(outsider_key)
            .is_err(),
        "outsider should be rejected from p2p replicator list"
    );
    node.p2p_replicator_list_with_identity(&admin_key)
        .expect("admin should list p2p replicators");
}
