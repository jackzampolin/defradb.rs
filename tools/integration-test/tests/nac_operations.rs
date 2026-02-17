//! NAC operation gate tests.
//!
//! Verifies that each NAC-gated HTTP endpoint rejects unauthenticated callers
//! and accepts the startup admin identity.

use integration_test::{for_each_runtime, generate_identity, TestCluster, PRODUCT_SCHEMA};

for_each_runtime!(
    nac_operations_gate,
    nac_operations_gate_test,
    .with_acp_local().with_nac().with_encryption()
);

async fn nac_operations_gate_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let admin_key = cluster
        .startup_identity()
        .expect("NAC cluster must have startup identity")
        .to_string();

    let outsider = generate_identity(&binary).expect("outsider identity");
    let outsider_key = &outsider.private_key_hex;

    // Deploy a schema so encrypted-index and view operations have a collection
    node.schema_add_with_identity(PRODUCT_SCHEMA, &admin_key)
        .expect("deploy schema");

    // Create a document so the collection is non-empty
    node.query_with_identity(
        r#"mutation { create_Product(input: {name: "Widget", sku: "W001", price: 100}) { _docID } }"#,
        &admin_key,
    )
    .expect("create product");

    // =========================================================================
    // P2P peer info — outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.p2p_info_with_identity(outsider_key).is_err(),
        "outsider should be rejected from p2p info"
    );
    assert!(
        node.p2p_info_with_identity(&admin_key).is_ok(),
        "admin should access p2p info"
    );

    // =========================================================================
    // P2P active peers — outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.p2p_active_peers_with_identity(outsider_key).is_err(),
        "outsider should be rejected from active peers"
    );
    assert!(
        node.p2p_active_peers_with_identity(&admin_key).is_ok(),
        "admin should access active peers"
    );

    // =========================================================================
    // Encrypted index create — outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.encrypted_index_create_with_identity("Product", "name", outsider_key)
            .is_err(),
        "outsider should be rejected from encrypted index create"
    );
    node.encrypted_index_create_with_identity("Product", "name", &admin_key)
        .expect("admin should create encrypted index");

    // =========================================================================
    // Encrypted index list — outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.encrypted_index_list_with_identity("Product", outsider_key)
            .is_err(),
        "outsider should be rejected from encrypted index list"
    );
    node.encrypted_index_list_with_identity("Product", &admin_key)
        .expect("admin should list encrypted indexes");

    // =========================================================================
    // Encrypted index delete — outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.encrypted_index_delete_with_identity("Product", "name", outsider_key)
            .is_err(),
        "outsider should be rejected from encrypted index delete"
    );
    node.encrypted_index_delete_with_identity("Product", "name", &admin_key)
        .expect("admin should delete encrypted index");

    // =========================================================================
    // Lens list — outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.lens_list_with_identity(outsider_key).is_err(),
        "outsider should be rejected from lens list"
    );
    node.lens_list_with_identity(&admin_key)
        .expect("admin should list lenses");

    // =========================================================================
    // View add — outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.view_add_with_identity(
            "query { Product { name } }",
            "type ProductView { name: String }",
            outsider_key,
        )
        .is_err(),
        "outsider should be rejected from view add"
    );
    node.view_add_with_identity(
        "query { Product { name } }",
        "type ProductView { name: String }",
        &admin_key,
    )
    .expect("admin should add view");

    // =========================================================================
    // View refresh — outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.view_refresh_with_identity(None, outsider_key).is_err(),
        "outsider should be rejected from view refresh"
    );
    node.view_refresh_with_identity(None, &admin_key)
        .expect("admin should refresh views");
}
