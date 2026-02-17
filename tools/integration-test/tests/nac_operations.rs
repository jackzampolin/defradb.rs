//! NAC operation gate tests.
//!
//! Verifies that each NAC-gated HTTP endpoint rejects unauthenticated callers
//! and accepts the startup admin identity.

use integration_test::{for_each_runtime, generate_identity, TestCluster, PRODUCT_SCHEMA};

// ---------------------------------------------------------------------------
// Non-P2P operations: encrypted index create, lens, views
// ---------------------------------------------------------------------------
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
    // Encrypted index create — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.encrypted_index_create("Product", "name").is_err(),
        "anonymous should be rejected from encrypted index create"
    );
    assert!(
        node.encrypted_index_create_with_identity("Product", "name", outsider_key)
            .is_err(),
        "outsider should be rejected from encrypted index create"
    );
    node.encrypted_index_create_with_identity("Product", "name", &admin_key)
        .expect("admin should create encrypted index");

    // =========================================================================
    // Lens list — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.lens_list().is_err(),
        "anonymous should be rejected from lens list"
    );
    assert!(
        node.lens_list_with_identity(outsider_key).is_err(),
        "outsider should be rejected from lens list"
    );
    node.lens_list_with_identity(&admin_key)
        .expect("admin should list lenses");

    // =========================================================================
    // View add — anonymous rejected, outsider rejected, admin accepted
    // Note: query must NOT include the "query" keyword prefix.
    // =========================================================================
    assert!(
        node.view_add("Product { name }", "type ProductView { name: String }")
            .is_err(),
        "anonymous should be rejected from view add"
    );
    assert!(
        node.view_add_with_identity(
            "Product { name }",
            "type ProductView { name: String }",
            outsider_key,
        )
        .is_err(),
        "outsider should be rejected from view add"
    );
    node.view_add_with_identity(
        "Product { name }",
        "type ProductView { name: String }",
        &admin_key,
    )
    .expect("admin should add view");

    // =========================================================================
    // View refresh — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.view_refresh(None).is_err(),
        "anonymous should be rejected from view refresh"
    );
    assert!(
        node.view_refresh_with_identity(None, outsider_key).is_err(),
        "outsider should be rejected from view refresh"
    );
    node.view_refresh_with_identity(None, &admin_key)
        .expect("admin should refresh views");
}

// ---------------------------------------------------------------------------
// P2P operations: peer info, active peers (requires P2P subsystem)
// ---------------------------------------------------------------------------
for_each_runtime!(
    nac_p2p_operations_gate,
    nac_p2p_operations_gate_test,
    .with_acp_local().with_nac().with_p2p()
);

async fn nac_p2p_operations_gate_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let admin_key = cluster
        .startup_identity()
        .expect("NAC cluster must have startup identity")
        .to_string();

    let outsider = generate_identity(&binary).expect("outsider identity");
    let outsider_key = &outsider.private_key_hex;

    // =========================================================================
    // P2P peer info — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.p2p_info().is_err(),
        "anonymous should be rejected from p2p info"
    );
    assert!(
        node.p2p_info_with_identity(outsider_key).is_err(),
        "outsider should be rejected from p2p info"
    );
    node.p2p_info_with_identity(&admin_key)
        .expect("admin should access p2p info");

    // =========================================================================
    // P2P active peers — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.p2p_active_peers().is_err(),
        "anonymous should be rejected from active peers"
    );
    assert!(
        node.p2p_active_peers_with_identity(outsider_key).is_err(),
        "outsider should be rejected from active peers"
    );
    node.p2p_active_peers_with_identity(&admin_key)
        .expect("admin should access active peers");
}
