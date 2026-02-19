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
        node.encrypted_index_add("Product", "name").is_err(),
        "anonymous should be rejected from encrypted index create"
    );
    assert!(
        node.encrypted_index_add_with_identity("Product", "name", outsider_key)
            .is_err(),
        "outsider should be rejected from encrypted index create"
    );
    node.encrypted_index_add_with_identity("Product", "name", &admin_key)
        .expect("admin should add encrypted index");

    // =========================================================================
    // Encrypted index list — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.encrypted_index_list("Product").is_err(),
        "anonymous should be rejected from encrypted index list"
    );
    assert!(
        node.encrypted_index_list_with_identity("Product", outsider_key)
            .is_err(),
        "outsider should be rejected from encrypted index list"
    );
    node.encrypted_index_list_with_identity("Product", &admin_key)
        .expect("admin should list encrypted indexes");

    // =========================================================================
    // Encrypted index delete — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    assert!(
        node.encrypted_index_delete("Product", "name").is_err(),
        "anonymous should be rejected from encrypted index delete"
    );
    assert!(
        node.encrypted_index_delete_with_identity("Product", "name", outsider_key)
            .is_err(),
        "outsider should be rejected from encrypted index delete"
    );
    node.encrypted_index_delete_with_identity("Product", "name", &admin_key)
        .expect("admin should delete encrypted index");

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
    // Lens add — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    let lens_config = r#"{"lenses":[]}"#;
    assert!(
        node.lens_add(lens_config).is_err(),
        "anonymous should be rejected from lens add"
    );
    assert!(
        node.lens_add_with_identity(lens_config, outsider_key)
            .is_err(),
        "outsider should be rejected from lens add"
    );
    // Admin call: may succeed or fail for non-NAC reasons (empty config),
    // but must not be NAC-rejected.
    let admin_lens_result = node.lens_add_with_identity(lens_config, &admin_key);
    if let Err(ref e) = admin_lens_result {
        let msg = e.to_string().to_lowercase();
        assert!(
            !msg.contains("unauthorized") && !msg.contains("forbidden") && !msg.contains("nac"),
            "admin should pass NAC gate for lens add, got: {e}"
        );
    }

    // =========================================================================
    // Lens set (migration-set) — anonymous rejected, outsider rejected, admin accepted
    // =========================================================================
    let migration_config = r#"{"Lenses":[]}"#;
    assert!(
        node.lens_set("dummy-src-v1", "dummy-dst-v2", migration_config)
            .is_err(),
        "anonymous should be rejected from lens set (migration-set)"
    );
    assert!(
        node.lens_set_with_identity(
            "dummy-src-v1",
            "dummy-dst-v2",
            migration_config,
            outsider_key
        )
        .is_err(),
        "outsider should be rejected from lens set (migration-set)"
    );
    // Admin call: will fail because dummy version IDs don't exist, but must pass NAC gate.
    let admin_migration_result =
        node.lens_set_with_identity("dummy-src-v1", "dummy-dst-v2", migration_config, &admin_key);
    if let Err(ref e) = admin_migration_result {
        let msg = e.to_string().to_lowercase();
        assert!(
            !msg.contains("unauthorized") && !msg.contains("forbidden") && !msg.contains("nac"),
            "admin should pass NAC gate for lens set, got: {e}"
        );
    }

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

    // =========================================================================
    // P2P connect — anonymous rejected, outsider rejected, admin passes NAC gate
    // Note: admin call will fail because the peer doesn't exist, but must pass NAC.
    // =========================================================================
    let dummy_addr =
        "/ip4/127.0.0.1/tcp/19999/p2p/12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN";
    assert!(
        node.p2p_connect(&[dummy_addr]).is_err(),
        "anonymous should be rejected from p2p connect"
    );
    assert!(
        node.p2p_connect_with_identity(&[dummy_addr], outsider_key)
            .is_err(),
        "outsider should be rejected from p2p connect"
    );
    let admin_connect = node.p2p_connect_with_identity(&[dummy_addr], &admin_key);
    if let Err(ref e) = admin_connect {
        let msg = e.to_string().to_lowercase();
        assert!(
            !msg.contains("unauthorized") && !msg.contains("forbidden") && !msg.contains("nac"),
            "admin should pass NAC gate for p2p connect, got: {e}"
        );
    }

    // =========================================================================
    // P2P sync collection versions — anonymous rejected, outsider rejected, admin passes NAC
    // =========================================================================
    assert!(
        node.p2p_collection_sync_versions(&["dummy-version-id"])
            .is_err(),
        "anonymous should be rejected from sync collection versions"
    );
    assert!(
        node.p2p_collection_sync_versions_with_identity(&["dummy-version-id"], outsider_key)
            .is_err(),
        "outsider should be rejected from sync collection versions"
    );
    let admin_sync_versions =
        node.p2p_collection_sync_versions_with_identity(&["dummy-version-id"], &admin_key);
    if let Err(ref e) = admin_sync_versions {
        let msg = e.to_string().to_lowercase();
        assert!(
            !msg.contains("unauthorized") && !msg.contains("forbidden") && !msg.contains("nac"),
            "admin should pass NAC gate for sync collection versions, got: {e}"
        );
    }

    // =========================================================================
    // P2P sync branchable collection — anonymous rejected, outsider rejected, admin passes NAC
    // =========================================================================
    assert!(
        node.p2p_collection_sync_branchable("1").is_err(),
        "anonymous should be rejected from sync branchable collection"
    );
    assert!(
        node.p2p_collection_sync_branchable_with_identity("1", outsider_key)
            .is_err(),
        "outsider should be rejected from sync branchable collection"
    );
    let admin_sync_branchable = node.p2p_collection_sync_branchable_with_identity("1", &admin_key);
    if let Err(ref e) = admin_sync_branchable {
        let msg = e.to_string().to_lowercase();
        assert!(
            !msg.contains("unauthorized") && !msg.contains("forbidden") && !msg.contains("nac"),
            "admin should pass NAC gate for sync branchable collection, got: {e}"
        );
    }
}
