//! Management-channel auth (NAC) family — node-administration endpoints are
//! identity-gated. With NAC enabled, the startup admin can perform privileged
//! index-management operations, while a freshly generated non-admin identity
//! and an anonymous caller are both rejected for the exact same operation.
//!
//! Anti-tautology: every gated operation is first proven to SUCCEED for the
//! admin (a hard `.expect`) before the non-admin/anonymous denials are asserted.
//! If NAC silently failed open, the admin-success step would still pass but the
//! denial assertions would fail; if NAC failed closed for everyone, the
//! admin-success step itself would fail. Either regression breaks the test.

use crate::support;
use defra_harness::{generate_identity, TestCluster};

const NAC_SCHEMA: &str = r#"
type Product {
    name: String
    sku: String
    price: Int
}
"#;

#[tokio::test]
async fn nac_management_requires_admin() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .with_nac()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build NAC-enabled single-node cluster");

    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    // The startup (root admin) identity provisioned when NAC is enabled.
    let admin_key = cluster
        .startup_identity()
        .expect("NAC cluster must expose a startup admin identity")
        .to_string();

    // A real, well-formed identity that is NOT the admin and holds no grants.
    let outsider = generate_identity(&binary).expect("generate non-admin identity");
    let outsider_key = &outsider.private_key_hex;

    // Setup: the admin deploys the schema (itself a NAC-gated op that must
    // succeed for the admin, establishing that the admin path is live).
    node.schema_add_with_identity(NAC_SCHEMA, &admin_key)
        .expect("admin deploys schema");

    // ====================================================================
    // index create — POSITIVE FIRST (anti-tautology), then the two denials.
    // ====================================================================
    node.index_create_with_identity("Product", &["name"], Some("idx_name"), false, &admin_key)
        .expect("admin must be able to create an index (NAC management op)");

    assert!(
        node.index_create("Product", &["sku"], Some("idx_sku_anon"), false,)
            .is_err(),
        "anonymous caller must be rejected from index create"
    );
    assert!(
        node.index_create_with_identity(
            "Product",
            &["sku"],
            Some("idx_sku_out"),
            false,
            outsider_key,
        )
        .is_err(),
        "non-admin identity must be rejected from index create"
    );

    // ====================================================================
    // index list — admin succeeds; anonymous and non-admin rejected.
    // ====================================================================
    node.index_list_with_identity(Some("Product"), &admin_key)
        .expect("admin must be able to list indexes (NAC management op)");

    assert!(
        node.index_list(Some("Product")).is_err(),
        "anonymous caller must be rejected from index list"
    );
    assert!(
        node.index_list_with_identity(Some("Product"), outsider_key)
            .is_err(),
        "non-admin identity must be rejected from index list"
    );

    // ====================================================================
    // index delete — admin succeeds; anonymous and non-admin rejected.
    // Denials are asserted BEFORE the admin delete so the target index still
    // exists when the non-admins attempt it (a missing index could otherwise
    // mask a NAC gate behind a not-found error).
    // ====================================================================
    assert!(
        node.index_delete("Product", "idx_name").is_err(),
        "anonymous caller must be rejected from index delete"
    );
    assert!(
        node.index_delete_with_identity("Product", "idx_name", outsider_key)
            .is_err(),
        "non-admin identity must be rejected from index delete"
    );
    node.index_delete_with_identity("Product", "idx_name", &admin_key)
        .expect("admin must be able to delete an index (NAC management op)");
}
