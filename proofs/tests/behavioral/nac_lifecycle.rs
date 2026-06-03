use crate::support;
use defra_harness::fixtures::PRODUCT_SCHEMA;
use defra_harness::{generate_identity, TestCluster};

/// NAC lifecycle privilege-escalation: a NAC `admin` grant that is later
/// revoked must not leave the actor with residual admin capability.
///
/// The probe is `index new` — a NAC-gated control-plane op (see
/// crates/.../nac gating; mirrors tools/integration-test/tests/nac/
/// relation_admin.rs which proves grant→use→revoke→deny end to end).
///
/// Anti-tautology, asserted *before* every negative:
///   (A) the startup admin can perform the op  -> the op is genuinely reachable;
///   (B) after the grant, the outsider CAN perform the op -> the deny in the
///       final phase is a real loss of capability, not a setup that never worked.
/// Without (A)/(B) an unconditional failure of `index new` (wrong collection,
/// build error, dead node) would vacuously satisfy the "denied" assertions.
#[tokio::test]
async fn nac_lifecycle_no_escalation() {
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

    // The startup identity is the bootstrap NAC admin minted by `with_nac()`.
    let admin_key = cluster
        .startup_identity()
        .expect("NAC cluster must have a startup admin identity")
        .to_string();

    // A fresh identity that is NEVER part of the admin relationship at start.
    let outsider = generate_identity(&binary).expect("generate outsider identity");
    let outsider_key = outsider.private_key_hex.clone();

    // Admin deploys the schema so `index new` has a real target collection.
    node.schema_add_with_identity(PRODUCT_SCHEMA, &admin_key)
        .expect("admin deploys Product schema");

    // ---------------------------------------------------------------------
    // (A) POSITIVE BASELINE — the gated op is reachable for a real admin.
    // Asserted before any negative so a later "denied" can't pass vacuously.
    // ---------------------------------------------------------------------
    node.index_create_with_identity(
        "Product",
        &["name"],
        Some("idx_admin_baseline"),
        false,
        &admin_key,
    )
    .expect("startup admin must be able to create an index (gated op is reachable)");

    // ---------------------------------------------------------------------
    // NEGATIVE BASELINE — never-granted outsider is denied the admin op.
    // Establishes that the gate actually distinguishes admin from non-admin
    // (so the grant in the next phase is meaningful, not a no-op gate).
    // ---------------------------------------------------------------------
    assert!(
        node.index_create_with_identity(
            "Product",
            &["sku"],
            Some("idx_outsider_pre"),
            false,
            &outsider_key,
        )
        .is_err(),
        "never-granted outsider must be denied the NAC-gated index op"
    );

    // ---------------------------------------------------------------------
    // LIFECYCLE STEP 1: grant the outsider NAC `admin`.
    // (B) SECOND ANTI-TAUTOLOGY: outsider must now SUCCEED at the same op.
    // This proves the grant path is real and the op is achievable by this
    // identity, so the post-revoke deny is a genuine capability loss.
    // ---------------------------------------------------------------------
    node.acp_node_relationship_add("admin", &outsider.did, &admin_key)
        .expect("admin grants outsider the NAC admin relationship");

    node.index_create_with_identity(
        "Product",
        &["sku"],
        Some("idx_outsider_granted"),
        false,
        &outsider_key,
    )
    .expect("granted NAC admin must be able to perform the gated index op");

    // ---------------------------------------------------------------------
    // LIFECYCLE STEP 2: revoke the outsider's NAC `admin`.
    // INVARIANT (the escalation test): the formerly-admin identity must now
    // be denied the gated op — no residual/peak-historical privilege.
    // ---------------------------------------------------------------------
    node.acp_node_relationship_delete("admin", &outsider.did, &admin_key)
        .expect("admin revokes outsider's NAC admin relationship");

    assert!(
        node.index_create_with_identity(
            "Product",
            &["price"],
            Some("idx_outsider_post_revoke"),
            false,
            &outsider_key,
        )
        .is_err(),
        "PRIVILEGE ESCALATION: revoked NAC admin retained admin capability \
         (index op succeeded after revoke) — a removed admin must be denied"
    );

    // ---------------------------------------------------------------------
    // Lifecycle integrity: the legitimate admin is unaffected by the
    // grant/revoke churn — confirms the deny above is scoped to the revoked
    // actor, not a global breakage of the gate after relationship edits.
    // ---------------------------------------------------------------------
    node.index_create_with_identity(
        "Product",
        &["name", "sku"],
        Some("idx_admin_after_lifecycle"),
        false,
        &admin_key,
    )
    .expect("startup admin must still hold admin capability after the lifecycle");
}
