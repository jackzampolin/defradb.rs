//! End-to-end proof that multi-document / implicit-batch create mutations are
//! NAC-gated through the real node (F1: multi-doc/batch gating).
//!
//! An authorized (admin) identity may create multiple documents in a single
//! request (both array-input multi-doc create and a two-alias implicit batch);
//! an unauthorized (outsider) identity is denied the same operations.

use integration_test::{for_each_runtime, generate_identity, TestCluster, PRODUCT_SCHEMA};

for_each_runtime!(
    nac_multi_doc_create_gate,
    nac_multi_doc_create_gate_test,
    .with_acp_local().with_nac()
);

async fn nac_multi_doc_create_gate_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let admin_key = cluster
        .startup_identity()
        .expect("NAC cluster must have startup identity")
        .to_string();

    let outsider = generate_identity(&binary).expect("outsider identity");
    let outsider_key = &outsider.private_key_hex;

    node.schema_add_with_identity(PRODUCT_SCHEMA, &admin_key)
        .expect("deploy schema");

    // =========================================================================
    // Multi-doc array create — routes through create_many_impl's multi-doc
    // branch. Admin allowed, outsider denied.
    // =========================================================================
    let multi_doc_create = r#"mutation {
        add_Product(input: [
            {name: "A", sku: "A001", price: 1},
            {name: "B", sku: "B001", price: 2}
        ]) { _docID }
    }"#;

    let admin_multi = node
        .query_with_identity(multi_doc_create, &admin_key)
        .expect("admin multi-doc create should be accepted");
    let created = admin_multi["add_Product"]
        .as_array()
        .expect("add_Product returns an array");
    assert_eq!(
        created.len(),
        2,
        "admin multi-doc create should persist 2 documents"
    );

    assert!(
        node.query_with_identity(multi_doc_create, outsider_key)
            .is_err(),
        "outsider should be denied multi-doc create"
    );

    // =========================================================================
    // Implicit batch (two aliased create mutations in one request) — routes
    // through BatchMutator. Admin allowed, outsider denied.
    // =========================================================================
    let batch_create = r#"mutation {
        c: add_Product(input: {name: "C", sku: "C001", price: 3}) { _docID }
        d: add_Product(input: {name: "D", sku: "D001", price: 4}) { _docID }
    }"#;

    let admin_batch = node
        .query_with_identity(batch_create, &admin_key)
        .expect("admin implicit-batch create should be accepted");
    assert!(
        admin_batch.get("c").is_some() && admin_batch.get("d").is_some(),
        "admin implicit-batch create should resolve both aliases: {admin_batch}"
    );

    assert!(
        node.query_with_identity(batch_create, outsider_key)
            .is_err(),
        "outsider should be denied implicit-batch create"
    );
}
