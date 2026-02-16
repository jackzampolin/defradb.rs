use integration_test::{for_each_runtime, TestCluster, PRODUCT_SCHEMA};
use serde_json::Value;

/// Extract array length from encrypted-index list output.
/// Handles both `[...]` and `{"indexes": [...]}` formats.
fn index_count(val: &Value) -> usize {
    if let Some(arr) = val.as_array() {
        return arr.len();
    }
    if let Some(obj) = val.as_object() {
        for v in obj.values() {
            if let Some(arr) = v.as_array() {
                return arr.len();
            }
        }
    }
    0
}

async fn encrypted_index_test(cluster: TestCluster) {
    let node = cluster.client(0);

    // Deploy Product schema
    node.schema_add(PRODUCT_SCHEMA).expect("add Product schema");

    // Create 3 products
    node.query(
        r#"mutation { create_Product(input: {name: "Widget", sku: "W001", price: 100}) { _docID } }"#,
    )
    .expect("create product 1");
    node.query(
        r#"mutation { create_Product(input: {name: "Gadget", sku: "G002", price: 200}) { _docID } }"#,
    )
    .expect("create product 2");
    node.query(
        r#"mutation { create_Product(input: {name: "Doohickey", sku: "D003", price: 50}) { _docID } }"#,
    )
    .expect("create product 3");

    // Create encrypted index on name
    node.encrypted_index_create("Product", "name")
        .expect("create encrypted index on name");

    // Create encrypted index on sku
    node.encrypted_index_create("Product", "sku")
        .expect("create encrypted index on sku");

    // List encrypted indexes — should be 2
    let list1 = node
        .encrypted_index_list("Product")
        .expect("list encrypted indexes");
    assert_eq!(
        index_count(&list1),
        2,
        "expected 2 encrypted indexes, got: {}",
        serde_json::to_string_pretty(&list1).unwrap()
    );

    // Verify queries still work with encrypted indexes
    let products = node
        .query("query { Product { name sku price } }")
        .expect("query products");
    let product_arr = products["Product"].as_array().expect("products array");
    assert_eq!(product_arr.len(), 3, "should still have 3 products");

    // Delete encrypted index on name
    node.encrypted_index_delete("Product", "name")
        .expect("delete encrypted index on name");

    // List — should be 1
    let list2 = node
        .encrypted_index_list("Product")
        .expect("list after first delete");
    assert_eq!(index_count(&list2), 1, "expected 1 encrypted index");

    // Delete encrypted index on sku
    node.encrypted_index_delete("Product", "sku")
        .expect("delete encrypted index on sku");

    // List — should be 0
    let list3 = node
        .encrypted_index_list("Product")
        .expect("list after second delete");
    assert_eq!(index_count(&list3), 0, "expected 0 encrypted indexes");
}

for_each_runtime!(encrypted_index, encrypted_index_test, .with_encryption());
