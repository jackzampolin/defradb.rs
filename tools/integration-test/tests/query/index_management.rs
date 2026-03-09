use integration_test::{for_each_runtime, TestCluster, PRODUCT_SCHEMA};
use serde_json::Value;

/// Extract indexes from list output, handling both flat array `[...]`
/// and Go's map format `{"CollectionName": [...]}`.
fn extract_indexes(val: &Value) -> Vec<&Value> {
    if let Some(arr) = val.as_array() {
        return arr.iter().collect();
    }
    if let Some(obj) = val.as_object() {
        for v in obj.values() {
            if let Some(arr) = v.as_array() {
                return arr.iter().collect();
            }
        }
    }
    vec![]
}

/// Check if any index in the list has the given name (handles both `name` and `Name` keys).
fn has_index_name(indexes: &[&Value], name: &str) -> bool {
    indexes.iter().any(|idx| {
        idx.get("name")
            .or_else(|| idx.get("Name"))
            .and_then(|v| v.as_str())
            .map(|n| n == name)
            .unwrap_or(false)
    })
}

async fn index_management_test(cluster: TestCluster) {
    let node = cluster.client(0);

    // Deploy Product schema
    node.schema_add(PRODUCT_SCHEMA).expect("add Product schema");

    // Insert sample data so queries have something to exercise
    node.query(
        r#"mutation { add_Product(input: {name: "Widget", sku: "W001", price: 100}) { _docID } }"#,
    )
    .expect("create product");

    // 1. Create a named single-field index
    node.index_create("Product", &["name"], Some("idx_name"), false)
        .expect("create index idx_name");

    // 2. Create a unique index
    node.index_create("Product", &["sku"], Some("idx_sku_unique"), true)
        .expect("create unique index idx_sku_unique");

    // 3. Create a composite (multi-field) index
    node.index_create("Product", &["name", "price"], Some("idx_name_price"), false)
        .expect("create composite index idx_name_price");

    // 4. List all indexes for the collection — should have 3
    let list = node
        .index_list(Some("Product"))
        .expect("index_list Product");
    let indexes = extract_indexes(&list);
    assert_eq!(
        indexes.len(),
        3,
        "expected 3 indexes, got {}: {}",
        indexes.len(),
        serde_json::to_string_pretty(&list).unwrap()
    );
    assert!(
        has_index_name(&indexes, "idx_name"),
        "idx_name not found in list"
    );
    assert!(
        has_index_name(&indexes, "idx_sku_unique"),
        "idx_sku_unique not found in list"
    );
    assert!(
        has_index_name(&indexes, "idx_name_price"),
        "idx_name_price not found in list"
    );

    // 5. Delete one index and verify removal
    node.index_delete("Product", "idx_name")
        .expect("delete idx_name");

    let list_after = node
        .index_list(Some("Product"))
        .expect("index_list after drop");
    let indexes_after = extract_indexes(&list_after);
    assert_eq!(
        indexes_after.len(),
        2,
        "expected 2 indexes after drop, got {}",
        indexes_after.len()
    );
    assert!(
        !has_index_name(&indexes_after, "idx_name"),
        "idx_name should be gone after drop"
    );

    // 6. Verify queries still work with remaining indexes
    let products = node
        .query("query { Product { name sku price } }")
        .expect("query products with indexes");
    let arr = products["Product"].as_array().expect("products array");
    assert_eq!(arr.len(), 1, "should still have 1 product");
}

for_each_runtime!(index_management, index_management_test);
