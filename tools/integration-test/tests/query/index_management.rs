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

/// Check if any index result has the given name.
fn has_index_name(indexes: &[&Value], name: &str) -> bool {
    indexes.iter().any(|idx| {
        // The Rust-native CLI remains flat; Go v1 and the compatible HTTP
        // surface wrap each descriptor with its action execution.
        let desc = idx.get("Description").unwrap_or(idx);
        desc.get("name")
            .or_else(|| desc.get("Name"))
            .and_then(|v| v.as_str())
            .map(|n| n == name)
            .unwrap_or(false)
    })
}

fn assert_completed_index(index: &Value) {
    let index_id = index["Description"]["ID"]
        .as_u64()
        .expect("Go v1 index description should include ID");
    let execution = index
        .get("Execution")
        .expect("Go v1 index result should include Execution");
    assert_eq!(execution["Action"], 0);
    assert_eq!(execution["Status"], 3);
    assert!(execution["CollectionID"]
        .as_str()
        .is_some_and(|id| !id.is_empty()));
    assert_eq!(execution["Subject"], index_id.to_string());
    assert_eq!(execution["Reason"], "");
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

    let compatible_list: Value = reqwest::get(format!(
        "{}/api/v0/collections/Product/indexes",
        cluster.api_url(0)
    ))
    .await
    .expect("list Go-compatible indexes request")
    .error_for_status()
    .expect("list Go-compatible indexes status")
    .json()
    .await
    .expect("list Go-compatible indexes response");
    let compatible_indexes = extract_indexes(&compatible_list);
    assert_eq!(compatible_indexes.len(), 3);
    for index in &compatible_indexes {
        assert_completed_index(index);
    }

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

    // Completed actions are removed rather than retained as terminal records.
    node.collection_truncate("Product")
        .expect("truncate Product");
    let actions: Value = reqwest::get(format!("{}/api/v0/actions", cluster.api_url(0)))
        .await
        .expect("list actions request")
        .json()
        .await
        .expect("list actions response");
    assert_eq!(actions, serde_json::json!([]));
}

for_each_runtime!(index_management, index_management_test);
