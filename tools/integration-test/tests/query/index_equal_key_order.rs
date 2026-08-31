use integration_test::{DefraClient, TestCluster};

fn add_user(node: &DefraClient, name: &str) -> String {
    let result = node
        .query(&format!(
            r#"mutation {{ add_User(input: {{name: "{name}", age: 21}}) {{ _docID }} }}"#
        ))
        .unwrap_or_else(|e| panic!("add {name}: {e}"));
    result["add_User"][0]["_docID"]
        .as_str()
        .unwrap_or_else(|| panic!("missing _docID for {name}: {result}"))
        .to_string()
}

fn delete_users(node: &DefraClient, ids: &[String]) {
    let quoted = ids
        .iter()
        .map(|id| format!("\"{id}\""))
        .collect::<Vec<_>>()
        .join(", ");
    node.query(&format!(
        r#"mutation {{ delete_User(docIDs: [{quoted}]) {{ _docID }} }}"#
    ))
    .unwrap_or_else(|e| panic!("delete {ids:?}: {e}"));
}

/// Insert two age=21 users so the lexicographically larger public DocID is
/// assigned the smaller node-local short ID.
fn seed_reversed_cid_pair(node: &DefraClient) -> (String, String, String, String) {
    let candidates = [
        ("John", "Andy"),
        ("zeta", "alpha"),
        ("zz", "aa"),
        ("UserZ", "UserA"),
        ("m", "a"),
    ];

    for (first_name, second_name) in candidates {
        let first_id = add_user(node, first_name);
        let second_id = add_user(node, second_name);
        if second_id.as_str() < first_id.as_str() {
            return (
                first_name.to_string(),
                first_id,
                second_name.to_string(),
                second_id,
            );
        }
        delete_users(node, &[first_id, second_id]);
    }
    panic!("could not find a pair whose public DocID order opposes insert order");
}

fn assert_doc_id_order(
    node: &DefraClient,
    query: &str,
    first_name: &str,
    first_id: &str,
    second_name: &str,
    second_id: &str,
) {
    let result = node.query(query).expect("query equal indexed keys");
    let users = result["User"]
        .as_array()
        .unwrap_or_else(|| panic!("User array missing from {result}"));
    assert_eq!(users.len(), 2, "expected both age=21 users, got {result}");

    let got_names: Vec<&str> = users
        .iter()
        .map(|row| row["name"].as_str().expect("name"))
        .collect();
    let got_ids: Vec<&str> = users
        .iter()
        .map(|row| row["_docID"].as_str().expect("_docID"))
        .collect();

    assert_eq!(
        got_ids,
        vec![second_id, first_id],
        "equal index keys must come back in public DocID order \
         (inserted {first_name} then {second_name}); got {got_names:?}"
    );
    assert_eq!(got_names, vec![second_name, first_name]);
}

fn add_schema(node: &DefraClient) {
    node.schema_add(
        r#"
        type User {
            name: String
            age: Int @index
        }
        "#,
    )
    .expect("add schema");
}

/// Two documents that share an indexed value, queried with no `order` clause,
/// must come back in public DocID order (#1602).
#[tokio::test]
async fn rust_equal_index_keys_are_ordered_by_doc_id() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    add_schema(&node);
    let (first_name, first_id, second_name, second_id) = seed_reversed_cid_pair(&node);
    assert_doc_id_order(
        &node,
        r#"query { User(filter: {age: {_eq: 21}}) { name _docID } }"#,
        &first_name,
        &first_id,
        &second_name,
        &second_id,
    );
}

/// `_in` is InScan, not ExactMatch. TypeJoinMany uses InScan whenever there
/// are two or more parents, so equal-key groups there must use the same
/// public-DocID tie-break (#1602).
#[tokio::test]
async fn rust_equal_index_keys_in_filter_are_ordered_by_doc_id() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    add_schema(&node);
    let (first_name, first_id, second_name, second_id) = seed_reversed_cid_pair(&node);
    assert_doc_id_order(
        &node,
        r#"query { User(filter: {age: {_in: [21]}}) { name _docID } }"#,
        &first_name,
        &first_id,
        &second_name,
        &second_id,
    );
}

const PRODUCT_SCHEMA: &str = r#"
    type Product @index(fields: ["category", "rank"]) {
        category: String
        rank: Int
        name: String
    }
"#;

/// Ranks repeat so one scan covers the order across full keys and the
/// tie-break within one of them.
const PRODUCTS: [(&str, i64); 5] = [("p0", 1), ("p1", 1), ("p2", 2), ("p3", 2), ("p4", 3)];

/// A partial `_in` over a composite index prefix-scans, so one `_in` value
/// spans several distinct full index keys. The public-DocID tie-break must
/// stay inside one key rather than reorder across the secondary field (#1602).
#[tokio::test]
async fn rust_partial_in_over_composite_index_keeps_secondary_field_order() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);
    node.schema_add(PRODUCT_SCHEMA).expect("add schema");

    for (name, rank) in PRODUCTS {
        node.query(&format!(
            r#"mutation {{ add_Product(input: {{category: "a", rank: {rank}, name: "{name}"}}) {{ _docID }} }}"#
        ))
        .unwrap_or_else(|e| panic!("add {name}: {e}"));
    }

    let result = node
        .query(r#"query { Product(filter: {category: {_in: ["a"]}}) { rank _docID } }"#)
        .expect("partial _in over the composite index");
    let rows = result["Product"]
        .as_array()
        .unwrap_or_else(|| panic!("Product array missing from {result}"));
    assert_eq!(
        rows.len(),
        PRODUCTS.len(),
        "expected every product, got {result}"
    );

    let got: Vec<(i64, String)> = rows
        .iter()
        .map(|row| {
            (
                row["rank"].as_i64().expect("rank"),
                row["_docID"].as_str().expect("_docID").to_string(),
            )
        })
        .collect();

    let mut index_order = got.clone();
    index_order.sort();
    let mut doc_id_order = got.clone();
    doc_id_order.sort_by(|a, b| a.1.cmp(&b.1));
    assert_ne!(
        index_order, doc_id_order,
        "the seed must distinguish index order from a flat DocID sort, \
         otherwise this test cannot fail"
    );
    assert_eq!(
        got, index_order,
        "a partial `_in` must return rank order, with the DocID tie-break \
         applied only within one rank"
    );
}
