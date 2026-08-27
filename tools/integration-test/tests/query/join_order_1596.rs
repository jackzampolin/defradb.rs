use integration_test::{DefraClient, TestCluster};

/// Mirrors the Go one_to_many fixture: authors first, then books in the order
/// listed by the Go tests, so child scan order == insertion order.
fn setup(node: &DefraClient) {
    node.schema_add(
        r#"
        type Book {
            name: String
            rating: Float
            author: Author
        }

        type Author {
            name: String
            age: Int
            verified: Boolean
            published: [Book]
        }
        "#,
    )
    .expect("add schema");

    let john = node
        .query(
            r#"mutation { add_Author(input: {name: "John Grisham", age: 65, verified: true}) { _docID } }"#,
        )
        .expect("add John");
    let john_id = john["add_Author"][0]["_docID"]
        .as_str()
        .unwrap()
        .to_string();

    let cornelia = node
        .query(
            r#"mutation { add_Author(input: {name: "Cornelia Funke", age: 62, verified: false}) { _docID } }"#,
        )
        .expect("add Cornelia");
    let cornelia_id = cornelia["add_Author"][0]["_docID"]
        .as_str()
        .unwrap()
        .to_string();

    for (name, rating, author) in [
        ("Painted House", 4.9, &john_id),
        ("A Time for Mercy", 4.5, &john_id),
        ("The Associate", 4.2, &john_id),
        ("Theif Lord", 4.8, &cornelia_id),
    ] {
        node.query(&format!(
            r#"mutation {{ add_Book(input: {{name: "{}", rating: {}, author: "{}"}}) {{ _docID }} }}"#,
            name, rating, author
        ))
        .unwrap_or_else(|e| panic!("add {}: {}", name, e));
    }
}

fn sum_for(result: &serde_json::Value, author: &str, key: &str) -> f64 {
    result["Author"]
        .as_array()
        .expect("Author array")
        .iter()
        .find(|a| a["name"] == author)
        .unwrap_or_else(|| panic!("author {} missing", author))[key]
        .as_f64()
        .expect("numeric aggregate")
}

async fn sum_with_alias_order_test(cluster: TestCluster) {
    let node = cluster.client(0);
    setup(&node);

    let result = node
        .query(
            r#"
            query {
                Author(order: {_alias: {totalRating: DESC}}) {
                    name
                    totalRating: SUM(published: {field: rating})
                }
            }
            "#,
        )
        .expect("query authors");

    // Go asserts 13.600000000000001 here (4.9 + 4.5 + 4.2 accumulated in
    // insertion order). Our response path cannot express that 17th significant
    // digit, so the order-sensitive assertions live in the limit/offset tests
    // below, where the difference is visible at 2 significant digits.
    assert_eq!(
        result["Author"],
        serde_json::json!([
            {"name": "John Grisham", "totalRating": 13.6},
            {"name": "Cornelia Funke", "totalRating": 4.8},
        ])
    );
}

async fn sum_with_limit_test(cluster: TestCluster) {
    let node = cluster.client(0);
    setup(&node);

    let result = node
        .query(
            r#"
            query {
                Author {
                    name
                    SUM(published: {field: rating, limit: 2})
                }
            }
            "#,
        )
        .expect("query authors");

    assert_eq!(sum_for(&result, "John Grisham", "SUM"), 9.4);
    assert_eq!(sum_for(&result, "Cornelia Funke", "SUM"), 4.8);
}

async fn sum_with_offset_limit_test(cluster: TestCluster) {
    let node = cluster.client(0);
    setup(&node);

    let result = node
        .query(
            r#"
            query {
                Author {
                    name
                    SUM(published: {field: rating, offset: 1, limit: 2})
                }
            }
            "#,
        )
        .expect("query authors");

    assert_eq!(sum_for(&result, "John Grisham", "SUM"), 8.7);
    assert_eq!(sum_for(&result, "Cornelia Funke", "SUM"), 0.0);
}

async fn relation_render_order_test(cluster: TestCluster) {
    let node = cluster.client(0);
    setup(&node);

    let result = node
        .query(
            r#"
            query {
                Author(filter: {name: {_eq: "John Grisham"}}) {
                    name
                    published { name }
                }
            }
            "#,
        )
        .expect("query authors");

    assert_eq!(
        result["Author"][0]["published"],
        serde_json::json!([
            {"name": "Painted House"},
            {"name": "A Time for Mercy"},
            {"name": "The Associate"},
        ])
    );
}

#[tokio::test]
async fn rust_join_order_1596_sum_with_alias_order() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    sum_with_alias_order_test(cluster).await;
}

#[tokio::test]
async fn rust_join_order_1596_sum_with_limit() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    sum_with_limit_test(cluster).await;
}

#[tokio::test]
async fn rust_join_order_1596_sum_with_offset_limit() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    sum_with_offset_limit_test(cluster).await;
}

#[tokio::test]
async fn rust_join_order_1596_relation_render_order() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    relation_render_order_test(cluster).await;
}
