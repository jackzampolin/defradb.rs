use crate::one_to_many_common::{add_author, add_book, add_schema};
use integration_test::{DefraClient, TestCluster};

/// Mirrors the Go one_to_many fixture: authors first, then books in the order
/// listed by the Go tests, so child scan order == insertion order.
fn setup(node: &DefraClient) {
    add_schema(node);

    let john = add_author(node, "John Grisham", 65, true);
    let cornelia = add_author(node, "Cornelia Funke", 62, false);

    add_book(node, "Painted House", 4.9, &john);
    add_book(node, "A Time for Mercy", 4.5, &john);
    add_book(node, "The Associate", 4.2, &john);
    add_book(node, "Theif Lord", 4.8, &cornelia);
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

    assert_eq!(
        result["Author"],
        serde_json::json!([
            {"name": "John Grisham", "totalRating": 13.600000000000001},
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

    assert_eq!(
        result["Author"],
        serde_json::json!([
            {"name": "John Grisham", "SUM": 9.4},
            {"name": "Cornelia Funke", "SUM": 4.8},
        ])
    );
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

    assert_eq!(
        result["Author"],
        serde_json::json!([
            {"name": "John Grisham", "SUM": 8.7},
            {"name": "Cornelia Funke", "SUM": 0},
        ])
    );
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
