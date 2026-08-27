//! A stored `Float` must round-trip its exact f64 bits. `13.600000000000001`
//! is a different double from `13.6`, and Go DefraDB returns it verbatim.

use crate::one_to_many_common::{add_author, add_book, add_schema};
use integration_test::{DefraClient, TestCluster};

/// `4.9 + 4.5 + 4.2` accumulated in insertion order.
const PRECISE: f64 = 13.600000000000001;

fn add_rated(node: &DefraClient) {
    node.schema_add("type Rated { name: String rating: Float }")
        .expect("add Rated schema");
    node.collection_create(
        "Rated",
        r#"{"name": "precise", "rating": 13.600000000000001}"#,
    )
    .expect("create Rated doc");
}

async fn create_round_trip_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_rated(&node);

    let result = node
        .query("query { Rated { name rating } }")
        .expect("query Rated");

    assert_eq!(
        result["Rated"],
        serde_json::json!([{"name": "precise", "rating": PRECISE}])
    );
}

async fn filter_round_trip_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_rated(&node);

    let result = node
        .query("query { Rated(filter: {rating: {_eq: 13.600000000000001}}) { name } }")
        .expect("query Rated by exact rating");

    assert_eq!(result["Rated"], serde_json::json!([{"name": "precise"}]));
}

async fn sum_test(cluster: TestCluster) {
    let node = cluster.client(0);
    add_schema(&node);

    let john = add_author(&node, "John Grisham", 65, true);
    add_book(&node, "Painted House", 4.9, &john);
    add_book(&node, "A Time for Mercy", 4.5, &john);
    add_book(&node, "The Associate", 4.2, &john);

    let result = node
        .query("query { Author { name totalRating: SUM(published: {field: rating}) } }")
        .expect("query authors");

    assert_eq!(
        result["Author"],
        serde_json::json!([{"name": "John Grisham", "totalRating": PRECISE}])
    );
}

#[tokio::test]
async fn rust_float_precision_create_round_trip() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    create_round_trip_test(cluster).await;
}

#[tokio::test]
async fn rust_float_precision_filter_round_trip() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    filter_round_trip_test(cluster).await;
}

#[tokio::test]
async fn rust_float_precision_sum() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    sum_test(cluster).await;
}
