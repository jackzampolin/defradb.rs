use std::collections::BTreeSet;

use integration_test::{for_each_runtime, TestCluster};

async fn json_missing_key_in_test(cluster: TestCluster) {
    let client = cluster.client(0);

    client
        .schema_add("type Widget { name: String meta: JSON }")
        .expect("add Widget schema");
    client
        .query(
            r#"mutation {
                add_Widget(input: {name: "scored", meta: {score: 5}}) { _docID }
            }"#,
        )
        .expect("add scored Widget");
    client
        .query(
            r#"mutation {
                add_Widget(input: {name: "missing", meta: {other: 1}}) { _docID }
            }"#,
        )
        .expect("add Widget without score");

    let result = client
        .query(
            r#"query {
                Widget(filter: {meta: {score: {_in: [null, 5]}}}) { name }
            }"#,
        )
        .expect("query score membership including null");
    let names: BTreeSet<_> = result["Widget"]
        .as_array()
        .expect("Widget rows")
        .iter()
        .map(|row| row["name"].as_str().expect("Widget name"))
        .collect();
    assert_eq!(names, BTreeSet::from(["missing", "scored"]));

    let result = client
        .query(
            r#"query {
                Widget(filter: {meta: {score: {_in: [5]}}}) { name }
            }"#,
        )
        .expect("query score membership excluding null");
    assert_eq!(result["Widget"], serde_json::json!([{ "name": "scored" }]));
}

for_each_runtime!(json_missing_key_in, json_missing_key_in_test);
