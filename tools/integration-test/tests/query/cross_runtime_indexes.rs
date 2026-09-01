use integration_test::{assert_query_equivalent, DefraClient, TestCluster};
use serde_json::{json, Value};

const SCHEMA: &str = r#"
    type IndexedValue @index(includes: [{field: "group", direction: ASC}, {field: "rank", direction: DESC}]) {
        name: String!
        integer: Int @index
        ratio: Float64 @index
        text: String @index
        enabled: Boolean @index
        observed: DateTime @index
        optionalInteger: Int @index
        group: String
        rank: Int
    }

    type ArrayValue {
        name: String!
        values: [Int!] @index
    }

    type UniqueArrayValue {
        name: String!
        values: [Int!] @index(unique: true)
    }
"#;

const DOCUMENTS: [&str; 6] = [
    r#"{
        "name": "minimum",
        "integer": -9223372036854775808,
        "ratio": -10.5,
        "text": "",
        "enabled": false,
        "observed": "1600-01-01T00:00:00Z",
        "optionalInteger": null,
        "group": "Alice",
        "rank": 22
    }"#,
    r#"{
        "name": "negative",
        "integer": -9,
        "ratio": -1.25,
        "text": "alpha",
        "enabled": true,
        "observed": "1969-12-31T23:59:59Z",
        "optionalInteger": -1,
        "group": "Alan",
        "rank": 29
    }"#,
    r#"{
        "name": "negative_zero",
        "integer": -1,
        "ratio": -0.0,
        "text": "zeta",
        "enabled": false,
        "observed": "2000-01-01T00:00:00Z",
        "group": "Alice",
        "rank": 38
    }"#,
    r#"{
        "name": "positive_zero",
        "integer": 0,
        "ratio": 0.0,
        "text": "alphabet",
        "enabled": true,
        "observed": "2024-06-01T12:00:00Z",
        "optionalInteger": 0,
        "group": "Andy",
        "rank": 24
    }"#,
    r#"{
        "name": "positive",
        "integer": 9,
        "ratio": 1.25,
        "text": "東京",
        "enabled": false,
        "observed": "2400-01-01T00:00:00Z",
        "optionalInteger": 1,
        "group": "Alice",
        "rank": 24
    }"#,
    r#"{
        "name": "maximum",
        "integer": 9223372036854775807,
        "ratio": 42.5,
        "text": "🙂",
        "enabled": true,
        "observed": "9999-12-31T23:59:59Z",
        "optionalInteger": 9223372036854775807,
        "group": "Bob",
        "rank": 1
    }"#,
];

fn create_both(rust: &DefraClient, go: &DefraClient, collection: &str, document: &str) {
    rust.collection_create(collection, document)
        .unwrap_or_else(|error| panic!("Rust create failed: {error}\n{document}"));
    go.collection_create(collection, document)
        .unwrap_or_else(|error| panic!("Go create failed: {error}\n{document}"));
}

fn contains_index_fetch(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(contains_index_fetch),
        Value::Object(values) => values.iter().any(|(key, value)| {
            (key == "indexFetches" && value.as_u64().is_some_and(|count| count > 0))
                || contains_index_fetch(value)
        }),
        _ => false,
    }
}

fn assert_uses_index(runtime: &str, node: &DefraClient, query: &str) {
    let explain_query = query.replacen("query", "query @explain(type: execute)", 1);
    let explain = node
        .query(&explain_query)
        .unwrap_or_else(|error| panic!("{runtime} explain failed: {error}\n{explain_query}"));
    assert!(
        contains_index_fetch(&explain),
        "{runtime} did not report an index fetch for {query}: {explain}"
    );
}

fn assert_index_query(rust: &DefraClient, go: &DefraClient, query: &str, expected_names: &[&str]) {
    let response = assert_query_equivalent(rust, go, query);
    let rows = response["IndexedValue"]
        .as_array()
        .unwrap_or_else(|| panic!("IndexedValue array missing from {response}"));
    let names: Vec<_> = rows
        .iter()
        .map(|row| row["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, expected_names, "unexpected index order for {query}");
    assert_uses_index("Rust", rust, query);
    assert_uses_index("Go", go, query);
}

#[tokio::test]
async fn go_cross_runtime_index_key_ordering() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .build()
        .await
        .unwrap();
    let rust = cluster.client(0);
    let go = cluster.client(1);
    rust.schema_add(SCHEMA).expect("add Rust schema");
    go.schema_add(SCHEMA).expect("add Go schema");

    for document in DOCUMENTS {
        create_both(&rust, &go, "IndexedValue", document);
    }

    assert_index_query(
        &rust,
        &go,
        "query { IndexedValue(order: {integer: ASC}) { name integer } }",
        &[
            "minimum",
            "negative",
            "negative_zero",
            "positive_zero",
            "positive",
            "maximum",
        ],
    );
    assert_index_query(
        &rust,
        &go,
        "query { IndexedValue(filter: {integer: {_gt: -10}}, order: {integer: ASC}) { name integer } }",
        &[
            "negative",
            "negative_zero",
            "positive_zero",
            "positive",
            "maximum",
        ],
    );
    assert_index_query(
        &rust,
        &go,
        "query { IndexedValue(filter: {ratio: {_geq: -10.5}}, order: {ratio: ASC}) { name ratio } }",
        &[
            "minimum",
            "negative",
            "negative_zero",
            "positive_zero",
            "positive",
            "maximum",
        ],
    );
    assert_index_query(
        &rust,
        &go,
        "query { IndexedValue(order: {text: ASC}) { name text } }",
        &[
            "minimum",
            "negative",
            "positive_zero",
            "negative_zero",
            "positive",
            "maximum",
        ],
    );
    assert_index_query(
        &rust,
        &go,
        r#"query { IndexedValue(filter: {text: {_like: "alpha%"}}) { name text } }"#,
        &["negative", "positive_zero"],
    );

    let bool_query = "query { IndexedValue(filter: {enabled: {_in: [false, true]}}, order: {enabled: ASC}) { enabled } }";
    let bool_response = assert_query_equivalent(&rust, &go, bool_query);
    let bools: Vec<_> = bool_response["IndexedValue"]
        .as_array()
        .expect("IndexedValue array")
        .iter()
        .map(|row| row["enabled"].as_bool().expect("enabled"))
        .collect();
    assert_eq!(bools, [false, false, false, true, true, true]);
    assert_uses_index("Rust", &rust, bool_query);
    assert_uses_index("Go", &go, bool_query);

    assert_index_query(
        &rust,
        &go,
        r#"query { IndexedValue(filter: {observed: {_geq: "1600-01-01T00:00:00Z"}}, order: {observed: ASC}) { name observed } }"#,
        &[
            "minimum",
            "negative",
            "negative_zero",
            "positive_zero",
            "positive",
            "maximum",
        ],
    );

    let nullable_query =
        "query { IndexedValue(order: {optionalInteger: ASC}) { optionalInteger } }";
    let nullable_response = assert_query_equivalent(&rust, &go, nullable_query);
    let values: Vec<_> = nullable_response["IndexedValue"]
        .as_array()
        .expect("IndexedValue array")
        .iter()
        .map(|row| row["optionalInteger"].clone())
        .collect();
    assert_eq!(
        values,
        [
            Value::Null,
            Value::Null,
            json!(-1),
            json!(0),
            json!(1),
            json!(i64::MAX),
        ]
    );
    assert_uses_index("Rust", &rust, nullable_query);
    assert_uses_index("Go", &go, nullable_query);

    assert_index_query(
        &rust,
        &go,
        r#"query { IndexedValue(filter: {group: {_like: "Al%"}}) { name group rank } }"#,
        &["negative", "negative_zero", "positive", "minimum"],
    );

    for document in [
        r#"{"name":"repeated","values":[5,5,8]}"#,
        r#"{"name":"other","values":[5,9]}"#,
    ] {
        create_both(&rust, &go, "ArrayValue", document);
    }
    let array_query = "query { ArrayValue(filter: {values: {_any: {_eq: 5}}}, order: {name: ASC}) { name values } }";
    let array_response = assert_query_equivalent(&rust, &go, array_query);
    assert_eq!(
        array_response["ArrayValue"],
        json!([
            {"name": "other", "values": [5, 9]},
            {"name": "repeated", "values": [5, 5, 8]}
        ])
    );
    assert_uses_index("Rust", &rust, array_query);
    assert_uses_index("Go", &go, array_query);

    let repeated_unique = r#"mutation { add_UniqueArrayValue(input: {name: "accepted", values: [7, 7, 8]}) { name values } }"#;
    assert_query_equivalent(&rust, &go, repeated_unique);

    let collision = r#"{"name":"rejected","values":[8,9]}"#;
    let _ = rust
        .collection_create("UniqueArrayValue", collision)
        .expect_err("Rust must reject a cross-document unique collision");
    let _ = go
        .collection_create("UniqueArrayValue", collision)
        .expect_err("Go must reject a cross-document unique collision");

    let unique_query =
        "query { UniqueArrayValue(filter: {values: {_any: {_eq: 7}}}) { name values } }";
    let unique_response = assert_query_equivalent(&rust, &go, unique_query);
    assert_eq!(
        unique_response["UniqueArrayValue"],
        json!([{"name": "accepted", "values": [7, 7, 8]}])
    );
    assert_uses_index("Rust", &rust, unique_query);
    assert_uses_index("Go", &go, unique_query);
}
