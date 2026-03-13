use integration_test::TestCluster;
use serde_json::Value;

const SCHEMA: &str = "type Metric { label: String  value: Int }";

fn extract_doc_id(data: &Value, mutation_name: &str) -> String {
    data[mutation_name]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|value| value["_docID"].as_str())
        .or_else(|| data[mutation_name]["_docID"].as_str())
        .expect("missing _docID")
        .to_string()
}

fn extract_commits<'a>(data: &'a Value) -> &'a [Value] {
    data["_commits"]
        .as_array()
        .unwrap_or_else(|| {
            panic!(
                "_commits array missing from response: {}",
                serde_json::to_string_pretty(data).unwrap()
            )
        })
        .as_slice()
}

fn assert_aggregate_row(
    row: &Value,
    expected_count: i64,
    expected_sum: i64,
    expected_avg: f64,
    expected_min: i64,
    expected_max: i64,
) {
    assert_eq!(row["count"].as_i64(), Some(expected_count));
    assert_eq!(row["sum"].as_i64(), Some(expected_sum));
    assert_eq!(row["avg"].as_f64(), Some(expected_avg));
    assert_eq!(row["min"].as_i64(), Some(expected_min));
    assert_eq!(row["max"].as_i64(), Some(expected_max));
}

async fn commits_aggregate_test(cluster: TestCluster) {
    let node = cluster.client(0);
    node.schema_add(SCHEMA).expect("add schema");

    let create = node
        .query(r#"mutation { add_Metric(input: {label: "cpu", value: 10}) { _docID } }"#)
        .expect("create metric");
    let doc_id = extract_doc_id(&create, "add_Metric");

    node.query(&format!(
        r#"mutation {{ update_Metric(docID: "{doc_id}", input: {{value: 20}}) {{ _docID }} }}"#,
    ))
    .expect("update value to 20");
    node.query(&format!(
        r#"mutation {{ update_Metric(docID: "{doc_id}", input: {{value: 30}}) {{ _docID }} }}"#,
    ))
    .expect("update value to 30");

    let all_values = node
        .query(&format!(
            r#"query {{
                _commits(
                    docID: ["{doc_id}"]
                    filter: {{fieldName: {{_eq: "value"}}}}
                ) {{
                    count: COUNT
                    sum: SUM(field: delta)
                    avg: AVG(field: delta)
                    min: MIN(field: delta)
                    max: MAX(field: delta)
                }}
            }}"#,
        ))
        .expect("aggregate all value commits");

    let all_rows = extract_commits(&all_values);
    assert_eq!(all_rows.len(), 1, "expected a single aggregate row");
    assert_aggregate_row(&all_rows[0], 3, 60, 20.0, 10, 30);

    let windowed = node
        .query(&format!(
            r#"query {{
                _commits(
                    docID: ["{doc_id}"]
                    filter: {{
                        fieldName: {{_eq: "value"}}
                        height: {{_gte: 2, _lt: 4}}
                    }}
                ) {{
                    count: COUNT
                    sum: SUM(field: delta)
                    avg: AVG(field: delta)
                    min: MIN(field: delta)
                    max: MAX(field: delta)
                }}
            }}"#,
        ))
        .expect("aggregate windowed value commits");

    let window_rows = extract_commits(&windowed);
    assert_eq!(window_rows.len(), 1, "expected a single aggregate row");
    assert_aggregate_row(&window_rows[0], 2, 50, 25.0, 20, 30);
}

#[tokio::test]
async fn rust_commits_aggregate() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    commits_aggregate_test(cluster).await;
}
