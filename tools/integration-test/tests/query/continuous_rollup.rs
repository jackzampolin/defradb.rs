use std::sync::{Arc, Mutex};
use std::time::Duration;

use integration_test::{open_events_sse, poll_until, TestCluster};
use serde_json::Value;

const SCHEMA: &str = r#"
type CpuRaw {
  host: String
  ts: Int
  value: Int
}

type Cpu2Rollup {
  source_doc_id: String
  window_start: Int
  window_end: Int
  count: Int
  sum: Int
  avg: Float
  min: Int
  max: Int
}

type Cpu3Rollup {
  source_doc_id: String
  window_start: Int
  window_end: Int
  count: Int
  sum: Int
  avg: Float
  min: Int
  max: Int
}
"#;

const EVENT_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy)]
struct RollupSpec {
    collection: &'static str,
    window_size: i64,
}

#[derive(Debug, Clone, Copy)]
struct RollupAggregate {
    count: i64,
    sum: i64,
    avg: f64,
    min: i64,
    max: i64,
}

fn extract_doc_id(data: &Value, mutation_name: &str) -> String {
    data[mutation_name]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|value| value["_docID"].as_str())
        .or_else(|| data[mutation_name]["_docID"].as_str())
        .expect("missing _docID")
        .to_string()
}

fn extract_rows<'a>(data: &'a Value, collection: &str) -> &'a [Value] {
    data[collection]
        .as_array()
        .unwrap_or_else(|| {
            panic!(
                "{collection} array missing from response: {}",
                serde_json::to_string_pretty(data).unwrap()
            )
        })
        .as_slice()
}

fn extract_single_aggregate_row(data: &Value) -> &Value {
    let rows = extract_rows(data, "_commits");
    assert_eq!(rows.len(), 1, "expected a single aggregate row");
    &rows[0]
}

fn json_f64(value: &Value, field: &str) -> f64 {
    value[field]
        .as_f64()
        .unwrap_or_else(|| panic!("missing float field {field} in row: {}", value))
}

fn count_update_events_for_doc(events: &Arc<Mutex<Vec<Value>>>, doc_id: &str) -> usize {
    events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.pointer("/data/doc_id").and_then(Value::as_str) == Some(doc_id))
        .count()
}

fn latest_value_height(node: &integration_test::DefraClient, source_doc_id: &str) -> i64 {
    let data = node
        .query(&format!(
            r#"query {{
                _commits(
                    docID: ["{source_doc_id}"]
                    filter: {{fieldName: {{_eq: "value"}}}}
                    order: {{height: DESC}}
                    limit: 1
                ) {{
                    height
                }}
            }}"#,
        ))
        .expect("query latest value commit height");

    extract_rows(&data, "_commits")
        .first()
        .and_then(|row| row["height"].as_i64())
        .expect("latest value commit height")
}

fn aggregate_value_window(
    node: &integration_test::DefraClient,
    source_doc_id: &str,
    window_start: i64,
    window_end_exclusive: i64,
) -> RollupAggregate {
    let data = node
        .query(&format!(
            r#"query {{
                _commits(
                    docID: ["{source_doc_id}"]
                    filter: {{
                        fieldName: {{_eq: "value"}}
                        height: {{_gte: {window_start}, _lt: {window_end_exclusive}}}
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
        .expect("aggregate value commit window");

    let row = extract_single_aggregate_row(&data);
    RollupAggregate {
        count: row["count"].as_i64().expect("aggregate count"),
        sum: row["sum"].as_i64().expect("aggregate sum"),
        avg: json_f64(row, "avg"),
        min: row["min"].as_i64().expect("aggregate min"),
        max: row["max"].as_i64().expect("aggregate max"),
    }
}

fn find_rollup_doc_id(
    node: &integration_test::DefraClient,
    collection: &str,
    source_doc_id: &str,
    window_start: i64,
) -> Option<String> {
    let data = node
        .query(&format!(
            r#"query {{
                {collection}(
                    filter: {{
                        source_doc_id: {{_eq: "{source_doc_id}"}}
                        window_start: {{_eq: {window_start}}}
                    }}
                ) {{
                    _docID
                }}
            }}"#,
        ))
        .expect("query rollup documents");

    extract_rows(&data, collection)
        .first()
        .and_then(|row| row["_docID"].as_str())
        .map(str::to_string)
}

fn upsert_rollup(
    node: &integration_test::DefraClient,
    spec: RollupSpec,
    source_doc_id: &str,
    latest_height: i64,
) {
    let window_start = ((latest_height - 1) / spec.window_size) * spec.window_size + 1;
    let window_end_exclusive = window_start + spec.window_size;
    let aggregate = aggregate_value_window(node, source_doc_id, window_start, window_end_exclusive);
    let window_end = window_end_exclusive - 1;

    if let Some(doc_id) = find_rollup_doc_id(node, spec.collection, source_doc_id, window_start) {
        node.query(&format!(
            r#"mutation {{
                update_{collection}(
                    docID: "{doc_id}"
                    input: {{
                        window_end: {window_end}
                        count: {count}
                        sum: {sum}
                        avg: {avg}
                        min: {min}
                        max: {max}
                    }}
                ) {{
                    _docID
                }}
            }}"#,
            collection = spec.collection,
            count = aggregate.count,
            sum = aggregate.sum,
            avg = aggregate.avg,
            min = aggregate.min,
            max = aggregate.max,
        ))
        .expect("update rollup document");
    } else {
        node.query(&format!(
            r#"mutation {{
                add_{collection}(
                    input: {{
                        source_doc_id: "{source_doc_id}"
                        window_start: {window_start}
                        window_end: {window_end}
                        count: {count}
                        sum: {sum}
                        avg: {avg}
                        min: {min}
                        max: {max}
                    }}
                ) {{
                    _docID
                }}
            }}"#,
            collection = spec.collection,
            count = aggregate.count,
            sum = aggregate.sum,
            avg = aggregate.avg,
            min = aggregate.min,
            max = aggregate.max,
        ))
        .expect("create rollup document");
    }
}

fn recompute_rollups(
    node: &integration_test::DefraClient,
    source_doc_id: &str,
    specs: &[RollupSpec],
) {
    let latest_height = latest_value_height(node, source_doc_id);
    for spec in specs {
        upsert_rollup(node, *spec, source_doc_id, latest_height);
    }
}

fn assert_rollup_row(
    row: &Value,
    source_doc_id: &str,
    window_start: i64,
    window_end: i64,
    count: i64,
    sum: i64,
    avg: f64,
    min: i64,
    max: i64,
) {
    assert_eq!(row["source_doc_id"].as_str(), Some(source_doc_id));
    assert_eq!(row["window_start"].as_i64(), Some(window_start));
    assert_eq!(row["window_end"].as_i64(), Some(window_end));
    assert_eq!(row["count"].as_i64(), Some(count));
    assert_eq!(row["sum"].as_i64(), Some(sum));
    assert_eq!(json_f64(row, "avg"), avg);
    assert_eq!(row["min"].as_i64(), Some(min));
    assert_eq!(row["max"].as_i64(), Some(max));
}

async fn continuous_rollup_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let api_url = cluster.api_url(0);
    let specs = [
        RollupSpec {
            collection: "Cpu2Rollup",
            window_size: 2,
        },
        RollupSpec {
            collection: "Cpu3Rollup",
            window_size: 3,
        },
    ];

    node.schema_add(SCHEMA).expect("add schema");

    let create = node
        .query(r#"mutation { add_CpuRaw(input: {host: "node-a", ts: 10, value: 10}) { _docID } }"#)
        .expect("create raw metric");
    let source_doc_id = extract_doc_id(&create, "add_CpuRaw");

    let (handle, events) = open_events_sse(api_url, "update").await;

    let updates = [(20, 20), (70, 30), (80, 40)];
    for (event_count, (ts, value)) in updates.into_iter().enumerate() {
        node.query(&format!(
            r#"mutation {{
                update_CpuRaw(
                    docID: "{source_doc_id}"
                    input: {{ts: {ts}, value: {value}}}
                ) {{
                    _docID
                }}
            }}"#,
        ))
        .expect("update raw metric");

        let expected_events = event_count + 1;
        poll_until(
            || count_update_events_for_doc(&events, &source_doc_id) >= expected_events,
            EVENT_TIMEOUT,
            POLL_INTERVAL,
            "raw metric update event should arrive",
        )
        .await;

        recompute_rollups(&node, &source_doc_id, &specs);
    }

    let rollup2 = node
        .query(
            r#"query {
                Cpu2Rollup(order: {window_start: ASC}) {
                    source_doc_id
                    window_start
                    window_end
                    count
                    sum
                    avg
                    min
                    max
                }
            }"#,
        )
        .expect("query 2-commit rollups");
    let rows2 = extract_rows(&rollup2, "Cpu2Rollup");
    assert_eq!(rows2.len(), 2, "expected two 2-commit rollup windows");
    assert_rollup_row(&rows2[0], &source_doc_id, 1, 2, 2, 30, 15.0, 10, 20);
    assert_rollup_row(&rows2[1], &source_doc_id, 3, 4, 2, 70, 35.0, 30, 40);

    let rollup3 = node
        .query(
            r#"query {
                Cpu3Rollup(order: {window_start: ASC}) {
                    source_doc_id
                    window_start
                    window_end
                    count
                    sum
                    avg
                    min
                    max
                }
            }"#,
        )
        .expect("query 3-commit rollups");
    let rows3 = extract_rows(&rollup3, "Cpu3Rollup");
    assert_eq!(rows3.len(), 2, "expected two 3-commit rollup windows");
    assert_rollup_row(&rows3[0], &source_doc_id, 1, 3, 3, 60, 20.0, 10, 30);
    assert_rollup_row(&rows3[1], &source_doc_id, 4, 6, 1, 40, 40.0, 40, 40);

    handle.abort();
}

#[tokio::test]
async fn rust_continuous_rollup_from_update_events() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    continuous_rollup_test(cluster).await;
}
