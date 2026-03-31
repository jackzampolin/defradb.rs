use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, TimeZone, Utc};
use integration_test::{workspace_root, TestCluster};
use serde_json::Value;
use std::time::{Duration, Instant};

const RAW_SCHEMA: &str = "type Metric { label: String  ts: DateTime  value: Int }";
const ROLLUP2_SDL: &str = r#"
type Metric2Rollup @downsample(interval: "1000ms", timeField: "ts") {
  label: String
  source_doc_id: String
  source_height: Int
  window_start: DateTime
  window_end: DateTime
  count: Int
  sum: Int
  avg: Float
  min: Int
  max: Int
}
"#;
const ROLLUP4_SDL: &str = r#"
type Metric4Rollup @downsample(interval: "2000ms", timeField: "window_start") {
  label: String
  source_doc_id: String
  source_height: Int
  window_start: DateTime
  window_end: DateTime
  count: Int
  sum: Int
  avg: Float
  min: Int
  max: Int
}
"#;

const WAIT_TIMEOUT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const BUCKET_MS: i64 = 1_000;
const PARENT_BUCKET_MS: i64 = 2_000;
const BASE_OFFSET_BUCKETS: i64 = 3;

fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    if timestamp.timestamp_subsec_nanos() == 0 {
        timestamp.to_rfc3339_opts(SecondsFormat::Secs, true)
    } else {
        let s = timestamp.to_rfc3339_opts(SecondsFormat::Nanos, true);
        document::encoding::trim_rfc3339_trailing_zeros(&s)
    }
}

fn align_base_timestamp() -> DateTime<Utc> {
    let now_ms = Utc::now().timestamp_millis();
    let base_ms = ((now_ms / PARENT_BUCKET_MS) + BASE_OFFSET_BUCKETS) * PARENT_BUCKET_MS;
    Utc.timestamp_millis_opt(base_ms)
        .single()
        .expect("aligned base timestamp")
}

async fn sleep_until(timestamp: DateTime<Utc>) {
    let delay = timestamp
        .signed_duration_since(Utc::now())
        .to_std()
        .unwrap_or_default();
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
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

fn query_rollup(
    client: &integration_test::DefraClient,
    collection: &str,
    source_doc_id: &str,
) -> Option<Value> {
    let data = client
        .query(&format!(
            r#"query {{
                {collection}(
                    filter: {{ source_doc_id: {{ _eq: "{source_doc_id}" }} }}
                ) {{
                    _docID
                    label
                    source_doc_id
                    source_height
                    window_start
                    window_end
                    count
                    sum
                    avg
                    min
                    max
                }}
            }}"#
        ))
        .expect("query rollup collection");

    extract_rows(&data, collection).first().cloned()
}

fn query_metric(client: &integration_test::DefraClient, doc_id: &str) -> Value {
    let data = client
        .query(&format!(
            r#"query {{
                Metric(docID: "{doc_id}") {{
                    _docID
                    label
                    ts
                    value
                }}
            }}"#
        ))
        .expect("query Metric collection");

    let rows = extract_rows(&data, "Metric");
    assert_eq!(rows.len(), 1, "expected a single Metric row");
    rows[0].clone()
}

#[allow(clippy::too_many_arguments)]
fn assert_rollup_row(
    row: &Value,
    source_doc_id: &str,
    expected_source_height: i64,
    expected_window_start: &str,
    expected_window_end: &str,
    expected_count: i64,
    expected_sum: i64,
    expected_avg: f64,
    expected_min: i64,
    expected_max: i64,
) {
    assert_eq!(row["label"].as_str(), Some("cpu"));
    assert_eq!(row["source_doc_id"].as_str(), Some(source_doc_id));
    assert_eq!(row["source_height"].as_i64(), Some(expected_source_height));
    assert_eq!(row["window_start"].as_str(), Some(expected_window_start));
    assert_eq!(row["window_end"].as_str(), Some(expected_window_end));
    assert_eq!(row["count"].as_i64(), Some(expected_count));
    assert_eq!(row["sum"].as_i64(), Some(expected_sum));
    assert_eq!(row["avg"].as_f64(), Some(expected_avg));
    assert_eq!(row["min"].as_i64(), Some(expected_min));
    assert_eq!(row["max"].as_i64(), Some(expected_max));
}

#[allow(clippy::too_many_arguments)]
async fn wait_for_rollup(
    client: &integration_test::DefraClient,
    collection: &str,
    source_doc_id: &str,
    expected_source_height: i64,
    expected_window_start: &str,
    expected_window_end: &str,
    expected_count: i64,
    expected_sum: i64,
    expected_avg: f64,
    expected_min: i64,
    expected_max: i64,
) -> Value {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    let mut last_row = None;

    loop {
        if let Some(row) = query_rollup(client, collection, source_doc_id) {
            let matches = row["source_height"].as_i64() == Some(expected_source_height)
                && row["window_start"].as_str() == Some(expected_window_start)
                && row["window_end"].as_str() == Some(expected_window_end)
                && row["count"].as_i64() == Some(expected_count)
                && row["sum"].as_i64() == Some(expected_sum)
                && row["avg"].as_f64() == Some(expected_avg)
                && row["min"].as_i64() == Some(expected_min)
                && row["max"].as_i64() == Some(expected_max);

            if matches {
                return row;
            }
            last_row = Some(row);
        }

        if Instant::now() >= deadline {
            panic!(
                "{collection} did not converge for source_doc_id={source_doc_id}; last row: {}",
                last_row
                    .map(|row| serde_json::to_string_pretty(&row).unwrap())
                    .unwrap_or_else(|| "<none>".to_string())
            );
        }

        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn aggregate_commit_field(
    client: &integration_test::DefraClient,
    doc_id: &str,
    field_name: &str,
) -> Value {
    let data = client
        .query(&format!(
            r#"query {{
                _commits(
                    docID: ["{doc_id}"]
                    filter: {{ fieldName: {{ _eq: "{field_name}" }} }}
                ) {{
                    count: COUNT
                    total: SUM(field: delta)
                    maxValue: MAX(field: delta)
                }}
            }}"#
        ))
        .expect("query rollup commit aggregates");

    let rows = extract_rows(&data, "_commits");
    assert_eq!(rows.len(), 1, "expected a single aggregate row");
    rows[0].clone()
}

async fn downsample_test(cluster: TestCluster) {
    let client = cluster.client(0);

    client.schema_add(RAW_SCHEMA).expect("add raw schema");

    let created_rollup2 = client
        .view_add("Metric { label ts value }", ROLLUP2_SDL)
        .expect("create Metric2Rollup downsample");
    assert_eq!(
        created_rollup2[0]["DownsampleInterval"].as_str(),
        Some("1000ms")
    );
    assert_eq!(
        created_rollup2[0]["DownsampleTimeField"].as_str(),
        Some("ts")
    );

    let created_rollup4 = client
        .view_add(
            "Metric2Rollup { label source_doc_id source_height window_start window_end count sum avg min max }",
            ROLLUP4_SDL,
        )
        .expect("create Metric4Rollup downsample");
    assert_eq!(
        created_rollup4[0]["DownsampleInterval"].as_str(),
        Some("2000ms")
    );
    assert_eq!(
        created_rollup4[0]["DownsampleTimeField"].as_str(),
        Some("window_start")
    );

    let base = align_base_timestamp();
    let t1 = base;
    let t2 = base + ChronoDuration::milliseconds(BUCKET_MS / 2);
    let t3 = base + ChronoDuration::milliseconds(BUCKET_MS);
    let t4 = base + ChronoDuration::milliseconds(BUCKET_MS + (BUCKET_MS / 2));
    let first_window_end = base + ChronoDuration::milliseconds(BUCKET_MS);
    let second_window_end = base + ChronoDuration::milliseconds(PARENT_BUCKET_MS);

    let create = client
        .query(&format!(
            r#"mutation {{ add_Metric(input: {{label: "cpu", ts: "{}", value: 10}}) {{ _docID }} }}"#,
            format_timestamp(t1)
        ))
        .expect("create metric");
    let source_doc_id = extract_doc_id(&create, "add_Metric");

    let value_only_update = client.query(&format!(
        r#"mutation {{
            update_Metric(docID: "{source_doc_id}", input: {{value: 11}}) {{ _docID }}
        }}"#
    ));
    let value_only_error = value_only_update.expect_err("value-only update should be rejected");
    assert!(
        value_only_error
            .to_string()
            .contains("'ts' and 'value' must be updated together"),
        "unexpected value-only update error: {value_only_error}",
    );

    let ts_only_update = client.query(&format!(
        r#"mutation {{
            update_Metric(docID: "{source_doc_id}", input: {{ts: "{}"}}) {{ _docID }}
        }}"#,
        format_timestamp(t2)
    ));
    let ts_only_error = ts_only_update.expect_err("ts-only update should be rejected");
    assert!(
        ts_only_error
            .to_string()
            .contains("'ts' and 'value' must be updated together"),
        "unexpected ts-only update error: {ts_only_error}",
    );

    let unchanged_source = query_metric(&client, &source_doc_id);
    assert_eq!(
        unchanged_source["_docID"].as_str(),
        Some(source_doc_id.as_str())
    );
    assert_eq!(unchanged_source["label"].as_str(), Some("cpu"));
    assert_eq!(
        unchanged_source["ts"].as_str(),
        Some(format_timestamp(t1).as_str())
    );
    assert_eq!(unchanged_source["value"].as_i64(), Some(10));

    client
        .query(&format!(
            r#"mutation {{ update_Metric(docID: "{source_doc_id}", input: {{ts: "{}", value: 20}}) {{ _docID }} }}"#,
            format_timestamp(t2)
        ))
        .expect("update metric to 20");

    assert!(
        query_rollup(&client, "Metric2Rollup", &source_doc_id).is_none(),
        "Metric2Rollup should stay empty before the first time bucket closes",
    );

    sleep_until(first_window_end + ChronoDuration::milliseconds(350)).await;

    let first_rollup = wait_for_rollup(
        &client,
        "Metric2Rollup",
        &source_doc_id,
        2,
        &format_timestamp(t1),
        &format_timestamp(first_window_end),
        2,
        30,
        15.0,
        10,
        20,
    )
    .await;
    assert_rollup_row(
        &first_rollup,
        &source_doc_id,
        2,
        &format_timestamp(t1),
        &format_timestamp(first_window_end),
        2,
        30,
        15.0,
        10,
        20,
    );
    let rollup2_doc_id = first_rollup["_docID"]
        .as_str()
        .expect("Metric2Rollup row should include _docID")
        .to_string();

    client
        .query(&format!(
            r#"mutation {{ update_Metric(docID: "{source_doc_id}", input: {{ts: "{}", value: 30}}) {{ _docID }} }}"#,
            format_timestamp(t3)
        ))
        .expect("update metric to 30");

    let stale_rollup = query_rollup(&client, "Metric2Rollup", &source_doc_id)
        .expect("Metric2Rollup row should still exist during an incomplete bucket");
    assert_rollup_row(
        &stale_rollup,
        &source_doc_id,
        2,
        &format_timestamp(t1),
        &format_timestamp(first_window_end),
        2,
        30,
        15.0,
        10,
        20,
    );

    client
        .query(&format!(
            r#"mutation {{ update_Metric(docID: "{source_doc_id}", input: {{ts: "{}", value: 40}}) {{ _docID }} }}"#,
            format_timestamp(t4)
        ))
        .expect("update metric to 40");

    sleep_until(second_window_end + ChronoDuration::milliseconds(350)).await;

    let second_rollup = wait_for_rollup(
        &client,
        "Metric2Rollup",
        &source_doc_id,
        4,
        &format_timestamp(t3),
        &format_timestamp(second_window_end),
        2,
        70,
        35.0,
        30,
        40,
    )
    .await;
    assert_rollup_row(
        &second_rollup,
        &source_doc_id,
        4,
        &format_timestamp(t3),
        &format_timestamp(second_window_end),
        2,
        70,
        35.0,
        30,
        40,
    );
    assert_eq!(
        second_rollup["_docID"].as_str(),
        Some(rollup2_doc_id.as_str()),
        "Metric2Rollup should keep a stable document identity for the series",
    );

    let rollup2_sum_history = aggregate_commit_field(&client, &rollup2_doc_id, "sum");
    assert_eq!(rollup2_sum_history["count"].as_i64(), Some(2));
    assert_eq!(rollup2_sum_history["total"].as_i64(), Some(100));
    assert_eq!(rollup2_sum_history["maxValue"].as_i64(), Some(70));

    let rollup4 = wait_for_rollup(
        &client,
        "Metric4Rollup",
        &source_doc_id,
        2,
        &format_timestamp(t1),
        &format_timestamp(second_window_end),
        4,
        100,
        25.0,
        10,
        40,
    )
    .await;
    assert_rollup_row(
        &rollup4,
        &source_doc_id,
        2,
        &format_timestamp(t1),
        &format_timestamp(second_window_end),
        4,
        100,
        25.0,
        10,
        40,
    );

    let rollup4_doc_id = rollup4["_docID"]
        .as_str()
        .expect("Metric4Rollup row should include _docID");
    let rollup4_height_history = aggregate_commit_field(&client, rollup4_doc_id, "source_height");
    assert_eq!(rollup4_height_history["count"].as_i64(), Some(1));
    assert_eq!(rollup4_height_history["maxValue"].as_i64(), Some(2));

    let late_update = client.query(&format!(
        r#"mutation {{
            update_Metric(
                docID: "{source_doc_id}"
                input: {{ts: "{}", value: 5}}
            ) {{ _docID }}
        }}"#,
        format_timestamp(t2)
    ));
    let late_error = late_update.expect_err("late data update should be rejected");
    let late_error_message = late_error.to_string();
    assert!(
        late_error_message.contains("late data is not accepted"),
        "unexpected late-data error: {late_error_message}"
    );

    let source_row = query_metric(&client, &source_doc_id);
    assert_eq!(source_row["_docID"].as_str(), Some(source_doc_id.as_str()));
    assert_eq!(source_row["label"].as_str(), Some("cpu"));
    assert_eq!(
        source_row["ts"].as_str(),
        Some(format_timestamp(t4).as_str())
    );
    assert_eq!(source_row["value"].as_i64(), Some(40));

    let unchanged_rollup = query_rollup(&client, "Metric2Rollup", &source_doc_id)
        .expect("Metric2Rollup row should remain after late-data rejection");
    assert_rollup_row(
        &unchanged_rollup,
        &source_doc_id,
        4,
        &format_timestamp(t3),
        &format_timestamp(second_window_end),
        2,
        70,
        35.0,
        30,
        40,
    );
}

#[tokio::test]
async fn rust_downsample_updates_stable_rollup_docs() {
    let _root = workspace_root();
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .build()
        .await
        .expect("build cluster");
    downsample_test(cluster).await;
}
