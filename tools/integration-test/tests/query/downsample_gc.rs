use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, TimeZone, Utc};
use integration_test::{workspace_root, TestCluster};
use serde_json::Value;
use std::time::{Duration, Instant};

const RAW_SCHEMA: &str = "type Metric { label: String  ts: DateTime  value: Int }";
const ROLLUP2_SDL: &str = r#"
type Metric2Rollup @downsample(interval: "1000ms", timeField: "ts", retention: "1400ms") {
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
type Metric4Rollup @downsample(interval: "2000ms", timeField: "window_start", retention: "900ms") {
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
        timestamp.to_rfc3339_opts(SecondsFormat::Nanos, true)
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
        .expect("query commit aggregates");

    let rows = extract_rows(&data, "_commits");
    assert_eq!(rows.len(), 1, "expected a single aggregate row");
    rows[0].clone()
}

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

async fn run_view_gc(api_url: &str, name: &str) {
    let body = serde_json::json!({ "Names": [name] });
    let response = reqwest::Client::new()
        .post(format!("{api_url}/api/v0/views/gc"))
        .json(&body)
        .send()
        .await
        .expect("send downsample gc request");
    let status = response.status();
    let text = response.text().await.expect("read gc response body");
    assert!(
        status.is_success(),
        "downsample gc request should succeed: status={status}, body={text}"
    );
}

async fn downsample_gc_test(cluster: TestCluster) {
    let client = cluster.client(0);

    client.schema_add(RAW_SCHEMA).expect("add raw schema");
    let created_rollup2 = client
        .view_add("Metric { label ts value }", ROLLUP2_SDL)
        .expect("create Metric2Rollup downsample");
    assert_eq!(
        created_rollup2[0]["DownsampleRetention"].as_str(),
        Some("1400ms")
    );
    let created_rollup4 = client
        .view_add(
            "Metric2Rollup { label source_doc_id source_height window_start window_end count sum avg min max }",
            ROLLUP4_SDL,
        )
        .expect("create Metric4Rollup downsample");
    assert_eq!(
        created_rollup4[0]["DownsampleRetention"].as_str(),
        Some("900ms")
    );

    let base = align_base_timestamp();
    let t1 = base;
    let t2 = base + ChronoDuration::milliseconds(BUCKET_MS / 2);
    let t3 = base + ChronoDuration::milliseconds(BUCKET_MS);
    let t4 = base + ChronoDuration::milliseconds(BUCKET_MS + (BUCKET_MS / 2));
    let second_window_end = base + ChronoDuration::milliseconds(PARENT_BUCKET_MS);

    let create = client
        .query(&format!(
            r#"mutation {{ add_Metric(input: {{label: "cpu", ts: "{}", value: 10}}) {{ _docID }} }}"#,
            format_timestamp(t1)
        ))
        .expect("create metric");
    let source_doc_id = extract_doc_id(&create, "add_Metric");

    for (timestamp, value) in [(t2, 20), (t3, 30), (t4, 40)] {
        client
            .query(&format!(
                r#"mutation {{ update_Metric(docID: "{source_doc_id}", input: {{ts: "{}", value: {value}}}) {{ _docID }} }}"#,
                format_timestamp(timestamp)
            ))
            .expect("update metric");
    }

    sleep_until(second_window_end + ChronoDuration::milliseconds(350)).await;

    let rollup2 = wait_for_rollup(
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
    let rollup2_doc_id = rollup2["_docID"]
        .as_str()
        .expect("Metric2Rollup row should include _docID")
        .to_string();
    assert_rollup_row(
        &rollup2,
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
    let rollup4_doc_id = rollup4["_docID"]
        .as_str()
        .expect("Metric4Rollup row should include _docID")
        .to_string();

    let raw_history = aggregate_commit_field(&client, &source_doc_id, "value");
    assert_eq!(raw_history["count"].as_i64(), Some(4));
    assert_eq!(raw_history["total"].as_i64(), Some(100));
    assert_eq!(raw_history["maxValue"].as_i64(), Some(40));

    let rollup2_history = aggregate_commit_field(&client, &rollup2_doc_id, "sum");
    assert_eq!(rollup2_history["count"].as_i64(), Some(2));
    assert_eq!(rollup2_history["total"].as_i64(), Some(100));
    assert_eq!(rollup2_history["maxValue"].as_i64(), Some(70));

    run_view_gc(cluster.api_url(0), "Metric2Rollup").await;

    let raw_history_after_gc = aggregate_commit_field(&client, &source_doc_id, "value");
    assert_eq!(raw_history_after_gc["count"].as_i64(), Some(2));
    assert_eq!(raw_history_after_gc["total"].as_i64(), Some(70));
    assert_eq!(raw_history_after_gc["maxValue"].as_i64(), Some(40));

    let source_row = query_metric(&client, &source_doc_id);
    assert_eq!(source_row["_docID"].as_str(), Some(source_doc_id.as_str()));
    assert_eq!(source_row["label"].as_str(), Some("cpu"));
    assert_eq!(
        source_row["ts"].as_str(),
        Some(format_timestamp(t4).as_str())
    );
    assert_eq!(source_row["value"].as_i64(), Some(40));

    let rollup2_after_source_gc = query_rollup(&client, "Metric2Rollup", &source_doc_id)
        .expect("Metric2Rollup row should remain after raw-source GC");
    assert_eq!(
        rollup2_after_source_gc["_docID"].as_str(),
        Some(rollup2_doc_id.as_str())
    );
    assert_rollup_row(
        &rollup2_after_source_gc,
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

    run_view_gc(cluster.api_url(0), "Metric4Rollup").await;

    let rollup2_history_after_gc = aggregate_commit_field(&client, &rollup2_doc_id, "sum");
    assert_eq!(rollup2_history_after_gc["count"].as_i64(), Some(1));
    assert_eq!(rollup2_history_after_gc["total"].as_i64(), Some(70));
    assert_eq!(rollup2_history_after_gc["maxValue"].as_i64(), Some(70));

    let rollup4_after_gc = query_rollup(&client, "Metric4Rollup", &source_doc_id)
        .expect("Metric4Rollup row should remain after recursive rollup GC");
    assert_eq!(
        rollup4_after_gc["_docID"].as_str(),
        Some(rollup4_doc_id.as_str())
    );
    assert_rollup_row(
        &rollup4_after_gc,
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
}

#[tokio::test]
async fn rust_downsample_gc_prunes_retained_source_history() {
    let _root = workspace_root();
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .build()
        .await
        .expect("build cluster");
    downsample_gc_test(cluster).await;
}
