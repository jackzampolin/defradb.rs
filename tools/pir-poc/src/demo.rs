use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use defra_node::EmbeddedNode;
use serde::Serialize;

use crate::http;
use crate::snapshot::{records_from_json, Snapshot, SnapshotConfig};

const DEMO_SCHEMA: &str = r#"
type PrivateRecord {
    lookupKey: String
    payload: String
}
"#;

const DEMO_ROWS: &[(&str, &str)] = &[
    ("account:alice", "alice has 17 private candidates"),
    ("account:bob", "bob has 4 private candidates"),
    ("account:carol", "carol has no private candidates"),
];

#[derive(Debug, Serialize)]
pub struct DemoReport {
    pub defradb_documents: usize,
    pub snapshot_id: String,
    pub bucket_count: usize,
    pub row_size: usize,
    pub query_share_bytes_per_server: usize,
    pub answer_share_bytes_per_server: usize,
    pub server_a: String,
    pub server_b: String,
    pub queried_key: String,
    pub recovered_value: String,
    pub elapsed_ms: f64,
}

pub async fn run() -> Result<DemoReport> {
    let node = EmbeddedNode::builder().build().await?;
    node.add_schema(DEMO_SCHEMA).await?;
    for (key, payload) in DEMO_ROWS {
        let query = format!(
            r#"mutation {{ add_PrivateRecord(input: {{lookupKey: "{key}", payload: "{payload}"}}) {{ _docID }} }}"#
        );
        ensure_success(&node.execute(&query).await, "insert demo record")?;
    }

    let response = node
        .execute("query { PrivateRecord { lookupKey payload } }")
        .await;
    ensure_success(&response, "read demo records")?;
    let data = response.data.context("DefraDB query returned no data")?;
    let records = records_from_json(&data, Some("PrivateRecord"), "lookupKey", "payload")?;
    let document_count = records.len();
    let snapshot = Arc::new(Snapshot::build(
        records,
        SnapshotConfig {
            bucket_count: 64,
            bucket_capacity: 4,
            max_key_bytes: 64,
            max_value_bytes: 256,
            source: "PrivateRecord.lookupKey->payload".into(),
            source_cutoff: "sealed-demo-query".into(),
        },
    )?);

    let server_a = http::spawn(Arc::clone(&snapshot), "127.0.0.1:0").await?;
    let server_b = http::spawn(Arc::clone(&snapshot), "127.0.0.1:0").await?;
    let url_a = format!("http://{}", server_a.address);
    let url_b = format!("http://{}", server_b.address);
    let key = "account:alice";
    let started = Instant::now();
    let (manifest, values) = http::private_lookup(key.as_bytes(), &url_a, &url_b).await?;
    let elapsed = started.elapsed();
    let value = values
        .first()
        .context("private lookup returned no matching value")?;
    let recovered_value = String::from_utf8(value.clone())?;
    if recovered_value != DEMO_ROWS[0].1 {
        bail!("private result did not match the DefraDB document");
    }
    node.shutdown().await;

    Ok(DemoReport {
        defradb_documents: document_count,
        snapshot_id: manifest.snapshot_id,
        bucket_count: manifest.bucket_count,
        row_size: manifest.row_size,
        query_share_bytes_per_server: crate::dense::query_size(manifest.bucket_count),
        answer_share_bytes_per_server: manifest.row_size,
        server_a: url_a,
        server_b: url_b,
        queried_key: key.into(),
        recovered_value,
        elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
    })
}

fn ensure_success(response: &query::QueryResponse, operation: &str) -> Result<()> {
    if response.errors.is_empty() {
        return Ok(());
    }
    let messages = response
        .errors
        .iter()
        .map(|error| error.message.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    bail!("{operation} failed: {messages}")
}
