use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use defra_node::EmbeddedNode;
use serde::Serialize;

use crate::http;
use crate::single_pass::{self, ClientState};
use crate::snapshot::{bucket_for_key, records_from_json, Snapshot, SnapshotConfig};

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
    pub lookup_pages: usize,
    pub server_count: usize,
    pub query_share_bytes_per_server: usize,
    pub total_query_bytes: usize,
    pub answer_share_bytes_per_server: usize,
    pub total_answer_bytes: usize,
    pub servers: Vec<String>,
    pub queried_key: String,
    pub recovered_value: String,
    pub client_connect_ms: f64,
    pub private_query_ms: f64,
}

#[derive(Debug, Serialize)]
pub struct SinglePassDemoReport {
    pub defradb_documents: usize,
    pub snapshot_id: String,
    pub bucket_count: usize,
    pub row_size: usize,
    pub lookup_pages: usize,
    pub server_count: usize,
    pub partition_count_q: usize,
    pub setup_ms: f64,
    pub client_state_bytes: usize,
    pub query_bytes_per_server: usize,
    pub total_query_bytes: usize,
    pub answer_bytes_per_server: usize,
    pub total_answer_bytes: usize,
    pub queried_key: String,
    pub recovered_value: String,
    pub private_query_ms: f64,
}

pub async fn run() -> Result<DemoReport> {
    let (node, snapshot, document_count) = build_demo_snapshot().await?;

    let server_count = 3;
    let mut running_servers = Vec::with_capacity(server_count);
    for _ in 0..server_count {
        running_servers.push(http::spawn(Arc::clone(&snapshot), "127.0.0.1:0").await?);
    }
    let servers = running_servers
        .iter()
        .map(|server| format!("http://{}", server.address))
        .collect::<Vec<_>>();
    let key = "account:alice";
    let connect_started = Instant::now();
    let client = http::PirClient::connect(&servers).await?;
    let client_connect_ms = connect_started.elapsed().as_secs_f64() * 1_000.0;
    let manifest = client.manifest().clone();
    let lookup_pages = manifest.lookup_keys(key.as_bytes())?.len();
    let started = Instant::now();
    let values = client.private_lookup(key.as_bytes()).await?;
    let private_query_ms = started.elapsed().as_secs_f64() * 1_000.0;
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
        lookup_pages,
        server_count,
        query_share_bytes_per_server: crate::dense::query_size(manifest.bucket_count),
        total_query_bytes: crate::dense::query_size(manifest.bucket_count)
            * server_count
            * lookup_pages,
        answer_share_bytes_per_server: manifest.row_size,
        total_answer_bytes: manifest.row_size * server_count * lookup_pages,
        servers,
        queried_key: key.into(),
        recovered_value,
        client_connect_ms,
        private_query_ms,
    })
}

pub async fn run_single_pass() -> Result<SinglePassDemoReport> {
    const PARTITION_COUNT: usize = 8;

    let (node, snapshot, document_count) = build_demo_snapshot().await?;
    let mut rng = rand::rngs::OsRng;
    let setup_started = Instant::now();
    let mut state = ClientState::setup(snapshot.view(), PARTITION_COUNT, &mut rng)?;
    let setup_ms = setup_started.elapsed().as_secs_f64() * 1_000.0;
    let client_state_bytes = state.payload_bytes();

    let key = "account:alice";
    let lookup_keys = snapshot.manifest.lookup_keys(key.as_bytes())?;
    let started = Instant::now();
    let mut values = Vec::new();
    for lookup_key in &lookup_keys {
        let bucket = bucket_for_key(lookup_key, snapshot.manifest.bucket_count);
        let prepared = state.prepare_query(bucket, &mut rng)?;
        let answers = prepared
            .server_queries()
            .iter()
            .map(|query| single_pass::answer(snapshot.view(), query))
            .collect::<Result<Vec<_>>>()?;
        let row = state.complete_query(prepared, &answers)?;
        values.extend(snapshot.manifest.values_from_row(&row, lookup_key)?);
    }
    let private_query_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let recovered_value = String::from_utf8(
        values
            .first()
            .context("SinglePass private lookup returned no matching value")?
            .clone(),
    )?;
    if recovered_value != DEMO_ROWS[0].1 {
        bail!("SinglePass private result did not match the DefraDB document");
    }
    node.shutdown().await;

    let query_bytes_per_server = PARTITION_COUNT * size_of::<u32>();
    let answer_bytes_per_server = PARTITION_COUNT * snapshot.manifest.row_size;
    Ok(SinglePassDemoReport {
        defradb_documents: document_count,
        snapshot_id: snapshot.manifest.snapshot_id.clone(),
        bucket_count: snapshot.manifest.bucket_count,
        row_size: snapshot.manifest.row_size,
        lookup_pages: lookup_keys.len(),
        server_count: single_pass::SERVER_COUNT,
        partition_count_q: PARTITION_COUNT,
        setup_ms,
        client_state_bytes,
        query_bytes_per_server,
        total_query_bytes: query_bytes_per_server * single_pass::SERVER_COUNT * lookup_keys.len(),
        answer_bytes_per_server,
        total_answer_bytes: answer_bytes_per_server * single_pass::SERVER_COUNT * lookup_keys.len(),
        queried_key: key.into(),
        recovered_value,
        private_query_ms,
    })
}

async fn build_demo_snapshot() -> Result<(EmbeddedNode, Arc<Snapshot>, usize)> {
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
    let snapshot = Arc::new(Snapshot::build_paged(
        records,
        SnapshotConfig {
            bucket_count: 64,
            bucket_capacity: 4,
            values_per_page: 2,
            max_key_bytes: 64,
            max_value_bytes: 256,
            source: "PrivateRecord.lookupKey->payload".into(),
            source_cutoff: "sealed-demo-query".into(),
        },
    )?);
    Ok((node, snapshot, document_count))
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
