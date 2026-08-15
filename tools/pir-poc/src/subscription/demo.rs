use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use defra_node::{EmbeddedNode, EventName};
use serde::Serialize;

use super::{combine_compact, compact_registration, CompactSubscriptionServer};
use crate::http;
use crate::snapshot::{bucket_for_key, records_from_json, Snapshot, SnapshotConfig};

const BUCKET_COUNT: usize = 1 << 20;
const TARGET_TAG: &str = "topic:private-alice";
const TARGET_PAYLOAD: &str = "the private subscription found this record";
const DEMO_SCHEMA: &str = r#"
type LivePrivateRecord {
    tag: String
    payload: String
}
"#;

#[derive(Debug, Serialize)]
pub struct SubscriptionDemoReport {
    pub event_source: &'static str,
    pub event_semantics: &'static str,
    pub compact_dpf_servers: usize,
    pub compact_dpf_bucket_count: usize,
    pub compact_dpf_key_bytes_per_server: usize,
    pub compact_dpf_response_bytes_per_server_per_event: usize,
    pub subscription_id: String,
    pub non_matching_event_detected: bool,
    pub matching_event_detected: bool,
    pub matching_event_cid: String,
    pub compact_dpf_server_eval_us: Vec<f64>,
    pub dense_snapshot_servers: usize,
    pub dense_snapshot_id: String,
    pub dense_query_share_bytes_per_server: usize,
    pub recovered_value: String,
    pub dense_private_lookup_ms: f64,
}

pub async fn run() -> Result<SubscriptionDemoReport> {
    let node = EmbeddedNode::builder().build().await?;
    node.add_schema(DEMO_SCHEMA).await?;
    let mut events = node.subscribe(&[EventName::Update]);

    let target_bucket = bucket_for_key(TARGET_TAG.as_bytes(), BUCKET_COUNT);
    let registration = compact_registration(target_bucket, BUCKET_COUNT, &mut rand::thread_rng())?;
    let mut compact_servers = [
        CompactSubscriptionServer::new(0, BUCKET_COUNT)?,
        CompactSubscriptionServer::new(1, BUCKET_COUNT)?,
    ];
    for (server, key) in compact_servers.iter_mut().zip(&registration.server_keys) {
        server.register(registration.id, key)?;
    }

    let (_, _) = insert_and_wait_for_event(
        &node,
        &mut events,
        "topic:public-bob",
        "this event must not match",
    )
    .await?;
    let non_match_bucket = bucket_for_key(b"topic:public-bob", BUCKET_COUNT);
    let non_match_shares = compact_servers
        .iter()
        .map(|server| server.evaluate_one(registration.id, non_match_bucket))
        .collect::<Result<Vec<_>>>()?;
    let non_matching_event_detected = combine_compact(&non_match_shares)?;
    if non_matching_event_detected {
        bail!("Compact DPF subscription produced a false positive");
    }

    let (_, matching_event_cid) =
        insert_and_wait_for_event(&node, &mut events, TARGET_TAG, TARGET_PAYLOAD).await?;
    let mut evaluation_us = Vec::with_capacity(2);
    let mut matching_shares = Vec::with_capacity(2);
    for server in &compact_servers {
        let started = Instant::now();
        matching_shares.push(server.evaluate_one(registration.id, target_bucket)?);
        evaluation_us.push(started.elapsed().as_secs_f64() * 1_000_000.0);
    }
    let matching_event_detected = combine_compact(&matching_shares)?;
    if !matching_event_detected {
        bail!("Compact DPF subscription missed its target event");
    }

    // A live notification is only a private hint.  Seal current DefraDB state
    // and use the existing Dense XOR path to privately recover the actual row.
    let response = node
        .execute("query { LivePrivateRecord { tag payload } }")
        .await;
    ensure_success(&response, "read live demo records")?;
    let data = response.data.context("DefraDB query returned no data")?;
    let records = records_from_json(&data, Some("LivePrivateRecord"), "tag", "payload")?;
    let snapshot = Arc::new(Snapshot::build_paged(
        records,
        SnapshotConfig {
            bucket_count: 64,
            bucket_capacity: 4,
            values_per_page: 2,
            max_key_bytes: 64,
            max_value_bytes: 256,
            source: "LivePrivateRecord.tag->payload".into(),
            source_cutoff: matching_event_cid.clone(),
        },
    )?);
    let dense_server_count = 2;
    let mut dense_servers = Vec::with_capacity(dense_server_count);
    for _ in 0..dense_server_count {
        dense_servers.push(http::spawn(Arc::clone(&snapshot), "127.0.0.1:0").await?);
    }
    let addresses = dense_servers
        .iter()
        .map(|server| format!("http://{}", server.address))
        .collect::<Vec<_>>();
    let client = http::PirClient::connect(&addresses).await?;
    let manifest = client.manifest().clone();
    let lookup_started = Instant::now();
    let values = client.private_lookup(TARGET_TAG.as_bytes()).await?;
    let dense_private_lookup_ms = lookup_started.elapsed().as_secs_f64() * 1_000.0;
    let recovered_value = String::from_utf8(
        values
            .first()
            .context("Dense XOR lookup returned no matching live record")?
            .clone(),
    )?;
    if recovered_value != TARGET_PAYLOAD {
        bail!("Dense XOR returned the wrong live record payload");
    }
    node.shutdown().await;

    Ok(SubscriptionDemoReport {
        event_source: "EmbeddedNode::subscribe(EventName::Update)",
        event_semantics: "live-only notification; snapshot remains the source of truth",
        compact_dpf_servers: 2,
        compact_dpf_bucket_count: BUCKET_COUNT,
        compact_dpf_key_bytes_per_server: registration.server_keys[0].len(),
        compact_dpf_response_bytes_per_server_per_event: 16,
        subscription_id: registration.id.to_string(),
        non_matching_event_detected,
        matching_event_detected,
        matching_event_cid,
        compact_dpf_server_eval_us: evaluation_us,
        dense_snapshot_servers: dense_server_count,
        dense_snapshot_id: manifest.snapshot_id,
        dense_query_share_bytes_per_server: crate::dense::query_size(manifest.bucket_count),
        recovered_value,
        dense_private_lookup_ms,
    })
}

async fn insert_and_wait_for_event(
    node: &EmbeddedNode,
    events: &mut events::Subscription,
    tag: &str,
    payload: &str,
) -> Result<(String, String)> {
    let query = format!(
        r#"mutation {{ add_LivePrivateRecord(input: {{tag: "{tag}", payload: "{payload}"}}) {{ _docID }} }}"#
    );
    let response = node.execute(&query).await;
    ensure_success(&response, "insert live demo record")?;
    let doc_id = response
        .data
        .as_ref()
        .and_then(|data| data.pointer("/add_LivePrivateRecord/0/_docID"))
        .and_then(serde_json::Value::as_str)
        .context("insert response omitted _docID")?
        .to_owned();
    let message = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
        .await
        .context("timed out waiting for DefraDB update event")?
        .context("DefraDB update event stream closed")?;
    let update = message
        .as_update()
        .context("DefraDB emitted a non-update event on an update subscription")?;
    if update.doc_id != doc_id {
        bail!("DefraDB update event refers to a different document");
    }
    Ok((doc_id, update.cid.to_string()))
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
