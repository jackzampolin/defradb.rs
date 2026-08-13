use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::dense;
use crate::snapshot::{bucket_for_key, Manifest, Snapshot};

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
    pub snapshot_id: String,
    pub query_share: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AnswerResponse {
    pub snapshot_id: String,
    pub answer_share: String,
}

pub struct RunningServer {
    pub address: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn spawn(snapshot: Arc<Snapshot>, bind: &str) -> Result<RunningServer> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let app = router(snapshot);
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            eprintln!("PIR sidecar stopped: {error}");
        }
    });
    Ok(RunningServer { address, task })
}

pub async fn serve(snapshot: Arc<Snapshot>, bind: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!("PIR sidecar listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router(snapshot)).await?;
    Ok(())
}

pub async fn private_lookup(
    key: &[u8],
    server_a: &str,
    server_b: &str,
) -> Result<(Manifest, Vec<Vec<u8>>)> {
    let client = reqwest::Client::new();
    let manifest_url_a = format!("{}/v1/manifest", server_a.trim_end_matches('/'));
    let manifest_url_b = format!("{}/v1/manifest", server_b.trim_end_matches('/'));
    let (manifest_a, manifest_b) = tokio::try_join!(
        fetch_manifest(&client, &manifest_url_a),
        fetch_manifest(&client, &manifest_url_b)
    )?;
    manifest_a.validate()?;
    manifest_b.validate()?;
    if manifest_a != manifest_b {
        bail!("PIR servers advertise different snapshots");
    }

    let bucket = bucket_for_key(key, manifest_a.bucket_count);
    let (query_a, query_b) = dense::query_shares(bucket, manifest_a.bucket_count, &mut OsRng)?;
    let request_a = QueryRequest {
        snapshot_id: manifest_a.snapshot_id.clone(),
        query_share: STANDARD.encode(query_a),
    };
    let request_b = QueryRequest {
        snapshot_id: manifest_a.snapshot_id.clone(),
        query_share: STANDARD.encode(query_b),
    };
    let query_url_a = format!("{}/v1/query", server_a.trim_end_matches('/'));
    let query_url_b = format!("{}/v1/query", server_b.trim_end_matches('/'));
    let (answer_a, answer_b) = tokio::try_join!(
        send_query(&client, &query_url_a, &request_a),
        send_query(&client, &query_url_b, &request_b)
    )?;
    if answer_a.snapshot_id != manifest_a.snapshot_id
        || answer_b.snapshot_id != manifest_a.snapshot_id
    {
        bail!("PIR answer references the wrong snapshot");
    }
    let left = STANDARD.decode(answer_a.answer_share)?;
    let right = STANDARD.decode(answer_b.answer_share)?;
    let row = dense::combine(&left, &right)?;
    let layout = SnapshotLayout::from_manifest(manifest_a.clone())?;
    Ok((manifest_a, layout.values_from_row(&row, key)?))
}

fn router(snapshot: Arc<Snapshot>) -> Router {
    Router::new()
        .route("/v1/manifest", get(get_manifest))
        .route("/v1/query", post(post_query))
        .with_state(snapshot)
}

async fn get_manifest(State(snapshot): State<Arc<Snapshot>>) -> Json<Manifest> {
    Json(snapshot.manifest.clone())
}

async fn post_query(
    State(snapshot): State<Arc<Snapshot>>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<AnswerResponse>, (StatusCode, String)> {
    if request.snapshot_id != snapshot.manifest.snapshot_id {
        return Err((StatusCode::CONFLICT, "snapshot ID mismatch".into()));
    }
    let query = STANDARD
        .decode(request.query_share)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let answer = dense::answer(&snapshot, &query)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    Ok(Json(AnswerResponse {
        snapshot_id: snapshot.manifest.snapshot_id.clone(),
        answer_share: STANDARD.encode(answer),
    }))
}

async fn fetch_manifest(client: &reqwest::Client, url: &str) -> Result<Manifest> {
    client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .with_context(|| format!("decode manifest from {url}"))
}

async fn send_query(
    client: &reqwest::Client,
    url: &str,
    request: &QueryRequest,
) -> Result<AnswerResponse> {
    client
        .post(url)
        .json(request)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .with_context(|| format!("decode answer from {url}"))
}

struct SnapshotLayout {
    manifest: Manifest,
}

impl SnapshotLayout {
    fn from_manifest(manifest: Manifest) -> Result<Self> {
        manifest.validate()?;
        Ok(Self { manifest })
    }

    fn values_from_row(&self, row: &[u8], key: &[u8]) -> Result<Vec<Vec<u8>>> {
        if row.len() != self.manifest.row_size {
            bail!("answer row size mismatch");
        }
        let slot_size = 6 + self.manifest.max_key_bytes + self.manifest.max_value_bytes;
        let mut values = Vec::new();
        for slot in row.chunks_exact(slot_size) {
            let key_len = u16::from_le_bytes([slot[0], slot[1]]) as usize;
            let value_len = u32::from_le_bytes([slot[2], slot[3], slot[4], slot[5]]) as usize;
            if key_len == 0 && value_len == 0 {
                continue;
            }
            if key_len > self.manifest.max_key_bytes || value_len > self.manifest.max_value_bytes {
                bail!("answer row contains invalid lengths");
            }
            let value_start = 6 + self.manifest.max_key_bytes;
            if &slot[6..6 + key_len] == key {
                values.push(slot[value_start..value_start + value_len].to_vec());
            }
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{Record, SnapshotConfig};

    #[tokio::test]
    async fn two_http_servers_return_a_private_value() {
        let snapshot = Arc::new(
            Snapshot::build(
                vec![Record::new(b"key", b"value")],
                SnapshotConfig {
                    bucket_count: 16,
                    bucket_capacity: 2,
                    max_key_bytes: 16,
                    max_value_bytes: 32,
                    source: "test".into(),
                    source_cutoff: "1".into(),
                },
            )
            .unwrap(),
        );
        let left = spawn(Arc::clone(&snapshot), "127.0.0.1:0").await.unwrap();
        let right = spawn(snapshot, "127.0.0.1:0").await.unwrap();
        let values = private_lookup(
            b"key",
            &format!("http://{}", left.address),
            &format!("http://{}", right.address),
        )
        .await
        .unwrap()
        .1;
        assert_eq!(values, vec![b"value".to_vec()]);
    }
}
