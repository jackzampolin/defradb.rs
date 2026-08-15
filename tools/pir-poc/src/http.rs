use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use tokio::task::{JoinHandle, JoinSet};

use crate::dense;
use crate::service::{EvaluationError, PirService, PirServiceConfig};
use crate::snapshot::{bucket_for_key, Manifest, Snapshot};

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
    pub snapshot_id: String,
    pub query_shares: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AnswerResponse {
    pub snapshot_id: String,
    pub answer_shares: Vec<String>,
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

#[derive(Clone)]
pub struct PirClient {
    http: reqwest::Client,
    servers: Arc<[String]>,
    manifest: Manifest,
}

impl PirClient {
    pub async fn connect<S: AsRef<str>>(server_urls: &[S]) -> Result<Self> {
        if server_urls.len() < 2 {
            bail!("Dense XOR PIR requires at least two servers");
        }
        let servers = server_urls
            .iter()
            .map(|server| server.as_ref().trim_end_matches('/').to_owned())
            .collect::<Vec<_>>();
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        let manifests = fetch_all_manifests(&http, &servers).await?;
        let manifest = manifests.first().context("no PIR manifests returned")?;
        manifest.validate()?;
        for candidate in &manifests[1..] {
            candidate.validate()?;
            if candidate != manifest {
                bail!("PIR servers advertise different snapshots");
            }
        }
        Ok(Self {
            http,
            servers: servers.into(),
            manifest: manifest.clone(),
        })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn server_count(&self) -> usize {
        self.servers.len()
    }

    pub async fn private_lookup(&self, key: &[u8]) -> Result<Vec<Vec<u8>>> {
        let lookup_keys = self.manifest.lookup_keys(key)?;
        let mut per_server_queries = (0..self.servers.len())
            .map(|_| Vec::with_capacity(lookup_keys.len()))
            .collect::<Vec<_>>();
        for lookup_key in &lookup_keys {
            let bucket = bucket_for_key(lookup_key, self.manifest.bucket_count);
            let shares = dense::query_shares(
                bucket,
                self.manifest.bucket_count,
                self.servers.len(),
                &mut OsRng,
            )?;
            for (server_queries, share) in per_server_queries.iter_mut().zip(shares) {
                server_queries.push(share);
            }
        }

        let responses = send_all_queries(
            &self.http,
            &self.servers,
            &self.manifest.snapshot_id,
            per_server_queries,
        )
        .await?;
        let mut per_server_answers = Vec::with_capacity(responses.len());
        for response in responses {
            if response.snapshot_id != self.manifest.snapshot_id {
                bail!("PIR answer references the wrong snapshot");
            }
            if response.answer_shares.len() != lookup_keys.len() {
                bail!("PIR server returned the wrong number of answers");
            }
            per_server_answers.push(
                response
                    .answer_shares
                    .into_iter()
                    .map(|answer| STANDARD.decode(answer).map_err(Into::into))
                    .collect::<Result<Vec<_>>>()?,
            );
        }

        let mut values = Vec::new();
        for (query_index, lookup_key) in lookup_keys.iter().enumerate() {
            let shares = per_server_answers
                .iter()
                .map(|answers| answers[query_index].as_slice())
                .collect::<Vec<_>>();
            let row = dense::combine(&shares)?;
            values.extend(self.manifest.values_from_row(&row, lookup_key)?);
        }
        Ok(values)
    }
}

pub async fn spawn(snapshot: Arc<Snapshot>, bind: &str) -> Result<RunningServer> {
    spawn_with_config(snapshot, bind, PirServiceConfig::default()).await
}

pub async fn spawn_with_config(
    snapshot: Arc<Snapshot>,
    bind: &str,
    config: PirServiceConfig,
) -> Result<RunningServer> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let service = PirService::new(snapshot, config)?;
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router(service)).await {
            eprintln!("PIR sidecar stopped: {error}");
        }
    });
    Ok(RunningServer { address, task })
}

pub async fn serve(snapshot: Arc<Snapshot>, bind: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!("PIR sidecar listening on http://{}", listener.local_addr()?);
    let service = PirService::new(snapshot, PirServiceConfig::default())?;
    axum::serve(listener, router(service)).await?;
    Ok(())
}

async fn fetch_all_manifests(
    client: &reqwest::Client,
    servers: &[String],
) -> Result<Vec<Manifest>> {
    let mut tasks = JoinSet::new();
    for (index, server) in servers.iter().enumerate() {
        let client = client.clone();
        let url = format!("{server}/v1/manifest");
        tasks.spawn(async move {
            let manifest = fetch_manifest(&client, &url).await?;
            Ok::<_, anyhow::Error>((index, manifest))
        });
    }
    collect_indexed(&mut tasks, servers.len(), "manifest request").await
}

async fn send_all_queries(
    client: &reqwest::Client,
    servers: &[String],
    snapshot_id: &str,
    per_server_queries: Vec<Vec<Vec<u8>>>,
) -> Result<Vec<AnswerResponse>> {
    if servers.len() != per_server_queries.len() {
        bail!("server and query share counts differ");
    }
    let mut tasks = JoinSet::new();
    for (index, (server, query_shares)) in servers.iter().zip(per_server_queries).enumerate() {
        let client = client.clone();
        let url = format!("{server}/v1/query");
        let request = QueryRequest {
            snapshot_id: snapshot_id.to_owned(),
            query_shares: query_shares
                .into_iter()
                .map(|query| STANDARD.encode(query))
                .collect(),
        };
        tasks.spawn(async move {
            let response = send_query(&client, &url, &request).await?;
            Ok::<_, anyhow::Error>((index, response))
        });
    }
    collect_indexed(&mut tasks, servers.len(), "query request").await
}

async fn collect_indexed<T: Send + 'static>(
    tasks: &mut JoinSet<Result<(usize, T)>>,
    count: usize,
    operation: &str,
) -> Result<Vec<T>> {
    let mut values = (0..count).map(|_| None).collect::<Vec<_>>();
    while let Some(result) = tasks.join_next().await {
        let (index, value) = result.with_context(|| format!("{operation} task failed"))??;
        values[index] = Some(value);
    }
    values
        .into_iter()
        .map(|value| value.with_context(|| format!("{operation} returned no value")))
        .collect()
}

fn router(service: PirService) -> Router {
    let max_body_bytes = dense::query_size(service.manifest().bucket_count)
        .saturating_mul(service.max_batch_size())
        .saturating_mul(2)
        .max(1024);
    Router::new()
        .route("/v1/manifest", get(get_manifest))
        .route("/v1/query", post(post_query))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(service)
}

async fn get_manifest(State(service): State<PirService>) -> Json<Manifest> {
    Json(service.manifest().clone())
}

async fn post_query(
    State(service): State<PirService>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<AnswerResponse>, (StatusCode, String)> {
    if request.snapshot_id != service.manifest().snapshot_id {
        return Err((StatusCode::CONFLICT, "snapshot ID mismatch".into()));
    }
    let queries = request
        .query_shares
        .into_iter()
        .map(|query| {
            STANDARD
                .decode(query)
                .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let answers = service
        .evaluate_batch(queries)
        .await
        .map_err(|error| match error {
            EvaluationError::Overloaded => (StatusCode::TOO_MANY_REQUESTS, error.to_string()),
            EvaluationError::Invalid(_) => (StatusCode::BAD_REQUEST, error.to_string()),
            EvaluationError::Worker(_) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
        })?;
    Ok(Json(AnswerResponse {
        snapshot_id: service.manifest().snapshot_id.clone(),
        answer_shares: answers
            .into_iter()
            .map(|answer| STANDARD.encode(answer))
            .collect(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{Record, SnapshotConfig};

    fn config() -> SnapshotConfig {
        SnapshotConfig {
            bucket_count: 64,
            bucket_capacity: 4,
            values_per_page: 2,
            max_key_bytes: 16,
            max_value_bytes: 32,
            source: "test".into(),
            source_cutoff: "1".into(),
        }
    }

    #[tokio::test]
    async fn reusable_client_supports_two_and_three_servers() {
        let snapshot =
            Arc::new(Snapshot::build(vec![Record::new(b"key", b"value")], config()).unwrap());
        let mut servers = Vec::new();
        for _ in 0..3 {
            servers.push(spawn(Arc::clone(&snapshot), "127.0.0.1:0").await.unwrap());
        }
        let urls = servers
            .iter()
            .map(|server| format!("http://{}", server.address))
            .collect::<Vec<_>>();
        for server_count in 2..=3 {
            let client = PirClient::connect(&urls[..server_count]).await.unwrap();
            assert_eq!(
                client.private_lookup(b"key").await.unwrap(),
                vec![b"value".to_vec()]
            );
            assert_eq!(client.server_count(), server_count);
        }
    }

    #[tokio::test]
    async fn paged_tag_lookup_returns_every_value() {
        let records = (0..5)
            .map(|index| Record::new("tag", format!("value-{index}")))
            .collect();
        let snapshot = Arc::new(Snapshot::build_paged(records, config()).unwrap());
        let left = spawn(Arc::clone(&snapshot), "127.0.0.1:0").await.unwrap();
        let right = spawn(snapshot, "127.0.0.1:0").await.unwrap();
        let urls = [
            format!("http://{}", left.address),
            format!("http://{}", right.address),
        ];
        let client = PirClient::connect(&urls).await.unwrap();
        let values = client.private_lookup(b"tag").await.unwrap();
        assert_eq!(values.len(), 5);
        assert_eq!(client.manifest().lookup_page_count, 3);
    }
}
