use std::collections::BTreeSet;
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
use crate::service::{EvaluationError, PirService, PirServiceConfig, WindowEvaluation};
use crate::snapshot::{bucket_for_key, CatalogManifest, Manifest, Snapshot, SnapshotCatalog};

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

#[derive(Debug, Serialize, Deserialize)]
pub struct WindowQueryRequest {
    pub windows: Vec<WindowQueryShares>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WindowQueryShares {
    pub window_id: String,
    pub snapshot_id: String,
    pub query_shares: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WindowAnswerResponse {
    pub windows: Vec<WindowAnswerShares>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WindowAnswerShares {
    pub window_id: String,
    pub snapshot_id: String,
    pub answer_shares: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowLookup {
    pub window_id: String,
    pub values: Vec<Vec<u8>>,
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
    catalog: CatalogManifest,
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
        let catalogs = fetch_all_catalogs(&http, &servers).await?;
        let catalog = catalogs.first().context("no PIR catalogs returned")?;
        catalog.validate()?;
        for candidate in &catalogs[1..] {
            candidate.validate()?;
            if candidate != catalog {
                bail!("PIR servers advertise different snapshot catalogs");
            }
        }
        Ok(Self {
            http,
            servers: servers.into(),
            catalog: catalog.clone(),
        })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.catalog.global
    }

    pub fn catalog(&self) -> &CatalogManifest {
        &self.catalog
    }

    pub fn server_count(&self) -> usize {
        self.servers.len()
    }

    pub async fn private_lookup(&self, key: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.private_lookup_global(key).await
    }

    pub async fn private_lookup_global(&self, key: &[u8]) -> Result<Vec<Vec<u8>>> {
        let manifest = &self.catalog.global;
        let (lookup_keys, per_server_queries) = make_queries(manifest, key, self.servers.len())?;

        let responses = send_all_queries(
            &self.http,
            &self.servers,
            "/v1/query/global",
            &manifest.snapshot_id,
            per_server_queries,
        )
        .await?;
        decode_answers(manifest, &lookup_keys, responses)
    }

    pub async fn private_lookup_windows<S: AsRef<str>>(
        &self,
        key: &[u8],
        window_ids: &[S],
    ) -> Result<Vec<WindowLookup>> {
        if window_ids.is_empty() {
            bail!("at least one public window is required");
        }
        let mut seen = BTreeSet::new();
        let mut lookups = Vec::with_capacity(window_ids.len());
        let mut per_server_windows = (0..self.servers.len())
            .map(|_| Vec::with_capacity(window_ids.len()))
            .collect::<Vec<_>>();

        for window_id in window_ids {
            let window_id = window_id.as_ref();
            if !seen.insert(window_id.to_owned()) {
                bail!("duplicate public window {window_id}");
            }
            let manifest = self
                .catalog
                .windows
                .get(window_id)
                .with_context(|| format!("unknown public window {window_id}"))?;
            let (lookup_keys, per_server_queries) =
                make_queries(manifest, key, self.servers.len())?;
            lookups.push((window_id.to_owned(), manifest.clone(), lookup_keys));
            for (server_windows, query_shares) in
                per_server_windows.iter_mut().zip(per_server_queries)
            {
                server_windows.push(WindowQueryShares {
                    window_id: window_id.to_owned(),
                    snapshot_id: manifest.snapshot_id.clone(),
                    query_shares: query_shares
                        .into_iter()
                        .map(|query| STANDARD.encode(query))
                        .collect(),
                });
            }
        }

        let responses =
            send_all_window_queries(&self.http, &self.servers, per_server_windows).await?;
        if responses
            .iter()
            .any(|response| response.windows.len() != lookups.len())
        {
            bail!("PIR server returned the wrong number of public windows");
        }

        let mut results = Vec::with_capacity(lookups.len());
        for (window_index, (window_id, manifest, lookup_keys)) in lookups.into_iter().enumerate() {
            let mut answers = Vec::with_capacity(responses.len());
            for response in &responses {
                let response = &response.windows[window_index];
                if response.window_id != window_id || response.snapshot_id != manifest.snapshot_id {
                    bail!("PIR answer references the wrong public window snapshot");
                }
                if response.answer_shares.len() != lookup_keys.len() {
                    bail!("PIR server returned the wrong number of answers");
                }
                answers.push(
                    response
                        .answer_shares
                        .iter()
                        .map(|answer| STANDARD.decode(answer).map_err(Into::into))
                        .collect::<Result<Vec<_>>>()?,
                );
            }
            results.push(WindowLookup {
                window_id,
                values: combine_answers(&manifest, &lookup_keys, &answers)?,
            });
        }
        Ok(results)
    }
}

fn make_queries(
    manifest: &Manifest,
    key: &[u8],
    server_count: usize,
) -> Result<(Vec<Vec<u8>>, Vec<Vec<Vec<u8>>>)> {
    let lookup_keys = manifest.lookup_keys(key)?;
    let mut per_server_queries = (0..server_count)
        .map(|_| Vec::with_capacity(lookup_keys.len()))
        .collect::<Vec<_>>();
    for lookup_key in &lookup_keys {
        let bucket = bucket_for_key(lookup_key, manifest.bucket_count);
        let shares = dense::query_shares(bucket, manifest.bucket_count, server_count, &mut OsRng)?;
        for (server_queries, share) in per_server_queries.iter_mut().zip(shares) {
            server_queries.push(share);
        }
    }
    Ok((lookup_keys, per_server_queries))
}

fn decode_answers(
    manifest: &Manifest,
    lookup_keys: &[Vec<u8>],
    responses: Vec<AnswerResponse>,
) -> Result<Vec<Vec<u8>>> {
    let mut per_server_answers = Vec::with_capacity(responses.len());
    for response in responses {
        if response.snapshot_id != manifest.snapshot_id {
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

    combine_answers(manifest, lookup_keys, &per_server_answers)
}

fn combine_answers(
    manifest: &Manifest,
    lookup_keys: &[Vec<u8>],
    per_server_answers: &[Vec<Vec<u8>>],
) -> Result<Vec<Vec<u8>>> {
    let mut values = Vec::new();
    for (query_index, lookup_key) in lookup_keys.iter().enumerate() {
        let shares = per_server_answers
            .iter()
            .map(|answers| answers[query_index].as_slice())
            .collect::<Vec<_>>();
        let row = dense::combine(&shares)?;
        values.extend(manifest.values_from_row(&row, lookup_key)?);
    }
    Ok(values)
}

pub async fn spawn(snapshot: Arc<Snapshot>, bind: &str) -> Result<RunningServer> {
    spawn_with_config(snapshot, bind, PirServiceConfig::default()).await
}

pub async fn spawn_with_config(
    snapshot: Arc<Snapshot>,
    bind: &str,
    config: PirServiceConfig,
) -> Result<RunningServer> {
    let catalog = Arc::new(SnapshotCatalog::global_only(snapshot)?);
    spawn_catalog_with_config(catalog, bind, config).await
}

pub async fn spawn_catalog(catalog: Arc<SnapshotCatalog>, bind: &str) -> Result<RunningServer> {
    spawn_catalog_with_config(catalog, bind, PirServiceConfig::default()).await
}

pub async fn spawn_catalog_with_config(
    catalog: Arc<SnapshotCatalog>,
    bind: &str,
    config: PirServiceConfig,
) -> Result<RunningServer> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let service = PirService::from_catalog(catalog, config)?;
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router(service)).await {
            eprintln!("PIR sidecar stopped: {error}");
        }
    });
    Ok(RunningServer { address, task })
}

pub async fn serve(snapshot: Arc<Snapshot>, bind: &str) -> Result<()> {
    let catalog = Arc::new(SnapshotCatalog::global_only(snapshot)?);
    serve_catalog(catalog, bind).await
}

pub async fn serve_catalog(catalog: Arc<SnapshotCatalog>, bind: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!("PIR sidecar listening on http://{}", listener.local_addr()?);
    let service = PirService::from_catalog(catalog, PirServiceConfig::default())?;
    axum::serve(listener, router(service)).await?;
    Ok(())
}

async fn fetch_all_catalogs(
    client: &reqwest::Client,
    servers: &[String],
) -> Result<Vec<CatalogManifest>> {
    let mut tasks = JoinSet::new();
    for (index, server) in servers.iter().enumerate() {
        let client = client.clone();
        let url = format!("{server}/v1/catalog");
        tasks.spawn(async move {
            let catalog = fetch_catalog(&client, &url).await?;
            Ok::<_, anyhow::Error>((index, catalog))
        });
    }
    collect_indexed(&mut tasks, servers.len(), "catalog request").await
}

async fn send_all_queries(
    client: &reqwest::Client,
    servers: &[String],
    route: &str,
    snapshot_id: &str,
    per_server_queries: Vec<Vec<Vec<u8>>>,
) -> Result<Vec<AnswerResponse>> {
    if servers.len() != per_server_queries.len() {
        bail!("server and query share counts differ");
    }
    let mut tasks = JoinSet::new();
    for (index, (server, query_shares)) in servers.iter().zip(per_server_queries).enumerate() {
        let client = client.clone();
        let url = format!("{server}{route}");
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

async fn send_all_window_queries(
    client: &reqwest::Client,
    servers: &[String],
    per_server_windows: Vec<Vec<WindowQueryShares>>,
) -> Result<Vec<WindowAnswerResponse>> {
    if servers.len() != per_server_windows.len() {
        bail!("server and public-window share counts differ");
    }
    let mut tasks = JoinSet::new();
    for (index, (server, windows)) in servers.iter().zip(per_server_windows).enumerate() {
        let client = client.clone();
        let url = format!("{server}/v1/query/windows");
        tasks.spawn(async move {
            let response =
                send_window_query(&client, &url, &WindowQueryRequest { windows }).await?;
            Ok::<_, anyhow::Error>((index, response))
        });
    }
    collect_indexed(&mut tasks, servers.len(), "public-window query request").await
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
    let max_body_bytes = service
        .max_query_size()
        .saturating_mul(service.max_batch_size())
        .saturating_mul(2)
        .max(1024);
    Router::new()
        .route("/v1/catalog", get(get_catalog))
        .route("/v1/manifest", get(get_manifest))
        .route("/v1/query/global", post(post_query))
        .route("/v1/query/windows", post(post_window_query))
        .route("/v1/query", post(post_query))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(service)
}

async fn get_catalog(State(service): State<PirService>) -> Json<CatalogManifest> {
    Json(service.catalog_manifest().clone())
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

async fn post_window_query(
    State(service): State<PirService>,
    Json(request): Json<WindowQueryRequest>,
) -> Result<Json<WindowAnswerResponse>, (StatusCode, String)> {
    if request.windows.is_empty() || request.windows.len() > service.max_batch_size() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "public window count must be between 1 and {}",
                service.max_batch_size()
            ),
        ));
    }
    for window in &request.windows {
        let Some(manifest) = service.catalog_manifest().windows.get(&window.window_id) else {
            return Err((
                StatusCode::NOT_FOUND,
                format!("unknown public window {}", window.window_id),
            ));
        };
        if window.snapshot_id != manifest.snapshot_id {
            return Err((
                StatusCode::CONFLICT,
                format!(
                    "snapshot ID mismatch for public window {}",
                    window.window_id
                ),
            ));
        }
    }
    let requests = request
        .windows
        .into_iter()
        .map(|window| {
            let query_shares = window
                .query_shares
                .into_iter()
                .map(|query| {
                    STANDARD
                        .decode(query)
                        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(WindowEvaluation {
                window_id: window.window_id,
                snapshot_id: window.snapshot_id,
                query_shares,
            })
        })
        .collect::<std::result::Result<Vec<_>, (StatusCode, String)>>()?;
    let answers = service
        .evaluate_windows(requests)
        .await
        .map_err(evaluation_error_response)?;
    Ok(Json(WindowAnswerResponse {
        windows: answers
            .into_iter()
            .map(|answer| WindowAnswerShares {
                window_id: answer.window_id,
                snapshot_id: answer.snapshot_id,
                answer_shares: answer
                    .answer_shares
                    .into_iter()
                    .map(|share| STANDARD.encode(share))
                    .collect(),
            })
            .collect(),
    }))
}

fn evaluation_error_response(error: EvaluationError) -> (StatusCode, String) {
    match error {
        EvaluationError::Overloaded => (StatusCode::TOO_MANY_REQUESTS, error.to_string()),
        EvaluationError::Invalid(_) => (StatusCode::BAD_REQUEST, error.to_string()),
        EvaluationError::Worker(_) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn fetch_catalog(client: &reqwest::Client, url: &str) -> Result<CatalogManifest> {
    client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .with_context(|| format!("decode snapshot catalog from {url}"))
}

async fn send_window_query(
    client: &reqwest::Client,
    url: &str,
    request: &WindowQueryRequest,
) -> Result<WindowAnswerResponse> {
    client
        .post(url)
        .json(request)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .with_context(|| format!("decode public-window answer from {url}"))
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
    use std::collections::BTreeMap;

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

    #[tokio::test]
    async fn global_and_public_window_endpoints_return_the_expected_scope() {
        let global = Arc::new(
            Snapshot::build_paged(
                vec![
                    Record::new("tag", "global-old"),
                    Record::new("tag", "global-new"),
                    Record::new("other", "not-returned"),
                ],
                config(),
            )
            .unwrap(),
        );
        let mut old_config = config();
        old_config.source_cutoff = "2026-W31".into();
        let old = Arc::new(
            Snapshot::build_paged(vec![Record::new("tag", "old-match")], old_config).unwrap(),
        );
        let mut new_config = config();
        new_config.bucket_count = 32;
        new_config.source_cutoff = "2026-W32".into();
        let new = Arc::new(
            Snapshot::build_paged(
                vec![
                    Record::new("tag", "new-match"),
                    Record::new("other", "not-returned"),
                ],
                new_config,
            )
            .unwrap(),
        );
        let catalog = Arc::new(
            SnapshotCatalog::new(
                global,
                BTreeMap::from([("2026-W31".into(), old), ("2026-W32".into(), new)]),
            )
            .unwrap(),
        );

        let mut servers = Vec::new();
        for _ in 0..3 {
            servers.push(
                spawn_catalog(Arc::clone(&catalog), "127.0.0.1:0")
                    .await
                    .unwrap(),
            );
        }
        let urls = servers
            .iter()
            .map(|server| format!("http://{}", server.address))
            .collect::<Vec<_>>();
        let client = PirClient::connect(&urls).await.unwrap();

        assert_eq!(
            client.private_lookup_global(b"tag").await.unwrap(),
            vec![b"global-new".to_vec(), b"global-old".to_vec()]
        );
        let window_values = client
            .private_lookup_windows(b"tag", &["2026-W32", "2026-W31"])
            .await
            .unwrap();
        assert_eq!(
            window_values,
            vec![
                WindowLookup {
                    window_id: "2026-W32".into(),
                    values: vec![b"new-match".to_vec()],
                },
                WindowLookup {
                    window_id: "2026-W31".into(),
                    values: vec![b"old-match".to_vec()],
                },
            ]
        );
        assert_eq!(client.catalog().windows.len(), 2);
        assert_eq!(client.catalog().windows["2026-W32"].bucket_count, 32);
        assert!(client
            .private_lookup_windows(b"tag", &["unknown"])
            .await
            .unwrap_err()
            .to_string()
            .contains("unknown public window"));
    }
}
