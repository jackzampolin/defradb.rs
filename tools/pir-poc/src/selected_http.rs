//! HTTP transport for the three selected use cases.
//!
//! Each replica exposes the same authenticated immutable manifest.  Dense
//! requests contain only that replica's selector shares; decoy requests contain
//! visible keys.  Compact-DPF registration remains two-party and mutable while
//! being generation-bound at the HTTP boundary.

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
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio::task::{JoinHandle, JoinSet};

use crate::dense;
use crate::selected::{
    decode_table_answer, AuthenticatedUseCaseManifest, OrdinalDirectory, TableUseCase, UseCaseStore,
};
use crate::subscription::{
    combine_compact, compact_registration, NotificationShare, SubscriptionId,
};
use crate::verification::{decrypt_projection_values, verify_nullifier_witness};

const LOCAL_MAX_HTTP_BODY_BYTES: usize = 512 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UseCaseMetadata {
    pub manifest: AuthenticatedUseCaseManifest,
    pub nullifier_directory: OrdinalDirectory,
    pub encrypted_tag_directory: OrdinalDirectory,
}

impl UseCaseMetadata {
    pub(crate) fn validate(&self, operator_key: &[u8; 32]) -> Result<()> {
        let limits = &self.manifest.manifest.limits;
        self.manifest.verify(operator_key, limits)?;
        self.nullifier_directory
            .validate(limits.max_client_metadata_bytes)?;
        self.encrypted_tag_directory
            .validate(limits.max_client_metadata_bytes)?;
        if self.nullifier_directory.digest
            != self.manifest.manifest.nullifier_table.directory_digest
            || self.encrypted_tag_directory.digest
                != self.manifest.manifest.encrypted_tag_table.directory_digest
        {
            bail!("selected POC metadata is not bound to its manifest");
        }
        Ok(())
    }

    pub(crate) fn table_parts(
        &self,
        use_case: TableUseCase,
        decoy: bool,
    ) -> (
        &crate::selected::PrivateTableManifest,
        &OrdinalDirectory,
        &'static str,
    ) {
        match (use_case, decoy) {
            (TableUseCase::Nullifier, false) => (
                &self.manifest.manifest.nullifier_table,
                &self.nullifier_directory,
                "/v1/nullifier/private",
            ),
            (TableUseCase::Nullifier, true) => (
                &self.manifest.manifest.nullifier_table,
                &self.nullifier_directory,
                "/v1/nullifier/decoy",
            ),
            (TableUseCase::EncryptedTag, false) => (
                &self.manifest.manifest.encrypted_tag_table,
                &self.encrypted_tag_directory,
                "/v1/tag/private",
            ),
            (TableUseCase::EncryptedTag, true) => (
                &self.manifest.manifest.encrypted_tag_table,
                &self.encrypted_tag_directory,
                "/v1/tag/decoy",
            ),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PrivateQueryRequest {
    pub body_digest_hex: String,
    pub query_shares: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PrivateAnswerResponse {
    pub body_digest_hex: String,
    pub answer_shares: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DecoyQueryRequest {
    pub body_digest_hex: String,
    pub candidate_keys: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DecoyAnswerResponse {
    pub body_digest_hex: String,
    pub rows: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ShinzoRegistrationRequest {
    pub body_digest_hex: String,
    pub subscription_id_hex: String,
    pub server_key_base64: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ShinzoEventRequest {
    pub body_digest_hex: String,
    pub subscription_id_hex: String,
    pub event_bucket: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ShinzoEventResponse {
    pub body_digest_hex: String,
    pub subscription_id_hex: String,
    pub party_index: usize,
    pub value_base64: String,
}

#[derive(Clone)]
pub(crate) struct SelectedService {
    store: Arc<UseCaseStore>,
    permits: Arc<Semaphore>,
}

impl SelectedService {
    pub(crate) fn new(store: Arc<UseCaseStore>) -> Result<Self> {
        store.limits.validate()?;
        Ok(Self {
            permits: Arc::new(Semaphore::new(store.limits.max_in_flight)),
            store,
        })
    }

    pub(crate) fn body_digest_hex(&self) -> String {
        hex::encode(self.store.manifest.manifest.body_digest)
    }

    fn require_generation(&self, supplied: &str) -> Result<()> {
        if supplied != self.body_digest_hex() {
            bail!("selected POC generation mismatch");
        }
        Ok(())
    }

    pub(crate) fn metadata(&self) -> UseCaseMetadata {
        UseCaseMetadata {
            manifest: self.store.manifest.clone(),
            nullifier_directory: self.store.nullifiers.directory.clone(),
            encrypted_tag_directory: self.store.encrypted_tags.directory.clone(),
        }
    }

    async fn private_query(
        &self,
        use_case: TableUseCase,
        request: PrivateQueryRequest,
    ) -> Result<PrivateAnswerResponse> {
        self.require_generation(&request.body_digest_hex)?;
        let queries = request
            .query_shares
            .into_iter()
            .map(|share| STANDARD.decode(share).map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        let table = Arc::clone(self.store.table(use_case));
        let limits = self.store.limits.clone();
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .context("selected POC evaluator is at capacity")?;
        let answers = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            table.evaluate_batch(&queries, &limits)
        })
        .await
        .context("selected POC evaluator worker failed")??;
        Ok(PrivateAnswerResponse {
            body_digest_hex: self.body_digest_hex(),
            answer_shares: answers
                .into_iter()
                .map(|answer| STANDARD.encode(answer))
                .collect(),
        })
    }

    async fn decoy_query(
        &self,
        use_case: TableUseCase,
        request: DecoyQueryRequest,
    ) -> Result<DecoyAnswerResponse> {
        self.require_generation(&request.body_digest_hex)?;
        let candidates = request
            .candidate_keys
            .into_iter()
            .map(|candidate| STANDARD.decode(candidate).map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        let table = Arc::clone(self.store.table(use_case));
        let limits = self.store.limits.clone();
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .context("selected POC evaluator is at capacity")?;
        let rows = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            table.direct_rows(&candidates, &limits)
        })
        .await
        .context("selected POC decoy worker failed")??;
        Ok(DecoyAnswerResponse {
            body_digest_hex: self.body_digest_hex(),
            rows: rows.into_iter().map(|row| STANDARD.encode(row)).collect(),
        })
    }

    async fn register_shinzo(&self, request: ShinzoRegistrationRequest) -> Result<()> {
        self.require_generation(&request.body_digest_hex)?;
        let id = parse_subscription_id(&request.subscription_id_hex)?;
        let key = STANDARD.decode(request.server_key_base64)?;
        let mut server = self
            .store
            .shinzo
            .lock()
            .map_err(|_| anyhow::anyhow!("subscription lock poisoned"))?;
        if server.subscription_count() >= self.store.limits.max_subscriptions {
            bail!("subscription admission limit reached");
        }
        server.register(id, &key)
    }

    async fn evaluate_shinzo(&self, request: ShinzoEventRequest) -> Result<ShinzoEventResponse> {
        self.require_generation(&request.body_digest_hex)?;
        let id = parse_subscription_id(&request.subscription_id_hex)?;
        let server = self
            .store
            .shinzo
            .lock()
            .map_err(|_| anyhow::anyhow!("subscription lock poisoned"))?;
        let share = server.evaluate_one(id, request.event_bucket)?;
        Ok(ShinzoEventResponse {
            body_digest_hex: self.body_digest_hex(),
            subscription_id_hex: id.to_string(),
            party_index: share.party_index(),
            value_base64: STANDARD.encode(share.value()),
        })
    }

    /// Dispatches the same JSON protocol used by the direct Axum routes.  The
    /// OHTTP gateway calls this after decrypting and validating Binary HTTP,
    /// keeping transport privacy independent from PIR evaluation.
    pub(crate) async fn dispatch_json(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> (StatusCode, Vec<u8>) {
        let result: Result<(StatusCode, Vec<u8>)> = async {
            match (method, path) {
                ("GET", "/v1/manifest") if body.is_empty() => {
                    Ok((StatusCode::OK, serde_json::to_vec(&self.metadata())?))
                }
                ("POST", "/v1/nullifier/private") => Ok((
                    StatusCode::OK,
                    serde_json::to_vec(
                        &self
                            .private_query(TableUseCase::Nullifier, serde_json::from_slice(body)?)
                            .await?,
                    )?,
                )),
                ("POST", "/v1/tag/private") => Ok((
                    StatusCode::OK,
                    serde_json::to_vec(
                        &self
                            .private_query(
                                TableUseCase::EncryptedTag,
                                serde_json::from_slice(body)?,
                            )
                            .await?,
                    )?,
                )),
                ("POST", "/v1/nullifier/decoy") => Ok((
                    StatusCode::OK,
                    serde_json::to_vec(
                        &self
                            .decoy_query(TableUseCase::Nullifier, serde_json::from_slice(body)?)
                            .await?,
                    )?,
                )),
                ("POST", "/v1/tag/decoy") => Ok((
                    StatusCode::OK,
                    serde_json::to_vec(
                        &self
                            .decoy_query(TableUseCase::EncryptedTag, serde_json::from_slice(body)?)
                            .await?,
                    )?,
                )),
                ("POST", "/v1/shinzo/register") => {
                    self.register_shinzo(serde_json::from_slice(body)?).await?;
                    Ok((StatusCode::NO_CONTENT, Vec::new()))
                }
                ("POST", "/v1/shinzo/event") => Ok((
                    StatusCode::OK,
                    serde_json::to_vec(
                        &self.evaluate_shinzo(serde_json::from_slice(body)?).await?,
                    )?,
                )),
                _ => Ok((
                    StatusCode::NOT_FOUND,
                    serde_json::to_vec("OHTTP target is not an admitted PIR route")?,
                )),
            }
        }
        .await;
        match result {
            Ok(response) => response,
            Err(error) => {
                let (status, message) = http_error(error);
                (
                    status,
                    serde_json::to_vec(&message).unwrap_or_else(|_| b"\"request failed\"".to_vec()),
                )
            }
        }
    }
}

pub struct RunningSelectedServer {
    pub address: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for RunningSelectedServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn spawn_selected(store: Arc<UseCaseStore>, bind: &str) -> Result<RunningSelectedServer> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let service = SelectedService::new(store)?;
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, selected_router(service)).await {
            eprintln!("selected PIR sidecar stopped: {error}");
        }
    });
    Ok(RunningSelectedServer { address, task })
}

pub async fn serve_selected(store: Arc<UseCaseStore>, bind: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!(
        "selected PIR sidecar listening on http://{}",
        listener.local_addr()?
    );
    axum::serve(listener, selected_router(SelectedService::new(store)?)).await?;
    Ok(())
}

fn selected_router(service: SelectedService) -> Router {
    let body_limit = service
        .store
        .limits
        .max_query_bytes
        .saturating_mul(2)
        .clamp(16 * 1024, 512 * 1024 * 1024);
    Router::new()
        .route("/v1/manifest", get(get_metadata))
        .route("/v1/nullifier/private", post(post_nullifier_private))
        .route("/v1/nullifier/decoy", post(post_nullifier_decoy))
        .route("/v1/tag/private", post(post_tag_private))
        .route("/v1/tag/decoy", post(post_tag_decoy))
        .route("/v1/shinzo/register", post(post_shinzo_register))
        .route("/v1/shinzo/event", post(post_shinzo_event))
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(service)
}

async fn get_metadata(State(service): State<SelectedService>) -> Json<UseCaseMetadata> {
    Json(service.metadata())
}

async fn post_nullifier_private(
    State(service): State<SelectedService>,
    Json(request): Json<PrivateQueryRequest>,
) -> Result<Json<PrivateAnswerResponse>, (StatusCode, String)> {
    service
        .private_query(TableUseCase::Nullifier, request)
        .await
        .map(Json)
        .map_err(http_error)
}

async fn post_tag_private(
    State(service): State<SelectedService>,
    Json(request): Json<PrivateQueryRequest>,
) -> Result<Json<PrivateAnswerResponse>, (StatusCode, String)> {
    service
        .private_query(TableUseCase::EncryptedTag, request)
        .await
        .map(Json)
        .map_err(http_error)
}

async fn post_nullifier_decoy(
    State(service): State<SelectedService>,
    Json(request): Json<DecoyQueryRequest>,
) -> Result<Json<DecoyAnswerResponse>, (StatusCode, String)> {
    service
        .decoy_query(TableUseCase::Nullifier, request)
        .await
        .map(Json)
        .map_err(http_error)
}

async fn post_tag_decoy(
    State(service): State<SelectedService>,
    Json(request): Json<DecoyQueryRequest>,
) -> Result<Json<DecoyAnswerResponse>, (StatusCode, String)> {
    service
        .decoy_query(TableUseCase::EncryptedTag, request)
        .await
        .map(Json)
        .map_err(http_error)
}

async fn post_shinzo_register(
    State(service): State<SelectedService>,
    Json(request): Json<ShinzoRegistrationRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    service.register_shinzo(request).await.map_err(http_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn post_shinzo_event(
    State(service): State<SelectedService>,
    Json(request): Json<ShinzoEventRequest>,
) -> Result<Json<ShinzoEventResponse>, (StatusCode, String)> {
    service
        .evaluate_shinzo(request)
        .await
        .map(Json)
        .map_err(http_error)
}

fn http_error(error: anyhow::Error) -> (StatusCode, String) {
    let message = error.to_string();
    let status = if message.contains("capacity") || message.contains("limit") {
        StatusCode::TOO_MANY_REQUESTS
    } else if message.contains("generation mismatch") {
        StatusCode::CONFLICT
    } else {
        StatusCode::BAD_REQUEST
    };
    (status, message)
}

pub struct UseCaseClient {
    http: reqwest::Client,
    servers: Arc<[String]>,
    pub metadata: UseCaseMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DecoyClientResult {
    pub values: Option<Vec<Vec<u8>>>,
    pub returned_rows: usize,
    pub processed_rows: usize,
    pub ignored_without_decoding: usize,
    pub target_index: usize,
}

impl UseCaseClient {
    pub async fn connect<S: AsRef<str>>(
        server_urls: &[S],
        operator_key: &[u8; 32],
    ) -> Result<Self> {
        Self::connect_with_minimum(server_urls, operator_key, 2).await
    }

    pub async fn connect_decoy(server_url: &str, operator_key: &[u8; 32]) -> Result<Self> {
        Self::connect_with_minimum(&[server_url], operator_key, 1).await
    }

    async fn connect_with_minimum<S: AsRef<str>>(
        server_urls: &[S],
        operator_key: &[u8; 32],
        minimum_servers: usize,
    ) -> Result<Self> {
        if server_urls.len() < minimum_servers {
            bail!("selected POC client received too few replicas");
        }
        let servers = server_urls
            .iter()
            .map(|server| server.as_ref().trim_end_matches('/').to_owned())
            .collect::<Vec<_>>();
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()?;
        let metadata = fetch_all_metadata(&http, &servers).await?;
        let first = metadata
            .first()
            .context("no selected POC metadata returned")?;
        for candidate in &metadata {
            if candidate != first {
                bail!("selected POC replicas advertise different generations");
            }
        }
        first.validate(operator_key)?;
        Ok(Self {
            http,
            servers: servers.into(),
            metadata: first.clone(),
        })
    }

    pub fn server_count(&self) -> usize {
        self.servers.len()
    }

    pub async fn strict_lookup(
        &self,
        use_case: TableUseCase,
        key: &[u8],
    ) -> Result<Option<Vec<Vec<u8>>>> {
        let (manifest, directory, route) = self.metadata.table_parts(use_case, false);
        let (ordinal, _) = directory.ordinal(key);
        let shares =
            dense::query_shares(ordinal, manifest.row_count, self.servers.len(), &mut OsRng)?;
        let body_digest_hex = hex::encode(self.metadata.manifest.manifest.body_digest);
        let maximum_answer_body = manifest
            .answer_share_bytes
            .saturating_mul(2)
            .saturating_add(4 * 1024)
            .min(LOCAL_MAX_HTTP_BODY_BYTES);
        let mut tasks = JoinSet::new();
        for (index, (server, share)) in self.servers.iter().zip(shares).enumerate() {
            let http = self.http.clone();
            let url = format!("{server}{route}");
            let request = PrivateQueryRequest {
                body_digest_hex: body_digest_hex.clone(),
                query_shares: vec![STANDARD.encode(share)],
            };
            tasks.spawn(async move {
                let response = http
                    .post(url)
                    .json(&request)
                    .send()
                    .await?
                    .error_for_status()?;
                let response: PrivateAnswerResponse =
                    bounded_json(response, maximum_answer_body).await?;
                Ok::<_, anyhow::Error>((index, response))
            });
        }
        let responses = collect_indexed(&mut tasks, self.servers.len()).await?;
        let mut answers = Vec::with_capacity(responses.len());
        for response in responses {
            if response.body_digest_hex != body_digest_hex || response.answer_shares.len() != 1 {
                bail!("selected POC server returned a mismatched private answer");
            }
            answers.push(STANDARD.decode(&response.answer_shares[0])?);
        }
        let row = dense::combine(&answers)?;
        decode_table_answer(manifest, &row, key)
    }

    /// Reconstructs and verifies a Shieldd indexed-tree witness against the
    /// authenticated generation root before returning it to the wallet.
    pub async fn verified_nullifier_lookup(&self, nullifier: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        let Some(values) = self
            .strict_lookup(TableUseCase::Nullifier, nullifier)
            .await?
        else {
            return Ok(None);
        };
        if values.len() != 1 {
            bail!("nullifier table returned a non-canonical result count");
        }
        let witness = values.into_iter().next().expect("one value was checked");
        verify_nullifier_witness(
            nullifier,
            &witness,
            &self
                .metadata
                .manifest
                .manifest
                .active_generation
                .manifest
                .root,
        )?;
        Ok(Some(witness))
    }

    /// Reconstructs and authenticates every encrypted projection value.  The
    /// result contains plaintext only after all AEAD tags verify.
    pub async fn verified_tag_lookup(
        &self,
        tag: &[u8],
        projection_key: &[u8; 32],
    ) -> Result<Option<Vec<Vec<u8>>>> {
        let Some(values) = self.strict_lookup(TableUseCase::EncryptedTag, tag).await? else {
            return Ok(None);
        };
        let generation = &self.metadata.manifest.manifest.active_generation.manifest;
        Ok(Some(decrypt_projection_values(
            projection_key,
            generation.height,
            &generation.root,
            tag,
            &values,
        )?))
    }

    /// Uses one visible-candidate server and decodes only the target row.  The
    /// remaining Base64 row strings are dropped without row parsing or AEAD.
    pub async fn decoy_lookup(
        &self,
        use_case: TableUseCase,
        target: &[u8],
        candidates: &[Vec<u8>],
    ) -> Result<DecoyClientResult> {
        let target_indices = candidates
            .iter()
            .enumerate()
            .filter_map(|(index, candidate)| (candidate.as_slice() == target).then_some(index))
            .collect::<Vec<_>>();
        if target_indices.len() != 1 {
            bail!("decoy set must contain the target exactly once");
        }
        let target_index = target_indices[0];
        let (manifest, _, route) = self.metadata.table_parts(use_case, true);
        let body_digest_hex = hex::encode(self.metadata.manifest.manifest.body_digest);
        let request = DecoyQueryRequest {
            body_digest_hex: body_digest_hex.clone(),
            candidate_keys: candidates
                .iter()
                .map(|candidate| STANDARD.encode(candidate))
                .collect(),
        };
        let maximum_answer_body = manifest
            .answer_share_bytes
            .saturating_mul(candidates.len())
            .saturating_mul(2)
            .saturating_add(4 * 1024)
            .min(LOCAL_MAX_HTTP_BODY_BYTES);
        let response = self
            .http
            .post(format!("{}{}", self.servers[0], route))
            .json(&request)
            .send()
            .await?
            .error_for_status()?;
        let response: DecoyAnswerResponse = bounded_json(response, maximum_answer_body).await?;
        if response.body_digest_hex != body_digest_hex || response.rows.len() != candidates.len() {
            bail!("selected POC server returned a mismatched decoy answer");
        }
        let target_row = STANDARD.decode(&response.rows[target_index])?;
        let values = decode_table_answer(manifest, &target_row, target)?;
        Ok(DecoyClientResult {
            values,
            returned_rows: response.rows.len(),
            processed_rows: 1,
            ignored_without_decoding: response.rows.len() - 1,
            target_index,
        })
    }

    pub async fn subscribe_and_evaluate(
        &self,
        target_bucket: usize,
        event_bucket: usize,
    ) -> Result<bool> {
        if self.servers.len() != 2 {
            bail!("Compact DPF subscriptions require exactly two servers");
        }
        let registration = compact_registration(
            target_bucket,
            self.metadata.manifest.manifest.shinzo_bucket_count,
            &mut OsRng,
        )?;
        let body_digest_hex = hex::encode(self.metadata.manifest.manifest.body_digest);
        for (server, key) in self.servers.iter().zip(registration.server_keys.iter()) {
            self.http
                .post(format!("{server}/v1/shinzo/register"))
                .json(&ShinzoRegistrationRequest {
                    body_digest_hex: body_digest_hex.clone(),
                    subscription_id_hex: registration.id.to_string(),
                    server_key_base64: STANDARD.encode(key),
                })
                .send()
                .await?
                .error_for_status()?;
        }
        let mut shares = Vec::with_capacity(2);
        for server in self.servers.iter() {
            let response = self
                .http
                .post(format!("{server}/v1/shinzo/event"))
                .json(&ShinzoEventRequest {
                    body_digest_hex: body_digest_hex.clone(),
                    subscription_id_hex: registration.id.to_string(),
                    event_bucket,
                })
                .send()
                .await?
                .error_for_status()?;
            let response: ShinzoEventResponse = bounded_json(response, 16 * 1024).await?;
            if response.body_digest_hex != body_digest_hex
                || response.subscription_id_hex != registration.id.to_string()
            {
                bail!("Compact DPF server returned a mismatched event share");
            }
            let value: [u8; 16] = STANDARD
                .decode(response.value_base64)?
                .try_into()
                .map_err(|_| anyhow::anyhow!("Compact DPF event share has the wrong length"))?;
            shares.push(NotificationShare::from_wire(
                registration.id,
                response.party_index,
                value,
            )?);
        }
        combine_compact(&shares)
    }
}

async fn fetch_all_metadata(
    client: &reqwest::Client,
    servers: &[String],
) -> Result<Vec<UseCaseMetadata>> {
    let mut tasks = JoinSet::new();
    for (index, server) in servers.iter().enumerate() {
        let client = client.clone();
        let url = format!("{server}/v1/manifest");
        tasks.spawn(async move {
            let response = client.get(url).send().await?.error_for_status()?;
            let metadata = bounded_json(response, LOCAL_MAX_HTTP_BODY_BYTES).await?;
            Ok::<_, anyhow::Error>((index, metadata))
        });
    }
    collect_indexed(&mut tasks, servers.len()).await
}

pub(crate) async fn collect_indexed<T: Send + 'static>(
    tasks: &mut JoinSet<Result<(usize, T)>>,
    count: usize,
) -> Result<Vec<T>> {
    let mut values = (0..count).map(|_| None).collect::<Vec<_>>();
    while let Some(result) = tasks.join_next().await {
        let (index, value) = result.context("selected POC replica request task failed")??;
        values[index] = Some(value);
    }
    values
        .into_iter()
        .map(|value| value.context("selected POC replica returned no value"))
        .collect()
}

fn parse_subscription_id(value: &str) -> Result<SubscriptionId> {
    let bytes: [u8; 16] = hex::decode(value)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("subscription ID must be 16 bytes"))?;
    Ok(SubscriptionId::from_bytes(bytes))
}

async fn bounded_json<T: DeserializeOwned>(
    response: reqwest::Response,
    maximum_bytes: usize,
) -> Result<T> {
    let bytes =
        bounded_response_bytes(response, maximum_bytes, "selected POC HTTP response").await?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub(crate) async fn bounded_response_bytes(
    mut response: reqwest::Response,
    maximum_bytes: usize,
    label: &str,
) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum_bytes as u64)
    {
        bail!("{label} exceeds local admission limit");
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        let next_len = bytes
            .len()
            .checked_add(chunk.len())
            .context("HTTP response length overflow")?;
        if next_len > maximum_bytes {
            bail!("{label} exceeds local admission limit");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selected::{
        EncryptedTagBuildRecord, NullifierBuildRecord, PocLimits, UseCaseBuildInput,
        NULLIFIER_WITNESS_BYTES,
    };

    const KEY: [u8; 32] = [9; 32];

    fn input() -> UseCaseBuildInput {
        UseCaseBuildInput {
            generation_height: 7,
            generation_root_hex: hex::encode([3; 32]),
            nullifiers: (1..=100)
                .map(|value| {
                    let mut key = [0; 32];
                    key[0] = value;
                    NullifierBuildRecord {
                        nullifier_hex: hex::encode(key),
                        position: u64::from(value),
                        witness_base64: STANDARD.encode(vec![value; NULLIFIER_WITNESS_BYTES]),
                    }
                })
                .collect(),
            encrypted_tags: (0..100)
                .map(|value| EncryptedTagBuildRecord {
                    tag_base64: STANDARD.encode(format!("tag-{value}")),
                    encrypted_values_base64: vec![STANDARD.encode(format!("cipher-{value}"))],
                })
                .collect(),
            shinzo_bucket_count: 1 << 16,
            limits: PocLimits::default(),
        }
    }

    #[tokio::test]
    async fn endpoints_execute_all_three_selected_use_cases() {
        let left = Arc::new(UseCaseStore::build(input(), &KEY, 0).unwrap());
        let right = Arc::new(UseCaseStore::build(input(), &KEY, 1).unwrap());
        let left_server = spawn_selected(left, "127.0.0.1:0").await.unwrap();
        let right_server = spawn_selected(right, "127.0.0.1:0").await.unwrap();
        let urls = [
            format!("http://{}", left_server.address),
            format!("http://{}", right_server.address),
        ];
        let client = UseCaseClient::connect(&urls, &KEY).await.unwrap();

        let mut nullifier = [0; 32];
        nullifier[0] = 7;
        let strict = client
            .strict_lookup(TableUseCase::Nullifier, &nullifier)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(strict[0], vec![7; NULLIFIER_WITNESS_BYTES]);

        let candidates = (0..100)
            .map(|value| format!("tag-{value}").into_bytes())
            .collect::<Vec<_>>();
        let decoy = client
            .decoy_lookup(TableUseCase::EncryptedTag, b"tag-37", &candidates)
            .await
            .unwrap();
        assert_eq!(decoy.processed_rows, 1);
        assert_eq!(decoy.ignored_without_decoding, 99);
        assert_eq!(decoy.values.unwrap()[0], b"cipher-37");

        assert!(client.subscribe_and_evaluate(1234, 1234).await.unwrap());
    }
}
