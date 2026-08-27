//! RFC 9458 Oblivious HTTP transport for the selected PIR protocol.
//!
//! The client encrypts a Binary HTTP request to a fixed gateway.  A relay sees
//! the client network address and opaque bytes; the gateway sees the PIR share
//! and the relay address.  Query privacy still comes from Dense XOR or Compact
//! DPF, so a two-replica query uses two independently operated relay/gateway
//! paths.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bhttp::{Message, Mode, StatusCode as BhttpStatusCode};
use ohttp::hpke::{Aead, Kdf, Kem};
use ohttp::{ClientRequest, KeyConfig, Server, SymmetricSuite};
use rand::rngs::OsRng;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::task::{JoinHandle, JoinSet};

use crate::dense;
use crate::selected::{decode_table_answer, TableUseCase, UseCaseStore};
use crate::selected_http::{
    bounded_response_bytes, collect_indexed, DecoyAnswerResponse, DecoyClientResult,
    DecoyQueryRequest, PrivateAnswerResponse, PrivateQueryRequest, SelectedService,
    ShinzoEventRequest, ShinzoEventResponse, ShinzoNotification, ShinzoPollRequest,
    ShinzoPollResponse, ShinzoRegistrationRequest, ShinzoSubscription, UseCaseMetadata,
};
use crate::subscription::{combine_compact, compact_registration, NotificationShare};
use crate::verification::{decrypt_projection_values, verify_nullifier_witness};

const KEY_DOCUMENT_VERSION: u32 = 1;
const KEY_DOCUMENT_MAC_DOMAIN: &[u8] = b"defradb-pir-ohttp-key-document-v1";
const GATEWAY_AUTHORITY: &[u8] = b"pir-gateway.invalid";
const OHTTP_REQUEST_MEDIA_TYPE: &str = "message/ohttp-req";
const OHTTP_RESPONSE_MEDIA_TYPE: &str = "message/ohttp-res";
const JSON_MEDIA_TYPE: &[u8] = b"application/json";
const RESPONSE_PADDING_HEADER: &[u8] = b"x-defradb-response-padding";
const RESPONSE_PADDING_NONE: &[u8] = b"none";
const RESPONSE_PADDING_POWER_OF_TWO: &[u8] = b"power-of-two";
const MAX_OHTTP_PLAINTEXT_BYTES: usize = 512 * 1024 * 1024;
const MAX_OHTTP_WIRE_BYTES: usize = MAX_OHTTP_PLAINTEXT_BYTES + 1_024;
const MAX_OHTTP_KEY_DOCUMENT_BYTES: usize = 64 * 1024;
const DEFAULT_REPLAY_CAPACITY: usize = 16_384;

/// Network path used to reach an OHTTP relay.  OHTTP still protects the PIR
/// plaintext on both paths; Tor additionally prevents the relay from learning
/// the wallet's network address.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OriginTransportConfig {
    Direct,
    TorSocks5 { proxy_url: String },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OriginTransportKind {
    Direct,
    TorSocks5,
}

/// Minimal transport seam inspired by Ethereum's Abstract Access Layer.  PIR
/// and OHTTP do not need to know whether bytes travel directly or through Tor.
pub trait AnonymousHttpTransport: Send + Sync {
    fn kind(&self) -> OriginTransportKind;
    fn http(&self) -> &reqwest::Client;
}

struct ConfiguredTransport {
    kind: OriginTransportKind,
    http: reqwest::Client,
}

impl AnonymousHttpTransport for ConfiguredTransport {
    fn kind(&self) -> OriginTransportKind {
        self.kind
    }

    fn http(&self) -> &reqwest::Client {
        &self.http
    }
}

impl OriginTransportConfig {
    pub fn build(&self) -> Result<Arc<dyn AnonymousHttpTransport>> {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(120));
        let kind = match self {
            Self::Direct => OriginTransportKind::Direct,
            Self::TorSocks5 { proxy_url } => {
                // `socks5h` resolves relay hostnames through Tor.  Accepting
                // `socks5` here would silently leak DNS to the local network.
                if !proxy_url.starts_with("socks5h://") {
                    bail!("Tor transport requires a socks5h:// proxy URL");
                }
                builder = builder.proxy(reqwest::Proxy::all(proxy_url)?);
                OriginTransportKind::TorSocks5
            }
        };
        Ok(Arc::new(ConfiguredTransport {
            kind,
            http: builder.build()?,
        }))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum PaddingStrategy {
    None,
    PowerOfTwo {
        minimum_bytes: usize,
    },
    Fixed {
        request_bytes: usize,
        response_bytes: usize,
    },
}

impl PaddingStrategy {
    fn request_target(self, encoded_bytes: usize) -> Result<Option<usize>> {
        match self {
            Self::None => Ok(None),
            Self::PowerOfTwo { minimum_bytes } => {
                Ok(Some(power_of_two_target(encoded_bytes, minimum_bytes)?))
            }
            Self::Fixed { request_bytes, .. } => Ok(Some(request_bytes)),
        }
    }

    fn response_instruction(self) -> String {
        match self {
            Self::None => String::from_utf8_lossy(RESPONSE_PADDING_NONE).into_owned(),
            Self::PowerOfTwo { minimum_bytes } => format!(
                "{}:{minimum_bytes}",
                String::from_utf8_lossy(RESPONSE_PADDING_POWER_OF_TWO)
            ),
            Self::Fixed { response_bytes, .. } => response_bytes.to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OhttpKeyDocumentBody {
    pub format_version: u32,
    pub replica_id: String,
    pub generation_body_digest_hex: String,
    pub key_id: u8,
    pub encoded_config_base64: String,
    pub kem: String,
    pub kdf: String,
    pub aead: String,
    pub maximum_plaintext_bytes: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthenticatedOhttpKeyDocument {
    pub document: OhttpKeyDocumentBody,
    pub mac_hex: String,
}

impl AuthenticatedOhttpKeyDocument {
    pub fn verify(&self, operator_key: &[u8; 32]) -> Result<Vec<u8>> {
        if self.document.format_version != KEY_DOCUMENT_VERSION
            || self.document.maximum_plaintext_bytes != MAX_OHTTP_PLAINTEXT_BYTES
            || self.document.kem != "DHKEM(X25519, HKDF-SHA256)"
            || self.document.kdf != "HKDF-SHA256"
            || self.document.aead != "AES-128-GCM"
        {
            bail!("unsupported OHTTP key document");
        }
        if self.document.generation_body_digest_hex.len() != 64 {
            bail!("OHTTP key document has an invalid generation digest");
        }
        let supplied: [u8; 32] = hex::decode(&self.mac_hex)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("OHTTP key document MAC must be 32 bytes"))?;
        if supplied != key_document_mac(operator_key, &self.document)? {
            bail!("OHTTP key document authentication failed");
        }
        let config = STANDARD.decode(&self.document.encoded_config_base64)?;
        if config.first().copied() != Some(self.document.key_id) {
            bail!("OHTTP key document key ID does not match its encoded config");
        }
        ClientRequest::from_encoded_config(&config)
            .context("OHTTP key document contains an unusable HPKE config")?;
        Ok(config)
    }
}

fn key_document_mac(operator_key: &[u8; 32], document: &OhttpKeyDocumentBody) -> Result<[u8; 32]> {
    let encoded = serde_json::to_vec(document)?;
    let mut hasher = blake3::Hasher::new_keyed(operator_key);
    hasher.update(KEY_DOCUMENT_MAC_DOMAIN);
    hasher.update(&(encoded.len() as u64).to_le_bytes());
    hasher.update(&encoded);
    Ok(*hasher.finalize().as_bytes())
}

#[derive(Debug)]
struct ReplayWindow {
    capacity: usize,
    order: VecDeque<[u8; 32]>,
    seen: HashSet<[u8; 32]>,
}

impl ReplayWindow {
    fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            bail!("OHTTP replay window capacity must be non-zero");
        }
        Ok(Self {
            capacity,
            order: VecDeque::with_capacity(capacity),
            seen: HashSet::with_capacity(capacity),
        })
    }

    fn admit(&mut self, encrypted_request: &[u8]) -> Result<()> {
        let digest = *blake3::hash(encrypted_request).as_bytes();
        if !self.seen.insert(digest) {
            bail!("replayed OHTTP request rejected");
        }
        self.order.push_back(digest);
        if self.order.len() > self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.seen.remove(&expired);
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct OhttpGateway {
    service: SelectedService,
    operator_key: [u8; 32],
    replica_id: String,
    current_key_id: u8,
    servers: BTreeMap<u8, Server>,
    replay: Arc<Mutex<ReplayWindow>>,
}

impl OhttpGateway {
    pub fn new(
        store: Arc<UseCaseStore>,
        operator_key: &[u8; 32],
        replica_id: impl Into<String>,
        key_id: u8,
    ) -> Result<Self> {
        let service = SelectedService::new(store)?;
        let server = new_ohttp_server(key_id)?;
        Ok(Self {
            service,
            operator_key: *operator_key,
            replica_id: replica_id.into(),
            current_key_id: key_id,
            servers: BTreeMap::from([(key_id, server)]),
            replay: Arc::new(Mutex::new(ReplayWindow::new(DEFAULT_REPLAY_CAPACITY)?)),
        })
    }

    /// Installs a fresh current key while retaining the requested number of
    /// older receive keys for in-flight clients.
    pub fn rotate(&mut self, new_key_id: u8, retain_previous: usize) -> Result<()> {
        if self.servers.contains_key(&new_key_id) {
            bail!("OHTTP rotation requires a fresh key ID");
        }
        self.servers
            .insert(new_key_id, new_ohttp_server(new_key_id)?);
        self.current_key_id = new_key_id;
        while self.servers.len() > retain_previous.saturating_add(1) {
            let expired = self
                .servers
                .keys()
                .copied()
                .find(|key_id| *key_id != self.current_key_id)
                .context("OHTTP key ring has no removable previous key")?;
            self.servers.remove(&expired);
        }
        Ok(())
    }

    pub fn key_document(&self) -> Result<AuthenticatedOhttpKeyDocument> {
        let server = self
            .servers
            .get(&self.current_key_id)
            .context("current OHTTP key is missing from the receive ring")?;
        let document = OhttpKeyDocumentBody {
            format_version: KEY_DOCUMENT_VERSION,
            replica_id: self.replica_id.clone(),
            generation_body_digest_hex: self.service.body_digest_hex(),
            key_id: self.current_key_id,
            encoded_config_base64: STANDARD.encode(server.config().encode()?),
            kem: "DHKEM(X25519, HKDF-SHA256)".to_owned(),
            kdf: "HKDF-SHA256".to_owned(),
            aead: "AES-128-GCM".to_owned(),
            maximum_plaintext_bytes: MAX_OHTTP_PLAINTEXT_BYTES,
        };
        Ok(AuthenticatedOhttpKeyDocument {
            mac_hex: hex::encode(key_document_mac(&self.operator_key, &document)?),
            document,
        })
    }

    async fn handle(&self, encrypted_request: &[u8]) -> Result<Vec<u8>> {
        let key_id = encrypted_request
            .first()
            .copied()
            .context("empty OHTTP request")?;
        let server = self
            .servers
            .get(&key_id)
            .context("unknown or expired OHTTP key ID")?;
        let (plaintext, response_context) = server.decapsulate(encrypted_request)?;
        self.replay
            .lock()
            .map_err(|_| anyhow::anyhow!("OHTTP replay lock poisoned"))?
            .admit(encrypted_request)?;
        let decoded = decode_request(&plaintext)?;
        let (status, body) = self
            .service
            .dispatch_json(&decoded.method, &decoded.path, &decoded.body)
            .await;
        let response = encode_response(status, &body, decoded.response_padding)?;
        Ok(response_context.encapsulate(&response)?)
    }
}

fn new_ohttp_server(key_id: u8) -> Result<Server> {
    let config = KeyConfig::new(
        key_id,
        Kem::X25519Sha256,
        vec![SymmetricSuite::new(Kdf::HkdfSha256, Aead::Aes128Gcm)],
    )?;
    Ok(Server::new(config)?)
}

#[derive(Clone, Copy, Debug)]
enum ResponsePadding {
    None,
    PowerOfTwo { minimum_bytes: usize },
    Fixed { bytes: usize },
}

struct DecodedRequest {
    method: String,
    path: String,
    body: Vec<u8>,
    response_padding: ResponsePadding,
}

fn encode_request(
    method: &str,
    path: &str,
    body: &[u8],
    padding: PaddingStrategy,
) -> Result<Vec<u8>> {
    if !is_admitted_route(method, path) {
        bail!("OHTTP client target is not an admitted PIR route");
    }
    let mut message = Message::request(
        method.as_bytes().to_vec(),
        b"https".to_vec(),
        GATEWAY_AUTHORITY.to_vec(),
        path.as_bytes().to_vec(),
    );
    message.put_header(b"content-type".to_vec(), JSON_MEDIA_TYPE.to_vec());
    message.put_header(
        RESPONSE_PADDING_HEADER.to_vec(),
        padding.response_instruction().into_bytes(),
    );
    message.write_content(body);
    let mut encoded = Vec::new();
    message.write_bhttp(Mode::KnownLength, &mut encoded)?;
    let target = padding.request_target(encoded.len())?;
    pad_binary_http(&mut encoded, target)?;
    Ok(encoded)
}

fn decode_request(encoded: &[u8]) -> Result<DecodedRequest> {
    if encoded.len() > MAX_OHTTP_PLAINTEXT_BYTES {
        bail!("OHTTP request plaintext exceeds the admission limit");
    }
    let (message, consumed) = read_binary_http(encoded)?;
    require_zero_padding(encoded, consumed)?;
    if !message.informational().is_empty() || !message.trailer().is_empty() {
        bail!("OHTTP request cannot carry informational responses or trailers");
    }
    let method = std::str::from_utf8(
        message
            .control()
            .method()
            .context("OHTTP plaintext is not a request")?,
    )?
    .to_owned();
    let scheme = message
        .control()
        .scheme()
        .context("OHTTP request has no scheme")?;
    let authority = message
        .control()
        .authority()
        .context("OHTTP request has no authority")?;
    let path = std::str::from_utf8(
        message
            .control()
            .path()
            .context("OHTTP request has no path")?,
    )?
    .to_owned();
    if scheme != b"https" || authority != GATEWAY_AUTHORITY || !is_admitted_route(&method, &path) {
        bail!("OHTTP request target is not the fixed PIR gateway");
    }
    for field in message.header().fields() {
        if field.name() != b"content-type" && field.name() != RESPONSE_PADDING_HEADER {
            bail!("OHTTP request contains a non-admitted header");
        }
    }
    if message.header().get(b"content-type") != Some(JSON_MEDIA_TYPE) {
        bail!("OHTTP PIR request must contain JSON");
    }
    let response_padding = parse_response_padding(
        message
            .header()
            .get(RESPONSE_PADDING_HEADER)
            .context("OHTTP request has no response padding instruction")?,
    )?;
    Ok(DecodedRequest {
        method,
        path,
        body: message.content().to_vec(),
        response_padding,
    })
}

fn parse_response_padding(value: &[u8]) -> Result<ResponsePadding> {
    if value == RESPONSE_PADDING_NONE {
        return Ok(ResponsePadding::None);
    }
    let value = std::str::from_utf8(value)?;
    if let Some(minimum) = value.strip_prefix("power-of-two:") {
        return Ok(ResponsePadding::PowerOfTwo {
            minimum_bytes: minimum.parse()?,
        });
    }
    Ok(ResponsePadding::Fixed {
        bytes: value.parse()?,
    })
}

fn encode_response(status: StatusCode, body: &[u8], padding: ResponsePadding) -> Result<Vec<u8>> {
    let mut message = Message::response(BhttpStatusCode::try_from(status.as_u16())?);
    message.put_header(b"content-type".to_vec(), JSON_MEDIA_TYPE.to_vec());
    message.write_content(body);
    let mut encoded = Vec::new();
    message.write_bhttp(Mode::KnownLength, &mut encoded)?;
    let target = match padding {
        ResponsePadding::None => None,
        ResponsePadding::PowerOfTwo { minimum_bytes } => {
            Some(power_of_two_target(encoded.len(), minimum_bytes)?)
        }
        ResponsePadding::Fixed { bytes } => Some(bytes),
    };
    pad_binary_http(&mut encoded, target)?;
    Ok(encoded)
}

fn decode_response(encoded: &[u8]) -> Result<(StatusCode, Vec<u8>)> {
    if encoded.len() > MAX_OHTTP_PLAINTEXT_BYTES {
        bail!("OHTTP response plaintext exceeds the admission limit");
    }
    let (message, consumed) = read_binary_http(encoded)?;
    require_zero_padding(encoded, consumed)?;
    if !message.informational().is_empty() || !message.trailer().is_empty() {
        bail!("OHTTP response cannot carry informational responses or trailers");
    }
    for field in message.header().fields() {
        if field.name() != b"content-type" {
            bail!("OHTTP response contains a non-admitted header");
        }
    }
    let code = message
        .control()
        .status()
        .context("OHTTP response plaintext is not a response")?
        .code();
    Ok((StatusCode::from_u16(code)?, message.content().to_vec()))
}

fn read_binary_http(encoded: &[u8]) -> Result<(Message, usize)> {
    let mut cursor = Cursor::new(encoded);
    let message = Message::read_bhttp(&mut cursor)?;
    Ok((message, usize::try_from(cursor.position())?))
}

fn require_zero_padding(encoded: &[u8], consumed: usize) -> Result<()> {
    if encoded[consumed..].iter().any(|byte| *byte != 0) {
        bail!("Binary HTTP padding contains non-zero bytes");
    }
    Ok(())
}

fn power_of_two_target(encoded_bytes: usize, minimum_bytes: usize) -> Result<usize> {
    encoded_bytes
        .max(minimum_bytes)
        .checked_next_power_of_two()
        .context("OHTTP padding target overflow")
}

fn pad_binary_http(encoded: &mut Vec<u8>, target: Option<usize>) -> Result<()> {
    let target = target.unwrap_or(encoded.len());
    if target < encoded.len() {
        bail!("OHTTP fixed padding bucket is too small");
    }
    if target > MAX_OHTTP_PLAINTEXT_BYTES {
        bail!("OHTTP padding target exceeds the admission limit");
    }
    encoded.resize(target, 0);
    Ok(())
}

fn is_admitted_route(method: &str, path: &str) -> bool {
    matches!(
        (method, path),
        ("GET", "/v1/manifest")
            | ("POST", "/v1/nullifier/private")
            | ("POST", "/v1/nullifier/decoy")
            | ("POST", "/v1/tag/private")
            | ("POST", "/v1/tag/decoy")
            | ("POST", "/v1/shinzo/register")
            | ("POST", "/v1/shinzo/event")
            | ("POST", "/v1/shinzo/poll")
    )
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct RelayMetricsSnapshot {
    pub forwarded_requests: u64,
    pub encrypted_request_bytes: u64,
    pub encrypted_response_bytes: u64,
}

#[derive(Default)]
struct RelayMetricsInner {
    forwarded_requests: AtomicU64,
    encrypted_request_bytes: AtomicU64,
    encrypted_response_bytes: AtomicU64,
}

#[derive(Clone, Default)]
pub struct RelayMetrics(Arc<RelayMetricsInner>);

impl RelayMetrics {
    pub fn snapshot(&self) -> RelayMetricsSnapshot {
        RelayMetricsSnapshot {
            forwarded_requests: self.0.forwarded_requests.load(Ordering::Relaxed),
            encrypted_request_bytes: self.0.encrypted_request_bytes.load(Ordering::Relaxed),
            encrypted_response_bytes: self.0.encrypted_response_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
struct RelayState {
    http: reqwest::Client,
    gateway_url: String,
    metrics: RelayMetrics,
}

pub struct RunningOhttpServer {
    pub address: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for RunningOhttpServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct RunningOhttpReplica {
    pub gateway: RunningOhttpServer,
    pub relay: RunningOhttpServer,
    pub metrics: RelayMetrics,
}

impl RunningOhttpReplica {
    pub fn relay_url(&self) -> String {
        format!("http://{}", self.relay.address)
    }
}

pub async fn spawn_ohttp_replica(
    store: Arc<UseCaseStore>,
    operator_key: &[u8; 32],
    replica_id: impl Into<String>,
    key_id: u8,
) -> Result<RunningOhttpReplica> {
    spawn_ohttp_replica_on(
        store,
        operator_key,
        replica_id,
        key_id,
        "127.0.0.1:0",
        "127.0.0.1:0",
    )
    .await
}

pub async fn spawn_ohttp_replica_on(
    store: Arc<UseCaseStore>,
    operator_key: &[u8; 32],
    replica_id: impl Into<String>,
    key_id: u8,
    gateway_bind: &str,
    relay_bind: &str,
) -> Result<RunningOhttpReplica> {
    let gateway = OhttpGateway::new(store, operator_key, replica_id, key_id)?;
    let gateway_server = spawn_gateway(gateway, gateway_bind).await?;
    let gateway_url = format!("http://{}", gateway_server.address);
    let metrics = RelayMetrics::default();
    let relay = spawn_relay(&gateway_url, relay_bind, metrics.clone()).await?;
    Ok(RunningOhttpReplica {
        gateway: gateway_server,
        relay,
        metrics,
    })
}

pub async fn spawn_gateway(gateway: OhttpGateway, bind: &str) -> Result<RunningOhttpServer> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let router = Router::new()
        .route("/.well-known/ohttp-gateway", get(get_ohttp_keys))
        .route("/ohttp", post(post_ohttp_gateway))
        .layer(DefaultBodyLimit::max(MAX_OHTTP_WIRE_BYTES))
        .with_state(gateway);
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router).await {
            eprintln!("OHTTP gateway stopped: {error}");
        }
    });
    Ok(RunningOhttpServer { address, task })
}

pub async fn spawn_relay(
    gateway_url: &str,
    bind: &str,
    metrics: RelayMetrics,
) -> Result<RunningOhttpServer> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let state = RelayState {
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?,
        gateway_url: gateway_url.trim_end_matches('/').to_owned(),
        metrics,
    };
    let router = Router::new()
        .route("/.well-known/ohttp-gateway", get(relay_ohttp_keys))
        .route("/relay", post(post_ohttp_relay))
        .layer(DefaultBodyLimit::max(MAX_OHTTP_WIRE_BYTES))
        .with_state(state);
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router).await {
            eprintln!("OHTTP relay stopped: {error}");
        }
    });
    Ok(RunningOhttpServer { address, task })
}

async fn get_ohttp_keys(
    State(gateway): State<OhttpGateway>,
) -> Result<Json<AuthenticatedOhttpKeyDocument>, (StatusCode, String)> {
    gateway
        .key_document()
        .map(Json)
        .map_err(internal_http_error)
}

async fn post_ohttp_gateway(
    State(gateway): State<OhttpGateway>,
    body: Bytes,
) -> Result<([(&'static str, &'static str); 1], Vec<u8>), (StatusCode, String)> {
    let response = gateway
        .handle(&body)
        .await
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    Ok((
        [(header::CONTENT_TYPE.as_str(), OHTTP_RESPONSE_MEDIA_TYPE)],
        response,
    ))
}

async fn relay_ohttp_keys(
    State(relay): State<RelayState>,
) -> Result<(StatusCode, [(&'static str, &'static str); 1], Vec<u8>), (StatusCode, String)> {
    let response = relay
        .http
        .get(format!("{}/.well-known/ohttp-gateway", relay.gateway_url))
        .send()
        .await
        .map_err(bad_gateway)?;
    let status = response.status();
    let body = bounded_response_bytes(response, MAX_OHTTP_KEY_DOCUMENT_BYTES, "OHTTP key document")
        .await
        .map_err(bad_gateway_error)?;
    Ok((
        status,
        [(header::CONTENT_TYPE.as_str(), "application/json")],
        body,
    ))
}

async fn post_ohttp_relay(
    State(relay): State<RelayState>,
    body: Bytes,
) -> Result<(StatusCode, [(&'static str, &'static str); 1], Vec<u8>), (StatusCode, String)> {
    relay
        .metrics
        .0
        .forwarded_requests
        .fetch_add(1, Ordering::Relaxed);
    relay
        .metrics
        .0
        .encrypted_request_bytes
        .fetch_add(body.len() as u64, Ordering::Relaxed);
    let response = relay
        .http
        .post(format!("{}/ohttp", relay.gateway_url))
        .header(header::CONTENT_TYPE, OHTTP_REQUEST_MEDIA_TYPE)
        .body(body)
        .send()
        .await
        .map_err(bad_gateway)?;
    let status = response.status();
    let response_body =
        bounded_response_bytes(response, MAX_OHTTP_WIRE_BYTES, "OHTTP gateway response")
            .await
            .map_err(bad_gateway_error)?;
    relay
        .metrics
        .0
        .encrypted_response_bytes
        .fetch_add(response_body.len() as u64, Ordering::Relaxed);
    Ok((
        status,
        [(header::CONTENT_TYPE.as_str(), OHTTP_RESPONSE_MEDIA_TYPE)],
        response_body,
    ))
}

fn internal_http_error(error: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn bad_gateway(error: reqwest::Error) -> (StatusCode, String) {
    (StatusCode::BAD_GATEWAY, error.to_string())
}

fn bad_gateway_error(error: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::BAD_GATEWAY, error.to_string())
}

#[derive(Clone, Debug, Serialize)]
pub struct OhttpExchangeMetrics {
    pub binary_http_request_bytes: usize,
    pub encrypted_request_bytes: usize,
    pub encrypted_response_bytes: usize,
    pub binary_http_response_bytes: usize,
    pub client_encapsulate_ms: f64,
    pub relay_round_trip_ms: f64,
    pub client_decapsulate_ms: f64,
}

#[derive(Clone, Debug)]
pub struct OhttpExchange<T> {
    pub value: T,
    pub metrics: OhttpExchangeMetrics,
}

#[derive(Clone)]
pub struct OhttpClient {
    transport: Arc<dyn AnonymousHttpTransport>,
    relay_url: String,
    config: Arc<[u8]>,
    pub key_document: AuthenticatedOhttpKeyDocument,
    padding: PaddingStrategy,
}

impl OhttpClient {
    pub async fn connect(
        relay_url: &str,
        operator_key: &[u8; 32],
        padding: PaddingStrategy,
    ) -> Result<Self> {
        Self::connect_with_transport_config(
            relay_url,
            operator_key,
            padding,
            &OriginTransportConfig::Direct,
        )
        .await
    }

    pub async fn connect_with_transport_config(
        relay_url: &str,
        operator_key: &[u8; 32],
        padding: PaddingStrategy,
        transport: &OriginTransportConfig,
    ) -> Result<Self> {
        Self::connect_with_transport(relay_url, operator_key, padding, transport.build()?).await
    }

    pub async fn connect_with_transport(
        relay_url: &str,
        operator_key: &[u8; 32],
        padding: PaddingStrategy,
        transport: Arc<dyn AnonymousHttpTransport>,
    ) -> Result<Self> {
        let relay_url = relay_url.trim_end_matches('/').to_owned();
        let response = transport
            .http()
            .get(format!("{relay_url}/.well-known/ohttp-gateway"))
            .send()
            .await?
            .error_for_status()?;
        let key_document: AuthenticatedOhttpKeyDocument = serde_json::from_slice(
            &bounded_response_bytes(response, MAX_OHTTP_KEY_DOCUMENT_BYTES, "OHTTP key document")
                .await?,
        )?;
        let config = key_document.verify(operator_key)?;
        Ok(Self {
            transport,
            relay_url,
            config: config.into(),
            key_document,
            padding,
        })
    }

    pub fn transport_kind(&self) -> OriginTransportKind {
        self.transport.kind()
    }

    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<OhttpExchange<T>> {
        let exchange = self.exchange("GET", path, &[]).await?;
        Ok(OhttpExchange {
            value: serde_json::from_slice(&exchange.value)?,
            metrics: exchange.metrics,
        })
    }

    pub async fn post_json<I: Serialize, O: DeserializeOwned>(
        &self,
        path: &str,
        input: &I,
    ) -> Result<OhttpExchange<O>> {
        let body = serde_json::to_vec(input)?;
        let exchange = self.exchange("POST", path, &body).await?;
        Ok(OhttpExchange {
            value: serde_json::from_slice(&exchange.value)?,
            metrics: exchange.metrics,
        })
    }

    pub async fn post_empty<I: Serialize>(
        &self,
        path: &str,
        input: &I,
    ) -> Result<OhttpExchange<()>> {
        let body = serde_json::to_vec(input)?;
        let exchange = self.exchange("POST", path, &body).await?;
        if !exchange.value.is_empty() {
            bail!("OHTTP no-content response unexpectedly contained a body");
        }
        Ok(OhttpExchange {
            value: (),
            metrics: exchange.metrics,
        })
    }

    async fn exchange(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<OhttpExchange<Vec<u8>>> {
        let binary_request = encode_request(method, path, body, self.padding)?;
        let encapsulate_started = Instant::now();
        let request = ClientRequest::from_encoded_config(&self.config)?;
        let (encrypted_request, response_context) = request.encapsulate(&binary_request)?;
        let client_encapsulate_ms = elapsed_ms(encapsulate_started.elapsed());
        let encrypted_request_bytes = encrypted_request.len();
        let relay_started = Instant::now();
        let response = self
            .transport
            .http()
            .post(format!("{}/relay", self.relay_url))
            .header(header::CONTENT_TYPE, OHTTP_REQUEST_MEDIA_TYPE)
            .body(encrypted_request)
            .send()
            .await?
            .error_for_status()?;
        if response.headers().get(header::CONTENT_TYPE)
            != Some(&HeaderValue::from_static(OHTTP_RESPONSE_MEDIA_TYPE))
        {
            bail!("OHTTP relay returned an unexpected media type");
        }
        let encrypted_response =
            bounded_response_bytes(response, MAX_OHTTP_WIRE_BYTES, "OHTTP relay response").await?;
        let relay_round_trip_ms = elapsed_ms(relay_started.elapsed());
        let encrypted_response_bytes = encrypted_response.len();
        let decapsulate_started = Instant::now();
        let binary_response = response_context.decapsulate(&encrypted_response)?;
        let client_decapsulate_ms = elapsed_ms(decapsulate_started.elapsed());
        let binary_http_response_bytes = binary_response.len();
        let (status, response_body) = decode_response(&binary_response)?;
        if !status.is_success() {
            let message = serde_json::from_slice::<String>(&response_body)
                .unwrap_or_else(|_| String::from_utf8_lossy(&response_body).into_owned());
            bail!("OHTTP gateway returned {status}: {message}");
        }
        Ok(OhttpExchange {
            value: response_body,
            metrics: OhttpExchangeMetrics {
                binary_http_request_bytes: binary_request.len(),
                encrypted_request_bytes,
                encrypted_response_bytes,
                binary_http_response_bytes,
                client_encapsulate_ms,
                relay_round_trip_ms,
                client_decapsulate_ms,
            },
        })
    }
}

pub struct OhttpUseCaseClient {
    replicas: Arc<[OhttpClient]>,
    pub metadata: UseCaseMetadata,
}

impl OhttpUseCaseClient {
    pub async fn connect<S: AsRef<str>>(
        relay_urls: &[S],
        operator_key: &[u8; 32],
        padding: PaddingStrategy,
    ) -> Result<Self> {
        let transports = vec![OriginTransportConfig::Direct; relay_urls.len()];
        Self::connect_with_transport_configs(relay_urls, operator_key, padding, &transports).await
    }

    pub async fn connect_with_transport_configs<S: AsRef<str>>(
        relay_urls: &[S],
        operator_key: &[u8; 32],
        padding: PaddingStrategy,
        transports: &[OriginTransportConfig],
    ) -> Result<Self> {
        if relay_urls.len() < 2 {
            bail!("strict OHTTP PIR requires at least two replica paths");
        }
        if transports.len() != relay_urls.len() {
            bail!("OHTTP requires exactly one origin transport per replica path");
        }
        let mut tasks = JoinSet::new();
        for (index, (relay, transport)) in relay_urls.iter().zip(transports).enumerate() {
            let relay = relay.as_ref().to_owned();
            let transport = transport.clone();
            let key = *operator_key;
            tasks.spawn(async move {
                let client =
                    OhttpClient::connect_with_transport_config(&relay, &key, padding, &transport)
                        .await?;
                let metadata = client.get_json::<UseCaseMetadata>("/v1/manifest").await?;
                Ok::<_, anyhow::Error>((index, client, metadata.value))
            });
        }
        let mut connected = (0..relay_urls.len()).map(|_| None).collect::<Vec<_>>();
        while let Some(result) = tasks.join_next().await {
            let (index, client, metadata) = result.context("OHTTP connect task failed")??;
            connected[index] = Some((client, metadata));
        }
        let connected = connected
            .into_iter()
            .map(|value| value.context("OHTTP replica did not connect"))
            .collect::<Result<Vec<_>>>()?;
        let expected_metadata = connected[0].1.clone();
        expected_metadata.validate(operator_key)?;
        for (client, candidate_metadata) in &connected {
            candidate_metadata.validate(operator_key)?;
            if candidate_metadata != &expected_metadata
                || client.key_document.document.generation_body_digest_hex
                    != hex::encode(expected_metadata.manifest.manifest.body_digest)
            {
                bail!("OHTTP replicas advertise different PIR generations");
            }
        }
        Ok(Self {
            replicas: connected
                .into_iter()
                .map(|(client, _)| client)
                .collect::<Vec<_>>()
                .into(),
            metadata: expected_metadata,
        })
    }

    pub async fn strict_lookup(
        &self,
        use_case: TableUseCase,
        key: &[u8],
    ) -> Result<Option<Vec<Vec<u8>>>> {
        let (manifest, directory, route) = self.metadata.table_parts(use_case, false);
        let (ordinal, _) = directory.ordinal(key);
        let shares =
            dense::query_shares(ordinal, manifest.row_count, self.replicas.len(), &mut OsRng)?;
        let digest = hex::encode(self.metadata.manifest.manifest.body_digest);
        let mut tasks = JoinSet::new();
        for (index, (replica, share)) in self.replicas.iter().zip(shares).enumerate() {
            let replica = replica.clone();
            let request = PrivateQueryRequest {
                body_digest_hex: digest.clone(),
                query_shares: vec![STANDARD.encode(share)],
            };
            tasks.spawn(async move {
                let response = replica
                    .post_json::<_, PrivateAnswerResponse>(route, &request)
                    .await?;
                Ok::<_, anyhow::Error>((index, response.value))
            });
        }
        let responses = collect_indexed(&mut tasks, self.replicas.len()).await?;
        let mut answer_shares = Vec::with_capacity(responses.len());
        for response in responses {
            if response.body_digest_hex != digest || response.answer_shares.len() != 1 {
                bail!("OHTTP replica returned a mismatched private answer");
            }
            answer_shares.push(STANDARD.decode(&response.answer_shares[0])?);
        }
        let row = dense::combine(&answer_shares)?;
        decode_table_answer(manifest, &row, key)
    }

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

    pub async fn decoy_lookup(
        &self,
        use_case: TableUseCase,
        target: &[u8],
        candidates: &[Vec<u8>],
    ) -> Result<DecoyClientResult> {
        let target_indices = candidates
            .iter()
            .enumerate()
            .filter_map(|(index, value)| (value.as_slice() == target).then_some(index))
            .collect::<Vec<_>>();
        if target_indices.len() != 1 {
            bail!("decoy set must contain the target exactly once");
        }
        let target_index = target_indices[0];
        let (manifest, _, route) = self.metadata.table_parts(use_case, true);
        let digest = hex::encode(self.metadata.manifest.manifest.body_digest);
        let response = self.replicas[0]
            .post_json::<_, DecoyAnswerResponse>(
                route,
                &DecoyQueryRequest {
                    body_digest_hex: digest.clone(),
                    candidate_keys: candidates
                        .iter()
                        .map(|candidate| STANDARD.encode(candidate))
                        .collect(),
                },
            )
            .await?
            .value;
        if response.body_digest_hex != digest || response.rows.len() != candidates.len() {
            bail!("OHTTP replica returned a mismatched decoy answer");
        }
        let row = STANDARD.decode(&response.rows[target_index])?;
        Ok(DecoyClientResult {
            values: decode_table_answer(manifest, &row, target)?,
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
        if self.replicas.len() != 2 {
            bail!("Compact DPF subscriptions require exactly two OHTTP replica paths");
        }
        let registration = compact_registration(
            target_bucket,
            self.metadata.manifest.manifest.shinzo_bucket_count,
            &mut OsRng,
        )?;
        let digest = hex::encode(self.metadata.manifest.manifest.body_digest);
        for (replica, key) in self.replicas.iter().zip(&registration.server_keys) {
            replica
                .post_empty(
                    "/v1/shinzo/register",
                    &ShinzoRegistrationRequest {
                        body_digest_hex: digest.clone(),
                        subscription_id_hex: registration.id.to_string(),
                        server_key_base64: STANDARD.encode(key),
                    },
                )
                .await?;
        }
        let mut shares = Vec::with_capacity(2);
        for replica in self.replicas.iter() {
            let response = replica
                .post_json::<_, ShinzoEventResponse>(
                    "/v1/shinzo/event",
                    &ShinzoEventRequest {
                        body_digest_hex: digest.clone(),
                        subscription_id_hex: registration.id.to_string(),
                        event_bucket,
                    },
                )
                .await?
                .value;
            if response.body_digest_hex != digest
                || response.subscription_id_hex != registration.id.to_string()
            {
                bail!("OHTTP Compact DPF response does not match its registration");
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

    pub async fn register_shinzo_subscription(
        &self,
        target_bucket: usize,
    ) -> Result<ShinzoSubscription> {
        if self.replicas.len() != 2 {
            bail!("Compact DPF subscriptions require exactly two OHTTP replica paths");
        }
        let registration = compact_registration(
            target_bucket,
            self.metadata.manifest.manifest.shinzo_bucket_count,
            &mut OsRng,
        )?;
        let digest = hex::encode(self.metadata.manifest.manifest.body_digest);
        for (replica, key) in self.replicas.iter().zip(&registration.server_keys) {
            replica
                .post_empty(
                    "/v1/shinzo/register",
                    &ShinzoRegistrationRequest {
                        body_digest_hex: digest.clone(),
                        subscription_id_hex: registration.id.to_string(),
                        server_key_base64: STANDARD.encode(key),
                    },
                )
                .await?;
        }
        Ok(ShinzoSubscription {
            id: registration.id,
            cursor: 0,
        })
    }

    pub async fn poll_shinzo_subscription(
        &self,
        subscription: &mut ShinzoSubscription,
        limit: usize,
    ) -> Result<Vec<ShinzoNotification>> {
        if self.replicas.len() != 2 {
            bail!("Compact DPF subscriptions require exactly two OHTTP replica paths");
        }
        let digest = hex::encode(self.metadata.manifest.manifest.body_digest);
        let request = ShinzoPollRequest {
            body_digest_hex: digest.clone(),
            subscription_id_hex: subscription.id.to_string(),
            after_cursor: subscription.cursor,
            limit,
        };
        let mut responses = Vec::with_capacity(2);
        for replica in self.replicas.iter() {
            let response = replica
                .post_json::<_, ShinzoPollResponse>("/v1/shinzo/poll", &request)
                .await?
                .value;
            if response.body_digest_hex != digest
                || response.subscription_id_hex != subscription.id.to_string()
            {
                bail!("OHTTP Compact DPF replica returned a mismatched mailbox");
            }
            if response.gap {
                bail!("OHTTP Compact DPF mailbox history was truncated before polling");
            }
            responses.push(response);
        }
        if responses[0].party_index == responses[1].party_index {
            bail!("OHTTP Compact DPF mailboxes came from the same server party");
        }
        let common = responses[0].entries.len().min(responses[1].entries.len());
        let mut notifications = Vec::with_capacity(common);
        for index in 0..common {
            let left = &responses[0].entries[index];
            let right = &responses[1].entries[index];
            if left.cursor != right.cursor || left.event_id != right.event_id {
                bail!("OHTTP Compact DPF replicas returned divergent event streams");
            }
            let shares = [left, right]
                .iter()
                .zip(&responses)
                .map(|(entry, response)| {
                    let value: [u8; 16] = STANDARD
                        .decode(&entry.value_base64)?
                        .try_into()
                        .map_err(|_| {
                            anyhow::anyhow!("Compact DPF mailbox share has the wrong length")
                        })?;
                    NotificationShare::from_wire(subscription.id, response.party_index, value)
                })
                .collect::<Result<Vec<_>>>()?;
            notifications.push(ShinzoNotification {
                cursor: left.cursor,
                event_id: left.event_id.clone(),
                matched: combine_compact(&shares)?,
            });
        }
        if let Some(last) = notifications.last() {
            subscription.cursor = last.cursor;
        }
        Ok(notifications)
    }
}

fn elapsed_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[derive(Clone, Debug)]
pub(crate) struct OhttpCryptoObservation {
    pub binary_http_request_bytes: usize,
    pub encrypted_request_bytes: usize,
    pub binary_http_response_bytes: usize,
    pub encrypted_response_bytes: usize,
    pub client_encode_and_encrypt: Duration,
    pub gateway_decrypt_and_decode: Duration,
    pub gateway_encode_and_encrypt: Duration,
    pub client_decrypt_and_decode: Duration,
}

/// Exercises the actual RFC 9458 HPKE and Binary HTTP implementation without
/// loopback TCP noise.  PIR evaluation is intentionally excluded: this
/// isolates the additional client and gateway cost of origin hiding.
pub(crate) fn measure_ohttp_crypto(
    request_body_bytes: usize,
    response_body_bytes: usize,
    padding: PaddingStrategy,
    samples: usize,
) -> Result<Vec<OhttpCryptoObservation>> {
    if samples == 0 {
        bail!("OHTTP benchmark requires at least one sample");
    }
    let server = new_ohttp_server(97)?;
    let config = server.config().encode()?;
    let request_body = vec![0x5a; request_body_bytes];
    let response_body = vec![0xa5; response_body_bytes];
    let mut observations = Vec::with_capacity(samples);
    for _ in 0..samples {
        let client_started = Instant::now();
        let binary_request = encode_request("POST", "/v1/tag/private", &request_body, padding)?;
        let request = ClientRequest::from_encoded_config(&config)?;
        let (encrypted_request, client_response) = request.encapsulate(&binary_request)?;
        let client_encode_and_encrypt = client_started.elapsed();

        let gateway_decode_started = Instant::now();
        let (decoded_request, server_response) = server.decapsulate(&encrypted_request)?;
        let request = decode_request(&decoded_request)?;
        if request.body != request_body {
            bail!("OHTTP benchmark request failed to round-trip");
        }
        let gateway_decrypt_and_decode = gateway_decode_started.elapsed();

        let gateway_encode_started = Instant::now();
        let binary_response =
            encode_response(StatusCode::OK, &response_body, request.response_padding)?;
        let encrypted_response = server_response.encapsulate(&binary_response)?;
        let gateway_encode_and_encrypt = gateway_encode_started.elapsed();

        let client_decode_started = Instant::now();
        let decoded_response = client_response.decapsulate(&encrypted_response)?;
        let (status, decoded_body) = decode_response(&decoded_response)?;
        let client_decrypt_and_decode = client_decode_started.elapsed();
        if status != StatusCode::OK || decoded_body != response_body {
            bail!("OHTTP benchmark response failed to round-trip");
        }
        observations.push(OhttpCryptoObservation {
            binary_http_request_bytes: binary_request.len(),
            encrypted_request_bytes: encrypted_request.len(),
            binary_http_response_bytes: binary_response.len(),
            encrypted_response_bytes: encrypted_response.len(),
            client_encode_and_encrypt,
            gateway_decrypt_and_decode,
            gateway_encode_and_encrypt,
            client_decrypt_and_decode,
        });
    }
    Ok(observations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selected::{
        EncryptedTagBuildRecord, NullifierBuildRecord, PocLimits, UseCaseBuildInput,
        NULLIFIER_WITNESS_BYTES,
    };

    const OPERATOR_KEY: [u8; 32] = [71; 32];

    fn input() -> UseCaseBuildInput {
        UseCaseBuildInput {
            generation_height: 900,
            generation_root_hex: hex::encode([42; 32]),
            nullifiers: (1..=128)
                .map(|value| {
                    let mut nullifier = [0; 32];
                    nullifier[0] = value;
                    NullifierBuildRecord {
                        nullifier_hex: hex::encode(nullifier),
                        position: u64::from(value),
                        witness_base64: STANDARD.encode(vec![value; NULLIFIER_WITNESS_BYTES]),
                    }
                })
                .collect(),
            encrypted_tags: (0..128)
                .map(|value| EncryptedTagBuildRecord {
                    tag_base64: STANDARD.encode(format!("tag-{value}")),
                    encrypted_values_base64: vec![
                        STANDARD.encode(format!("ciphertext-marker-{value}"))
                    ],
                })
                .collect(),
            shinzo_bucket_count: 1 << 16,
            limits: PocLimits::default(),
        }
    }

    fn stores() -> (Arc<UseCaseStore>, Arc<UseCaseStore>) {
        (
            Arc::new(UseCaseStore::build(input(), &OPERATOR_KEY, 0).unwrap()),
            Arc::new(UseCaseStore::build(input(), &OPERATOR_KEY, 1).unwrap()),
        )
    }

    #[test]
    fn bhttp_padding_modes_round_trip_and_reject_malformed_padding() {
        for padding in [
            PaddingStrategy::None,
            PaddingStrategy::PowerOfTwo { minimum_bytes: 256 },
            PaddingStrategy::Fixed {
                request_bytes: 2_048,
                response_bytes: 4_096,
            },
        ] {
            let body = br#"{"secret":"ciphertext-marker"}"#;
            let encoded = encode_request("POST", "/v1/tag/private", body, padding).unwrap();
            let decoded = decode_request(&encoded).unwrap();
            assert_eq!(decoded.body, body);
            if !matches!(padding, PaddingStrategy::None) {
                assert_eq!(encoded.len().count_ones(), 1);
                let mut malformed = encoded;
                *malformed.last_mut().unwrap() = 1;
                assert!(decode_request(&malformed).is_err());
            }
        }
        assert!(encode_request(
            "POST",
            "/v1/tag/private",
            b"{}",
            PaddingStrategy::Fixed {
                request_bytes: MAX_OHTTP_PLAINTEXT_BYTES + 1,
                response_bytes: 256,
            },
        )
        .is_err());
    }

    #[test]
    fn tor_transport_requires_remote_dns_resolution() {
        assert_eq!(
            OriginTransportConfig::Direct.build().unwrap().kind(),
            OriginTransportKind::Direct
        );
        assert!(OriginTransportConfig::TorSocks5 {
            proxy_url: "socks5://127.0.0.1:9050".to_owned(),
        }
        .build()
        .is_err());
        assert_eq!(
            OriginTransportConfig::TorSocks5 {
                proxy_url: "socks5h://127.0.0.1:9050".to_owned(),
            }
            .build()
            .unwrap()
            .kind(),
            OriginTransportKind::TorSocks5
        );
    }

    #[tokio::test]
    async fn key_rotation_retains_then_expires_previous_key() {
        let (left, _) = stores();
        let mut gateway = OhttpGateway::new(left, &OPERATOR_KEY, "left", 7).unwrap();
        let old = gateway.key_document().unwrap();
        let old_config = old.verify(&OPERATOR_KEY).unwrap();
        let mut tampered_document = old.clone();
        tampered_document.document.replica_id.push_str("-attacker");
        assert!(tampered_document.verify(&OPERATOR_KEY).is_err());
        gateway.rotate(8, 1).unwrap();
        assert!(gateway.servers.contains_key(&7));
        assert_eq!(gateway.key_document().unwrap().document.key_id, 8);
        let old_plaintext = encode_request(
            "GET",
            "/v1/manifest",
            &[],
            PaddingStrategy::PowerOfTwo { minimum_bytes: 256 },
        )
        .unwrap();
        let (old_encrypted, _) = ClientRequest::from_encoded_config(&old_config)
            .unwrap()
            .encapsulate(&old_plaintext)
            .unwrap();
        assert!(gateway.handle(&old_encrypted).await.is_ok());
        gateway.rotate(9, 1).unwrap();
        assert!(!gateway.servers.contains_key(&7));
        let (expired_encrypted, _) = ClientRequest::from_encoded_config(&old_config)
            .unwrap()
            .encapsulate(&old_plaintext)
            .unwrap();
        assert!(gateway.handle(&expired_encrypted).await.is_err());
    }

    #[tokio::test]
    async fn two_replica_ohttp_executes_all_selected_protocols() {
        let (left, right) = stores();
        let left = spawn_ohttp_replica(left, &OPERATOR_KEY, "left", 1)
            .await
            .unwrap();
        let right = spawn_ohttp_replica(right, &OPERATOR_KEY, "right", 2)
            .await
            .unwrap();
        let client = OhttpUseCaseClient::connect(
            &[left.relay_url(), right.relay_url()],
            &OPERATOR_KEY,
            PaddingStrategy::PowerOfTwo { minimum_bytes: 256 },
        )
        .await
        .unwrap();

        let mut nullifier = [0; 32];
        nullifier[0] = 31;
        let witness = client
            .strict_lookup(TableUseCase::Nullifier, &nullifier)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(witness[0], vec![31; NULLIFIER_WITNESS_BYTES]);

        let ciphertexts = client
            .strict_lookup(TableUseCase::EncryptedTag, b"tag-37")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ciphertexts[0], b"ciphertext-marker-37");
        assert!(client.subscribe_and_evaluate(12_345, 12_345).await.unwrap());
        let mut live = client.register_shinzo_subscription(12_345).await.unwrap();
        assert!(client
            .poll_shinzo_subscription(&mut live, 8)
            .await
            .unwrap()
            .is_empty());
        assert!(left.metrics.snapshot().forwarded_requests >= 6);
        assert!(right.metrics.snapshot().forwarded_requests >= 6);
    }

    #[tokio::test]
    async fn fixed_padding_equalizes_success_and_application_error_responses() {
        let (left, _) = stores();
        let replica = spawn_ohttp_replica(left, &OPERATOR_KEY, "left", 3)
            .await
            .unwrap();
        let client = OhttpClient::connect(
            &replica.relay_url(),
            &OPERATOR_KEY,
            PaddingStrategy::Fixed {
                request_bytes: 4_096,
                response_bytes: 4_096,
            },
        )
        .await
        .unwrap();
        let registration = compact_registration(123, 1 << 16, &mut OsRng).unwrap();
        let valid = ShinzoRegistrationRequest {
            body_digest_hex: client
                .key_document
                .document
                .generation_body_digest_hex
                .clone(),
            subscription_id_hex: registration.id.to_string(),
            server_key_base64: STANDARD.encode(&registration.server_keys[0]),
        };
        let before = replica.metrics.snapshot();
        client
            .post_empty("/v1/shinzo/register", &valid)
            .await
            .unwrap();
        let invalid_generation = ShinzoRegistrationRequest {
            body_digest_hex: hex::encode(
                client
                    .key_document
                    .document
                    .generation_body_digest_hex
                    .as_bytes(),
            ),
            ..valid
        };
        assert!(client
            .post_empty("/v1/shinzo/register", &invalid_generation)
            .await
            .is_err());
        let after = replica.metrics.snapshot();
        assert_eq!(
            after.encrypted_response_bytes - before.encrypted_response_bytes,
            2 * 4_128
        );
    }

    #[tokio::test]
    async fn gateway_rejects_replay_and_ciphertext_tampering() {
        let (left, _) = stores();
        let gateway = OhttpGateway::new(left, &OPERATOR_KEY, "left", 11).unwrap();
        let document = gateway.key_document().unwrap();
        let config = document.verify(&OPERATOR_KEY).unwrap();
        let plaintext = encode_request(
            "GET",
            "/v1/manifest",
            &[],
            PaddingStrategy::PowerOfTwo { minimum_bytes: 256 },
        )
        .unwrap();
        let (encrypted, _) = ClientRequest::from_encoded_config(&config)
            .unwrap()
            .encapsulate(&plaintext)
            .unwrap();
        assert!(!encrypted
            .windows(b"/v1/manifest".len())
            .any(|window| window == b"/v1/manifest"));
        assert!(gateway.handle(&encrypted).await.is_ok());
        assert!(gateway
            .handle(&encrypted)
            .await
            .unwrap_err()
            .to_string()
            .contains("replayed"));
        let mut tampered = encrypted;
        *tampered.last_mut().unwrap() ^= 1;
        assert!(gateway.handle(&tampered).await.is_err());
    }
}
