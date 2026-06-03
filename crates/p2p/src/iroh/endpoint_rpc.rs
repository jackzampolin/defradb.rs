//! Request-response and fire-and-forget RPC helpers for the iroh endpoint.

use std::collections::HashMap;
use std::sync::Arc;

use iroh::{Endpoint, EndpointAddr};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::message::CarFetchRequest;
use crate::transport::{PeerId, TransportEvent};
use crate::QueryId;

use super::peer_map::{parse_endpoint_id, PeerMap};
use super::protocols;

#[derive(Clone, Eq, Hash, PartialEq)]
pub(super) struct ConnectionCacheKey {
    endpoint_id: iroh::EndpointId,
    alpn: Vec<u8>,
}

pub(super) type ConnectionCache =
    Arc<parking_lot::Mutex<HashMap<ConnectionCacheKey, iroh::endpoint::Connection>>>;

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const OPEN_STREAM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Timeout for request-response round trips.
///
/// Covers the time from sending the request to receiving the full response.
/// Longer than the fire-and-forget timeout (5 s) because the remote peer
/// needs time to process the request before replying.
pub(super) const REQUEST_RESPONSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Debug, Clone)]
struct CarFetchAttempt {
    provider: PeerId,
    outcome: CarFetchOutcome,
}

#[derive(Debug, Clone)]
enum CarFetchOutcome {
    Success,
    InvalidPeerId(String),
    ConnectFailed(String),
    OpenBiFailed(String),
    WriteFailed(String),
    ReadFailed(String),
    EmptyResponse,
    HeaderOnlyCar,
    EventChannelClosed,
}

impl CarFetchOutcome {
    fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::InvalidPeerId(_) => "invalid_peer_id",
            Self::ConnectFailed(_) => "connect_failed",
            Self::OpenBiFailed(_) => "open_bi_failed",
            Self::WriteFailed(_) => "write_failed",
            Self::ReadFailed(_) => "read_failed",
            Self::EmptyResponse => "empty_response",
            Self::HeaderOnlyCar => "header_only_car",
            Self::EventChannelClosed => "event_channel_closed",
        }
    }

    fn detail(&self) -> Option<&str> {
        match self {
            Self::InvalidPeerId(detail)
            | Self::ConnectFailed(detail)
            | Self::OpenBiFailed(detail)
            | Self::WriteFailed(detail)
            | Self::ReadFailed(detail) => Some(detail),
            Self::Success
            | Self::EmptyResponse
            | Self::HeaderOnlyCar
            | Self::EventChannelClosed => None,
        }
    }
}

fn summarize_car_fetch_attempts(attempts: &[CarFetchAttempt]) -> String {
    const MAX_ATTEMPTS: usize = 4;

    let mut parts = Vec::new();
    for attempt in attempts.iter().take(MAX_ATTEMPTS) {
        let mut summary = format!("{}:{}", attempt.provider, attempt.outcome.label());
        if let Some(detail) = attempt.outcome.detail() {
            summary.push('(');
            summary.push_str(detail);
            summary.push(')');
        }
        parts.push(summary);
    }

    if attempts.len() > MAX_ATTEMPTS {
        parts.push(format!("+{} more", attempts.len() - MAX_ATTEMPTS));
    }

    parts.join(", ")
}

fn summarize_cid_sample(cids: &[cid::Cid]) -> String {
    const MAX_CIDS: usize = 4;

    let mut parts: Vec<String> = cids
        .iter()
        .take(MAX_CIDS)
        .map(ToString::to_string)
        .collect();

    if cids.len() > MAX_CIDS {
        parts.push(format!("+{} more", cids.len() - MAX_CIDS));
    }

    parts.join(", ")
}

fn endpoint_addr(
    endpoint_id: iroh::EndpointId,
    direct_addr: Option<std::net::SocketAddr>,
) -> EndpointAddr {
    let mut addr = EndpointAddr::from(endpoint_id);
    if let Some(sa) = direct_addr {
        addr = addr.with_ip_addr(sa);
    }
    addr
}

async fn connect_once(
    endpoint: &Endpoint,
    endpoint_id: iroh::EndpointId,
    alpn: &[u8],
    direct_addr: Option<std::net::SocketAddr>,
) -> crate::error::Result<iroh::endpoint::Connection> {
    tokio::time::timeout(
        CONNECT_TIMEOUT,
        endpoint.connect(endpoint_addr(endpoint_id, direct_addr), alpn),
    )
    .await
    .map_err(|_| {
        crate::error::Error::Dial(format!("timed out after {}s", CONNECT_TIMEOUT.as_secs()))
    })?
    .map_err(|e| crate::error::Error::Dial(e.to_string()))
}

async fn open_bi_with_timeout(
    connection: &iroh::endpoint::Connection,
    peer_id: &PeerId,
    alpn: &[u8],
) -> crate::error::Result<(iroh::endpoint::SendStream, iroh::endpoint::RecvStream)> {
    tokio::time::timeout(OPEN_STREAM_TIMEOUT, connection.open_bi())
        .await
        .map_err(|_| {
            crate::error::Error::Transport(format!(
                "timed out opening {} stream to {} after {}s",
                String::from_utf8_lossy(alpn),
                peer_id,
                OPEN_STREAM_TIMEOUT.as_secs()
            ))
        })?
        .map_err(|e| crate::error::Error::Transport(e.to_string()))
}

fn connection_cache_key(peer_id: &PeerId, alpn: &[u8]) -> crate::error::Result<ConnectionCacheKey> {
    Ok(ConnectionCacheKey {
        endpoint_id: parse_endpoint_id(peer_id)?,
        alpn: alpn.to_vec(),
    })
}

fn cached_connection(
    cache: &ConnectionCache,
    peer_id: &PeerId,
    alpn: &[u8],
) -> crate::error::Result<Option<iroh::endpoint::Connection>> {
    let key = connection_cache_key(peer_id, alpn)?;
    Ok(cache.lock().get(&key).cloned())
}

fn remember_connection(
    cache: &ConnectionCache,
    peer_id: &PeerId,
    alpn: &[u8],
    connection: &iroh::endpoint::Connection,
) -> crate::error::Result<()> {
    let key = connection_cache_key(peer_id, alpn)?;
    cache.lock().insert(key, connection.clone());
    Ok(())
}

fn evict_connection(cache: &ConnectionCache, peer_id: &PeerId, alpn: &[u8]) {
    if let Ok(key) = connection_cache_key(peer_id, alpn) {
        cache.lock().remove(&key);
    }
}

async fn connect_with_cache(
    endpoint: &Endpoint,
    peer_id: &PeerId,
    alpn: &[u8],
    direct_addr: Option<std::net::SocketAddr>,
    cache: &ConnectionCache,
) -> crate::error::Result<iroh::endpoint::Connection> {
    if let Some(connection) = cached_connection(cache, peer_id, alpn)? {
        return Ok(connection);
    }

    let connection =
        connect_with_direct_addr_fallback(endpoint, peer_id, alpn, direct_addr).await?;
    remember_connection(cache, peer_id, alpn, &connection)?;
    Ok(connection)
}

async fn connect_with_direct_addr_fallback(
    endpoint: &Endpoint,
    peer_id: &PeerId,
    alpn: &[u8],
    direct_addr: Option<std::net::SocketAddr>,
) -> crate::error::Result<iroh::endpoint::Connection> {
    let endpoint_id = parse_endpoint_id(peer_id)?;

    if let Some(sa) = direct_addr {
        match connect_once(endpoint, endpoint_id, alpn, Some(sa)).await {
            Ok(connection) => return Ok(connection),
            Err(error) => {
                debug!(
                    peer_id = %peer_id,
                    direct_addr = %sa,
                    alpn = %String::from_utf8_lossy(alpn),
                    error = %error,
                    "Direct iroh dial failed, retrying without cached direct address"
                );
            }
        }
    }

    connect_once(endpoint, endpoint_id, alpn, None).await
}

/// Send a request and wait for a response (bidirectional stream).
///
/// `direct_addr` is an optional cached socket address for the peer; when provided it is
/// added to the `EndpointAddr` so iroh can connect directly without relay discovery.
pub(super) async fn handle_request_response<Req, Resp>(
    endpoint: &Endpoint,
    peer_id: &PeerId,
    alpn: &[u8],
    request: &Req,
    direct_addr: Option<std::net::SocketAddr>,
    cache: &ConnectionCache,
) -> crate::error::Result<Resp>
where
    Req: serde::Serialize,
    Resp: serde::de::DeserializeOwned,
{
    let connection = connect_with_cache(endpoint, peer_id, alpn, direct_addr, cache).await?;

    let (mut send, mut recv) = match open_bi_with_timeout(&connection, peer_id, alpn).await {
        Ok(streams) => streams,
        Err(error) => {
            evict_connection(cache, peer_id, alpn);
            return Err(error);
        }
    };

    if let Err(error) = protocols::write_message(&mut send, request).await {
        evict_connection(cache, peer_id, alpn);
        return Err(error);
    }
    if let Err(error) = send
        .finish()
        .map_err(|e| crate::error::Error::Transport(e.to_string()))
    {
        evict_connection(cache, peer_id, alpn);
        return Err(error);
    }

    let response: Resp = tokio::time::timeout(
        REQUEST_RESPONSE_TIMEOUT,
        protocols::read_message(&mut recv, protocols::MAX_MESSAGE_SIZE),
    )
    .await
    .map_err(|_| {
        let alpn_str = String::from_utf8_lossy(alpn);
        warn!(
            peer_id = %peer_id,
            alpn = %alpn_str,
            timeout_secs = REQUEST_RESPONSE_TIMEOUT.as_secs(),
            "request-response timed out waiting for peer"
        );
        evict_connection(cache, peer_id, alpn);
        crate::error::Error::ResponseTimeout
    })?
    .inspect_err(|_| {
        evict_connection(cache, peer_id, alpn);
    })?;
    Ok(response)
}

async fn send_one_way_message<T: serde::Serialize>(
    endpoint: &Endpoint,
    peer_id: &PeerId,
    alpn: &[u8],
    msg: &T,
    direct_addr: Option<std::net::SocketAddr>,
    cache: &ConnectionCache,
) -> crate::error::Result<()> {
    let connection = connect_with_cache(endpoint, peer_id, alpn, direct_addr, cache).await?;

    let (mut send, mut recv) = match open_bi_with_timeout(&connection, peer_id, alpn).await {
        Ok(streams) => streams,
        Err(error) => {
            evict_connection(cache, peer_id, alpn);
            return Err(error);
        }
    };

    if let Err(error) = protocols::write_message(&mut send, msg).await {
        evict_connection(cache, peer_id, alpn);
        return Err(error);
    }
    if let Err(error) = send
        .finish()
        .map_err(|e| crate::error::Error::Transport(e.to_string()))
    {
        evict_connection(cache, peer_id, alpn);
        return Err(error);
    }

    // Wait for peer to close their side of the stream (via RESET_STREAM or FIN).
    // This ensures the connection stays open long enough for the peer's accept_bi()
    // to run and read the message before CONNECTION_CLOSE is sent.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), recv.read_to_end(16)).await;

    Ok(())
}

/// Send a message without expecting a response.
///
/// Keeps the connection alive until the peer closes their stream, ensuring
/// the message is received before CONNECTION_CLOSE is sent.
pub(super) async fn handle_fire_and_forget<T: serde::Serialize>(
    endpoint: &Endpoint,
    peer_id: &PeerId,
    alpn: &[u8],
    msg: &T,
    direct_addr: Option<std::net::SocketAddr>,
    cache: &ConnectionCache,
) -> crate::error::Result<()> {
    send_one_way_message(endpoint, peer_id, alpn, msg, direct_addr, cache).await
}

/// Send a one-way message, then keep the bidirectional stream alive briefly so
/// the peer can finish reading it.
///
/// The two-stream request reply arrives on a separate response ALPN, so we do
/// not read an application reply here. But dropping the bidi stream immediately
/// after `finish()` can cause the remote iroh reader to see
/// `connection lost` mid-frame under burst load. Waiting for the peer to close
/// their side (or timing out) preserves the request-stream lifetime without
/// collapsing back to same-stream request/response semantics.
pub(super) async fn handle_send_only<T: serde::Serialize>(
    endpoint: &Endpoint,
    peer_id: &PeerId,
    alpn: &[u8],
    msg: &T,
    direct_addr: Option<std::net::SocketAddr>,
    cache: &ConnectionCache,
) -> crate::error::Result<()> {
    send_one_way_message(endpoint, peer_id, alpn, msg, direct_addr, cache).await
}

/// Send a CAR request and emit the response as a transport event.
///
/// Unlike generic fire-and-forget messages, CAR requests expect the peer to
/// stream raw CAR bytes back on the same bidirectional stream. We must consume
/// that response here; otherwise the remote writer sees a broken stream and the
/// caller never receives a `CarFetchResponse`.
pub(super) async fn handle_car_request_response(
    endpoint: &Endpoint,
    peer_id: &PeerId,
    root_cid: cid::Cid,
    direct_addr: Option<std::net::SocketAddr>,
    cache: &ConnectionCache,
    event_tx: &mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
) -> crate::error::Result<()> {
    let connection =
        connect_with_cache(endpoint, peer_id, protocols::ALPN_CAR, direct_addr, cache).await?;

    let (mut send, mut recv) =
        match open_bi_with_timeout(&connection, peer_id, protocols::ALPN_CAR).await {
            Ok(streams) => streams,
            Err(error) => {
                evict_connection(cache, peer_id, protocols::ALPN_CAR);
                return Err(error);
            }
        };

    let request = CarFetchRequest::full_dag(root_cid);
    if let Err(error) = protocols::write_message(&mut send, &request).await {
        evict_connection(cache, peer_id, protocols::ALPN_CAR);
        return Err(error);
    }
    if let Err(error) = send
        .finish()
        .map_err(|e| crate::error::Error::Transport(e.to_string()))
    {
        evict_connection(cache, peer_id, protocols::ALPN_CAR);
        return Err(error);
    }

    let car_data = tokio::time::timeout(
        REQUEST_RESPONSE_TIMEOUT,
        recv.read_to_end(protocols::MAX_CAR_SIZE),
    )
    .await
    .map_err(|_| {
        evict_connection(cache, peer_id, protocols::ALPN_CAR);
        crate::error::Error::ResponseTimeout
    })?
    .map_err(|e| {
        evict_connection(cache, peer_id, protocols::ALPN_CAR);
        crate::error::Error::Transport(e.to_string())
    })?;

    if event_tx
        .send(TransportEvent::CarFetchResponse {
            peer_id: peer_id.clone(),
            root_cid,
            car_data,
        })
        .await
        .is_err()
    {
        warn!("Event channel closed, cannot emit CarFetchResponse");
    }

    Ok(())
}

/// Try to fetch CAR blocks from a single provider.
///
/// Returns a provider outcome so the caller can aggregate useful diagnostics.
async fn try_fetch_from_provider(
    endpoint: &Endpoint,
    provider: &PeerId,
    request: CarFetchRequest,
    direct_addr: Option<std::net::SocketAddr>,
    cache: &ConnectionCache,
    event_tx: &mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
) -> CarFetchAttempt {
    // Per-provider CAR failures log at debug — the caller aggregates per-DAG
    // outcomes into a single WARN via BitswapComplete (see issue #858).
    if let Err(error) = parse_endpoint_id(provider) {
        debug!(
            provider = %provider,
            error = %error,
            "CAR fetch: invalid provider peer ID"
        );
        return CarFetchAttempt {
            provider: provider.clone(),
            outcome: CarFetchOutcome::InvalidPeerId(error.to_string()),
        };
    }

    let connection =
        match connect_with_cache(endpoint, provider, protocols::ALPN_CAR, direct_addr, cache).await
        {
            Ok(conn) => conn,
            Err(e) => {
                debug!(
                    provider = %provider,
                    root = %request.root_cid,
                    recursive = request.recursive,
                    requested_count = request.wanted_cids.len(),
                    error = %e,
                    "CAR fetch: connection failed"
                );
                return CarFetchAttempt {
                    provider: provider.clone(),
                    outcome: CarFetchOutcome::ConnectFailed(e.to_string()),
                };
            }
        };

    let (mut send, mut recv) =
        match open_bi_with_timeout(&connection, provider, protocols::ALPN_CAR).await {
            Ok(streams) => streams,
            Err(e) => {
                evict_connection(cache, provider, protocols::ALPN_CAR);
                debug!(
                    provider = %provider,
                    root = %request.root_cid,
                    error = %e,
                    "CAR fetch: open_bi failed"
                );
                return CarFetchAttempt {
                    provider: provider.clone(),
                    outcome: CarFetchOutcome::OpenBiFailed(e.to_string()),
                };
            }
        };

    if let Err(e) = protocols::write_message(&mut send, &request).await {
        evict_connection(cache, provider, protocols::ALPN_CAR);
        debug!(
            provider = %provider,
            root = %request.root_cid,
            error = %e,
            "CAR fetch: write_message failed"
        );
        return CarFetchAttempt {
            provider: provider.clone(),
            outcome: CarFetchOutcome::WriteFailed(e.to_string()),
        };
    }
    if let Err(e) = send.finish() {
        evict_connection(cache, provider, protocols::ALPN_CAR);
        debug!(
            provider = %provider,
            root = %request.root_cid,
            error = %e,
            "CAR fetch: finish stream failed"
        );
        return CarFetchAttempt {
            provider: provider.clone(),
            outcome: CarFetchOutcome::WriteFailed(e.to_string()),
        };
    }

    debug!(
        provider = %provider,
        root = %request.root_cid,
        recursive = request.recursive,
        requested_count = request.wanted_cids.len(),
        "CAR fetch: request sent, waiting for response"
    );

    let car_data =
        match tokio::time::timeout(REQUEST_RESPONSE_TIMEOUT, recv.read_to_end(64 * 1024 * 1024))
            .await
        {
            Ok(Ok(data)) => data,
            Ok(Err(e)) => {
                evict_connection(cache, provider, protocols::ALPN_CAR);
                debug!(
                    provider = %provider,
                    root = %request.root_cid,
                    error = %e,
                    "CAR fetch: read response failed"
                );
                return CarFetchAttempt {
                    provider: provider.clone(),
                    outcome: CarFetchOutcome::ReadFailed(e.to_string()),
                };
            }
            Err(_) => {
                evict_connection(cache, provider, protocols::ALPN_CAR);
                debug!(
                    provider = %provider,
                    root = %request.root_cid,
                    recursive = request.recursive,
                    requested_count = request.wanted_cids.len(),
                    timeout_secs = REQUEST_RESPONSE_TIMEOUT.as_secs(),
                    "CAR fetch: response timed out"
                );
                return CarFetchAttempt {
                    provider: provider.clone(),
                    outcome: CarFetchOutcome::ReadFailed(format!(
                        "timed out after {}s",
                        REQUEST_RESPONSE_TIMEOUT.as_secs()
                    )),
                };
            }
        };

    if car_data.is_empty() {
        debug!(
            provider = %provider,
            root = %request.root_cid,
            "CAR fetch: empty response"
        );
        // Surface the empty response to the coordinator so it can increment
        // the car_empty_responses diagnostic. Still counts as a provider
        // failure for the aggregation in handle_block_sync (issue #858).
        let _ = event_tx
            .send(TransportEvent::CarFetchResponse {
                peer_id: provider.clone(),
                root_cid: request.root_cid,
                car_data: Vec::new(),
            })
            .await;
        return CarFetchAttempt {
            provider: provider.clone(),
            outcome: CarFetchOutcome::EmptyResponse,
        };
    }

    // Distinguish a header-only CAR (server's "no blocks" signal) from a
    // usable fetch. The coordinator still needs the bytes so it can count
    // car_empty_responses, but the aggregation in `handle_block_sync` must
    // treat a header-only response as a provider miss — otherwise the final
    // "none returned usable blocks" WARN is suppressed when every provider
    // replies empty (issue #858 review round 3).
    let has_blocks = crate::sync::car::car_has_any_block(&car_data);

    debug!(
        provider = %provider,
        root = %request.root_cid,
        recursive = request.recursive,
        requested_count = request.wanted_cids.len(),
        car_bytes = car_data.len(),
        has_blocks,
        "CAR fetch: response received"
    );

    if event_tx
        .send(TransportEvent::CarFetchResponse {
            peer_id: provider.clone(),
            root_cid: request.root_cid,
            car_data,
        })
        .await
        .is_err()
    {
        warn!("Event channel closed, cannot emit CarFetchResponse");
        return CarFetchAttempt {
            provider: provider.clone(),
            outcome: CarFetchOutcome::EventChannelClosed,
        };
    }
    CarFetchAttempt {
        provider: provider.clone(),
        outcome: if has_blocks {
            CarFetchOutcome::Success
        } else {
            CarFetchOutcome::HeaderOnlyCar
        },
    }
}

#[derive(Clone)]
pub(super) struct BlockSyncResources {
    endpoint: Endpoint,
    peer_map: std::sync::Arc<parking_lot::Mutex<PeerMap>>,
    connection_cache: ConnectionCache,
    event_tx: mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
}

impl BlockSyncResources {
    pub(super) fn new(
        endpoint: Endpoint,
        peer_map: std::sync::Arc<parking_lot::Mutex<PeerMap>>,
        connection_cache: ConnectionCache,
        event_tx: mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
    ) -> Self {
        Self {
            endpoint,
            peer_map,
            connection_cache,
            event_tx,
        }
    }
}

/// CAR-based block sync: fetch blocks from providers concurrently.
///
/// Full-DAG requests are recursive from `root`; partial recovery requests carry
/// the exact missing CIDs and expect a selective CAR response.
pub(super) async fn handle_block_sync(
    resources: BlockSyncResources,
    query_id: QueryId,
    root: cid::Cid,
    providers: Vec<PeerId>,
    missing: Vec<cid::Cid>,
) {
    use tokio::task::JoinSet;

    if !missing.is_empty() {
        debug!(
            root = %root,
            missing_count = missing.len(),
            "Block sync requested with {} missing CIDs",
            missing.len()
        );
    }

    let mut tasks: JoinSet<CarFetchAttempt> = JoinSet::new();

    let request = if missing.is_empty() {
        CarFetchRequest::full_dag(root)
    } else {
        CarFetchRequest::selective_blocks(root, missing.clone())
    };

    let BlockSyncResources {
        endpoint,
        peer_map,
        connection_cache,
        event_tx,
    } = resources;

    for provider in &providers {
        let endpoint = endpoint.clone();
        let peer_map = std::sync::Arc::clone(&peer_map);
        let connection_cache = Arc::clone(&connection_cache);
        let event_tx = event_tx.clone();
        let provider = provider.clone();
        let request = request.clone();
        tasks.spawn(async move {
            let direct_addr = super::endpoint::peer_direct_addr(&peer_map, &provider);
            try_fetch_from_provider(
                &endpoint,
                &provider,
                request,
                direct_addr,
                &connection_cache,
                &event_tx,
            )
            .await
        });
    }

    let mut any_success = false;
    let mut failures = Vec::new();
    let provider_count = providers.len();
    let kind = if missing.is_empty() {
        "full-dag"
    } else {
        "selective"
    };

    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(attempt) if attempt.outcome.is_success() => {
                any_success = true;
                tasks.abort_all();
                break;
            }
            Ok(attempt) => failures.push(attempt),
            Err(e) => {
                debug!("Block sync task panicked: {}", e);
                failures.push(CarFetchAttempt {
                    provider: PeerId::new("task".to_string()),
                    outcome: CarFetchOutcome::ReadFailed(format!("join_error: {e}")),
                });
            }
        }
    }

    let error = if any_success {
        None
    } else {
        let missing_summary = if missing.is_empty() {
            String::new()
        } else {
            format!("; missing_sample=[{}]", summarize_cid_sample(&missing))
        };
        Some(format!(
            "{} CAR fetch failed: {} provider(s) tried, none returned usable blocks (root={}); outcomes=[{}]{}",
            kind,
            provider_count,
            root,
            summarize_car_fetch_attempts(&failures),
            missing_summary,
        ))
    };

    if event_tx
        .send(TransportEvent::BitswapComplete {
            query_id,
            success: any_success,
            error,
        })
        .await
        .is_err()
    {
        warn!("Event channel closed, cannot emit BitswapComplete");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use multihash_codetable::{Code, MultihashDigest};

    fn peer_id(label: &[u8]) -> PeerId {
        let hash = Code::Sha2_256.digest(label);
        let cid = cid::Cid::new_v1(0x71, hash);
        PeerId::new(cid.to_string())
    }

    fn cid(label: &[u8]) -> cid::Cid {
        let hash = Code::Sha2_256.digest(label);
        cid::Cid::new_v1(0x71, hash)
    }

    #[test]
    fn summarize_car_fetch_attempts_includes_provider_and_reason() {
        let attempts = vec![
            CarFetchAttempt {
                provider: peer_id(b"provider-a"),
                outcome: CarFetchOutcome::HeaderOnlyCar,
            },
            CarFetchAttempt {
                provider: peer_id(b"provider-b"),
                outcome: CarFetchOutcome::ConnectFailed("timeout".into()),
            },
        ];

        let summary = summarize_car_fetch_attempts(&attempts);

        assert!(summary.contains("header_only_car"));
        assert!(summary.contains("connect_failed(timeout)"));
        assert!(summary.contains(&attempts[0].provider.to_string()));
        assert!(summary.contains(&attempts[1].provider.to_string()));
    }

    #[test]
    fn summarize_cid_sample_limits_output() {
        let summary =
            summarize_cid_sample(&[cid(b"a"), cid(b"b"), cid(b"c"), cid(b"d"), cid(b"e")]);

        assert!(summary.contains(&cid(b"a").to_string()));
        assert!(summary.contains(&cid(b"d").to_string()));
        assert!(summary.contains("+1 more"));
    }
}
