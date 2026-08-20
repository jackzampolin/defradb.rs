//! Request-response and fire-and-forget RPC helpers for the iroh endpoint.

use std::collections::HashMap;
use std::sync::Arc;

use iroh::{Endpoint, EndpointAddr};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::message::{CarFetchRequest, PushLogReply};
use crate::transport::{PeerId, TransportEvent};
use crate::QueryId;

use super::peer_map::{parse_endpoint_id, PeerMap};
use super::protocols;

#[derive(Clone, Eq, Hash, PartialEq)]
pub(super) struct ConnectionCacheKey {
    endpoint_id: iroh::EndpointId,
    alpn: Vec<u8>,
}

#[derive(Default)]
pub(super) struct ConnectionCacheState {
    connections: parking_lot::Mutex<HashMap<ConnectionCacheKey, iroh::endpoint::Connection>>,
    dial_guards: parking_lot::Mutex<HashMap<ConnectionCacheKey, Arc<tokio::sync::Mutex<()>>>>,
}

pub(super) type ConnectionCache = Arc<ConnectionCacheState>;

pub(super) fn new_connection_cache() -> ConnectionCache {
    Arc::new(ConnectionCacheState::default())
}

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
    Ok(cache.connections.lock().get(&key).cloned())
}

fn remember_connection(
    cache: &ConnectionCache,
    peer_id: &PeerId,
    alpn: &[u8],
    connection: &iroh::endpoint::Connection,
) -> crate::error::Result<()> {
    let key = connection_cache_key(peer_id, alpn)?;
    cache.connections.lock().insert(key, connection.clone());
    Ok(())
}

fn evict_connection(cache: &ConnectionCache, peer_id: &PeerId, alpn: &[u8]) {
    if let Ok(key) = connection_cache_key(peer_id, alpn) {
        cache.connections.lock().remove(&key);
    }
}

fn dial_guard(cache: &ConnectionCache, key: ConnectionCacheKey) -> Arc<tokio::sync::Mutex<()>> {
    let mut guards = cache.dial_guards.lock();
    guards.retain(|_, guard| Arc::strong_count(guard) > 1);
    Arc::clone(
        guards
            .entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
    )
}

/// QUIC application error code used when locally closing a connection in
/// response to a `disconnect` request.
const DISCONNECT_ERROR_CODE: u32 = 0;

/// Close and remove every cached connection to `endpoint_id`.
///
/// Used by `disconnect` to tear down the outbound-send connection cache for a
/// peer. Closing is idempotent — a peer with no cached connections is a no-op.
pub(super) fn close_cached_connections(cache: &ConnectionCache, endpoint_id: &iroh::EndpointId) {
    let mut guard = cache.connections.lock();
    let keys: Vec<ConnectionCacheKey> = guard
        .keys()
        .filter(|key| &key.endpoint_id == endpoint_id)
        .cloned()
        .collect();
    for key in keys {
        if let Some(connection) = guard.remove(&key) {
            connection.close(DISCONNECT_ERROR_CODE.into(), b"disconnect");
        }
    }
}

/// Hang up every connection we hold to a peer: all handles retained in
/// `peer_map` (covering dial- and accept-initiated connections across every
/// ALPN) and any cached outbound-send connections. Each stream task then
/// observes the `accept_bi` error and decrements the count until it reaches
/// zero and `PeerDisconnected` is emitted. Idempotent.
pub(super) fn close_peer_connections(
    peer_map: &Arc<parking_lot::Mutex<PeerMap>>,
    cache: &ConnectionCache,
    endpoint_id: &iroh::EndpointId,
) {
    for connection in peer_map.lock().take_connections(endpoint_id) {
        connection.close(DISCONNECT_ERROR_CODE.into(), b"disconnect");
    }
    close_cached_connections(cache, endpoint_id);
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

    let key = connection_cache_key(peer_id, alpn)?;
    let guard = dial_guard(cache, key);
    let _dial_guard = guard.lock().await;

    // Another request may have established the shared QUIC connection while
    // this request waited. Only the keyed dial owner may create it; requests
    // remain concurrent as independent streams after this point.
    if let Some(connection) = cached_connection(cache, peer_id, alpn)? {
        return Ok(connection);
    }

    let connection =
        connect_with_direct_addr_fallback(endpoint, peer_id, alpn, direct_addr).await?;
    remember_connection(cache, peer_id, alpn, &connection)?;
    Ok(connection)
}

pub(super) async fn connect_with_direct_addr_fallback(
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

/// Send a two-stream PushLog request and accept either response shape.
///
/// Current peers reply on this request's receive stream. During a rolling
/// upgrade, older peers may still reverse-dial `ALPN_TWOSTREAM_RESP`, which is
/// delivered through `legacy_reply`. A failure on either path is therefore not
/// terminal while the other path can still produce the ACK.
pub(super) async fn handle_two_stream_request(
    endpoint: &Endpoint,
    peer_id: &PeerId,
    request: &crate::message::PushLogRequest,
    direct_addr: Option<std::net::SocketAddr>,
    cache: &ConnectionCache,
    legacy_reply: oneshot::Receiver<PushLogReply>,
) -> crate::error::Result<PushLogReply> {
    let alpn = protocols::ALPN_TWOSTREAM;
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

    let wait_for_reply = async {
        let same_stream_reply = protocols::read_message(&mut recv, protocols::MAX_MESSAGE_SIZE);
        tokio::pin!(same_stream_reply);
        tokio::pin!(legacy_reply);

        tokio::select! {
            result = &mut same_stream_reply => match result {
                Ok(reply) => Ok(reply),
                Err(same_stream_error) => match legacy_reply.await {
                    Ok(reply) => Ok(reply),
                    Err(_) => Err(same_stream_error),
                },
            },
            result = &mut legacy_reply => match result {
                Ok(reply) => Ok(reply),
                Err(_) => same_stream_reply.await,
            },
        }
    };

    tokio::time::timeout(REQUEST_RESPONSE_TIMEOUT, wait_for_reply)
        .await
        .map_err(|_| {
            warn!(
                peer_id = %peer_id,
                timeout_secs = REQUEST_RESPONSE_TIMEOUT.as_secs(),
                "two-stream request timed out waiting for same-stream or legacy reply"
            );
            evict_connection(cache, peer_id, alpn);
            crate::error::Error::ResponseTimeout
        })?
        .inspect_err(|_| {
            evict_connection(cache, peer_id, alpn);
        })
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
/// This remains used for messages that do not carry an application reply on
/// their request stream, including the legacy reverse-stream PushLog response.
/// Waiting for the peer to close their side avoids dropping the bidi stream
/// while the remote reader is still consuming the frame.
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
            query_id: None,
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
    query_id: QueryId,
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
                query_id: Some(query_id),
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
            query_id: Some(query_id),
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
/// Full-DAG requests are recursive from the root; partial recovery requests
/// recurse only from the known missing frontier. The responder's shared
/// block/byte caps bound that descendant closure, and a truncated response is
/// resumed from the recomputed frontier by the same receiver owner.
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
        CarFetchRequest::selective_dag(root, missing.clone())
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
                query_id,
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

    // A usable response completes from the coordinator only after its blocks
    // are durable. An independent success event can otherwise overtake the
    // concurrently dispatched CAR response. Failures have no useful response
    // payload to order behind and retain the aggregate completion event.
    if !any_success
        && event_tx
            .send(TransportEvent::BitswapComplete {
                query_id,
                success: false,
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

    async fn localhost_endpoint(alpns: Vec<Vec<u8>>) -> iroh::Endpoint {
        iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .relay_mode(iroh::RelayMode::Disabled)
            .alpns(alpns)
            .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
            .expect("bind addr")
            .bind()
            .await
            .expect("bind endpoint")
    }

    /// Regression (#1092 review): a peer can hold several live connections
    /// (one per ALPN, dial + accept). Hanging up must close every retained
    /// handle — closing only the most recent one leaves the others alive, the
    /// connection count never reaches zero, and `PeerDisconnected` never
    /// fires.
    #[tokio::test]
    async fn close_peer_connections_closes_every_retained_handle() {
        let accept_ep = localhost_endpoint(vec![b"test/a".to_vec(), b"test/b".to_vec()]).await;
        let dial_ep = localhost_endpoint(vec![]).await;

        let accept_task = tokio::spawn({
            let ep = accept_ep.clone();
            async move {
                let mut held = Vec::new();
                while let Some(incoming) = ep.accept().await {
                    if let Ok(conn) = incoming.await {
                        held.push(conn);
                    }
                }
            }
        });

        let addr = accept_ep.addr();
        let conn_a = dial_ep
            .connect(addr.clone(), b"test/a")
            .await
            .expect("connect a");
        let conn_b = dial_ep.connect(addr, b"test/b").await.expect("connect b");

        let peer_map = Arc::new(parking_lot::Mutex::new(PeerMap::new()));
        let cache = new_connection_cache();
        let id = accept_ep.id();
        peer_map
            .lock()
            .increment_connections(id, None, conn_a.clone());
        peer_map
            .lock()
            .increment_connections(id, None, conn_b.clone());

        close_peer_connections(&peer_map, &cache, &id);

        assert!(
            conn_a.close_reason().is_some(),
            "first retained handle must be closed"
        );
        assert!(
            conn_b.close_reason().is_some(),
            "second retained handle must be closed"
        );

        accept_task.abort();
    }

    #[tokio::test]
    async fn concurrent_requests_share_one_connection_dial() {
        const ALPN: &[u8] = b"test/shared-dial";
        const CALLERS: usize = 16;

        let accept_ep = localhost_endpoint(vec![ALPN.to_vec()]).await;
        let dial_ep = localhost_endpoint(vec![]).await;
        let direct_addr = accept_ep
            .addr()
            .ip_addrs()
            .next()
            .copied()
            .expect("listener direct address");
        let peer_id = PeerId::new(accept_ep.id().to_string());
        let cache = new_connection_cache();
        let barrier = Arc::new(tokio::sync::Barrier::new(CALLERS));
        let (accepted_tx, mut accepted_rx) = tokio::sync::mpsc::unbounded_channel();

        let accept_task = tokio::spawn({
            let endpoint = accept_ep.clone();
            async move {
                let mut held = Vec::new();
                while let Some(incoming) = endpoint.accept().await {
                    if let Ok(connection) = incoming.await {
                        held.push(connection);
                        let _ = accepted_tx.send(());
                    }
                }
            }
        });

        let mut callers = tokio::task::JoinSet::new();
        for _ in 0..CALLERS {
            let endpoint = dial_ep.clone();
            let peer_id = peer_id.clone();
            let cache = Arc::clone(&cache);
            let barrier = Arc::clone(&barrier);
            callers.spawn(async move {
                barrier.wait().await;
                connect_with_cache(&endpoint, &peer_id, ALPN, Some(direct_addr), &cache)
                    .await
                    .expect("shared connection")
            });
        }

        while let Some(result) = callers.join_next().await {
            result.expect("dial task");
        }

        tokio::time::timeout(std::time::Duration::from_secs(1), accepted_rx.recv())
            .await
            .expect("receiver accepted the connection")
            .expect("accept observer remained open");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), accepted_rx.recv())
                .await
                .is_err(),
            "one peer/ALPN key must establish only one transport connection"
        );

        accept_task.abort();
    }
}
