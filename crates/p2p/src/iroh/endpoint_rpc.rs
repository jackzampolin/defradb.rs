//! Request-response and fire-and-forget RPC helpers for the iroh endpoint.

use iroh::{Endpoint, EndpointAddr};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::message::CarFetchRequest;
use crate::transport::{PeerId, TransportEvent};
use crate::QueryId;

use super::peer_map::{parse_endpoint_id, PeerMap};
use super::protocols;

/// Timeout for request-response round trips.
///
/// Covers the time from sending the request to receiving the full response.
/// Longer than the fire-and-forget timeout (5 s) because the remote peer
/// needs time to process the request before replying.
pub(super) const REQUEST_RESPONSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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
) -> crate::error::Result<Resp>
where
    Req: serde::Serialize,
    Resp: serde::de::DeserializeOwned,
{
    let endpoint_id = parse_endpoint_id(peer_id)?;
    let mut addr = EndpointAddr::from(endpoint_id);
    if let Some(sa) = direct_addr {
        addr = addr.with_ip_addr(sa);
    }

    let connection = endpoint
        .connect(addr, alpn)
        .await
        .map_err(|e| crate::error::Error::Dial(e.to_string()))?;

    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|e| crate::error::Error::Transport(e.to_string()))?;

    protocols::write_message(&mut send, request).await?;
    send.finish()
        .map_err(|e| crate::error::Error::Transport(e.to_string()))?;

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
        crate::error::Error::ResponseTimeout
    })??;
    Ok(response)
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
) -> crate::error::Result<()> {
    let endpoint_id = parse_endpoint_id(peer_id)?;
    let mut addr = EndpointAddr::from(endpoint_id);
    if let Some(sa) = direct_addr {
        addr = addr.with_ip_addr(sa);
    }

    let connection = endpoint
        .connect(addr, alpn)
        .await
        .map_err(|e| crate::error::Error::Dial(e.to_string()))?;

    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|e| crate::error::Error::Transport(e.to_string()))?;

    protocols::write_message(&mut send, msg).await?;
    send.finish()
        .map_err(|e| crate::error::Error::Transport(e.to_string()))?;

    // Wait for peer to close their side of the stream (via RESET_STREAM or FIN).
    // This ensures the connection stays open long enough for the peer's accept_bi()
    // to run and read the message before CONNECTION_CLOSE is sent.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), recv.read_to_end(16)).await;

    Ok(())
}

/// Try to fetch CAR blocks from a single provider.
///
/// Returns `true` if the fetch succeeded and a `CarFetchResponse` was emitted.
pub(super) async fn try_fetch_from_provider(
    endpoint: &Endpoint,
    provider: &PeerId,
    request: CarFetchRequest,
    direct_addr: Option<std::net::SocketAddr>,
    event_tx: &mpsc::Sender<TransportEvent>,
) -> bool {
    let endpoint_id = match parse_endpoint_id(provider) {
        Ok(id) => id,
        Err(e) => {
            warn!(provider = %provider, error = %e, "CAR fetch: invalid provider peer ID");
            return false;
        }
    };

    let mut addr = EndpointAddr::from(endpoint_id);
    if let Some(sa) = direct_addr {
        addr = addr.with_ip_addr(sa);
    }
    let connection = match endpoint.connect(addr, protocols::ALPN_CAR).await {
        Ok(conn) => conn,
        Err(e) => {
            warn!(
                provider = %provider,
                root = %request.root_cid,
                recursive = request.recursive,
                requested_count = request.wanted_cids.len(),
                error = %e,
                "CAR fetch: connection failed"
            );
            return false;
        }
    };

    let (mut send, mut recv) = match connection.open_bi().await {
        Ok(streams) => streams,
        Err(e) => {
            warn!(
                provider = %provider,
                root = %request.root_cid,
                error = %e,
                "CAR fetch: open_bi failed"
            );
            return false;
        }
    };

    if let Err(e) = protocols::write_message(&mut send, &request).await {
        warn!(
            provider = %provider,
            root = %request.root_cid,
            error = %e,
            "CAR fetch: write_message failed"
        );
        return false;
    }
    let _ = send.finish();

    info!(
        provider = %provider,
        root = %request.root_cid,
        recursive = request.recursive,
        requested_count = request.wanted_cids.len(),
        "CAR fetch: request sent, waiting for response"
    );

    let car_data = match recv.read_to_end(64 * 1024 * 1024).await {
        Ok(data) => data,
        Err(e) => {
            warn!(
                provider = %provider,
                root = %request.root_cid,
                error = %e,
                "CAR fetch: read response failed"
            );
            return false;
        }
    };

    if car_data.is_empty() {
        warn!(
            provider = %provider,
            root = %request.root_cid,
            "CAR fetch: empty response"
        );
        return false;
    }

    debug!(
        provider = %provider,
        root = %request.root_cid,
        recursive = request.recursive,
        requested_count = request.wanted_cids.len(),
        car_bytes = car_data.len(),
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
        return false;
    }
    true
}

/// CAR-based block sync: fetch blocks from providers concurrently.
///
/// Full-DAG requests are recursive from `root`; partial recovery requests carry
/// the exact missing CIDs and expect a selective CAR response.
pub(super) async fn handle_block_sync(
    endpoint: Endpoint,
    peer_map: std::sync::Arc<parking_lot::Mutex<PeerMap>>,
    query_id: QueryId,
    root: cid::Cid,
    providers: Vec<PeerId>,
    missing: Vec<cid::Cid>,
    event_tx: mpsc::Sender<TransportEvent>,
) {
    use tokio::task::JoinHandle;

    if !missing.is_empty() {
        debug!(
            root = %root,
            missing_count = missing.len(),
            "Block sync requested with {} missing CIDs",
            missing.len()
        );
    }

    let mut tasks: Vec<JoinHandle<bool>> = Vec::with_capacity(providers.len());

    let request = if missing.is_empty() {
        CarFetchRequest::full_dag(root)
    } else {
        CarFetchRequest::selective_blocks(root, missing.clone())
    };

    for provider in &providers {
        let endpoint = endpoint.clone();
        let peer_map = std::sync::Arc::clone(&peer_map);
        let event_tx = event_tx.clone();
        let provider = provider.clone();
        let request = request.clone();
        tasks.push(tokio::spawn(async move {
            let direct_addr = super::endpoint::peer_direct_addr(&peer_map, &provider);
            try_fetch_from_provider(&endpoint, &provider, request, direct_addr, &event_tx).await
        }));
    }

    let mut any_success = false;

    for task in tasks {
        match task.await {
            Ok(true) => {
                any_success = true;
                break;
            }
            Ok(false) => {}
            Err(e) => {
                debug!("Block sync task panicked: {}", e);
            }
        }
    }

    if event_tx
        .send(TransportEvent::BitswapComplete {
            query_id,
            success: any_success,
            error: if any_success {
                None
            } else {
                Some("all providers failed".to_string())
            },
        })
        .await
        .is_err()
    {
        warn!("Event channel closed, cannot emit BitswapComplete");
    }
}
