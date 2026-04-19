//! Incoming connection and stream processing for the iroh endpoint.

use std::collections::HashMap;
use std::sync::Arc;

use iroh::endpoint::Connection;
use iroh::EndpointId;
use iroh_gossip::net::Gossip;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::message::PushLogReply;
use crate::transport::{PeerId, TransportEvent};

use super::endpoint::{join_peer_to_subscriptions, TopicSubscription};
use super::peer_map::{endpoint_id_to_peer_id, PeerMap};
use super::protocols;

/// Handle an incoming QUIC connection.
pub(super) async fn handle_incoming(
    incoming: iroh::endpoint::Incoming,
    gossip: &Gossip,
    peer_map: &Arc<parking_lot::Mutex<PeerMap>>,
    pending_pushlog_replies: &Arc<
        parking_lot::Mutex<HashMap<String, oneshot::Sender<PushLogReply>>>,
    >,
    subscriptions: &HashMap<String, TopicSubscription>,
    event_tx: &mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
) {
    let remote_addr = match incoming.remote_addr() {
        iroh::endpoint::IncomingAddr::Ip(addr) => Some(addr),
        _ => None,
    };

    // Accept the connection
    let connection = match incoming.accept() {
        Ok(accepting) => match accepting.await {
            Ok(conn) => conn,
            Err(e) => {
                let error_msg = e.to_string();
                if crate::error::Error::is_connection_loss_reason(&error_msg) {
                    debug!(
                        remote_addr = ?remote_addr,
                        error = %error_msg,
                        "Incoming connection handshake ended before completion"
                    );
                } else {
                    warn!(
                        remote_addr = ?remote_addr,
                        error = %error_msg,
                        "Failed to complete connection handshake"
                    );
                }
                return;
            }
        },
        Err(e) => {
            warn!("Failed to accept connection: {}", e);
            return;
        }
    };

    let conn_alpn = connection.alpn().to_vec();

    // If it's a gossip ALPN, hand off to the gossip layer
    if conn_alpn == iroh_gossip::net::GOSSIP_ALPN {
        if let Err(e) = gossip.handle_connection(connection).await {
            debug!("Gossip handle_connection error: {}", e);
        }
        return;
    }

    let remote_id = connection.remote_id();

    let is_new = peer_map
        .lock()
        .increment_connections(remote_id, remote_addr);

    if is_new
        && event_tx
            .send(TransportEvent::PeerConnected(endpoint_id_to_peer_id(
                &remote_id,
            )))
            .await
            .is_err()
    {
        warn!("Event channel closed, cannot emit PeerConnected");
    }

    if is_new {
        join_peer_to_subscriptions(subscriptions, remote_id).await;
    }

    // Spawn handler for this connection's streams
    let event_tx = event_tx.clone();
    let peer_map = Arc::clone(peer_map);
    let pending_pushlog_replies = Arc::clone(pending_pushlog_replies);
    tokio::spawn(async move {
        handle_connection_streams(
            connection,
            remote_id,
            conn_alpn,
            event_tx,
            peer_map,
            pending_pushlog_replies,
        )
        .await;
    });
}

/// Process streams on an accepted connection, dispatching by ALPN.
///
/// Emits `PeerDisconnected` only when the last connection for this peer closes.
async fn handle_connection_streams(
    connection: Connection,
    remote_id: EndpointId,
    alpn: Vec<u8>,
    event_tx: mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
    peer_map: Arc<parking_lot::Mutex<PeerMap>>,
    pending_pushlog_replies: Arc<
        parking_lot::Mutex<HashMap<String, oneshot::Sender<PushLogReply>>>,
    >,
) {
    let peer_id = endpoint_id_to_peer_id(&remote_id);

    while let Ok((send, mut recv)) = connection.accept_bi().await {
        let peer_id = peer_id.clone();
        let event_tx = event_tx.clone();
        let alpn = alpn.clone();
        let pending_pushlog_replies = pending_pushlog_replies.clone();
        tokio::spawn(async move {
            if let Err(e) = dispatch_stream(
                &alpn,
                &peer_id,
                send,
                &mut recv,
                &event_tx,
                &pending_pushlog_replies,
            )
            .await
            {
                debug!("Stream error from {}: {}", peer_id, e);
            }
        });
    }

    let fully_disconnected = peer_map.lock().decrement_connections(&remote_id);
    debug!(peer_id = %peer_id, fully_disconnected, "Connection closed");

    if fully_disconnected
        && event_tx
            .send(TransportEvent::PeerDisconnected(peer_id))
            .await
            .is_err()
    {
        debug!("Event channel closed, cannot emit PeerDisconnected");
    }
}

/// Variant for connections initiated by dial (reuses the same stream handling).
pub(super) async fn handle_connection_streams_from_dial(
    connection: Connection,
    remote_id: EndpointId,
    alpn: Vec<u8>,
    event_tx: mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
    peer_map: Arc<parking_lot::Mutex<PeerMap>>,
    pending_pushlog_replies: Arc<
        parking_lot::Mutex<HashMap<String, oneshot::Sender<PushLogReply>>>,
    >,
) {
    handle_connection_streams(
        connection,
        remote_id,
        alpn,
        event_tx,
        peer_map,
        pending_pushlog_replies,
    )
    .await;
}

/// Dispatch a stream based on the connection ALPN.
async fn dispatch_stream(
    alpn: &[u8],
    peer_id: &PeerId,
    send: iroh::endpoint::SendStream,
    recv: &mut iroh::endpoint::RecvStream,
    event_tx: &mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
    pending_pushlog_replies: &Arc<
        parking_lot::Mutex<HashMap<String, oneshot::Sender<PushLogReply>>>,
    >,
) -> crate::error::Result<()> {
    match alpn {
        x if x == protocols::ALPN_PUSHLOG => {
            let request: crate::message::PushLogRequest =
                protocols::read_message(recv, protocols::MAX_MESSAGE_SIZE).await?;
            if event_tx
                .send(TransportEvent::PushLogRequest {
                    peer_id: peer_id.clone(),
                    request,
                    token: send,
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit PushLogRequest");
            }
        }
        x if x == protocols::ALPN_TWOSTREAM => {
            let payload = protocols::read_message_bytes(recv, protocols::MAX_MESSAGE_SIZE).await?;
            if let Ok(reply) = serde_cbor::from_slice::<PushLogReply>(&payload) {
                let sender = pending_pushlog_replies.lock().remove(&reply.message_id);
                if let Some(sender) = sender {
                    let _ = sender.send(reply);
                    return Ok(());
                }
            }

            let request: crate::message::PushLogRequest = serde_cbor::from_slice(&payload)
                .map_err(|e| crate::error::Error::Codec(e.to_string()))?;
            if event_tx
                .send(TransportEvent::TwoStreamRequest {
                    peer_id: peer_id.clone(),
                    request,
                    token: Some(send),
                    is_explicit_replicator: false,
                    explicit_replay_authorization: None,
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit TwoStreamRequest");
            }
        }
        x if x == protocols::ALPN_DOCSYNC => {
            let request: crate::message::DocSyncRequest =
                protocols::read_message(recv, protocols::MAX_MESSAGE_SIZE).await?;
            if event_tx
                .send(TransportEvent::DocSyncRequest {
                    peer_id: peer_id.clone(),
                    request,
                    token: Some(send),
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit DocSyncRequest");
            }
        }
        x if x == protocols::ALPN_BRANCHABLE => {
            let request: crate::message::BranchableSyncRequest =
                protocols::read_message(recv, protocols::MAX_MESSAGE_SIZE).await?;
            if event_tx
                .send(TransportEvent::BranchableSyncRequest {
                    peer_id: peer_id.clone(),
                    request,
                    token: Some(send),
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit BranchableSyncRequest");
            }
        }
        x if x == protocols::ALPN_CAR => {
            debug!(peer_id = %peer_id, "CAR dispatch: reading request");
            let request: crate::message::CarFetchRequest =
                protocols::read_message(recv, protocols::MAX_MESSAGE_SIZE).await?;
            debug!(
                peer_id = %peer_id,
                root_cid = %request.root_cid,
                recursive = request.recursive,
                requested_count = request.wanted_cids.len(),
                "CAR dispatch: emitting CarFetchRequest"
            );
            if event_tx
                .send(TransportEvent::CarFetchRequest {
                    peer_id: peer_id.clone(),
                    request,
                    token: Some(send),
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit CarFetchRequest");
            }
        }
        x if x == protocols::ALPN_CAR_RESP => {
            let car_data: Vec<u8> = protocols::read_message(recv, protocols::MAX_CAR_SIZE).await?;
            // Extract the root CID from the CAR headers for event correlation.
            let root_cid = match crate::sync::car::decode_car(&car_data) {
                Ok((roots, _)) => roots.into_iter().next(),
                Err(e) => {
                    warn!("Failed to decode CAR response: {}", e);
                    None
                }
            };
            if let Some(root_cid) = root_cid {
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
            }
        }
        x if x == protocols::ALPN_DOCSYNC_RESP => {
            let reply: crate::message::DocSyncReply =
                protocols::read_message(recv, protocols::MAX_MESSAGE_SIZE).await?;
            debug!(peer_id = %peer_id, "Received doc sync response via fire-and-forget");
            if event_tx
                .send(TransportEvent::DocSyncReply {
                    peer_id: peer_id.clone(),
                    reply,
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit DocSyncReply");
            }
        }
        x if x == protocols::ALPN_BRANCHABLE_RESP => {
            let reply: crate::message::BranchableSyncReply =
                protocols::read_message(recv, protocols::MAX_MESSAGE_SIZE).await?;
            debug!(peer_id = %peer_id, "Received branchable sync response via fire-and-forget");
            if event_tx
                .send(TransportEvent::BranchableSyncReply {
                    peer_id: peer_id.clone(),
                    reply,
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit BranchableSyncReply");
            }
        }
        x if x == protocols::ALPN_SE => {
            let request: crate::message::PushSEArtifactsRequest =
                protocols::read_message(recv, protocols::MAX_MESSAGE_SIZE).await?;
            debug!(
                peer_id = %peer_id,
                collection_id = %request.collection_id,
                artifact_count = request.artifacts.len(),
                "Received SE artifacts"
            );
            // SE artifact processing is handled at the database layer
        }
        _ => {
            debug!("Unknown ALPN: {:?}", String::from_utf8_lossy(alpn));
        }
    }
    Ok(())
}
