//! Incoming connection and stream processing for the iroh endpoint.

use std::collections::HashMap;
use std::sync::Arc;

use iroh::endpoint::Connection;
use iroh::EndpointId;
use iroh_gossip::net::Gossip;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::error::Error;
use crate::message::{Message, PushLogReply};
use crate::transport::{PeerId, TransportEvent};

use super::endpoint::{
    join_peer_to_subscription_senders, track_task, SpawnedTasks, SubscriptionSenders,
};
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
    subscription_senders: &SubscriptionSenders,
    spawned_tasks: &SpawnedTasks,
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
        join_peer_to_subscription_senders(subscription_senders, remote_id).await;
    }

    // Spawn handler for this connection's streams
    let event_tx = event_tx.clone();
    let peer_map = Arc::clone(peer_map);
    let pending_pushlog_replies = Arc::clone(pending_pushlog_replies);
    let spawned_tasks_for_connection = Arc::clone(spawned_tasks);
    let task = tokio::spawn(async move {
        handle_connection_streams(
            connection,
            remote_id,
            conn_alpn,
            event_tx,
            peer_map,
            pending_pushlog_replies,
            &spawned_tasks_for_connection,
        )
        .await;
    });
    track_task(spawned_tasks, task);
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
    spawned_tasks: &SpawnedTasks,
) {
    let peer_id = endpoint_id_to_peer_id(&remote_id);

    while let Ok((send, mut recv)) = connection.accept_bi().await {
        let peer_id = peer_id.clone();
        let event_tx = event_tx.clone();
        let alpn = alpn.clone();
        let pending_pushlog_replies = pending_pushlog_replies.clone();
        let task = tokio::spawn(async move {
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
        track_task(spawned_tasks, task);
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
    spawned_tasks: &SpawnedTasks,
) {
    handle_connection_streams(
        connection,
        remote_id,
        alpn,
        event_tx,
        peer_map,
        pending_pushlog_replies,
        spawned_tasks,
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
            let request: crate::message::PushLogRequest =
                protocols::read_message(recv, protocols::MAX_MESSAGE_SIZE).await?;
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
        x if x == protocols::ALPN_TWOSTREAM_RESP => {
            let reply: PushLogReply =
                protocols::read_message(recv, protocols::MAX_MESSAGE_SIZE).await?;
            let (sender, pending_len_after_remove) = {
                let mut pending = pending_pushlog_replies.lock();
                let sender = pending.remove(&reply.message_id);
                (sender, pending.len())
            };
            if let Some(sender) = sender {
                let _ = sender.send(reply);
            } else {
                warn!(
                    peer_id = %peer_id,
                    message_id = %reply.message_id,
                    pending_reply_count = pending_len_after_remove,
                    "Received unmatched two-stream reply on response protocol"
                );
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
            verify_iroh_message(&request)?;
            ensure_iroh_signed_sender(peer_id, request.sender_id.as_str())?;
            debug!(
                peer_id = %peer_id,
                collection_id = %request.collection_id,
                artifact_count = request.artifacts.len(),
                "Received SE artifacts"
            );
            let data = serde_cbor::to_vec(&request)
                .map_err(|e| crate::error::Error::Codec(e.to_string()))?;
            if event_tx
                .send(TransportEvent::SEArtifactsReceived {
                    peer_id: peer_id.clone(),
                    data,
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit SEArtifactsReceived");
            }
        }
        x if x == protocols::ALPN_SE_QUERY_REQ => {
            let request: crate::message::QuerySEArtifactsRequest =
                protocols::read_message(recv, protocols::MAX_MESSAGE_SIZE).await?;
            verify_iroh_message(&request)?;
            ensure_iroh_signed_sender(peer_id, request.sender_id.as_str())?;
            debug!(
                peer_id = %peer_id,
                message_id = %request.message_id,
                collection_id = %request.collection_id,
                query_count = request.queries.len(),
                "Received SE query request"
            );
            if event_tx
                .send(TransportEvent::SEQueryRequest {
                    peer_id: peer_id.clone(),
                    request,
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit SEQueryRequest");
            }
        }
        x if x == protocols::ALPN_SE_QUERY_RESP => {
            let reply: crate::message::QuerySEArtifactsReply =
                protocols::read_message(recv, protocols::MAX_MESSAGE_SIZE).await?;
            verify_iroh_message(&reply)?;
            ensure_iroh_signed_sender(peer_id, reply.sender_id.as_str())?;
            debug!(
                peer_id = %peer_id,
                message_id = %reply.message_id,
                doc_count = reply.doc_ids.len(),
                "Received SE query response"
            );
            if event_tx
                .send(TransportEvent::SEQueryReply {
                    peer_id: peer_id.clone(),
                    reply,
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit SEQueryReply");
            }
        }
        x if x == protocols::ALPN_MANAGE_REQ => {
            let request: crate::message::ManageRequest =
                protocols::read_message(recv, protocols::MAX_MANAGE_MSG_SIZE).await?;
            verify_iroh_message(&request)?;
            ensure_iroh_signed_sender(peer_id, request.sender_id.as_str())?;
            debug!(
                peer_id = %peer_id,
                message_id = %request.message_id,
                "Received manage request"
            );
            if event_tx
                .send(TransportEvent::ManageRequest {
                    peer_id: peer_id.clone(),
                    request,
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit ManageRequest");
            }
        }
        x if x == protocols::ALPN_MANAGE_RESP => {
            let reply: crate::message::ManageReply =
                protocols::read_message(recv, protocols::MAX_MANAGE_MSG_SIZE).await?;
            verify_iroh_message(&reply)?;
            ensure_iroh_signed_sender(peer_id, reply.sender_id.as_str())?;
            debug!(
                peer_id = %peer_id,
                message_id = %reply.message_id,
                "Received manage reply"
            );
            if event_tx
                .send(TransportEvent::ManageReply {
                    peer_id: peer_id.clone(),
                    reply,
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit ManageReply");
            }
        }
        x if x == protocols::ALPN_MANAGE_QUERY_REQ => {
            let request: crate::message::ManageQueryRequest =
                protocols::read_message(recv, protocols::MAX_MANAGE_MSG_SIZE).await?;
            verify_iroh_message(&request)?;
            ensure_iroh_signed_sender(peer_id, request.sender_id.as_str())?;
            debug!(
                peer_id = %peer_id,
                message_id = %request.message_id,
                "Received manage query request"
            );
            if event_tx
                .send(TransportEvent::ManageQueryRequest {
                    peer_id: peer_id.clone(),
                    request,
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit ManageQueryRequest");
            }
        }
        x if x == protocols::ALPN_MANAGE_QUERY_RESP => {
            let reply: crate::message::ManageQueryReply =
                protocols::read_message(recv, protocols::MAX_MANAGE_MSG_SIZE).await?;
            verify_iroh_message(&reply)?;
            ensure_iroh_signed_sender(peer_id, reply.sender_id.as_str())?;
            debug!(
                peer_id = %peer_id,
                message_id = %reply.message_id,
                "Received manage query reply"
            );
            if event_tx
                .send(TransportEvent::ManageQueryReply {
                    peer_id: peer_id.clone(),
                    reply,
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit ManageQueryReply");
            }
        }
        _ => {
            debug!("Unknown ALPN: {:?}", String::from_utf8_lossy(alpn));
        }
    }
    Ok(())
}

fn ensure_iroh_signed_sender(peer_id: &PeerId, sender_id: &str) -> crate::error::Result<()> {
    if sender_id == peer_id.as_str() {
        Ok(())
    } else {
        Err(crate::error::Error::Transport(format!(
            "transport peer {} did not match signed sender {}",
            peer_id, sender_id
        )))
    }
}

fn verify_iroh_message<M>(msg: &M) -> crate::error::Result<()>
where
    M: Message + serde::Serialize + Clone,
{
    let signature = msg.signature().ok_or(Error::MissingSignature)?;

    let sender_id: iroh::EndpointId = msg
        .sender_id()
        .parse()
        .map_err(|error: iroh::KeyParsingError| Error::InvalidPeerId(error.to_string()))?;

    let pubkey_bytes: [u8; 32] = msg
        .pubkey()
        .try_into()
        .map_err(|_| Error::PublicKeyDecode("expected 32-byte iroh public key".into()))?;
    let pubkey = iroh::PublicKey::from_bytes(&pubkey_bytes)
        .map_err(|error| Error::PublicKeyDecode(error.to_string()))?;

    if pubkey != sender_id {
        return Err(Error::PubkeyPeerIdMismatch);
    }

    let signature = iroh::Signature::try_from(signature).map_err(|_| Error::InvalidSignature)?;
    let mut msg_for_verify = msg.clone();
    msg_for_verify.set_signature(None);
    let bytes =
        serde_cbor::to_vec(&msg_for_verify).map_err(|e| Error::CborSerialization(e.to_string()))?;

    pubkey
        .verify(&bytes, &signature)
        .map_err(|_| Error::InvalidSignature)
}
