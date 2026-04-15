//! Command handlers for the iroh endpoint event loop.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use iroh::Endpoint;
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::message::CarFetchRequest;
use crate::replicator::ReplicatorInfo;
use crate::transport::{MessageId, PeerAddr, PeerId, TransportEvent};
use crate::QueryId;

use super::addr::{endpoint_addr_from_parts, endpoint_ticket_string};
use super::command::IrohCommand;
use super::endpoint::{
    join_peer_to_subscriptions, peer_direct_addr, ActiveSync, TopicSubscription,
};
use super::endpoint_rpc::{handle_block_sync, handle_fire_and_forget, handle_request_response};
use super::peer_map::{endpoint_id_to_peer_id, parse_endpoint_id, PeerMap};
use super::protocols;

/// Handle a command from `IrohTransport`.
///
/// Returns `true` if the event loop should shut down.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_command(
    cmd: IrohCommand,
    endpoint: &Endpoint,
    gossip: &Gossip,
    peer_map: &Arc<parking_lot::Mutex<PeerMap>>,
    subscriptions: &mut HashMap<String, TopicSubscription>,
    replicators: &mut HashMap<String, ReplicatorInfo>,
    active_syncs: &mut HashMap<u64, ActiveSync>,
    next_query_id: &mut u64,
    event_tx: &mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
) -> bool {
    match cmd {
        IrohCommand::Dial {
            peer_id,
            addrs,
            reply,
        } => {
            let result =
                handle_dial(endpoint, peer_map, subscriptions, &peer_id, addrs, event_tx).await;
            let _ = reply.send(result);
        }
        IrohCommand::Listen { addr: _, reply } => {
            // iroh endpoint is already listening after bind
            let _ = reply.send(Ok(()));
        }
        IrohCommand::ConnectedPeers { reply } => {
            let _ = reply.send(Ok(peer_map.lock().connected_peers()));
        }
        IrohCommand::ListenAddresses { reply } => {
            let endpoint_addr = endpoint.addr();
            let mut addrs = vec![
                PeerAddr::new(format!("iroh://{}", endpoint.id())),
                PeerAddr::new(endpoint_ticket_string(&endpoint_addr)),
            ];
            for socket_addr in endpoint_addr.ip_addrs() {
                let addr = PeerAddr::new(socket_addr.to_string());
                if !addrs.contains(&addr) {
                    addrs.push(addr);
                }
            }
            let _ = reply.send(Ok(addrs));
        }
        IrohCommand::PeerAddresses { reply } => {
            let _ = reply.send(Ok(peer_map.lock().peer_addresses()));
        }
        IrohCommand::NetworkChange { reply } => {
            endpoint.network_change().await;
            let _ = reply.send(Ok(()));
        }
        IrohCommand::Subscribe { topic, reply } => {
            let result = handle_subscribe(gossip, subscriptions, peer_map, topic, event_tx).await;
            let _ = reply.send(result);
        }
        IrohCommand::Unsubscribe { topic, reply } => {
            let topic_str = topic.to_string();
            if let Some(sub) = subscriptions.remove(&topic_str) {
                sub.reader_task.abort();
                let _ = reply.send(Ok(true));
            } else {
                let _ = reply.send(Ok(false));
            }
        }
        IrohCommand::Publish { topic, msg, reply } => {
            let result =
                handle_publish(gossip, subscriptions, peer_map, topic, &msg, event_tx).await;
            let _ = reply.send(result);
        }
        IrohCommand::SendPushLogResponse {
            mut send_stream,
            reply_msg,
            reply,
        } => {
            tokio::spawn(async move {
                let result = async {
                    protocols::write_message(&mut send_stream, &reply_msg).await?;
                    send_stream.finish().map_err(|e| {
                        crate::error::Error::Transport(format!("failed to finish stream: {}", e))
                    })?;
                    Ok(())
                }
                .await;
                let _ = reply.send(result);
            });
        }
        IrohCommand::SendTwoStreamRequest {
            peer_id,
            request,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                let result = handle_request_response(
                    &endpoint,
                    &peer_id,
                    protocols::ALPN_TWOSTREAM,
                    &request,
                    direct_addr,
                )
                .await;
                let _ = reply.send(result);
            });
        }
        IrohCommand::SendTwoStreamResponse {
            peer_id,
            reply_msg,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                let result = handle_fire_and_forget(
                    &endpoint,
                    &peer_id,
                    protocols::ALPN_TWOSTREAM,
                    &reply_msg,
                    direct_addr,
                )
                .await;
                let _ = reply.send(result);
            });
        }
        IrohCommand::SendDocSyncRequest {
            peer_id,
            request,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let endpoint = endpoint.clone();
            let event_tx = event_tx.clone();
            tokio::spawn(async move {
                let result: crate::error::Result<crate::message::DocSyncReply> =
                    handle_request_response(
                        &endpoint,
                        &peer_id,
                        protocols::ALPN_DOCSYNC,
                        &request,
                        direct_addr,
                    )
                    .await;
                match result {
                    Ok(doc_reply) => {
                        let _ = event_tx
                            .send(TransportEvent::DocSyncReply {
                                peer_id,
                                reply: doc_reply,
                            })
                            .await;
                        let _ = reply.send(Ok(()));
                    }
                    Err(e) => {
                        let _ = reply.send(Err(e));
                    }
                }
            });
        }
        IrohCommand::SendBranchableSyncRequest {
            peer_id,
            request,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let endpoint = endpoint.clone();
            let event_tx = event_tx.clone();
            tokio::spawn(async move {
                let result: crate::error::Result<crate::message::BranchableSyncReply> =
                    handle_request_response(
                        &endpoint,
                        &peer_id,
                        protocols::ALPN_BRANCHABLE,
                        &request,
                        direct_addr,
                    )
                    .await;
                match result {
                    Ok(br_reply) => {
                        let _ = event_tx
                            .send(TransportEvent::BranchableSyncReply {
                                peer_id,
                                reply: br_reply,
                            })
                            .await;
                        let _ = reply.send(Ok(()));
                    }
                    Err(e) => {
                        let _ = reply.send(Err(e));
                    }
                }
            });
        }
        IrohCommand::SendDocSyncResponse {
            peer_id,
            reply_msg,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                let result = handle_fire_and_forget(
                    &endpoint,
                    &peer_id,
                    protocols::ALPN_DOCSYNC_RESP,
                    &reply_msg,
                    direct_addr,
                )
                .await;
                let _ = reply.send(result);
            });
        }
        IrohCommand::SendBranchableSyncResponse {
            peer_id,
            reply_msg,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                let result = handle_fire_and_forget(
                    &endpoint,
                    &peer_id,
                    protocols::ALPN_BRANCHABLE_RESP,
                    &reply_msg,
                    direct_addr,
                )
                .await;
                let _ = reply.send(result);
            });
        }
        IrohCommand::SendDocSyncResponseToken {
            mut send_stream,
            reply_msg,
            reply,
        } => {
            tokio::spawn(async move {
                let result = async {
                    protocols::write_message(&mut send_stream, &reply_msg).await?;
                    send_stream.finish().map_err(|e| {
                        crate::error::Error::Transport(format!("failed to finish stream: {}", e))
                    })?;
                    Ok(())
                }
                .await;
                let _ = reply.send(result);
            });
        }
        IrohCommand::SendBranchableSyncResponseToken {
            mut send_stream,
            reply_msg,
            reply,
        } => {
            tokio::spawn(async move {
                let result = async {
                    protocols::write_message(&mut send_stream, &reply_msg).await?;
                    send_stream.finish().map_err(|e| {
                        crate::error::Error::Transport(format!("failed to finish stream: {}", e))
                    })?;
                    Ok(())
                }
                .await;
                let _ = reply.send(result);
            });
        }
        IrohCommand::SendCarRequest {
            peer_id,
            root_cid,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let request = CarFetchRequest::full_dag(root_cid);
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                let result = handle_fire_and_forget(
                    &endpoint,
                    &peer_id,
                    protocols::ALPN_CAR,
                    &request,
                    direct_addr,
                )
                .await;
                let _ = reply.send(result);
            });
        }
        IrohCommand::SendCarResponse {
            peer_id,
            car_data,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                let result = handle_fire_and_forget(
                    &endpoint,
                    &peer_id,
                    protocols::ALPN_CAR_RESP,
                    &car_data,
                    direct_addr,
                )
                .await;
                let _ = reply.send(result);
            });
        }
        IrohCommand::SendSEArtifacts {
            peer_id,
            request,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                let result = handle_fire_and_forget(
                    &endpoint,
                    &peer_id,
                    protocols::ALPN_SE,
                    &request,
                    direct_addr,
                )
                .await;
                let _ = reply.send(result);
            });
        }
        IrohCommand::SyncBlocks {
            root,
            providers,
            missing,
            reply,
        } => {
            let query_id = QueryId(*next_query_id);
            *next_query_id += 1;

            let endpoint = endpoint.clone();
            let peer_map = Arc::clone(peer_map);
            let event_tx = event_tx.clone();
            let task = tokio::spawn(async move {
                handle_block_sync(
                    endpoint, peer_map, query_id, root, providers, missing, event_tx,
                )
                .await;
            });
            active_syncs.insert(
                query_id.0,
                ActiveSync {
                    abort_handle: task.abort_handle(),
                },
            );
            let _ = reply.send(Ok(query_id));
        }
        IrohCommand::CancelSync { query_id, reply } => {
            if let Some(sync) = active_syncs.remove(&query_id.0) {
                sync.abort_handle.abort();
                let _ = reply.send(Ok(true));
            } else {
                let _ = reply.send(Ok(false));
            }
        }
        IrohCommand::CreateReplicator {
            peer_id,
            collections,
            reply,
        } => {
            let info = ReplicatorInfo::from_raw(peer_id.to_string(), collections, Vec::new());
            replicators.insert(peer_id.to_string(), info);
            let _ = reply.send(Ok(()));
        }
        IrohCommand::DeleteReplicator { peer_id, reply } => {
            replicators.remove(peer_id.as_str());
            let _ = reply.send(Ok(()));
        }
        IrohCommand::ListReplicators { reply } => {
            let list: Vec<ReplicatorInfo> = replicators.values().cloned().collect();
            let _ = reply.send(Ok(list));
        }
        IrohCommand::GetReplicator { peer_id, reply } => {
            let info = replicators.get(peer_id.as_str()).cloned();
            let _ = reply.send(Ok(info));
        }
        IrohCommand::RemoveReplicatorCollections {
            peer_id,
            collections,
            reply,
        } => {
            if let Some(info) = replicators.get_mut(peer_id.as_str()) {
                info.collections.retain(|c| !collections.contains(c));
                if info.collections.is_empty() {
                    replicators.remove(peer_id.as_str());
                    let _ = reply.send(Ok(true));
                } else {
                    let _ = reply.send(Ok(false));
                }
            } else {
                let _ = reply.send(Ok(false));
            }
        }
        IrohCommand::Shutdown { reply } => {
            let _ = reply.send(Ok(()));
            return true;
        }
    }
    false
}

/// Dial a peer by EndpointId.
///
/// Keeps the connection alive by spawning a stream handler task.
async fn handle_dial(
    endpoint: &Endpoint,
    peer_map: &Arc<parking_lot::Mutex<PeerMap>>,
    subscriptions: &HashMap<String, TopicSubscription>,
    peer_id: &PeerId,
    addrs: Vec<PeerAddr>,
    event_tx: &mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
) -> crate::error::Result<()> {
    let endpoint_id = parse_endpoint_id(peer_id)?;
    let endpoint_addr = endpoint_addr_from_parts(peer_id, &addrs)?;

    let direct_addresses: Vec<std::net::SocketAddr> = addrs
        .iter()
        .filter_map(|a| a.as_str().parse().ok())
        .collect();

    let connection = endpoint
        .connect(endpoint_addr, protocols::ALPN_PUSHLOG)
        .await
        .map_err(|e| crate::error::Error::Dial(e.to_string()))?;

    let conn_alpn = connection.alpn().to_vec();

    let is_new = peer_map
        .lock()
        .increment_connections(endpoint_id, direct_addresses.first().copied());

    if is_new
        && event_tx
            .send(TransportEvent::PeerConnected(peer_id.clone()))
            .await
            .is_err()
    {
        warn!("Event channel closed, cannot emit PeerConnected");
    }

    if is_new {
        join_peer_to_subscriptions(subscriptions, endpoint_id).await;
    }

    // Keep connection alive by spawning a handler for incoming streams.
    let event_tx = event_tx.clone();
    let peer_map = Arc::clone(peer_map);
    tokio::spawn(async move {
        super::endpoint_streams::handle_connection_streams_from_dial(
            connection,
            endpoint_id,
            conn_alpn,
            event_tx,
            peer_map,
        )
        .await;
    });

    Ok(())
}

/// Subscribe to a gossip topic.
///
/// Passes all currently connected peers as initial neighbors so gossip messages
/// are immediately deliverable. iroh-gossip requires explicit neighbors unlike
/// libp2p-gossipsub which discovers them automatically.
pub(super) async fn handle_subscribe(
    gossip: &Gossip,
    subscriptions: &mut HashMap<String, TopicSubscription>,
    peer_map: &Arc<parking_lot::Mutex<PeerMap>>,
    topic: crate::topics::DefraTopic,
    event_tx: &mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
) -> crate::error::Result<bool> {
    use futures::StreamExt;

    let topic_str = topic.to_string();
    if subscriptions.contains_key(&topic_str) {
        return Ok(false);
    }

    let topic_id = topic_to_id(&topic_str);
    let initial_peers: Vec<iroh::EndpointId> = peer_map.lock().endpoint_ids().collect();
    let gossip_topic = gossip
        .subscribe(topic_id, initial_peers)
        .await
        .map_err(|e| crate::error::Error::GossipSubSubscription(e.to_string()))?;

    let (sender, mut receiver) = gossip_topic.split();

    let event_tx = event_tx.clone();
    let topic_str_clone = topic_str.clone();
    let reader_task = tokio::spawn(async move {
        while let Some(result) = receiver.next().await {
            match result {
                Ok(event) => match event {
                    iroh_gossip::api::Event::Received(msg) => {
                        match postcard::from_bytes::<crate::message::PushLogBroadcast>(&msg.content)
                        {
                            Ok(broadcast) => {
                                let sender_peer_id = endpoint_id_to_peer_id(&msg.delivered_from);
                                let msg_id = MessageId::new(uuid::Uuid::new_v4().to_string());
                                if event_tx
                                    .send(TransportEvent::GossipMessage {
                                        propagation_source: sender_peer_id,
                                        message_id: msg_id,
                                        topic: topic_str_clone.clone(),
                                        message: broadcast,
                                    })
                                    .await
                                    .is_err()
                                {
                                    debug!("Event channel closed, stopping gossip reader");
                                    break;
                                }
                            }
                            Err(e) => {
                                // Exponential-backoff sampling: warn on the
                                // 1st, 2nd, 4th, 8th... occurrence; remainder
                                // at debug. Counter is process-global and
                                // surfaced via SyncDiagnostics (issue #858).
                                let count = crate::sync::record_gossip_decode_failure();
                                if count == 1 || count.is_power_of_two() {
                                    warn!(
                                        total_failures = count,
                                        error = %e,
                                        "Failed to decode gossip message \
                                         (version skew or malformed sender?)"
                                    );
                                } else {
                                    debug!(
                                        total_failures = count,
                                        error = %e,
                                        "Failed to decode gossip message"
                                    );
                                }
                            }
                        }
                    }
                    iroh_gossip::api::Event::NeighborUp(id) => {
                        if event_tx
                            .send(TransportEvent::PeerSubscribed {
                                peer_id: endpoint_id_to_peer_id(&id),
                                topic: topic_str_clone.clone(),
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    iroh_gossip::api::Event::NeighborDown(id) => {
                        if event_tx
                            .send(TransportEvent::PeerUnsubscribed {
                                peer_id: endpoint_id_to_peer_id(&id),
                                topic: topic_str_clone.clone(),
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    iroh_gossip::api::Event::Lagged => {
                        warn!(
                            topic = %topic_str_clone,
                            "Gossip lagged — some messages were missed"
                        );
                    }
                },
                Err(e) => {
                    debug!("Gossip receiver error: {}", e);
                    break;
                }
            }
        }
    });

    subscriptions.insert(
        topic_str,
        TopicSubscription {
            sender,
            reader_task,
        },
    );
    Ok(true)
}

/// Publish a message on a gossip topic.
///
/// If the topic is not yet subscribed, lazily subscribes first (matching Go gossipsub
/// behavior where publishing to a topic implicitly joins it). This is needed for
/// document-level topics which are not subscribed at startup.
async fn handle_publish(
    gossip: &Gossip,
    subscriptions: &mut HashMap<String, TopicSubscription>,
    peer_map: &Arc<parking_lot::Mutex<PeerMap>>,
    topic: crate::topics::DefraTopic,
    msg: &crate::message::PushLogBroadcast,
    event_tx: &mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
) -> crate::error::Result<MessageId> {
    let topic_str = topic.to_string();

    // Auto-subscribe to the topic if not already subscribed
    if !subscriptions.contains_key(&topic_str) {
        debug!(topic = %topic_str, "Auto-subscribing to topic on first publish");
        handle_subscribe(gossip, subscriptions, peer_map, topic, event_tx).await?;
    }

    let sub = subscriptions
        .get_mut(&topic_str)
        .ok_or_else(|| crate::error::Error::InvalidTopic(topic_str.clone()))?;

    let payload =
        postcard::to_allocvec(msg).map_err(|e| crate::error::Error::Codec(e.to_string()))?;

    sub.sender
        .broadcast(Bytes::from(payload))
        .await
        .map_err(|e| crate::error::Error::GossipSubPublish(e.to_string()))?;

    Ok(MessageId::new(uuid::Uuid::new_v4().to_string()))
}

/// Hash a topic string to an iroh-gossip `TopicId`.
fn topic_to_id(topic: &str) -> TopicId {
    let hash = blake3::hash(topic.as_bytes());
    TopicId::from(*hash.as_bytes())
}
