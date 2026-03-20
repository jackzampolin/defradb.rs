//! Background event loop owning all iroh state.
//!
//! `IrohEndpoint` runs as a spawned tokio task and processes:
//! - Incoming QUIC connections
//! - Gossip events from iroh-gossip
//! - Commands from the `IrohTransport` facade

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use iroh::address_lookup::{DnsAddressLookup, PkarrPublisher};
use iroh::endpoint::BindOpts;
use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::message::CarFetchRequest;
use crate::replicator::ReplicatorInfo;
use crate::transport::{MessageId, PeerAddr, PeerId, TransportEvent};
use crate::QueryId;

use super::addr::{endpoint_addr_from_parts, endpoint_ticket_string};
use super::command::IrohCommand;
use super::config::{IrohDiscoveryConfig, IrohRelayModeConfig};
use super::peer_map::{endpoint_id_to_peer_id, parse_endpoint_id, PeerMap};
use super::protocols;

/// Timeout for request-response round trips.
///
/// Covers the time from sending the request to receiving the full response.
/// Longer than the fire-and-forget timeout (5 s) because the remote peer
/// needs time to process the request before replying.
const REQUEST_RESPONSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Handle to a gossip topic subscription.
struct TopicSubscription {
    sender: iroh_gossip::api::GossipSender,
    reader_task: JoinHandle<()>,
}

/// Active block sync task.
struct ActiveSync {
    abort_handle: tokio::task::AbortHandle,
}

/// Configuration for creating an `IrohEndpoint`.
pub struct IrohEndpointConfig {
    pub secret_key: SecretKey,
    /// Relay behavior for this endpoint.
    pub relay_mode: IrohRelayModeConfig,
    /// Address publishing / lookup behavior for this endpoint.
    pub discovery: IrohDiscoveryConfig,
    /// UDP port for the QUIC listener. `None` = ephemeral (OS-assigned).
    pub bind_port: Option<u16>,
    /// Bind to a specific IP address. When set, IROH only listens on this
    /// interface — prevents advertising unreachable LAN addresses to peers
    /// on different networks. None = 0.0.0.0 (all interfaces).
    pub bind_addr: Option<std::net::IpAddr>,
}

impl Default for IrohEndpointConfig {
    fn default() -> Self {
        Self {
            secret_key: SecretKey::generate(&mut rand::rng()),
            relay_mode: IrohRelayModeConfig::default(),
            discovery: IrohDiscoveryConfig::default(),
            bind_port: None,
            bind_addr: None,
        }
    }
}

/// Spawn the iroh endpoint background task.
///
/// Returns the command sender, event receiver, and background task handle.
pub async fn spawn_endpoint(
    config: IrohEndpointConfig,
) -> crate::error::Result<(
    mpsc::Sender<IrohCommand>,
    mpsc::Receiver<TransportEvent>,
    JoinHandle<()>,
)> {
    let mut alpns: Vec<Vec<u8>> = protocols::ALL_ALPNS.iter().map(|a| a.to_vec()).collect();
    alpns.push(iroh_gossip::net::GOSSIP_ALPN.to_vec());

    let relay_mode = relay_mode_from_config(&config.relay_mode)?;
    let mut builder = Endpoint::empty_builder()
        .relay_mode(relay_mode)
        .secret_key(config.secret_key.clone())
        .alpns(alpns);
    builder = apply_discovery_config(builder, &config.discovery)?;
    builder = apply_bind_config(builder, config.bind_addr, config.bind_port)?;

    let endpoint = builder.bind().await.map_err(|e| {
        crate::error::Error::Transport(format!("failed to bind iroh endpoint: {}", e))
    })?;

    let gossip = Gossip::builder().spawn(endpoint.clone());

    let (command_tx, command_rx) = mpsc::channel::<IrohCommand>(256);
    let (event_tx, event_rx) = mpsc::channel::<TransportEvent>(256);

    let task = tokio::spawn(run_event_loop(endpoint, gossip, command_rx, event_tx));

    Ok((command_tx, event_rx, task))
}

/// Main event loop processing incoming connections, gossip, and commands.
async fn run_event_loop(
    endpoint: Endpoint,
    gossip: Gossip,
    mut command_rx: mpsc::Receiver<IrohCommand>,
    event_tx: mpsc::Sender<TransportEvent>,
) {
    let peer_map = Arc::new(parking_lot::Mutex::new(PeerMap::new()));
    let mut subscriptions: HashMap<String, TopicSubscription> = HashMap::new();
    let mut replicators: HashMap<String, ReplicatorInfo> = HashMap::new();
    let mut active_syncs: HashMap<u64, ActiveSync> = HashMap::new();
    let mut next_query_id: u64 = 1;

    // Emit Listening event with our endpoint address
    let addr_str = format!("iroh://{}", endpoint.id());
    if event_tx
        .send(TransportEvent::Listening(PeerAddr::new(addr_str)))
        .await
        .is_err()
    {
        warn!("Event channel closed, cannot emit Listening event");
        return;
    }

    loop {
        tokio::select! {
            incoming = endpoint.accept() => {
                match incoming {
                    Some(incoming) => {
                        handle_incoming(
                            incoming,
                            &gossip,
                            &peer_map,
                            &subscriptions,
                            &event_tx,
                        ).await;
                    }
                    None => break,
                }
            }
            Some(cmd) = command_rx.recv() => {
                let should_shutdown = handle_command(
                    cmd,
                    &endpoint,
                    &gossip,
                    &peer_map,
                    &mut subscriptions,
                    &mut replicators,
                    &mut active_syncs,
                    &mut next_query_id,
                    &event_tx,
                ).await;
                if should_shutdown {
                    break;
                }
            }
            else => break,
        }
    }

    // Clean up
    for (_, sub) in subscriptions.drain() {
        sub.reader_task.abort();
    }
    for (_, sync) in active_syncs.drain() {
        sync.abort_handle.abort();
    }
    endpoint.close().await;
    info!("Iroh endpoint shut down");
}

/// Handle an incoming QUIC connection.
async fn handle_incoming(
    incoming: iroh::endpoint::Incoming,
    gossip: &Gossip,
    peer_map: &Arc<parking_lot::Mutex<PeerMap>>,
    subscriptions: &HashMap<String, TopicSubscription>,
    event_tx: &mpsc::Sender<TransportEvent>,
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
                warn!("Failed to complete connection handshake: {}", e);
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
    tokio::spawn(async move {
        handle_connection_streams(connection, remote_id, conn_alpn, event_tx, peer_map).await;
    });
}

/// Process streams on an accepted connection, dispatching by ALPN.
///
/// Emits `PeerDisconnected` only when the last connection for this peer closes.
async fn handle_connection_streams(
    connection: Connection,
    remote_id: EndpointId,
    alpn: Vec<u8>,
    event_tx: mpsc::Sender<TransportEvent>,
    peer_map: Arc<parking_lot::Mutex<PeerMap>>,
) {
    let peer_id = endpoint_id_to_peer_id(&remote_id);

    while let Ok((send, mut recv)) = connection.accept_bi().await {
        let peer_id = peer_id.clone();
        let event_tx = event_tx.clone();
        let alpn = alpn.clone();
        tokio::spawn(async move {
            if let Err(e) = dispatch_stream(&alpn, &peer_id, send, &mut recv, &event_tx).await {
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

/// Dispatch a stream based on the connection ALPN.
async fn dispatch_stream(
    alpn: &[u8],
    peer_id: &PeerId,
    send: iroh::endpoint::SendStream,
    recv: &mut iroh::endpoint::RecvStream,
    event_tx: &mpsc::Sender<TransportEvent>,
) -> crate::error::Result<()> {
    match alpn {
        x if x == protocols::ALPN_PUSHLOG => {
            let request: crate::message::PushLogRequest =
                protocols::read_message(recv, protocols::MAX_MESSAGE_SIZE).await?;
            let token = crate::transport::ResponseToken::new(send);
            if event_tx
                .send(TransportEvent::PushLogRequest {
                    peer_id: peer_id.clone(),
                    request,
                    token,
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
            let token = crate::transport::ResponseToken::new(send);
            if event_tx
                .send(TransportEvent::TwoStreamRequest {
                    peer_id: peer_id.clone(),
                    request,
                    token: Some(token),
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
            let token = crate::transport::ResponseToken::new(send);
            if event_tx
                .send(TransportEvent::DocSyncRequest {
                    peer_id: peer_id.clone(),
                    request,
                    token: Some(token),
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
            let token = crate::transport::ResponseToken::new(send);
            if event_tx
                .send(TransportEvent::BranchableSyncRequest {
                    peer_id: peer_id.clone(),
                    request,
                    token: Some(token),
                })
                .await
                .is_err()
            {
                warn!("Event channel closed, cannot emit BranchableSyncRequest");
            }
        }
        x if x == protocols::ALPN_CAR => {
            debug!(peer_id = %peer_id, "CAR dispatch: reading request");
            let request: CarFetchRequest =
                protocols::read_message(recv, protocols::MAX_MESSAGE_SIZE).await?;
            debug!(
                peer_id = %peer_id,
                root_cid = %request.root_cid,
                recursive = request.recursive,
                requested_count = request.wanted_cids.len(),
                "CAR dispatch: emitting CarFetchRequest"
            );
            let token = crate::transport::ResponseToken::new(send);
            if event_tx
                .send(TransportEvent::CarFetchRequest {
                    peer_id: peer_id.clone(),
                    request,
                    token: Some(token),
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

/// Handle a command from `IrohTransport`.
///
/// Returns `true` if the event loop should shut down.
#[allow(clippy::too_many_arguments)]
async fn handle_command(
    cmd: IrohCommand,
    endpoint: &Endpoint,
    gossip: &Gossip,
    peer_map: &Arc<parking_lot::Mutex<PeerMap>>,
    subscriptions: &mut HashMap<String, TopicSubscription>,
    replicators: &mut HashMap<String, ReplicatorInfo>,
    active_syncs: &mut HashMap<u64, ActiveSync>,
    next_query_id: &mut u64,
    event_tx: &mpsc::Sender<TransportEvent>,
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
            let result = async {
                protocols::write_message(&mut send_stream, &reply_msg).await?;
                send_stream.finish().map_err(|e| {
                    crate::error::Error::Transport(format!("failed to finish stream: {}", e))
                })?;
                Ok(())
            }
            .await;
            let _ = reply.send(result);
        }
        IrohCommand::SendTwoStreamRequest {
            peer_id,
            request,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let result = handle_request_response(
                endpoint,
                &peer_id,
                protocols::ALPN_TWOSTREAM,
                &request,
                direct_addr,
            )
            .await;
            let _ = reply.send(result);
        }
        IrohCommand::SendTwoStreamResponse {
            peer_id,
            reply_msg,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let result = handle_fire_and_forget(
                endpoint,
                &peer_id,
                protocols::ALPN_TWOSTREAM,
                &reply_msg,
                direct_addr,
            )
            .await;
            let _ = reply.send(result);
        }
        IrohCommand::SendDocSyncRequest {
            peer_id,
            request,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let result: crate::error::Result<crate::message::DocSyncReply> =
                handle_request_response(
                    endpoint,
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
        }
        IrohCommand::SendBranchableSyncRequest {
            peer_id,
            request,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let result: crate::error::Result<crate::message::BranchableSyncReply> =
                handle_request_response(
                    endpoint,
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
        }
        IrohCommand::SendDocSyncResponse {
            peer_id,
            reply_msg,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let result = handle_fire_and_forget(
                endpoint,
                &peer_id,
                protocols::ALPN_DOCSYNC_RESP,
                &reply_msg,
                direct_addr,
            )
            .await;
            let _ = reply.send(result);
        }
        IrohCommand::SendBranchableSyncResponse {
            peer_id,
            reply_msg,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let result = handle_fire_and_forget(
                endpoint,
                &peer_id,
                protocols::ALPN_BRANCHABLE_RESP,
                &reply_msg,
                direct_addr,
            )
            .await;
            let _ = reply.send(result);
        }
        IrohCommand::SendDocSyncResponseToken {
            mut send_stream,
            reply_msg,
            reply,
        } => {
            let result = async {
                protocols::write_message(&mut send_stream, &reply_msg).await?;
                send_stream.finish().map_err(|e| {
                    crate::error::Error::Transport(format!("failed to finish stream: {}", e))
                })?;
                Ok(())
            }
            .await;
            let _ = reply.send(result);
        }
        IrohCommand::SendBranchableSyncResponseToken {
            mut send_stream,
            reply_msg,
            reply,
        } => {
            let result = async {
                protocols::write_message(&mut send_stream, &reply_msg).await?;
                send_stream.finish().map_err(|e| {
                    crate::error::Error::Transport(format!("failed to finish stream: {}", e))
                })?;
                Ok(())
            }
            .await;
            let _ = reply.send(result);
        }
        IrohCommand::SendCarRequest {
            peer_id,
            root_cid,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let request = CarFetchRequest::full_dag(root_cid);
            let result = handle_fire_and_forget(
                endpoint,
                &peer_id,
                protocols::ALPN_CAR,
                &request,
                direct_addr,
            )
            .await;
            let _ = reply.send(result);
        }
        IrohCommand::SendCarResponse {
            peer_id,
            car_data,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let result = handle_fire_and_forget(
                endpoint,
                &peer_id,
                protocols::ALPN_CAR_RESP,
                &car_data,
                direct_addr,
            )
            .await;
            let _ = reply.send(result);
        }
        IrohCommand::SendSEArtifacts {
            peer_id,
            request,
            reply,
        } => {
            let direct_addr = peer_direct_addr(peer_map, &peer_id);
            let result = handle_fire_and_forget(
                endpoint,
                &peer_id,
                protocols::ALPN_SE,
                &request,
                direct_addr,
            )
            .await;
            let _ = reply.send(result);
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
    event_tx: &mpsc::Sender<TransportEvent>,
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
        handle_connection_streams(connection, endpoint_id, conn_alpn, event_tx, peer_map).await;
    });

    Ok(())
}

/// Subscribe to a gossip topic.
///
/// Passes all currently connected peers as initial neighbors so gossip messages
/// are immediately deliverable. iroh-gossip requires explicit neighbors unlike
/// libp2p-gossipsub which discovers them automatically.
async fn handle_subscribe(
    gossip: &Gossip,
    subscriptions: &mut HashMap<String, TopicSubscription>,
    peer_map: &Arc<parking_lot::Mutex<PeerMap>>,
    topic: crate::topics::DefraTopic,
    event_tx: &mpsc::Sender<TransportEvent>,
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
                                warn!("Failed to decode gossip message: {}", e);
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

/// Join a newly connected peer into all active gossip topic subscriptions.
///
/// iroh-gossip subscriptions are created with an explicit neighbor list.
/// When a new peer connects after subscription, we add them as a neighbor
/// so they can receive (and send us) gossip messages on all subscribed topics.
async fn join_peer_to_subscriptions(
    subscriptions: &HashMap<String, TopicSubscription>,
    endpoint_id: iroh::EndpointId,
) {
    for (topic, sub) in subscriptions.iter() {
        if let Err(e) = sub.sender.join_peers(vec![endpoint_id]).await {
            debug!(
                topic = %topic,
                peer = %endpoint_id,
                "Failed to add new peer to gossip topic: {}",
                e
            );
        }
    }
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
    event_tx: &mpsc::Sender<TransportEvent>,
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

/// Send a request and wait for a response (bidirectional stream).
///
/// `direct_addr` is an optional cached socket address for the peer; when provided it is
/// added to the `EndpointAddr` so iroh can connect directly without relay discovery.
async fn handle_request_response<Req, Resp>(
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

/// Look up the cached direct socket address for a peer from the peer map.
fn peer_direct_addr(
    peer_map: &Arc<parking_lot::Mutex<PeerMap>>,
    peer_id: &PeerId,
) -> Option<std::net::SocketAddr> {
    let id = parse_endpoint_id(peer_id).ok()?;
    let map = peer_map.lock();
    map.get(&id).and_then(|info| info.remote_addr)
}

/// Send a message without expecting a response.
///
/// Keeps the connection alive until the peer closes their stream, ensuring
/// the message is received before CONNECTION_CLOSE is sent.
async fn handle_fire_and_forget<T: serde::Serialize>(
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
/// Returns `Ok(())` if the fetch succeeded and a `CarFetchResponse` was emitted.
async fn try_fetch_from_provider(
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
async fn handle_block_sync(
    endpoint: Endpoint,
    peer_map: Arc<parking_lot::Mutex<PeerMap>>,
    query_id: QueryId,
    root: cid::Cid,
    providers: Vec<PeerId>,
    missing: Vec<cid::Cid>,
    event_tx: mpsc::Sender<TransportEvent>,
) {
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
        let peer_map = Arc::clone(&peer_map);
        let event_tx = event_tx.clone();
        let provider = provider.clone();
        let request = request.clone();
        tasks.push(tokio::spawn(async move {
            let direct_addr = peer_direct_addr(&peer_map, &provider);
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

fn relay_mode_from_config(config: &IrohRelayModeConfig) -> crate::error::Result<iroh::RelayMode> {
    match config {
        IrohRelayModeConfig::Default => Ok(iroh::endpoint::default_relay_mode()),
        IrohRelayModeConfig::Disabled => Ok(iroh::RelayMode::Disabled),
        IrohRelayModeConfig::Custom(urls) => {
            let relay_map = iroh::RelayMap::try_from_iter(urls.iter().map(String::as_str))
                .map_err(|e| {
                    crate::error::Error::Transport(format!("invalid relay URL list: {}", e))
                })?;
            Ok(iroh::RelayMode::Custom(relay_map))
        }
    }
}

fn apply_discovery_config(
    mut builder: iroh::endpoint::Builder,
    config: &IrohDiscoveryConfig,
) -> crate::error::Result<iroh::endpoint::Builder> {
    builder = match config {
        IrohDiscoveryConfig::N0 => builder
            .address_lookup(PkarrPublisher::n0_dns())
            .address_lookup(DnsAddressLookup::n0_dns()),
        IrohDiscoveryConfig::Disabled => builder.clear_address_lookup(),
        IrohDiscoveryConfig::CustomDns {
            origin_domain,
            pkarr_relay_url,
        } => {
            let pkarr_relay = pkarr_relay_url.parse().map_err(|e| {
                crate::error::Error::Transport(format!(
                    "invalid pkarr relay URL '{}': {}",
                    pkarr_relay_url, e
                ))
            })?;
            builder
                .address_lookup(PkarrPublisher::builder(pkarr_relay))
                .address_lookup(DnsAddressLookup::builder(origin_domain.clone()))
        }
    };

    Ok(builder)
}

fn apply_bind_config(
    mut builder: iroh::endpoint::Builder,
    bind_addr: Option<std::net::IpAddr>,
    bind_port: Option<u16>,
) -> crate::error::Result<iroh::endpoint::Builder> {
    let bind_error =
        |error| crate::error::Error::Transport(format!("invalid bind addr: {}", error));

    match (bind_addr, bind_port) {
        (Some(ip), port) => {
            builder = builder
                .bind_addr(std::net::SocketAddr::new(ip, port.unwrap_or(0)))
                .map_err(bind_error)?;
        }
        (None, Some(port)) => {
            builder = builder.clear_ip_transports();
            builder = builder
                .bind_addr(std::net::SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                    port,
                ))
                .map_err(bind_error)?;
            builder = builder
                .bind_addr_with_opts(
                    std::net::SocketAddr::new(
                        std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
                        port,
                    ),
                    BindOpts::default().set_is_required(false),
                )
                .map_err(bind_error)?;
        }
        (None, None) => {}
    }

    Ok(builder)
}

/// Hash a topic string to an iroh-gossip `TopicId`.
fn topic_to_id(topic: &str) -> TopicId {
    let hash = blake3::hash(topic.as_bytes());
    TopicId::from(*hash.as_bytes())
}
