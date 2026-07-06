//! Background event loop owning all iroh state.
//!
//! `IrohEndpoint` runs as a spawned tokio task and processes:
//! - Incoming QUIC connections
//! - Gossip events from iroh-gossip
//! - Commands from the `IrohTransport` facade

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use iroh::{Endpoint, EndpointId};
use iroh_gossip::net::Gossip;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::bitswap::ReplicatorRegistry;
use crate::message::PushLogReply;
use crate::transport::{PeerAddr, PeerId, TransportEvent};

use super::command::IrohCommand;
use super::endpoint_commands::handle_command;
use super::endpoint_config::{
    apply_bind_config, apply_discovery_config, relay_mode_from_config, IrohEndpointConfig,
};
use super::endpoint_rpc::ConnectionCache;
use super::endpoint_streams::handle_incoming;
use super::peer_map::{parse_endpoint_id, PeerMap};
use super::protocols;

const MAX_COMMAND_BATCH: usize = 16;

/// Handle to a gossip topic subscription.
pub(super) struct TopicSubscription {
    pub(super) sender: iroh_gossip::api::GossipSender,
    pub(super) reader_task: JoinHandle<()>,
    pub(super) neighbors: Arc<parking_lot::Mutex<HashSet<EndpointId>>>,
}

pub(super) type SubscriptionSenders = Vec<(String, iroh_gossip::api::GossipSender)>;

/// Active block sync task.
pub(super) struct ActiveSync {
    pub(super) abort_handle: tokio::task::AbortHandle,
}

pub(super) type SpawnedTasks = Arc<parking_lot::Mutex<Vec<JoinHandle<()>>>>;

pub(super) fn track_task(spawned_tasks: &SpawnedTasks, task: JoinHandle<()>) {
    spawned_tasks.lock().push(task);
}

async fn shutdown_tracked_tasks(spawned_tasks: SpawnedTasks) {
    let mut tasks = {
        let mut guard = spawned_tasks.lock();
        std::mem::take(&mut *guard)
    };

    if tasks.is_empty() {
        return;
    }

    debug!(
        task_count = tasks.len(),
        "Aborting tracked Iroh spawned tasks during shutdown"
    );
    for task in &tasks {
        task.abort();
    }

    let shutdown_start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);
    while let Some(task) = tasks.pop() {
        let remaining = timeout.saturating_sub(shutdown_start.elapsed());
        if remaining.is_zero() {
            warn!(
                remaining_tasks = tasks.len() + 1,
                "Timed out draining tracked Iroh spawned tasks during shutdown"
            );
            break;
        }
        match tokio::time::timeout(remaining, task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if error.is_cancelled() => {}
            Ok(Err(error)) => {
                debug!(%error, "Tracked Iroh spawned task failed during shutdown");
            }
            Err(_) => {
                warn!(
                    remaining_tasks = tasks.len() + 1,
                    "Timed out waiting for tracked Iroh spawned task during shutdown"
                );
                break;
            }
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
    mpsc::Receiver<TransportEvent<iroh::endpoint::SendStream>>,
    Arc<ReplicatorRegistry>,
    JoinHandle<()>,
)> {
    let mut alpns: Vec<Vec<u8>> = protocols::ALL_ALPNS.iter().map(|a| a.to_vec()).collect();
    alpns.push(iroh_gossip::net::GOSSIP_ALPN.to_vec());

    let relay_mode = relay_mode_from_config(&config.relay_mode)?;
    let relay_urls: Vec<String> = relay_mode
        .relay_map()
        .urls::<Vec<_>>()
        .iter()
        .map(|u| u.to_string())
        .collect();
    tracing::info!(?relay_urls, "iroh endpoint relay configuration");
    let mut builder = Endpoint::builder(iroh::endpoint::presets::Minimal)
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
    let (event_tx, event_rx) = mpsc::channel::<TransportEvent<iroh::endpoint::SendStream>>(256);
    let replicators = Arc::new(ReplicatorRegistry::new());

    let task = tokio::spawn(run_event_loop(
        endpoint,
        gossip,
        command_rx,
        event_tx,
        replicators.clone(),
    ));

    Ok((command_tx, event_rx, replicators, task))
}

/// Main event loop processing incoming connections, gossip, and commands.
async fn run_event_loop(
    endpoint: Endpoint,
    gossip: Gossip,
    mut command_rx: mpsc::Receiver<IrohCommand>,
    event_tx: mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
    replicators: Arc<ReplicatorRegistry>,
) {
    let shutdown_started = std::time::Instant::now();
    let peer_map = Arc::new(parking_lot::Mutex::new(PeerMap::new()));
    let pending_pushlog_replies = Arc::new(parking_lot::Mutex::new(HashMap::<
        String,
        oneshot::Sender<PushLogReply>,
    >::new()));
    let connection_cache: ConnectionCache = Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let mut subscriptions: HashMap<String, TopicSubscription> = HashMap::new();
    let raw_topics: Arc<parking_lot::Mutex<std::collections::HashSet<String>>> =
        Arc::new(parking_lot::Mutex::new(std::collections::HashSet::new()));
    let mut active_syncs: HashMap<u64, ActiveSync> = HashMap::new();
    let spawned_tasks: SpawnedTasks = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let mut next_query_id: u64 = 1;

    // Emit Listening event with our endpoint address
    let addr_str = format!("iroh://{}", endpoint.id());
    if event_tx
        .send(TransportEvent::Listening(PeerAddr::new(addr_str)))
        .await
        .is_err()
    {
        warn!("Event channel closed, cannot emit Listening event");
    }

    'endpoint: loop {
        tokio::select! {
            Some(cmd) = command_rx.recv() => {
                let mut pending_cmd = Some(cmd);
                let mut processed = 0;
                while let Some(cmd) = pending_cmd.take() {
                    let should_shutdown = handle_command(
                        cmd,
                        &endpoint,
                        &gossip,
                        &peer_map,
                        &pending_pushlog_replies,
                        &connection_cache,
                        &mut subscriptions,
                        &raw_topics,
                        &replicators,
                        &mut active_syncs,
                        &spawned_tasks,
                        &mut next_query_id,
                        &event_tx,
                    ).await;
                    if should_shutdown {
                        break 'endpoint;
                    }

                    processed += 1;
                    if processed >= MAX_COMMAND_BATCH {
                        break;
                    }
                    pending_cmd = command_rx.try_recv().ok();
                }
            }
            incoming = endpoint.accept() => {
                match incoming {
                    Some(incoming) => {
                        let gossip = gossip.clone();
                        let peer_map = Arc::clone(&peer_map);
                        let pending_pushlog_replies = Arc::clone(&pending_pushlog_replies);
                        let subscription_senders = snapshot_subscription_senders(&subscriptions);
                        let spawned_tasks_for_incoming = Arc::clone(&spawned_tasks);
                        let event_tx = event_tx.clone();
                        let task = tokio::spawn(async move {
                            handle_incoming(
                                incoming,
                                &gossip,
                                &peer_map,
                                &pending_pushlog_replies,
                                &subscription_senders,
                                &spawned_tasks_for_incoming,
                                &event_tx,
                            )
                            .await;
                        });
                        track_task(&spawned_tasks, task);
                    }
                    None => break,
                }
            }
            else => break,
        }
    }

    // Clean up
    let subscriptions_started = std::time::Instant::now();
    for (_, sub) in subscriptions.drain() {
        sub.reader_task.abort();
    }
    warn!(
        elapsed_ms = subscriptions_started.elapsed().as_millis(),
        "Iroh endpoint shutdown: subscriptions aborted"
    );

    let syncs_started = std::time::Instant::now();
    for (_, sync) in active_syncs.drain() {
        sync.abort_handle.abort();
    }
    warn!(
        elapsed_ms = syncs_started.elapsed().as_millis(),
        "Iroh endpoint shutdown: active syncs aborted"
    );

    let tracked_started = std::time::Instant::now();
    shutdown_tracked_tasks(spawned_tasks).await;
    warn!(
        elapsed_ms = tracked_started.elapsed().as_millis(),
        "Iroh endpoint shutdown: tracked spawned tasks drained"
    );

    let gossip_started = std::time::Instant::now();
    match tokio::time::timeout(std::time::Duration::from_secs(1), gossip.shutdown()).await {
        Ok(Ok(())) => warn!(
            elapsed_ms = gossip_started.elapsed().as_millis(),
            "Iroh endpoint shutdown: gossip stopped"
        ),
        Ok(Err(error)) => debug!(%error, "Iroh gossip shutdown failed"),
        Err(_) => debug!("Timed out waiting for Iroh gossip shutdown"),
    }

    let close_started = std::time::Instant::now();
    endpoint.close().await;
    warn!(
        close_elapsed_ms = close_started.elapsed().as_millis(),
        total_elapsed_ms = shutdown_started.elapsed().as_millis(),
        "Iroh endpoint shut down"
    );
}

pub(super) fn snapshot_subscription_senders(
    subscriptions: &HashMap<String, TopicSubscription>,
) -> SubscriptionSenders {
    subscriptions
        .iter()
        .map(|(topic, sub)| (topic.clone(), sub.sender.clone()))
        .collect()
}

/// Join a newly connected peer into all active gossip topic subscriptions.
///
/// iroh-gossip subscriptions are created with an explicit neighbor list.
/// When a new peer connects after subscription, we add them as a neighbor
/// so they can receive (and send us) gossip messages on all subscribed topics.
pub(super) async fn join_peer_to_subscription_senders(
    subscriptions: &[(String, iroh_gossip::api::GossipSender)],
    endpoint_id: EndpointId,
) {
    for (topic, sub) in subscriptions.iter() {
        if let Err(e) = sub.join_peers(vec![endpoint_id]).await {
            debug!(
                topic = %topic,
                peer = %endpoint_id,
                "Failed to add new peer to gossip topic: {}",
                e
            );
        }
    }
}

/// Look up the cached direct socket address for a peer from the peer map.
pub(super) fn peer_direct_addr(
    peer_map: &Arc<parking_lot::Mutex<PeerMap>>,
    peer_id: &PeerId,
) -> Option<std::net::SocketAddr> {
    let id = parse_endpoint_id(peer_id).ok()?;
    let map = peer_map.lock();
    map.get(&id).and_then(|info| info.remote_addr)
}
