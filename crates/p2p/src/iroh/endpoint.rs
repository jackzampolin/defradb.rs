//! Background event loop owning all iroh state.
//!
//! `IrohEndpoint` runs as a spawned tokio task and processes:
//! - Incoming QUIC connections
//! - Gossip events from iroh-gossip
//! - Commands from the `IrohTransport` facade

use std::collections::HashMap;
use std::sync::Arc;

use iroh::{Endpoint, EndpointId};
use iroh_gossip::net::Gossip;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::replicator::ReplicatorInfo;
use crate::transport::{PeerAddr, PeerId, TransportEvent};

use super::command::IrohCommand;
use super::endpoint_commands::handle_command;
use super::endpoint_config::{
    apply_bind_config, apply_discovery_config, relay_mode_from_config, IrohEndpointConfig,
};
use super::endpoint_streams::handle_incoming;
use super::peer_map::{parse_endpoint_id, PeerMap};
use super::protocols;

/// Handle to a gossip topic subscription.
pub(super) struct TopicSubscription {
    pub(super) sender: iroh_gossip::api::GossipSender,
    pub(super) reader_task: JoinHandle<()>,
}

/// Active block sync task.
pub(super) struct ActiveSync {
    pub(super) abort_handle: tokio::task::AbortHandle,
}

/// Spawn the iroh endpoint background task.
///
/// Returns the command sender, event receiver, and background task handle.
pub async fn spawn_endpoint(
    config: IrohEndpointConfig,
) -> crate::error::Result<(
    mpsc::Sender<IrohCommand>,
    mpsc::Receiver<TransportEvent<iroh::endpoint::SendStream>>,
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
    let (event_tx, event_rx) = mpsc::channel::<TransportEvent<iroh::endpoint::SendStream>>(256);

    let task = tokio::spawn(run_event_loop(endpoint, gossip, command_rx, event_tx));

    Ok((command_tx, event_rx, task))
}

/// Main event loop processing incoming connections, gossip, and commands.
async fn run_event_loop(
    endpoint: Endpoint,
    gossip: Gossip,
    mut command_rx: mpsc::Receiver<IrohCommand>,
    event_tx: mpsc::Sender<TransportEvent<iroh::endpoint::SendStream>>,
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

/// Join a newly connected peer into all active gossip topic subscriptions.
///
/// iroh-gossip subscriptions are created with an explicit neighbor list.
/// When a new peer connects after subscription, we add them as a neighbor
/// so they can receive (and send us) gossip messages on all subscribed topics.
pub(super) async fn join_peer_to_subscriptions(
    subscriptions: &HashMap<String, TopicSubscription>,
    endpoint_id: EndpointId,
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

/// Look up the cached direct socket address for a peer from the peer map.
pub(super) fn peer_direct_addr(
    peer_map: &Arc<parking_lot::Mutex<PeerMap>>,
    peer_id: &PeerId,
) -> Option<std::net::SocketAddr> {
    let id = parse_endpoint_id(peer_id).ok()?;
    let map = peer_map.lock();
    map.get(&id).and_then(|info| info.remote_addr)
}
