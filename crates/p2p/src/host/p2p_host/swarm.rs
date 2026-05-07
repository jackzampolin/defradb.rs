//! Swarm event handling.

use iroh_bitswap::Store;
use libp2p::swarm::SwarmEvent;
use tracing::{debug, error, info, warn};

use crate::behaviour::DefraEvent;

use super::P2PHost;
use crate::host::event::HostEvent;

impl<S: Store> P2PHost<S> {
    /// Handle a swarm event.
    ///
    /// Returns `true` when the host should schedule a debounced DHT bootstrap.
    pub(super) async fn handle_swarm_event(&mut self, event: SwarmEvent<DefraEvent>) -> bool {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                info!(address = %address, "Created LibP2P host");
                if self
                    .event_tx
                    .send(HostEvent::Listening(address.clone()))
                    .await
                    .is_err()
                {
                    warn!(address = %address, "Failed to send Listening event - receiver dropped");
                }
                false
            }

            SwarmEvent::ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                ..
            } => {
                info!(peer_id = %peer_id, "Peer connected");
                self.connection_manager.on_established(
                    connection_id,
                    peer_id,
                    tokio::time::Instant::now(),
                );

                // Store the remote peer's address from the connection endpoint.
                // For dialer: the address we dialed (peer's listen addr).
                // For listener: the send_back_addr. With TCP port reuse enabled,
                // this IS the peer's listen address (Go-compatible behavior).
                let peer_addr = match &endpoint {
                    libp2p::core::ConnectedPoint::Dialer { address, .. } => address.clone(),
                    libp2p::core::ConnectedPoint::Listener { send_back_addr, .. } => {
                        send_back_addr.clone()
                    }
                };
                self.peer_addrs.insert(peer_id, peer_addr.clone());

                // Add peer to Kademlia BEFORE bootstrap. Kademlia's own
                // ConnectionEstablished handler doesn't add peers to the
                // routing table until protocol negotiation completes (async).
                // We add the address now so scheduled bootstrap queries have
                // at least one peer to query.
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer_id, peer_addr);

                // Pre-announce bitswap protocols so the peer immediately transitions
                // from Connected → Responsive in iroh-bitswap. Without this, there's
                // a race between the Identify protocol completing and the first
                // Bitswap fetch: after a node restart, GossipSub notifications can
                // trigger Bitswap fetches before Identify finishes, leaving the peer
                // in Connected state where peer_connected() is never called and the
                // peer has no MessageQueue, so want messages are never sent.
                // The actual protocol version is negotiated per-substream regardless.
                debug!(peer_id = %peer_id, "Pre-announcing Bitswap protocols");
                self.swarm.behaviour().on_identify(
                    &peer_id,
                    &[
                        "/ipfs/bitswap/1.2.0".to_string(),
                        "/ipfs/bitswap/1.1.0".to_string(),
                        "/ipfs/bitswap/1.0.0".to_string(),
                    ],
                );
                debug!(peer_id = %peer_id, "Bitswap protocol pre-announce complete");

                if self
                    .event_tx
                    .send(HostEvent::PeerConnected(peer_id))
                    .await
                    .is_err()
                {
                    warn!(peer_id = %peer_id, "Failed to send PeerConnected event - receiver dropped");
                }
                true
            }

            SwarmEvent::ConnectionClosed {
                peer_id,
                connection_id,
                num_established,
                ..
            } => {
                info!(peer_id = %peer_id, "Peer disconnected");
                self.connection_manager.on_closed(connection_id);
                if num_established == 0 {
                    self.peer_addrs.remove(&peer_id);
                }
                if self
                    .event_tx
                    .send(HostEvent::PeerDisconnected(peer_id))
                    .await
                    .is_err()
                {
                    warn!(peer_id = %peer_id, "Failed to send PeerDisconnected event - receiver dropped");
                }
                false
            }

            SwarmEvent::Behaviour(DefraEvent::Identify(identify_event)) => {
                self.handle_identify_event(identify_event).await;
                false
            }

            SwarmEvent::Behaviour(DefraEvent::PushLog(pushlog_event)) => {
                self.handle_pushlog_event(pushlog_event).await;
                false
            }

            SwarmEvent::Behaviour(DefraEvent::GossipSub(gossipsub_event)) => {
                self.handle_gossipsub_event(gossipsub_event).await;
                false
            }

            SwarmEvent::Behaviour(DefraEvent::Bitswap(bitswap_event)) => {
                self.handle_bitswap_event(bitswap_event).await;
                false
            }

            SwarmEvent::Behaviour(DefraEvent::Relay(relay_event)) => {
                use libp2p::relay;
                match relay_event {
                    relay::client::Event::ReservationReqAccepted {
                        relay_peer_id,
                        renewal,
                        limit,
                    } => {
                        info!(
                            relay_peer_id = %relay_peer_id,
                            renewal = renewal,
                            limit = ?limit,
                            "Relay reservation accepted"
                        );
                    }
                    relay::client::Event::OutboundCircuitEstablished {
                        relay_peer_id,
                        limit,
                    } => {
                        info!(
                            relay_peer_id = %relay_peer_id,
                            limit = ?limit,
                            "Outbound relay circuit established"
                        );
                    }
                    relay::client::Event::InboundCircuitEstablished { src_peer_id, limit } => {
                        info!(
                            src_peer_id = %src_peer_id,
                            limit = ?limit,
                            "Inbound relay circuit established"
                        );
                    }
                }
                false
            }

            SwarmEvent::Behaviour(DefraEvent::Kademlia(kad_event)) => {
                self.handle_kademlia_event(kad_event).await;
                false
            }

            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                warn!(
                    peer_id = ?peer_id,
                    error = %error,
                    "Outgoing connection failed"
                );
                false
            }

            SwarmEvent::IncomingConnectionError {
                local_addr,
                send_back_addr,
                error,
                ..
            } => {
                warn!(
                    local_addr = %local_addr,
                    remote_addr = %send_back_addr,
                    error = %error,
                    "Incoming connection failed"
                );
                false
            }

            SwarmEvent::ListenerError { listener_id, error } => {
                error!(
                    listener_id = ?listener_id,
                    error = %error,
                    "Listener error"
                );
                false
            }

            SwarmEvent::ListenerClosed {
                listener_id,
                reason,
                ..
            } => {
                warn!(
                    listener_id = ?listener_id,
                    reason = ?reason,
                    "Listener closed"
                );
                false
            }

            SwarmEvent::ExpiredListenAddr {
                listener_id,
                address,
            } => {
                debug!(
                    listener_id = ?listener_id,
                    address = %address,
                    "Listen address expired"
                );
                false
            }

            SwarmEvent::Dialing {
                peer_id: Some(peer_id),
                ..
            } => {
                debug!(peer_id = %peer_id, "Dialing peer");
                false
            }

            SwarmEvent::Dialing { peer_id: None, .. } => {
                // Dialing without a specific peer ID (rare, usually has peer_id)
                false
            }

            _ => {
                // Other swarm events (e.g., Dialing, NewExternalAddrCandidate) are
                // handled by libp2p internally and don't require explicit handling
                false
            }
        }
    }
}
