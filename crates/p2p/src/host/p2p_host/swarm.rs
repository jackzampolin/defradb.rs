//! Swarm event handling.

use iroh_bitswap::Store;
use libp2p::swarm::{ConnectionId, SwarmEvent};
use libp2p::PeerId;
use tracing::{debug, error, info, warn};

use crate::behaviour::DefraEvent;

use super::P2PHost;
use crate::host::event::HostEvent;

impl<S: Store> P2PHost<S> {
    /// Handle a swarm event.
    ///
    /// Returns `true` when the event added a peer address that may unblock the
    /// one-time initial DHT bootstrap.
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

                // Add peer to Kademlia before any scheduled bootstrap. Kademlia's own
                // ConnectionEstablished handler doesn't add peers to the
                // routing table until protocol negotiation completes (async).
                // We add the address now so the initial or periodic
                // bootstrap has at least one peer to query.
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
                self.handle_connection_closed(peer_id, connection_id, num_established)
                    .await;
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

    /// Handle a closed connection.
    ///
    /// `num_established` is the number of connections to `peer_id` that remain
    /// open *after* this one closed. We only treat the peer as disconnected —
    /// dropping its cached address and surfacing `PeerDisconnected` to the sync
    /// layer — once the last connection closes. Emitting `PeerDisconnected` while
    /// another connection is still live flips the peer's `connected` gate off,
    /// which stalls provider selection, subscription targeting and ingress
    /// authorization even though the peer is still reachable, leaving blocks
    /// unreconciled until a brand-new connection forms. Duplicate connections
    /// (e.g. a simultaneous dial-dial between two peers) form far more readily
    /// under Linux's multi-core socket scheduling than on macOS loopback, so the
    /// previous unconditional emit surfaced as Linux-only P2P sync stalls. The
    /// iroh transport already refcounts connections per peer; this matches it.
    pub(super) async fn handle_connection_closed(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        num_established: u32,
    ) {
        self.connection_manager.on_closed(connection_id);

        if num_established > 0 {
            debug!(
                peer_id = %peer_id,
                remaining_connections = num_established,
                "Connection closed but peer still reachable; not emitting PeerDisconnected"
            );
            return;
        }

        info!(peer_id = %peer_id, "Peer disconnected");
        self.peer_addrs.remove(&peer_id);
        if self
            .event_tx
            .send(HostEvent::PeerDisconnected(peer_id))
            .await
            .is_err()
        {
            warn!(peer_id = %peer_id, "Failed to send PeerDisconnected event - receiver dropped");
        }
    }
}

#[cfg(test)]
mod tests {
    use libp2p::swarm::ConnectionId;
    use libp2p::PeerId;

    use crate::host::event::HostEvent;
    use crate::host::P2PHost;
    use crate::testutil::MockBitswapStore;

    fn drain(rx: &mut tokio::sync::mpsc::Receiver<HostEvent>) -> Vec<HostEvent> {
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    fn saw_disconnect(events: &[HostEvent], peer: PeerId) -> bool {
        events
            .iter()
            .any(|event| matches!(event, HostEvent::PeerDisconnected(p) if *p == peer))
    }

    /// Closing one of several connections to a peer must NOT report the peer as
    /// disconnected; only the last connection closing does. Before the fix,
    /// `ConnectionClosed` emitted `PeerDisconnected` unconditionally, so tearing
    /// down a duplicate connection (common under Linux socket scheduling) flipped
    /// the peer's `connected` gate off and stalled block reconciliation.
    #[tokio::test]
    async fn peer_disconnect_only_on_last_connection_close() {
        let store = MockBitswapStore::new();
        let (mut host, _handle, mut events, _registry) = P2PHost::new(store).await.unwrap();
        let peer = PeerId::random();

        // One of two connections closes; one remains (num_established == 1).
        host.handle_connection_closed(peer, ConnectionId::new_unchecked(1), 1)
            .await;
        assert!(
            !saw_disconnect(&drain(&mut events), peer),
            "PeerDisconnected must not fire while the peer still has a live connection"
        );

        // The last connection closes (num_established == 0).
        host.handle_connection_closed(peer, ConnectionId::new_unchecked(2), 0)
            .await;
        assert!(
            saw_disconnect(&drain(&mut events), peer),
            "PeerDisconnected must fire once the last connection closes"
        );
    }
}
