//! Network commands: Listen, Dial, PeerAddresses.

use std::collections::HashSet;

use iroh_bitswap::Store;
use libp2p::{Multiaddr, PeerId};
use tracing::debug;

use crate::error::{Error, Result};

use super::super::p2p_host::P2PHost;

/// libp2p's `TransportError::Other` Displays as an empty string — the io
/// error is only reachable through `source()` — so `to_string()` on a failed
/// listen erased the "Address already in use" detail that the test harness
/// keys its fresh-port start retry on (#1501). Surface the io error directly.
fn format_listen_error(error: &libp2p::TransportError<std::io::Error>) -> String {
    match error {
        libp2p::TransportError::MultiaddrNotSupported(addr) => {
            format!("multiaddr not supported: {addr}")
        }
        libp2p::TransportError::Other(io_error) => io_error.to_string(),
    }
}

impl<S: Store> P2PHost<S> {
    pub(super) fn handle_listen(
        &mut self,
        addr: Multiaddr,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
        let result = self
            .swarm
            .listen_on(addr.clone())
            .map(|_| ())
            .map_err(|e| Error::Transport(format_listen_error(&e)));
        if response.send(result).is_err() {
            debug!(addr = %addr, "Listen command response dropped - caller cancelled");
        }
    }

    pub(super) fn handle_dial(
        &mut self,
        peer_id: PeerId,
        addrs: Vec<Multiaddr>,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
        let result = self.dial_peer(peer_id, addrs);
        if response.send(result).is_err() {
            debug!(peer_id = %peer_id, "Dial command response dropped - caller cancelled");
        }
    }

    pub(super) fn handle_disconnect(
        &mut self,
        peer_id: PeerId,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    ) {
        self.peer_addrs.remove(&peer_id);
        self.swarm.behaviour_mut().kademlia.remove_peer(&peer_id);
        // `disconnect_peer_id` returns `Err(())` when the peer is not connected.
        // Disconnect is idempotent: hanging up on an absent peer is success.
        let _ = self.swarm.disconnect_peer_id(peer_id);
        if response.send(Ok(())).is_err() {
            debug!(peer_id = %peer_id, "Disconnect command response dropped - caller cancelled");
        }
    }

    pub(super) fn handle_peer_addresses(
        &self,
        response: tokio::sync::oneshot::Sender<Vec<String>>,
    ) {
        // Build full multiaddrs for connected peers (matches Go's ActivePeers).
        let connected: HashSet<PeerId> = self.swarm.connected_peers().cloned().collect();
        let addrs: Vec<String> = connected
            .iter()
            .filter_map(|pid| {
                self.peer_addrs
                    .get(pid)
                    .map(|addr| format!("{}/p2p/{}", addr, pid))
            })
            .collect();
        if response.send(addrs).is_err() {
            debug!("PeerAddresses command response dropped - caller cancelled");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::format_listen_error;

    #[test]
    fn listen_error_surfaces_the_bind_failure() {
        let eaddrinuse = if cfg!(target_os = "linux") { 98 } else { 48 };
        let error = libp2p::TransportError::Other(std::io::Error::from_raw_os_error(eaddrinuse));

        let formatted = format_listen_error(&error);

        // The harness retries a node start with fresh ports only when it can
        // see this phrase in the node's output (#1501).
        assert!(
            formatted.to_lowercase().contains("address already in use"),
            "expected the bind failure detail, got: {formatted:?}"
        );
    }

    #[test]
    fn listen_error_names_the_unsupported_multiaddr() {
        let addr: libp2p::Multiaddr = "/ip4/127.0.0.1/tcp/9171".parse().unwrap();
        let error = libp2p::TransportError::<std::io::Error>::MultiaddrNotSupported(addr);

        assert_eq!(
            format_listen_error(&error),
            "multiaddr not supported: /ip4/127.0.0.1/tcp/9171"
        );
    }
}
