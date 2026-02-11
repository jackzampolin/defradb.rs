//! Network commands: Listen, Dial, PeerAddresses.

use std::collections::HashSet;

use iroh_bitswap::Store;
use libp2p::{Multiaddr, PeerId};
use tracing::debug;

use crate::error::{Error, Result};

use super::super::p2p_host::P2PHost;

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
            .map_err(|e| Error::Transport(e.to_string()));
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
