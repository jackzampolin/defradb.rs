// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Composite NetworkBehaviour for DefraDB P2P protocol.
//!
//! This module combines multiple libp2p behaviours into a single
//! composite behaviour that handles:
//! - Peer identification (identify)
//! - Local peer discovery (mDNS)
//! - Request-response for PushLog synchronization

use std::time::Duration;

use libp2p::{
    identify, mdns,
    request_response::{self, ProtocolSupport},
    swarm::NetworkBehaviour,
    PeerId,
};

use crate::codec::PushLogCodec;
use crate::message::{PushLogReply, PushLogRequest};
use crate::protocol::pushlog_protocol;

/// Timeout for PushLog requests.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Composite network behaviour for DefraDB nodes.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "DefraEvent")]
pub struct DefraBehaviour {
    /// Peer identification protocol.
    pub identify: identify::Behaviour,

    /// Local network peer discovery via mDNS.
    pub mdns: mdns::tokio::Behaviour,

    /// Request-response protocol for PushLog messages.
    pub pushlog: request_response::Behaviour<PushLogCodec>,
}

/// Events emitted by the DefraDB network behaviour.
#[allow(clippy::large_enum_variant)]
pub enum DefraEvent {
    /// Identify protocol event.
    Identify(identify::Event),

    /// mDNS discovery event.
    Mdns(mdns::Event),

    /// PushLog request-response event.
    PushLog(request_response::Event<PushLogRequest, PushLogReply>),
}

impl From<identify::Event> for DefraEvent {
    fn from(event: identify::Event) -> Self {
        DefraEvent::Identify(event)
    }
}

impl From<mdns::Event> for DefraEvent {
    fn from(event: mdns::Event) -> Self {
        DefraEvent::Mdns(event)
    }
}

impl From<request_response::Event<PushLogRequest, PushLogReply>> for DefraEvent {
    fn from(event: request_response::Event<PushLogRequest, PushLogReply>) -> Self {
        DefraEvent::PushLog(event)
    }
}

impl DefraBehaviour {
    /// Create a new DefraDB network behaviour.
    ///
    /// # Arguments
    ///
    /// * `local_peer_id` - The local peer's ID
    /// * `local_public_key` - The local peer's public key
    ///
    /// # Returns
    ///
    /// A new `DefraBehaviour` instance or an error if initialization fails.
    pub fn new(
        local_peer_id: PeerId,
        local_public_key: libp2p::identity::PublicKey,
    ) -> Result<Self, std::io::Error> {
        // Configure identify behaviour
        let identify_config = identify::Config::new(
            "/defra/identify/0.0.1".to_string(),
            local_public_key,
        )
        .with_agent_version(format!("defradb-rs/{}", env!("CARGO_PKG_VERSION")));

        let identify = identify::Behaviour::new(identify_config);

        // Configure mDNS for local network discovery
        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

        // Configure request-response for PushLog
        let pushlog = request_response::Behaviour::new(
            [(pushlog_protocol(), ProtocolSupport::Full)],
            request_response::Config::default().with_request_timeout(REQUEST_TIMEOUT),
        );

        Ok(Self {
            identify,
            mdns,
            pushlog,
        })
    }

    /// Send a PushLog request to a peer.
    ///
    /// Returns a request ID that can be used to correlate with the response.
    pub fn send_pushlog_request(
        &mut self,
        peer: &PeerId,
        request: PushLogRequest,
    ) -> request_response::OutboundRequestId {
        self.pushlog.send_request(peer, request)
    }

    /// Send a PushLog response to a peer.
    pub fn send_pushlog_response(
        &mut self,
        channel: request_response::ResponseChannel<PushLogReply>,
        response: PushLogReply,
    ) -> Result<(), PushLogReply> {
        self.pushlog.send_response(channel, response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity::Keypair;

    #[test]
    fn test_behaviour_creation() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let public_key = keypair.public();

        let behaviour = DefraBehaviour::new(peer_id, public_key);
        assert!(behaviour.is_ok());
    }
}
