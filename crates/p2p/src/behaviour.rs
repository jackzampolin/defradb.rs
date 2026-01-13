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
//! - GossipSub for pubsub messaging
//!
//! # Wire Compatibility with Go
//!
//! The Go implementation uses separate request/response protocol IDs:
//! - Request: `/defradb/rep_req/0.0.1`
//! - Response: `/defradb/rep_resp/0.0.1`
//!
//! This Rust implementation uses libp2p's request-response protocol which
//! handles both request and response on a single stream. For full Go
//! compatibility, both protocols are supported.
//!
//! For GossipSub, we use libp2p's native message signing via
//! `MessageAuthenticity::Signed` which matches Go's approach.

use std::time::Duration;

use libp2p::{
    gossipsub::{self, MessageAuthenticity, MessageId, ValidationMode},
    identify, mdns,
    request_response::{self, ProtocolSupport},
    swarm::NetworkBehaviour,
    PeerId,
};

use libp2p::identity::Keypair;

use crate::codec::PushLogCodec;
use crate::message::{PushLogReply, PushLogRequest};
use crate::protocol::{rep_request_protocol, rep_response_protocol};

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

    /// GossipSub for pubsub messaging.
    pub gossipsub: gossipsub::Behaviour,
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

    /// GossipSub event.
    GossipSub(gossipsub::Event),
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

impl From<gossipsub::Event> for DefraEvent {
    fn from(event: gossipsub::Event) -> Self {
        DefraEvent::GossipSub(event)
    }
}

impl DefraBehaviour {
    /// Create a new DefraDB network behaviour with message signing enabled.
    ///
    /// # Arguments
    ///
    /// * `local_peer_id` - The local peer's ID
    /// * `local_public_key` - The local peer's public key
    /// * `keypair` - The keypair for message signing/verification
    ///
    /// # Returns
    ///
    /// A new `DefraBehaviour` instance or an error if initialization fails.
    pub fn new(
        local_peer_id: PeerId,
        local_public_key: libp2p::identity::PublicKey,
        keypair: Keypair,
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

        // Configure request-response for PushLog (replicator protocol)
        // Support both request and response protocols for Go compatibility
        // Use codec with keypair for message signing/verification
        let codec = PushLogCodec::with_keypair(keypair.clone());
        let pushlog = request_response::Behaviour::with_codec(
            codec,
            [
                (rep_request_protocol(), ProtocolSupport::Full),
                (rep_response_protocol(), ProtocolSupport::Full),
            ],
            request_response::Config::default().with_request_timeout(REQUEST_TIMEOUT),
        );

        // Configure GossipSub with native message signing
        // MessageAuthenticity::Signed uses libp2p's built-in signing
        // This matches Go's approach where pubsub handles authentication
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(1))
            .validation_mode(ValidationMode::Strict)
            // Use content-based message ID to match Go behavior for deduplication
            .message_id_fn(|message: &gossipsub::Message| {
                let hash = crypto::sha256(&message.data);
                MessageId::from(hash.to_vec())
            })
            .build()
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("gossipsub config error: {}", e),
                )
            })?;

        let gossipsub = gossipsub::Behaviour::new(
            MessageAuthenticity::Signed(keypair),
            gossipsub_config,
        )
        .map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("gossipsub creation error: {}", e),
            )
        })?;

        Ok(Self {
            identify,
            mdns,
            pushlog,
            gossipsub,
        })
    }

    /// Create a new DefraDB network behaviour without message signing.
    ///
    /// This is useful for testing but should not be used in production
    /// as messages will not be authenticated.
    ///
    /// # Arguments
    ///
    /// * `local_peer_id` - The local peer's ID
    /// * `local_public_key` - The local peer's public key
    ///
    /// # Returns
    ///
    /// A new `DefraBehaviour` instance or an error if initialization fails.
    #[cfg(test)]
    pub fn new_without_signing(
        local_peer_id: PeerId,
        local_public_key: libp2p::identity::PublicKey,
    ) -> Result<Self, std::io::Error> {
        let identify_config = identify::Config::new(
            "/defra/identify/0.0.1".to_string(),
            local_public_key,
        )
        .with_agent_version(format!("defradb-rs/{}", env!("CARGO_PKG_VERSION")));

        let identify = identify::Behaviour::new(identify_config);
        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

        let pushlog = request_response::Behaviour::new(
            [
                (rep_request_protocol(), ProtocolSupport::Full),
                (rep_response_protocol(), ProtocolSupport::Full),
            ],
            request_response::Config::default().with_request_timeout(REQUEST_TIMEOUT),
        );

        // For testing, use RandomAuthor for gossipsub (no signing)
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(1))
            .validation_mode(ValidationMode::Permissive)
            .message_id_fn(|message: &gossipsub::Message| {
                let hash = crypto::sha256(&message.data);
                MessageId::from(hash.to_vec())
            })
            .build()
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("gossipsub config error: {}", e),
                )
            })?;

        let gossipsub = gossipsub::Behaviour::new(
            MessageAuthenticity::RandomAuthor,
            gossipsub_config,
        )
        .map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("gossipsub creation error: {}", e),
            )
        })?;

        Ok(Self {
            identify,
            mdns,
            pushlog,
            gossipsub,
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

    /// Subscribe to a GossipSub topic.
    ///
    /// Returns `true` if this is a new subscription, `false` if already subscribed.
    pub fn subscribe(
        &mut self,
        topic: &gossipsub::IdentTopic,
    ) -> Result<bool, gossipsub::SubscriptionError> {
        self.gossipsub.subscribe(topic)
    }

    /// Unsubscribe from a GossipSub topic.
    ///
    /// Returns `true` if was subscribed, `false` if wasn't subscribed.
    pub fn unsubscribe(
        &mut self,
        topic: &gossipsub::IdentTopic,
    ) -> Result<bool, gossipsub::PublishError> {
        self.gossipsub.unsubscribe(topic)
    }

    /// Publish a message to a GossipSub topic.
    ///
    /// Returns the message ID on success.
    pub fn publish(
        &mut self,
        topic: gossipsub::IdentTopic,
        data: Vec<u8>,
    ) -> Result<gossipsub::MessageId, gossipsub::PublishError> {
        self.gossipsub.publish(topic, data)
    }

    /// Get the list of subscribed topics.
    pub fn subscribed_topics(&self) -> impl Iterator<Item = &gossipsub::TopicHash> {
        self.gossipsub.topics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity::Keypair;

    #[test]
    fn test_behaviour_creation_with_signing() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let public_key = keypair.public();

        let behaviour = DefraBehaviour::new(peer_id, public_key, keypair);
        assert!(behaviour.is_ok());
    }

    #[test]
    fn test_behaviour_creation_without_signing() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let public_key = keypair.public();

        let behaviour = DefraBehaviour::new_without_signing(peer_id, public_key);
        assert!(behaviour.is_ok());
    }
}
