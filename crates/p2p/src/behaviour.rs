//! Composite NetworkBehaviour for DefraDB P2P protocol.
//!
//! This module combines multiple libp2p behaviours into a single
//! composite behaviour that handles:
//! - Peer identification (identify)
//! - Kademlia DHT for peer discovery
//! - Bitswap for block exchange (Go compatibility via iroh-bitswap)
//! - Request-response for PushLog synchronization
//! - GossipSub for pubsub messaging
//! - Circuit relay client for NAT traversal (optional)
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
//!
//! For Bitswap, we use iroh-bitswap which implements the standard
//! IPFS block exchange protocol (1.0.0, 1.1.0, 1.2.0), enabling
//! interoperability with Go DefraDB.

use std::time::Duration;

use iroh_bitswap::{Bitswap, BitswapEvent, Config as BitswapConfig, Store};
use libp2p::{
    connection_limits::{self, ConnectionLimits},
    gossipsub::{self, MessageAuthenticity, MessageId, ValidationMode},
    identify,
    kad::{self, store::MemoryStore, Mode},
    relay,
    request_response::{self, ProtocolSupport},
    swarm::{behaviour::toggle::Toggle, NetworkBehaviour},
    PeerId, StreamProtocol,
};
use libp2p_stream as stream;

use libp2p::identity::Keypair;

use crate::codec::PushLogCodec;
use crate::message::{PushLogReply, PushLogRequest};

/// Timeout for PushLog requests.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Composite network behaviour for DefraDB nodes.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "DefraEvent")]
pub struct DefraBehaviour<S: Store> {
    /// Peer identification protocol.
    pub identify: identify::Behaviour,

    /// Kademlia DHT for peer discovery and content routing.
    /// Required for Bitswap to find peers who have specific blocks.
    pub kademlia: kad::Behaviour<MemoryStore>,

    /// Bitswap block exchange protocol (Go compatibility).
    /// Uses iroh-bitswap for IPFS block exchange.
    pub bitswap: Bitswap<S>,

    /// Request-response protocol for PushLog messages.
    pub pushlog: request_response::Behaviour<PushLogCodec>,

    /// GossipSub for pubsub messaging (optional, controlled by `pubsub_enabled` config).
    pub gossipsub: Toggle<gossipsub::Behaviour>,

    /// Relay client for circuit relay (optional, controlled by `relay_enabled` config).
    /// Initialized as disabled; the SwarmBuilder injects the client when relay is enabled.
    pub relay: Toggle<relay::client::Behaviour>,

    /// Raw stream protocol for Go two-stream compatibility.
    /// Go's DefraDB uses separate streams for request and response.
    pub stream: stream::Behaviour,

    /// Connection limits to prevent resource exhaustion from too many peers.
    /// Watermarks match Go DefraDB's defaults: 100 pending/established inbound,
    /// 400 established outbound, 4 streams per peer.
    pub connection_limits: connection_limits::Behaviour,
}

/// Events emitted by the DefraDB network behaviour.
#[allow(clippy::large_enum_variant)]
pub enum DefraEvent {
    /// Identify protocol event.
    Identify(identify::Event),

    /// Kademlia DHT event.
    Kademlia(kad::Event),

    /// Bitswap block exchange event.
    Bitswap(BitswapEvent),

    /// PushLog request-response event.
    PushLog(request_response::Event<PushLogRequest, PushLogReply>),

    /// GossipSub event.
    GossipSub(gossipsub::Event),

    /// Relay client event.
    Relay(relay::client::Event),
}

impl From<identify::Event> for DefraEvent {
    fn from(event: identify::Event) -> Self {
        DefraEvent::Identify(event)
    }
}

impl From<kad::Event> for DefraEvent {
    fn from(event: kad::Event) -> Self {
        DefraEvent::Kademlia(event)
    }
}

impl From<BitswapEvent> for DefraEvent {
    fn from(event: BitswapEvent) -> Self {
        DefraEvent::Bitswap(event)
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

impl From<relay::client::Event> for DefraEvent {
    fn from(event: relay::client::Event) -> Self {
        DefraEvent::Relay(event)
    }
}

impl From<()> for DefraEvent {
    fn from(_: ()) -> Self {
        // stream::Behaviour emits () events which we ignore
        // Stream handling happens through Control, not events
        unreachable!("stream::Behaviour should not emit events")
    }
}

impl From<void::Void> for DefraEvent {
    fn from(v: void::Void) -> Self {
        // connection_limits::Behaviour emits Void — it never actually fires events,
        // it only enforces limits by refusing connections at the transport layer.
        void::unreachable(v)
    }
}

impl<S: Store> DefraBehaviour<S> {
    /// Create a new DefraDB network behaviour with message signing enabled.
    ///
    /// # Arguments
    ///
    /// * `local_peer_id` - The local peer's ID
    /// * `local_public_key` - The local peer's public key
    /// * `keypair` - The keypair for message signing/verification
    /// * `bitswap_store` - The blockstore for Bitswap block exchange
    ///
    /// # Returns
    ///
    /// A new `DefraBehaviour` instance or an error if initialization fails.
    ///
    /// # Note
    ///
    /// This must be called within a tokio runtime context as Bitswap spawns
    /// background tasks for operations.
    pub async fn new(
        local_peer_id: PeerId,
        local_public_key: libp2p::identity::PublicKey,
        keypair: Keypair,
        bitswap_store: S,
        enable_pubsub: bool,
        config: &super::P2PHostConfig,
    ) -> Result<Self, std::io::Error> {
        // Configure identify behaviour
        let identify_config =
            identify::Config::new("/defra/identify/0.0.1".to_string(), local_public_key)
                .with_agent_version(format!("defradb-rs/{}", env!("CARGO_PKG_VERSION")));

        let identify = identify::Behaviour::new(identify_config);

        // Configure request-response for PushLog (Rust-to-Rust only)
        // NOTE: We do NOT register rep_request or rep_response protocols here
        // because stream::Behaviour handles those for Go two-stream compatibility.
        // Request-response is kept for potential future Rust-only protocols.
        let codec = PushLogCodec::with_keypair(keypair.clone());
        let pushlog = request_response::Behaviour::with_codec(
            codec,
            std::iter::empty::<(StreamProtocol, ProtocolSupport)>(),
            request_response::Config::default().with_request_timeout(REQUEST_TIMEOUT),
        );

        // Configure GossipSub (matches Go's `if options.EnablePubSub` conditional)
        let gossipsub = if enable_pubsub {
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(1))
                .validation_mode(ValidationMode::Strict)
                // Enable peer exchange (PX) in PRUNE messages — matches Go's
                // pubsub.WithPeerExchange(true). Allows peers sharing a topic
                // to discover each other through mesh management.
                .do_px()
                .flood_publish(true)
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

            let gs =
                gossipsub::Behaviour::new(MessageAuthenticity::Signed(keypair), gossipsub_config)
                    .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("gossipsub creation error: {}", e),
                    )
                })?;
            Toggle::from(Some(gs))
        } else {
            Toggle::from(None)
        };

        // Configure Kademlia DHT for peer discovery
        // Required for Bitswap to find peers who have specific blocks
        let kad_store = MemoryStore::new(local_peer_id);
        let mut kademlia = kad::Behaviour::new(local_peer_id, kad_store);
        // Run as server to respond to DHT queries from other peers
        kademlia.set_mode(Some(Mode::Server));

        // Configure Bitswap for block exchange (Go compatibility)
        // iroh-bitswap implements the standard IPFS bitswap protocols
        let bitswap_config = BitswapConfig::default();
        let bitswap = Bitswap::new(local_peer_id, bitswap_store, bitswap_config).await;

        // Configure stream behaviour for Go two-stream compatibility
        let stream = stream::Behaviour::new();

        let limits = ConnectionLimits::default()
            .with_max_pending_incoming(Some(config.max_connections_in))
            .with_max_pending_outgoing(Some(config.max_connections_out))
            .with_max_established_incoming(Some(config.max_connections_in))
            .with_max_established_outgoing(Some(config.max_connections_out))
            .with_max_established_per_peer(Some(config.max_connections_per_peer));
        let connection_limits = connection_limits::Behaviour::new(limits);

        Ok(Self {
            identify,
            kademlia,
            bitswap,
            pushlog,
            gossipsub,
            relay: Toggle::from(None),
            stream,
            connection_limits,
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
    /// * `bitswap_store` - The blockstore for Bitswap block exchange
    ///
    /// # Returns
    ///
    /// A new `DefraBehaviour` instance or an error if initialization fails.
    #[cfg(test)]
    pub async fn new_without_signing(
        local_peer_id: PeerId,
        local_public_key: libp2p::identity::PublicKey,
        bitswap_store: S,
        enable_pubsub: bool,
    ) -> Result<Self, std::io::Error> {
        let identify_config =
            identify::Config::new("/defra/identify/0.0.1".to_string(), local_public_key)
                .with_agent_version(format!("defradb-rs/{}", env!("CARGO_PKG_VERSION")));

        let identify = identify::Behaviour::new(identify_config);

        // NOTE: Do NOT register rep_request or rep_response protocols here
        // because stream::Behaviour handles those for Go two-stream compatibility.
        let pushlog = request_response::Behaviour::new(
            std::iter::empty::<(StreamProtocol, ProtocolSupport)>(),
            request_response::Config::default().with_request_timeout(REQUEST_TIMEOUT),
        );

        // Configure GossipSub (optional, matches Go's `if options.EnablePubSub`)
        let gossipsub = if enable_pubsub {
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

            let gs = gossipsub::Behaviour::new(MessageAuthenticity::RandomAuthor, gossipsub_config)
                .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("gossipsub creation error: {}", e),
                    )
                })?;
            Toggle::from(Some(gs))
        } else {
            Toggle::from(None)
        };

        // Configure Kademlia DHT for peer discovery
        let kad_store = MemoryStore::new(local_peer_id);
        let mut kademlia = kad::Behaviour::new(local_peer_id, kad_store);
        kademlia.set_mode(Some(Mode::Server));

        // Configure Bitswap for block exchange
        let bitswap_config = BitswapConfig::default();
        let bitswap = Bitswap::new(local_peer_id, bitswap_store, bitswap_config).await;

        // Configure stream behaviour for Go two-stream compatibility
        let stream = stream::Behaviour::new();

        // Configure connection limits — matches Go DefraDB watermarks
        let limits = ConnectionLimits::default()
            .with_max_pending_incoming(Some(100))
            .with_max_pending_outgoing(Some(100))
            .with_max_established_incoming(Some(100))
            .with_max_established_outgoing(Some(400))
            .with_max_established_per_peer(Some(4));
        let connection_limits = connection_limits::Behaviour::new(limits);

        Ok(Self {
            identify,
            kademlia,
            bitswap,
            pushlog,
            gossipsub,
            relay: Toggle::from(None),
            stream,
            connection_limits,
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
    #[allow(clippy::result_large_err)]
    pub fn send_pushlog_response(
        &mut self,
        channel: request_response::ResponseChannel<PushLogReply>,
        response: PushLogReply,
    ) -> Result<(), PushLogReply> {
        self.pushlog.send_response(channel, response)
    }

    /// Subscribe to a GossipSub topic.
    ///
    /// Returns `true` if this is a new subscription, `false` if already subscribed
    /// or if pubsub is disabled.
    pub fn subscribe(
        &mut self,
        topic: &gossipsub::IdentTopic,
    ) -> Result<bool, gossipsub::SubscriptionError> {
        match self.gossipsub.as_mut() {
            Some(gs) => gs.subscribe(topic),
            None => Ok(false),
        }
    }

    /// Unsubscribe from a GossipSub topic.
    ///
    /// Returns `true` if was subscribed, `false` if wasn't subscribed
    /// or if pubsub is disabled.
    pub fn unsubscribe(
        &mut self,
        topic: &gossipsub::IdentTopic,
    ) -> Result<bool, gossipsub::PublishError> {
        match self.gossipsub.as_mut() {
            Some(gs) => gs.unsubscribe(topic),
            None => Ok(false),
        }
    }

    /// Publish a message to a GossipSub topic.
    ///
    /// Returns the message ID on success, or `InsufficientPeers` if pubsub is disabled.
    pub fn publish(
        &mut self,
        topic: gossipsub::IdentTopic,
        data: Vec<u8>,
    ) -> Result<gossipsub::MessageId, gossipsub::PublishError> {
        match self.gossipsub.as_mut() {
            Some(gs) => gs.publish(topic, data),
            None => Err(gossipsub::PublishError::InsufficientPeers),
        }
    }

    /// Get the list of subscribed topics.
    pub fn subscribed_topics(&self) -> Box<dyn Iterator<Item = &gossipsub::TopicHash> + '_> {
        match self.gossipsub.as_ref() {
            Some(gs) => Box::new(gs.topics()),
            None => Box::new(std::iter::empty()),
        }
    }

    // === Bitswap operations ===
    // Note: iroh-bitswap uses a client/server model. The Bitswap behaviour
    // handles protocol negotiation, but block fetching is done through the client.

    /// Get a reference to the Bitswap client for block fetching.
    pub fn bitswap_client(&self) -> &iroh_bitswap::Client<S> {
        self.bitswap.client()
    }

    /// Called when identify reports peer protocols - informs bitswap about supported protocols.
    pub fn on_identify(&self, peer: &PeerId, protocols: &[String]) {
        self.bitswap.on_identify(peer, protocols);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::MockBitswapStore;
    use libp2p::identity::Keypair;

    #[tokio::test]
    async fn test_behaviour_creation_with_signing() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let public_key = keypair.public();
        let store = MockBitswapStore::new();

        let behaviour = DefraBehaviour::new(peer_id, public_key, keypair, store, true).await;
        assert!(behaviour.is_ok());
    }

    #[tokio::test]
    async fn test_behaviour_creation_without_signing() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let public_key = keypair.public();
        let store = MockBitswapStore::new();

        let behaviour = DefraBehaviour::new_without_signing(peer_id, public_key, store, true).await;
        assert!(behaviour.is_ok());
    }

    #[tokio::test]
    async fn test_behaviour_creation_pubsub_disabled() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let public_key = keypair.public();
        let store = MockBitswapStore::new();

        let behaviour = DefraBehaviour::new(peer_id, public_key, keypair, store, false).await;
        assert!(behaviour.is_ok());
        let behaviour = behaviour.unwrap();
        assert!(behaviour.gossipsub.as_ref().is_none());
    }
}
