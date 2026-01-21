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
//! - Kademlia DHT for peer discovery
//! - Bitswap for block exchange (Go compatibility)
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
//!
//! For Bitswap, we use libp2p-bitswap-next which implements the standard
//! IPFS block exchange protocol, enabling interoperability with Go DefraDB.

use std::time::Duration;

use futures::future::BoxFuture;
use libipld::DefaultParams;
use libp2p::{
    gossipsub::{self, MessageAuthenticity, MessageId, ValidationMode},
    identify,
    kad::{self, store::MemoryStore, Mode},
    mdns,
    request_response::{self, ProtocolSupport},
    swarm::NetworkBehaviour,
    Multiaddr, PeerId, StreamProtocol,
};
use libp2p_bitswap_next::{Bitswap, BitswapConfig, BitswapEvent, BitswapStore, QueryId};
use libp2p_stream as stream;

use libp2p::identity::Keypair;

use crate::codec::PushLogCodec;
use crate::message::{PushLogReply, PushLogRequest};

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

    /// Kademlia DHT for peer discovery and content routing.
    /// Required for Bitswap to find peers who have specific blocks.
    pub kademlia: kad::Behaviour<MemoryStore>,

    /// Bitswap block exchange protocol (Go compatibility).
    /// Uses libp2p-bitswap-next for IPFS block exchange.
    pub bitswap: Bitswap<DefaultParams>,

    /// Request-response protocol for PushLog messages.
    pub pushlog: request_response::Behaviour<PushLogCodec>,

    /// GossipSub for pubsub messaging.
    pub gossipsub: gossipsub::Behaviour,

    /// Raw stream protocol for Go two-stream compatibility.
    /// Go's DefraDB uses separate streams for request and response.
    pub stream: stream::Behaviour,
}

/// Events emitted by the DefraDB network behaviour.
#[allow(clippy::large_enum_variant)]
pub enum DefraEvent {
    /// Identify protocol event.
    Identify(identify::Event),

    /// mDNS discovery event.
    Mdns(mdns::Event),

    /// Kademlia DHT event.
    Kademlia(kad::Event),

    /// Bitswap block exchange event.
    Bitswap(BitswapEvent),

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

impl From<()> for DefraEvent {
    fn from(_: ()) -> Self {
        // stream::Behaviour emits () events which we ignore
        // Stream handling happens through Control, not events
        unreachable!("stream::Behaviour should not emit events")
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
    /// * `bitswap_store` - The blockstore for Bitswap block exchange
    ///
    /// # Returns
    ///
    /// A new `DefraBehaviour` instance or an error if initialization fails.
    ///
    /// # Note
    ///
    /// This must be called within a tokio runtime context as Bitswap spawns
    /// a background task for database operations.
    pub fn new<S: BitswapStore<Params = DefaultParams>>(
        local_peer_id: PeerId,
        local_public_key: libp2p::identity::PublicKey,
        keypair: Keypair,
        bitswap_store: S,
    ) -> Result<Self, std::io::Error> {
        // Configure identify behaviour
        let identify_config =
            identify::Config::new("/defra/identify/0.0.1".to_string(), local_public_key)
                .with_agent_version(format!("defradb-rs/{}", env!("CARGO_PKG_VERSION")));

        let identify = identify::Behaviour::new(identify_config);

        // Configure mDNS for local network discovery
        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

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

        let gossipsub =
            gossipsub::Behaviour::new(MessageAuthenticity::Signed(keypair), gossipsub_config)
                .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("gossipsub creation error: {}", e),
                    )
                })?;

        // Configure Kademlia DHT for peer discovery
        // Required for Bitswap to find peers who have specific blocks
        let kad_store = MemoryStore::new(local_peer_id);
        let mut kademlia = kad::Behaviour::new(local_peer_id, kad_store);
        // Run as server to respond to DHT queries from other peers
        kademlia.set_mode(Some(Mode::Server));

        // Configure Bitswap for block exchange (Go compatibility)
        // The executor spawns a background task for database operations
        let executor: Box<dyn FnOnce(BoxFuture<'static, ()>)> = Box::new(|fut| {
            tokio::spawn(fut);
        });
        let bitswap = Bitswap::new(BitswapConfig::default(), bitswap_store, executor);

        // Configure stream behaviour for Go two-stream compatibility
        let stream = stream::Behaviour::new();

        Ok(Self {
            identify,
            mdns,
            kademlia,
            bitswap,
            pushlog,
            gossipsub,
            stream,
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
    pub fn new_without_signing<S: BitswapStore<Params = DefaultParams>>(
        local_peer_id: PeerId,
        local_public_key: libp2p::identity::PublicKey,
        bitswap_store: S,
    ) -> Result<Self, std::io::Error> {
        let identify_config =
            identify::Config::new("/defra/identify/0.0.1".to_string(), local_public_key)
                .with_agent_version(format!("defradb-rs/{}", env!("CARGO_PKG_VERSION")));

        let identify = identify::Behaviour::new(identify_config);
        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

        // NOTE: Do NOT register rep_request or rep_response protocols here
        // because stream::Behaviour handles those for Go two-stream compatibility.
        let pushlog = request_response::Behaviour::new(
            std::iter::empty::<(StreamProtocol, ProtocolSupport)>(),
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

        let gossipsub =
            gossipsub::Behaviour::new(MessageAuthenticity::RandomAuthor, gossipsub_config)
                .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("gossipsub creation error: {}", e),
                    )
                })?;

        // Configure Kademlia DHT for peer discovery
        let kad_store = MemoryStore::new(local_peer_id);
        let mut kademlia = kad::Behaviour::new(local_peer_id, kad_store);
        kademlia.set_mode(Some(Mode::Server));

        // Configure Bitswap for block exchange
        let executor: Box<dyn FnOnce(BoxFuture<'static, ()>)> = Box::new(|fut| {
            tokio::spawn(fut);
        });
        let bitswap = Bitswap::new(BitswapConfig::default(), bitswap_store, executor);

        // Configure stream behaviour for Go two-stream compatibility
        let stream = stream::Behaviour::new();

        Ok(Self {
            identify,
            mdns,
            kademlia,
            bitswap,
            pushlog,
            gossipsub,
            stream,
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

    // === Bitswap operations ===

    /// Add a peer address for Bitswap block exchange.
    pub fn add_bitswap_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.bitswap.add_address(peer_id, addr);
    }

    /// Remove a peer address from Bitswap.
    pub fn remove_bitswap_address(&mut self, peer_id: &PeerId, addr: &Multiaddr) {
        self.bitswap.remove_address(peer_id, addr);
    }

    /// Start a Bitswap get query to fetch a block by CID.
    ///
    /// Returns a query ID that can be used to track progress and completion.
    pub fn bitswap_get(
        &mut self,
        cid: libipld::Cid,
        peers: impl Iterator<Item = PeerId>,
    ) -> QueryId {
        self.bitswap.get(cid, peers)
    }

    /// Start a Bitswap sync query to fetch a block and all its linked blocks.
    ///
    /// This is used to sync a DAG starting from a root CID.
    /// The `missing` iterator should contain the initial set of missing CIDs.
    pub fn bitswap_sync(
        &mut self,
        cid: libipld::Cid,
        peers: Vec<PeerId>,
        missing: impl Iterator<Item = libipld::Cid>,
    ) -> QueryId {
        self.bitswap.sync(cid, peers, missing)
    }

    /// Cancel an in-progress Bitswap query.
    ///
    /// Returns `true` if a query was cancelled.
    pub fn bitswap_cancel(&mut self, id: QueryId) -> bool {
        self.bitswap.cancel(id)
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

        let behaviour = DefraBehaviour::new(peer_id, public_key, keypair, store);
        assert!(behaviour.is_ok());
    }

    #[tokio::test]
    async fn test_behaviour_creation_without_signing() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let public_key = keypair.public();
        let store = MockBitswapStore::new();

        let behaviour = DefraBehaviour::new_without_signing(peer_id, public_key, store);
        assert!(behaviour.is_ok());
    }
}
