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

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use iroh_bitswap::{Bitswap, BitswapEvent, Config as BitswapConfig, Store};
use libp2p::{
    connection_limits::{self, ConnectionLimits},
    gossipsub::{self, MessageAuthenticity, MessageId, ValidationMode},
    identify, memory_connection_limits, relay,
    request_response::{self, ProtocolSupport},
    swarm::{behaviour::toggle::Toggle, NetworkBehaviour},
    PeerId, StreamProtocol,
};
use libp2p_stream as stream;

use libp2p::identity::Keypair;

use crate::bitswap::{
    make_peer_block_access_filter, AccessMode, BlockClassifier, LateBoundServeAcp,
    ReplicatorRegistry,
};
use crate::codec::PushLogCodec;
use crate::message::{PushLogReply, PushLogRequest};

mod dual_kademlia;

pub use dual_kademlia::{
    validate_pk_namespaced_record, DefraKademliaEvent, DualKademlia, KademliaNetwork,
    PublicKeyRecordValidationError, LAN_KAD_PROTOCOL, WAN_KAD_PROTOCOL,
};

/// Timeout for PushLog requests.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Enough for the common identity multihash plus an 8-byte sequence number.
const GO_MESSAGE_ID_CAPACITY: usize = 48;

/// Pending dial/listen limits are intentionally separate from the exposed
/// established-connection watermarks and follow the Go default shape.
const MAX_PENDING_INCOMING_CONNECTIONS: u32 = 100;
const MAX_PENDING_OUTGOING_CONNECTIONS: u32 = 400;

/// Go-compatible gossipsub message ID derivation.
///
/// go-libp2p-pubsub@v0.15.0 pubsub.go:1356 uses raw `from || seqno` bytes.
/// rust-libp2p's default uses base58/decimal strings, which breaks cross-impl
/// IHAVE/IWANT parity.
///
/// This requires message authenticity modes that populate source and sequence
/// number; anonymous messages would all collapse to the empty ID.
pub fn go_compatible_gossipsub_message_id(message: &gossipsub::Message) -> MessageId {
    let mut id = Vec::with_capacity(GO_MESSAGE_ID_CAPACITY);
    if let Some(peer_id) = message.source {
        id.extend_from_slice(&peer_id.to_bytes());
    }
    if let Some(seqno) = message.sequence_number {
        id.extend_from_slice(&seqno.to_be_bytes());
    }
    MessageId::from(id)
}

/// Composite network behaviour for DefraDB nodes.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "DefraEvent")]
pub struct DefraBehaviour<S: Store> {
    /// Peer identification protocol.
    pub identify: identify::Behaviour,

    /// LAN and WAN Kademlia DHTs for peer discovery and content routing.
    pub kademlia: DualKademlia,

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

    /// Hard connection limits for pending handshakes and per-peer fan-out.
    /// Total established connection pruning is handled by `P2PHost` so new
    /// peers are not rejected before the Go-compatible low/high-water pruner
    /// can trim older connections.
    pub connection_limits: connection_limits::Behaviour,

    /// Process-memory guard approximating Go's ResourceManager system scope.
    ///
    /// rust-libp2p does not expose go-libp2p ResourceManager scopes for
    /// transient, per-peer, service, or protocol memory/FD accounting. This
    /// behaviour can only refuse new connections once process physical memory
    /// exceeds the Go-compatible system budget. Existing connection count
    /// limits cover pending/per-peer counts, but the Go transient 25% byte
    /// scope and service/protocol/peer byte scopes have no Rust equivalent
    /// here.
    pub memory_connection_limits: memory_connection_limits::Behaviour,
}

/// Events emitted by the DefraDB network behaviour.
#[allow(clippy::large_enum_variant)]
#[non_exhaustive]
pub enum DefraEvent {
    /// Identify protocol event.
    Identify(identify::Event),

    /// Kademlia DHT event.
    Kademlia(DefraKademliaEvent),

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

impl From<DefraKademliaEvent> for DefraEvent {
    fn from(event: DefraKademliaEvent) -> Self {
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
        unreachable!("stream::Behaviour should not emit events")
    }
}

impl From<Infallible> for DefraEvent {
    fn from(event: Infallible) -> Self {
        match event {}
    }
}

impl<S: Store + Clone + Send + Sync + 'static> DefraBehaviour<S> {
    /// Create a new DefraDB network behaviour with message signing enabled.
    ///
    /// # Arguments
    ///
    /// * `local_peer_id` - The local peer's ID
    /// * `local_public_key` - The local peer's public key
    /// * `keypair` - The keypair for message signing/verification
    /// * `bitswap_store` - The blockstore for Bitswap block exchange
    /// * `access_mode` - Controls whether the Bitswap filter enforces
    ///   per-peer access control on served blocks
    /// * `replicators` - Registry used by the filter to authorize peers
    ///   per collection (shared with SyncCoordinator)
    ///
    /// # Returns
    ///
    /// A new `DefraBehaviour` instance or an error if initialization fails.
    ///
    /// # Note
    ///
    /// This must be called within a tokio runtime context as Bitswap spawns
    /// background tasks for operations.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        local_peer_id: PeerId,
        local_public_key: libp2p::identity::PublicKey,
        keypair: Keypair,
        bitswap_store: S,
        access_mode: AccessMode,
        replicators: Arc<ReplicatorRegistry>,
        classifier: Arc<dyn BlockClassifier>,
        serve_acp: Arc<LateBoundServeAcp>,
        enable_pubsub: bool,
        config: &super::P2PHostConfig,
        resource_manager_system_memory_budget_bytes: usize,
    ) -> Result<Self, crate::error::Error> {
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
                // See `go_compatible_gossipsub_message_id`.
                .message_id_fn(go_compatible_gossipsub_message_id)
                // Enable peer exchange (PX) in PRUNE messages — matches Go's
                // pubsub.WithPeerExchange(true). Allows peers sharing a topic
                // to discover each other through mesh management.
                .do_px()
                .flood_publish(true)
                .build()
                .map_err(|e| crate::error::Error::GossipSubConfig(e.to_string()))?;

            let gs =
                gossipsub::Behaviour::new(MessageAuthenticity::Signed(keypair), gossipsub_config)
                    .map_err(|e| crate::error::Error::GossipSubConfig(e.to_string()))?;
            Toggle::from(Some(gs))
        } else {
            Toggle::from(None)
        };

        let kademlia = DualKademlia::new(local_peer_id);

        // Configure Bitswap for block exchange (Go compatibility).
        // iroh-bitswap implements the standard IPFS bitswap protocols.
        //
        // Install a per-peer block-request filter to enforce ACP on the
        // egress path. Without this, any connected peer could fetch any
        // block in the blockstore — see #830. Matches Go's
        // `bitswap.WithPeerBlockRequestFilter(hasAccess)` wiring
        // (`go-p2p/peer.go:146`).
        let filter = make_peer_block_access_filter(
            access_mode,
            Arc::clone(&replicators),
            bitswap_store.clone(),
            classifier,
            serve_acp,
        );
        let mut bitswap_config = BitswapConfig::default();
        if let Some(server_cfg) = bitswap_config.server.as_mut() {
            server_cfg.decision_config.peer_block_request_filter = Some(Box::new(filter));
        }
        let bitswap = Bitswap::new(local_peer_id, bitswap_store, bitswap_config).await;

        // Configure stream behaviour for Go two-stream compatibility
        let stream = stream::Behaviour::new();

        let limits = ConnectionLimits::default()
            .with_max_pending_incoming(Some(MAX_PENDING_INCOMING_CONNECTIONS))
            .with_max_pending_outgoing(Some(MAX_PENDING_OUTGOING_CONNECTIONS))
            .with_max_established_per_peer(Some(config.max_connections_per_peer));
        let connection_limits = connection_limits::Behaviour::new(limits);
        let memory_connection_limits = memory_connection_limits::Behaviour::with_max_bytes(
            resource_manager_system_memory_budget_bytes,
        );

        Ok(Self {
            identify,
            kademlia,
            bitswap,
            pushlog,
            gossipsub,
            relay: Toggle::from(None),
            stream,
            connection_limits,
            memory_connection_limits,
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
    ) -> Result<Self, crate::error::Error> {
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

        // Configure GossipSub (optional, matches Go's `if options.EnablePubSub`).
        let gossipsub = if enable_pubsub {
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(1))
                .validation_mode(ValidationMode::Permissive)
                // See `go_compatible_gossipsub_message_id`.
                .message_id_fn(go_compatible_gossipsub_message_id)
                .build()
                .map_err(|e| crate::error::Error::GossipSubConfig(e.to_string()))?;

            let gs = gossipsub::Behaviour::new(MessageAuthenticity::RandomAuthor, gossipsub_config)
                .map_err(|e| crate::error::Error::GossipSubConfig(e.to_string()))?;
            Toggle::from(Some(gs))
        } else {
            Toggle::from(None)
        };

        let kademlia = DualKademlia::new(local_peer_id);

        // Configure Bitswap for block exchange
        let bitswap_config = BitswapConfig::default();
        let bitswap = Bitswap::new(local_peer_id, bitswap_store, bitswap_config).await;

        // Configure stream behaviour for Go two-stream compatibility
        let stream = stream::Behaviour::new();

        // Configure hard pending/per-peer limits; total connection pruning
        // happens at the host layer to match Go's active conn manager.
        let limits = ConnectionLimits::default()
            .with_max_pending_incoming(Some(100))
            .with_max_pending_outgoing(Some(100))
            .with_max_established_per_peer(Some(4));
        let connection_limits = connection_limits::Behaviour::new(limits);
        let memory_connection_limits =
            memory_connection_limits::Behaviour::with_max_bytes(usize::MAX);

        Ok(Self {
            identify,
            kademlia,
            bitswap,
            pushlog,
            gossipsub,
            relay: Toggle::from(None),
            stream,
            connection_limits,
            memory_connection_limits,
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
            Some(gs) => Ok(gs.unsubscribe(topic)),
            None => Ok(false),
        }
    }

    /// Publish a message to a GossipSub topic.
    ///
    /// Returns the message ID on success, or `NoPeersSubscribedToTopic` if pubsub is disabled.
    pub fn publish(
        &mut self,
        topic: gossipsub::IdentTopic,
        data: Vec<u8>,
    ) -> Result<gossipsub::MessageId, gossipsub::PublishError> {
        match self.gossipsub.as_mut() {
            Some(gs) => gs.publish(topic, data),
            None => Err(gossipsub::PublishError::NoPeersSubscribedToTopic),
        }
    }

    /// Get the list of subscribed topics.
    pub fn subscribed_topics(&self) -> Box<dyn Iterator<Item = &gossipsub::TopicHash> + '_> {
        match self.gossipsub.as_ref() {
            Some(gs) => Box::new(gs.topics()),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Get all peers known to GossipSub for a topic (mesh + non-mesh).
    pub fn topic_peers(&self, topic_hash: &gossipsub::TopicHash) -> Vec<PeerId> {
        match self.gossipsub.as_ref() {
            Some(gs) => gs
                .all_peers()
                .filter(|(_, topics)| topics.contains(&topic_hash))
                .map(|(peer_id, _)| *peer_id)
                .collect(),
            None => Vec::new(),
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
    use libp2p::kad;
    use libp2p::kad::store::RecordStore;

    fn pk_record_key(peer_id: PeerId) -> Vec<u8> {
        let mut key = b"/pk/".to_vec();
        key.extend_from_slice(&peer_id.to_bytes());
        key
    }

    #[tokio::test]
    async fn test_behaviour_creation_with_signing() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let public_key = keypair.public();
        let store = MockBitswapStore::new();

        let config = crate::P2PHostConfig::default();
        let registry = Arc::new(ReplicatorRegistry::new());
        let behaviour = DefraBehaviour::new(
            peer_id,
            public_key,
            keypair,
            store,
            AccessMode::Open,
            registry,
            Arc::new(crate::bitswap::DefaultBlockClassifier),
            Arc::new(crate::bitswap::LateBoundServeAcp::new()),
            true,
            &config,
            usize::MAX,
        )
        .await;
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
        let mut behaviour = behaviour.unwrap();

        assert_eq!(
            behaviour.kademlia.lan.protocol_names()[0].as_ref(),
            LAN_KAD_PROTOCOL
        );
        assert_eq!(
            behaviour.kademlia.wan.protocol_names()[0].as_ref(),
            WAN_KAD_PROTOCOL
        );

        let record = kad::Record::new(b"/v/split".to_vec(), b"lan".to_vec());
        let key = record.key.clone();
        behaviour
            .kademlia
            .store_mut(KademliaNetwork::Lan)
            .put(record)
            .unwrap();

        assert!(behaviour
            .kademlia
            .store_mut(KademliaNetwork::Lan)
            .get(&key)
            .is_some());
        assert!(behaviour
            .kademlia
            .store_mut(KademliaNetwork::Wan)
            .get(&key)
            .is_none());
    }

    #[tokio::test]
    async fn test_behaviour_creation_pubsub_disabled() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let public_key = keypair.public();
        let store = MockBitswapStore::new();

        let config = crate::P2PHostConfig::default();
        let registry = Arc::new(ReplicatorRegistry::new());
        let behaviour = DefraBehaviour::new(
            peer_id,
            public_key,
            keypair,
            store,
            AccessMode::Open,
            registry,
            Arc::new(crate::bitswap::DefaultBlockClassifier),
            Arc::new(crate::bitswap::LateBoundServeAcp::new()),
            false,
            &config,
            usize::MAX,
        )
        .await;
        assert!(behaviour.is_ok());
        let behaviour = behaviour.unwrap();
        assert!(behaviour.gossipsub.as_ref().is_none());
    }

    #[test]
    fn pk_namespaced_validator_accepts_matching_public_key() {
        let keypair = Keypair::generate_ed25519();
        let public_key = keypair.public();
        let record = kad::Record::new(
            pk_record_key(public_key.to_peer_id()),
            public_key.encode_protobuf(),
        );

        assert_eq!(validate_pk_namespaced_record(&record), Ok(()));
    }

    #[test]
    fn pk_namespaced_validator_rejects_mismatched_public_key() {
        let keypair = Keypair::generate_ed25519();
        let other_keypair = Keypair::generate_ed25519();
        let record = kad::Record::new(
            pk_record_key(keypair.public().to_peer_id()),
            other_keypair.public().encode_protobuf(),
        );

        assert!(matches!(
            validate_pk_namespaced_record(&record),
            Err(PublicKeyRecordValidationError::PeerIdMismatch { .. })
        ));
    }

    #[test]
    fn pk_namespaced_validator_rejects_invalid_peer_id_key() {
        let record = kad::Record::new(b"/pk/not-a-peer-id".to_vec(), b"not checked".to_vec());

        assert!(matches!(
            validate_pk_namespaced_record(&record),
            Err(PublicKeyRecordValidationError::InvalidPeerId(_))
        ));
    }

    #[test]
    fn pk_namespaced_validator_rejects_invalid_public_key() {
        let keypair = Keypair::generate_ed25519();
        let record = kad::Record::new(
            pk_record_key(keypair.public().to_peer_id()),
            b"not protobuf".to_vec(),
        );

        assert!(matches!(
            validate_pk_namespaced_record(&record),
            Err(PublicKeyRecordValidationError::InvalidPublicKey(_))
        ));
    }

    #[test]
    fn pk_namespaced_validator_ignores_other_namespaces() {
        let record = kad::Record::new(b"/v/hello".to_vec(), b"not a public key".to_vec());

        assert_eq!(validate_pk_namespaced_record(&record), Ok(()));
    }
}
