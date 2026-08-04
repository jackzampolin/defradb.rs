//! LAN/WAN Kademlia composition and pk record validation.

use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::task::{Context, Poll};

use libp2p::{
    core::{transport::PortUse, Endpoint},
    identity::PublicKey,
    kad::{self, store::MemoryStore},
    swarm::{
        ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent,
        THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId, StreamProtocol,
};

/// Kademlia query parallelism. Matches Go's `dht.Concurrency(10)`
/// (`go-p2p/host.go:44`). rust-libp2p defaults to `ALPHA_VALUE` = 3.
const KAD_PARALLELISM: NonZeroUsize = match NonZeroUsize::new(10) {
    Some(n) => n,
    None => unreachable!(),
};

/// Go dualdht LAN protocol ID (`ProtocolExtension("/lan")`).
pub const LAN_KAD_PROTOCOL: &str = "/ipfs/lan/kad/1.0.0";

/// WAN protocol ID for the Rust dual-DHT split.
///
/// go-libp2p's dualdht keeps the WAN side on `/ipfs/kad/1.0.0` and uses
/// address/query filters that rust-libp2p does not expose. We give the WAN DHT
/// its own protocol ID so the two rust-libp2p routing tables remain explicit
/// and non-overlapping.
pub const WAN_KAD_PROTOCOL: &str = "/ipfs/wan/kad/1.0.0";

const PK_RECORD_PREFIX: &[u8] = b"/pk/";

/// Which side of the dual Kademlia routing split produced an event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KademliaNetwork {
    Lan,
    Wan,
}

impl KademliaNetwork {
    pub fn as_str(self) -> &'static str {
        match self {
            KademliaNetwork::Lan => "lan",
            KademliaNetwork::Wan => "wan",
        }
    }

    fn protocol(self) -> &'static str {
        match self {
            KademliaNetwork::Lan => LAN_KAD_PROTOCOL,
            KademliaNetwork::Wan => WAN_KAD_PROTOCOL,
        }
    }
}

/// A Kademlia event tagged with the routing table that emitted it.
#[derive(Debug)]
pub enum DefraKademliaEvent {
    Lan(kad::Event),
    Wan(kad::Event),
}

impl DefraKademliaEvent {
    pub fn split(self) -> (KademliaNetwork, kad::Event) {
        match self {
            DefraKademliaEvent::Lan(event) => (KademliaNetwork::Lan, event),
            DefraKademliaEvent::Wan(event) => (KademliaNetwork::Wan, event),
        }
    }
}

/// Kademlia behaviour wrapper that tags events with their LAN/WAN origin.
pub struct DefraKademlia {
    network: KademliaNetwork,
    inner: kad::Behaviour<MemoryStore>,
}

impl DefraKademlia {
    fn new(local_peer_id: PeerId, network: KademliaNetwork) -> Self {
        let kad_store = MemoryStore::new(local_peer_id);
        let mut kad_config = kad::Config::new(StreamProtocol::new(network.protocol()));
        // Match Go DefraDB's `dht.Concurrency(10)` (`go-p2p/host.go:44`).
        // rust-libp2p defaults to ALPHA_VALUE = 3.
        kad_config.set_parallelism(KAD_PARALLELISM);
        // rust-libp2p has no pluggable record validator. FilterBoth emits
        // inbound records so the host can apply the pk namespace validator
        // before explicitly storing accepted records.
        kad_config.set_record_filtering(kad::StoreInserts::FilterBoth);

        let mut inner = kad::Behaviour::with_config(local_peer_id, kad_store, kad_config);
        // Go's dual DHT forces the LAN side into server mode whenever the WAN
        // side is not explicitly client-only.
        match network {
            KademliaNetwork::Lan => inner.set_mode(Some(kad::Mode::Server)),
            KademliaNetwork::Wan => inner.set_mode(None),
        }

        Self { network, inner }
    }
}

// Deref is only for ergonomic access to kad methods; swarm dispatch uses the
// explicit NetworkBehaviour impl below.
impl Deref for DefraKademlia {
    type Target = kad::Behaviour<MemoryStore>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for DefraKademlia {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl NetworkBehaviour for DefraKademlia {
    type ConnectionHandler = <kad::Behaviour<MemoryStore> as NetworkBehaviour>::ConnectionHandler;
    type ToSwarm = DefraKademliaEvent;

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.inner
            .handle_pending_inbound_connection(connection_id, local_addr, remote_addr)
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.inner.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr],
        effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        self.inner.handle_pending_outbound_connection(
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
        port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.inner.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
            port_use,
        )
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        self.inner.on_swarm_event(event);
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.inner
            .on_connection_handler_event(peer_id, connection_id, event);
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        self.inner.poll(cx).map(|event| {
            event.map_out(|event| match self.network {
                KademliaNetwork::Lan => DefraKademliaEvent::Lan(event),
                KademliaNetwork::Wan => DefraKademliaEvent::Wan(event),
            })
        })
    }
}

/// Separate LAN and WAN Kademlia routing tables.
///
/// This emulates go-libp2p's `dualdht.DHT` shape with two concrete DHTs.
/// rust-libp2p does not expose Go's public/private routing-table or
/// query filters, so this split is limited to independent routing tables and
/// protocol IDs. Addresses are inserted into both tables, which means the LAN
/// table can contain WAN-only peers and the WAN table can contain LAN/private
/// peers.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "DefraKademliaEvent")]
pub struct DualKademlia {
    pub lan: DefraKademlia,
    pub wan: DefraKademlia,
}

impl DualKademlia {
    pub(crate) fn new(local_peer_id: PeerId) -> Self {
        Self {
            lan: DefraKademlia::new(local_peer_id, KademliaNetwork::Lan),
            wan: DefraKademlia::new(local_peer_id, KademliaNetwork::Wan),
        }
    }

    /// Insert an address into both routing tables.
    ///
    /// go-libp2p dualdht classifies public/private addresses before routing
    /// them to WAN/LAN tables. rust-libp2p 0.53 does not expose those filters,
    /// so this emulation stores every address in both tables and accepts the
    /// resulting query-routing divergence.
    pub fn add_address(
        &mut self,
        peer: &PeerId,
        address: Multiaddr,
    ) -> (kad::RoutingUpdate, kad::RoutingUpdate) {
        let lan = self.lan.add_address(peer, address.clone());
        let wan = self.wan.add_address(peer, address);
        (lan, wan)
    }

    pub fn remove_peer(&mut self, peer: &PeerId) {
        self.lan.remove_peer(peer);
        self.wan.remove_peer(peer);
    }

    pub fn bootstrap(&mut self) -> [(KademliaNetwork, Result<kad::QueryId, kad::NoKnownPeers>); 2] {
        [
            (KademliaNetwork::Lan, self.lan.bootstrap()),
            (KademliaNetwork::Wan, self.wan.bootstrap()),
        ]
    }

    pub fn store_mut(&mut self, network: KademliaNetwork) -> &mut MemoryStore {
        match network {
            KademliaNetwork::Lan => self.lan.store_mut(),
            KademliaNetwork::Wan => self.wan.store_mut(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum PublicKeyRecordValidationError {
    InvalidPeerId(String),
    InvalidPublicKey(String),
    PeerIdMismatch { expected: String, actual: String },
}

impl std::fmt::Display for PublicKeyRecordValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublicKeyRecordValidationError::InvalidPeerId(error) => {
                write!(f, "invalid pk record peer id: {error}")
            }
            PublicKeyRecordValidationError::InvalidPublicKey(error) => {
                write!(f, "invalid pk record public key: {error}")
            }
            PublicKeyRecordValidationError::PeerIdMismatch { expected, actual } => write!(
                f,
                "pk record public key does not match key: expected {expected}, got {actual}"
            ),
        }
    }
}

impl std::error::Error for PublicKeyRecordValidationError {}

/// Validate Go-compatible `/pk/<peer-id>` public-key records.
///
/// Records outside the `/pk/` namespace are accepted because Rust DefraDB does
/// not yet have Go's full `NamespacedValidator` registry. Today only `/pk/`
/// records are validated before explicit storage.
pub fn validate_pk_namespaced_record(
    record: &kad::Record,
) -> Result<(), PublicKeyRecordValidationError> {
    let Some(peer_id_bytes) = record.key.as_ref().strip_prefix(PK_RECORD_PREFIX) else {
        return Ok(());
    };

    let expected = PeerId::from_bytes(peer_id_bytes)
        .map_err(|error| PublicKeyRecordValidationError::InvalidPeerId(error.to_string()))?;
    let public_key = PublicKey::try_decode_protobuf(&record.value)
        .map_err(|error| PublicKeyRecordValidationError::InvalidPublicKey(error.to_string()))?;
    let actual = public_key.to_peer_id();

    if actual != expected {
        return Err(PublicKeyRecordValidationError::PeerIdMismatch {
            expected: expected.to_string(),
            actual: actual.to_string(),
        });
    }

    Ok(())
}
