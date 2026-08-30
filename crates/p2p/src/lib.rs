//! P2P networking crate for DefraDB.
//!
//! This crate provides peer-to-peer networking capabilities for DefraDB,
//! enabling CRDT synchronization between nodes using libp2p.
//!
//! # Architecture
//!
//! The crate is organized into the following modules:
//!
//! - [`protocol`] - Protocol constants and identifiers
//! - [`message`] - Wire message types (CBOR-encoded)
//! - [`codec`] - CBOR codec for request-response protocol
//! - [`behaviour`] - Composite NetworkBehaviour
//! - [`host`] - P2P host that manages the swarm
//! - [`transport`] - Transport-agnostic trait and types
//! - [`topics`] - GossipSub topic definitions
//! - [`error`] - Error types
//!
//! # Example
//!
//! ```rust,ignore
//! use p2p::{P2PHost, P2PHostHandle, BitswapStoreAdapter};
//! use blockstore::DefraBlockstore;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> p2p::Result<()> {
//!     // Create a blockstore for Bitswap
//!     let blockstore = Arc::new(DefraBlockstore::new(/* storage backend */));
//!     let bitswap_store = BitswapStoreAdapter::new(blockstore);
//!
//!     // Create a new P2P host with the blockstore
//!     let (host, handle, mut events) = P2PHost::new(bitswap_store)?;
//!
//!     // Spawn the host event loop
//!     tokio::spawn(host.run());
//!
//!     // Start listening
//!     handle.listen("/ip4/0.0.0.0/tcp/9000".parse().unwrap()).await?;
//!
//!     // Handle events
//!     while let Some(event) = events.recv().await {
//!         println!("Event: {:?}", event);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Wire Compatibility
//!
//! This implementation is wire-compatible with the Go implementation:
//! - Protocol ID: `/defra/0.0.1` (multicodec 961)
//! - Messages: CBOR encoded using serde

#[cfg(feature = "libp2p-transport")]
pub mod address;
#[cfg(feature = "libp2p-transport")]
pub mod behaviour;
pub mod bitswap;
#[cfg(feature = "libp2p-transport")]
pub mod codec;
pub mod error;
mod explicit_replay;
#[cfg(feature = "libp2p-transport")]
pub mod host;
#[cfg(feature = "kms")]
pub mod kms;
pub mod manage_correlator;
pub mod message;
pub mod peer_identity;
pub mod protocol;
#[cfg(any(feature = "kms", feature = "libp2p-transport"))]
pub mod pubsub_rpc;
pub mod replicator;
pub mod se_correlator;
pub mod signing;
pub mod sync;
#[cfg(all(any(test, feature = "test-utils"), feature = "libp2p-transport"))]
pub mod testutil;
pub mod topics;
pub mod transport;
#[cfg(feature = "libp2p-transport")]
pub mod two_stream;

#[cfg(feature = "iroh-transport")]
pub mod iroh;

// Re-export address parsing
#[cfg(feature = "libp2p-transport")]
pub use address::{parse_multiaddr_with_peer_id, ParsedMultiaddr};

// Re-export main types for convenience
pub use error::{Error, Result};
pub use explicit_replay::{
    generate_capability as generate_explicit_replay_capability,
    generate_capability_from_claims as generate_explicit_replay_capability_from_claims,
    is_capability_revoked as is_explicit_replay_capability_revoked,
    revoke_capability as revoke_explicit_replay_capability,
    verify_capability as verify_explicit_replay_capability,
    verify_capability_for_key_request as verify_explicit_replay_capability_for_key_request,
    verify_capability_with_revocations as verify_explicit_replay_capability_with_revocations,
    ExplicitReplayAuthorization, ExplicitReplayCapabilityClaims, ExplicitReplayRevocationRegistry,
    DEFAULT_CAPABILITY_TTL as DEFAULT_EXPLICIT_REPLAY_CAPABILITY_TTL,
    MAX_CAPABILITY_TTL as MAX_EXPLICIT_REPLAY_CAPABILITY_TTL,
};
#[cfg(feature = "libp2p-transport")]
pub use host::{
    convert_host_event, HostCommand, HostEvent, Libp2pTransport, P2PHost, P2PHostConfig,
    P2PHostHandle, ResponseChannel,
};
pub use message::{Message, MetaData, PushLogBroadcast, PushLogReply, PushLogRequest};
#[cfg(feature = "libp2p-transport")]
pub use peer_identity::HandlePeerIdentityResolver;
#[cfg(feature = "iroh-transport")]
pub use peer_identity::IrohPeerIdentityResolver;
pub use peer_identity::{AnonymousResolver, PeerIdentityResolver};
pub use protocol::{
    BASE_PROTOCOL_ID, CODE, MESSAGE_VERSION, NAME, PROTOCOL_BASE, REP_REQUEST_PROTOCOL,
    REP_RESPONSE_PROTOCOL, VERSION,
};

// Re-export deprecated aliases for backwards compatibility
#[allow(deprecated)]
pub use protocol::{PUSHLOG_REQUEST_PROTOCOL, PUSHLOG_RESPONSE_PROTOCOL};

// Re-export signing functions
pub use signing::sign_with_transport;
#[cfg(feature = "libp2p-transport")]
pub use signing::{sign_message, sign_message_cloned, verify_message};

// Re-export topic types
pub use topics::{DefraTopic, DOC_SYNC_TOPIC, ENCRYPTION_TOPIC, SYNC_BRANCHABLE_TOPIC};

// Re-export transport types
pub use transport::{P2PTransport, TransportEvent};

// Re-export sync types
#[cfg(feature = "iroh-transport")]
pub use sync::IrohSyncCoordinator;
#[cfg(feature = "libp2p-transport")]
pub use sync::Libp2pSyncCoordinator;
pub use sync::{
    Broadcaster, CreateReplicatorResult, DagSync, DagSyncConfig, DagSyncState,
    LoadReplicatorsResult, NeedsFetchData, PeerStateTracker, ProcessQueue, SyncConfig,
    SyncCoordinator, SyncEvent, SyncManager, SyncPlan,
};

// Re-export bitswap types
#[cfg(feature = "libp2p-transport")]
pub use bitswap::BitswapStoreAdapter;
pub use bitswap::{AccessMode, ReplicatorRegistry};
#[cfg(feature = "libp2p-transport")]
pub use iroh_bitswap::Store as BitswapStore;

/// Query ID for tracking Bitswap operations.
/// This is a simple wrapper that allows correlating sync requests with completions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryId(pub u64);

// Re-export replicator types
pub use replicator::{
    EqOnlyFilterMatcher, ReplicationFilter, ReplicationFilterMatcher, ReplicationFilters,
    ReplicatorInfo, ReplicatorStatus,
};

// Re-export management correlators
pub use manage_correlator::{
    ManageCorrelator, ManageQueryCorrelator, PendingManage, PendingManageQuery,
};

// Re-export SE query correlator
pub use se_correlator::{PendingSeQuery, SeQueryCorrelator};

// Re-export two-stream protocol types
#[cfg(feature = "libp2p-transport")]
pub use two_stream::{TwoStreamEvent, TwoStreamHandler, TwoStreamRunner};

// Re-export commonly used libp2p types
#[cfg(feature = "libp2p-transport")]
pub use libp2p::{gossipsub, identity::Keypair, Multiaddr, PeerId};
