//! Iroh QUIC-native P2P transport for DefraDB.
//!
//! This module provides an alternative to the libp2p transport, using iroh's
//! QUIC-based networking stack. It is feature-gated behind `iroh-transport`.
//!
//! # Architecture
//!
//! - `IrohTransport`: Thin `Clone + Send + Sync` facade implementing `P2PTransport`
//! - `IrohEndpoint`: Background tokio task owning all iroh state
//! - Communication via `IrohCommand` enum over mpsc channel

mod addr;
mod command;
mod config;
mod endpoint;
mod endpoint_commands;
mod endpoint_config;
mod endpoint_rpc;
mod endpoint_streams;
mod gossip_heal;
mod peer_map;
mod protocols;
mod secret_key;
mod transport;
#[cfg(test)]
mod two_stream_tests;

pub use addr::{
    best_shareable_public_addr, canonical_peer_id, endpoint_addr_from_parts,
    endpoint_ticket_string, format_public_listen_addrs, is_ticket_string, parse_canonical_peer_id,
    parse_public_peer_addr,
};
pub use config::{IrohDiscoveryConfig, IrohRelayModeConfig};
pub use endpoint::spawn_endpoint;
pub use endpoint_config::IrohEndpointConfig;
pub use gossip_heal::GossipHealConfig;
pub use secret_key::load_or_generate_secret_key;
pub use transport::IrohTransport;
