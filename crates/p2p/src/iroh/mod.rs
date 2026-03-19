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
mod peer_map;
mod protocols;
mod transport;

pub use addr::{
    endpoint_addr_from_parts, endpoint_ticket_string, format_public_listen_addrs, is_ticket_string,
    parse_public_peer_addr,
};
pub use config::{IrohDiscoveryConfig, IrohRelayModeConfig};
pub use endpoint::{spawn_endpoint, IrohEndpointConfig};
pub use transport::IrohTransport;
