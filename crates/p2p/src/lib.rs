// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
//! - [`error`] - Error types
//!
//! # Example
//!
//! ```rust,no_run
//! use p2p::{P2PHost, P2PHostHandle};
//!
//! #[tokio::main]
//! async fn main() -> p2p::Result<()> {
//!     // Create a new P2P host
//!     let (host, handle, mut events) = P2PHost::new()?;
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

pub mod behaviour;
pub mod codec;
pub mod error;
pub mod host;
pub mod message;
pub mod protocol;

// Re-export main types for convenience
pub use error::{Error, Result};
pub use host::{HostCommand, HostEvent, P2PHost, P2PHostHandle, ResponseChannel};
pub use message::{Message, MetaData, PushLogReply, PushLogRequest};
pub use protocol::{CODE, MESSAGE_VERSION, NAME, PROTOCOL_ID, VERSION};

// Re-export commonly used libp2p types
pub use libp2p::{Multiaddr, PeerId};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exports() {
        // Verify key constants are accessible
        assert_eq!(PROTOCOL_ID, "/defra/0.0.1");
        assert_eq!(CODE, 961);
        assert_eq!(NAME, "defra");
        assert_eq!(VERSION, "0.0.1");
    }
}
