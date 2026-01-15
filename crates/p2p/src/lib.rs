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

pub mod behaviour;
pub mod bitswap;
pub mod codec;
pub mod error;
pub mod host;
pub mod message;
pub mod protocol;
pub mod signing;
pub mod sync;
#[cfg(any(test, feature = "test-utils"))]
pub mod testutil;
pub mod topics;

// Re-export main types for convenience
pub use error::{Error, Result};
pub use host::{HostCommand, HostEvent, P2PHost, P2PHostHandle, ResponseChannel};
pub use message::{Message, MetaData, PushLogBroadcast, PushLogReply, PushLogRequest};
pub use protocol::{
    BASE_PROTOCOL_ID, CODE, MESSAGE_VERSION, NAME, PROTOCOL_BASE, REP_REQUEST_PROTOCOL,
    REP_RESPONSE_PROTOCOL, VERSION,
};

// Re-export deprecated aliases for backwards compatibility
#[allow(deprecated)]
pub use protocol::{PUSHLOG_REQUEST_PROTOCOL, PUSHLOG_RESPONSE_PROTOCOL};

// Re-export signing functions
pub use signing::{sign_message, sign_message_cloned, verify_message};

// Re-export topic types
pub use topics::{DefraTopic, DOC_SYNC_TOPIC, ENCRYPTION_TOPIC};

// Re-export sync types
pub use sync::{
    Broadcaster, DagSync, DagSyncConfig, DagSyncState, ProcessQueue, SyncConfig, SyncCoordinator,
    SyncEvent, SyncManager, SyncPlan,
};

// Re-export bitswap types
pub use bitswap::{BitswapStoreAdapter, BlockAccessController, BlockAccessFn, ReplicatorRegistry};
pub use libp2p_bitswap_next::BitswapStore;

// Re-export commonly used libp2p types
pub use libp2p::{gossipsub, Multiaddr, PeerId};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exports() {
        // Verify key constants are accessible
        assert_eq!(BASE_PROTOCOL_ID, "/defra/0.0.1");
        assert_eq!(REP_REQUEST_PROTOCOL, "/defradb/rep_req/0.0.1");
        assert_eq!(REP_RESPONSE_PROTOCOL, "/defradb/rep_resp/0.0.1");
        assert_eq!(CODE, 961);
        assert_eq!(NAME, "defra");
        assert_eq!(VERSION, "0.0.1");
    }
}
