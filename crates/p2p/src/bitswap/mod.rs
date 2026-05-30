//! Bitswap integration for DefraDB P2P.
//!
//! This module provides Bitswap (IPFS block exchange) support for DefraDB,
//! enabling Go DefraDB compatibility for block fetching during DAG sync.
//!
//! # Architecture
//!
//! ```text
//! Remote Peer
//!     ↓ Bitswap request
//! BitswapBehaviour
//!     ↓
//! BitswapStoreAdapter
//!     ↓
//! DefraBlockstore (async)
//!     ↓
//! Storage backend
//! ```
//!
//! # Go Compatibility
//!
//! Go DefraDB uses Bitswap to fetch missing blocks when processing PushLog
//! messages. When a peer receives a new block via PushLog, it checks for
//! missing DAG links and fetches them via Bitswap.

mod access;
#[cfg(feature = "libp2p-transport")]
mod filter;
mod registry;
#[cfg(feature = "libp2p-transport")]
mod store;

pub use access::AccessMode;
#[cfg(feature = "libp2p-transport")]
pub use filter::make_peer_block_access_filter;
pub use registry::ReplicatorRegistry;
#[cfg(feature = "libp2p-transport")]
pub use store::BitswapStoreAdapter;
