// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
mod store;

pub use access::{AccessMode, BlockAccessController, BlockAccessFn, ReplicatorRegistry};
pub use store::BitswapStoreAdapter;
