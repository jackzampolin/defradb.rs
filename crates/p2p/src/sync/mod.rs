// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! P2P synchronization module for DefraDB.
//!
//! This module provides block synchronization between DefraDB peers.
//! It handles:
//! - Receiving blocks from the network (via PushLog messages)
//! - Storing blocks in the blockstore with merge tracking
//! - Applying CRDT merges to integrate remote changes
//! - Broadcasting local changes to the network
//!
//! # Architecture
//!
//! The sync flow follows the Go implementation:
//!
//! ```text
//! Network (PubSub/Replicator)
//!         ↓
//! PushLogRequest received
//!         ↓
//! SyncManager.process_pushlog()
//!         ↓
//! ┌───────┴───────┐
//! │ Process Queue │  ← Deduplicates concurrent syncs for same CID
//! └───────┬───────┘
//!         ↓
//! Check if already merged (blockstore.is_merged())
//!         ↓ (if not merged)
//! Store block in blockstore
//!         ↓
//! Apply CRDT merge
//!         ↓
//! Mark as merged
//!         ↓
//! Broadcast to network (optional)
//! ```

mod broadcaster;
mod coordinator;
mod dag_sync;
mod manager;
mod merge;
mod peer_state;
mod queue;
mod replication;

pub use broadcaster::Broadcaster;
pub use coordinator::SyncCoordinator;
pub use dag_sync::{DagSync, DagSyncConfig, DagSyncState, SyncPlan};
pub use manager::{SyncConfig, SyncEvent, SyncManager};
pub use merge::{MergeHandler, MergeOutcome};
pub use peer_state::{PeerStateTracker, PeerStats};
pub use queue::ProcessQueue;
pub use replication::{ReplicationConfig, ReplicationLoop, ReplicationResult};
