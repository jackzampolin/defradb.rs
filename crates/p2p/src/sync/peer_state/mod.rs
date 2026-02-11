//! Peer state tracking for P2P synchronization.
//!
//! Tracks which blocks each peer has, enabling:
//! - Efficient block requests (ask peers who have the block)
//! - Avoiding redundant sends (don't send blocks peers already have)
//! - Replication status monitoring

mod stats;
mod tracker;

pub use stats::PeerStats;
pub use tracker::PeerStateTracker;
