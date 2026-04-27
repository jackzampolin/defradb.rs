//! Access control mode for P2P synchronization.
//!
//! This module provides access control primitives:
//! - `AccessMode`: Controls whether access control is enabled (Open vs Controlled)
//!
//! # Security Model
//!
//! Access control is enforced on **both the ingress and egress paths**:
//!
//! - **Ingress:** `SyncCoordinator` checks incoming PushLog and GossipSub
//!   messages against the `ReplicatorRegistry` before storing blocks.
//!   Unauthorized peers cannot push blocks into this node.
//! - **Egress:** A per-peer Bitswap filter
//!   (`crate::bitswap::make_peer_block_access_filter`) is installed at
//!   behaviour-construction time (`crates/p2p/src/behaviour.rs`). On every
//!   incoming Bitswap `WANT`, the filter reads the block, decodes its
//!   `collectionVersionID`, and consults the same `ReplicatorRegistry`.
//!   Peers that are not registered replicators for the block's collection
//!   are denied (signature and definition blocks are exempt to allow
//!   signature verification and schema bootstrap).
//!
//! This matches Go DefraDB's `bitswap.WithPeerBlockRequestFilter(hasAccess)`
//! wiring (`go-p2p/peer.go:146`), closing issue #830.
//!
//! Note: in `AccessMode::Open` the filter allows all requests, preserving the
//! previous behaviour for nodes that haven't enabled ACP.

/// Access control mode for P2P synchronization.
///
/// Controls whether access control is enforced at the SyncCoordinator level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum AccessMode {
    /// No access control - all requests are allowed.
    /// This is the default mode when ACP is not configured.
    #[default]
    Open,

    /// Access control enabled - check replicator status.
    /// Only replicators for the specific collection have access.
    Controlled,
}

impl AccessMode {
    /// Returns true if access control is enabled.
    pub fn is_controlled(&self) -> bool {
        matches!(self, AccessMode::Controlled)
    }

    /// Returns true if access is open (no control).
    pub fn is_open(&self) -> bool {
        matches!(self, AccessMode::Open)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_mode_helpers() {
        assert!(AccessMode::Open.is_open());
        assert!(!AccessMode::Open.is_controlled());
        assert!(AccessMode::Controlled.is_controlled());
        assert!(!AccessMode::Controlled.is_open());
        assert_eq!(AccessMode::default(), AccessMode::Open);
    }
}
