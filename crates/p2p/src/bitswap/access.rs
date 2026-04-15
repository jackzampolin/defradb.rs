//! Access control mode for P2P synchronization.
//!
//! This module provides access control primitives:
//! - `AccessMode`: Controls whether access control is enabled (Open vs Controlled)
//!
//! # Security Model (current state)
//!
//! Access control today is enforced **only on the ingress path** via the
//! SyncCoordinator: incoming PushLog and GossipSub messages are checked against
//! the `ReplicatorRegistry` before blocks are stored. This means unauthorized
//! peers cannot push blocks *into* this node.
//!
//! **The egress path via Bitswap is not filtered.** Once a block is in the
//! blockstore — however it got there — it is served to any connected peer that
//! requests it via Bitswap. The `BitswapStoreAdapter::get(&self, cid: &Cid)`
//! signature (`crates/p2p/src/bitswap/store.rs`) has no peer context, so a
//! per-peer access check cannot be expressed at this layer today.
//!
//! **This diverges from Go DefraDB.** Go wires Bitswap with
//! `bitswap.WithPeerBlockRequestFilter(p.hasAccess)` (`go-p2p/peer.go:146`),
//! so every incoming block request is filtered per (peer, CID) against the
//! replicator registry. Go denies block fetches from peers that are not
//! authorized for the collection that owns the CID.
//!
//! In practice this means that in a Rust node running `AccessMode::Controlled`,
//! any peer that can open a libp2p connection can fetch any stored block,
//! regardless of their authorization for the owning collection. ACP-encrypted
//! collections still protect plaintext confidentiality, but ciphertext and
//! block-existence signals leak to all connected peers.
//!
//! See issue #830 for the tracking fix (adding a per-peer request filter to
//! Rust's Bitswap integration to restore parity with Go).

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
