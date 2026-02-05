//! Access control mode for P2P synchronization.
//!
//! This module provides access control primitives:
//! - `AccessMode`: Controls whether access control is enabled (Open vs Controlled)
//!
//! # Security Model
//!
//! Access control is enforced at the **SyncCoordinator level**, not at the Bitswap level.
//! The SyncCoordinator checks access on incoming PushLog and GossipSub messages before
//! blocks are stored. This means:
//!
//! 1. Unauthorized peers cannot push blocks to this node
//! 2. Bitswap inherently only serves blocks that passed the coordinator's access check
//! 3. Per-collection authorization is enforced (a replicator for collection A cannot
//!    access collection B)
//!
//! This follows the Go DefraDB security model where each replicator is authorized
//! per-collection.

/// Access control mode for P2P synchronization.
///
/// Controls whether access control is enforced at the SyncCoordinator level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
