//! Sync plan types for DAG synchronization.

use cid::Cid;
use libp2p::PeerId;

/// Data for a NeedsFetch sync plan with enforced invariants.
///
/// The `missing` field is guaranteed to be non-empty.
/// Use `NeedsFetchData::new()` to construct.
#[derive(Debug, Clone)]
pub struct NeedsFetchData {
    /// Root block CID that triggered the sync.
    root: Cid,
    /// CIDs that need to be fetched (guaranteed non-empty).
    missing: Vec<Cid>,
    /// Potential providers for the blocks.
    providers: Vec<PeerId>,
}

impl NeedsFetchData {
    /// Create a new NeedsFetchData with validation.
    ///
    /// Returns `None` if `missing` is empty.
    pub fn new(root: Cid, missing: Vec<Cid>, providers: Vec<PeerId>) -> Option<Self> {
        if missing.is_empty() {
            None
        } else {
            Some(Self {
                root,
                missing,
                providers,
            })
        }
    }

    /// Get the root CID.
    pub fn root(&self) -> Cid {
        self.root
    }

    /// Get the missing CIDs (guaranteed non-empty).
    pub fn missing(&self) -> &[Cid] {
        &self.missing
    }

    /// Get the providers.
    pub fn providers(&self) -> &[PeerId] {
        &self.providers
    }
}

/// Result of preparing a sync operation.
#[derive(Debug)]
#[non_exhaustive]
pub enum SyncPlan {
    /// Already syncing this block.
    AlreadySyncing,

    /// Already synced this block.
    AlreadySynced,

    /// All blocks are available locally, no fetch needed.
    Complete,

    /// Need to fetch missing blocks via Bitswap.
    ///
    /// Use `SyncPlan::needs_fetch_new()` or `NeedsFetchData::new()` to create,
    /// which enforces the invariant that `missing` is non-empty.
    NeedsFetch(NeedsFetchData),
}

impl SyncPlan {
    /// Create a NeedsFetch plan with validation.
    ///
    /// Returns `None` if `missing` is empty (use `SyncPlan::Complete` instead).
    pub fn needs_fetch_new(root: Cid, missing: Vec<Cid>, providers: Vec<PeerId>) -> Option<Self> {
        NeedsFetchData::new(root, missing, providers).map(Self::NeedsFetch)
    }

    /// Get the root CID if this is a NeedsFetch plan.
    pub fn root(&self) -> Option<Cid> {
        match self {
            SyncPlan::NeedsFetch(data) => Some(data.root()),
            _ => None,
        }
    }

    /// Check if a fetch is needed.
    pub fn needs_fetch(&self) -> bool {
        matches!(self, SyncPlan::NeedsFetch(_))
    }

    /// Get the missing CIDs if a fetch is needed.
    pub fn missing(&self) -> Option<&[Cid]> {
        match self {
            SyncPlan::NeedsFetch(data) => Some(data.missing()),
            _ => None,
        }
    }

    /// Get the providers if a fetch is needed.
    pub fn providers(&self) -> Option<&[PeerId]> {
        match self {
            SyncPlan::NeedsFetch(data) => Some(data.providers()),
            _ => None,
        }
    }

    /// Get the NeedsFetchData if this is a NeedsFetch plan.
    pub fn fetch_data(&self) -> Option<&NeedsFetchData> {
        match self {
            SyncPlan::NeedsFetch(data) => Some(data),
            _ => None,
        }
    }
}
