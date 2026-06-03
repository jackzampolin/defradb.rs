//! Deferred serve dependencies for the inbound management channel.
//!
//! The P2P event loops are spawned before the controller (`P2POperations`) and
//! NAC manager exist (they need ACP/NAC, which initialize later in node
//! assembly). The loops therefore read these deps lazily from a shared
//! [`ManageHooksCell`], populated once both are built. Until populated, inbound
//! manage requests are dropped (never served unauthenticated).

use std::sync::Arc;

use p2p::{ManageCorrelator, ManageQueryCorrelator};

/// Serve-side dependencies the manage event arms need: the controller to
/// dispatch authorized ops, the NAC manager to authorize, and the two
/// correlators to deliver inbound replies.
#[derive(Clone)]
pub struct ManageHooks {
    pub ops: Arc<dyn defra_http::P2POperations>,
    pub nac: Arc<dyn db::NacManagerApi>,
    pub correlator: ManageCorrelator,
    pub query_correlator: ManageQueryCorrelator,
}

/// Shared, write-once cell for [`ManageHooks`]. Cloned into each event loop
/// (read side) and into node assembly (populated after the controller + NAC
/// manager are built).
pub type ManageHooksCell = Arc<tokio::sync::OnceCell<ManageHooks>>;

/// Create an empty, unpopulated hooks cell.
pub fn new_manage_hooks_cell() -> ManageHooksCell {
    Arc::new(tokio::sync::OnceCell::new())
}
