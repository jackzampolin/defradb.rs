use std::sync::Arc;

/// P2P state for FFI nodes.
pub struct P2PState {
    pub system: Arc<embedded::ManagedP2PSystem>,
}

impl P2PState {
    pub fn new(system: Arc<embedded::ManagedP2PSystem>) -> Self {
        Self { system }
    }
}
