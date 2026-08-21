use crate::database::DB;
use crate::error::Result;
use acp::nac::NodePermission;
use async_trait::async_trait;
use storage::corekv::Store;

/// Type-erased handle to DB-layer NAC enforcement, for components that are
/// generic-erased over the store and cannot hold an `Arc<DB<S>>`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait NodeAccessChecker: defra_core::thread_bounds::MaybeSendSync {
    /// Enforce `permission` for the ambient acting identity. Mirrors
    /// `DB::check_node_access(None, permission)`: a no-op when NAC is unset or
    /// disabled, with node-identity bypass and ambient-identity resolution.
    async fn check_node_access(&self, permission: NodePermission) -> Result<()>;
}

struct DbNodeAccessChecker<S: Store> {
    db: std::sync::Arc<DB<S>>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store> NodeAccessChecker for DbNodeAccessChecker<S> {
    async fn check_node_access(&self, permission: NodePermission) -> Result<()> {
        self.db.check_node_access(None, permission).await
    }
}

/// Build a type-erased NAC checker from a database handle.
pub fn node_access_checker<S: Store + 'static>(
    db: std::sync::Arc<DB<S>>,
) -> std::sync::Arc<dyn NodeAccessChecker> {
    std::sync::Arc::new(DbNodeAccessChecker { db })
}
