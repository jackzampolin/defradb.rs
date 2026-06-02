use crate::database::DB;
use crate::error::{Error, Result};
use acp::nac::NodePermission;
use identity::Did;
use storage::corekv::Store;

impl<S: Store> DB<S> {
    /// Check NAC permission before proceeding with a node-level operation.
    /// Matches Go's `db.checkNodeAccess()` pattern.
    ///
    /// Returns `Ok(())` when NAC is not configured or not enabled (all
    /// operations allowed), or when the identity holds the permission.
    /// Returns `Error::NotAuthorized` when the identity lacks it.
    pub async fn check_node_access(
        &self,
        identity: Option<&Did>,
        permission: NodePermission,
    ) -> Result<()> {
        let nac = match self.nac_manager() {
            Some(nac) => nac,
            None => return Ok(()),
        };
        if !nac.is_enabled().await {
            return Ok(());
        }
        // Resolve the acting identity: explicit param wins, otherwise fall
        // back to the ambient request identity set at the HTTP/FFI boundary.
        let resolved: Option<Did> = match identity {
            Some(d) => Some(d.clone()),
            None => defra_core::current_identity::get_current_identity()
                .and_then(|s| Did::new(s).ok()),
        };
        // Node's own identity always has access (mirrors Go db_nac.go).
        if let Some(node) = self.node_did() {
            if resolved.as_ref() == Some(&node) {
                return Ok(());
            }
        }
        let did = resolved.unwrap_or_else(Did::wildcard);
        let allowed = nac
            .check_permission(&did, permission)
            .await
            .map_err(|e| Error::Acp(e.to_string()))?;
        if allowed {
            Ok(())
        } else {
            Err(Error::NotAuthorized {
                permission: permission.as_str().to_string(),
            })
        }
    }
}
