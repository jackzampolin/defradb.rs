//! Node Access Control (NAC) implementation.
//!
//! NAC provides node-level access control using the Zanzibar permission model.
//! It wraps a local Zanzibar store and provides methods for:
//! - Permission checking for node operations
//! - Managing admin relationships
//! - NAC lifecycle (enable, disable, purge)
//!
//! ## Security Features
//!
//! - **Write blocking when disabled**: All relationship modifications are blocked
//!   when NAC is disabled to prevent privilege escalation attacks.

mod lifecycle;
mod operations;

use std::sync::Arc;
use storage::corekv::MaybeSendSync;

use async_lock::RwLock;
use async_trait::async_trait;
use identity::Did;

use crate::error::{Error, Result};
use crate::nac::permission::NodePermission;
use crate::nac::policy::{
    validate_node_policy, NODE_POLICY_ID, NODE_RESOURCE_NAME, OWNER_RELATION,
};
use crate::zanzibar::{PermissionEngine, Subject, ZanzibarStore};

/// The fixed object ID for the node resource.
/// There's only one "node" instance, so we use a constant ID.
pub const NODE_OBJECT_ID: &str = "singleton";

/// Sentinel relation name used to persist "disabled temporarily" status.
/// Stored as a relationship in the Zanzibar store so it survives restarts.
const DISABLED_RELATION: &str = "_disabled";

/// NAC status indicating whether node access control is active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NacStatus {
    /// NAC has never been configured (first run or after purge)
    NotConfigured,

    /// NAC is enabled and actively enforcing permissions
    Enabled,

    /// NAC is temporarily disabled but state is preserved
    DisabledTemporarily,
}

impl std::fmt::Display for NacStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured => write!(f, "not configured"),
            Self::Enabled => write!(f, "enabled"),
            Self::DisabledTemporarily => write!(f, "disabled temporarily"),
        }
    }
}

/// Node Access Control implementation.
///
/// NAC uses a local Zanzibar store to manage node-level permissions.
/// Unlike DAC, NAC is always local (no SourceHub option).
pub struct NodeACP<S: ZanzibarStore> {
    store: Arc<S>,
    engine: RwLock<PermissionEngine<S>>,
    status: RwLock<NacStatus>,
    /// The owner identity (set when NAC is enabled)
    owner: RwLock<Option<Did>>,
}

impl<S: ZanzibarStore> NodeACP<S> {
    /// Create a new NodeACP with the given store.
    ///
    /// The NAC starts in NotConfigured status. Call `enable()` to activate it.
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store: store.clone(),
            engine: RwLock::new(PermissionEngine::new(store)),
            status: RwLock::new(NacStatus::NotConfigured),
            owner: RwLock::new(None),
        }
    }

    /// Load existing NAC state from the store.
    ///
    /// This should be called during startup to restore NAC state.
    pub async fn load(&self) -> Result<()> {
        // Try to load the policy
        if let Some(policy) = self.store.get_policy(NODE_POLICY_ID).await? {
            // Validate the policy structure
            validate_node_policy(&policy).map_err(Error::InvalidPolicy)?;

            // Load policy into engine
            let mut engine = self.engine.write().await;
            engine.add_policy(&policy);

            // Find the owner
            let subjects = self
                .store
                .get_relation_subjects(
                    NODE_POLICY_ID,
                    NODE_RESOURCE_NAME,
                    NODE_OBJECT_ID,
                    OWNER_RELATION,
                )
                .await?;

            // Extract the DID from the first Entity subject
            if let Some(Subject::Entity(owner_did)) = subjects.first() {
                *self.owner.write().await = Some(owner_did.clone());

                // Check if NAC was disabled before restart
                let disabled_subjects = self
                    .store
                    .get_relation_subjects(
                        NODE_POLICY_ID,
                        NODE_RESOURCE_NAME,
                        NODE_OBJECT_ID,
                        DISABLED_RELATION,
                    )
                    .await?;

                if disabled_subjects.is_empty() {
                    *self.status.write().await = NacStatus::Enabled;
                } else {
                    *self.status.write().await = NacStatus::DisabledTemporarily;
                }

                tracing::info!(
                    target: "nac::audit",
                    event = "nac_loaded",
                    owner = %owner_did,
                    disabled = !disabled_subjects.is_empty(),
                    "NAC state loaded from store"
                );
            }
        }

        Ok(())
    }

    /// Get the current NAC status.
    pub async fn status(&self) -> NacStatus {
        *self.status.read().await
    }

    /// Get the owner identity.
    pub async fn owner(&self) -> Option<Did> {
        self.owner.read().await.clone()
    }
}

/// Trait for NAC operations accessible via HTTP.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait NodeAcpOperations: MaybeSendSync {
    /// Check if an identity has a specific permission.
    async fn check_permission(&self, identity: &Did, permission: NodePermission) -> Result<bool>;

    /// Get NAC status.
    async fn get_status(&self) -> NacStatus;

    /// Add admin relationship.
    async fn add_admin(&self, requestor: &Did, target: &Did) -> Result<bool>;

    /// Remove admin relationship.
    async fn remove_admin(&self, requestor: &Did, target: &Did) -> Result<bool>;

    /// Get the owner identity.
    async fn owner(&self) -> Option<Did>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: ZanzibarStore + 'static> NodeAcpOperations for NodeACP<S> {
    async fn check_permission(&self, identity: &Did, permission: NodePermission) -> Result<bool> {
        NodeACP::check_permission(self, identity, permission).await
    }

    async fn get_status(&self) -> NacStatus {
        self.status().await
    }

    async fn add_admin(&self, requestor: &Did, target: &Did) -> Result<bool> {
        NodeACP::add_admin(self, requestor, target).await
    }

    async fn remove_admin(&self, requestor: &Did, target: &Did) -> Result<bool> {
        NodeACP::remove_admin(self, requestor, target).await
    }

    async fn owner(&self) -> Option<Did> {
        NodeACP::owner(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zanzibar::MemoryZanzibarStore;

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    fn test_did2() -> Did {
        Did::new("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH").unwrap()
    }

    #[tokio::test]
    async fn test_nac_starts_not_configured() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let nac = NodeACP::new(store);

        assert_eq!(nac.status().await, NacStatus::NotConfigured);
        assert!(nac.owner().await.is_none());
    }

    #[tokio::test]
    async fn test_nac_allows_all_when_not_enabled() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let nac = NodeACP::new(store);

        let identity = test_did();

        // All permissions should be granted when NAC is not enabled
        for perm in NodePermission::all() {
            let allowed = nac.check_permission(&identity, *perm).await.unwrap();
            assert!(
                allowed,
                "permission {} should be allowed when NAC not enabled",
                perm
            );
        }
    }

    #[tokio::test]
    async fn test_enable_nac() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let nac = NodeACP::new(store);

        let owner = test_did();
        nac.enable(&owner).await.unwrap();

        assert_eq!(nac.status().await, NacStatus::Enabled);
        assert_eq!(nac.owner().await, Some(owner.clone()));
    }

    #[tokio::test]
    async fn test_owner_has_all_permissions() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let nac = NodeACP::new(store);

        let owner = test_did();
        nac.enable(&owner).await.unwrap();

        // Owner should have all permissions
        for perm in NodePermission::all() {
            let allowed = nac.check_permission(&owner, *perm).await.unwrap();
            assert!(allowed, "owner should have permission {}", perm);
        }
    }

    #[tokio::test]
    async fn test_non_owner_denied_when_nac_enabled() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let nac = NodeACP::new(store);

        let owner = test_did();
        let other = test_did2();
        nac.enable(&owner).await.unwrap();

        // Non-owner should be denied
        let allowed = nac
            .check_permission(&other, NodePermission::DacBypass)
            .await
            .unwrap();
        assert!(!allowed);
    }

    #[tokio::test]
    async fn test_disable_and_re_enable() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let nac = NodeACP::new(store);

        let owner = test_did();
        nac.enable(&owner).await.unwrap();

        // Disable
        nac.disable().await.unwrap();
        assert_eq!(nac.status().await, NacStatus::DisabledTemporarily);

        // All operations allowed when disabled
        let other = test_did2();
        let allowed = nac
            .check_permission(&other, NodePermission::DacBypass)
            .await
            .unwrap();
        assert!(allowed);

        // Re-enable
        nac.re_enable().await.unwrap();
        assert_eq!(nac.status().await, NacStatus::Enabled);

        // Non-owner denied again
        let allowed = nac
            .check_permission(&other, NodePermission::DacBypass)
            .await
            .unwrap();
        assert!(!allowed);
    }

    #[tokio::test]
    async fn test_add_admin() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let nac = NodeACP::new(store);

        let owner = test_did();
        let admin = test_did2();
        nac.enable(&owner).await.unwrap();

        // Add admin
        let added = nac.add_admin(&owner, &admin).await.unwrap();
        assert!(added);

        // Admin should have all permissions
        for perm in NodePermission::all() {
            let allowed = nac.check_permission(&admin, *perm).await.unwrap();
            assert!(allowed, "admin should have permission {}", perm);
        }
    }

    #[tokio::test]
    async fn test_remove_admin() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let nac = NodeACP::new(store);

        let owner = test_did();
        let admin = test_did2();
        nac.enable(&owner).await.unwrap();

        // Add then remove admin
        nac.add_admin(&owner, &admin).await.unwrap();
        let removed = nac.remove_admin(&owner, &admin).await.unwrap();
        assert!(removed);

        // Former admin should be denied
        let allowed = nac
            .check_permission(&admin, NodePermission::DacBypass)
            .await
            .unwrap();
        assert!(!allowed);
    }

    #[tokio::test]
    async fn test_cannot_remove_owner() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let nac = NodeACP::new(store);

        let owner = test_did();
        nac.enable(&owner).await.unwrap();

        // Cannot remove owner
        let result = nac.remove_admin(&owner, &owner).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_purge_nac() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let nac = NodeACP::new(store);

        let owner = test_did();
        nac.enable(&owner).await.unwrap();

        // Purge
        nac.purge().await.unwrap();

        assert_eq!(nac.status().await, NacStatus::NotConfigured);
        assert!(nac.owner().await.is_none());
    }

    #[tokio::test]
    async fn test_load_existing_state() {
        let store = Arc::new(MemoryZanzibarStore::new());

        // Enable NAC
        {
            let nac = NodeACP::new(store.clone());
            let owner = test_did();
            nac.enable(&owner).await.unwrap();
        }

        // Create new instance and load
        {
            let nac = NodeACP::new(store);
            nac.load().await.unwrap();

            assert_eq!(nac.status().await, NacStatus::Enabled);
            assert_eq!(nac.owner().await, Some(test_did()));
        }
    }

    #[tokio::test]
    async fn test_write_operations_blocked_when_disabled() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let nac = NodeACP::new(store);

        let owner = test_did();
        let other = test_did2();
        nac.enable(&owner).await.unwrap();

        // Disable NAC
        nac.disable().await.unwrap();
        assert_eq!(nac.status().await, NacStatus::DisabledTemporarily);

        // Attempting to add admin while disabled should fail
        let result = nac.add_admin(&owner, &other).await;
        assert!(
            result.is_err(),
            "add_admin should be blocked when NAC is disabled"
        );

        // Attempting to remove admin while disabled should fail
        let result = nac.remove_admin(&owner, &other).await;
        assert!(
            result.is_err(),
            "remove_admin should be blocked when NAC is disabled"
        );

        // Attempting to grant permission while disabled should fail
        let result = nac
            .add_permission_grant(&owner, &other, NodePermission::DacBypass)
            .await;
        assert!(
            result.is_err(),
            "add_permission_grant should be blocked when NAC is disabled"
        );

        // Attempting to revoke permission while disabled should fail
        let result = nac
            .remove_permission_grant(&owner, &other, NodePermission::DacBypass)
            .await;
        assert!(
            result.is_err(),
            "remove_permission_grant should be blocked when NAC is disabled"
        );

        // Re-enable and verify writes work again
        nac.re_enable().await.unwrap();

        // Now add admin should work
        let result = nac.add_admin(&owner, &other).await;
        assert!(result.is_ok(), "add_admin should work when NAC is enabled");
    }

    #[tokio::test]
    async fn test_wildcard_admin_grants_all_permissions() {
        let store = Arc::new(MemoryZanzibarStore::new());
        let nac = NodeACP::new(store);

        let owner = test_did();
        let other = test_did2();
        let wildcard = Did::wildcard();

        nac.enable(&owner).await.unwrap();

        // Grant admin to wildcard (all identities)
        let added = nac.add_admin(&owner, &wildcard).await.unwrap();
        assert!(added, "should successfully add wildcard admin");

        // Now any identity should have NacStatus permission
        let has_perm = nac
            .check_permission(&other, NodePermission::NacStatus)
            .await
            .unwrap();
        assert!(
            has_perm,
            "any identity should have NacStatus after wildcard admin grant"
        );

        // Check another permission too
        let has_perm = nac
            .check_permission(&other, NodePermission::CollectionGet)
            .await
            .unwrap();
        assert!(
            has_perm,
            "any identity should have CollectionGet after wildcard admin grant"
        );
    }
}
