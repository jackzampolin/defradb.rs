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

use std::sync::Arc;
use storage::corekv::MaybeSendSync;

use async_lock::RwLock;
use async_trait::async_trait;
use identity::Did;

use super::permission::NodePermission;
use super::policy::{
    create_node_policy, validate_node_policy, ADMIN_RELATION, NODE_POLICY_ID, NODE_RESOURCE_NAME,
    OWNER_RELATION,
};
use crate::error::{Error, Result};
use crate::zanzibar::{PermissionEngine, Relationship, Subject, ZanzibarStore};

/// The fixed object ID for the node resource.
/// There's only one "node" instance, so we use a constant ID.
pub const NODE_OBJECT_ID: &str = "singleton";

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
                *self.status.write().await = NacStatus::Enabled;

                tracing::info!(
                    target: "nac::audit",
                    event = "nac_loaded",
                    owner = %owner_did,
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

    /// Enable NAC with the given owner identity.
    ///
    /// The owner identity has full control over the node and can grant
    /// admin permissions to other identities.
    ///
    /// Returns an error if NAC is already enabled.
    pub async fn enable(&self, owner: &Did) -> Result<()> {
        let status = *self.status.read().await;
        if status == NacStatus::Enabled {
            return Err(Error::InvalidPolicy("NAC is already enabled".into()));
        }

        // Create and store the policy
        let policy = create_node_policy();
        self.store.store_policy(&policy).await?;

        // Load policy into engine
        {
            let mut engine = self.engine.write().await;
            engine.add_policy(&policy);
        }

        // Create owner relationship
        let owner_rel = Relationship::with_entity(
            NODE_RESOURCE_NAME,
            NODE_OBJECT_ID,
            OWNER_RELATION,
            owner.clone(),
        );
        self.store
            .store_relationship(NODE_POLICY_ID, &owner_rel)
            .await?;

        // Update state
        *self.owner.write().await = Some(owner.clone());
        *self.status.write().await = NacStatus::Enabled;

        tracing::info!(
            target: "nac::audit",
            event = "nac_enabled",
            owner = %owner,
            "Node Access Control enabled"
        );

        Ok(())
    }

    /// Temporarily disable NAC.
    ///
    /// Preserves all state but stops enforcing permissions.
    /// NAC remains disabled until explicitly re-enabled via `re_enable()`.
    ///
    /// Write operations (adding/removing admins, granting/revoking permissions)
    /// are blocked while NAC is disabled to prevent privilege escalation.
    ///
    /// # Security Warning
    ///
    /// **This method does NOT perform authorization checks.** Callers MUST verify
    /// the requestor has admin permissions before calling this method.
    ///
    /// In production code, use `NacManager::disable()` instead, which wraps
    /// this method with proper authorization checks.
    #[doc(hidden)]
    pub async fn disable(&self) -> Result<()> {
        let status = *self.status.read().await;
        match status {
            NacStatus::NotConfigured => {
                return Err(Error::InvalidPolicy("node acp is not configured".into()));
            }
            NacStatus::DisabledTemporarily => {
                return Err(Error::InvalidPolicy("node acp is already disabled".into()));
            }
            NacStatus::Enabled => {} // proceed
        }

        *self.status.write().await = NacStatus::DisabledTemporarily;

        tracing::info!(
            target: "nac::audit",
            event = "nac_disabled",
            "Node Access Control temporarily disabled"
        );

        Ok(())
    }

    /// Re-enable NAC after temporary disable.
    ///
    /// # Security Warning
    ///
    /// **This method does NOT perform authorization checks.** Callers MUST verify
    /// the requestor has admin permissions before calling this method.
    ///
    /// In production code, use `NacManager::re_enable()` instead, which wraps
    /// this method with proper authorization checks.
    #[doc(hidden)]
    pub async fn re_enable(&self) -> Result<()> {
        let status = *self.status.read().await;
        match status {
            NacStatus::NotConfigured => {
                return Err(Error::InvalidPolicy("node acp is not configured".into()));
            }
            NacStatus::Enabled => {
                return Err(Error::InvalidPolicy("node acp is already enabled".into()));
            }
            NacStatus::DisabledTemporarily => {} // proceed
        }

        *self.status.write().await = NacStatus::Enabled;

        tracing::info!(
            target: "nac::audit",
            event = "nac_re_enabled",
            "Node Access Control re-enabled"
        );

        Ok(())
    }

    /// Purge all NAC data and reset to NotConfigured.
    ///
    /// This is a destructive operation that deletes all NAC relationships
    /// and policies. After purging, NAC must be re-enabled from scratch.
    ///
    /// # Security Warning
    ///
    /// **This method does NOT perform authorization checks.** Callers MUST verify:
    /// 1. The requestor has admin permissions
    /// 2. The node is running in dev mode
    ///
    /// In production code, use `NacManager::purge()` instead, which wraps
    /// this method with proper authorization and dev-mode checks.
    #[doc(hidden)]
    pub async fn purge(&self) -> Result<()> {
        // Delete all relationships for the node object
        self.store
            .delete_object_relationships(NODE_POLICY_ID, NODE_RESOURCE_NAME, NODE_OBJECT_ID)
            .await?;

        // Delete the policy
        self.store.delete_policy(NODE_POLICY_ID).await?;

        // Clear engine cache
        {
            let mut engine = self.engine.write().await;
            engine.remove_policy(NODE_POLICY_ID);
        }

        // Reset state
        *self.owner.write().await = None;
        *self.status.write().await = NacStatus::NotConfigured;

        tracing::warn!(
            target: "nac::audit",
            event = "nac_purged",
            "Node Access Control state purged"
        );

        Ok(())
    }

    /// Check if an identity has a specific node permission.
    ///
    /// Returns `true` if:
    /// - NAC is not enabled (all operations allowed)
    /// - The identity has the required permission (via owner or admin)
    pub async fn check_permission(
        &self,
        identity: &Did,
        permission: NodePermission,
    ) -> Result<bool> {
        let status = *self.status.read().await;

        // If NAC is not enabled, allow all operations
        if status != NacStatus::Enabled {
            return Ok(true);
        }

        // Use the engine to check permission
        let engine = self.engine.read().await;
        let has_permission = engine
            .check(
                NODE_POLICY_ID,
                NODE_RESOURCE_NAME,
                NODE_OBJECT_ID,
                permission.as_str(),
                identity,
            )
            .await?;

        if has_permission {
            tracing::debug!(
                target: "nac::audit",
                event = "permission_granted",
                identity = %identity,
                permission = %permission,
                "NAC permission granted"
            );
        } else {
            tracing::info!(
                target: "nac::audit",
                event = "permission_denied",
                identity = %identity,
                permission = %permission,
                "NAC permission denied"
            );
        }

        Ok(has_permission)
    }

    /// Check if an identity is the owner.
    pub async fn is_owner(&self, identity: &Did) -> bool {
        if let Some(owner) = self.owner.read().await.as_ref() {
            owner == identity
        } else {
            false
        }
    }

    /// Check if an identity is an admin (owner or has admin relation).
    pub async fn is_admin(&self, identity: &Did) -> Result<bool> {
        let status = *self.status.read().await;
        if status != NacStatus::Enabled {
            return Ok(true); // Everyone is admin when NAC is disabled
        }

        self.is_admin_persisted(identity).await
    }

    /// Check if an identity is an admin based on stored relationships.
    ///
    /// Unlike `is_admin()`, this checks the actual stored relationships
    /// regardless of the current NAC status. Used for operations like
    /// `re_enable` where we need to verify admin access even when NAC
    /// is temporarily disabled.
    pub async fn is_admin_persisted(&self, identity: &Did) -> Result<bool> {
        // Check if owner
        if self.is_owner(identity).await {
            return Ok(true);
        }

        // Check admin relation from stored relationships
        let engine = self.engine.read().await;
        engine
            .check(
                NODE_POLICY_ID,
                NODE_RESOURCE_NAME,
                NODE_OBJECT_ID,
                ADMIN_RELATION,
                identity,
            )
            .await
    }

    /// Add an admin relationship.
    ///
    /// Only the owner or existing admins can add new admins.
    /// Write operations are blocked when NAC is disabled to prevent privilege escalation.
    pub async fn add_admin(&self, requestor: &Did, target: &Did) -> Result<bool> {
        // Block write operations when disabled to prevent privilege escalation
        let status = *self.status.read().await;
        if status == NacStatus::DisabledTemporarily {
            return Err(Error::InvalidPolicy(
                "cannot modify relationships while NAC is disabled - re-enable NAC first".into(),
            ));
        }

        // Check if requestor can manage admin relation
        if !self.is_admin(requestor).await? {
            return Err(Error::NotOwner {
                operation: "add admin".into(),
            });
        }

        // Convert target DID to appropriate Subject (wildcard "*" → Subject::Wildcard)
        let target_subject = if target.is_wildcard() {
            Subject::Wildcard
        } else {
            Subject::Entity(target.clone())
        };

        // Check if already admin
        let is_already_admin = self.is_admin(target).await?;
        if is_already_admin && !self.is_owner(target).await {
            // Target is already a direct admin
            let has_direct = self
                .store
                .has_relationship(
                    NODE_POLICY_ID,
                    NODE_RESOURCE_NAME,
                    NODE_OBJECT_ID,
                    ADMIN_RELATION,
                    &target_subject,
                )
                .await?;
            if has_direct {
                return Ok(false); // Already exists
            }
        }

        // Store admin relationship
        let admin_rel = Relationship::new(
            NODE_RESOURCE_NAME,
            NODE_OBJECT_ID,
            ADMIN_RELATION,
            target_subject,
        );
        self.store
            .store_relationship(NODE_POLICY_ID, &admin_rel)
            .await?;

        tracing::info!(
            target: "nac::audit",
            event = "admin_added",
            requestor = %requestor,
            target = %target,
            "NAC admin relationship added"
        );

        Ok(true)
    }

    /// Remove an admin relationship.
    ///
    /// Only the owner or existing admins can remove admins.
    /// The owner cannot be removed.
    /// Write operations are blocked when NAC is disabled to prevent privilege escalation.
    pub async fn remove_admin(&self, requestor: &Did, target: &Did) -> Result<bool> {
        // Block write operations when disabled to prevent privilege escalation
        let status = *self.status.read().await;
        if status == NacStatus::DisabledTemporarily {
            return Err(Error::InvalidPolicy(
                "cannot modify relationships while NAC is disabled - re-enable NAC first".into(),
            ));
        }

        // Cannot remove owner
        if self.is_owner(target).await {
            return Err(Error::InvalidRelation(
                "cannot remove owner's admin access".into(),
            ));
        }

        // Check if requestor can manage admin relation
        if !self.is_admin(requestor).await? {
            return Err(Error::NotOwner {
                operation: "remove admin".into(),
            });
        }

        // Convert target DID to appropriate Subject (wildcard "*" → Subject::Wildcard)
        let target_subject = if target.is_wildcard() {
            Subject::Wildcard
        } else {
            Subject::Entity(target.clone())
        };

        // Delete admin relationship
        let admin_rel = Relationship::new(
            NODE_RESOURCE_NAME,
            NODE_OBJECT_ID,
            ADMIN_RELATION,
            target_subject,
        );
        let deleted = self
            .store
            .delete_relationship(NODE_POLICY_ID, &admin_rel)
            .await?;

        if deleted {
            tracing::info!(
                target: "nac::audit",
                event = "admin_removed",
                requestor = %requestor,
                target = %target,
                "NAC admin relationship removed"
            );
        }

        Ok(deleted)
    }

    /// Add a direct permission grant to an identity.
    ///
    /// This is for granting individual permissions rather than full admin access.
    /// Write operations are blocked when NAC is disabled to prevent privilege escalation.
    pub async fn add_permission_grant(
        &self,
        requestor: &Did,
        target: &Did,
        permission: NodePermission,
    ) -> Result<bool> {
        // Block write operations when disabled to prevent privilege escalation
        let status = *self.status.read().await;
        if status == NacStatus::DisabledTemporarily {
            return Err(Error::InvalidPolicy(
                "cannot modify relationships while NAC is disabled - re-enable NAC first".into(),
            ));
        }

        // Only admins can grant permissions
        if !self.is_admin(requestor).await? {
            return Err(Error::NotOwner {
                operation: "grant permission".into(),
            });
        }

        // Convert target DID to appropriate Subject (wildcard "*" → Subject::Wildcard)
        let target_subject = if target.is_wildcard() {
            Subject::Wildcard
        } else {
            Subject::Entity(target.clone())
        };

        // Store direct relation for the permission
        let rel = Relationship::new(
            NODE_RESOURCE_NAME,
            NODE_OBJECT_ID,
            permission.as_str(),
            target_subject.clone(),
        );

        // Check if already exists
        let exists = self
            .store
            .has_relationship(
                NODE_POLICY_ID,
                NODE_RESOURCE_NAME,
                NODE_OBJECT_ID,
                permission.as_str(),
                &target_subject,
            )
            .await?;

        if exists {
            return Ok(false);
        }

        self.store.store_relationship(NODE_POLICY_ID, &rel).await?;

        tracing::info!(
            target: "nac::audit",
            event = "permission_granted",
            requestor = %requestor,
            target = %target,
            permission = %permission,
            "NAC permission granted"
        );

        Ok(true)
    }

    /// Remove a direct permission grant from an identity.
    ///
    /// Write operations are blocked when NAC is disabled to prevent privilege escalation.
    pub async fn remove_permission_grant(
        &self,
        requestor: &Did,
        target: &Did,
        permission: NodePermission,
    ) -> Result<bool> {
        // Block write operations when disabled to prevent privilege escalation
        let status = *self.status.read().await;
        if status == NacStatus::DisabledTemporarily {
            return Err(Error::InvalidPolicy(
                "cannot modify relationships while NAC is disabled - re-enable NAC first".into(),
            ));
        }

        // Only admins can revoke permissions
        if !self.is_admin(requestor).await? {
            return Err(Error::NotOwner {
                operation: "revoke permission".into(),
            });
        }

        let target_subject = if target.is_wildcard() {
            Subject::Wildcard
        } else {
            Subject::Entity(target.clone())
        };
        let rel = Relationship::new(
            NODE_RESOURCE_NAME,
            NODE_OBJECT_ID,
            permission.as_str(),
            target_subject,
        );
        let deleted = self.store.delete_relationship(NODE_POLICY_ID, &rel).await?;

        if deleted {
            tracing::info!(
                target: "nac::audit",
                event = "permission_revoked",
                requestor = %requestor,
                target = %target,
                permission = %permission,
                "NAC permission revoked"
            );
        }

        Ok(deleted)
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
}
