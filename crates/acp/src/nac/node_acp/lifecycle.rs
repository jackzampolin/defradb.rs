//! NAC lifecycle management: enable, disable, re-enable, purge.

use identity::Did;

use super::{NacStatus, NodeACP, DISABLED_RELATION, NODE_OBJECT_ID};
use crate::error::{Error, Result};
use crate::nac::policy::{create_node_policy, NODE_POLICY_ID, NODE_RESOURCE_NAME, OWNER_RELATION};
use zanzibar::{Relationship, ZanzibarStore};

impl<S: ZanzibarStore> NodeACP<S> {
    /// Enable NAC with the given owner identity.
    ///
    /// The owner identity has full control over the node and can grant
    /// admin permissions to other identities.
    ///
    /// Succeeds silently if NAC is already enabled (idempotent).
    pub async fn enable(&self, owner: &Did) -> Result<()> {
        let status = *self.status.read().await;
        if status == NacStatus::Enabled {
            return Ok(());
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

        // Persist disabled flag so it survives restarts
        if let Some(owner) = self.owner.read().await.clone() {
            let disabled_rel = Relationship::with_entity(
                NODE_RESOURCE_NAME,
                NODE_OBJECT_ID,
                DISABLED_RELATION,
                owner.clone(),
            );
            self.store
                .store_relationship(NODE_POLICY_ID, &disabled_rel)
                .await?;
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

        // Remove persisted disabled flag
        if let Some(owner) = self.owner.read().await.clone() {
            let disabled_rel = Relationship::with_entity(
                NODE_RESOURCE_NAME,
                NODE_OBJECT_ID,
                DISABLED_RELATION,
                owner.clone(),
            );
            let _ = self
                .store
                .delete_relationship(NODE_POLICY_ID, &disabled_rel)
                .await;
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
}
