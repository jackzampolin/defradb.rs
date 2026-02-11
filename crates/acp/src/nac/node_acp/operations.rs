//! NAC permission checking and relationship management operations.

use identity::Did;

use super::{NacStatus, NodeACP, NODE_OBJECT_ID};
use crate::error::{Error, Result};
use crate::nac::permission::NodePermission;
use crate::nac::policy::{ADMIN_RELATION, NODE_POLICY_ID, NODE_RESOURCE_NAME};
use crate::zanzibar::{Relationship, Subject, ZanzibarStore};

impl<S: ZanzibarStore> NodeACP<S> {
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

        // Convert target DID to appropriate Subject (wildcard "*" -> Subject::Wildcard)
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

        // Convert target DID to appropriate Subject (wildcard "*" -> Subject::Wildcard)
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

        // Convert target DID to appropriate Subject (wildcard "*" -> Subject::Wildcard)
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
